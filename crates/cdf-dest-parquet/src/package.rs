use std::io::{self, BufWriter, Write};

use cdf_memory::MemoryCoordinator;
use cdf_runtime::{SpillBudgetCoordinator, SpillReservation};
use parquet::{
    arrow::ArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties},
};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::*;

const SPILL_GROWTH_BYTES: u64 = 8 * 1024 * 1024;
const OUTPUT_BUFFER_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy)]
pub(crate) struct ParquetWriterSettings {
    pub(crate) rows_per_batch: u64,
    pub(crate) bytes_per_batch: u64,
}

impl ParquetWriterSettings {
    fn validate(self) -> Result<Self> {
        if self.rows_per_batch == 0 || self.bytes_per_batch == 0 {
            return Err(CdfError::contract(
                "Parquet writer row and byte bounds must be nonzero",
            ));
        }
        usize::try_from(self.rows_per_batch)
            .map_err(|_| CdfError::contract("Parquet writer row bound exceeds platform usize"))?;
        usize::try_from(self.bytes_per_batch)
            .map_err(|_| CdfError::contract("Parquet writer byte bound exceeds platform usize"))?;
        Ok(self)
    }
}

pub(crate) struct EncodedParquetObject {
    pub(crate) file: NamedTempFile,
    pub(crate) byte_count: u64,
    pub(crate) sha256: String,
    pub(crate) _spill: SpillReservation,
}

struct ParquetBatchWritePlan<'a> {
    retained_bytes: u64,
    expected_rows: u64,
    expected_schema: Option<&'a arrow_schema::Schema>,
    cancellation: Option<&'a cdf_runtime::RunCancellation>,
    settings: ParquetWriterSettings,
}

#[cfg(test)]
pub(crate) fn write_parquet_segment(
    segment: CommitSegment,
    settings: ParquetWriterSettings,
    writer_memory: Arc<dyn MemoryCoordinator>,
    spill: Arc<dyn SpillBudgetCoordinator>,
    file: NamedTempFile,
) -> Result<(StateSegment, u64, EncodedParquetObject)> {
    let retained_bytes = segment.retained_bytes().max(1);
    let expected_rows = segment.state.row_count;
    let state = segment.state.clone();
    let package_byte_count = segment.package_byte_count;
    let mut batches = segment.into_batches()?;
    let encoded = write_parquet_batches(
        ParquetBatchWritePlan {
            retained_bytes,
            expected_rows,
            expected_schema: None,
            cancellation: None,
            settings,
        },
        writer_memory,
        spill,
        file,
        || Ok(batches.next().map(|batch| batch.batch)),
    )?;
    Ok((state, package_byte_count, encoded))
}

pub(crate) fn write_parquet_staged_segment(
    mut segment: cdf_runtime::StagedSegmentRequest,
    expected_schema: &arrow_schema::Schema,
    writer_memory: Arc<dyn MemoryCoordinator>,
    spill: Arc<dyn SpillBudgetCoordinator>,
    file: NamedTempFile,
    cancellation: &cdf_runtime::RunCancellation,
    settings: ParquetWriterSettings,
) -> Result<(cdf_runtime::StagedSegmentIdentity, EncodedParquetObject)> {
    let identity = segment.identity.clone();
    let encoded = write_parquet_batches(
        ParquetBatchWritePlan {
            retained_bytes: identity.byte_count.max(1),
            expected_rows: identity.row_count,
            expected_schema: Some(expected_schema),
            cancellation: Some(cancellation),
            settings,
        },
        writer_memory,
        spill,
        file,
        || segment.reader_mut().next_batch(),
    )?;
    Ok((identity, encoded))
}

fn write_parquet_batches(
    plan: ParquetBatchWritePlan<'_>,
    writer_memory: Arc<dyn MemoryCoordinator>,
    spill: Arc<dyn SpillBudgetCoordinator>,
    file: NamedTempFile,
    mut next_batch: impl FnMut() -> Result<Option<arrow_array::RecordBatch>>,
) -> Result<EncodedParquetObject> {
    let ParquetBatchWritePlan {
        retained_bytes,
        expected_rows,
        expected_schema,
        cancellation,
        settings,
    } = plan;
    let settings = settings.validate()?;
    let writer_bytes = settings
        .bytes_per_batch
        .clamp(1024 * 1024, 64 * 1024 * 1024);
    let request = cdf_memory::ReservationRequest::new(
        cdf_memory::ConsumerKey::new(
            "parquet-row-group-writer",
            cdf_memory::MemoryClass::Destination,
        )?,
        writer_bytes,
    )?
    .as_minimum_working_set();
    let snapshot = writer_memory.snapshot();
    let memory_failure = |detail: Option<&CdfError>| {
        CdfError::data(format!(
            "Parquet destination needs {writer_bytes} additional accounted writer bytes while staged input retains memory, but only {} of {} managed bytes are free{}; reduce canonical segment size or destination concurrency, or raise the memory budget",
            snapshot.budget_bytes.saturating_sub(snapshot.current_bytes),
            snapshot.budget_bytes,
            detail.map_or_else(String::new, |error| format!(" ({error})"))
        ))
    };
    let _writer_lease = writer_memory
        .try_reserve(&request)
        .map_err(|error| memory_failure(Some(&error)))?
        .ok_or_else(|| memory_failure(None))?;
    let initial_spill = retained_bytes.clamp(1, SPILL_GROWTH_BYTES);
    let reservation = spill.try_reserve(initial_spill)?.ok_or_else(|| {
        CdfError::data(format!(
            "Parquet segment staging requires at least {initial_spill} spill bytes; raise the spill budget"
        ))
    })?;
    let first = next_batch()?
        .ok_or_else(|| CdfError::data("Parquet destination segment contains no Arrow batches"))?;
    if let Some(cancellation) = cancellation {
        cancellation.check()?;
    }
    let schema = first.schema();
    cdf_package::validate_parquet_schema(schema.as_ref())?;
    if expected_schema.is_some_and(|expected| expected != schema.as_ref()) {
        return Err(CdfError::data(
            "Parquet staged segment schema differs from the planned output schema",
        ));
    }
    let mut output = SpillHashWriter::new(file, reservation);
    let rows_per_batch = usize::try_from(settings.rows_per_batch)
        .map_err(|_| CdfError::contract("Parquet writer row bound exceeds platform usize"))?;
    let bytes_per_batch = usize::try_from(settings.bytes_per_batch)
        .map_err(|_| CdfError::contract("Parquet writer byte bound exceeds platform usize"))?;
    let data_page_rows = rows_per_batch.min(64 * 1024);
    let data_page_bytes = bytes_per_batch.min(8 * 1024 * 1024);
    let properties = WriterProperties::builder()
        .set_created_by("cdf native arrow-rs parquet writer".to_owned())
        .set_write_batch_size(rows_per_batch)
        .set_data_page_row_count_limit(data_page_rows)
        .set_data_page_size_limit(data_page_bytes)
        .set_max_row_group_row_count(Some(rows_per_batch))
        .set_max_row_group_bytes(Some(bytes_per_batch))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    {
        let mut writer = ArrowWriter::try_new(&mut output, schema.clone(), Some(properties))
            .map_err(|error| parquet_error("create streaming Parquet writer", error))?;
        let mut rows = u64::try_from(first.num_rows())
            .map_err(|_| CdfError::data("Parquet destination row count exceeds u64"))?;
        writer
            .write(&first)
            .map_err(|error| parquet_error("write Parquet record batch", error))?;
        while let Some(batch) = next_batch()? {
            if let Some(cancellation) = cancellation {
                cancellation.check()?;
            }
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(CdfError::data(
                    "Parquet destination segment contains mixed Arrow schemas",
                ));
            }
            rows = rows
                .checked_add(
                    u64::try_from(batch.num_rows())
                        .map_err(|_| CdfError::data("Parquet destination row count exceeds u64"))?,
                )
                .ok_or_else(|| CdfError::data("Parquet destination row count overflow"))?;
            writer
                .write(&batch)
                .map_err(|error| parquet_error("write Parquet record batch", error))?;
        }
        if let Some(cancellation) = cancellation {
            cancellation.check()?;
        }
        if rows != expected_rows {
            return Err(CdfError::data(format!(
                "Parquet destination segment has {rows} payload rows but its durable identity expects {expected_rows}"
            )));
        }
        writer
            .close()
            .map_err(|error| parquet_error("finish streaming Parquet writer", error))?;
    }
    output.finish()
}

struct SpillHashWriter {
    file: BufWriter<NamedTempFile>,
    hash: Sha256,
    bytes: u64,
    spill: SpillReservation,
}

impl SpillHashWriter {
    fn new(file: NamedTempFile, spill: SpillReservation) -> Self {
        Self {
            file: BufWriter::with_capacity(OUTPUT_BUFFER_BYTES, file),
            hash: Sha256::new(),
            bytes: 0,
            spill,
        }
    }

    fn finish(mut self) -> Result<EncodedParquetObject> {
        self.flush().map_err(|error| {
            CdfError::destination(format!("flush Parquet staging file: {error}"))
        })?;
        self.file.get_ref().as_file().sync_all().map_err(|error| {
            CdfError::destination(format!("sync Parquet staging file: {error}"))
        })?;
        let file = self.file.into_inner().map_err(|error| {
            CdfError::destination(format!("finish Parquet staging buffer: {error}"))
        })?;
        Ok(EncodedParquetObject {
            file,
            byte_count: self.bytes,
            sha256: hex::encode(self.hash.finalize()),
            _spill: self.spill,
        })
    }

    fn ensure_spill(&mut self, additional: usize) -> io::Result<()> {
        let required = self
            .bytes
            .checked_add(u64::try_from(additional).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("Parquet staging byte count overflowed"))?;
        if required <= self.spill.bytes() {
            return Ok(());
        }
        let growth = required
            .saturating_sub(self.spill.bytes())
            .next_multiple_of(SPILL_GROWTH_BYTES);
        if self.spill.try_grow(growth).map_err(io::Error::other)? {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "Parquet staging exceeded the spill budget at {required} bytes"
            )))
        }
    }
}

impl Write for SpillHashWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.ensure_spill(bytes.len())?;
        let written = self.file.write(bytes)?;
        self.hash.update(&bytes[..written]);
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(written).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("Parquet staging byte count overflowed"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn parquet_error(context: &str, error: impl std::fmt::Display) -> CdfError {
    CdfError::destination(format!("{context}: {error}"))
}
