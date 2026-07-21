use arrow_schema::{DataType, Field, Schema, UnionMode};
use cdf_kernel::{CdfError, Result};

use crate::DuckDbNativeResources;

/// Destination-local admission for DuckDB's canonical scan/sink pipeline.
///
/// DuckDB's global worker count remains available to query execution. This
/// envelope independently limits the number of canonical IPC batches that can
/// be resident while a bulk insert is building row groups.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DuckDbIngestEnvelope {
    initial_scan_threads: usize,
    automatic: bool,
    estimated_worker_bytes: u64,
}

impl DuckDbIngestEnvelope {
    pub(crate) fn resolve(
        resources: &DuckDbNativeResources,
        segment_schema: &Schema,
        rows_per_batch: u64,
        bytes_per_batch: u64,
    ) -> Result<Self> {
        let global_threads = usize::try_from(resources.internal_threads.max(1))
            .map_err(|_| CdfError::contract("DuckDB global thread count exceeds usize"))?;
        if let Some(explicit) = resources.scan_threads_override {
            return Ok(Self {
                initial_scan_threads: explicit.min(global_threads),
                automatic: false,
                estimated_worker_bytes: estimate_worker_bytes(
                    segment_schema,
                    rows_per_batch,
                    bytes_per_batch,
                ),
            });
        }

        let estimated_worker_bytes =
            estimate_worker_bytes(segment_schema, rows_per_batch, bytes_per_batch);
        // Half of the admitted DuckDB envelope is reserved for storage/sink
        // state. The other half admits concurrently retained Arrow batches.
        // A typed OOM retry below is the backstop for variable-width values
        // whose payload cannot be known from the compiled schema alone.
        let scan_budget = resources.memory_limit_bytes / 2;
        let admitted =
            scan_budget
                .checked_div(estimated_worker_bytes)
                .map_or(global_threads, |workers| {
                    usize::try_from(workers)
                        .unwrap_or(usize::MAX)
                        .max(1)
                        .min(global_threads)
                });
        Ok(Self {
            initial_scan_threads: admitted,
            automatic: true,
            estimated_worker_bytes,
        })
    }

    pub(crate) const fn initial_scan_threads(self) -> usize {
        self.initial_scan_threads
    }

    pub(crate) const fn is_automatic(self) -> bool {
        self.automatic
    }

    pub(crate) const fn estimated_worker_bytes(self) -> u64 {
        self.estimated_worker_bytes
    }

    pub(crate) fn next_retry_threads(self, current: usize) -> Option<usize> {
        if !self.automatic || current <= 1 {
            None
        } else {
            Some((current / 2).max(1))
        }
    }
}

fn estimate_worker_bytes(schema: &Schema, rows_per_batch: u64, bytes_per_batch: u64) -> u64 {
    let arrow_row_bytes = schema.fields().iter().fold(0_u128, |total, field| {
        total.saturating_add(u128::from(arrow_row_width(field)))
    });
    let duckdb_row_bytes = schema.fields().iter().fold(0_u128, |total, field| {
        total.saturating_add(u128::from(duckdb_vector_row_width(field)))
    });
    // Query the linked DuckDB rather than freezing its standard vector size in
    // CDF. The call has no mutable state and is stable for the linked runtime.
    // SAFETY: `duckdb_vector_size` accepts no pointers and returns a value.
    let duckdb_vector_rows = unsafe { duckdb::ffi::duckdb_vector_size() };
    let total = arrow_row_bytes
        .saturating_mul(u128::from(rows_per_batch))
        .saturating_add(duckdb_row_bytes.saturating_mul(u128::from(duckdb_vector_rows)))
        .saturating_add(u128::from(bytes_per_batch));
    u64::try_from(total).unwrap_or(u64::MAX)
}

fn arrow_row_width(field: &Field) -> u64 {
    type_width(field.data_type(), Layout::Arrow).saturating_add(if field.is_nullable() {
        1
    } else {
        0
    })
}

fn duckdb_vector_row_width(field: &Field) -> u64 {
    type_width(field.data_type(), Layout::DuckDb).saturating_add(if field.is_nullable() {
        1
    } else {
        0
    })
}

#[derive(Clone, Copy)]
enum Layout {
    Arrow,
    DuckDb,
}

fn type_width(data_type: &DataType, layout: Layout) -> u64 {
    match data_type {
        DataType::Null => 0,
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => 2,
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Decimal32(_, _) => 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Decimal64(_, _) => 8,
        DataType::Interval(_) | DataType::Decimal128(_, _) => 16,
        DataType::Decimal256(_, _) => 32,
        DataType::Utf8 | DataType::Binary => match layout {
            Layout::Arrow => 4,
            Layout::DuckDb => 16,
        },
        DataType::LargeUtf8 | DataType::LargeBinary => match layout {
            Layout::Arrow => 8,
            Layout::DuckDb => 16,
        },
        DataType::Utf8View | DataType::BinaryView => 16,
        DataType::FixedSizeBinary(width) => u64::try_from(*width).unwrap_or(u64::MAX),
        DataType::List(child) | DataType::ListView(child) => list_width(4, child.as_ref(), layout),
        DataType::LargeList(child) | DataType::LargeListView(child) => {
            list_width(8, child.as_ref(), layout)
        }
        DataType::FixedSizeList(child, count) => type_width(child.data_type(), layout)
            .saturating_mul(u64::try_from(*count).unwrap_or(u64::MAX)),
        DataType::Struct(fields) => fields.iter().fold(0_u64, |total, field| {
            total.saturating_add(type_width(field.data_type(), layout))
        }),
        DataType::Union(fields, mode) => {
            let children = match mode {
                UnionMode::Sparse => fields.iter().fold(0_u64, |total, (_, field)| {
                    total.saturating_add(type_width(field.data_type(), layout))
                }),
                UnionMode::Dense => fields
                    .iter()
                    .map(|(_, field)| type_width(field.data_type(), layout))
                    .max()
                    .unwrap_or(0),
            };
            children.saturating_add(if *mode == UnionMode::Dense { 5 } else { 1 })
        }
        DataType::Dictionary(index, value) => {
            type_width(index, layout).saturating_add(type_width(value, layout).min(16))
        }
        DataType::Map(entries, _) => list_width(4, entries.as_ref(), layout),
        DataType::RunEndEncoded(run_ends, values) => type_width(run_ends.data_type(), layout)
            .saturating_add(type_width(values.data_type(), layout)),
    }
}

fn list_width(offset_bytes: u64, child: &Field, layout: Layout) -> u64 {
    match layout {
        Layout::Arrow => offset_bytes.saturating_add(type_width(child.data_type(), layout)),
        Layout::DuckDb => 16,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::DuckDbNativeResourceOverrides;
    use arrow_schema::{DataType, Field, Schema};

    fn resources(memory: u64, threads: i64, scan_threads: Option<usize>) -> DuckDbNativeResources {
        let spill = Arc::new(cdf_runtime::FixedSpillBudget::new(2 * 1024 * 1024 * 1024).unwrap());
        DuckDbNativeResources::for_budgets_with_overrides(
            memory,
            spill,
            DuckDbNativeResourceOverrides {
                memory_limit_bytes: Some(memory),
                maximum_temp_directory_bytes: Some(1024 * 1024),
                internal_threads: Some(threads),
                scan_threads,
                max_in_flight_bytes: None,
            },
        )
        .unwrap()
    }

    #[test]
    fn ordinary_schema_retains_global_parallelism() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                false,
            ),
        ]);
        let resources = resources(4 * 1024 * 1024 * 1024, 16, None);
        let envelope =
            DuckDbIngestEnvelope::resolve(&resources, &schema, 64 * 1024, 16 * 1024 * 1024)
                .unwrap();
        assert_eq!(envelope.initial_scan_threads(), 16);
        assert!(envelope.is_automatic());
    }

    #[test]
    fn wide_string_schema_derives_memory_bounded_parallelism_without_a_field_cutoff() {
        let fields = (0..2_052)
            .map(|index| Field::new(format!("field_{index}"), DataType::Utf8, true))
            .collect::<Vec<_>>();
        let schema = Schema::new(fields);
        let resources = resources(4 * 1024 * 1024 * 1024, 16, None);
        let envelope =
            DuckDbIngestEnvelope::resolve(&resources, &schema, 64 * 1024, 16 * 1024 * 1024)
                .unwrap();
        assert_eq!(envelope.initial_scan_threads(), 2);
        assert!(envelope.estimated_worker_bytes() > 512 * 1024 * 1024);
        assert_eq!(envelope.next_retry_threads(2), Some(1));
        assert_eq!(envelope.next_retry_threads(1), None);
    }

    #[test]
    fn explicit_scan_concurrency_is_authoritative_and_disables_automatic_retry() {
        let schema = Schema::new(vec![Field::new("value", DataType::Utf8, true)]);
        let resources = resources(64 * 1024 * 1024, 16, Some(12));
        let envelope =
            DuckDbIngestEnvelope::resolve(&resources, &schema, 64 * 1024, 16 * 1024 * 1024)
                .unwrap();
        assert_eq!(envelope.initial_scan_threads(), 12);
        assert!(!envelope.is_automatic());
        assert_eq!(envelope.next_retry_threads(12), None);
    }
}
