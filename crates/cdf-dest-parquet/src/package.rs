use crate::*;

#[derive(Clone, Debug)]
pub(crate) struct PackageData {
    pub(crate) segments: Vec<LoadedSegment>,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct LoadedSegment {
    pub(crate) entry: SegmentEntry,
    pub(crate) row_count: u64,
    pub(crate) batches: Vec<RecordBatch>,
}

pub(crate) fn load_package_data(package_dir: &Path) -> Result<PackageData> {
    let reader = PackageReader::open(package_dir)?;
    reader.verify()?;
    let segments = reader.read_all_segments()?;
    package_data_from_segments(segments)
}

pub(crate) fn package_data_from_commit_segments(
    segments: Vec<CommitSegment>,
) -> Result<PackageData> {
    let segments = segments
        .into_iter()
        .map(|segment| {
            let CommitSegment {
                state,
                package_byte_count,
                batches,
            } = segment;
            (
                SegmentEntry {
                    segment_id: state.segment_id,
                    path: String::new(),
                    row_count: state.row_count,
                    byte_count: package_byte_count,
                    sha256: String::new(),
                },
                batches,
            )
        })
        .collect::<Vec<_>>();
    package_data_from_segments(segments)
}

fn package_data_from_segments(
    segments: Vec<(SegmentEntry, Vec<RecordBatch>)>,
) -> Result<PackageData> {
    if segments.is_empty() {
        return Err(CdfError::data(
            "Parquet destination requires at least one package segment",
        ));
    }

    let schema = first_schema(&segments)?;
    let mut loaded = Vec::with_capacity(segments.len());
    let mut rows = 0_u64;
    let mut bytes = 0_u64;
    for (entry, batches) in segments {
        if batches.is_empty() {
            return Err(CdfError::data(format!(
                "package segment {} contains no batches",
                entry.segment_id.as_str()
            )));
        }
        let mut row_count = 0_u64;
        for batch in &batches {
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(CdfError::data(
                    "Parquet destination requires all package segments to share one schema",
                ));
            }
            row_count += batch.num_rows() as u64;
        }
        if row_count != entry.row_count {
            return Err(CdfError::data(format!(
                "segment {} manifest row count {} differs from package data {}",
                entry.segment_id.as_str(),
                entry.row_count,
                row_count
            )));
        }
        rows += row_count;
        bytes += entry.byte_count;
        loaded.push(LoadedSegment {
            entry,
            row_count,
            batches,
        });
    }

    Ok(PackageData {
        segments: loaded,
        rows,
        bytes,
    })
}

pub(crate) fn validate_requested_segments(
    requested: &[StateSegment],
    package: &PackageData,
) -> Result<()> {
    if requested.is_empty() {
        return Ok(());
    }
    let package_segments = package
        .segments
        .iter()
        .map(|segment| {
            (
                segment.entry.segment_id.as_str(),
                (segment.row_count, segment.entry.byte_count),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for segment in requested {
        match package_segments.get(segment.segment_id.as_str()) {
            Some((row_count, _byte_count)) if *row_count == segment.row_count => {}
            Some((row_count, byte_count)) => {
                return Err(CdfError::data(format!(
                    "requested segment {} has {} rows/{} bytes but package has {} rows/{} package bytes",
                    segment.segment_id.as_str(),
                    segment.row_count,
                    segment.byte_count,
                    row_count,
                    byte_count
                )));
            }
            None => {
                return Err(CdfError::data(format!(
                    "requested segment {} is not present in package",
                    segment.segment_id.as_str()
                )));
            }
        }
    }
    Ok(())
}

fn first_schema(segments: &[(SegmentEntry, Vec<RecordBatch>)]) -> Result<SchemaRef> {
    segments
        .iter()
        .flat_map(|(_, batches)| batches.iter())
        .next()
        .map(RecordBatch::schema)
        .ok_or_else(|| CdfError::data("Parquet destination found no record batches in package"))
}

pub(crate) fn write_parquet_segment(segment: &LoadedSegment) -> Result<Vec<u8>> {
    cdf_package::transcode_record_batches_to_parquet_bytes(&segment.batches)
}
