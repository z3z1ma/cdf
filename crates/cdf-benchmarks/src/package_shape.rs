use std::{fs::File, path::Path, time::Instant};

use arrow_ipc::reader::FileReader;
use serde::Serialize;

use crate::BenchResult;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PackageShapeSummary {
    pub package_id: String,
    pub package_hash: String,
    pub segment_count: u64,
    pub batch_count: u64,
    pub row_count: u64,
    pub package_data_bytes: u64,
    pub min_segment_rows: u64,
    pub max_segment_rows: u64,
    pub min_estimated_batch_rows: u64,
    pub max_estimated_batch_rows: u64,
    pub average_segment_rows: u64,
    pub average_batch_rows: u64,
    pub single_batch_segments: u64,
    pub multi_batch_segments: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PackageReadSummary {
    pub package_id: String,
    pub package_hash: String,
    pub segment_count: u64,
    pub batch_count: u64,
    pub row_count: u64,
    pub package_data_bytes: u64,
    pub timed_wall_time_ns: u64,
}

pub fn summarize_package_shape(package_dir: impl AsRef<Path>) -> BenchResult<PackageShapeSummary> {
    let package_dir = package_dir.as_ref();
    let reader = cdf_package::PackageReader::open(package_dir)?;
    let manifest = reader.manifest();
    let mut segment_count = 0_u64;
    let mut batch_count = 0_u64;
    let mut row_count = 0_u64;
    let mut package_data_bytes = 0_u64;
    let mut min_segment_rows = u64::MAX;
    let mut max_segment_rows = 0_u64;
    let mut min_estimated_batch_rows = u64::MAX;
    let mut max_estimated_batch_rows = 0_u64;
    let mut single_batch_segments = 0_u64;
    let mut multi_batch_segments = 0_u64;

    for segment in &manifest.identity.segments {
        segment_count = segment_count.saturating_add(1);
        package_data_bytes = package_data_bytes.saturating_add(segment.byte_count);
        min_segment_rows = min_segment_rows.min(segment.row_count);
        max_segment_rows = max_segment_rows.max(segment.row_count);
        row_count = row_count.saturating_add(segment.row_count);
        let path = package_dir.join(&segment.path);
        let file = File::open(&path)?;
        let segment_batch_count = u64::try_from(FileReader::try_new(file, None)?.num_batches())?;
        batch_count = batch_count.saturating_add(segment_batch_count);
        if segment_batch_count == 1 {
            single_batch_segments = single_batch_segments.saturating_add(1);
        } else {
            multi_batch_segments = multi_batch_segments.saturating_add(1);
        }
        if let Some(average) = segment.row_count.checked_div(segment_batch_count) {
            let ceiling = segment.row_count.div_ceil(segment_batch_count);
            min_estimated_batch_rows = min_estimated_batch_rows.min(average);
            max_estimated_batch_rows = max_estimated_batch_rows.max(ceiling);
        }
    }

    if segment_count == 0 {
        min_segment_rows = 0;
    }
    if batch_count == 0 {
        min_estimated_batch_rows = 0;
    }

    Ok(PackageShapeSummary {
        package_id: manifest.identity.package_id.clone(),
        package_hash: manifest.package_hash.clone(),
        segment_count,
        batch_count,
        row_count,
        package_data_bytes,
        min_segment_rows,
        max_segment_rows,
        min_estimated_batch_rows,
        max_estimated_batch_rows,
        average_segment_rows: row_count.checked_div(segment_count).unwrap_or(0),
        average_batch_rows: row_count.checked_div(batch_count).unwrap_or(0),
        single_batch_segments,
        multi_batch_segments,
    })
}

pub fn read_package_batches(package_dir: impl AsRef<Path>) -> BenchResult<PackageReadSummary> {
    let package_dir = package_dir.as_ref();
    let reader = cdf_package::PackageReader::open(package_dir)?;
    let manifest = reader.manifest();
    let started = Instant::now();
    let mut segment_count = 0_u64;
    let mut batch_count = 0_u64;
    let mut row_count = 0_u64;
    let mut package_data_bytes = 0_u64;

    for segment in &manifest.identity.segments {
        segment_count = segment_count.saturating_add(1);
        package_data_bytes = package_data_bytes.saturating_add(segment.byte_count);
        let path = package_dir.join(&segment.path);
        let file = File::open(&path)?;
        let file_reader = FileReader::try_new(file, None)?;
        let mut segment_rows = 0_u64;
        for batch in file_reader {
            let batch = batch?;
            let rows = u64::try_from(batch.num_rows())?;
            batch_count = batch_count.saturating_add(1);
            row_count = row_count.saturating_add(rows);
            segment_rows = segment_rows.saturating_add(rows);
            std::hint::black_box(batch);
        }
        if segment_rows != segment.row_count {
            return Err(format!(
                "package segment {} decoded {segment_rows} rows but manifest records {}",
                segment.segment_id.as_str(),
                segment.row_count
            )
            .into());
        }
    }

    Ok(PackageReadSummary {
        package_id: manifest.identity.package_id.clone(),
        package_hash: manifest.package_hash.clone(),
        segment_count,
        batch_count,
        row_count,
        package_data_bytes,
        timed_wall_time_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
    })
}
