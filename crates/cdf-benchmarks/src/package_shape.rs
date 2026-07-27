use std::{error::Error as _, fs::File, path::Path, time::Instant};

use arrow_ipc::reader::FileReader;
use arrow_schema::ArrowError;
use cdf_kernel::CdfError;
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

    reader.for_each_identity_segment(&mut |segment| {
        segment_count = segment_count.saturating_add(1);
        package_data_bytes = package_data_bytes.saturating_add(segment.byte_count);
        min_segment_rows = min_segment_rows.min(segment.row_count);
        max_segment_rows = max_segment_rows.max(segment.row_count);
        row_count = row_count.saturating_add(segment.row_count);
        let path = package_dir.join(&segment.path);
        let segment_batch_count = u64::try_from(open_segment_reader(&path)?.num_batches())
            .map_err(|_| CdfError::data("package segment batch count exceeds u64"))?;
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
        Ok(())
    })?;

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

    reader.for_each_identity_segment(&mut |segment| {
        segment_count = segment_count.saturating_add(1);
        package_data_bytes = package_data_bytes.saturating_add(segment.byte_count);
        let path = package_dir.join(&segment.path);
        let file_reader = open_segment_reader(&path)?;
        let mut segment_rows = 0_u64;
        for batch in file_reader {
            let batch = batch.map_err(|error| package_segment_decode_error(&path, error))?;
            let rows = u64::try_from(batch.num_rows())
                .map_err(|_| CdfError::data("package batch row count exceeds u64"))?;
            batch_count = batch_count.saturating_add(1);
            row_count = row_count.saturating_add(rows);
            segment_rows = segment_rows.saturating_add(rows);
            std::hint::black_box(batch);
        }
        if segment_rows != segment.row_count {
            return Err(CdfError::data(format!(
                "package segment {} decoded {segment_rows} rows but manifest records {}",
                segment.segment_id.as_str(),
                segment.row_count
            )));
        }
        Ok(())
    })?;

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

fn open_segment_reader(path: &Path) -> Result<FileReader<File>, CdfError> {
    let file = File::open(path).map_err(|error| package_segment_open_error(path, error))?;
    FileReader::try_new(file, None).map_err(|error| package_segment_decode_error(path, error))
}

fn package_segment_open_error(path: &Path, error: std::io::Error) -> CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidData
            | std::io::ErrorKind::IsADirectory
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::data(format!(
            "benchmark package segment {} is missing or has the wrong filesystem shape: {error}",
            path.display()
        ))
    } else {
        CdfError::environment(format!(
            "benchmark host cannot open package segment {}: {error}; check path permissions, device availability, memory, and process file limits before retrying",
            path.display()
        ))
    }
}

fn package_segment_decode_error(path: &Path, error: ArrowError) -> CdfError {
    let mut first_raw_io = None;
    let mut source = error.source();
    while let Some(candidate) = source {
        if let Some(error) = candidate.downcast_ref::<CdfError>() {
            return package_segment_error_context(path, error.clone());
        }
        if let Some(io_error) = candidate.downcast_ref::<std::io::Error>() {
            if let Some(error) = cdf_kernel::embedded_cdf_error(io_error) {
                return package_segment_error_context(path, error);
            }
            first_raw_io.get_or_insert_with(|| (io_error.kind(), io_error.to_string()));
        }
        source = candidate.source();
    }
    if let Some((kind, message)) = first_raw_io {
        return package_segment_open_error(path, std::io::Error::new(kind, message));
    }
    match error {
        ArrowError::IoError(_, source) => package_segment_open_error(path, source),
        ArrowError::MemoryError(message) => CdfError::environment(format!(
            "benchmark host cannot decode package segment {}: {message}; free memory or reduce the benchmark workload before retrying",
            path.display()
        )),
        error => CdfError::data(format!(
            "benchmark package segment {} is malformed: {error}",
            path.display()
        )),
    }
}

fn package_segment_error_context(path: &Path, mut error: CdfError) -> CdfError {
    error.message = format!(
        "decode benchmark package segment {}: {}",
        path.display(),
        error.message
    );
    error
}

#[cfg(test)]
mod tests {
    use cdf_kernel::ErrorKind;

    use super::*;

    #[test]
    fn package_segment_errors_distinguish_product_artifacts_from_benchmark_host_failures() {
        let path = Path::new("segments/part.arrow");
        let missing =
            package_segment_open_error(path, std::io::Error::from(std::io::ErrorKind::NotFound));
        assert_eq!(missing.kind, ErrorKind::Data);
        assert!(missing.message.contains("package segment"));

        let denied = package_segment_open_error(
            path,
            std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        );
        assert_eq!(denied.kind, ErrorKind::Environment);
        assert!(denied.message.contains("benchmark host"));
    }

    #[test]
    fn package_segment_parent_file_shape_and_malformed_ipc_are_data_owned() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("segments");
        std::fs::write(&parent, b"not a directory").unwrap();
        let nested = parent.join("part.arrow");
        let open_error = open_segment_reader(&nested).unwrap_err();
        assert_eq!(open_error.kind, ErrorKind::Data);

        let malformed = root.path().join("malformed.arrow");
        std::fs::write(&malformed, b"not arrow ipc").unwrap();
        let decode_error = open_segment_reader(&malformed).unwrap_err();
        assert_eq!(decode_error.kind, ErrorKind::Data);
        assert!(decode_error.message.contains("malformed"));
    }

    #[test]
    fn package_segment_decoder_preserves_embedded_cdf_ownership() {
        let embedded = CdfError::rate_limited("embedded package reader owner", Some(125));
        let error = ArrowError::ExternalError(Box::new(std::io::Error::other(embedded.clone())));

        let classified = package_segment_decode_error(Path::new("segment.arrow"), error);

        assert_eq!(classified.kind, embedded.kind);
        assert_eq!(classified.retry_after_ms, embedded.retry_after_ms);
        assert!(classified.message.contains("segment.arrow"));
        assert!(classified.message.contains("embedded package reader owner"));

        let contract = CdfError::contract("embedded package contract");
        let direct = ArrowError::ExternalError(Box::new(contract.clone()));
        let classified = package_segment_decode_error(Path::new("segment.arrow"), direct);
        assert_eq!(classified.kind, contract.kind);
        assert!(classified.message.contains("embedded package contract"));

        let deeply_embedded = CdfError::rate_limited("deep package reader owner", Some(375));
        let nested_io = std::io::Error::other(std::io::Error::other(deeply_embedded.clone()));
        let error = ArrowError::ExternalError(Box::new(nested_io));
        let classified = package_segment_decode_error(Path::new("segment.arrow"), error);
        assert_eq!(classified.kind, deeply_embedded.kind);
        assert_eq!(classified.retry_after_ms, deeply_embedded.retry_after_ms);
        assert!(classified.message.contains("deep package reader owner"));
    }

    #[test]
    fn package_segment_decoder_classifies_nested_raw_host_io() {
        let error = ArrowError::ExternalError(Box::new(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        )));

        let classified = package_segment_decode_error(Path::new("segment.arrow"), error);

        assert_eq!(classified.kind, ErrorKind::Environment);
        assert!(classified.message.contains("benchmark host"));
    }
}
