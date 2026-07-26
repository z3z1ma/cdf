use std::{
    fs::{self, File},
    io::Read,
    path::Path,
    sync::Arc,
};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BenchResult, bench_error};

const MAX_GENERATOR_BATCH_BYTES: u64 = 64 * 1024 * 1024;
const MAX_STRESS_FILES: u32 = 1_000_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstantMemoryParquetRecipe {
    pub schema_version: u16,
    pub generator_version: String,
    pub file_count: u32,
    pub rows_per_file: u64,
    pub total_rows: u64,
    pub logical_bytes_per_file: u64,
    pub total_logical_bytes: u64,
    pub unique_physical_bytes: u64,
    pub represented_physical_bytes: u64,
    pub generator_peak_batch_bytes: u64,
    pub base_file_sha256: String,
    pub file_glob: String,
}

pub fn generate_constant_memory_parquet(
    output_root: &Path,
    file_count: u32,
    minimum_logical_bytes_per_file: u64,
    batch_rows: usize,
    payload_bytes: usize,
) -> BenchResult<ConstantMemoryParquetRecipe> {
    if file_count == 0 || file_count > MAX_STRESS_FILES {
        return Err(bench_error(format!(
            "constant-memory generator file_count must be between 1 and {MAX_STRESS_FILES}"
        )));
    }
    if minimum_logical_bytes_per_file == 0 {
        return Err(bench_error(
            "constant-memory generator logical bytes per file must be positive",
        ));
    }
    if batch_rows == 0 || payload_bytes == 0 {
        return Err(bench_error(
            "constant-memory generator batch rows and payload bytes must be positive",
        ));
    }
    let estimated_batch_bytes = batch_rows
        .checked_mul(
            payload_bytes
                .checked_add(32)
                .ok_or_else(|| bench_error("constant-memory generator payload size overflow"))?,
        )
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| bench_error("constant-memory generator batch size overflow"))?;
    if estimated_batch_bytes > MAX_GENERATOR_BATCH_BYTES {
        return Err(bench_error(format!(
            "constant-memory generator batch would retain at least {estimated_batch_bytes} bytes above its {MAX_GENERATOR_BATCH_BYTES}-byte setup ceiling; lower batch rows or payload bytes"
        )));
    }
    fs::create_dir_all(output_root)?;
    if fs::read_dir(output_root)?.next().is_some() {
        return Err(bench_error(format!(
            "constant-memory generator output must be empty: {}",
            output_root.display()
        )));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let payload = deterministic_payload(payload_bytes);
    let base_path = output_root.join("part-000000.parquet");
    let file = File::create(&base_path)?;
    let writer_properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(batch_rows))
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(writer_properties))?;
    let mut rows_per_file = 0_u64;
    let mut logical_bytes_per_file = 0_u64;
    let mut generator_peak_batch_bytes = 0_u64;

    while logical_bytes_per_file < minimum_logical_bytes_per_file {
        let remaining_rows = u64::MAX
            .checked_sub(rows_per_file)
            .ok_or_else(|| bench_error("constant-memory generator row ordinal overflow"))?;
        let rows = usize::try_from(remaining_rows.min(batch_rows as u64))
            .map_err(|_| bench_error("constant-memory generator row count is not portable"))?;
        let start = i64::try_from(rows_per_file)
            .map_err(|_| bench_error("constant-memory generator exceeded i64 row identity"))?;
        let end = start
            .checked_add(
                i64::try_from(rows)
                    .map_err(|_| bench_error("constant-memory generator batch is too large"))?,
            )
            .ok_or_else(|| bench_error("constant-memory generator row identity overflow"))?;
        let ids = Arc::new(Int64Array::from_iter_values(start..end));
        let payloads = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            payload.as_str(),
            rows,
        )));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![ids, payloads])?;
        let batch_bytes = u64::try_from(batch.get_array_memory_size())
            .map_err(|_| bench_error("constant-memory generator batch size overflow"))?;
        if batch_bytes > MAX_GENERATOR_BATCH_BYTES {
            return Err(bench_error(format!(
                "constant-memory generator batch retains {batch_bytes} bytes above its {MAX_GENERATOR_BATCH_BYTES}-byte setup ceiling; lower batch rows or payload bytes"
            )));
        }
        writer.write(&batch)?;
        rows_per_file = rows_per_file
            .checked_add(rows as u64)
            .ok_or_else(|| bench_error("constant-memory generator row count overflow"))?;
        logical_bytes_per_file = logical_bytes_per_file
            .checked_add(batch_bytes)
            .ok_or_else(|| bench_error("constant-memory generator logical byte overflow"))?;
        generator_peak_batch_bytes = generator_peak_batch_bytes.max(batch_bytes);
    }
    writer.close()?;
    File::open(&base_path)?.sync_all()?;

    for ordinal in 1..file_count {
        fs::hard_link(
            &base_path,
            output_root.join(format!("part-{ordinal:06}.parquet")),
        )?;
    }

    let unique_physical_bytes = fs::metadata(&base_path)?.len();
    let total_rows = rows_per_file
        .checked_mul(u64::from(file_count))
        .ok_or_else(|| bench_error("constant-memory generator total row count overflow"))?;
    let total_logical_bytes = logical_bytes_per_file
        .checked_mul(u64::from(file_count))
        .ok_or_else(|| bench_error("constant-memory generator total logical byte overflow"))?;
    let represented_physical_bytes = unique_physical_bytes
        .checked_mul(u64::from(file_count))
        .ok_or_else(|| bench_error("constant-memory generator represented byte overflow"))?;

    Ok(ConstantMemoryParquetRecipe {
        schema_version: 1,
        generator_version: "constant-memory-parquet-v1".to_owned(),
        file_count,
        rows_per_file,
        total_rows,
        logical_bytes_per_file,
        total_logical_bytes,
        unique_physical_bytes,
        represented_physical_bytes,
        generator_peak_batch_bytes,
        base_file_sha256: hash_file(&base_path)?,
        file_glob: "part-*.parquet".to_owned(),
    })
}

fn deterministic_payload(bytes: usize) -> String {
    const ALPHABET: &[u8] = b"cdf-constant-memory-0123456789-";
    let mut payload = Vec::with_capacity(bytes);
    while payload.len() < bytes {
        let remaining = bytes - payload.len();
        payload.extend_from_slice(&ALPHABET[..remaining.min(ALPHABET.len())]);
    }
    String::from_utf8(payload).expect("generator alphabet is valid UTF-8")
}

fn hash_file(path: &Path) -> BenchResult<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    #[test]
    fn stress_generator_is_bounded_deterministic_and_partitioned() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let first_recipe =
            generate_constant_memory_parquet(first.path(), 4, 1_048_576, 4_096, 192).unwrap();
        let second_recipe =
            generate_constant_memory_parquet(second.path(), 4, 1_048_576, 4_096, 192).unwrap();

        assert_eq!(first_recipe, second_recipe);
        assert_eq!(first_recipe.schema_version, 1);
        assert_eq!(first_recipe.file_count, 4);
        assert!(first_recipe.logical_bytes_per_file >= 1_048_576);
        assert_eq!(
            first_recipe.total_logical_bytes,
            first_recipe.logical_bytes_per_file * 4
        );
        assert!(first_recipe.generator_peak_batch_bytes <= MAX_GENERATOR_BATCH_BYTES);
        assert_eq!(fs::read_dir(first.path()).unwrap().count(), 4);

        let reader = ParquetRecordBatchReaderBuilder::try_new(
            File::open(first.path().join("part-000000.parquet")).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        let observed_rows = reader
            .map(|batch| batch.unwrap().num_rows() as u64)
            .sum::<u64>();
        assert_eq!(observed_rows, first_recipe.rows_per_file);

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let first_inode = fs::metadata(first.path().join("part-000000.parquet"))
                .unwrap()
                .ino();
            let last_inode = fs::metadata(first.path().join("part-000003.parquet"))
                .unwrap()
                .ino();
            assert_eq!(first_inode, last_inode);
        }
    }

    #[test]
    fn stress_generator_rejects_unbounded_setup_and_nonempty_output() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("existing"), b"occupied").unwrap();
        assert!(
            generate_constant_memory_parquet(root.path(), 1, 1024, 1024, 64)
                .unwrap_err()
                .to_string()
                .contains("output must be empty")
        );

        let root = tempfile::tempdir().unwrap();
        assert!(
            generate_constant_memory_parquet(root.path(), 1, 1024, 262_144, 4_096)
                .unwrap_err()
                .to_string()
                .contains("setup ceiling")
        );
    }
}
