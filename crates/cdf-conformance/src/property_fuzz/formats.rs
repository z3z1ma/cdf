use std::{collections::BTreeMap, panic, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use cdf_format_json::NdjsonFormatDriver;
use cdf_format_parquet::ParquetFormatDriver;
use cdf_kernel::{ErrorKind, PartitionId, ResourceId};
use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
use cdf_runtime::{BoundedFormatRequest, MemoryByteSource, ReadOptions, decode_bounded_format};
use parquet::file::reader::{FileReader, SerializedFileReader};
use proptest::prelude::*;

fn read_options() -> ReadOptions {
    ReadOptions::new(
        ResourceId::new("property_fuzz_ndjson").unwrap(),
        PartitionId::new("p0").unwrap(),
    )
    .with_batch_size(8)
    .unwrap()
}

fn assert_ndjson_data_error(bytes: &[u8]) {
    let error = read_ndjson_bytes(bytes).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Data);
}

fn read_ndjson_bytes(bytes: &[u8]) -> cdf_kernel::Result<cdf_runtime::BoundedFormatRead> {
    futures_executor::block_on(async {
        let memory = Arc::new(
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let source = Arc::new(
            MemoryByteSource::from_bytes("property-fuzz:ndjson", bytes.to_vec(), memory.clone())
                .await?,
        );
        decode_bounded_format(
            Arc::new(NdjsonFormatDriver::new()?),
            source,
            BoundedFormatRequest::new(read_options(), memory),
        )
        .await
    })
}

fn read_parquet_bytes(bytes: &[u8]) -> cdf_kernel::Result<cdf_runtime::BoundedFormatRead> {
    futures_executor::block_on(async {
        let memory = Arc::new(
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let source = Arc::new(
            MemoryByteSource::from_bytes("property-fuzz:parquet", bytes.to_vec(), memory.clone())
                .await?,
        );
        decode_bounded_format(
            Arc::new(ParquetFormatDriver::new()?),
            source,
            BoundedFormatRequest::new(
                ReadOptions::new(
                    ResourceId::new("property_fuzz_parquet")?,
                    PartitionId::new("p0")?,
                )
                .with_batch_size(8)?,
                memory,
            ),
        )
        .await
    })
}

#[test]
fn parquet_late_page_corruption_fails_without_publishing_partial_read() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(0..50_000))],
    )
    .unwrap();
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    let valid = read_parquet_bytes(&bytes).unwrap();
    assert_eq!(
        valid
            .batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        50_000
    );
    drop(valid);

    let reader = SerializedFileReader::new(Bytes::copy_from_slice(&bytes)).unwrap();
    let column = reader.metadata().row_group(0).column(0);
    let column_start = usize::try_from(
        column
            .dictionary_page_offset()
            .unwrap_or_else(|| column.data_page_offset()),
    )
    .unwrap();
    let column_length = usize::try_from(column.compressed_size()).unwrap();
    assert!(column_length > 128);
    let corrupt_start = column_start + column_length / 2;
    let mut corrupt = Vec::with_capacity(bytes.len() - 16);
    corrupt.extend_from_slice(&bytes[..corrupt_start]);
    corrupt.extend_from_slice(&bytes[corrupt_start + 16..]);

    let error = match read_parquet_bytes(&corrupt) {
        Err(error) => error,
        Ok(read) => panic!(
            "late-page corruption unexpectedly published {} rows",
            read.batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>()
        ),
    };
    assert_eq!(error.kind, ErrorKind::Data);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn property_fuzz_ndjson_parser_never_panics_on_adversarial_bytes(
        bytes in prop::collection::vec(any::<u8>(), 0..=1024)
    ) {
        let outcome = panic::catch_unwind(|| read_ndjson_bytes(&bytes));

        prop_assert!(outcome.is_ok());
        if std::str::from_utf8(&bytes).is_err() {
            prop_assert!(outcome.unwrap().is_err());
        }
    }

    #[test]
    fn property_fuzz_parquet_parser_never_panics_or_emits_partial_malformed_bytes(
        mut bytes in prop::collection::vec(any::<u8>(), 0..=2048)
    ) {
        bytes.extend_from_slice(b"NOPE");
        let outcome = panic::catch_unwind(|| read_parquet_bytes(&bytes));

        prop_assert!(outcome.is_ok());
        prop_assert!(outcome.unwrap().is_err());
    }
}

#[test]
fn property_fuzz_ndjson_malformed_and_mixed_inputs_error_without_partial_read() {
    for bytes in [
        b"{bad}\n".as_slice(),
        b"{\"id\":1}\nnot-json\n{\"id\":2}\n",
        b"{\"id\":1}\n{\"id\":\n",
        b"[1,2,3]\n",
        b"42\n",
        b"{\"id\":1}\n\xff\n",
    ] {
        assert_ndjson_data_error(bytes);
    }
}

#[test]
fn property_fuzz_ndjson_strange_scalar_values_are_all_or_error() {
    let large = "x".repeat(16 * 1024);
    let bytes = format!(
        "{{\"id\":1,\"payload\":\"{large}\",\"min\":{},\"flag\":true,\"escaped\":\"\\u0000\"}}\n",
        i64::MIN
    );

    match read_ndjson_bytes(bytes.as_bytes()) {
        Ok(read) => {
            let rows = read
                .batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>();
            assert_eq!(rows, 1);
        }
        Err(error) => assert_eq!(error.kind, ErrorKind::Data),
    }
}

#[test]
fn bounded_ndjson_decode_releases_input_and_output_leases() {
    futures_executor::block_on(async {
        let memory = Arc::new(
            DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let source = Arc::new(
            MemoryByteSource::from_bytes(
                "conformance:bounded-ndjson",
                b"{\"id\":1}\n{\"id\":2}\n".to_vec(),
                memory.clone(),
            )
            .await
            .unwrap(),
        );
        let read = decode_bounded_format(
            Arc::new(NdjsonFormatDriver::new().unwrap()),
            source.clone(),
            BoundedFormatRequest::new(read_options(), memory.clone()),
        )
        .await
        .unwrap();

        assert_eq!(
            read.batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            2
        );
        assert!(memory.snapshot().current_bytes > 0);
        drop(read);
        drop(source);
        assert_eq!(memory.snapshot().current_bytes, 0);
    });
}
