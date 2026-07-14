use std::panic;

use cdf_formats::{JsonOptions, read_ndjson_bytes};
use cdf_kernel::{ErrorKind, PartitionId, ResourceId};
use cdf_runtime::ReadOptions;
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
    let error = read_ndjson_bytes(bytes, &read_options(), &JsonOptions::default()).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Data);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn property_fuzz_ndjson_parser_never_panics_on_adversarial_bytes(
        bytes in prop::collection::vec(any::<u8>(), 0..=1024)
    ) {
        let outcome = panic::catch_unwind(|| {
            read_ndjson_bytes(&bytes, &read_options(), &JsonOptions::default())
        });

        prop_assert!(outcome.is_ok());
        if std::str::from_utf8(&bytes).is_err() {
            prop_assert!(outcome.unwrap().is_err());
        }
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

    match read_ndjson_bytes(bytes.as_bytes(), &read_options(), &JsonOptions::default()) {
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
