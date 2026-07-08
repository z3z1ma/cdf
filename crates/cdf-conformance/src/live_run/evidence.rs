use std::collections::BTreeMap;

use super::{
    LiveLocalFileFixture, LiveLocalFileFixtureSpec, LiveRunGoldenEvidence, destinations,
    live_run_expected_from_fixture, read_verified_golden_package_evidence,
    run_live_local_file_fixture_with_destination,
};

pub(super) fn run_live_fixture(
    spec: LiveLocalFileFixtureSpec,
    handle: &destinations::LiveRunDestinationHandle,
) -> LiveLocalFileFixture {
    let destination = handle.resolved(&spec).unwrap();
    let report = futures_executor::block_on(run_live_local_file_fixture_with_destination(
        spec.clone(),
        destination,
        None,
    ))
    .unwrap();
    handle.verify_receipt(&report.receipt).unwrap();
    let package_evidence = read_verified_golden_package_evidence(&report.package_dir).unwrap();
    LiveLocalFileFixture {
        spec,
        report,
        package_evidence,
    }
}

pub(super) fn destination_row_counts(
    fixture: &LiveLocalFileFixture,
    handle: &destinations::LiveRunDestinationHandle,
) -> BTreeMap<String, u64> {
    handle
        .destination_row_counts(&fixture.report.receipt)
        .unwrap()
}

pub(super) fn duckdb_row_counts(fixture: &LiveLocalFileFixture) -> BTreeMap<String, u64> {
    destinations::LiveRunDestinationHandle::duckdb(&fixture.spec)
        .destination_row_counts(&fixture.report.receipt)
        .unwrap()
}

pub(super) fn maybe_print_expected(
    label: &str,
    fixture: &LiveLocalFileFixture,
    row_counts: BTreeMap<String, u64>,
) {
    if std::env::var_os("CDF_PRINT_LIVE_RUN_GOLDEN").is_some() {
        let expected: LiveRunGoldenEvidence = live_run_expected_from_fixture(fixture, row_counts);
        println!(
            "CDF_LIVE_RUN_GOLDEN_{label}={}",
            serde_json::to_string_pretty(&expected).unwrap()
        );
    }
}
