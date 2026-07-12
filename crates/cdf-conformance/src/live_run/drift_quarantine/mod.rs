mod assertions;
mod fixture;

use std::path::Path;

use cdf_dest_postgres::PostgresTarget;
use cdf_kernel::{DestinationProtocol, TargetName};
use cdf_package::PackageReader;
use serde::Serialize;

use self::{
    assertions::{
        assert_accepted_rows_committed_through_gate, assert_clean_run_promoted,
        assert_drift_quarantine_package_evidence,
        assert_parquet_quarantine_mirror_excluded_by_sheet, assert_postgres_quarantine_mirror,
        assert_postgres_target_contains_deduped_accepted_row,
        assert_supported_quarantine_mirror_artifact, assert_unsupported_quarantine_mirror_artifact,
    },
    fixture::{CLEAN_SOURCE, DRIFT_SOURCE, ScenarioSpec, TARGET, run_scenario},
};
use crate::{package_replay::DuckDbDestination, run_matrix::local_postgres::LivePostgres};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct DuckDbDriftQuarantineDemoEvidence {
    pub clean_package_id: String,
    pub drift_package_id: String,
    pub accepted_rows: u64,
    pub quarantined_rows: u64,
    pub receipt_verified: bool,
    pub checkpoint_gated_after_receipt_verification: bool,
    pub destination_load_rows: usize,
    pub destination_state_rows: usize,
    pub quarantine_mirror_outcome: String,
}

pub(crate) fn run_duckdb_demo(root: &Path) -> DuckDbDriftQuarantineDemoEvidence {
    let spec = ScenarioSpec::new(root, "mvp-acceptance-demo").unwrap();
    let duckdb = DuckDbDestination::new(&spec.destination_path).unwrap();

    let clean = run_scenario(
        &spec,
        CLEAN_SOURCE,
        "mvp-clean",
        crate::destination_catalog::resolve(
            &crate::destination_catalog::local_uri("duckdb", &spec.destination_path),
            &spec.project_root,
            spec.target.clone(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_clean_run_promoted(&clean);

    let drift = run_scenario(
        &spec,
        DRIFT_SOURCE,
        "mvp-drift",
        crate::destination_catalog::resolve(
            &crate::destination_catalog::local_uri("duckdb", &spec.destination_path),
            &spec.project_root,
            spec.target.clone(),
        )
        .unwrap(),
    )
    .unwrap();

    assert_drift_quarantine_package_evidence(&drift);
    assert_accepted_rows_committed_through_gate(&spec, &drift, &duckdb);
    assert_unsupported_quarantine_mirror_artifact(&drift, "duckdb");
    assert_parquet_quarantine_mirror_excluded_by_sheet();

    let quarantined_rows = PackageReader::open(&drift.package_dir)
        .unwrap()
        .read_quarantine_records()
        .unwrap()
        .len();
    let snapshot = duckdb.read_mirror_snapshot_read_only().unwrap();
    DuckDbDriftQuarantineDemoEvidence {
        clean_package_id: clean.package_id,
        drift_package_id: drift.package_id,
        accepted_rows: drift.row_count,
        quarantined_rows: u64::try_from(quarantined_rows).unwrap(),
        receipt_verified: DestinationProtocol::verify(&duckdb, &drift.receipt)
            .unwrap()
            .verified,
        checkpoint_gated_after_receipt_verification: true,
        destination_load_rows: snapshot.loads.len(),
        destination_state_rows: snapshot.state.len(),
        quarantine_mirror_outcome: "not_mirrored".to_owned(),
    }
}

#[test]
fn drift_quarantine_duckdb_conformance_asserts_unsupported_mirror_exclusion() {
    let temp = tempfile::tempdir().unwrap();
    let spec = ScenarioSpec::new(temp.path(), "duckdb").unwrap();
    let duckdb = DuckDbDestination::new(&spec.destination_path).unwrap();

    let clean = run_scenario(
        &spec,
        CLEAN_SOURCE,
        "clean",
        crate::destination_catalog::resolve(
            &crate::destination_catalog::local_uri("duckdb", &spec.destination_path),
            &spec.project_root,
            spec.target.clone(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_clean_run_promoted(&clean);

    let drift = run_scenario(
        &spec,
        DRIFT_SOURCE,
        "drift",
        crate::destination_catalog::resolve(
            &crate::destination_catalog::local_uri("duckdb", &spec.destination_path),
            &spec.project_root,
            spec.target.clone(),
        )
        .unwrap(),
    )
    .unwrap();

    assert_drift_quarantine_package_evidence(&drift);
    assert_accepted_rows_committed_through_gate(&spec, &drift, &duckdb);
    assert_unsupported_quarantine_mirror_artifact(&drift, "duckdb");
    assert_parquet_quarantine_mirror_excluded_by_sheet();
}

#[test]
fn drift_quarantine_postgres_conformance_asserts_supported_mirror() {
    let postgres = LivePostgres::start().expect(
        "E6 drift-quarantine Postgres conformance requires Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    let temp = tempfile::tempdir().unwrap();
    let spec = ScenarioSpec::new(temp.path(), "postgres").unwrap();
    let target = PostgresTarget::new(Some(postgres.schema()), TARGET).unwrap();
    let target_name = TargetName::new(target.display_name()).unwrap();

    let clean = run_scenario(
        &spec.with_target(target_name.clone()),
        CLEAN_SOURCE,
        "clean",
        crate::destination_catalog::resolve(
            postgres.url(),
            &spec.project_root,
            target_name.clone(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_clean_run_promoted(&clean);

    let drift = run_scenario(
        &spec.with_target(target_name.clone()),
        DRIFT_SOURCE,
        "drift",
        crate::destination_catalog::resolve(postgres.url(), &spec.project_root, target_name)
            .unwrap(),
    )
    .unwrap();

    assert_drift_quarantine_package_evidence(&drift);
    assert_supported_quarantine_mirror_artifact(&drift, "postgres");
    assert_postgres_quarantine_mirror(&postgres, &drift);
    assert_postgres_target_contains_deduped_accepted_row(&postgres);
}
