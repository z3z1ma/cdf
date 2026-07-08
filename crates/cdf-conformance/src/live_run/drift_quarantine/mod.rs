mod assertions;
mod fixture;

use cdf_dest_postgres::{MergeDedupPolicy, PostgresTarget};
use cdf_kernel::TargetName;
use cdf_project::ResolvedProjectDestination;

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

#[test]
fn drift_quarantine_duckdb_conformance_asserts_unsupported_mirror_exclusion() {
    let temp = tempfile::tempdir().unwrap();
    let spec = ScenarioSpec::new(temp.path(), "duckdb").unwrap();
    let duckdb = DuckDbDestination::new(&spec.destination_path).unwrap();

    let clean = run_scenario(
        &spec,
        CLEAN_SOURCE,
        "clean",
        ResolvedProjectDestination::duckdb(&spec.destination_path, spec.target.clone()).unwrap(),
    )
    .unwrap();
    assert_clean_run_promoted(&clean);

    let drift = run_scenario(
        &spec,
        DRIFT_SOURCE,
        "drift",
        ResolvedProjectDestination::duckdb(&spec.destination_path, spec.target.clone()).unwrap(),
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
        ResolvedProjectDestination::postgres(
            postgres.url().to_owned(),
            target.clone(),
            MergeDedupPolicy::Last,
            None,
        )
        .unwrap(),
    )
    .unwrap();
    assert_clean_run_promoted(&clean);

    let drift = run_scenario(
        &spec.with_target(target_name),
        DRIFT_SOURCE,
        "drift",
        ResolvedProjectDestination::postgres(
            postgres.url().to_owned(),
            target,
            MergeDedupPolicy::Last,
            None,
        )
        .unwrap(),
    )
    .unwrap();

    assert_drift_quarantine_package_evidence(&drift);
    assert_supported_quarantine_mirror_artifact(&drift, "postgres");
    assert_postgres_quarantine_mirror(&postgres, &drift);
    assert_postgres_target_contains_deduped_accepted_row(&postgres);
}
