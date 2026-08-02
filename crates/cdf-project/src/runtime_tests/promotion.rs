use super::{
    AnomalyFact, CheckpointStatus, CheckpointStore, ContractPolicy, PackageStatus, Path,
    PipelineId, ProjectRunRequest, QueryableResource, ResourceStream, RowRule, RunEventKind,
    RunEventValue, SqliteCheckpointStore, fs,
    support::{
        SIMPLE_FILE_RESOURCE_APPEND, live_plan_for_queryable_with_exact_policy,
        live_plan_with_policy, parquet_project_run_request, project_run_request, run_project,
        simple_file_resource,
    },
};

pub(super) const SIMPLE_FILE_RESOURCE_APPEND_DRIFT: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
  { name = "note", type = "string", nullable = true },
] }
"#;
pub(super) fn project_run_request_with_policy<'a>(
    resource: &'a dyn QueryableResource,
    package_id: &str,
    package_root: &Path,
    duckdb_path: &Path,
    state_path: &Path,
    run_id: &str,
    policy: &ContractPolicy,
) -> ProjectRunRequest<'a> {
    let mut request = project_run_request(
        resource,
        package_id,
        package_root,
        duckdb_path,
        state_path,
        run_id,
    );
    request.plan = live_plan_with_policy(resource, package_id, policy);
    request
}

#[test]
fn trust_ring_clean_stable_runs_gate_sampled_fast_path_promotion() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.promotion.allow_sampled_fast_path = true;
    policy.promotion.clean_runs_required = 2;

    let first = project_run_request_with_policy(
        &resource,
        "pkg-trust-promotion-1",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-promotion-1",
        &policy,
    );
    let first_report = futures_executor::block_on(run_project(first)).unwrap();
    let first_transitions = first_report
        .ledger_snapshot
        .events
        .iter()
        .filter(|event| event.kind == RunEventKind::ValidationDepthTransitionRecorded)
        .collect::<Vec<_>>();
    assert_eq!(first_transitions.len(), 1);
    assert_eq!(
        first_transitions[0].details.attributes.get("trigger"),
        Some(&RunEventValue::String("new_resource".to_owned()))
    );

    fs::write(
        temp.path().join("data/events.ndjson"),
        "{\"id\":3,\"name\":\"katherine\"}\n\
         {\"id\":4,\"name\":\"dorothy\"}\n",
    )
    .unwrap();
    let second = project_run_request_with_policy(
        &resource,
        "pkg-trust-promotion-2",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-promotion-2",
        &policy,
    );
    let second_report = futures_executor::block_on(run_project(second)).unwrap();
    let transition = second_report
        .ledger_snapshot
        .events
        .iter()
        .find(|event| event.kind == RunEventKind::ValidationDepthTransitionRecorded)
        .expect("promotion transition event");

    assert_eq!(
        transition.package_hash,
        Some(second_report.package_hash.clone())
    );
    assert_eq!(
        transition.details.attributes.get("from_depth"),
        Some(&RunEventValue::String("full".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("to_depth"),
        Some(&RunEventValue::String("sampled_fast_path".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("trigger"),
        Some(&RunEventValue::String("clean_stable_runs".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("clean_run_count"),
        Some(&RunEventValue::U64(2))
    );
    assert_eq!(
        transition.details.attributes.get("clean_runs_required"),
        Some(&RunEventValue::U64(2))
    );
    assert_eq!(
        transition.details.attributes.get("schema_hash"),
        Some(&RunEventValue::String(
            second_report.receipt.schema_hash.as_str().to_owned()
        ))
    );
    assert_eq!(second_report.package_status, PackageStatus::Checkpointed);
    assert_eq!(second_report.checkpoint.status, CheckpointStatus::Committed);
}

#[test]
fn trust_ring_schema_drift_demotes_sampled_fast_path() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_root = temp.path().join(".cdf/packages");
    let parquet_root = temp.path().join(".cdf/lake");
    let state_path = temp.path().join(".cdf/state.db");
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.promotion.allow_sampled_fast_path = true;
    policy.promotion.clean_runs_required = 1;
    policy.promotion.demote_on_drift = true;

    let mut clean = parquet_project_run_request(
        &resource,
        "pkg-trust-drift-clean",
        &package_root,
        &parquet_root,
        &state_path,
        "run-trust-drift-clean",
    );
    policy.normalization.identifier = clean.plan.validation_program.identifier_policy.clone();
    clean.plan =
        live_plan_for_queryable_with_exact_policy(&resource, "pkg-trust-drift-clean", &policy);
    let clean_report = futures_executor::block_on(run_project(clean)).unwrap();
    assert!(
        clean_report.ledger_snapshot.events.iter().any(|event| event
            .details
            .attributes
            .get("trigger")
            == Some(&RunEventValue::String("clean_stable_runs".to_owned())))
    );

    let drift_resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND_DRIFT);
    fs::write(
        temp.path().join("data/events.ndjson"),
        "{\"id\":3,\"name\":\"katherine\",\"note\":\"schema drift\"}\n\
         {\"id\":4,\"name\":\"dorothy\",\"note\":\"schema drift\"}\n",
    )
    .unwrap();
    let mut drift = parquet_project_run_request(
        &drift_resource,
        "pkg-trust-drift-schema",
        &package_root,
        &parquet_root,
        &state_path,
        "run-trust-drift-schema",
    );
    drift.plan = live_plan_for_queryable_with_exact_policy(
        &drift_resource,
        "pkg-trust-drift-schema",
        &policy,
    );
    let report = futures_executor::block_on(run_project(drift)).unwrap();
    let transition = report
        .ledger_snapshot
        .events
        .iter()
        .find(|event| {
            event.kind == RunEventKind::ValidationDepthTransitionRecorded
                && event.details.attributes.get("trigger")
                    == Some(&RunEventValue::String("drift".to_owned()))
        })
        .expect("drift demotion transition event");

    assert_eq!(
        transition.details.attributes.get("from_depth"),
        Some(&RunEventValue::String("sampled_fast_path".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("to_depth"),
        Some(&RunEventValue::String("full".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("schema_hash"),
        Some(&RunEventValue::String(
            report.receipt.schema_hash.as_str().to_owned()
        ))
    );
    assert_eq!(
        transition.details.attributes.get("previous_schema_hash"),
        Some(&RunEventValue::String(
            clean_report.receipt.schema_hash.as_str().to_owned()
        ))
    );
    assert_eq!(
        transition.checkpoint_id,
        Some(report.checkpoint.delta.checkpoint_id.clone())
    );
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
}

#[test]
fn trust_ring_quarantine_demotes_sampled_fast_path_without_checkpoint_bypass() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.promotion.allow_sampled_fast_path = true;
    policy.promotion.clean_runs_required = 1;
    policy.promotion.demote_on_quarantine = true;
    policy.rows.rules = vec![RowRule::Domain {
        column: "name".to_owned(),
        allowed: vec!["ada".to_owned(), "grace".to_owned()],
    }];

    let clean = project_run_request_with_policy(
        &resource,
        "pkg-trust-demotion-clean",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-demotion-clean",
        &policy,
    );
    futures_executor::block_on(run_project(clean)).unwrap();

    fs::write(
        temp.path().join("data/events.ndjson"),
        "{\"id\":1,\"name\":\"ada\"}\n\
         {\"id\":2,\"name\":\"raw-secret\"}\n",
    )
    .unwrap();
    let quarantine = project_run_request_with_policy(
        &resource,
        "pkg-trust-demotion-quarantine",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-demotion-quarantine",
        &policy,
    );
    let report = futures_executor::block_on(run_project(quarantine)).unwrap();
    let transition_index = report
        .ledger_snapshot
        .events
        .iter()
        .position(|event| event.kind == RunEventKind::ValidationDepthTransitionRecorded)
        .expect("demotion transition event");
    let transition = &report.ledger_snapshot.events[transition_index];

    assert_eq!(transition.package_hash, Some(report.package_hash.clone()));
    assert_eq!(
        transition.details.attributes.get("from_depth"),
        Some(&RunEventValue::String("sampled_fast_path".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("to_depth"),
        Some(&RunEventValue::String("full".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("trigger"),
        Some(&RunEventValue::String("quarantine_event".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("schema_hash"),
        Some(&RunEventValue::String(
            report.receipt.schema_hash.as_str().to_owned()
        ))
    );
    let transition_json = serde_json::to_string(transition).unwrap();
    assert!(!transition_json.contains("raw-secret"));
    assert!(!transition_json.contains("secret://"));

    let kinds = report
        .ledger_snapshot
        .events
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(
        kinds
            .iter()
            .position(|kind| *kind == RunEventKind::PackageFinalized)
            .unwrap()
            < transition_index
    );
    assert!(
        transition_index
            < kinds
                .iter()
                .position(|kind| *kind == RunEventKind::CheckpointProposed)
                .unwrap()
    );
    assert!(kinds.contains(&RunEventKind::CheckpointCommitted));
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let head = store
        .head(
            &PipelineId::new("pipeline-live").unwrap(),
            &resource.descriptor().resource_id,
            &resource.descriptor().state_scope,
        )
        .unwrap()
        .expect("checkpoint head");
    assert_eq!(
        head.delta.checkpoint_id,
        report.checkpoint.delta.checkpoint_id
    );
}

#[test]
fn trust_ring_explicit_anomaly_fact_demotes_sampled_fast_path() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.promotion.allow_sampled_fast_path = true;
    policy.promotion.clean_runs_required = 1;
    policy.promotion.demote_on_anomaly = true;

    let clean = project_run_request_with_policy(
        &resource,
        "pkg-trust-anomaly-clean",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-anomaly-clean",
        &policy,
    );
    futures_executor::block_on(run_project(clean)).unwrap();

    fs::write(
        temp.path().join("data/events.ndjson"),
        "{\"id\":3,\"name\":\"katherine\"}\n\
         {\"id\":4,\"name\":\"dorothy\"}\n",
    )
    .unwrap();
    let mut anomaly = project_run_request_with_policy(
        &resource,
        "pkg-trust-anomaly-spike",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-anomaly-spike",
        &policy,
    );
    anomaly
        .plan
        .validation_program
        .explicit_anomalies
        .push(AnomalyFact {
            metric: "profile.value_distribution_zscore".to_owned(),
            observed: "12.4".to_owned(),
            threshold: "3.0".to_owned(),
            window: "last_5_committed_packages".to_owned(),
        });
    let anomaly_program = anomaly.plan.validation_program.clone();
    anomaly
        .plan
        .rebind_validation_program(anomaly_program, resource.schema().as_ref())
        .unwrap();
    let report = futures_executor::block_on(run_project(anomaly)).unwrap();
    let transition = report
        .ledger_snapshot
        .events
        .iter()
        .find(|event| {
            event.kind == RunEventKind::ValidationDepthTransitionRecorded
                && event.details.attributes.get("trigger")
                    == Some(&RunEventValue::String("anomaly_spike".to_owned()))
        })
        .expect("anomaly demotion transition event");

    assert_eq!(transition.package_hash, Some(report.package_hash.clone()));
    assert_eq!(
        transition.details.attributes.get("from_depth"),
        Some(&RunEventValue::String("sampled_fast_path".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("to_depth"),
        Some(&RunEventValue::String("full".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("schema_hash"),
        Some(&RunEventValue::String(
            report.receipt.schema_hash.as_str().to_owned()
        ))
    );
    assert_eq!(
        transition.details.attributes.get("metric"),
        Some(&RunEventValue::String(
            "profile.value_distribution_zscore".to_owned()
        ))
    );
    assert_eq!(
        transition.details.attributes.get("observed"),
        Some(&RunEventValue::String("12.4".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("threshold"),
        Some(&RunEventValue::String("3.0".to_owned()))
    );
    assert_eq!(
        transition.details.attributes.get("window"),
        Some(&RunEventValue::String(
            "last_5_committed_packages".to_owned()
        ))
    );
    assert_eq!(
        transition.checkpoint_id,
        Some(report.checkpoint.delta.checkpoint_id.clone())
    );
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
}

#[test]
fn trust_ring_anomaly_demotion_requires_explicit_fact() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.promotion.allow_sampled_fast_path = true;
    policy.promotion.clean_runs_required = 1;
    policy.promotion.demote_on_anomaly = true;

    let clean = project_run_request_with_policy(
        &resource,
        "pkg-trust-no-anomaly-clean",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-no-anomaly-clean",
        &policy,
    );
    futures_executor::block_on(run_project(clean)).unwrap();

    // The second run must contain new work. An unchanged FileManifest is a
    // verified no-op and therefore cannot exercise the promotion decision.
    fs::write(
        temp.path().join("data/events.ndjson"),
        "{\"id\":1,\"name\":\"ada\"}\n\
         {\"id\":2,\"name\":\"grace\"}\n\
         {\"id\":3,\"name\":\"linus\"}\n",
    )
    .unwrap();

    let no_anomaly = project_run_request_with_policy(
        &resource,
        "pkg-trust-no-anomaly-current",
        &package_root,
        &duckdb_path,
        &state_path,
        "run-trust-no-anomaly-current",
        &policy,
    );
    let report = futures_executor::block_on(run_project(no_anomaly)).unwrap();

    assert!(!report.ledger_snapshot.events.iter().any(|event| {
        event.kind == RunEventKind::ValidationDepthTransitionRecorded
            && event.details.attributes.get("trigger")
                == Some(&RunEventValue::String("anomaly_spike".to_owned()))
    }));
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
}
