use std::fs;

use arrow_array::{Int64Array, StringArray};
use cdf_dest_parquet::ParquetDestination;
use cdf_dest_postgres::CDF_QUARANTINE_TABLE;
use cdf_kernel::{
    CapabilitySupport, CheckpointStatus, CheckpointStore, DestinationProtocol, SourcePosition,
    WriteDisposition,
};
use cdf_package::{PackageReader, PackageStatus, QuarantineObservedValue};
use cdf_project::ProjectRunReport;
use cdf_state_sqlite::{RunEventKind, RunEventValue, SqliteCheckpointStore};
use postgres::{Client, NoTls};
use serde_json::{self, Value};

use super::fixture::{ALLOWED_EVENT_TYPE, DRIFTED_EVENT_TYPE_OBSERVED, ScenarioSpec, TARGET};
use crate::{
    package_replay::DuckDbDestination,
    run_matrix::local_postgres::{LivePostgres, qualified_name},
};

pub(super) fn assert_drift_quarantine_package_evidence(report: &ProjectRunReport) {
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(report.row_count, 1);
    assert_eq!(report.segment_count, 1);
    assert_eq!(report.receipt.disposition, WriteDisposition::Merge);
    assert_eq!(report.receipt.counts.rows_written, 1);
    assert_eq!(
        report
            .receipt
            .segment_acks
            .iter()
            .map(|ack| ack.row_count)
            .sum::<u64>(),
        1
    );

    let reader = PackageReader::open(&report.package_dir).unwrap();
    reader.verify().unwrap();
    let files = reader
        .manifest()
        .identity
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    assert!(files.contains(&"plan/validation-program.json"));
    assert!(files.contains(&"stats/profile.json"));
    assert!(files.contains(&"stats/verdict-summary.json"));
    assert!(files.contains(&"stats/quarantine-summary.json"));
    assert!(files.contains(&"quarantine/part-000001.parquet"));
    assert!(files.contains(&cdf_package::DEDUP_SUMMARY_FILE));

    let validation_program: Value = serde_json::from_slice(
        &fs::read(report.package_dir.join("plan/validation-program.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        validation_program["promotion"]["clean_runs_required"],
        Value::from(1)
    );
    assert!(
        validation_program["schema_verdicts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|rule| rule["change"] == "type_narrowing" && rule["verdict"] == "quarantine")
    );

    let profile: Value =
        serde_json::from_slice(&fs::read(report.package_dir.join("stats/profile.json")).unwrap())
            .unwrap();
    assert_eq!(profile["output_rows"], Value::from(1));
    assert_eq!(profile["output_batches"], Value::from(1));

    let verdict_summary: Value = serde_json::from_slice(
        &fs::read(report.package_dir.join("stats/verdict-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(verdict_summary["input_rows"], Value::from(3));
    assert_eq!(verdict_summary["accepted_rows"], Value::from(2));
    assert_eq!(verdict_summary["quarantined_rows"], Value::from(1));
    assert_eq!(verdict_summary["violation_count"], Value::from(1));
    assert_eq!(
        verdict_summary["quarantine_candidate_count"],
        Value::from(1)
    );

    let quarantine_summary: Value = serde_json::from_slice(
        &fs::read(report.package_dir.join("stats/quarantine-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine_summary["quarantined_rows"], Value::from(1));
    assert_eq!(
        quarantine_summary["quarantine_candidate_count"],
        Value::from(1)
    );
    assert_eq!(quarantine_summary["artifact_count"], Value::from(1));
    assert_eq!(
        quarantine_summary["artifacts"],
        serde_json::json!(["quarantine/part-000001.parquet"])
    );

    let quarantine = reader.read_quarantine_records().unwrap();
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].source_row_ordinal, 2);
    assert_eq!(
        quarantine[0].rule_id,
        "source-decode:event_type:type-mismatch"
    );
    assert_eq!(quarantine[0].error_code, "source_type_mismatch");
    assert!(matches!(
        quarantine[0].source_position,
        Some(SourcePosition::FileManifest(_))
    ));
    assert_eq!(
        quarantine[0].observed_value_redacted,
        QuarantineObservedValue::Preserved {
            value: DRIFTED_EVENT_TYPE_OBSERVED.to_owned()
        }
    );

    let dedup = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(dedup["rule_id"], "row-rule-0001-dedup");
    assert_eq!(dedup["keys"], serde_json::json!(["id"]));
    assert_eq!(dedup["keep"], "last");
    assert_eq!(dedup["input_rows"], 2);
    assert_eq!(dedup["output_rows"], 1);
    assert_eq!(dedup["duplicate_key_count"], 1);
    assert_eq!(dedup["dropped_row_count"], 1);
    assert_eq!(dedup["dropped_rows"][0]["package_row_ordinal"], 0);
    assert_eq!(dedup["dropped_rows"][0]["kept_package_row_ordinal"], 1);

    let batches = reader.read_all_segments().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].0.row_count, 1);
    let batch = &batches[0].1[0];
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let names = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let event_types = batch
        .column_by_name("event_type")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.values(), &[1]);
    assert_eq!(event_types.value(0), ALLOWED_EVENT_TYPE);
    assert_eq!(names.value(0), "second-accepted-last");

    assert_quarantine_demotion_event(report);
}

pub(super) fn assert_accepted_rows_committed_through_gate(
    spec: &ScenarioSpec,
    report: &ProjectRunReport,
    duckdb: &DuckDbDestination,
) {
    assert!(
        DestinationProtocol::verify(duckdb, &report.receipt)
            .unwrap()
            .verified,
        "DuckDB receipt must verify after accepted drift rows commit"
    );
    let store = SqliteCheckpointStore::open(&spec.state_store_path).unwrap();
    let head = store
        .head(
            &report.checkpoint.delta.pipeline_id,
            &report.checkpoint.delta.resource_id,
            &report.checkpoint.delta.scope,
        )
        .unwrap()
        .expect("checkpoint head");
    assert_eq!(head.status, CheckpointStatus::Committed);
    assert_eq!(head.delta, report.checkpoint.delta);
    assert_eq!(head.receipt.as_ref(), Some(&report.receipt));
    let snapshot = duckdb.read_mirror_snapshot_read_only().unwrap();
    assert_eq!(snapshot.loads.len(), 2);
    assert_eq!(snapshot.state.len(), 2);
    assert!(snapshot.state.iter().any(|row| {
        row.package_hash == report.package_hash.as_str() && row.row_count == report.row_count
    }));
}

pub(super) fn assert_clean_run_promoted(report: &ProjectRunReport) {
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    let triggers = validation_transition_triggers(report);
    assert!(triggers.contains(&"new_resource".to_owned()));
    assert!(triggers.contains(&"clean_stable_runs".to_owned()));
}

pub(super) fn assert_unsupported_quarantine_mirror_artifact(
    report: &ProjectRunReport,
    destination_id: &str,
) {
    let mirror = read_quarantine_mirror_outcome(report);
    assert_eq!(mirror["destination_id"], destination_id);
    assert_eq!(mirror["quarantine_table_support"], "unsupported");
    assert_eq!(mirror["outcome"], "not_mirrored");
    assert_eq!(
        mirror["reason"],
        "destination sheet declares quarantine_tables unsupported"
    );
    assert_eq!(
        mirror["quarantine_artifacts"],
        serde_json::json!(["quarantine/part-000001.parquet"])
    );
}

pub(super) fn assert_supported_quarantine_mirror_artifact(
    report: &ProjectRunReport,
    destination_id: &str,
) {
    let mirror = read_quarantine_mirror_outcome(report);
    assert_eq!(mirror["destination_id"], destination_id);
    assert_eq!(mirror["quarantine_table_support"], "supported");
    assert_eq!(mirror["outcome"], "mirror_supported");
    assert_eq!(
        mirror["quarantine_artifacts"],
        serde_json::json!(["quarantine/part-000001.parquet"])
    );
}

pub(super) fn assert_parquet_quarantine_mirror_excluded_by_sheet() {
    let temp = tempfile::tempdir().unwrap();
    let destination = ParquetDestination::new_filesystem(temp.path()).unwrap();
    assert_eq!(
        destination.sheet().quarantine_tables,
        CapabilitySupport::Unsupported
    );
}

pub(super) fn assert_postgres_quarantine_mirror(
    postgres: &LivePostgres,
    report: &ProjectRunReport,
) {
    let mut client = Client::connect(postgres.url(), NoTls).unwrap();
    let row = client
        .query_one(
            &format!(
                "SELECT \"source_row_ordinal\", \"rule_id\", \"error_code\", \"observed_value_json\"::text FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2",
                qualified_name(postgres.schema(), CDF_QUARANTINE_TABLE)
            ),
            &[&report.receipt.target.as_str(), &report.package_hash.as_str()],
        )
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 2);
    assert_eq!(
        row.get::<_, String>(1),
        "source-decode:event_type:type-mismatch"
    );
    assert_eq!(row.get::<_, String>(2), "source_type_mismatch");
    let observed = row.get::<_, String>(3);
    assert!(observed.contains(DRIFTED_EVENT_TYPE_OBSERVED));
}

pub(super) fn assert_postgres_target_contains_deduped_accepted_row(postgres: &LivePostgres) {
    let mut client = Client::connect(postgres.url(), NoTls).unwrap();
    let row = client
        .query_one(
            &format!(
                "SELECT \"event_type\", \"name\" FROM {} WHERE \"id\" = 1",
                qualified_name(postgres.schema(), TARGET)
            ),
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, String>(0), ALLOWED_EVENT_TYPE);
    assert_eq!(
        row.get::<_, Option<String>>(1).as_deref(),
        Some("second-accepted-last")
    );
    let count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                qualified_name(postgres.schema(), TARGET)
            ),
            &[],
        )
        .map(|row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

fn assert_quarantine_demotion_event(report: &ProjectRunReport) {
    let events = report.ledger_snapshot.events.iter().collect::<Vec<_>>();
    let transition_index = events
        .iter()
        .position(|event| {
            event.kind == RunEventKind::ValidationDepthTransitionRecorded
                && event.details.attributes.get("trigger")
                    == Some(&RunEventValue::String("quarantine_event".to_owned()))
        })
        .expect("quarantine validation-depth transition event");
    let transition = events[transition_index];
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
    assert!(
        events
            .iter()
            .position(|event| event.kind == RunEventKind::PackageFinalized)
            .unwrap()
            < transition_index
    );
    assert!(
        transition_index
            < events
                .iter()
                .position(|event| event.kind == RunEventKind::CheckpointProposed)
                .unwrap()
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == RunEventKind::DestinationReceiptRecorded)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == RunEventKind::CheckpointCommitted)
    );
}

fn validation_transition_triggers(report: &ProjectRunReport) -> Vec<String> {
    report
        .ledger_snapshot
        .events
        .iter()
        .filter(|event| event.kind == RunEventKind::ValidationDepthTransitionRecorded)
        .filter_map(|event| match event.details.attributes.get("trigger") {
            Some(RunEventValue::String(trigger)) => Some(trigger.clone()),
            _ => None,
        })
        .collect()
}

fn read_quarantine_mirror_outcome(report: &ProjectRunReport) -> Value {
    serde_json::from_slice(
        &fs::read(
            report
                .package_dir
                .join("destination/quarantine-mirror.json"),
        )
        .unwrap(),
    )
    .unwrap()
}
