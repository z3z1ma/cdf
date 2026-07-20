use std::{collections::BTreeMap, fs, sync::Arc};

use arrow_array::{Int64Array, StringArray};
use cdf_dest_parquet::ParquetDestination;
use cdf_dest_postgres::CDF_QUARANTINE_TABLE;
use cdf_kernel::{
    CapabilitySupport, CheckpointStatus, CheckpointStore, DestinationProtocol, SourcePosition,
    WriteDisposition,
};
use cdf_package::{PackageReader, STATISTICS_PROFILE_FILE, StatisticsProfileGrain};
use cdf_package_contract::{DEDUP_SUMMARY_FILE, PackageStatus, QuarantineObservedValue};
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
    let verified = reader.verify_for_consumption().unwrap();
    let mut files = std::collections::BTreeSet::new();
    reader
        .for_each_identity_file(&mut |file| {
            files.insert(file.path);
            Ok(())
        })
        .unwrap();
    assert!(files.contains("plan/validation-program.json"));
    assert!(files.contains(STATISTICS_PROFILE_FILE));
    assert!(files.contains("stats/verdict-summary.json"));
    assert!(files.contains("stats/quarantine-summary.json"));
    assert!(files.contains("quarantine/part-000001.parquet"));
    assert!(files.contains(DEDUP_SUMMARY_FILE));

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

    let mut profile = Vec::new();
    reader
        .for_each_verified_statistics_profile(&verified, &mut |row| {
            profile.push(row);
            Ok(())
        })
        .unwrap();
    assert!(profile.iter().any(|row| {
        row.grain == StatisticsProfileGrain::Package
            && row.row_count == 1
            && row.field_path[0].as_ref() == "id"
    }));

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

    let mut quarantine = Vec::new();
    reader
        .for_each_quarantine_record(&mut |record| {
            quarantine.push(record);
            Ok(())
        })
        .unwrap();
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].source_row_ordinal, 2);
    assert_eq!(
        quarantine[0].rule_id,
        "residual:event_type:control-critical"
    );
    assert_eq!(quarantine[0].error_code, "cdf.residual_control_critical");
    assert!(matches!(
        quarantine[0].source_position,
        Some(SourcePosition::FileManifest(_))
    ));
    let QuarantineObservedValue::Preserved { value } = &quarantine[0].observed_value_redacted
    else {
        panic!("non-PII drift evidence should be preserved in its typed residual envelope");
    };
    assert!(value.contains(DRIFTED_EVENT_TYPE_OBSERVED));

    let dedup = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(dedup["rule_id"], "row-rule-0001-dedup");
    assert_eq!(dedup["keys"], serde_json::json!(["id"]));
    assert_eq!(dedup["keep"], "last");
    assert_eq!(dedup["input_rows"], 2);
    assert_eq!(dedup["output_rows"], 1);
    assert_eq!(dedup["duplicate_key_count"], 1);
    assert_eq!(dedup["dropped_row_count"], 1);
    let mut dedup_provenance = Vec::new();
    reader
        .for_each_dedup_dropped_provenance(&mut |dropped, kept| {
            dedup_provenance.push((dropped, kept));
            Ok(())
        })
        .unwrap();
    assert_eq!(dedup_provenance, vec![(0, 1)]);

    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new()).unwrap(),
    );
    let mut segments = reader
        .verified_segment_stream_with(&verified, memory, 64 * 1024 * 1024)
        .unwrap();
    let segment = segments.next().unwrap().unwrap();
    assert_eq!(segment.entry.row_count, 1);
    let batch = &segment.batches[0];
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
    assert!(segments.next().is_none());
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
    assert_eq!(mirror["version"], 1);
    assert_eq!(mirror["quarantine_directory"], "quarantine/");
    assert_eq!(mirror["quarantine_part_count"], 1);
    assert_eq!(mirror["schema_observations_present"], false);
    assert!(mirror.get("quarantine_artifacts").is_none());
}

pub(super) fn assert_supported_quarantine_mirror_artifact(
    report: &ProjectRunReport,
    destination_id: &str,
) {
    let mirror = read_quarantine_mirror_outcome(report);
    assert_eq!(mirror["destination_id"], destination_id);
    assert_eq!(mirror["quarantine_table_support"], "supported");
    assert_eq!(mirror["outcome"], "mirror_supported");
    assert_eq!(mirror["version"], 1);
    assert_eq!(mirror["quarantine_directory"], "quarantine/");
    assert_eq!(mirror["quarantine_part_count"], 1);
    assert_eq!(mirror["schema_observations_present"], false);
    assert!(mirror.get("quarantine_artifacts").is_none());
}

pub(super) fn assert_parquet_quarantine_mirror_excluded_by_sheet() {
    let temp = tempfile::tempdir().unwrap();
    let destination =
        ParquetDestination::new_filesystem(temp.path(), crate::test_execution_services()).unwrap();
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
        "residual:event_type:control-critical"
    );
    assert_eq!(row.get::<_, String>(2), "cdf.residual_control_critical");
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
