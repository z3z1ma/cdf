use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampMillisecondArray,
    builder::{Int32Builder, MapBuilder, StringBuilder, StringDictionaryBuilder},
    types::Int32Type,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_contract::{
    ContractPolicy, DedupKeep, FieldCoercionDecision, NestedDataPolicy, ObservedSchema,
    RESIDUAL_ENCODING_METADATA_KEY, RESIDUAL_ENCODING_NAME, RowRule, SchemaEvolutionMode,
    VARIANT_COLUMN_NAME, VARIANT_SEMANTIC_TAG, VerdictAction, compile_resource_validation_program,
    compile_validation_program, reconcile_schema,
};
use cdf_kernel::{
    BackpressureSupport, Batch, BatchHeader, BatchId, BatchStream, CapabilitySupport, ContractRef,
    DeduplicationSpec, DeliveryGuarantee, DiscoveryExecutorBudgetEvidence, DiscoveryManifestHash,
    DiscoveryManifestReference, EffectiveSchemaCatalogEntry, EffectiveSchemaEvidence,
    EffectiveSchemaObservationEvidence, EffectiveSchemaRuntime, EstimateSupport, FileManifest,
    FilePosition, FilterCapabilities, FreshnessSpec, IncrementalShape,
    PLAN_SCHEMA_OBSERVATION_BINDING_KEY, PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionAttestation,
    PartitionId, PartitionPlan, PartitioningCapabilities, PreContractObservedValue,
    PreContractQuarantineFact, PreContractResidualCandidate, PredicateId, PushdownFidelity,
    QueryableResource, ResourceCapabilities, ResourceDescriptor, ResourceId, ResourceStream,
    Result, RunId, RunPhase, RunPhaseStatus, STRATIFIED_HASH_SELECTOR_V1, ScanPlan, ScanPredicate,
    ScanRequest, SchemaHash, SchemaObservationFieldQuarantine, SchemaObservationPolicy,
    SchemaSnapshotReference, SchemaSource, ScopeKey, SourcePosition,
    TerminalSchemaObservationQuarantine, TrustLevel, WriteDisposition, source_name, with_semantic,
};
use cdf_package::PackageStatus;
use datafusion::{
    catalog::TableProvider, physical_plan::common::collect as collect_stream, prelude::*,
};
use futures_executor::block_on;
use futures_util::stream;
use tempfile::TempDir;
use tracing::{
    Event, Id, Metadata, Subscriber,
    field::{Field as TracingField, Visit},
    span::{Attributes, Record},
};

use super::*;

#[test]
fn tier_a_resource_runs_engine_projection_filter_limit_into_package() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true"],
        Some(vec!["name".to_owned()]),
        Some(1),
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();

    assert_eq!(plan.explain.pushed_predicates, Vec::new());
    assert_eq!(plan.explain.unsupported_predicates.len(), 2);

    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.manifest.lifecycle.status, PackageStatus::Packaged);
    assert_eq!(output.profile.output_rows, 1);
    assert!(output.profile.output_bytes > 0);
    assert_eq!(output.segments.len(), 1);

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema().field(0).name(), "name");
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "two");
}

#[test]
fn residual_limit_is_consumed_across_partitions() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["active = true"],
        Some(vec!["name".to_owned()]),
        Some(1),
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    assert_eq!(output.profile.output_batches, 1);
    assert_eq!(output.segments.len(), 1);
}

#[test]
fn preview_traverses_every_planned_partition_through_the_engine_front_end() {
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, PlanBoundedness::Bounded),
        )
        .unwrap();
    let limits = EnginePreviewLimits::default();

    let preview = block_on(preview_resource(&plan, &resource, limits.clone())).unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(preview.planned_partition_count, 2);
    assert_eq!(preview.payload_opened_partition_count, 2);
    assert_eq!(preview.attested_partition_count, 0);
    assert_eq!(preview.inspected_partition_count, 2);
    assert_eq!(preview.inspected_batch_count, 2);
    assert_eq!(preview.selected_partition_count, 2);
    assert_eq!(
        preview.selection.policy,
        PREVIEW_POLICY_BALANCED_STRATIFIED_V1
    );
    assert_eq!(preview.selection.selector, STRATIFIED_HASH_SELECTOR_V1);
    assert_eq!(preview.selection.selected.len(), 2);
    assert_eq!(preview.selection.selected[0].batch_quota, 32);
    assert_eq!(preview.selection.selected[1].batch_quota, 32);
    assert_eq!(preview.row_count, 6);
    assert_eq!(preview.fields, vec!["id", "name", "active", "_cdf_variant"]);
    assert_eq!(preview.limits, limits);
    assert!(!preview.truncated);
}

#[test]
fn preview_applies_explicit_row_limit_globally_without_opening_later_payloads() {
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, Some(2), PlanBoundedness::Bounded),
        )
        .unwrap();
    let limits = EnginePreviewLimits::default().with_max_rows(2).unwrap();

    let preview = block_on(preview_resource(&plan, &resource, limits)).unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 1);
    assert_eq!(preview.payload_opened_partition_count, 1);
    assert_eq!(preview.attested_partition_count, 0);
    assert_eq!(preview.inspected_partition_count, 1);
    assert_eq!(preview.inspected_batch_count, 1);
    assert_eq!(preview.row_count, 2);
    assert_eq!(
        preview
            .selection
            .selected_but_uninspected_partition_ids
            .len(),
        1
    );
    assert_eq!(preview.payload_uninspected_partition_count, 1);
    assert!(preview.truncated);
}

#[test]
fn preview_configured_byte_limit_accounts_decoded_input_separately_from_output() {
    let baseline_resource = MockResource::tier_b(sample_batches());
    let baseline_plan = Planner::new()
        .plan_tier_b(
            &baseline_resource,
            plan_input(Vec::new(), None, Some(1), PlanBoundedness::Bounded),
        )
        .unwrap();
    let one_row = block_on(preview_resource(
        &baseline_plan,
        &baseline_resource,
        EnginePreviewLimits::default().with_max_rows(1).unwrap(),
    ))
    .unwrap();

    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, PlanBoundedness::Bounded),
        )
        .unwrap();
    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::new(500, one_row.byte_count, 64).unwrap(),
    ))
    .unwrap();

    assert_eq!(preview.byte_count, one_row.byte_count);
    assert!(preview.output_byte_count > 0);
    assert_eq!(preview.inspected_batch_count, 1);
    assert_eq!(preview.payload_opened_partition_count, 1);
    assert_eq!(preview.payload_uninspected_partition_count, 1);
    assert!(preview.truncated);
}

#[test]
fn preview_rejects_an_oversized_batch_atomically() {
    let baseline_resource = MockResource::tier_b(sample_batches());
    let baseline_plan = Planner::new()
        .plan_tier_b(
            &baseline_resource,
            plan_input(Vec::new(), None, None, PlanBoundedness::Bounded),
        )
        .unwrap();
    let baseline = block_on(preview_resource(
        &baseline_plan,
        &baseline_resource,
        EnginePreviewLimits::default().with_max_rows(1).unwrap(),
    ))
    .unwrap();
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, PlanBoundedness::Bounded),
        )
        .unwrap();

    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::new(500, baseline.byte_count - 1, 64).unwrap(),
    ))
    .unwrap();

    assert_eq!(preview.payload_opened_partition_count, 2);
    assert_eq!(preview.inspected_partition_count, 0);
    assert_eq!(preview.inspected_batch_count, 0);
    assert_eq!(preview.row_count, 0);
    assert_eq!(preview.byte_count, 0);
    assert_eq!(preview.output_byte_count, 0);
    assert_eq!(preview.payload_uninspected_partition_count, 2);
    assert!(preview.truncated);
}

#[test]
fn preview_fair_batch_quotas_are_fixed_before_payload_io() {
    let resource = MockResource::tier_b(sample_batches()).with_partition_count(3);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, PlanBoundedness::Bounded),
        )
        .unwrap();

    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::new(500, DEFAULT_PREVIEW_MAX_BYTES, 8).unwrap(),
    ))
    .unwrap();

    assert_eq!(preview.selected_partition_count, 3);
    assert_eq!(
        preview
            .selection
            .selected
            .iter()
            .map(|partition| partition.batch_quota)
            .collect::<Vec<_>>(),
        vec![3, 3, 2]
    );
}

#[test]
fn preview_large_plan_selects_and_opens_at_most_the_global_batch_budget() {
    let resource = MockResource::tier_b(Vec::new()).with_partition_count(10_000);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, PlanBoundedness::Bounded),
        )
        .unwrap();

    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::default(),
    ))
    .unwrap();

    assert_eq!(preview.planned_partition_count, 10_000);
    assert_eq!(preview.payload_eligible_partition_count, 10_000);
    assert_eq!(preview.selected_partition_count, 64);
    assert_eq!(preview.payload_opened_partition_count, 64);
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 64);
    assert_eq!(preview.selection.selected.len(), 64);
    assert!(
        preview
            .selection
            .selected
            .iter()
            .all(|partition| partition.batch_quota == 1)
    );
    assert_eq!(preview.inspected_partition_count, 64);
    assert_eq!(preview.payload_uninspected_partition_count, 9_936);
    assert!(preview.truncated);
}

#[test]
fn preview_terminal_quarantine_uses_run_attestation_without_opening_payloads() {
    let effective_schema = sample_schema();
    let physical_schema = sample_schema();
    let physical_hash =
        cdf_contract::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash.clone());
    let resource = MockResource::tier_b(sample_batches())
        .with_effective_schema_runtime(effective_schema, runtime)
        .with_attestation(PartitionAttestation::new(
            terminal_file_position(),
            Some(physical_hash),
        ));
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, PlanBoundedness::Bounded),
        )
        .unwrap();

    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::default(),
    ))
    .unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    assert_eq!(preview.planned_partition_count, 2);
    assert_eq!(preview.payload_opened_partition_count, 0);
    assert_eq!(preview.attested_partition_count, 2);
    assert_eq!(preview.terminal_quarantine_count, 1);
    assert_eq!(preview.row_count, 0);
}

#[test]
fn execution_returns_segment_source_position_evidence() {
    let resource = MockResource::tier_a(vec![batch_with_file_position()]);
    let input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert_eq!(output.output.segments.len(), 1);
    assert_eq!(output.segment_positions.len(), 1);
    assert_eq!(
        output.segment_positions[0].segment_id,
        output.output.segments[0].segment_id
    );
    let Some(SourcePosition::FileManifest(manifest)) = &output.segment_positions[0].output_position
    else {
        panic!("expected file manifest position evidence");
    };
    assert_eq!(manifest.files[0].path, "/tmp/cdf/events.ndjson");
}

#[test]
fn tier_b_negotiates_pushdown_fidelity_without_io() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true", "name != 'missing'"],
        Some(vec!["name".to_owned()]),
        Some(10),
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();

    assert_eq!(resource.negotiate_count.load(Ordering::SeqCst), 1);
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(plan.scan.pushed_predicates.len(), 2);
    assert_eq!(
        plan.scan.pushed_predicates[0].fidelity,
        PushdownFidelity::Exact
    );
    assert_eq!(
        datafusion_filter_pushdown(&plan.scan.pushed_predicates[0].fidelity),
        datafusion::logical_expr::TableProviderFilterPushDown::Exact
    );
    assert_eq!(
        plan.scan.pushed_predicates[1].fidelity,
        PushdownFidelity::Inexact
    );
    assert_eq!(plan.scan.unsupported_predicates.len(), 1);
    assert_eq!(plan.residual_predicates.len(), 2);
    assert!(plan.explain.projection_pushed);
    assert!(plan.explain.limit_pushed);
    assert_eq!(plan.explain.inexact_predicates.len(), 1);
    assert_eq!(plan.explain.unsupported_predicates.len(), 1);
    assert_eq!(plan.explain.partitions.len(), 2);
    assert_eq!(plan.explain.estimates.rows, Some(3));
    assert_eq!(
        plan.explain.delivery_guarantee,
        DeliveryGuarantee::EffectivelyOncePerKey
    );
}

#[test]
fn tier_b_explain_serializes_honest_cdf_native_operator_metadata() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true", "name != 'missing'"],
        Some(vec!["name".to_owned()]),
        Some(10),
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let explain_json = serde_json::to_value(&plan.explain).unwrap();

    assert_honest_cdf_native_operator_metadata(&plan);
    assert_explain_carries_required_fields(&explain_json);
    assert_eq!(plan.explain.pushed_predicates.len(), 2);
    assert_eq!(plan.explain.inexact_predicates.len(), 1);
    assert_eq!(plan.explain.unsupported_predicates.len(), 1);
    assert!(plan.explain.projection_pushed);
    assert!(plan.explain.limit_pushed);
    assert_eq!(plan.explain.partitions.len(), 2);
    assert_eq!(plan.explain.estimates.rows, Some(3));
    assert_eq!(
        plan.explain.delivery_guarantee,
        DeliveryGuarantee::EffectivelyOncePerKey
    );
}

#[test]
fn engine_plan_deserialization_rejects_missing_required_execution_policy() {
    let resource =
        MockResource::tier_a(sample_batches()).with_write_disposition(WriteDisposition::Append);
    let input = plan_input(Vec::new(), None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let mut plan_json = serde_json::to_value(&plan).unwrap();
    plan_json
        .as_object_mut()
        .unwrap()
        .remove("write_disposition");
    let error = serde_json::from_value::<EnginePlan>(plan_json).unwrap_err();
    assert!(error.to_string().contains("write_disposition"));

    let mut plan_json = serde_json::to_value(&plan).unwrap();
    for operator in plan_json["operator_chain"].as_array_mut().unwrap() {
        if operator["kind"] == "package_sink" {
            operator.as_object_mut().unwrap().remove("segmentation");
        }
    }
    let error = serde_json::from_value::<EnginePlan>(plan_json).unwrap_err();
    assert!(error.to_string().contains("segmentation"));
}

#[test]
fn effective_schema_reuses_observation_across_partitions_and_attests_only_attempted_inputs() {
    let effective_schema = sample_schema();
    let physical_schema = sample_schema();
    let physical_hash =
        cdf_contract::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let mut batches = vec![
        batch_for_partition_with_schema(
            "batch-limit-0",
            "part-0",
            physical_schema.clone(),
            vec![1, 2, 3],
            vec!["one", "two", "three"],
            vec![true, true, true],
        ),
        batch_for_partition_with_schema(
            "batch-limit-1",
            "part-1",
            physical_schema.clone(),
            vec![4, 5, 6],
            vec!["four", "five", "six"],
            vec![true, true, true],
        ),
    ];
    for batch in &mut batches {
        batch.header.observed_schema_hash = physical_hash.clone();
        batch.header.source_position = Some(terminal_file_position());
    }
    let descriptor = descriptor();
    let baseline_snapshot = descriptor.schema_source.pinned_snapshot().unwrap().clone();
    let evidence = EffectiveSchemaEvidence::new(
        baseline_snapshot,
        SchemaHash::new("effective-snapshot-v1").unwrap(),
        DiscoveryManifestReference {
            manifest_hash: DiscoveryManifestHash::new("manifest-v1").unwrap(),
            path: ".cdf/schemas/orders@manifest-v1.discovery.json".to_owned(),
        },
        vec![EffectiveSchemaObservationEvidence::new(
            "input-0",
            physical_hash.clone(),
        )],
    )
    .unwrap();
    let runtime = EffectiveSchemaRuntime::new(
        evidence,
        vec![EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap()
    .with_discovery_executor_budget(DiscoveryExecutorBudgetEvidence::new(64, 128, 2).unwrap())
    .unwrap();
    let resource =
        MockResource::tier_b(batches).with_effective_schema_runtime(effective_schema, runtime);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, Some(1), PlanBoundedness::Bounded),
        )
        .unwrap();
    assert_eq!(
        plan.effective_schema_evidence().unwrap().observations.len(),
        1
    );

    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 1);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 0);
    let witnessed: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/per-observation-coercion.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(witnessed["observations"].as_array().unwrap().len(), 1);

    let mut tampered = plan.clone();
    tampered
        .effective_schema_evidence
        .as_mut()
        .unwrap()
        .discovery_executor_budget =
        Some(DiscoveryExecutorBudgetEvidence::new(32, 128, 2).unwrap());
    let tampered_package = TempDir::new().unwrap();
    let error = block_on(execute_to_package(
        &tampered,
        &resource,
        tampered_package.path(),
    ))
    .unwrap_err();
    assert!(
        error.to_string().contains("discovery executor budget"),
        "{error}"
    );
}

#[test]
fn terminal_schema_observation_quarantine_processes_repeated_partitions_without_opening_data() {
    let effective_schema = sample_schema();
    let physical_schema = sample_schema();
    let physical_hash =
        cdf_contract::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash.clone());
    let processed_position = terminal_file_position();
    let secret_batches = vec![batch_for_partition_with_schema(
        "secret-batch",
        "part-0",
        effective_schema.clone(),
        vec![1],
        vec!["super-secret-row-value"],
        vec![true],
    )];
    let resource = MockResource::tier_b(secret_batches)
        .with_effective_schema_runtime(effective_schema, runtime)
        .with_attestation(PartitionAttestation::new(
            processed_position.clone(),
            Some(physical_hash),
        ));
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, PlanBoundedness::Bounded),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert!(output.output.segments.is_empty());
    assert!(output.segment_positions.is_empty());
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    let processed = output.execution_evidence().processed_observations();
    assert_eq!(processed.len(), 1);
    assert_eq!(processed[0].source_position, processed_position);
    assert!(
        temp.path()
            .join("quarantine/schema-observations.json")
            .is_file()
    );
    assert!(!temp.path().join("quarantine/records.parquet").is_file());
    let terminal_json =
        std::fs::read_to_string(temp.path().join("quarantine/schema-observations.json")).unwrap();
    assert!(!terminal_json.contains("super-secret-row-value"));

    let mut conflicting = plan.clone();
    conflicting.scan.partitions[1].metadata.insert(
        PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
        "conflicting-binding".to_owned(),
    );
    let conflicting_package = TempDir::new().unwrap();
    let error = block_on(execute_to_package(
        &conflicting,
        &resource,
        conflicting_package.path(),
    ))
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("missing or spoofed cdf:schema_observation_binding"),
        "{error}"
    );
}

#[test]
fn terminal_schema_observation_attestation_change_aborts_before_processed_evidence() {
    let effective_schema = sample_schema();
    let physical_schema = sample_schema();
    let physical_hash =
        cdf_contract::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash);
    let resource = MockResource::tier_b(Vec::new())
        .with_effective_schema_runtime(effective_schema, runtime)
        .with_attestation(PartitionAttestation::new(
            terminal_file_position(),
            Some(SchemaHash::new("changed-physical-schema").unwrap()),
        ));
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, PlanBoundedness::Bounded),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(
        error.to_string().contains("changed physical schema"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    assert!(
        !temp
            .path()
            .join("state/processed-observations.json")
            .exists()
    );
    assert!(
        !temp
            .path()
            .join("quarantine/schema-observations.json")
            .exists()
    );
}

#[test]
fn terminal_schema_observation_identity_attestation_failure_aborts_before_processed_evidence() {
    let effective_schema = sample_schema();
    let physical_schema = sample_schema();
    let physical_hash =
        cdf_contract::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash);
    let resource = MockResource::tier_b(Vec::new())
        .with_effective_schema_runtime(effective_schema, runtime)
        .with_attestation_error("file identity changed between planning and execution");
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, PlanBoundedness::Bounded),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(
        error.to_string().contains("file identity changed"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    assert!(
        !temp
            .path()
            .join("state/processed-observations.json")
            .exists()
    );
}

fn terminal_effective_schema_runtime(
    physical_schema: SchemaRef,
    physical_hash: SchemaHash,
) -> EffectiveSchemaRuntime {
    let descriptor = descriptor();
    let evidence = EffectiveSchemaEvidence::new(
        descriptor.schema_source.pinned_snapshot().unwrap().clone(),
        SchemaHash::new("effective-snapshot-v1").unwrap(),
        DiscoveryManifestReference {
            manifest_hash: DiscoveryManifestHash::new("manifest-v1").unwrap(),
            path: ".cdf/schemas/orders@manifest-v1.discovery.json".to_owned(),
        },
        vec![EffectiveSchemaObservationEvidence::new(
            "input-0",
            physical_hash.clone(),
        )],
    )
    .unwrap();
    let terminal = TerminalSchemaObservationQuarantine::new(
        "input-0",
        physical_hash.clone(),
        "schema-observation:freeze-deviation",
        "schema_observation_quarantined",
        SchemaObservationPolicy::Freeze,
        "refresh the pinned schema or correct the source file",
        vec![
            SchemaObservationFieldQuarantine::new_field_path(
                vec!["id".to_owned()],
                Some(
                    cdf_kernel::CanonicalArrowField::from_arrow(&Field::new(
                        "id",
                        DataType::Utf8,
                        false,
                    ))
                    .unwrap(),
                ),
                Some(
                    cdf_kernel::CanonicalArrowField::from_arrow(&Field::new(
                        "id",
                        DataType::Int64,
                        false,
                    ))
                    .unwrap(),
                ),
                "incompatible physical type",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    EffectiveSchemaRuntime::new(
        evidence,
        vec![EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap()
    .with_terminal_quarantines(vec![terminal])
    .unwrap()
    .with_discovery_executor_budget(DiscoveryExecutorBudgetEvidence::new(64, 128, 2).unwrap())
    .unwrap()
}

fn terminal_file_position() -> SourcePosition {
    SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "input-0".to_owned(),
            size_bytes: 10,
            etag: Some("etag-0".to_owned()),
            sha256: Some("sha-0".to_owned()),
        }],
    })
}

#[test]
fn inexact_and_unsupported_predicates_are_reapplied_during_execution() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true", "name != 'three'"],
        Some(vec!["name".to_owned()]),
        None,
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    for segment in output.segments {
        let batches = reader.read_segment(&segment.segment_id).unwrap();
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "two");
    }
}

#[test]
fn durable_segment_hook_runs_after_publish_with_exact_entry_and_batch() {
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(vec![], None, None, PlanBoundedness::Bounded),
        )
        .unwrap();
    let package_dir = TempDir::new().unwrap();
    let durable_root = package_dir.path().to_path_buf();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let hook_observed = Arc::clone(&observed);
    let mut durable_segment = move |entry: &cdf_package::SegmentEntry, batches: &[RecordBatch]| {
        assert!(durable_root.join(&entry.path).is_file());
        hook_observed.lock().unwrap().push((
            entry.segment_id.clone(),
            entry.sha256.clone(),
            entry.row_count,
            batches
                .iter()
                .map(|batch| batch.num_rows() as u64)
                .sum::<u64>(),
        ));
        Ok(())
    };
    fn pre_finalize(
        _builder: &cdf_package::PackageBuilder,
        _draft: EnginePackageDraft<'_>,
    ) -> Result<()> {
        Ok(())
    }
    let mut stream_finalize = || Ok(());

    let output = block_on(execute_to_package_with_streaming_hooks(
        &plan,
        &resource,
        package_dir.path(),
        &pre_finalize,
        &mut durable_segment,
        &mut stream_finalize,
        EngineExecutionOptions::default(),
    ))
    .unwrap();

    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), output.output.segments.len());
    for (actual, expected) in observed.iter().zip(&output.output.segments) {
        assert_eq!(&actual.0, &expected.segment_id);
        assert_eq!(&actual.1, &expected.sha256);
        assert_eq!(actual.2, actual.3);
        assert_eq!(actual.2, expected.row_count);
    }
}

#[test]
fn illegal_unbounded_live_plan_is_rejected() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(
        vec![],
        None,
        None,
        PlanBoundedness::UnboundedLive {
            checkpoint_cadence_ms: None,
            package_rotation_rows: None,
            watermark: None,
        },
    );
    let error = Planner::new().plan_tier_a(&resource, input).unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(error.message.contains("unbounded live plans are illegal"));
}

#[test]
fn explain_and_operator_chain_carry_contract_package_details() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(
        vec!["active = true"],
        Some(vec!["id".to_owned(), "name".to_owned()]),
        Some(2),
        PlanBoundedness::UnboundedDrain,
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let explain_json = serde_json::to_value(&plan.explain).unwrap();

    assert_honest_cdf_native_operator_metadata(&plan);
    assert_explain_carries_required_fields(&explain_json);
    assert!(plan.operator_chain.iter().any(|operator| {
        matches!(
            operator,
            OperatorNode::ContractExec {
                normalizer_version,
                ..
            } if normalizer_version == cdf_contract::NORMALIZER_NAMECASE_V1
        )
    }));
    assert!(plan.operator_chain.iter().any(|operator| {
        matches!(
            operator,
            OperatorNode::PackageSink { package_id, segmentation }
                if package_id == "pkg-engine-test"
                    && segmentation == &CanonicalSegmentationPolicy::p3_v2()
        )
    }));
}

#[test]
fn operator_graph_compiles_from_capabilities_without_driver_name_dispatch() {
    let resource = MockResource::tier_b(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, PlanBoundedness::Bounded),
        )
        .unwrap();
    let source =
        cdf_runtime::CompiledSourcePlan::new(
            cdf_runtime::SourceDriverDescriptor {
                driver_id: cdf_runtime::SourceDriverId::new("external_mock").unwrap(),
                driver_version: "mock-v1".to_owned(),
                option_schema_hash:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                kinds: vec!["external_mock".to_owned()],
                schemes: vec!["mock".to_owned()],
            },
            resource.capabilities().clone(),
            cdf_runtime::SourceExecutionCapabilities {
                minimum_poll_bytes: 1024,
                maximum_poll_bytes: 1024 * 1024,
                minimum_decode_bytes: 1024,
                maximum_decode_bytes: 8 * 1024 * 1024,
                maximum_concurrency: 8,
                useful_concurrency: 4,
                executor_class: cdf_runtime::SourceExecutorClass::Io,
                blocking_lane: None,
                pausable: true,
                spillable: false,
                idempotent_reads: true,
                reopenable: true,
                resumable: true,
                speculative_safe: true,
                retry_granularity: cdf_runtime::SourceRetryGranularity::Partition,
                retryable_errors: vec![cdf_kernel::ErrorKind::Transient],
                attestation: cdf_runtime::SourceAttestationStrength::ImmutableContent,
                rate_limit_per_second: None,
                quota_authority: None,
                canonical_order: true,
                bounded: true,
                telemetry_version: "mock-v1".to_owned(),
            },
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: resource.descriptor().clone(),
                schema: resource.schema().as_ref().clone(),
                type_policy_allowances: cdf_kernel::TypePolicyAllowances::default(),
                effective_schema_runtime: None,
                redacted_options: serde_json::json!({"endpoint": "redacted"}),
                physical_plan: serde_json::json!({"partitioning": "mock"}),
            },
        )
        .unwrap();

    let graph = compile_operator_graph(
        &plan,
        &source,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
    )
    .unwrap();

    graph.validate().unwrap();
    graph
        .validate_destination_join(&cdf_runtime::DestinationRuntimeCapabilities::default())
        .unwrap();
    let stale_staged = cdf_runtime::DestinationRuntimeCapabilities {
        ingress_mode: cdf_runtime::DestinationIngressMode::StagedDurableSegments,
        staged_ingress: Some(cdf_runtime::StagedIngressCapabilities {
            recovery: cdf_runtime::StagingRecoveryMode::RollbackRedrive,
            visibility: cdf_runtime::StagingVisibility::IsolatedUntilFinalBinding,
            abort_idempotent: true,
            lifecycle_cleanup: true,
            final_binding_requires_exclusive_writer: false,
        }),
        max_in_flight_bytes: Some(64 * 1024 * 1024),
        ..Default::default()
    };
    assert!(graph.validate_destination_join(&stale_staged).is_err());
    assert_eq!(graph.nodes[0].implementation_version, "mock-v1");
    assert!(
        graph
            .nodes
            .iter()
            .all(|node| node.node_id != "external_mock")
    );
    assert!(graph.edges.iter().any(|edge| {
        edge.transfer == cdf_runtime::GraphEdgeTransfer::Fused
            && edge.producer == "reconcile"
            && edge.consumer == "transform"
    }));
    assert!(graph.edges.iter().any(|edge| {
        edge.transfer == cdf_runtime::GraphEdgeTransfer::Durable
            && edge.producer == "segment_persist"
    }));
    plan = plan.bind_partition_schedule(&source).unwrap();
    plan.operator_graph = Some(graph.clone());
    let temp = TempDir::new().unwrap();
    let serial = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let packaged: cdf_runtime::CompiledOperatorGraph = serde_json::from_slice(
        &std::fs::read(temp.path().join("plan/operator-graph.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(packaged, graph);

    let parallel_temp = TempDir::new().unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        plan.scan.partitions.len(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(4),
    )
    .unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let parallel = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        parallel_temp.path(),
        &pre_finalize,
        EngineExecutionOptions::default()
            .with_execution_services(services.clone())
            .with_scheduler_resolution(scheduler),
    ))
    .unwrap();
    assert_eq!(parallel.output.manifest.identity, serial.manifest.identity);
    assert_eq!(parallel.output.lineage, serial.lineage);
    assert_eq!(services.memory().snapshot().current_bytes, 0);

    let destination = cdf_runtime::DestinationRuntimeCapabilities {
        blocking_lanes: vec![
            cdf_runtime::BlockingLaneSpec {
                lane_id: "mock.maintenance".to_owned(),
                maximum_concurrency: 1,
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
                affinity: cdf_runtime::LaneAffinity::Shared,
                interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
            },
            cdf_runtime::BlockingLaneSpec {
                lane_id: "mock.commit".to_owned(),
                maximum_concurrency: 1,
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
                affinity: cdf_runtime::LaneAffinity::Pinned,
                interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
            },
        ],
        final_binding_lane: Some("mock.commit".to_owned()),
        ..cdf_runtime::DestinationRuntimeCapabilities::default()
    };
    let graph = compile_operator_graph(&plan, &source, &destination).unwrap();
    graph.validate_destination_join(&destination).unwrap();
    let binding = graph
        .nodes
        .iter()
        .find(|node| node.node_id == "destination_bind")
        .unwrap();
    assert_eq!(binding.blocking_lane.as_deref(), Some("mock.commit"));
}

#[test]
fn validation_program_source_name_can_cover_and_rename_batch_field() {
    let resource = MockResource::tier_a(sample_batches());
    let mut input = plan_input(
        vec![],
        Some(vec!["name".to_owned()]),
        None,
        PlanBoundedness::Bounded,
    );
    rename_column_program_output(&mut input.validation_program, "name", "customer_name");
    retain_column_program_by_source(&mut input.validation_program, "name");
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let schema = batches[0].schema();
    let field = schema.field(0);
    assert_eq!(field.name(), "customer_name");
    assert_eq!(source_name(field), Some("name"));
}

#[test]
fn validation_program_output_name_can_cover_already_normalized_batch_field() {
    let resource = MockResource::tier_a(output_name_batches());
    let mut input = plan_input_for_schema(
        output_name_schema(),
        vec![],
        Some(vec!["customer_name".to_owned()]),
        None,
        PlanBoundedness::Bounded,
    );
    rename_column_program_source(&mut input.validation_program, "customer_name", "name");
    retain_column_program_by_output(&mut input.validation_program, "customer_name");
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let schema = batches[0].schema();
    let field = schema.field(0);
    assert_eq!(field.name(), "customer_name");
    assert_eq!(source_name(field), Some("name"));
}

#[test]
fn package_artifacts_record_schema_coercion_evidence_and_physical_type_metadata() {
    let resource = MockResource::tier_a(vec![parquet_reconciled_batch()]);
    let input = plan_input_for_schema(
        parquet_reconciled_schema(),
        vec![],
        None,
        None,
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);

    let validation_program: cdf_contract::ValidationProgram = serde_json::from_slice(
        &std::fs::read(temp.path().join("plan/validation-program.json")).unwrap(),
    )
    .unwrap();
    let plan_evidence = validation_program
        .schema_coercion
        .as_ref()
        .expect("validation program should carry schema coercion evidence");
    let widened = coercion_decision(plan_evidence, "id");
    assert_eq!(widened.decision, FieldCoercionDecision::Widened);
    assert_eq!(widened.observed_type.as_deref(), Some("Int32"));
    assert_eq!(widened.constraint_type.as_deref(), Some("Int64"));

    let preserved = coercion_decision(plan_evidence, "name");
    assert_eq!(preserved.decision, FieldCoercionDecision::Preserved);
    assert_eq!(preserved.observed_type.as_deref(), Some("Utf8"));
    assert_eq!(preserved.constraint_type.as_deref(), Some("Utf8"));

    let schema_evidence: cdf_contract::SchemaCoercionPlan = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/coercion-plan.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(&schema_evidence, plan_evidence);

    let output_schema: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("schema/output.json")).unwrap())
            .unwrap();
    assert_eq!(
        output_schema["fields"][0]["metadata"]["cdf:physical_type"],
        "Int32"
    );
    assert_eq!(
        output_schema["fields"][0]["metadata"]["cdf:source_name"],
        "id"
    );
    assert_eq!(
        output_schema["fields"][1]["metadata"]["cdf:source_name"],
        "name"
    );

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    assert_eq!(
        batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2]
    );
}

#[test]
fn compiled_output_schema_strips_runtime_provenance_only_after_serializing_evidence() {
    let observed = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let constraint = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let reconciliation = reconcile_schema(
        &observed,
        constraint.as_ref(),
        &ContractPolicy::default().types,
    )
    .unwrap();
    let serialized_plan = serde_json::to_string(&reconciliation.plan).unwrap();
    let runtime_schema = Arc::new(reconciliation.schema);
    assert_eq!(
        runtime_schema
            .field(0)
            .metadata()
            .get("cdf:physical_type")
            .map(String::as_str),
        Some("Int32")
    );
    let record_batch = RecordBatch::try_new(
        runtime_schema,
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-runtime-provenance").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-runtime-provenance").unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.schema_coercion_plan = Some(serialized_plan);
    let resource = MockResource::tier_a(vec![batch]).with_schema(constraint.clone());
    let input = plan_input_for_schema(constraint, vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    reader.verify().unwrap();
    let runtime_output = reader.runtime_arrow_schema().unwrap();
    assert_eq!(runtime_output, plan.output_arrow_schema().unwrap());
    assert!(
        !runtime_output
            .field(0)
            .metadata()
            .contains_key("cdf:physical_type")
    );
    assert_eq!(
        runtime_output
            .field(0)
            .metadata()
            .get("cdf:source_name")
            .map(String::as_str),
        Some("id")
    );
    let evidence: cdf_contract::SchemaCoercionPlan = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/coercion-plan.json")).unwrap(),
    )
    .unwrap();
    let widened = coercion_decision(&evidence, "id");
    assert_eq!(widened.observed_type.as_deref(), Some("Int32"));
    assert_eq!(widened.constraint_type.as_deref(), Some("Int64"));
    assert_eq!(widened.decision, FieldCoercionDecision::Widened);
}

#[test]
fn package_artifacts_preserve_exact_embedded_lossy_and_extra_reconciliation_decisions() {
    let observed = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("source_only", DataType::Utf8, true),
    ]);
    let constraint = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let mut type_policy = ContractPolicy::default().types;
    type_policy.allow_lossy_mapping = true;
    let reconciliation = reconcile_schema(&observed, &constraint, &type_policy).unwrap();
    let serialized_plan = serde_json::to_string(&reconciliation.plan).unwrap();
    let schema = Arc::new(reconciliation.schema);
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    let resource = MockResource::tier_a(vec![{
        let mut batch = Batch::from_record_batch(
            BatchId::new("batch-json-reconciled").unwrap(),
            ResourceId::new("orders").unwrap(),
            PartitionId::new("part-0").unwrap(),
            SchemaHash::new("schema-json-reconciled").unwrap(),
            record_batch,
        )
        .unwrap();
        batch.header.schema_coercion_plan = Some(serialized_plan);
        batch
    }]);
    let input = plan_input_for_schema(
        Arc::new(constraint),
        vec![],
        None,
        None,
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    let evidence: cdf_contract::SchemaCoercionPlan = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/coercion-plan.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        coercion_decision(&evidence, "id").decision,
        FieldCoercionDecision::LossyAllowed
    );
    assert_eq!(
        coercion_decision(&evidence, "source_only").decision,
        FieldCoercionDecision::Extra
    );
}

#[test]
fn package_execution_rejects_source_carried_coercion_metadata_without_trusted_header() {
    let injected_plan = serde_json::json!({
        "fields": [{
            "source_name": "id",
            "observed_name": "id",
            "output_name": "id",
            "observed_type": "Int64",
            "constraint_type": "Int64",
            "decision": "preserved",
            "outcome": "pass",
            "reason": "observed type already satisfies the constraint"
        }]
    })
    .to_string();
    let injected_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int64, false)],
        HashMap::from([("cdf:schema_coercion_plan".to_owned(), injected_plan)]),
    ));
    let record_batch =
        RecordBatch::try_new(injected_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let batch = Batch::from_record_batch(
        BatchId::new("batch-injected-coercion").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-injected-coercion").unwrap(),
        record_batch,
    )
    .unwrap();
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input_for_schema(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![],
        None,
        None,
        PlanBoundedness::Bounded,
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(error.to_string().contains("without trusted batch evidence"));
}

#[test]
fn package_execution_rejects_malformed_trusted_coercion_header() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-malformed-coercion").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-malformed-coercion").unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.schema_coercion_plan = Some("{not-json".to_owned());
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input_for_schema(schema, vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(error.to_string().contains("not a valid coercion plan"));
}

#[test]
fn package_execution_rejects_valid_header_only_coercion_injection() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-header-only-coercion").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-header-only-coercion").unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.schema_coercion_plan = Some(
        serde_json::json!({
            "fields": [{
                "source_name": "fabricated_extra",
                "observed_name": "fabricated_extra",
                "observed_type": "Utf8",
                "decision": "extra",
                "outcome": "admitted_as_variant",
                "reason": "observed field is outside the constraint projection"
            }]
        })
        .to_string(),
    );
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input_for_schema(schema, vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("has no matching reserved Arrow schema metadata")
    );
}

#[test]
fn contract_exec_filters_quarantined_rows_before_normalize() {
    let resource = MockResource::tier_a(sample_batches());
    let mut input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Domain {
        column: "name".to_owned(),
        allowed: vec!["two".to_owned(), "three".to_owned()],
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    assert_eq!(output.segments.len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let names = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "two");
    assert_eq!(names.value(1), "three");
}

#[test]
fn fused_and_unfused_transform_modes_produce_identical_packages() {
    let resource = MockResource::tier_a(sample_batches());
    let mut input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    policy.rows.rules = vec![RowRule::Domain {
        column: "name".to_owned(),
        allowed: vec!["two".to_owned(), "three".to_owned()],
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let fused_dir = TempDir::new().unwrap();
    let unfused_dir = TempDir::new().unwrap();
    let pre_finalize =
        |_: &cdf_package::PackageBuilder, _: EnginePackageDraft<'_>| -> Result<()> { Ok(()) };

    let fused = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        fused_dir.path(),
        &pre_finalize,
        EngineExecutionOptions::default(),
    ))
    .unwrap();
    let unfused = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        unfused_dir.path(),
        &pre_finalize,
        EngineExecutionOptions::default().with_unfused_transform_for_conformance(true),
    ))
    .unwrap();

    assert_eq!(fused, unfused);
    assert_eq!(
        std::fs::read(fused_dir.path().join("quarantine/part-000001.parquet")).unwrap(),
        std::fs::read(unfused_dir.path().join("quarantine/part-000001.parquet")).unwrap()
    );
    cdf_package::PackageReader::open(fused_dir.path())
        .unwrap()
        .verify()
        .unwrap();
    cdf_package::PackageReader::open(unfused_dir.path())
        .unwrap()
        .verify()
        .unwrap();
}

#[test]
fn fused_transform_reserves_before_allocation_and_releases_after_persist() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, PlanBoundedness::Bounded),
        )
        .unwrap();
    let pre_finalize =
        |_: &cdf_package::PackageBuilder, _: EnginePackageDraft<'_>| -> Result<()> { Ok(()) };
    let (_, services) =
        StandaloneExecutionHost::default_services_with_spill(64 * 1024 * 1024, 1024 * 1024)
            .unwrap();
    let output_dir = TempDir::new().unwrap();
    block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        output_dir.path(),
        &pre_finalize,
        EngineExecutionOptions::default().with_execution_services(services.clone()),
    ))
    .unwrap();
    let memory = services.memory().snapshot();
    assert!(memory.consumers.iter().any(|(consumer, usage)| {
        consumer.class == cdf_memory::MemoryClass::Transform && usage.peak_bytes > 0
    }));
    assert_eq!(memory.current_bytes, 0);

    let (_, tiny_services) =
        StandaloneExecutionHost::default_services_with_spill(64, 1024).unwrap();
    let failed_dir = TempDir::new().unwrap();
    let error = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        failed_dir.path(),
        &pre_finalize,
        EngineExecutionOptions::default().with_execution_services(tiny_services.clone()),
    ))
    .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("exceeds managed budget"));
    assert_eq!(tiny_services.memory().snapshot().current_bytes, 0);
}

#[test]
fn contract_exec_writes_redacted_quarantine_artifact_and_keeps_accepted_rows() {
    let raw_pii = "pii-fixture-sensitive";
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        with_semantic(Field::new("name", DataType::Utf8, false), "pii:email"),
        Field::new("active", DataType::Boolean, false),
    ]));
    let mut batch = batch_for_partition_with_schema(
        "batch-pii",
        "part-0",
        schema.clone(),
        vec![1, 2],
        vec!["ok@example.test", raw_pii],
        vec![true, true],
    );
    batch.header.source_position = Some(SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "/tmp/cdf/pii.ndjson".to_owned(),
            size_bytes: 64,
            etag: None,
            sha256: Some("sha256-pii-fixture".to_owned()),
        }],
    }));
    let resource = MockResource::tier_a(vec![batch]);
    let mut input = plan_input_for_schema(schema, vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Regex {
        column: "name".to_owned(),
        pattern: r"^[^@]+@example\.test$".to_owned(),
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    assert_eq!(output.segments.len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let accepted = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted.value(0), "ok@example.test");

    let quarantine = reader.read_quarantine_records().unwrap();
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].source_row_ordinal, 1);
    assert_eq!(quarantine[0].error_code, "regex_violation");
    assert!(matches!(
        quarantine[0].source_position,
        Some(SourcePosition::FileManifest(_))
    ));
    let cdf_package::QuarantineObservedValue::Hashed { algorithm, value } =
        &quarantine[0].observed_value_redacted
    else {
        panic!("pii semantic field must be hash-redacted");
    };
    assert_eq!(algorithm, "sha256");
    assert_eq!(
        value,
        "sha256:0a08d503e0f6794940fd8e6a1f547999622742616551894946ba6dc0489cf184"
    );

    let files = reader
        .manifest()
        .identity
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    assert!(files.contains(&"stats/verdict-summary.json"));
    assert!(files.contains(&"stats/quarantine-summary.json"));

    let verdict_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/verdict-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(verdict_summary["input_rows"], 2);
    assert_eq!(verdict_summary["accepted_rows"], 1);
    assert_eq!(verdict_summary["quarantined_rows"], 1);
    assert_eq!(verdict_summary["violation_count"], 1);
    assert_eq!(verdict_summary["quarantine_candidate_count"], 1);
    assert!(
        verdict_summary["rule_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary
                == &serde_json::json!({
                    "rule_id": "row-rule-0000-regex",
                    "error_code": "regex_violation",
                    "checked_rows": 2,
                    "violation_count": 1
                }))
    );

    let quarantine_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/quarantine-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine_summary["quarantined_rows"], 1);
    assert_eq!(quarantine_summary["quarantine_candidate_count"], 1);
    assert_eq!(quarantine_summary["artifact_count"], 1);
    assert_eq!(
        quarantine_summary["artifacts"],
        serde_json::json!(["quarantine/part-000001.parquet"])
    );

    let quarantine_path = temp.path().join("quarantine/part-000001.parquet");
    let artifact = std::fs::read(quarantine_path).unwrap();
    assert!(!String::from_utf8_lossy(&artifact).contains(raw_pii));
    assert!(
        reader
            .verify()
            .unwrap()
            .checked_files
            .iter()
            .any(|file| file.path == "quarantine/part-000001.parquet")
    );
}

#[test]
fn source_decode_quarantine_facts_fold_into_package_artifacts() {
    let mut batch = batch_for_partition(
        "batch-source-drift",
        "part-0",
        vec![3],
        vec!["three"],
        vec![true],
    );
    batch.header.source_position = Some(SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "/tmp/cdf/source-drift.ndjson".to_owned(),
            size_bytes: 96,
            etag: None,
            sha256: Some("sha256-source-drift-fixture".to_owned()),
        }],
    }));
    batch.header.pre_contract_quarantine = vec![PreContractQuarantineFact {
        source_row_ordinal: 1,
        rule_id: "source-decode:event_type:type-mismatch".to_owned(),
        error_code: "source_type_mismatch".to_owned(),
        source_position: batch.header.source_position.clone(),
        observed_value_redacted: PreContractObservedValue::Preserved {
            value: "42".to_owned(),
        },
    }];
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    assert_eq!(output.segments.len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let accepted = reader.read_segment(&output.segments[0].segment_id).unwrap();
    assert_eq!(batch_i32s(&accepted[0], "id"), vec![3]);
    assert_eq!(batch_strings(&accepted, "name"), vec!["three"]);
    let quarantine = reader.read_quarantine_records().unwrap();
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].source_row_ordinal, 1);
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
        cdf_package::QuarantineObservedValue::Preserved {
            value: "42".to_owned()
        }
    );

    let verdict_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/verdict-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(verdict_summary["input_rows"], 2);
    assert_eq!(verdict_summary["accepted_rows"], 1);
    assert_eq!(verdict_summary["quarantined_rows"], 1);
    assert_eq!(verdict_summary["violation_count"], 1);
    assert_eq!(verdict_summary["quarantine_candidate_count"], 1);
    assert!(
        verdict_summary["rule_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary
                == &serde_json::json!({
                    "rule_id": "source-decode:event_type:type-mismatch",
                    "error_code": "source_type_mismatch",
                    "checked_rows": 1,
                    "violation_count": 1
                }))
    );

    let quarantine_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/quarantine-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine_summary["quarantined_rows"], 1);
    assert_eq!(quarantine_summary["quarantine_candidate_count"], 1);
    assert_eq!(
        quarantine_summary["artifacts"],
        serde_json::json!(["quarantine/part-000001.parquet"])
    );
    reader.verify().unwrap();
}

#[test]
fn variant_capture_materializes_nested_values_and_contract_evolution_evidence() {
    let resource = MockResource::tier_a(vec![nested_variant_batch()]);
    let mut input = plan_input_for_schema(
        resource.schema(),
        vec![],
        None,
        None,
        PlanBoundedness::Bounded,
    );
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.normalization.nested = NestedDataPolicy::VariantCapture(Default::default());
    policy.rows.rules = vec![RowRule::Regex {
        column: "email".to_owned(),
        pattern: r"^[^@]+@example\.test$".to_owned(),
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let batch = &batches[0];
    assert_eq!(batch.schema().fields().len(), 3);
    assert!(batch.schema().field_with_name("payload").is_err());
    assert!(batch.schema().field_with_name("tags").is_err());
    assert!(batch.schema().field_with_name("attributes").is_err());
    let batch_schema = batch.schema();
    let variant_field = batch_schema.field_with_name(VARIANT_COLUMN_NAME).unwrap();
    assert_eq!(
        cdf_kernel::semantic(variant_field),
        Some(VARIANT_SEMANTIC_TAG)
    );
    assert_eq!(
        variant_field
            .metadata()
            .get(RESIDUAL_ENCODING_METADATA_KEY)
            .map(String::as_str),
        Some(RESIDUAL_ENCODING_NAME)
    );
    let variants = batch
        .column_by_name(VARIANT_COLUMN_NAME)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        variants.value(0),
        r#"{"v":1,"fields":{"/attributes":{"arrow_type":{"kind":"map","field":{"name":"entries","data_type":{"kind":"struct","fields":[{"name":"keys","data_type":{"kind":"utf8","offset_width":32},"nullable":false,"metadata":{}},{"name":"values","data_type":{"kind":"int","signed":true,"bits":32},"nullable":true,"metadata":{}}]},"nullable":false,"metadata":{}},"sorted":false},"encoding":"nested","value":[{"key":"tier","value":"1"}]},"/payload":{"arrow_type":{"kind":"struct","fields":[{"name":"kind","data_type":{"kind":"utf8","offset_width":32},"nullable":false,"metadata":{}},{"name":"count","data_type":{"kind":"int","signed":true,"bits":32},"nullable":false,"metadata":{}}]},"encoding":"nested","value":{"count":"7","kind":"alpha"}},"/tags":{"arrow_type":{"kind":"list","field":{"name":"item","data_type":{"kind":"int","signed":true,"bits":32},"nullable":true,"metadata":{}},"offset_width":32,"view":false},"encoding":"nested","value":["1","2"]}}}"#
    );
    let decoded = cdf_contract::decode_residual_json_v1(variants.value(0).as_bytes()).unwrap();
    assert_eq!(
        decoded
            .iter()
            .map(|field| field.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/attributes", "/payload", "/tags"]
    );
    let source_schema = resource.schema();
    assert_eq!(
        decoded[0].array.data_type(),
        source_schema
            .field_with_name("attributes")
            .unwrap()
            .data_type()
    );
    assert_eq!(
        decoded[1].array.data_type(),
        source_schema
            .field_with_name("payload")
            .unwrap()
            .data_type()
    );
    assert_eq!(
        decoded[2].array.data_type(),
        source_schema.field_with_name("tags").unwrap().data_type()
    );

    let output_schema: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("schema/output.json")).unwrap())
            .unwrap();
    assert_eq!(
        output_schema["fields"][2],
        serde_json::json!({
            "name": VARIANT_COLUMN_NAME,
            "data_type": "Utf8",
            "nullable": true,
            "semantic": VARIANT_SEMANTIC_TAG,
            "metadata": {
                (RESIDUAL_ENCODING_METADATA_KEY): RESIDUAL_ENCODING_NAME
            }
        })
    );
    let evolution_path = temp.path().join("schema/contract-evolution.json");
    let evolution_bytes = std::fs::read(&evolution_path).unwrap();
    let evolution: serde_json::Value = serde_json::from_slice(&evolution_bytes).unwrap();
    assert_eq!(evolution["implicit_promotion_count"], 0);
    assert_eq!(evolution["promotion_events"], serde_json::json!([]));
    assert_eq!(
        evolution["variant_capture"],
        serde_json::json!([
            {
                "source_field": "attributes",
                "variant_column": VARIANT_COLUMN_NAME,
                "semantic": VARIANT_SEMANTIC_TAG
            },
            {
                "source_field": "payload",
                "variant_column": VARIANT_COLUMN_NAME,
                "semantic": VARIANT_SEMANTIC_TAG
            },
            {
                "source_field": "tags",
                "variant_column": VARIANT_COLUMN_NAME,
                "semantic": VARIANT_SEMANTIC_TAG
            }
        ])
    );
    assert_eq!(
        evolution_bytes,
        cdf_package::canonical_json_bytes(&evolution).unwrap()
    );
    assert!(
        reader
            .verify()
            .unwrap()
            .checked_files
            .iter()
            .any(|file| file.path == "schema/contract-evolution.json")
    );
    assert_eq!(reader.replay_view().unwrap().segments.len(), 1);

    let quarantine = reader.read_quarantine_records().unwrap();
    assert_eq!(quarantine.len(), 1);
    let cdf_package::QuarantineObservedValue::Hashed { value, .. } =
        &quarantine[0].observed_value_redacted
    else {
        panic!("pii variant interaction must keep quarantine observed value hashed");
    };
    assert!(value.starts_with("sha256:"));
    let quarantine_artifact =
        std::fs::read(temp.path().join("quarantine/part-000001.parquet")).unwrap();
    assert!(!String::from_utf8_lossy(&quarantine_artifact).contains("raw-secret"));
}

#[test]
fn residual_contract_exec_captures_safe_values_redacts_pii_and_quarantines_controls() {
    let id_field = Field::new("id", DataType::Int32, true);
    let note_field = with_semantic(Field::new("note", DataType::Int32, true), "pii:note");
    let schema = Arc::new(Schema::new(vec![id_field.clone(), note_field.clone()]));
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-residual").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-v1").unwrap(),
        record_batch,
    )
    .unwrap();
    let note_values = Arc::new(StringArray::from(vec!["alice@example.test"])) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            1,
            1,
            vec!["note".to_owned()],
            with_semantic(Field::new("note", DataType::Utf8, true), "pii:note"),
            Some(note_field),
            note_values,
            0,
        )
        .unwrap(),
    );
    let unknown_values = Arc::new(StringArray::from(vec!["top-secret"])) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            1,
            1,
            vec!["new_secret".to_owned()],
            with_semantic(Field::new("new_secret", DataType::Utf8, true), "pii:secret"),
            None,
            unknown_values,
            0,
        )
        .unwrap(),
    );
    let id_values = Arc::new(StringArray::from(vec!["bad-id"])) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            2,
            2,
            vec!["id".to_owned()],
            Field::new("id", DataType::Utf8, true),
            Some(id_field),
            id_values,
            0,
        )
        .unwrap(),
    );

    let resource =
        MockResource::tier_a(vec![batch]).with_write_disposition(WriteDisposition::Append);
    let mut input = plan_input_for_schema(
        schema,
        vec![],
        Some(vec!["id".to_owned(), "note".to_owned()]),
        None,
        PlanBoundedness::Bounded,
    );
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let planned_schema = plan.output_arrow_schema().unwrap();
    assert_eq!(planned_schema.fields().len(), 3);
    assert_eq!(planned_schema.field(2).name(), VARIANT_COLUMN_NAME);
    assert_ne!(
        plan.schema_authority().unwrap().effective_schema_hash,
        plan.output_schema.as_ref().unwrap().arrow_schema_hash
    );

    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    let output = &batches[0];
    assert_eq!(output.num_rows(), 2);
    let variants = output
        .column_by_name(VARIANT_COLUMN_NAME)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(variants.is_null(0));
    assert!(variants.value(1).contains("sha256:"));
    assert!(!variants.value(1).contains("alice@example.test"));
    assert!(!variants.value(1).contains("top-secret"));

    let quarantine = reader.read_quarantine_records().unwrap();
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].error_code, "cdf.residual_control_critical");
    let evolution_bytes =
        std::fs::read(temp.path().join("schema/contract-evolution.json")).unwrap();
    let evolution_text = String::from_utf8(evolution_bytes.clone()).unwrap();
    assert!(!evolution_text.contains("alice@example.test"));
    assert!(!evolution_text.contains("top-secret"));
    let evolution: serde_json::Value = serde_json::from_slice(&evolution_bytes).unwrap();
    assert_eq!(evolution["version"], 1);
    assert_eq!(evolution["residual_decisions"].as_array().unwrap().len(), 3);
    reader.verify().unwrap();
    assert_eq!(reader.runtime_arrow_schema().unwrap(), planned_schema);
}

#[test]
fn residual_multi_partition_decisions_share_verified_effective_schema_and_keep_identity() {
    const CAPTURE_SENTINEL: &str = "rp2-captured-pii-sentinel";
    const QUARANTINE_SENTINEL: &str = "rp2-quarantined-pii-sentinel";

    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        with_semantic(Field::new("note", DataType::Int32, true), "pii:note"),
    ]));
    let physical_hash =
        cdf_contract::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let reconciliation = reconcile_schema(
        physical_schema.as_ref(),
        physical_schema.as_ref(),
        &ContractPolicy::default().types,
    )
    .unwrap();
    let serialized_coercion = serde_json::to_string(&reconciliation.plan).unwrap();
    let schema = Arc::new(reconciliation.schema);
    let id_field = schema.field(0).as_ref().clone();
    let note_field = schema.field(1).as_ref().clone();

    let captured_record = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut captured_batch = Batch::from_record_batch(
        BatchId::new("batch-residual-captured").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-v1").unwrap(),
        captured_record,
    )
    .unwrap();
    captured_batch.header.observed_schema_hash = physical_hash.clone();
    captured_batch.header.schema_coercion_plan = Some(serialized_coercion.clone());
    captured_batch.header.source_position = Some(terminal_file_position());
    captured_batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            10,
            0,
            vec!["note".to_owned()],
            with_semantic(Field::new("note", DataType::Utf8, true), "pii:note"),
            Some(note_field),
            Arc::new(StringArray::from(vec![CAPTURE_SENTINEL])) as ArrayRef,
            0,
        )
        .unwrap(),
    );

    let quarantined_record = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(30)])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut quarantined_batch = Batch::from_record_batch(
        BatchId::new("batch-residual-quarantined").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-1").unwrap(),
        SchemaHash::new("schema-v1").unwrap(),
        quarantined_record,
    )
    .unwrap();
    quarantined_batch.header.observed_schema_hash = physical_hash.clone();
    quarantined_batch.header.schema_coercion_plan = Some(serialized_coercion);
    quarantined_batch.header.source_position = Some(terminal_file_position());
    quarantined_batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            20,
            0,
            vec!["id".to_owned()],
            Field::new("id", DataType::Utf8, true),
            Some(id_field),
            Arc::new(StringArray::from(vec!["bad-control-id"])) as ArrayRef,
            0,
        )
        .unwrap(),
    );
    quarantined_batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            20,
            0,
            vec!["new_secret".to_owned()],
            with_semantic(Field::new("new_secret", DataType::Utf8, true), "pii:secret"),
            None,
            Arc::new(StringArray::from(vec![QUARANTINE_SENTINEL])) as ArrayRef,
            0,
        )
        .unwrap(),
    );

    let baseline_snapshot = descriptor()
        .schema_source
        .pinned_snapshot()
        .unwrap()
        .clone();
    let effective_schema_hash = SchemaHash::new("effective-snapshot-v1").unwrap();
    let evidence = EffectiveSchemaEvidence::new(
        baseline_snapshot,
        effective_schema_hash.clone(),
        DiscoveryManifestReference {
            manifest_hash: DiscoveryManifestHash::new("manifest-residual-mixed").unwrap(),
            path: ".cdf/schemas/orders@manifest-residual-mixed.discovery.json".to_owned(),
        },
        vec![EffectiveSchemaObservationEvidence::new(
            "input-0",
            physical_hash.clone(),
        )],
    )
    .unwrap();
    let runtime = EffectiveSchemaRuntime::new(
        evidence,
        vec![EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap()
    .with_discovery_executor_budget(DiscoveryExecutorBudgetEvidence::new(64, 128, 2).unwrap())
    .unwrap();
    let resource = MockResource::tier_b(vec![captured_batch, quarantined_batch])
        .with_effective_schema_runtime(schema.clone(), runtime)
        .with_write_disposition(WriteDisposition::Append);
    let mut input = plan_input_for_schema(
        schema,
        vec![],
        Some(vec!["id".to_owned(), "note".to_owned()]),
        None,
        PlanBoundedness::Bounded,
    );
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let planned_schema = plan.output_arrow_schema().unwrap();
    assert_eq!(
        plan.schema_authority().unwrap().effective_schema_hash,
        effective_schema_hash
    );

    let temp = TempDir::new().unwrap();
    let plain = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let managed_temp = TempDir::new().unwrap();
    let (_, services) =
        StandaloneExecutionHost::default_services_with_spill(64 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let managed = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        managed_temp.path(),
        &pre_finalize,
        EngineExecutionOptions::default().with_execution_services(services.clone()),
    ))
    .unwrap();
    assert_eq!(managed.output.manifest.identity, plain.manifest.identity);
    assert_eq!(
        managed.output.manifest.package_hash,
        plain.manifest.package_hash
    );
    assert!(services.spill().snapshot().peak_bytes > 0);
    assert_eq!(services.spill().snapshot().current_bytes, 0);
    assert_eq!(services.memory().snapshot().current_bytes, 0);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    reader.verify().unwrap();
    assert_eq!(reader.runtime_arrow_schema().unwrap(), planned_schema);

    let evolution: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/contract-evolution.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        evolution["baseline_schema_hash"],
        plan.schema_authority()
            .unwrap()
            .baseline_schema_hash
            .as_str()
    );
    assert_eq!(
        evolution["effective_schema_hash"],
        effective_schema_hash.as_str()
    );
    let decisions = evolution["residual_decisions"].as_array().unwrap();
    assert_eq!(decisions.len(), 3);
    assert!(decisions.iter().all(|decision| decision["version"] == 1));
    assert!(
        decisions
            .iter()
            .all(|decision| decision["observation_id"] == "input-0")
    );
    let captured = decisions
        .iter()
        .filter(|decision| decision["batch_id"] == "batch-residual-captured")
        .collect::<Vec<_>>();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0]["verdict"], "captured");
    assert_eq!(captured[0]["source_path"], serde_json::json!(["note"]));
    let quarantined = decisions
        .iter()
        .filter(|decision| decision["batch_id"] == "batch-residual-quarantined")
        .collect::<Vec<_>>();
    assert_eq!(quarantined.len(), 2);
    assert!(
        quarantined
            .iter()
            .all(|decision| decision["verdict"] == "quarantined")
    );
    assert_package_tree_excludes(temp.path(), &[CAPTURE_SENTINEL, QUARANTINE_SENTINEL]);
}

#[test]
fn residual_unsupported_encoding_becomes_named_quarantine() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-unsupported-residual").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-v1").unwrap(),
        record_batch,
    )
    .unwrap();
    let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
    dictionary.append("value").unwrap();
    let dictionary = Arc::new(dictionary.finish()) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            0,
            0,
            vec!["unsupported".to_owned()],
            Field::new("unsupported", dictionary.data_type().clone(), true),
            None,
            dictionary,
            0,
        )
        .unwrap(),
    );
    let resource =
        MockResource::tier_a(vec![batch]).with_write_disposition(WriteDisposition::Append);
    let mut input = plan_input_for_schema(schema, vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let quarantine = reader.read_quarantine_records().unwrap();
    assert_eq!(quarantine.len(), 1);
    assert_eq!(
        quarantine[0].error_code,
        cdf_contract::RESIDUAL_ENCODE_UNSUPPORTED_CODE
    );
    let evolution: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/contract-evolution.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        evolution["residual_decisions"][0]["observed_physical_type"]["kind"],
        "dictionary"
    );
}

#[test]
fn execution_rejects_schema_authority_and_zero_row_output_schema_tampering() {
    let resource =
        MockResource::tier_a(Vec::new()).with_write_disposition(WriteDisposition::Append);
    let input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();

    let mut authority_tamper = plan.clone();
    authority_tamper
        .schema_authority
        .as_mut()
        .unwrap()
        .effective_schema_hash = SchemaHash::new("sha256:forged-authority").unwrap();
    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(
        &authority_tamper,
        &resource,
        temp.path(),
    ))
    .unwrap_err();
    assert!(error.to_string().contains("schema authority"));

    let mut output_tamper = plan;
    let output = output_tamper.output_schema.as_mut().unwrap();
    output.fields.pop();
    let forged_schema = Schema::new(
        output
            .fields
            .iter()
            .map(|field| field.to_arrow().unwrap())
            .collect::<Vec<_>>(),
    );
    output.arrow_schema_hash = cdf_contract::canonical_arrow_schema_hash(&forged_schema).unwrap();
    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(&output_tamper, &resource, temp.path())).unwrap_err();
    assert!(error.to_string().contains("compiled output schema"));
}

#[test]
fn reject_batch_contract_abort_prevents_packaged_manifest() {
    let resource = MockResource::tier_a(sample_batches());
    let mut input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.verdicts.violation = VerdictAction::RejectBatch;
    policy.rows.rules = vec![RowRule::Domain {
        column: "name".to_owned(),
        allowed: vec!["missing".to_owned()],
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(error.to_string().contains("reject_batch"));
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    assert_ne!(reader.manifest().lifecycle.status, PackageStatus::Packaged);
}

#[test]
fn merge_dedup_keep_last_runs_after_contract_filtering_and_before_normalize() {
    let batches = vec![
        batch_for_partition(
            "batch-dedup-0",
            "part-0",
            vec![1, 2],
            vec!["one-first", "two"],
            vec![true, true],
        ),
        batch_for_partition(
            "batch-dedup-1",
            "part-0",
            vec![1, 3, 1],
            vec!["one-last", "three", "one-invalid"],
            vec![true, true, true],
        ),
    ];
    let resource = MockResource::tier_a(batches);
    let mut input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![
        RowRule::Domain {
            column: "name".to_owned(),
            allowed: vec![
                "one-first".to_owned(),
                "one-last".to_owned(),
                "two".to_owned(),
                "three".to_owned(),
            ],
        },
        RowRule::Dedup {
            keys: vec!["id".to_owned()],
            keep: DedupKeep::Last,
        },
    ];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    rename_column_program_output(&mut input.validation_program, "name", "customer_name");
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    assert_eq!(output.segments.len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let segment = reader.read_segment(&output.segments[0].segment_id).unwrap();
    assert_eq!(batch_i32s(&segment[0], "id"), vec![2, 1, 3]);
    assert_eq!(
        batch_strings(&segment, "customer_name"),
        vec!["two", "one-last", "three"]
    );

    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["rule_id"], "row-rule-0001-dedup");
    assert_eq!(summary["keep"], "last");
    assert_eq!(summary["input_rows"], 4);
    assert_eq!(summary["output_rows"], 3);
    assert_eq!(summary["duplicate_key_count"], 1);
    assert_eq!(summary["dropped_row_count"], 1);
    assert_eq!(
        reader.read_dedup_dropped_provenance().unwrap(),
        vec![(0, 2)]
    );
    assert!(
        reader
            .manifest()
            .identity
            .files
            .iter()
            .any(|file| file.path == cdf_package::DEDUP_SUMMARY_FILE)
    );
}

#[test]
fn merge_dedup_keep_first_uses_package_order() {
    let batches = vec![
        batch_for_partition(
            "batch-dedup-first-0",
            "part-0",
            vec![1, 2],
            vec!["one-first", "two"],
            vec![true, true],
        ),
        batch_for_partition(
            "batch-dedup-first-1",
            "part-0",
            vec![1, 3],
            vec!["one-last", "three"],
            vec![true, true],
        ),
    ];
    let resource = MockResource::tier_a(batches);
    let mut input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::First,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    assert_eq!(output.segments.len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let segment = reader.read_segment(&output.segments[0].segment_id).unwrap();
    assert_eq!(batch_i32s(&segment[0], "id"), vec![1, 2, 3]);
    assert_eq!(
        batch_strings(&segment, "name"),
        vec!["one-first", "two", "three"]
    );

    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["keep"], "first");
    assert_eq!(summary["input_rows"], 4);
    assert_eq!(summary["output_rows"], 3);
    assert_eq!(summary["duplicate_key_count"], 1);
    assert_eq!(summary["dropped_row_count"], 1);
    assert_eq!(
        reader.read_dedup_dropped_provenance().unwrap(),
        vec![(2, 0)]
    );
}

#[test]
fn package_identity_is_invariant_to_source_batch_rechunking() {
    let one = MockResource::tier_a(vec![batch_for_partition(
        "source-page-one",
        "part-0",
        vec![1, 2, 3, 4],
        vec!["one", "two", "three", "four"],
        vec![true; 4],
    )]);
    let many = MockResource::tier_a(vec![
        batch_for_partition("source-page-a", "part-0", vec![1], vec!["one"], vec![true]),
        batch_for_partition(
            "source-page-b",
            "part-0",
            vec![2, 3],
            vec!["two", "three"],
            vec![true; 2],
        ),
        batch_for_partition("source-page-c", "part-0", vec![4], vec!["four"], vec![true]),
    ]);
    let input = plan_input(Vec::new(), None, None, PlanBoundedness::Bounded);
    let one_plan = Planner::new().plan_tier_a(&one, input.clone()).unwrap();
    let many_plan = Planner::new().plan_tier_a(&many, input).unwrap();
    assert_eq!(one_plan, many_plan);
    let one_dir = TempDir::new().unwrap();
    let many_dir = TempDir::new().unwrap();
    let one_output = block_on(execute_to_package(&one_plan, &one, one_dir.path())).unwrap();
    let many_output = block_on(execute_to_package(&many_plan, &many, many_dir.path())).unwrap();
    assert_eq!(one_output.segments, many_output.segments);
    assert_eq!(one_output.lineage, many_output.lineage);
    assert_eq!(
        one_output.manifest.identity.files,
        many_output.manifest.identity.files
    );
    assert_eq!(
        one_output.manifest.package_hash,
        many_output.manifest.package_hash
    );
    assert_eq!(
        one_output.manifest.package_hash,
        "sha256:069fc3fe3e130ef6b44685178e3454c3d0a8c8afab7639463adf4d591bbbd69a"
    );
}

#[test]
fn append_plan_with_compiled_dedup_rule_does_not_change_rows_or_write_summary() {
    let resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-append-dedup",
        "part-0",
        vec![1, 1],
        vec!["one-first", "one-last"],
        vec![true, true],
    )])
    .with_write_disposition(WriteDisposition::Append);
    let mut input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Last,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    assert_eq!(output.segments.len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    assert_eq!(batch_i32s(&batches[0], "id"), vec![1, 1]);
    assert_eq!(
        batch_strings(&batches, "name"),
        vec!["one-first", "one-last"]
    );
    assert!(reader.read_dedup_summary_json().unwrap().is_none());
}

#[test]
fn append_exact_row_dedup_compiles_and_drops_only_complete_duplicates() {
    let mut resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-append-exact-row-dedup",
        "part-0",
        vec![1, 1, 1],
        vec!["same", "same", "different"],
        vec![true, true, true],
    )])
    .with_write_disposition(WriteDisposition::Append);
    resource.descriptor.deduplication = Some(DeduplicationSpec::ExactRow);
    let mut input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    input.validation_program = compile_resource_validation_program(
        &ContractPolicy::for_trust(TrustLevel::Governed),
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
        resource.descriptor(),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    assert_eq!(batch_i32s(&batches[0], "id"), vec![1, 1]);
    assert_eq!(batch_strings(&batches, "name"), vec!["same", "different"]);
    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["keep"], "first");
    assert_eq!(summary["input_rows"], 3);
    assert_eq!(summary["output_rows"], 2);
    assert_eq!(summary["dropped_row_count"], 1);
    assert_eq!(summary["version"], 2);
    assert_eq!(summary["provenance_format"], "parquet");
    assert_eq!(summary["provenance_shard_row_target"], 65_536);
    assert_eq!(summary["shard_count"], 1);
    assert!(summary.get("dropped_rows").is_none());
    assert!(
        temp.path()
            .join("stats/dedup-dropped/part-000001.parquet")
            .is_file()
    );

    let spill_temp = TempDir::new().unwrap();
    let (_, services) =
        StandaloneExecutionHost::default_services_with_spill(64 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let spilled = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        spill_temp.path(),
        &pre_finalize,
        EngineExecutionOptions::default().with_execution_services(services.clone()),
    ))
    .unwrap();

    assert_eq!(spilled.output.manifest.identity, output.manifest.identity);
    assert_eq!(
        spilled.output.manifest.package_hash,
        output.manifest.package_hash
    );
    let spill = services.spill().snapshot();
    assert!(spill.peak_bytes > 0);
    assert_eq!(spill.current_bytes, 0);
    let memory = services.memory().snapshot();
    assert!(memory.peak_bytes > 0);
    assert_eq!(memory.current_bytes, 0);
}

#[test]
fn replace_plan_with_compiled_dedup_rule_does_not_change_rows_or_write_summary() {
    let resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-replace-dedup",
        "part-0",
        vec![1, 1],
        vec!["one-first", "one-last"],
        vec![true, true],
    )])
    .with_write_disposition(WriteDisposition::Replace);
    let mut input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::First,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    assert_eq!(output.segments.len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = reader.read_segment(&output.segments[0].segment_id).unwrap();
    assert_eq!(batch_i32s(&batches[0], "id"), vec![1, 1]);
    assert_eq!(
        batch_strings(&batches, "name"),
        vec!["one-first", "one-last"]
    );
    assert!(reader.read_dedup_summary_json().unwrap().is_none());
}

#[test]
fn merge_dedup_fail_aborts_before_package_finalization() {
    let resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-dedup-fail",
        "part-0",
        vec![1, 1],
        vec!["one-first", "one-last"],
        vec![true, true],
    )]);
    let mut input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Fail,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(error.to_string().contains("keep=fail aborts"));
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    assert_ne!(reader.manifest().lifecycle.status, PackageStatus::Packaged);
    assert!(reader.manifest().identity.segments.is_empty());
    assert!(reader.read_dedup_summary_json().unwrap().is_none());
}

#[test]
fn freshness_contract_writes_observed_at_context_when_rule_requires_it() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "updated_at",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    )]));
    let batch = Batch::from_record_batch(
        BatchId::new("freshness-batch").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-v1").unwrap(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0]).with_timezone("UTC")) as ArrayRef,
            ],
        )
        .unwrap(),
    )
    .unwrap();
    let mut resource = MockResource::tier_a(vec![batch]);
    resource.descriptor.primary_key.clear();
    resource.descriptor.merge_key.clear();
    resource.descriptor.write_disposition = WriteDisposition::Append;
    let mut input = plan_input_for_schema(schema, vec![], None, None, PlanBoundedness::Bounded);
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Freshness {
        column: "updated_at".to_owned(),
        max_age_ms: 1,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 0);
    assert!(output.segments.is_empty());
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    assert!(
        reader
            .manifest()
            .identity
            .files
            .iter()
            .any(|file| { file.path == "plan/contract-evaluation-context.json" })
    );
}

#[test]
fn traced_execution_emits_run_resource_package_and_partition_spans() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let run_id = RunId::new("run-engine-trace-test").unwrap();
    let temp = TempDir::new().unwrap();
    let subscriber = CapturingSubscriber::default();

    let output = tracing::subscriber::with_default(subscriber.clone(), || {
        block_on(execute_to_package_with_run_id(
            &run_id,
            &plan,
            &resource,
            temp.path(),
        ))
    })
    .unwrap();

    assert_eq!(output.profile.output_batches, 1);
    let spans = subscriber.captured_spans();
    let package_span = spans
        .iter()
        .find(|span| span.name == "cdf_engine.package_execution")
        .expect("package execution span is emitted");
    assert_span_fields(
        package_span,
        &[
            ("run_id", "run-engine-trace-test"),
            ("resource_id", "orders"),
            ("package_id", "pkg-engine-test"),
        ],
    );

    let partition_span = spans
        .iter()
        .find(|span| span.name == "cdf_engine.partition_execution")
        .expect("partition execution span is emitted");
    assert_span_fields(
        partition_span,
        &[
            ("run_id", "run-engine-trace-test"),
            ("resource_id", "orders"),
            ("package_id", "pkg-engine-test"),
            ("partition_id", "part-0"),
        ],
    );
}

#[test]
fn traced_execution_preserves_manifest_identity_hash() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let untraced_temp = TempDir::new().unwrap();
    let traced_temp = TempDir::new().unwrap();

    let untraced = block_on(execute_to_package(&plan, &resource, untraced_temp.path())).unwrap();
    let traced = block_on(execute_to_package_with_run_id(
        &RunId::new("run-engine-hash-test").unwrap(),
        &plan,
        &resource,
        traced_temp.path(),
    ))
    .unwrap();

    assert_eq!(traced.manifest.identity, untraced.manifest.identity);
    assert_eq!(traced.manifest.package_hash, untraced.manifest.package_hash);
    assert_eq!(traced.manifest.signature, untraced.manifest.signature);
}

#[test]
fn phase_telemetry_is_additive_and_preserves_manifest_identity() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(vec![], None, None, PlanBoundedness::Bounded);
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let plain_temp = TempDir::new().unwrap();
    let measured_temp = TempDir::new().unwrap();
    let plain = block_on(execute_to_package(&plan, &resource, plain_temp.path())).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());

    let measured = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        measured_temp.path(),
        &pre_finalize,
        EngineExecutionOptions::default().with_phase_metrics(true),
    ))
    .unwrap();

    assert_eq!(measured.output.manifest.identity, plain.manifest.identity);
    assert_eq!(
        measured.output.manifest.package_hash,
        plain.manifest.package_hash
    );
    assert_eq!(measured.output.manifest.signature, plain.manifest.signature);
    assert!(!measured.phase_metrics.is_empty());
    assert!(measured.phase_metrics.iter().all(|metric| {
        metric.status == RunPhaseStatus::Completed
            && metric.duration_ns > 0
            && metric.operations > 0
    }));
    let phases = measured
        .phase_metrics
        .iter()
        .map(|metric| metric.phase)
        .collect::<std::collections::BTreeSet<_>>();
    for phase in [
        RunPhase::Decode,
        RunPhase::ValidationNormalization,
        RunPhase::SegmentEncode,
        RunPhase::PersistHash,
        RunPhase::PackageFinalize,
    ] {
        assert!(phases.contains(&phase), "missing {phase:?}");
    }
}

#[test]
fn parallel_segment_encoding_is_identical_to_inline_canonical_registration() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, PlanBoundedness::Bounded),
        )
        .unwrap();
    for operator in &mut plan.operator_chain {
        if let OperatorNode::PackageSink { segmentation, .. } = operator {
            segmentation.target_rows = 2;
            segmentation.maximum_rows = 2;
            segmentation.microbatch_minimum_rows = 1;
            segmentation.microbatch_maximum_rows = 2;
        }
    }
    let inline_dir = TempDir::new().unwrap();
    let parallel_dir = TempDir::new().unwrap();
    let inline = block_on(execute_to_package(&plan, &resource, inline_dir.path())).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let parallel = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        parallel_dir.path(),
        &pre_finalize,
        EngineExecutionOptions::default().with_execution_services(services.clone()),
    ))
    .unwrap();

    assert_eq!(parallel.output.manifest.identity, inline.manifest.identity);
    assert_eq!(parallel.output.segments, inline.segments);
    assert_eq!(parallel.output.lineage, inline.lineage);
    assert_eq!(
        parallel.segment_positions,
        inline
            .segments
            .iter()
            .map(|segment| {
                EngineSegmentPosition {
                    segment_id: segment.segment_id.clone(),
                    output_position: None,
                }
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(services.memory().snapshot().current_bytes, 0);
}

#[test]
fn parallel_segment_frontier_failure_joins_workers_and_prevents_finalization() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, PlanBoundedness::Bounded),
        )
        .unwrap();
    for operator in &mut plan.operator_chain {
        if let OperatorNode::PackageSink { segmentation, .. } = operator {
            segmentation.target_rows = 2;
            segmentation.maximum_rows = 2;
            segmentation.microbatch_minimum_rows = 1;
            segmentation.microbatch_maximum_rows = 2;
        }
    }
    let package_dir = TempDir::new().unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let mut durable_segment =
        |_entry: &cdf_package::SegmentEntry, _batches: &[RecordBatch]| -> Result<()> {
            Err(cdf_kernel::CdfError::internal(
                "stop at canonical segment frontier",
            ))
        };
    let mut stream_finalize =
        || -> Result<()> { panic!("failed segment frontier must not reach stream finalization") };

    let error = block_on(execute_to_package_with_streaming_hooks(
        &plan,
        &resource,
        package_dir.path(),
        &pre_finalize,
        &mut durable_segment,
        &mut stream_finalize,
        EngineExecutionOptions::default().with_execution_services(services.clone()),
    ))
    .unwrap_err();

    assert!(error.message.contains("canonical segment frontier"));
    assert_eq!(
        cdf_package::PackageReader::open(package_dir.path())
            .unwrap()
            .manifest()
            .lifecycle
            .status,
        PackageStatus::Extracting
    );
    assert_eq!(services.memory().snapshot().current_bytes, 0);
}

#[test]
fn datafusion_table_provider_pushdown_classification_delegates_to_resource() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(resource.clone(), ScopeKey::Resource);
    let filters = [
        col("id").gt(lit(1_i32)),
        col("active").eq(lit(true)),
        col("name").not_eq(lit("three")),
        col("id").add(lit(1_i32)).gt(lit(2_i32)),
    ];
    let filter_refs = filters.iter().collect::<Vec<_>>();

    let pushdown = provider.supports_filters_pushdown(&filter_refs).unwrap();

    assert_eq!(resource.negotiate_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        pushdown,
        vec![
            datafusion::logical_expr::TableProviderFilterPushDown::Exact,
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact,
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
        ]
    );
    let requests = resource.requests.lock().unwrap();
    assert_eq!(requests[0].filters.len(), 3);
    assert_eq!(requests[0].filters[0].expression, "id > 1");
    assert_eq!(requests[0].filters[1].expression, "active = true");
    assert_eq!(requests[0].filters[2].expression, "name != 'three'");
}

#[test]
fn datafusion_registered_table_executes_with_residuals_and_projection() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = queryable_resource_table_provider(resource.clone(), ScopeKey::Resource);
    let ctx = SessionContext::new();
    ctx.register_table("orders", provider).unwrap();

    let batches = block_on(async {
        let provider = ctx.table_provider("orders").await.unwrap();
        let projection = vec![1];
        let filters = vec![col("id").gt(lit(1_i32))];
        let plan = provider
            .scan(&ctx.state(), Some(&projection), &filters, None)
            .await
            .unwrap();
        collect_execution_plan_partitions(plan, ctx.task_ctx()).await
    });

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(
        batch_strings(&batches, "name"),
        vec!["two", "three", "two", "three"]
    );
    assert_eq!(batches[0].schema().fields().len(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "name");
}

#[test]
fn datafusion_unsupported_expression_stays_residual() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(resource.clone(), ScopeKey::Resource);
    let unsupported = col("id").add(lit(1_i32)).gt(lit(2_i32));
    let filter_refs = vec![&unsupported];
    let pushdown = provider.supports_filters_pushdown(&filter_refs).unwrap();

    assert_eq!(
        pushdown,
        vec![datafusion::logical_expr::TableProviderFilterPushDown::Unsupported]
    );
    let requests = resource.requests.lock().unwrap();
    assert!(requests.iter().all(|request| request.filters.is_empty()));
}

#[test]
fn datafusion_limit_pushdown_is_disabled_for_inexact_filters() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(resource.clone(), ScopeKey::Resource);
    let ctx = SessionContext::new();
    let filters = vec![col("active").eq(lit(true))];

    let _plan = block_on(provider.scan(&ctx.state(), None, &filters, Some(1))).unwrap();

    let requests = resource.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].limit, None);
    assert_eq!(requests[1].limit, None);
}

#[test]
fn datafusion_limit_pushdown_remains_enabled_for_exact_filters() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(resource.clone(), ScopeKey::Resource);
    let ctx = SessionContext::new();
    let filters = vec![col("id").gt(lit(1_i32))];

    let _plan = block_on(provider.scan(&ctx.state(), None, &filters, Some(1))).unwrap();

    let requests = resource.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].limit, None);
    assert_eq!(requests[1].limit, Some(1));
}

#[derive(Clone)]
struct MockResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    batches: Vec<Batch>,
    partition_count: usize,
    negotiate_count: Arc<AtomicUsize>,
    open_count: Arc<AtomicUsize>,
    attest_count: Arc<AtomicUsize>,
    attestation: Option<PartitionAttestation>,
    attestation_error: Option<String>,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
}

#[derive(Clone)]
struct DataFusionMockResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    batches: Vec<Batch>,
    negotiate_count: Arc<AtomicUsize>,
    open_count: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<ScanRequest>>>,
}

impl DataFusionMockResource {
    fn new() -> Self {
        Self {
            descriptor: descriptor(),
            schema: sample_schema(),
            batches: sample_batches(),
            negotiate_count: Arc::new(AtomicUsize::new(0)),
            open_count: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl ResourceStream for DataFusionMockResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        unreachable!("DataFusion adapter must use QueryableResource::negotiate")
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::BoxFuture<'_, Result<BatchStream>> {
        self.open_count.fetch_add(1, Ordering::SeqCst);
        let exact_filters = partition
            .metadata
            .get("exact_filters")
            .map(|filters| filters.split('\n').map(str::to_owned).collect::<Vec<_>>())
            .unwrap_or_default();
        let batches = self
            .batches
            .iter()
            .filter(|batch| batch.header.partition_id == partition.partition_id)
            .map(|batch| apply_mock_exact_filters(batch.clone(), &exact_filters))
            .collect::<Result<Vec<_>>>();
        Box::pin(
            async move { Ok(Box::pin(stream::iter(batches?.into_iter().map(Ok))) as BatchStream) },
        )
    }
}

impl QueryableResource for DataFusionMockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        static CAPABILITIES: std::sync::OnceLock<ResourceCapabilities> = std::sync::OnceLock::new();
        CAPABILITIES.get_or_init(|| ResourceCapabilities {
            projection: CapabilitySupport::Supported,
            filters: FilterCapabilities {
                default_fidelity: PushdownFidelity::Unsupported,
                supported_operators: vec![">".to_owned(), "=".to_owned(), "!=".to_owned()],
            },
            limits: CapabilitySupport::Supported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![cdf_kernel::ScopeKind::Partition],
            },
            incremental: IncrementalShape::Full,
            replay: cdf_kernel::ReplaySupport::ExactRecordedBatches,
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::Rows,
        })
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.negotiate_count.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());

        let mut pushed_predicates = Vec::new();
        let mut unsupported_predicates = Vec::new();
        for predicate in &request.filters {
            match predicate.expression.as_str() {
                "id > 1" => pushed_predicates.push(cdf_kernel::PushedPredicate {
                    predicate: predicate.clone(),
                    fidelity: PushdownFidelity::Exact,
                }),
                "active = true" => pushed_predicates.push(cdf_kernel::PushedPredicate {
                    predicate: predicate.clone(),
                    fidelity: PushdownFidelity::Inexact,
                }),
                _ => unsupported_predicates.push(predicate.clone()),
            }
        }

        let exact_filters = pushed_predicates
            .iter()
            .filter(|pushed| pushed.fidelity == PushdownFidelity::Exact)
            .map(|pushed| pushed.predicate.expression.clone())
            .collect::<Vec<_>>()
            .join("\n");
        let partitions = ["part-0", "part-1"]
            .into_iter()
            .map(|partition| {
                let partition_id = PartitionId::new(partition)?;
                Ok(PartitionPlan {
                    partition_id: partition_id.clone(),
                    scope: ScopeKey::Partition { partition_id },
                    start_position: None,
                    metadata: BTreeMap::from([("exact_filters".to_owned(), exact_filters.clone())]),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ScanPlan {
            plan_id: cdf_kernel::PlanId::new(format!(
                "df-plan-{}-{}",
                request.resource_id.as_str(),
                self.negotiate_count.load(Ordering::SeqCst)
            ))?,
            request: request.clone(),
            partitions,
            pushed_predicates,
            unsupported_predicates,
            estimated_rows: Some(6),
            estimated_bytes: None,
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerKey,
        })
    }
}

impl MockResource {
    fn tier_a(batches: Vec<Batch>) -> Self {
        Self::new(batches, false)
    }

    fn tier_b(batches: Vec<Batch>) -> Self {
        Self::new(batches, true)
    }

    fn new(batches: Vec<Batch>, tier_b: bool) -> Self {
        let schema = batches
            .first()
            .and_then(Batch::record_batch)
            .map(RecordBatch::schema)
            .unwrap_or_else(sample_schema);
        Self {
            descriptor: descriptor(),
            schema,
            batches,
            partition_count: if tier_b { 2 } else { 1 },
            negotiate_count: Arc::new(AtomicUsize::new(0)),
            open_count: Arc::new(AtomicUsize::new(0)),
            attest_count: Arc::new(AtomicUsize::new(0)),
            attestation: None,
            attestation_error: None,
            effective_schema_runtime: None,
        }
    }

    fn with_write_disposition(mut self, write_disposition: WriteDisposition) -> Self {
        self.descriptor.write_disposition = write_disposition;
        self
    }

    fn with_partition_count(mut self, partition_count: usize) -> Self {
        self.partition_count = partition_count;
        self
    }

    fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
        self
    }

    fn with_effective_schema_runtime(
        mut self,
        schema: SchemaRef,
        runtime: EffectiveSchemaRuntime,
    ) -> Self {
        self.schema = schema;
        self.effective_schema_runtime = Some(runtime);
        self
    }

    fn with_attestation(mut self, attestation: PartitionAttestation) -> Self {
        self.attestation = Some(attestation);
        self
    }

    fn with_attestation_error(mut self, error: impl Into<String>) -> Self {
        self.attestation_error = Some(error.into());
        self
    }
}

impl ResourceStream for MockResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        (0..self.partition_count)
            .map(|index| {
                let mut metadata = BTreeMap::from([("ordinal".to_owned(), index.to_string())]);
                if let Some(runtime) = &self.effective_schema_runtime {
                    metadata.insert(
                        PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
                        runtime.evidence.observations[0].observation_id.clone(),
                    );
                    metadata.insert(
                        PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
                        "binding-input-0".to_owned(),
                    );
                }
                Ok(PartitionPlan {
                    partition_id: PartitionId::new(format!("part-{index}"))?,
                    scope: ScopeKey::Partition {
                        partition_id: PartitionId::new(format!("part-{index}"))?,
                    },
                    start_position: None,
                    metadata,
                })
            })
            .collect()
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::BoxFuture<'_, Result<BatchStream>> {
        self.open_count.fetch_add(1, Ordering::SeqCst);
        let batches = self
            .batches
            .iter()
            .filter(|batch| batch.header.partition_id == partition.partition_id)
            .cloned()
            .collect::<Vec<_>>();
        Box::pin(
            async move { Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))) as BatchStream) },
        )
    }

    fn attest_partition(
        &self,
        _partition: &PartitionPlan,
    ) -> cdf_kernel::BoxFuture<'_, Result<Option<PartitionAttestation>>> {
        self.attest_count.fetch_add(1, Ordering::SeqCst);
        let attestation = self.attestation.clone();
        let error = self.attestation_error.clone();
        Box::pin(async move {
            if let Some(error) = error {
                return Err(cdf_kernel::CdfError::data(error));
            }
            Ok(attestation)
        })
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_ref()
    }
}

impl QueryableResource for MockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        static CAPABILITIES: std::sync::OnceLock<ResourceCapabilities> = std::sync::OnceLock::new();
        CAPABILITIES.get_or_init(|| ResourceCapabilities {
            projection: CapabilitySupport::Supported,
            filters: FilterCapabilities {
                default_fidelity: PushdownFidelity::Inexact,
                supported_operators: vec![">".to_owned(), "=".to_owned()],
            },
            limits: CapabilitySupport::Supported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![cdf_kernel::ScopeKind::Partition],
            },
            incremental: IncrementalShape::Cursor,
            replay: cdf_kernel::ReplaySupport::ExactRecordedBatches,
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::RowsAndBytes,
        })
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.negotiate_count.fetch_add(1, Ordering::SeqCst);
        let mut plan = negotiate_scan_plan(
            request.resource_id.clone(),
            request.clone(),
            self.capabilities(),
            self.plan_partitions(request)?,
            Some(3),
            Some(256),
            DeliveryGuarantee::EffectivelyOncePerKey,
        )?;
        for pushed in &mut plan.pushed_predicates {
            if pushed.predicate.expression == "id > 1" {
                pushed.fidelity = PushdownFidelity::Exact;
            }
        }
        Ok(plan)
    }
}

#[derive(Clone, Default)]
struct CapturingSubscriber {
    next_id: Arc<AtomicU64>,
    spans: Arc<Mutex<Vec<CapturedSpan>>>,
}

impl CapturingSubscriber {
    fn captured_spans(&self) -> Vec<CapturedSpan> {
        self.spans.lock().unwrap().clone()
    }
}

impl Subscriber for CapturingSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        self.spans.lock().unwrap().push(CapturedSpan {
            name: attrs.metadata().name().to_owned(),
            fields: visitor.fields,
        });
        Id::from_u64(self.next_id.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &Event<'_>) {}

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

#[derive(Clone, Debug)]
struct CapturedSpan {
    name: String,
    fields: BTreeMap<String, String>,
}

#[derive(Default)]
struct FieldVisitor {
    fields: BTreeMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &TracingField, value: &str) {
        self.fields
            .insert(field.name().to_owned(), value.to_owned());
    }

    fn record_bool(&mut self, field: &TracingField, value: bool) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_i64(&mut self, field: &TracingField, value: i64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_u64(&mut self, field: &TracingField, value: u64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_debug(&mut self, field: &TracingField, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), format!("{value:?}"));
    }
}

fn assert_span_fields(span: &CapturedSpan, expected: &[(&str, &str)]) {
    let expected = expected
        .iter()
        .map(|(field, value)| ((*field).to_owned(), (*value).to_owned()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        span.fields, expected,
        "span {} should record the exact field set",
        span.name
    );
}

fn assert_package_tree_excludes(root: &std::path::Path, sentinels: &[&str]) {
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            assert_package_tree_excludes(&path, sentinels);
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        for sentinel in sentinels {
            assert!(
                !bytes
                    .windows(sentinel.len())
                    .any(|window| window == sentinel.as_bytes()),
                "package artifact {} contains raw sentinel {sentinel:?}",
                path.display()
            );
        }
    }
}

fn assert_honest_cdf_native_operator_metadata(plan: &EnginePlan) {
    let plan_json = serde_json::to_value(plan).unwrap();
    let plan_text = serde_json::to_string(&plan_json).unwrap();
    assert!(!plan_text.contains("data_fusion_table_provider"));
    assert!(!plan_text.contains("data_fusion_scan_exec"));
    assert!(!plan_text.contains("datafusion_table_provider"));

    assert_cdf_native_operator_kinds(&plan_json["operator_chain"]);
    assert_cdf_native_operator_kinds(&plan_json["explain"]["operator_chain"]);
    assert_eq!(
        plan_json["operator_chain"][0]["adapter_kind"],
        "cdf_native_resource_adapter"
    );
    assert_eq!(
        plan_json["explain"]["operator_chain"][0]["adapter_kind"],
        "cdf_native_resource_adapter"
    );
}

fn assert_cdf_native_operator_kinds(operator_chain: &serde_json::Value) {
    let actual = operator_chain
        .as_array()
        .unwrap()
        .iter()
        .map(|operator| operator["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            "cdf_resource_adapter",
            "cdf_native_scan",
            "schema_fingerprint_exec",
            "contract_exec",
            "normalize_exec",
            "profile_exec",
            "lineage_exec",
            "package_sink",
        ]
    );
}

fn assert_explain_carries_required_fields(explain_json: &serde_json::Value) {
    for field in [
        "pushed_predicates",
        "inexact_predicates",
        "unsupported_predicates",
        "partitions",
        "estimates",
        "delivery_guarantee",
        "boundedness",
    ] {
        assert!(explain_json.get(field).is_some(), "missing {field}");
    }
}

fn batch_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let index = batch.schema().index_of(column).unwrap();
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn batch_i32s(batch: &RecordBatch, column: &str) -> Vec<i32> {
    let index = batch.schema().index_of(column).unwrap();
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    (0..array.len()).map(|row| array.value(row)).collect()
}

async fn collect_execution_plan_partitions(
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    task_ctx: Arc<datafusion::execution::TaskContext>,
) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    for partition in 0..plan.properties().partitioning.partition_count() {
        let stream = plan.execute(partition, Arc::clone(&task_ctx)).unwrap();
        batches.extend(collect_stream(stream).await.unwrap());
    }
    batches
}

fn apply_mock_exact_filters(batch: Batch, filters: &[String]) -> Result<Batch> {
    if filters.is_empty() {
        return Ok(batch);
    }
    let Some(record_batch) = batch.record_batch() else {
        return Ok(batch);
    };
    let mut keep = vec![true; record_batch.num_rows()];
    for filter in filters {
        if filter == "id > 1" {
            let id_index = record_batch.schema().index_of("id").unwrap();
            let ids = record_batch
                .column(id_index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for (row, keep_row) in keep.iter_mut().enumerate().take(ids.len()) {
                *keep_row &= ids.value(row) > 1;
            }
        }
    }
    let filtered =
        arrow_select::filter::filter_record_batch(record_batch, &BooleanArray::from(keep))
            .map_err(cdf_kernel::CdfError::from)?;
    let mut header = batch.header;
    header.set_payload_counts(
        filtered.num_rows() as u64,
        filtered.get_array_memory_size() as u64,
    );
    Ok(Batch {
        header,
        payload: cdf_kernel::BatchPayload::in_memory(filtered),
    })
}

fn plan_input(
    filters: Vec<&str>,
    projection: Option<Vec<String>>,
    limit: Option<u64>,
    boundedness: PlanBoundedness,
) -> EnginePlanInput {
    plan_input_for_schema(sample_schema(), filters, projection, limit, boundedness)
}

fn plan_input_for_schema(
    schema: SchemaRef,
    filters: Vec<&str>,
    projection: Option<Vec<String>>,
    limit: Option<u64>,
    boundedness: PlanBoundedness,
) -> EnginePlanInput {
    let observed = ObservedSchema::from_arrow(schema.as_ref());
    let validation_program =
        compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Governed), &observed)
            .unwrap();
    EnginePlanInput {
        request: ScanRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            projection,
            filters: filters
                .into_iter()
                .enumerate()
                .map(|(index, expression)| ScanPredicate {
                    predicate_id: PredicateId::new(format!("p{index}")).unwrap(),
                    expression: expression.to_owned(),
                })
                .collect(),
            limit,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        },
        validation_program,
        boundedness,
        package_id: "pkg-engine-test".to_owned(),
    }
}

fn rename_column_program_output(
    program: &mut cdf_contract::ValidationProgram,
    source_name: &str,
    output_name: &str,
) {
    let column = program
        .column_programs
        .iter_mut()
        .find(|column| column.source_name == source_name)
        .unwrap();
    column.output_name = output_name.to_owned();
}

fn rename_column_program_source(
    program: &mut cdf_contract::ValidationProgram,
    output_name: &str,
    source_name: &str,
) {
    let column = program
        .column_programs
        .iter_mut()
        .find(|column| column.output_name == output_name)
        .unwrap();
    column.source_name = source_name.to_owned();
}

fn retain_column_program_by_source(
    program: &mut cdf_contract::ValidationProgram,
    source_name: &str,
) {
    program
        .column_programs
        .retain(|column| column.source_name == source_name);
}

fn retain_column_program_by_output(
    program: &mut cdf_contract::ValidationProgram,
    output_name: &str,
) {
    program
        .column_programs
        .retain(|column| column.output_name == output_name);
}

fn coercion_decision<'a>(
    plan: &'a cdf_contract::SchemaCoercionPlan,
    source_name: &str,
) -> &'a cdf_contract::FieldCoercion {
    plan.fields
        .iter()
        .find(|field| field.source_name == source_name)
        .unwrap()
}

fn descriptor() -> ResourceDescriptor {
    let schema_hash = SchemaHash::new("schema-v1").unwrap();
    ResourceDescriptor {
        resource_id: ResourceId::new("orders").unwrap(),
        schema_source: SchemaSource::Discovered {
            snapshot: SchemaSnapshotReference {
                schema_hash,
                path: ".cdf/schemas/orders@schema-v1.json".to_owned(),
                metadata: BTreeMap::from([("probe".to_owned(), "engine-test".to_owned())]),
            },
        },
        primary_key: vec!["id".to_owned()],
        merge_key: vec!["id".to_owned()],
        cursor: None,
        write_disposition: WriteDisposition::Merge,
        deduplication: None,
        contract: Some(ContractRef::new("contract-orders").unwrap()),
        state_scope: ScopeKey::Resource,
        freshness: Some(FreshnessSpec { max_age_ms: 60_000 }),
        trust_level: TrustLevel::Governed,
    }
}

fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn output_name_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn parquet_reconciled_schema() -> SchemaRef {
    Arc::new(parquet_reconciliation().schema)
}

fn parquet_reconciliation() -> cdf_contract::SchemaReconciliation {
    reconcile_schema(
        &Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]),
        &Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]),
        &ContractPolicy::default().types,
    )
    .unwrap()
}

fn sample_batches() -> Vec<Batch> {
    vec![
        batch_for_partition(
            "batch-0",
            "part-0",
            vec![1, 2, 3],
            vec!["one", "two", "three"],
            vec![false, true, true],
        ),
        batch_for_partition(
            "batch-1",
            "part-1",
            vec![1, 2, 3],
            vec!["one", "two", "three"],
            vec![false, true, true],
        ),
    ]
}

fn output_name_batches() -> Vec<Batch> {
    vec![batch_for_partition_with_schema(
        "batch-0",
        "part-0",
        output_name_schema(),
        vec![1, 2, 3],
        vec!["one", "two", "three"],
        vec![false, true, true],
    )]
}

fn parquet_reconciled_batch() -> Batch {
    let reconciliation = parquet_reconciliation();
    let serialized_plan = serde_json::to_string(&reconciliation.plan).unwrap();
    let schema = Arc::new(reconciliation.schema);
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
        ],
    )
    .unwrap();

    Batch {
        header: {
            let mut header = BatchHeader::new(
                BatchId::new("batch-parquet-reconciled").unwrap(),
                ResourceId::new("orders").unwrap(),
                PartitionId::new("part-0").unwrap(),
                SchemaHash::new("schema-v1").unwrap(),
                record_batch.num_rows() as u64,
                record_batch.get_array_memory_size() as u64,
            );
            header.schema_coercion_plan = Some(serialized_plan);
            header
        },
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    }
}

fn batch_with_file_position() -> Batch {
    let mut batch = batch_for_partition(
        "batch-file",
        "part-0",
        vec![1, 2],
        vec!["one", "two"],
        vec![true, true],
    );
    batch.header.source_position = Some(SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "/tmp/cdf/events.ndjson".to_owned(),
            size_bytes: 42,
            etag: None,
            sha256: Some("sha256-file".to_owned()),
        }],
    }));
    batch
}

fn nested_variant_batch() -> Batch {
    let payload = StructArray::from(vec![
        (
            Arc::new(Field::new("kind", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("count", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![7, 9])) as ArrayRef,
        ),
    ]);
    let tags = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(3), None]),
    ]);
    let mut attributes = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    attributes.keys().append_value("tier");
    attributes.values().append_value(1);
    attributes.append(true).unwrap();
    attributes.keys().append_value("score");
    attributes.values().append_value(5);
    attributes.append(true).unwrap();
    let attributes = attributes.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        with_semantic(Field::new("email", DataType::Utf8, false), "pii:email"),
        Field::new("payload", payload.data_type().clone(), true),
        Field::new("tags", tags.data_type().clone(), true),
        Field::new("attributes", attributes.data_type().clone(), true),
    ]));
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ok@example.test", "raw-secret"])) as ArrayRef,
            Arc::new(payload) as ArrayRef,
            Arc::new(tags) as ArrayRef,
            Arc::new(attributes) as ArrayRef,
        ],
    )
    .unwrap();

    Batch {
        header: BatchHeader::new(
            BatchId::new("batch-variant").unwrap(),
            ResourceId::new("orders").unwrap(),
            PartitionId::new("part-0").unwrap(),
            SchemaHash::new("schema-v1").unwrap(),
            record_batch.num_rows() as u64,
            record_batch.get_array_memory_size() as u64,
        ),
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    }
}

fn batch_for_partition(
    batch_id: &str,
    partition_id: &str,
    ids: Vec<i32>,
    names: Vec<&str>,
    active: Vec<bool>,
) -> Batch {
    batch_for_partition_with_schema(batch_id, partition_id, sample_schema(), ids, names, active)
}

fn batch_for_partition_with_schema(
    batch_id: &str,
    partition_id: &str,
    schema: SchemaRef,
    ids: Vec<i32>,
    names: Vec<&str>,
    active: Vec<bool>,
) -> Batch {
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(BooleanArray::from(active)) as ArrayRef,
        ],
    )
    .unwrap();

    Batch {
        header: BatchHeader::new(
            BatchId::new(batch_id).unwrap(),
            ResourceId::new("orders").unwrap(),
            PartitionId::new(partition_id).unwrap(),
            SchemaHash::new("schema-v1").unwrap(),
            record_batch.num_rows() as u64,
            record_batch.get_array_memory_size() as u64,
        ),
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    }
}
