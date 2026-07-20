use std::cell::Cell;

use cdf_kernel::{CdfError, PipelineId, Result, RunId, SourcePosition};
use cdf_package::PackageReader;
use cdf_project::{ProjectRunReport, ProjectRunRequest, run_project};

use super::{
    ExcludedMatrixCell, ExecutedMatrixCell, RunMatrixCell, SourceArchetype,
    assertions::{
        assert_artifact_replay_identity, assert_committed_checkpoint, assert_duplicate_replay_noop,
        assert_plan_honesty, assert_replay_inputs_match_run, assert_run_report, receipt_gate,
    },
    destinations::{ConformanceEnvironment, destination_for_cell},
    source_catalog,
};

pub(crate) const ROW_COUNT: u64 = 2;
pub(crate) const SEGMENT_COUNT: u64 = 1;

pub(crate) fn execute_cell(
    cell: RunMatrixCell,
    environment: &ConformanceEnvironment,
) -> Result<ExecutedMatrixCell> {
    let temp = tempfile::tempdir()
        .map_err(|error| CdfError::data(format!("create run matrix tempdir: {error}")))?;
    let package_id = format!(
        "run-matrix-{}-{}-{}",
        cell.source_archetype.as_str(),
        cell.destination.as_str(),
        cell.disposition.as_str()
    );
    let checkpoint_id = cdf_kernel::CheckpointId::new(format!("checkpoint-{package_id}"))?;
    let pipeline_id = PipelineId::new(format!("pipeline-{package_id}"))?;
    let run_id = RunId::new(format!("run-{package_id}"))?;
    let package_root = temp.path().join(".cdf/packages");
    let state_store_path = temp.path().join(".cdf/state.sqlite");

    let source = source_catalog::prepare(&cell, temp.path(), environment)?;
    let destination = destination_for_cell(&cell, temp.path(), environment)?;
    let resolved_destination = destination.resolved()?;
    let identifier_policy = resolved_destination.column_identifier_policy()?;
    let plan = source.engine_plan(&package_id, cell.disposition, identifier_policy.as_ref())?;
    assert_plan_honesty(&plan, source.queryable(), &package_id);

    let gate_observed = Cell::new(false);
    let resource_id = source.queryable().descriptor().resource_id.clone();
    let scope = source.queryable().descriptor().state_scope.clone();
    let hook = receipt_gate(
        &state_store_path,
        &pipeline_id,
        &resource_id,
        &scope,
        &gate_observed,
    );
    let services = source.execution().clone();

    let report = futures_executor::block_on(run_project(
        ProjectRunRequest {
            resource: source.project_run_source(),
            plan,
            package_root,
            state_store_path: state_store_path.clone(),
            pipeline_id: pipeline_id.clone(),
            package_id: package_id.clone(),
            checkpoint_id: checkpoint_id.clone(),
            destination: resolved_destination,
            run_id: Some(run_id),
            event_sink: None,
            after_receipt_verified: Some(&hook),
        },
        &services,
    ))?
    .into_committed()?;

    assert!(
        gate_observed.get(),
        "receipt verification gate hook must run"
    );
    assert_run_report(cell.clone(), &report, &resource_id, &scope, &pipeline_id);
    PackageReader::open(&report.package_dir)?.verify()?;
    assert_replay_inputs_match_run(cell.clone(), &report);
    destination.assert_receipt_identity(&report.receipt)?;
    destination.verify_trait_receipt(&report.receipt)?;
    assert_committed_checkpoint(&state_store_path, &report);
    assert_segment_positions_match_checkpoint(&report);
    assert_checkpoint_head_contains_source_position(&report);
    source.assert_after_run(&report);

    let duplicate_behavior =
        assert_duplicate_replay_noop(cell.clone(), &destination, &report, temp.path())?;
    assert_artifact_replay_identity(cell.clone(), &destination, &report, temp.path())?;

    Ok(ExecutedMatrixCell {
        cell,
        package_id,
        checkpoint_id: checkpoint_id.as_str().to_owned(),
        receipt_id: report.receipt.receipt_id.as_str().to_owned(),
        row_count: report.row_count,
        plan_honesty_asserted: true,
        package_verified: true,
        destination_receipt_verified: true,
        checkpoint_gated_after_receipt_verification: true,
        artifact_replay_identity_asserted: true,
        duplicate_behavior,
        runtime_scheduler: report.runtime_scheduler,
    })
}

pub(crate) fn sheet_exclusion_reason(
    cell: &RunMatrixCell,
    environment: &ConformanceEnvironment,
) -> Result<Option<String>> {
    let temp = tempfile::tempdir()
        .map_err(|error| CdfError::data(format!("create exclusion tempdir: {error}")))?;
    let destination = destination_for_cell(cell, temp.path(), environment)?;
    let supported = destination.supported_dispositions()?;
    if supported.contains(&cell.disposition.to_write_disposition()) {
        return Ok(None);
    }
    Ok(Some(format!(
        "destination sheet supported_dispositions={supported:?}; {:?} is not listed",
        cell.disposition.to_write_disposition()
    )))
}

pub(crate) fn executed_for_source<'a>(
    output: &'a [ExecutedMatrixCell],
    source: &'a SourceArchetype,
) -> impl Iterator<Item = &'a ExecutedMatrixCell> + 'a {
    output
        .iter()
        .filter(move |executed| &executed.cell.source_archetype == source)
}

pub(crate) fn excluded_for_source<'a>(
    output: &'a [ExcludedMatrixCell],
    source: &'a SourceArchetype,
) -> impl Iterator<Item = &'a ExcludedMatrixCell> + 'a {
    output
        .iter()
        .filter(move |excluded| &excluded.cell.source_archetype == source)
}

fn assert_segment_positions_match_checkpoint(report: &ProjectRunReport) {
    assert!(!report.checkpoint.delta.segments.is_empty());
    for segment in &report.checkpoint.delta.segments {
        assert_eq!(segment.scope, report.checkpoint.delta.scope);
        assert_eq!(
            segment.output_position,
            report.checkpoint.delta.output_position
        );
    }
}

fn assert_checkpoint_head_contains_source_position(report: &ProjectRunReport) {
    match &report.checkpoint.delta.output_position {
        SourcePosition::FileManifest(_) | SourcePosition::Cursor(_) => {}
        position => panic!("unexpected run matrix source position: {position:?}"),
    }
}
