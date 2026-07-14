use std::{cell::Cell, sync::Arc};

use cdf_declarative::{RestResource, SqlResource};
use cdf_dest_parquet::ParquetDestination;
use cdf_kernel::{
    CdfError, DestinationProtocol, PipelineId, QueryableResource, Result, RunId, SourcePosition,
};
use cdf_package::PackageReader;
use cdf_project::{ProjectRunReport, ProjectRunRequest, ProjectRunSource, run_project};

use super::local_postgres::LivePostgres;
use super::{
    ExcludedMatrixCell, ExecutedMatrixCell, MatrixDestination, MatrixDisposition, RunMatrixCell,
    SourceArchetype,
    assertions::{
        assert_artifact_replay_identity, assert_committed_checkpoint, assert_duplicate_replay_noop,
        assert_plan_honesty, assert_replay_inputs_match_run, assert_run_report, receipt_gate,
    },
    destinations::{MatrixDestinationHandle, target_for_cell},
    file_fixture, plan_json, python_fixture, rest_fixture, sql_fixture,
    test_support::RecordingTransport,
};

pub(crate) const ROW_COUNT: u64 = 2;
pub(crate) const SEGMENT_COUNT: usize = 1;

pub(crate) fn execute_cell(
    cell: RunMatrixCell,
    postgres: &LivePostgres,
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

    let source = MatrixSource::new(cell, temp.path(), postgres)?;
    let target = target_for_cell(cell, postgres)?;
    let destination =
        MatrixDestinationHandle::new(cell.destination, temp.path(), target, postgres)?;
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

    let report = futures_executor::block_on(run_project(ProjectRunRequest {
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
    }))?;

    assert!(
        gate_observed.get(),
        "receipt verification gate hook must run"
    );
    assert_run_report(cell, &report, &resource_id, &scope, &pipeline_id);
    PackageReader::open(&report.package_dir)?.verify()?;
    assert_replay_inputs_match_run(cell, &report);
    destination.verify_trait_receipt(&report.receipt)?;
    assert_committed_checkpoint(&state_store_path, &report);
    source.assert_after_run(&report);

    let duplicate_behavior =
        assert_duplicate_replay_noop(cell, &destination, &report, temp.path())?;
    assert_artifact_replay_identity(cell, &destination, &report, temp.path())?;

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
    })
}

pub(crate) fn sheet_exclusion_reason(cell: &RunMatrixCell) -> Option<String> {
    if cell.destination != MatrixDestination::ParquetFilesystem
        || cell.disposition != MatrixDisposition::Merge
    {
        return None;
    }
    let temp = tempfile::tempdir().unwrap();
    let destination =
        ParquetDestination::new_filesystem(temp.path(), crate::test_execution_services()).unwrap();
    assert!(
        !destination
            .sheet()
            .supported_dispositions
            .contains(&cell.disposition.to_write_disposition())
    );
    Some(
        "Parquet destination sheet supported_dispositions=[append, replace]; merge is not listed"
            .to_owned(),
    )
}

pub(crate) fn executed_for_source<'a>(
    output: &'a [ExecutedMatrixCell],
    source: SourceArchetype,
) -> impl Iterator<Item = &'a ExecutedMatrixCell> + 'a {
    output
        .iter()
        .filter(move |executed| executed.cell.source_archetype == source)
}

pub(crate) fn excluded_for_source<'a>(
    output: &'a [ExcludedMatrixCell],
    source: SourceArchetype,
) -> impl Iterator<Item = &'a ExcludedMatrixCell> + 'a {
    output
        .iter()
        .filter(move |excluded| excluded.cell.source_archetype == source)
}

enum MatrixSource {
    File(Arc<dyn QueryableResource>),
    Python(Box<cdf_python::PythonResource>),
    Rest {
        resource: Box<RestResource>,
        transport: RecordingTransport,
    },
    Sql(Box<SqlResource>),
}

impl MatrixSource {
    fn new(
        cell: RunMatrixCell,
        project_root: &std::path::Path,
        postgres: &LivePostgres,
    ) -> Result<Self> {
        match cell.source_archetype {
            SourceArchetype::File => {
                let compiled = file_fixture::resource(project_root, cell.disposition)?;
                Ok(Self::File(crate::source_fixture::resolve_local_file(
                    &compiled,
                    project_root,
                )?))
            }
            SourceArchetype::Python => Ok(Self::Python(Box::new(python_fixture::resource(
                project_root,
                cell.disposition,
            )?))),
            SourceArchetype::Rest => {
                let (resource, transport) = rest_fixture::resource(cell.disposition)?;
                Ok(Self::Rest {
                    resource: Box::new(resource),
                    transport,
                })
            }
            SourceArchetype::Sql => Ok(Self::Sql(Box::new(sql_fixture::resource(cell, postgres)?))),
        }
    }

    fn queryable(&self) -> &dyn QueryableResource {
        match self {
            Self::File(resource) => resource.as_ref(),
            Self::Python(resource) => resource.as_ref(),
            Self::Rest { resource, .. } => resource.as_ref(),
            Self::Sql(resource) => resource.compiled(),
        }
    }

    fn engine_plan(
        &self,
        package_id: &str,
        disposition: MatrixDisposition,
        identifier_policy: Option<&cdf_contract::IdentifierPolicy>,
    ) -> Result<cdf_engine::EnginePlan> {
        match self {
            Self::File(resource) => plan_json::file_engine_plan(
                resource.as_ref(),
                package_id,
                disposition,
                identifier_policy,
            ),
            Self::Python(resource) => {
                plan_json::planned_engine_plan(resource.as_ref(), package_id, identifier_policy)
            }
            Self::Rest { resource, .. } => {
                plan_json::planned_engine_plan(resource.as_ref(), package_id, identifier_policy)
            }
            Self::Sql(resource) => {
                plan_json::planned_engine_plan(resource.as_ref(), package_id, identifier_policy)
            }
        }
    }

    fn project_run_source(&self) -> ProjectRunSource<'_> {
        match self {
            Self::File(resource) => ProjectRunSource::new(resource.as_ref()),
            Self::Python(resource) => ProjectRunSource::new(resource.as_ref()),
            Self::Rest { resource, .. } => ProjectRunSource::rest(resource.as_ref()),
            Self::Sql(resource) => ProjectRunSource::sql(resource.as_ref()),
        }
    }

    fn assert_after_run(&self, report: &ProjectRunReport) {
        assert_segment_positions_match_checkpoint(report);
        assert_checkpoint_head_contains_source_position(report);
        match self {
            Self::File(_) => file_fixture::assert_source_position(report),
            Self::Python(_) => python_fixture::assert_source_position(report),
            Self::Rest { transport, .. } => {
                rest_fixture::assert_runtime_observed(transport);
                rest_fixture::assert_source_position(report);
            }
            Self::Sql(_) => sql_fixture::assert_source_position(report),
        }
    }
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
