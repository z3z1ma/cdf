use std::path::{Path, PathBuf};

use cdf_kernel::{
    CdfError, Checkpoint, PackageHash, Receipt, RunEvent, RunEventAppend, RunEventDetails,
    RunEventKind, RunEventSink, RunEventValue, RunId, ScopeKey, SegmentAck, StateDelta,
};
use cdf_package::PackageReader;
use cdf_package_contract::PackageReplayInputs;
use cdf_project::{
    PackageArtifactRecoveryRequest, PackageArtifactReplayRequest, PackageReplayReport,
    ProjectResolutionContext, ResolvedProjectDestination, RuntimeStage,
    recover_package_from_artifacts, replay_package_from_artifacts_with_stage_hook,
    resolve_project_run_destination,
};
use cdf_state_sqlite::{SqliteCheckpointStore, SqliteRunLedger};

use crate::{
    args::Cli,
    context::ProjectOperationalContext,
    destination_uri::{
        operational_destination_error_suggestions, redact_destination_uri, redact_error_value,
    },
    error_catalog,
    output::{CliError, CommandOutput},
    progress::{ProgressDelivery, human_progress_sink},
    reports::{
        PreparedReplayReportRef, ReplayPackageCliReport, RunDestinationReport, replay_event_details,
    },
    run_command::ensure_parent_directory,
};

#[derive(Clone, Copy)]
pub(crate) struct PackageReplayDestinationArgs<'a> {
    pub(crate) destination_uri: Option<&'a str>,
    pub(crate) target: Option<&'a str>,
}

pub(crate) struct ReplayDestination {
    destination: Option<ResolvedProjectDestination>,
    report: RunDestinationReport,
    receipt_source_kind: &'static str,
    secret_redaction: Option<String>,
}

pub(crate) struct PackageReplayContext {
    pub(crate) project: ProjectOperationalContext,
    pub(crate) reader: PackageReader,
    pub(crate) package_id: String,
    pub(crate) inputs: PackageReplayInputs,
}

struct ReplayProgressRecorder<'a> {
    run_ledger: &'a SqliteRunLedger,
    run_id: &'a RunId,
    event_sink: Option<&'a dyn RunEventSink>,
    package_id: String,
    package_hash: PackageHash,
    package_path: String,
    state_delta: StateDelta,
}

impl<'a> ReplayProgressRecorder<'a> {
    fn new(
        run_ledger: &'a SqliteRunLedger,
        run_id: &'a RunId,
        event_sink: Option<&'a dyn RunEventSink>,
        package: &PackageReplayContext,
        package_dir: &Path,
    ) -> Self {
        Self {
            run_ledger,
            run_id,
            event_sink,
            package_id: package.package_id.clone(),
            package_hash: package.inputs.state_delta.package_hash.clone(),
            package_path: package_dir.display().to_string(),
            state_delta: package.inputs.state_delta.clone(),
        }
    }

    fn append_stage(&self, stage: RuntimeStage<'_>) -> cdf_kernel::Result<()> {
        let Some(event) = self.event_for_stage(stage)? else {
            return Ok(());
        };
        self.append_event(event)?;
        Ok(())
    }

    fn append_event(&self, event: RunEventAppend) -> cdf_kernel::Result<RunEvent> {
        let stored = self.run_ledger.append_event(self.run_id, event)?;
        if let Some(sink) = self.event_sink {
            let _ = sink.try_emit(&stored);
        }
        Ok(stored)
    }

    fn append_run_failed(&self, error: &CdfError) -> cdf_kernel::Result<()> {
        let mut event = self.base_event(RunEventKind::RunFailed);
        event.details = RunEventDetails::new([
            ("phase", RunEventValue::String("replay".to_owned())),
            (
                "error_kind",
                RunEventValue::String(format!("{:?}", error.kind).to_ascii_lowercase()),
            ),
        ]);
        self.append_event(event)?;
        Ok(())
    }

    fn event_for_stage(
        &self,
        stage: RuntimeStage<'_>,
    ) -> cdf_kernel::Result<Option<RunEventAppend>> {
        let event = match stage {
            RuntimeStage::PackageReplayVerified => {
                let mut event = self.base_event(RunEventKind::PackageFinalized);
                event.details = state_delta_details("package", &self.state_delta)?;
                event
            }
            RuntimeStage::CheckpointProposed { delta } => {
                let mut event = self.base_event(RunEventKind::CheckpointProposed);
                event.checkpoint_id = Some(delta.checkpoint_id.clone());
                event.package_hash = Some(delta.package_hash.clone());
                event.details = state_delta_details("checkpoint", delta)?;
                event
            }
            RuntimeStage::DestinationWriteReady => return Ok(None),
            RuntimeStage::DestinationCommitStarted {
                plan_id,
                segment_count,
                bulk_path,
            } => {
                let mut event = self.base_event(RunEventKind::DestinationCommitStarted);
                event.plan_id = Some(plan_id.clone());
                event.details = RunEventDetails::new([
                    ("phase", RunEventValue::String("destination".to_owned())),
                    ("segment_count", RunEventValue::U64(segment_count)),
                    (
                        "bulk_path_id",
                        RunEventValue::String(bulk_path.descriptor.path_id.clone()),
                    ),
                    (
                        "bulk_path_version",
                        RunEventValue::U64(u64::from(bulk_path.descriptor.version)),
                    ),
                    (
                        "bulk_rows_per_batch",
                        RunEventValue::U64(bulk_path.rows_per_batch),
                    ),
                    (
                        "bulk_bytes_per_batch",
                        RunEventValue::U64(bulk_path.bytes_per_batch),
                    ),
                    (
                        "bulk_writers",
                        RunEventValue::U64(u64::from(bulk_path.writers)),
                    ),
                ]);
                event
            }
            RuntimeStage::DestinationSegmentAcknowledged { ack } => {
                let mut event = self.base_event(RunEventKind::DestinationSegmentAcknowledged);
                event.details = segment_ack_details(ack);
                event
            }
            RuntimeStage::DestinationReceiptRecorded { receipt } => {
                let mut event = self.base_event(RunEventKind::DestinationReceiptRecorded);
                event.package_hash = Some(receipt.package_hash.clone());
                event.receipt_id = Some(receipt.receipt_id.clone());
                event.destination_id = Some(receipt.destination.clone());
                event.details = receipt_details(receipt)?;
                event
            }
            RuntimeStage::CheckpointCommitted { checkpoint } => {
                let mut event = self.base_event(RunEventKind::CheckpointCommitted);
                event.checkpoint_id = Some(checkpoint.delta.checkpoint_id.clone());
                event.package_hash = Some(checkpoint.delta.package_hash.clone());
                event.receipt_id = checkpoint
                    .receipt
                    .as_ref()
                    .map(|receipt| receipt.receipt_id.clone());
                event.details = checkpoint_details(checkpoint)?;
                event
            }
            RuntimeStage::PackageStatusUpdated { status } => {
                let mut event = self.base_event(RunEventKind::PackageStatusUpdated);
                event.details = RunEventDetails::new([
                    ("phase", RunEventValue::String("package".to_owned())),
                    ("status", RunEventValue::String(status.as_str().to_owned())),
                ]);
                event
            }
        };
        Ok(Some(event))
    }

    fn base_event(&self, kind: RunEventKind) -> RunEventAppend {
        let mut event = RunEventAppend::new(kind);
        event.resource_id = Some(self.state_delta.resource_id.clone());
        event.scope = Some(self.state_delta.scope.clone());
        event.partition_id = partition_id_for_scope(&self.state_delta.scope);
        event.package_id = Some(self.package_id.clone());
        event.package_hash = Some(self.package_hash.clone());
        event.package_path = Some(self.package_path.clone());
        event.checkpoint_id = Some(self.state_delta.checkpoint_id.clone());
        event
    }
}

impl ReplayDestination {
    pub(crate) fn bind_execution_services(
        &mut self,
        execution: cdf_runtime::ExecutionServices,
    ) -> Result<(), CliError> {
        self.destination
            .as_mut()
            .ok_or_else(|| CdfError::internal("replay destination was already consumed"))?
            .bind_execution_services(execution)?;
        Ok(())
    }

    fn replay(
        &mut self,
        package_dir: PathBuf,
        store: &SqliteCheckpointStore,
        progress: &ReplayProgressRecorder<'_>,
    ) -> Result<PackageReplayReport, CliError> {
        let destination = self
            .destination
            .take()
            .ok_or_else(|| CdfError::internal("replay destination was already consumed"))?;
        let stage_hook = |stage: RuntimeStage<'_>| progress.append_stage(stage);
        let result = replay_package_from_artifacts_with_stage_hook(
            PackageArtifactReplayRequest {
                package_dir,
                destination,
                checkpoint_store: store,
                after_receipt_verified: None,
            },
            Some(&stage_hook),
        )
        .map_err(|error| redact_error_value(error, self.secret_redaction.as_deref()));
        match result {
            Ok(report) => Ok(report),
            Err(error) => {
                let _ = progress.append_run_failed(&CdfError {
                    kind: error.kind.clone(),
                    message: error.message.clone(),
                    code: error.code.clone(),
                    retry_after_ms: None,
                });
                Err(error.into())
            }
        }
    }

    pub(crate) fn recover(
        &mut self,
        package_dir: PathBuf,
        store: &SqliteCheckpointStore,
        receipt: Receipt,
    ) -> Result<PackageReplayReport, CliError> {
        let destination = self
            .destination
            .take()
            .ok_or_else(|| CdfError::internal("replay destination was already consumed"))?;
        recover_package_from_artifacts(PackageArtifactRecoveryRequest {
            package_dir,
            destination,
            checkpoint_store: store,
            receipt,
            after_receipt_verified: None,
        })
        .map_err(|error| redact_error_value(error, self.secret_redaction.as_deref()).into())
    }

    pub(crate) fn report(&self) -> &RunDestinationReport {
        &self.report
    }

    pub(crate) fn receipt_source_kind(&self) -> &'static str {
        self.receipt_source_kind
    }
}

fn partition_id_for_scope(scope: &ScopeKey) -> Option<cdf_kernel::PartitionId> {
    match scope {
        ScopeKey::Partition { partition_id } => Some(partition_id.clone()),
        _ => None,
    }
}

fn state_delta_details(phase: &str, delta: &StateDelta) -> cdf_kernel::Result<RunEventDetails> {
    let mut details = phase_details(phase);
    details.insert(
        "segment_count".to_owned(),
        RunEventValue::U64(u64_from_usize(delta.segments.len())?),
    );
    details.insert(
        "row_count".to_owned(),
        RunEventValue::U64(delta.segments.iter().map(|segment| segment.row_count).sum()),
    );
    details.insert(
        "byte_count".to_owned(),
        RunEventValue::U64(
            delta
                .segments
                .iter()
                .map(|segment| segment.byte_count)
                .sum(),
        ),
    );
    Ok(RunEventDetails::new(details))
}

fn segment_ack_details(ack: &SegmentAck) -> RunEventDetails {
    let mut details = phase_details("destination");
    details.insert(
        "segment_id".to_owned(),
        RunEventValue::String(ack.segment_id.as_str().to_owned()),
    );
    details.insert("row_count".to_owned(), RunEventValue::U64(ack.row_count));
    details.insert("byte_count".to_owned(), RunEventValue::U64(ack.byte_count));
    RunEventDetails::new(details)
}

fn receipt_details(receipt: &Receipt) -> cdf_kernel::Result<RunEventDetails> {
    let mut details = phase_details("destination");
    details.insert(
        "segment_ack_count".to_owned(),
        RunEventValue::U64(u64_from_usize(receipt.segment_acks.len())?),
    );
    match &receipt.counts {
        cdf_kernel::CommitCounts::Rows {
            rows_written,
            rows_inserted,
            rows_updated,
            rows_deleted,
        } => {
            details.insert("rows_written".to_owned(), RunEventValue::U64(*rows_written));
            for (name, value) in [
                ("rows_inserted", rows_inserted),
                ("rows_updated", rows_updated),
                ("rows_deleted", rows_deleted),
            ] {
                if let Some(value) = value {
                    details.insert(name.to_owned(), RunEventValue::U64(*value));
                }
            }
        }
        cdf_kernel::CommitCounts::KeyedChanges {
            intent,
            rows_inserted,
            rows_updated,
            hard_deletes,
            soft_deletes,
            missing_delete_keys,
            ignored_deletes,
        } => {
            details.insert(
                "upsert_intent".to_owned(),
                RunEventValue::U64(intent.upserts),
            );
            details.insert(
                "delete_intent".to_owned(),
                RunEventValue::U64(intent.deletes),
            );
            for (name, value) in [
                ("rows_inserted", rows_inserted),
                ("rows_updated", rows_updated),
                ("hard_deletes", hard_deletes),
                ("soft_deletes", soft_deletes),
                ("missing_delete_keys", missing_delete_keys),
                ("ignored_deletes", ignored_deletes),
            ] {
                if let Some(value) = value {
                    details.insert(name.to_owned(), RunEventValue::U64(*value));
                }
            }
        }
        cdf_kernel::CommitCounts::Routed { targets } => {
            details.insert(
                "routed_target_count".to_owned(),
                RunEventValue::U64(u64_from_usize(targets.len())?),
            );
            if let Some(settled) = receipt.counts.settled_effect_count() {
                details.insert(
                    "settled_effect_count".to_owned(),
                    RunEventValue::U64(settled),
                );
            }
        }
    }
    details.insert(
        "migration_count".to_owned(),
        RunEventValue::U64(u64_from_usize(receipt.migrations.len())?),
    );
    Ok(RunEventDetails::new(details))
}

fn checkpoint_details(checkpoint: &Checkpoint) -> cdf_kernel::Result<RunEventDetails> {
    let mut details = state_delta_details("checkpoint", &checkpoint.delta)?.attributes;
    details.insert(
        "status".to_owned(),
        RunEventValue::String(checkpoint.status.as_str().to_owned()),
    );
    Ok(RunEventDetails::new(details))
}

fn phase_details(phase: &str) -> std::collections::BTreeMap<String, RunEventValue> {
    std::collections::BTreeMap::from([(
        "phase".to_owned(),
        RunEventValue::String(phase.to_owned()),
    )])
}

fn u64_from_usize(value: usize) -> cdf_kernel::Result<u64> {
    u64::try_from(value).map_err(|error| CdfError::internal(error.to_string()))
}

pub(crate) fn load_package_replay_context(
    cli: &Cli,
    package_dir: &Path,
) -> Result<PackageReplayContext, CliError> {
    let project = ProjectOperationalContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let reader = PackageReader::open(package_dir)?;
    let package_id = reader.manifest().identity.package_id.clone();
    let inputs = reader.replay_inputs()?;
    Ok(PackageReplayContext {
        project,
        reader,
        package_id,
        inputs,
    })
}

fn replay_report_ref(report: &PackageReplayReport) -> PreparedReplayReportRef<'_> {
    PreparedReplayReportRef {
        checkpoint: &report.checkpoint,
        receipt: &report.receipt,
        receipt_source: report.receipt_source.clone(),
        package_status: &report.package_status,
        phase_metrics: &report.phase_metrics,
    }
}

pub(crate) fn build_replay_destination(
    registry: &cdf_runtime::DestinationRegistry,
    context: &ProjectOperationalContext,
    args: PackageReplayDestinationArgs<'_>,
    inputs: &PackageReplayInputs,
    execution: &cdf_runtime::ExecutionServices,
) -> Result<ReplayDestination, CliError> {
    let uri = args
        .destination_uri
        .unwrap_or(context.environment.destination.as_str());
    let secret_provider = context.secret_provider();
    let inspection_context =
        cdf_runtime::DestinationResolutionContext::for_project_inspection(&context.root)
            .with_environment_name(&context.environment.name)
            .with_destination_policy(&context.environment.destination_policy);
    let inspection = registry
        .inspect(uri, &inspection_context)
        .map_err(|error| {
            replay_destination_resolution_error(context, args.destination_uri, error, uri)
        })?;
    let display_name = destination_display_name(&inspection.description.label);
    if inspection.runtime.replay_requires_explicit_target && args.target.is_none() {
        let target_hint = inspection
            .runtime
            .replay_target_hint
            .as_deref()
            .unwrap_or("TARGET");
        return Err(CliError::usage_with(
            format!("run --package to {display_name} requires --target {target_hint}"),
            error_catalog::RUN_PACKAGE_ARGUMENT,
        ));
    }
    let target = if inspection.runtime.replay_requires_explicit_target {
        let target_hint = inspection
            .runtime
            .replay_target_hint
            .as_deref()
            .unwrap_or("TARGET");
        let explicit = args.target.ok_or_else(|| {
            CliError::usage_with(
                format!("run --package to {display_name} requires --target {target_hint}"),
                error_catalog::RUN_PACKAGE_ARGUMENT,
            )
        })?;
        let target = registry.replay_target(uri, explicit).map_err(|_| {
            CliError::usage_with(
                format!("run --package to {display_name} requires --target {target_hint}"),
                error_catalog::RUN_PACKAGE_ARGUMENT,
            )
        })?;
        if target != inputs.destination_commit.target {
            return Err(CliError::mapped(
                CdfError::contract(format!(
                    "explicit {display_name} package target {target} does not match package destination commit target {}",
                    inputs.destination_commit.target
                )),
                error_catalog::RUN_PACKAGE_CONTRACT,
            ));
        }
        target
    } else {
        inputs.destination_commit.target.clone()
    };
    let destination_context = ProjectResolutionContext::for_project_run(&context.root, &target)
        .with_environment_name(&context.environment.name)
        .with_destination_policy(&context.environment.destination_policy)
        .with_secret_provider(&secret_provider)
        .with_execution_services(execution);
    let destination = resolve_project_run_destination(registry, uri, &destination_context)
        .map_err(|error| {
            replay_destination_resolution_error(context, args.destination_uri, error, uri)
        })?;
    let description = destination.describe();
    let report = RunDestinationReport::from_project(&description, destination.target());
    let secret_redaction = destination.secret_redaction().map(str::to_owned);
    Ok(ReplayDestination {
        destination: Some(destination),
        report,
        receipt_source_kind: description.product_receipt_source,
        secret_redaction,
    })
}

fn destination_display_name(label: &str) -> String {
    let mut chars = label.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => "destination".to_owned(),
    }
}

fn replay_destination_resolution_error(
    context: &ProjectOperationalContext,
    requested_destination: Option<&str>,
    error: CdfError,
    uri: &str,
) -> CliError {
    let error = redact_error_value(error, None);
    if error.message.contains("no destination driver registered") {
        CliError::not_supported_with(
            "run --package",
            format!(
                "destination URI `{}` is unsupported for package execution; supported destinations are duckdb://path, parquet://root, and postgres://...",
                redact_destination_uri(uri)
            ),
            "registered project destination driver",
            error_catalog::DESTINATION_NOT_SUPPORTED,
        )
        .with_suggestions(operational_destination_error_suggestions(
            context,
            requested_destination,
        ))
    } else if error.message.contains("malformed or non-local")
        || error.message.contains("is missing a scheme")
    {
        CliError::not_supported_with(
            "run --package",
            error.message,
            "registered project destination driver",
            error_catalog::DESTINATION_NOT_SUPPORTED,
        )
        .with_suggestions(operational_destination_error_suggestions(
            context,
            requested_destination,
        ))
    } else {
        error.into()
    }
}

pub(crate) fn run_package(
    cli: &Cli,
    package_dir: PathBuf,
    destination_uri: String,
    target: Option<String>,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
    progress_delivery: ProgressDelivery,
) -> Result<CommandOutput, CliError> {
    let package = load_package_replay_context(cli, &package_dir)?;
    let execution = package
        .project
        .execution_with_state_authorities(execution)?;
    let execution = package
        .project
        .execution_with_package_schema_authority(&execution, &package.inputs)?;
    let mut replay_destination = build_replay_destination(
        destinations,
        &package.project,
        PackageReplayDestinationArgs {
            destination_uri: Some(&destination_uri),
            target: target.as_deref(),
        },
        &package.inputs,
        &execution,
    )?;
    replay_destination.bind_execution_services(execution)?;
    let package_hash = package.inputs.state_delta.package_hash.clone();
    let state_store_path = package.project.state_store_path()?;
    let state_store_path_ownership = package.project.state_store_path_ownership();
    ensure_parent_directory(&state_store_path, state_store_path_ownership)?;
    let run_ledger =
        SqliteRunLedger::open_with_path_ownership(&state_store_path, state_store_path_ownership)?;
    let run = run_ledger.create_run(None)?;
    let store = package.project.state_store()?;
    let progress = human_progress_sink(cli.json, &cli.terminal, progress_delivery);
    let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);
    let progress_recorder =
        ReplayProgressRecorder::new(&run_ledger, &run.run_id, event_sink, &package, &package_dir);

    let replay_report =
        match replay_destination.replay(package_dir.clone(), &store, &progress_recorder) {
            Ok(report) => report,
            Err(error) => {
                let error = match progress {
                    Some(progress) => error.with_progress(progress.finish()),
                    None => error,
                };
                return Err(error);
            }
        };
    let report = replay_report_ref(&replay_report);

    let receipt_source = report.receipt_source.clone();
    let mut event = RunEventAppend::new(RunEventKind::ReplayRecorded);
    event.resource_id = Some(package.inputs.state_delta.resource_id.clone());
    event.scope = Some(package.inputs.state_delta.scope.clone());
    event.package_id = Some(package.package_id.clone());
    event.package_hash = Some(package_hash.clone());
    event.package_path = Some(package_dir.display().to_string());
    event.checkpoint_id = Some(report.checkpoint.delta.checkpoint_id.clone());
    event.receipt_id = Some(report.receipt.receipt_id.clone());
    event.destination_id = Some(report.receipt.destination.clone());
    event.details = replay_event_details(
        &receipt_source,
        replay_destination.receipt_source_kind(),
        report.package_status.as_str(),
    );
    progress_recorder.append_event(event)?;
    let ledger_snapshot = run_ledger
        .snapshot(&run.run_id)?
        .ok_or_else(|| CdfError::internal("created replay run is absent from run ledger"))?;

    let destination_report = replay_destination
        .report
        .with_receipt_destination(report.receipt.destination.to_string());
    let cli_report = ReplayPackageCliReport::from_report(
        run.run_id.to_string(),
        package.package_id,
        package_dir,
        report,
        receipt_source,
        destination_report,
        &ledger_snapshot,
    );
    let document = cli_report.render_document();
    match progress {
        Some(progress) => {
            CommandOutput::rendered_with_progress("run", document, cli_report, progress.finish())
        }
        None => CommandOutput::rendered("run", document, cli_report),
    }
}
