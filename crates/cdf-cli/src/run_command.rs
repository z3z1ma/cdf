use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_schema::Schema;
use cdf_declarative::CompiledResource;
use cdf_kernel::{
    CdfError, CheckpointId, CursorOrderingClaim, CursorSpec, PipelineId, PushdownFidelity,
    ResourceDescriptor, ResourceId, RunEventSink, SchemaSource, ScopeKey, TargetName, TrustLevel,
    WriteDisposition,
};
use cdf_project::{
    LOCK_FILE_NAME, ProjectFileExpectation, ProjectFileWrite, ProjectRunOutcome, ProjectRunRequest,
    RunTelemetryConfig, SchemaSnapshotStore, publish_project_files_transactionally,
    run_project_with_scheduler_and_telemetry,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    add_command::registered_source_resource_sql,
    args::{Cli, RunArgs, ScanArgs},
    context::ProjectContext,
    destination_uri::{
        destination_error_suggestions, redact_error_value,
        resolve_selected_destination_with_services,
    },
    error_catalog,
    output::{CliError, CommandOutput, HumanOutput},
    progress::{ProgressDelivery, human_progress_sink},
    project_run_resource::{PreparedRuntimeResourceForCli, prepare_runtime_resource_for_cli},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, StatusKind, StatusLine},
    },
    reports::{
        AdhocRunReport, RunCliReport, RunDestinationReport, RunMemoryReport, RunNoOpCliReport,
    },
    scan_command::{build_engine_plan_for_resource, planning_frontier},
};

pub(crate) const DEFAULT_RUN_PIPELINE_ID: &str = "cdf-run";
static ADHOC_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn run(
    cli: &Cli,
    args: RunArgs,
    host: &cdf_engine::StandaloneExecutionHost,
    services: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
    progress_delivery: ProgressDelivery,
) -> Result<CommandOutput, CliError> {
    if args.loop_mode {
        return Err(CliError::not_supported_with(
            "run --loop",
            "the local development loop supervisor is excluded from this explicit one-package run slice",
            "later loop/streaming supervisor",
            error_catalog::RUN_LOOP_NOT_SUPPORTED,
        ));
    }
    if args.selectors.is_empty() {
        return Err(CliError::usage_with(
            "run requires at least one resource selector",
            error_catalog::RUN_ARGUMENT,
        ));
    }
    if args.selectors.len() == 1 && looks_like_adhoc_location(&args.selectors[0])? {
        if !args.exclude.is_empty() {
            return Err(CliError::usage_with(
                "run --exclude cannot be combined with an ad-hoc location",
                error_catalog::RUN_ARGUMENT,
            ));
        }
        let prepared = prepare_single(
            cli,
            args.selectors[0].clone(),
            args,
            services,
            destinations,
            true,
            true,
        )?;
        return execute_prepared(cli, prepared, host, progress_delivery);
    }
    let (root, _) = crate::context::project_location(cli.project.as_ref())?;
    let selection =
        cdf_project::resolve_project_resource_selection(&root, &args.selectors, &args.exclude)
            .map_err(crate::compile_command::resource_selection_error)?;
    let mut outcomes = Vec::with_capacity(selection.resources.len());
    let mut prepared_runs = Vec::with_capacity(selection.resources.len());
    for selected in &selection.resources {
        let resource_id = selected.resource_id.to_string();
        let prepared = if args.locked {
            crate::compile_command::prepare_selected_resource(
                cli,
                selected,
                true,
                destinations,
                services,
            )
            .and_then(|()| {
                prepare_single(
                    cli,
                    resource_id.clone(),
                    args.clone(),
                    services,
                    destinations,
                    false,
                    false,
                )
            })
        } else {
            prepare_single(
                cli,
                resource_id.clone(),
                args.clone(),
                services,
                destinations,
                false,
                true,
            )
            .and_then(|prepared| {
                crate::compile_command::prepare_selected_resource(
                    cli,
                    selected,
                    false,
                    destinations,
                    services,
                )
                .map(|()| prepared)
            })
        };
        match prepared {
            Ok(prepared) => prepared_runs.push((resource_id, prepared)),
            Err(error) => outcomes.push(RunResourceOutcome::PreparationFailed {
                resource_id,
                error: RunResourceError::from(error),
            }),
        }
    }
    if !outcomes.is_empty() {
        outcomes.extend(
            prepared_runs
                .into_iter()
                .map(|(resource_id, _)| RunResourceOutcome::PreparationBlocked { resource_id }),
        );
        outcomes.sort_by(|left, right| left.resource_id().cmp(right.resource_id()));
        return run_batch_output(selection.selection, outcomes, Vec::new(), None);
    }

    let mut documents = Vec::with_capacity(prepared_runs.len());
    let mut buffered_progress = None;
    for (resource_id, prepared) in prepared_runs {
        match execute_prepared(cli, prepared, host, progress_delivery) {
            Ok(output) => {
                let CommandOutput { human, json, .. } = output;
                documents.push(match human {
                    HumanOutput::Rendered(document) => document,
                    HumanOutput::RenderedWithProgress { progress, document } => {
                        buffered_progress = Some(progress);
                        document
                    }
                });
                outcomes.push(RunResourceOutcome::Completed {
                    resource_id,
                    result: json,
                });
            }
            Err(error) => outcomes.push(RunResourceOutcome::Failed {
                resource_id,
                error: RunResourceError::from(error),
            }),
        }
    }
    run_batch_output(selection.selection, outcomes, documents, buffered_progress)
}

struct PreparedRun {
    context: ProjectContext,
    explicit: ResolvedRunArgs,
    run_services: cdf_runtime::ExecutionServices,
    prepared: PreparedRuntimeResourceForCli,
    state_store_path: std::path::PathBuf,
    destination: cdf_project::ResolvedProjectDestination,
    secret_redaction: Option<String>,
    plan: cdf_engine::EnginePlan,
    scheduler: cdf_runtime::RuntimeSchedulerResolution,
    destination_report: RunDestinationReport,
    adhoc: Option<AdhocRunReport>,
    explain_memory: bool,
}

fn prepare_single(
    cli: &Cli,
    requested: String,
    args: RunArgs,
    services: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
    adhoc_mode: bool,
    commit_schema: bool,
) -> Result<PreparedRun, CliError> {
    let explain_memory = args.explain_memory;
    let mut context = if adhoc_mode {
        ProjectContext::load_for_command_with_destination_registry(
            "run",
            cli.project.as_ref(),
            cli.env.as_deref(),
            true,
            destinations,
        )?
    } else {
        ProjectContext::load_selected_for_mutation(
            cli.project.as_ref(),
            cli.env.as_deref(),
            &requested,
            destinations,
        )?
    };
    let adhoc = if context.has_resource(&requested) {
        None
    } else if looks_like_adhoc_location(&requested)? {
        if args.destination_uri.is_none() {
            return Err(CliError::usage_with(
                "cdf run ad-hoc mode requires an explicit `--to <destination>`",
                error_catalog::RUN_ARGUMENT,
            ));
        }
        let synthesized = synthesize_adhoc_source(&mut context, &requested)?;
        Some(synthesized.report)
    } else {
        None
    };
    let resource_id = adhoc
        .as_ref()
        .map(|report| report.resource_id.clone())
        .unwrap_or(requested);
    let explicit = resolved_run_args(&context, resource_id, args)?;
    let host_jobs = services.capabilities().logical_cpu_slots;
    let provisional_jobs = explicit.jobs.unwrap_or(host_jobs).min(host_jobs);
    let run_services = services
        .with_run_job_ceiling(provisional_jobs)?
        .with_scheduler_measurement(true)?;
    let prepared = prepare_runtime_resource_for_cli(
        destinations,
        &context,
        &explicit.resource_id,
        commit_schema,
        Some(&run_services),
    )?;
    let state_store_path = context.state_store_path()?;
    let committed_frontier = planning_frontier(
        &context,
        prepared.resource.as_queryable().descriptor(),
        &explicit.pipeline_id,
    )?;
    let resolved = resolve_selected_destination_with_services(
        destinations,
        &context,
        &explicit.target,
        explicit.destination_uri.as_deref(),
        Some(&run_services),
    )
    .map_err(|error| {
        run_destination_resolution_error(&context, explicit.destination_uri.as_deref(), error)
    })?;
    let identifier_policy = resolved.destination.column_identifier_policy()?;
    let plan = build_engine_plan_for_resource(
        &prepared.resource,
        &ScanArgs {
            resource_id: explicit.resource_id.clone(),
            destination_uri: None,
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            segmentation: explicit.segmentation.clone(),
        },
        Some(&explicit.package_id),
        committed_frontier,
        identifier_policy.as_ref(),
        &resolved.destination.runtime_capabilities(),
    )?;
    let destination = resolved.destination;
    let source = prepared.resource.source_plan();
    let partition_count = plan.scan.partition_count()?;
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        partition_count,
        &source.execution_capabilities,
        &destination.runtime_capabilities(),
        &run_services,
        explicit.jobs,
    )?;
    run_services.tighten_run_job_ceiling(scheduler.effective_jobs.jobs)?;
    let destination_report =
        RunDestinationReport::from_project(&destination.describe(), destination.target());
    Ok(PreparedRun {
        context,
        explicit,
        run_services,
        prepared,
        state_store_path,
        destination,
        secret_redaction: resolved.secret_redaction,
        plan,
        scheduler,
        destination_report,
        adhoc,
        explain_memory,
    })
}

fn execute_prepared(
    cli: &Cli,
    prepared_run: PreparedRun,
    host: &cdf_engine::StandaloneExecutionHost,
    progress_delivery: ProgressDelivery,
) -> Result<CommandOutput, CliError> {
    let PreparedRun {
        context,
        explicit,
        run_services,
        prepared,
        state_store_path,
        destination,
        secret_redaction,
        plan,
        scheduler,
        destination_report,
        adhoc,
        explain_memory,
    } = prepared_run;
    let progress = human_progress_sink(cli.json, &cli.terminal, progress_delivery);
    let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);
    let report = match host
        .block_on_root(run_project_with_scheduler_and_telemetry(
            ProjectRunRequest {
                resource: prepared.resource.as_project_resource(),
                plan,
                package_root: context.package_root(),
                state_store_path,
                state_store_path_ownership: context.state_store_path_ownership(),
                pipeline_id: explicit.pipeline_id.clone(),
                package_id: explicit.package_id.clone(),
                checkpoint_id: explicit.checkpoint_id.clone(),
                destination,
                run_id: None,
                event_sink,
                after_receipt_verified: None,
            },
            &run_services,
            Some(scheduler),
            RunTelemetryConfig::phase_metrics().with_statistics_profile(explicit.stats_profile),
        ))
        .map_err(|error| redact_error_value(error, secret_redaction.as_deref()))
    {
        Ok(report) => report,
        Err(error) => {
            let error = CliError::from(error);
            let error = match progress {
                Some(progress) => error.with_progress(progress.finish()),
                None => error,
            };
            return Err(error);
        }
    };
    let memory = RunMemoryReport::capture(
        crate::runtime_budget::resolve(cli)?,
        run_services.memory().snapshot(),
    );
    match report {
        ProjectRunOutcome::Committed(report) => {
            let mut cli_report = RunCliReport::from_report(
                &report,
                destination_report,
                prepared.schema_snapshot,
                memory,
            );
            if let Some(adhoc) = adhoc {
                cli_report = cli_report.with_adhoc(adhoc);
            }
            cli_report = cli_report.with_explain_memory(explain_memory);
            let document = cli_report.render_document();
            match progress {
                Some(progress) => CommandOutput::rendered_with_progress(
                    "run",
                    document,
                    cli_report,
                    progress.finish(),
                ),
                None => CommandOutput::rendered("run", document, cli_report),
            }
        }
        ProjectRunOutcome::NoOp(report) => {
            let mut cli_report = RunNoOpCliReport::from_report(
                &report,
                explicit.resource_id.to_string(),
                explicit.pipeline_id.to_string(),
                destination_report,
                prepared.schema_snapshot,
                memory,
            );
            if let Some(adhoc) = adhoc {
                cli_report = cli_report.with_adhoc(adhoc);
            }
            cli_report = cli_report.with_explain_memory(explain_memory);
            let document = cli_report.render_document();
            match progress {
                Some(progress) => CommandOutput::rendered_with_progress(
                    "run",
                    document,
                    cli_report,
                    progress.finish(),
                ),
                None => CommandOutput::rendered("run", document, cli_report),
            }
        }
    }
}

fn run_destination_resolution_error(
    context: &ProjectContext,
    destination_uri: Option<&str>,
    error: CdfError,
) -> CliError {
    let error = redact_error_value(error, None);
    if error
        .message
        .contains("no project destination driver registered")
        || error.message.contains("malformed or non-local")
        || error.message.contains("is missing a scheme")
    {
        CliError::not_supported_with(
            "run",
            error.message,
            "registered project destination driver",
            error_catalog::DESTINATION_NOT_SUPPORTED,
        )
        .with_suggestions(destination_error_suggestions(context, destination_uri))
    } else {
        error.into()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct RunBatchReport {
    input_authority: &'static str,
    selection: cdf_project::ProjectResourceSelection,
    counts: RunBatchCounts,
    resources: Vec<RunResourceOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RunBatchCounts {
    selected: usize,
    completed: usize,
    blocked: usize,
    failed: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum RunResourceOutcome {
    Completed {
        resource_id: String,
        result: serde_json::Value,
    },
    PreparationFailed {
        resource_id: String,
        error: RunResourceError,
    },
    PreparationBlocked {
        resource_id: String,
    },
    Failed {
        resource_id: String,
        error: RunResourceError,
    },
}

impl RunResourceOutcome {
    fn resource_id(&self) -> &str {
        match self {
            Self::Completed { resource_id, .. }
            | Self::PreparationFailed { resource_id, .. }
            | Self::PreparationBlocked { resource_id }
            | Self::Failed { resource_id, .. } => resource_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RunResourceError {
    code: String,
    kind: cdf_kernel::ErrorKind,
    message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    suggestions: Vec<String>,
}

impl From<CliError> for RunResourceError {
    fn from(error: CliError) -> Self {
        Self {
            code: error.code,
            kind: error.kind,
            message: error.message,
            suggestions: error.suggestions.into_vec(),
        }
    }
}

fn run_batch_output(
    selection: cdf_project::ProjectResourceSelection,
    outcomes: Vec<RunResourceOutcome>,
    documents: Vec<RenderDocument>,
    progress: Option<crate::progress::ProgressSnapshot>,
) -> Result<CommandOutput, CliError> {
    let completed = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, RunResourceOutcome::Completed { .. }))
        .count();
    let failed = outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                RunResourceOutcome::PreparationFailed { .. } | RunResourceOutcome::Failed { .. }
            )
        })
        .count();
    let blocked = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, RunResourceOutcome::PreparationBlocked { .. }))
        .count();
    let counts = RunBatchCounts {
        selected: outcomes.len(),
        completed,
        blocked,
        failed,
    };
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            if failed == 0 {
                StatusKind::Success
            } else {
                StatusKind::Error
            },
            format!("ran {completed}/{} selected resource(s)", counts.selected),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Run summary")
                .row("selected", counts.selected.to_string())
                .row("completed", counts.completed.to_string())
                .row("blocked", counts.blocked.to_string())
                .row("failed", counts.failed.to_string()),
        );
    for outcome in &outcomes {
        if let RunResourceOutcome::PreparationFailed { resource_id, error }
        | RunResourceOutcome::Failed { resource_id, error } = outcome
        {
            document = document.blank_line().push(StatusLine::new(
                StatusKind::Error,
                format!("failed {resource_id} [{}]: {}", error.code, error.message),
            ));
        }
        if let RunResourceOutcome::PreparationBlocked { resource_id } = outcome {
            document = document.blank_line().push(StatusLine::new(
                StatusKind::Warning,
                format!("not run {resource_id}: another selected resource failed preparation"),
            ));
        }
    }
    for resource_document in documents {
        document = document.blank_line().append(resource_document);
    }
    let report = RunBatchReport {
        input_authority: "resource_set",
        selection,
        counts,
        resources: outcomes,
    };
    match progress {
        Some(progress) => CommandOutput::rendered_with_progress_and_exit_code(
            "run",
            document,
            report,
            progress,
            i32::from(failed != 0),
        ),
        None => {
            CommandOutput::rendered_with_exit_code("run", document, report, i32::from(failed != 0))
        }
    }
}

struct SynthesizedAdhoc {
    report: AdhocRunReport,
}

fn looks_like_adhoc_location(value: &str) -> Result<bool, CliError> {
    if value.contains("://")
        || value.contains(std::path::MAIN_SEPARATOR)
        || value.to_ascii_lowercase().ends_with(".parquet")
    {
        return Ok(true);
    }
    match fs::metadata(value) {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match fs::symlink_metadata(value) {
                Ok(metadata) if metadata.file_type().is_symlink() => Ok(true),
                Ok(_) => Ok(true),
                Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(link_error) => Err(adhoc_source_metadata_error(
                    "inspect ad-hoc source candidate entry",
                    link_error,
                )),
            }
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::NotADirectory
                || cdf_kernel::is_filesystem_loop(&error) =>
        {
            Ok(true)
        }
        Err(error) => Err(adhoc_source_metadata_error(
            "inspect ad-hoc source candidate",
            error,
        )),
    }
}

fn synthesize_adhoc_source(
    context: &mut ProjectContext,
    location: &str,
) -> Result<SynthesizedAdhoc, CliError> {
    let current_dir = std::env::current_dir().map_err(|error| {
        CliError::from(CdfError::environment(format!(
            "read current directory: {error}; change to an accessible directory or pass an absolute source location"
        )))
    })?;
    let is_remote = location.contains("://");
    let canonical_location = if is_remote {
        location.to_owned()
    } else {
        let input = Path::new(location);
        let candidates = if input.is_absolute() {
            vec![input.to_path_buf()]
        } else {
            vec![current_dir.join(input), context.root.join(input)]
        };
        let mut source = None;
        let mut wrong_shape = None;
        for candidate in candidates {
            match fs::metadata(&candidate) {
                Ok(metadata) if metadata.is_file() => {
                    source = Some(candidate);
                    break;
                }
                Ok(_) => wrong_shape = Some("expected a regular file".to_owned()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    match fs::symlink_metadata(&candidate) {
                        Ok(metadata) if metadata.file_type().is_symlink() => {
                            wrong_shape = Some("source is a dangling symlink".to_owned());
                        }
                        Ok(_) => {
                            wrong_shape = Some(
                                "source changed filesystem shape during inspection".to_owned(),
                            );
                        }
                        Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(link_error) => {
                            return Err(adhoc_source_metadata_error(
                                "inspect ad-hoc source candidate entry",
                                link_error,
                            ));
                        }
                    }
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotADirectory
                        || cdf_kernel::is_filesystem_loop(&error) =>
                {
                    wrong_shape = Some(error.to_string());
                }
                Err(error) => {
                    return Err(adhoc_source_metadata_error(
                        "inspect ad-hoc source candidate",
                        error,
                    ));
                }
            }
        }
        let source = match (source, wrong_shape) {
            (Some(source), _) => source,
            (None, Some(detail)) => {
                return Err(CdfError::data(format!(
                    "ad-hoc source `[redacted-local-source-path]` has an invalid filesystem shape: {detail}"
                ))
                .into());
            }
            (None, None) => {
                return Err(CliError::usage_with(
                    "cdf run ad-hoc could not find local source `[redacted-local-source-path]`",
                    error_catalog::USAGE,
                ));
            }
        };
        fs::canonicalize(source)
            .map_err(adhoc_source_canonicalize_error)?
            .to_str()
            .ok_or_else(|| CdfError::data("ad-hoc source path must be valid UTF-8"))?
            .to_owned()
    };
    let source_registry = crate::source_registry::builtin_source_registry()?;
    let initial_plan = source_registry
        .plan_add(
            cdf_runtime::SourceAddRequest {
                source_name: "adhoc".to_owned(),
                resource_name: "candidate".to_owned(),
                location: canonical_location.clone(),
                project_root: context.root.clone(),
                current_dir: current_dir.clone(),
                options: std::collections::BTreeMap::new(),
                project_options: None,
            },
            &context.config.driver_options,
        )
        .map_err(|error| {
            if is_remote {
                CliError::from(error)
            } else {
                CliError::usage_with(
                    "cdf run ad-hoc could not compile local source `[redacted-local-source-path]`",
                    error_catalog::USAGE,
                )
            }
        })?;
    let identity_prefix = initial_plan
        .proposal
        .resource_options
        .get("format")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| initial_plan.driver.driver_id.as_str());
    let digest = stable_adhoc_digest(&canonical_location);
    let resource_name = format!("{identity_prefix}_{}", &digest[..24]);
    let resource_id = format!("adhoc.{resource_name}");
    if context
        .resources
        .iter()
        .any(|resource| resource.descriptor().resource_id.as_str() == resource_id)
    {
        return Err(CliError::mapped(
            CdfError::contract(format!(
                "cdf run ad-hoc synthesized resource id `{resource_id}` conflicts with an already compiled project resource; rename or remove the conflicting project resource before retrying"
            )),
            error_catalog::PROJECT_RESOURCE_ID,
        ));
    }
    let definition_path = format!(".cdf/adhoc/{resource_name}.cdf.sql");
    let definition_path_abs = context.root.join(&definition_path);

    let (compiled_location, source_artifact_path, permanent_location) = if is_remote {
        (canonical_location.clone(), None, canonical_location.clone())
    } else {
        let file_name = Path::new(&canonical_location)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| CdfError::data("ad-hoc source requires a UTF-8 file name"))?;
        let staged_path = format!(".cdf/adhoc/data/{resource_name}/{file_name}");
        persist_local_adhoc_source(
            &context.root,
            Path::new(&canonical_location),
            &context.root.join(&staged_path),
        )?;
        (staged_path.clone(), Some(staged_path.clone()), staged_path)
    };
    let add_plan = source_registry.plan_add(
        cdf_runtime::SourceAddRequest {
            source_name: "adhoc".to_owned(),
            resource_name: resource_name.clone(),
            location: compiled_location,
            project_root: context.root.clone(),
            current_dir,
            options: std::collections::BTreeMap::new(),
            project_options: None,
        },
        &context.config.driver_options,
    )?;
    if !add_plan.proposal.private_files.is_empty() {
        return Err(CliError::usage_with(
            "cdf run ad-hoc cannot synthesize a source that requires private-file materialization; use cdf add",
            error_catalog::USAGE,
        ));
    }
    let resource_sql = registered_source_resource_sql("adhoc", &add_plan)?;
    let reused = read_adhoc_private_text(&context.root, &definition_path_abs)?
        .is_some_and(|existing| existing == resource_sql);
    if !reused {
        let expectation = match fs::read(&definition_path_abs) {
            Ok(existing) => ProjectFileExpectation::Exact(existing),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                ProjectFileExpectation::Absent
            }
            Err(error) => {
                return Err(adhoc_private_path_error(
                    "read ad-hoc resource definition before publication",
                    &definition_path_abs,
                    error,
                ));
            }
        };
        publish_project_files_transactionally(
            &context.root,
            &definition_path,
            vec![ProjectFileWrite::new(
                &definition_path,
                resource_sql.as_bytes().to_vec(),
                expectation,
            )],
        )?;
    }

    let authored = cdf_project::parse_resource_file(&resource_sql, &definition_path)?;
    let parsed = cdf_engine::parse_project_query_at(
        &authored.query_sql,
        &definition_path,
        authored.query_span.start_line,
        authored.query_span.start_column,
    )?;
    if parsed.upstream.configured_source != "adhoc"
        || parsed.upstream.resource_options != add_plan.proposal.resource_options
    {
        return Err(CliError::mapped(
            CdfError::internal("generated ad-hoc SQL changed its typed source proposal"),
            error_catalog::PROJECT_IO,
        ));
    }
    let resource = compile_adhoc_resource(
        source_registry,
        &context.root,
        &resource_name,
        &resource_id,
        &add_plan,
        parsed.upstream.resource_options,
    )?;
    let resource = hydrate_adhoc_locked_snapshot(context, resource)?;
    if resource.descriptor().resource_id.as_str() != resource_id {
        return Err(CliError::mapped(
            CdfError::internal("generated ad-hoc resource id did not match its stable identity"),
            error_catalog::PROJECT_IO,
        ));
    }
    context.resources.push(resource);
    context.register_adhoc_resource(resource_id.clone());

    Ok(SynthesizedAdhoc {
        report: AdhocRunReport {
            resource_id: resource_id.clone(),
            definition_path,
            source_artifact_path,
            reused,
            make_permanent_command: format!(
                "cdf add {resource_id} {}",
                shell_argument(&permanent_location)
            ),
        },
    })
}

fn compile_adhoc_resource(
    registry: &cdf_runtime::SourceRegistry,
    project_root: &Path,
    resource_name: &str,
    resource_id: &str,
    add_plan: &cdf_runtime::PlannedSourceAdd,
    resource_options: std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<CompiledResource, CliError> {
    let cursor = add_plan.proposal.cursor.as_ref().map(|cursor| CursorSpec {
        field: cursor.field.clone(),
        ordering: match cursor.ordering {
            cdf_runtime::SourceAddCursorOrdering::Exact => CursorOrderingClaim::Exact,
            cdf_runtime::SourceAddCursorOrdering::Inexact
            | cdf_runtime::SourceAddCursorOrdering::BestEffort => CursorOrderingClaim::Inexact,
            cdf_runtime::SourceAddCursorOrdering::Unordered => CursorOrderingClaim::Unordered,
        },
        lag_tolerance_ms: cursor.lag_tolerance_ms,
    });
    let descriptor = ResourceDescriptor {
        resource_id: ResourceId::new(resource_id)?,
        schema_source: SchemaSource::Discover,
        primary_key: Vec::new(),
        merge_key: Vec::new(),
        cursor,
        write_disposition: WriteDisposition::Append,
        deduplication: None,
        contract: None,
        state_scope: ScopeKey::Resource,
        freshness: None,
        trust_level: TrustLevel::Governed,
    };
    descriptor.validate()?;
    let cursor_pushdown =
        add_plan
            .proposal
            .cursor
            .as_ref()
            .map(|cursor| cdf_runtime::SourceCursorPushdown {
                parameter: cursor.parameter.clone(),
                fidelity: match cursor.ordering {
                    cdf_runtime::SourceAddCursorOrdering::Exact => PushdownFidelity::Exact,
                    cdf_runtime::SourceAddCursorOrdering::Inexact
                    | cdf_runtime::SourceAddCursorOrdering::BestEffort => PushdownFidelity::Inexact,
                    cdf_runtime::SourceAddCursorOrdering::Unordered => {
                        PushdownFidelity::Unsupported
                    }
                },
            });
    let source_plan = registry.compile(cdf_runtime::SourceCompileRequest {
        source_kind: add_plan.proposal.source_kind.clone(),
        context: cdf_runtime::SourceCompileContext {
            source_name: "adhoc".to_owned(),
            project_root: Some(project_root.to_path_buf()),
            cursor_pushdown,
        },
        source_options: add_plan.proposal.source_options.clone(),
        resource_options,
        descriptor,
        schema: Schema::empty(),
        type_policy_allowances: Default::default(),
        effective_schema_runtime: None,
        baseline_observation_schema_catalog: Vec::new(),
    })?;
    Ok(CompiledResource::from_compiled_source(
        "adhoc",
        resource_name,
        Some(project_root.to_path_buf()),
        source_plan,
    )?)
}

fn hydrate_adhoc_locked_snapshot(
    context: &ProjectContext,
    resource: CompiledResource,
) -> Result<CompiledResource, CliError> {
    let Some(lock) = context.lock.as_ref() else {
        return Ok(resource);
    };
    let Some(locked) = lock
        .resources
        .get(resource.descriptor().resource_id.as_str())
    else {
        return Ok(resource);
    };
    let Some(reference) = locked.schema_snapshot.as_ref() else {
        return Ok(resource);
    };
    if locked.schema_hash.as_deref() != Some(reference.schema_hash.as_str())
        || locked.descriptor.schema_source.pinned_snapshot() != Some(reference)
    {
        return Err(CliError::from(CdfError::data(format!(
            "{LOCK_FILE_NAME} has inconsistent schema snapshot pointers for ad-hoc resource `{}`",
            resource.descriptor().resource_id
        ))));
    }
    let artifact = SchemaSnapshotStore::new(&context.root).read(reference)?;
    if artifact.resource_id != resource.descriptor().resource_id.as_str() {
        return Err(CliError::from(CdfError::data(format!(
            "schema snapshot {} belongs to resource `{}` instead of ad-hoc resource `{}`",
            reference.path,
            artifact.resource_id,
            resource.descriptor().resource_id
        ))));
    }
    let pinned_source = resource
        .descriptor()
        .schema_source
        .with_pinned_snapshot(reference.clone())
        .ok_or_else(|| {
            CliError::from(CdfError::internal(
                "ad-hoc schema source does not support lock hydration",
            ))
        })?;
    Ok(
        resource
            .with_schema_source_and_schema(pinned_source, Arc::new(artifact.schema.to_arrow()?)),
    )
}

fn persist_local_adhoc_source(
    project_root: &Path,
    source: &Path,
    destination: &Path,
) -> Result<(), CliError> {
    let parent = destination.parent().ok_or_else(|| {
        CliError::mapped(
            CdfError::internal("ad-hoc staged source path has no parent"),
            error_catalog::PROJECT_IO,
        )
    })?;
    ensure_adhoc_private_parent(project_root, parent)?;
    let temporary = adhoc_temporary_path(parent, destination)?;
    if fs::hard_link(source, &temporary).is_err() {
        copy_local_adhoc_source(source, &temporary)?;
    }
    let publish_result = (|| {
        match fs::symlink_metadata(destination) {
            Ok(metadata) if metadata.is_file() => {
                fs::remove_file(destination).map_err(|error| {
                    adhoc_private_path_error("refresh staged ad-hoc source", destination, error)
                })?;
            }
            Ok(_) => {
                return Err(adhoc_private_shape_error(
                    "inspect staged ad-hoc source",
                    destination,
                    "expected a regular file",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(adhoc_private_path_error(
                    "inspect staged ad-hoc source",
                    destination,
                    error,
                ));
            }
        }
        fs::rename(&temporary, destination).map_err(|error| {
            adhoc_private_path_error("publish staged ad-hoc source", destination, error)
        })
    })();
    if publish_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    publish_result
}

fn read_adhoc_private_text(project_root: &Path, path: &Path) -> Result<Option<String>, CliError> {
    let parent = path.parent().ok_or_else(|| {
        CliError::mapped(
            CdfError::internal("ad-hoc private path has no parent"),
            error_catalog::PROJECT_IO,
        )
    })?;
    ensure_adhoc_private_parent(project_root, parent)?;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            fs::read_to_string(path).map(Some).map_err(|error| {
                adhoc_private_path_error("read ad-hoc resource config", path, error)
            })
        }
        Ok(_) => Err(adhoc_private_shape_error(
            "inspect ad-hoc resource config",
            path,
            "expected a real regular file",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(adhoc_private_path_error(
            "inspect ad-hoc resource config",
            path,
            error,
        )),
    }
}

fn ensure_adhoc_private_parent(project_root: &Path, parent: &Path) -> Result<(), CliError> {
    let relative = parent.strip_prefix(project_root).map_err(|_| {
        CliError::mapped(
            CdfError::internal(format!(
                "CDF-managed ad-hoc parent {} escapes project root {}",
                parent.display(),
                project_root.display()
            )),
            error_catalog::PROJECT_IO,
        )
    })?;
    let mut current = project_root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(adhoc_private_shape_error(
                    "inspect ad-hoc parent",
                    &current,
                    "expected a real directory",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        let metadata = fs::symlink_metadata(&current).map_err(|error| {
                            adhoc_private_path_error("revalidate ad-hoc parent", &current, error)
                        })?;
                        if metadata.file_type().is_symlink() || !metadata.is_dir() {
                            return Err(adhoc_private_shape_error(
                                "revalidate ad-hoc parent",
                                &current,
                                "concurrent non-directory or symlink",
                            ));
                        }
                    }
                    Err(error) => {
                        return Err(adhoc_private_path_error(
                            "create ad-hoc parent",
                            &current,
                            error,
                        ));
                    }
                }
            }
            Err(error) => {
                return Err(adhoc_private_path_error(
                    "inspect ad-hoc parent",
                    &current,
                    error,
                ));
            }
        }
    }
    Ok(())
}

fn adhoc_temporary_path(parent: &Path, destination: &Path) -> Result<std::path::PathBuf, CliError> {
    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            CliError::mapped(
                CdfError::internal("ad-hoc staged source path has no UTF-8 filename"),
                error_catalog::PROJECT_IO,
            )
        })?;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CdfError::environment(format!(
                "read the host clock for an ad-hoc staging temporary: {error}; correct the system clock and retry"
            ))
        })?
        .as_nanos();
    let sequence = ADHOC_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    Ok(parent.join(format!(
        ".{name}.{}.{}.{}.tmp",
        std::process::id(),
        nanos,
        sequence
    )))
}

fn copy_local_adhoc_source(source: &Path, temporary: &Path) -> Result<(), CliError> {
    let result = (|| {
        let mut input =
            File::open(source).map_err(|error| adhoc_source_read_error(source, error))?;
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(temporary)
            .map_err(|error| {
                adhoc_private_path_error("create ad-hoc staging temporary", temporary, error)
            })?;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = input
                .read(&mut buffer)
                .map_err(|error| adhoc_source_read_error(source, error))?;
            if count == 0 {
                break;
            }
            output.write_all(&buffer[..count]).map_err(|error| {
                adhoc_private_path_error("write ad-hoc staging temporary", temporary, error)
            })?;
        }
        output.sync_all().map_err(|error| {
            adhoc_private_path_error("sync ad-hoc staging temporary", temporary, error)
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn adhoc_source_metadata_error(action: &str, error: std::io::Error) -> CliError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::data(format!(
            "{action} `[redacted-local-source-path]` with invalid filesystem shape: {error}"
        ))
        .into()
    } else {
        CdfError::environment(format!(
            "{action} `[redacted-local-source-path]`: {error}; check source-path permissions, device availability, memory, and process file limits before retrying"
        ))
        .into()
    }
}

fn adhoc_source_canonicalize_error(error: std::io::Error) -> CliError {
    if error.kind() == std::io::ErrorKind::NotFound {
        CdfError::data(format!(
            "canonicalize ad-hoc source `[redacted-local-source-path]`: {error}"
        ))
        .into()
    } else {
        adhoc_source_metadata_error("canonicalize ad-hoc source", error)
    }
}

fn stable_adhoc_digest(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn shell_argument(value: &str) -> String {
    if value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-' | b':' | b'%')
    }) {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn resolved_run_args(
    context: &ProjectContext,
    resource_id: String,
    args: RunArgs,
) -> Result<ResolvedRunArgs, CliError> {
    let suffix = minted_run_suffix(&resource_id)?;
    let package_id = format!("pkg-{suffix}");
    let checkpoint_id = format!("checkpoint-{suffix}");
    let target = if context.is_adhoc_resource(&resource_id) {
        TargetName::new(adhoc_target_for_resource(&resource_id))?
    } else {
        context.resource_target(&resource_id)?.clone()
    };
    Ok(ResolvedRunArgs {
        resource_id: resource_id.clone(),
        pipeline_id: PipelineId::new(DEFAULT_RUN_PIPELINE_ID)?,
        destination_uri: args.destination_uri,
        target,
        package_id,
        checkpoint_id: CheckpointId::new(checkpoint_id)?,
        jobs: args.jobs,
        stats_profile: args.stats_profile,
        segmentation: args.segmentation,
    })
}

fn adhoc_target_for_resource(resource_id: &str) -> String {
    resource_id
        .rsplit('.')
        .next()
        .filter(|segment| !segment.is_empty())
        .unwrap_or(resource_id)
        .to_owned()
}

fn minted_run_suffix(resource_id: &str) -> Result<String, CliError> {
    minted_run_suffix_at(resource_id, SystemTime::now())
}

fn minted_run_suffix_at(resource_id: &str, now: SystemTime) -> Result<String, CliError> {
    let resource = resource_id.replace(|character: char| !character.is_ascii_alphanumeric(), "-");
    let nanos = now
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CdfError::environment(format!(
                "read the host clock for run identity generation: {error}; correct the system clock and retry"
            ))
        })?
        .as_nanos();
    Ok(format!("{resource}-{}-{nanos}", std::process::id()))
}

struct ResolvedRunArgs {
    resource_id: String,
    pipeline_id: PipelineId,
    destination_uri: Option<String>,
    target: TargetName,
    package_id: String,
    checkpoint_id: CheckpointId,
    jobs: Option<u16>,
    stats_profile: bool,
    segmentation: cdf_cli_core::args::SegmentationArgs,
}

pub(crate) fn ensure_parent_directory(
    path: &std::path::Path,
    ownership: cdf_project::StateStorePathOwnership,
) -> Result<(), CliError> {
    if path.parent().is_none() {
        return Err(CliError::mapped(
            CdfError::internal(format!("{} has no parent directory", path.display())),
            error_catalog::RUN_ARTIFACT_INTERNAL,
        ));
    }
    cdf_project::ensure_state_parent_directory(path, ownership).map_err(CliError::from)
}

fn adhoc_source_read_error(_source: &Path, error: std::io::Error) -> CliError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidData
            | std::io::ErrorKind::IsADirectory
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::data(format!(
            "read local ad-hoc source `[redacted-local-source-path]`: {error}"
        ))
        .into()
    } else {
        CdfError::environment(format!(
            "read local ad-hoc source `[redacted-local-source-path]`: {error}; check source-path permissions, device availability, memory, and process file limits before retrying"
        ))
        .into()
    }
}

fn adhoc_private_path_error(action: &str, path: &Path, error: std::io::Error) -> CliError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::AlreadyExists
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        adhoc_private_shape_error(action, path, &error.to_string())
    } else {
        project_path_environment(action, path, error)
    }
}

fn adhoc_private_shape_error(action: &str, path: &Path, detail: &str) -> CliError {
    CliError::mapped(
        CdfError::internal(format!(
            "{action} at CDF-managed ad-hoc path {}: {detail}",
            path.display()
        )),
        error_catalog::PROJECT_IO,
    )
}

fn project_path_environment(action: &str, path: &Path, error: std::io::Error) -> CliError {
    CdfError::environment(format!(
        "{action} {}: {error}; check project-path permissions, free space, device availability, and process file limits before retrying",
        path.display()
    ))
    .into()
}

#[cfg(test)]
mod tests {
    use cdf_kernel::ErrorKind;

    use super::*;

    #[test]
    fn adhoc_source_with_regular_file_parent_is_data_owned() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("source-parent");
        fs::write(&parent, b"not a directory").unwrap();
        let source = parent.join("input.csv");
        let destination = root.path().join("staged.csv");

        let error = persist_local_adhoc_source(root.path(), &source, &destination).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Data);
        assert!(error.message.contains("read local ad-hoc source"));
        assert!(error.message.contains("[redacted-local-source-path]"));
        assert!(!error.message.contains(&root.path().display().to_string()));
    }

    #[test]
    fn adhoc_source_host_failure_redacts_the_local_path() {
        let source = Path::new("/sensitive/local/customer/input.csv");
        let error = adhoc_source_read_error(
            source,
            std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        );

        assert_eq!(error.kind, ErrorKind::Environment);
        assert!(error.message.contains("[redacted-local-source-path]"));
        assert!(!error.message.contains(&source.display().to_string()));
    }

    #[test]
    fn adhoc_private_staging_wrong_shape_is_internal() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.csv");
        fs::write(&source, b"value\n1\n").unwrap();
        let parent = root.path().join("private-parent");
        fs::write(&parent, b"not a directory").unwrap();

        let error = persist_local_adhoc_source(root.path(), &source, &parent.join("staged.csv"))
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Internal);
        assert_eq!(error.code, error_catalog::PROJECT_IO.code);
    }

    #[test]
    fn run_identity_clock_failure_is_environment_owned() {
        let before_epoch = UNIX_EPOCH
            .checked_sub(std::time::Duration::from_secs(1))
            .unwrap();

        let error = minted_run_suffix_at("resource", before_epoch).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Environment);
        assert_eq!(error.code, "CDF-ENV-HOST");
        assert!(error.message.contains("correct the system clock"));
    }

    #[test]
    fn replay_configured_state_parent_wrong_shape_is_contract_owned() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("state");
        fs::write(&parent, b"not a directory").unwrap();

        let error = ensure_parent_directory(
            &parent.join("state.db"),
            cdf_project::StateStorePathOwnership::Configured,
        )
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Contract);
        assert!(error.message.contains("state-store parent"));
    }

    #[cfg(unix)]
    #[test]
    fn replay_state_parent_rejects_symlink_ancestor_without_writing_outside() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let managed = root.path().join(".cdf");
        symlink(outside.path(), &managed).unwrap();

        let error = ensure_parent_directory(
            &managed.join("state/state.db"),
            cdf_project::StateStorePathOwnership::CdfManaged,
        )
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Internal);
        assert!(!outside.path().join("state").exists());
    }
}
