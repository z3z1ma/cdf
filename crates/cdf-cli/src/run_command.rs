use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_declarative::{
    CompiledResource, compile_document_with_project_root, parse_toml as parse_declarative_toml,
};
use cdf_kernel::{CdfError, CheckpointId, PipelineId, RunEventSink, TargetName};
use cdf_project::{
    LOCK_FILE_NAME, ProjectResourceOrigin, ProjectRunRequest, RunTelemetryConfig,
    SchemaSnapshotStore, run_project_with_scheduler_and_telemetry,
};
use sha2::{Digest, Sha256};

use crate::{
    add_command::registered_source_resource_toml,
    args::{Cli, RunArgs, ScanArgs},
    context::ProjectContext,
    destination_uri::{
        destination_error_suggestions, redact_error_value,
        resolve_selected_destination_with_services,
    },
    error_catalog,
    output::{CliError, CommandOutput},
    progress::human_progress_sink,
    project_run_resource::prepare_runtime_resource_for_cli,
    reports::{AdhocRunReport, RunCliReport, RunDestinationReport},
    scan_command::{build_engine_plan_for_resource, default_target_for_resource},
};

pub(crate) const DEFAULT_RUN_PIPELINE_ID: &str = "cdf-run";

pub(crate) fn run(
    cli: &Cli,
    args: RunArgs,
    host: &cdf_engine::StandaloneExecutionHost,
    services: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    if args.loop_mode {
        return Err(CliError::not_supported_with(
            "run --loop",
            "the local development loop supervisor is excluded from this explicit one-package run slice",
            "later loop/streaming supervisor",
            error_catalog::RUN_LOOP_NOT_SUPPORTED,
        ));
    }
    let mut args = args;
    let requested = args.resource_id.clone().ok_or_else(|| {
        CliError::usage_with("run requires RESOURCE", error_catalog::RUN_ARGUMENT)
    })?;
    let mut context =
        ProjectContext::load_for_command("run", cli.project.as_ref(), cli.env.as_deref())?;
    let adhoc = if context.has_resource(&requested) {
        None
    } else if looks_like_adhoc_location(&requested) {
        if args.destination_uri.is_none() {
            return Err(CliError::usage_with(
                "cdf run ad-hoc mode requires an explicit `--to <destination>`",
                error_catalog::RUN_ARGUMENT,
            ));
        }
        let synthesized = synthesize_adhoc_source(&mut context, &requested)?;
        args.resource_id = Some(synthesized.resource_id.clone());
        Some(synthesized.report)
    } else {
        None
    };
    let explicit = resolved_run_args(args)?;
    let host_jobs = services.capabilities().logical_cpu_slots;
    let provisional_jobs = explicit.jobs.unwrap_or(host_jobs).min(host_jobs);
    let run_services = services
        .with_run_job_ceiling(provisional_jobs)?
        .with_scheduler_measurement(true)?;
    let prepared = prepare_runtime_resource_for_cli(
        destinations,
        &context,
        &explicit.resource_id,
        false,
        Some(&run_services),
    )?;
    let state_store_path = context.state_store_path()?;
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
            no_pin: false,
        },
        Some(&explicit.package_id),
        identifier_policy.as_ref(),
        &resolved.destination.runtime_capabilities(),
    )?;
    let destination = resolved.destination;
    let scheduler = prepared
        .resource
        .source_plan()
        .map(|source| {
            cdf_runtime::resolve_runtime_scheduler(
                plan.scan.partitions.len(),
                &source.execution_capabilities,
                &destination.runtime_capabilities(),
                &run_services,
                explicit.jobs,
            )
        })
        .transpose()?;
    if let Some(scheduler) = &scheduler {
        run_services.tighten_run_job_ceiling(scheduler.effective_jobs.jobs)?;
    }
    let destination_report =
        RunDestinationReport::from_project(&destination.describe(), destination.target());
    let progress = human_progress_sink(cli.json, &cli.terminal);
    let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);
    let report = match host
        .block_on_root(run_project_with_scheduler_and_telemetry(
            ProjectRunRequest {
                resource: prepared.resource.as_project_resource(),
                plan,
                package_root: context.package_root(),
                state_store_path,
                pipeline_id: explicit.pipeline_id.clone(),
                package_id: explicit.package_id.clone(),
                checkpoint_id: explicit.checkpoint_id.clone(),
                destination,
                run_id: None,
                event_sink,
                after_receipt_verified: None,
            },
            &run_services,
            scheduler,
            RunTelemetryConfig::phase_metrics(),
        ))
        .map_err(|error| redact_error_value(error, resolved.secret_redaction.as_deref()))
    {
        Ok(report) => report,
        Err(error) => {
            let error = CliError::from(error);
            let error = match progress.as_ref() {
                Some(progress) => error.with_progress(progress.snapshot()),
                None => error,
            };
            return Err(error);
        }
    };
    let mut cli_report =
        RunCliReport::from_report(&report, destination_report, prepared.schema_snapshot);
    if let Some(adhoc) = adhoc {
        cli_report = cli_report.with_adhoc(adhoc);
    }
    let document = cli_report.render_document();
    match progress {
        Some(progress) => {
            CommandOutput::rendered_with_progress("run", document, cli_report, progress.snapshot())
        }
        None => CommandOutput::rendered("run", document, cli_report),
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

struct SynthesizedAdhoc {
    resource_id: String,
    report: AdhocRunReport,
}

fn looks_like_adhoc_location(value: &str) -> bool {
    value.contains("://")
        || value.contains(std::path::MAIN_SEPARATOR)
        || value.to_ascii_lowercase().ends_with(".parquet")
        || Path::new(value).is_file()
}

fn synthesize_adhoc_source(
    context: &mut ProjectContext,
    location: &str,
) -> Result<SynthesizedAdhoc, CliError> {
    let current_dir = std::env::current_dir().map_err(|error| {
        CliError::mapped(
            CdfError::internal(format!("read current directory: {error}")),
            error_catalog::PROJECT_IO,
        )
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
        let source = candidates
            .into_iter()
            .find(|candidate| candidate.is_file())
            .ok_or_else(|| {
                CliError::usage_with(
                    "cdf run ad-hoc could not find local source `[redacted-local-source-path]`",
                    error_catalog::USAGE,
                )
            })?;
        fs::canonicalize(source)
            .map_err(|error| {
                CdfError::data(format!(
                    "canonicalize ad-hoc source `[redacted-local-source-path]`: {error}"
                ))
            })?
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
            error_catalog::PROJECT_RESOURCE_MAPPING,
        ));
    }
    let config_path = format!(".cdf/adhoc/{resource_name}.toml");
    let config_path_abs = context.root.join(&config_path);

    let (compiled_location, source_artifact_path, permanent_location) = if is_remote {
        (canonical_location.clone(), None, canonical_location.clone())
    } else {
        let file_name = Path::new(&canonical_location)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| CdfError::data("ad-hoc source requires a UTF-8 file name"))?;
        let staged_path = format!(".cdf/adhoc/data/{resource_name}/{file_name}");
        persist_local_adhoc_source(
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
    let resource_toml = registered_source_resource_toml("adhoc", &resource_name, &add_plan)?;
    let reused = fs::read_to_string(&config_path_abs).ok().as_deref() == Some(&resource_toml);
    if !reused {
        let parent = config_path_abs.parent().ok_or_else(|| {
            CliError::mapped(
                CdfError::internal("ad-hoc resource path has no parent"),
                error_catalog::PROJECT_IO,
            )
        })?;
        fs::create_dir_all(parent).map_err(|error| {
            CliError::mapped(
                CdfError::data(format!("create .cdf/adhoc resource directory: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?;
        fs::write(&config_path_abs, &resource_toml).map_err(|error| {
            CliError::mapped(
                CdfError::data(format!("write ad-hoc resource config: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?;
    }

    let document = parse_declarative_toml(&resource_toml)?;
    let mut resources =
        compile_document_with_project_root(source_registry, &document, &context.root)?;
    if resources.len() != 1 {
        return Err(CliError::mapped(
            CdfError::internal(format!(
                "generated ad-hoc TOML compiled {} resources instead of one",
                resources.len()
            )),
            error_catalog::PROJECT_IO,
        ));
    }
    let resource = hydrate_adhoc_locked_snapshot(context, resources.remove(0))?;
    if resource.descriptor().resource_id.as_str() != resource_id {
        return Err(CliError::mapped(
            CdfError::internal("generated ad-hoc resource id did not match its stable identity"),
            error_catalog::PROJECT_IO,
        ));
    }
    context.resources.push(resource);
    context.resource_origins.push(ProjectResourceOrigin {
        source_name: "adhoc".to_owned(),
        resource_name: resource_name.clone(),
        source_file: Some(config_path.clone()),
        mapping_pattern: resource_id.clone(),
        mapping_status: "synthesized".to_owned(),
    });

    Ok(SynthesizedAdhoc {
        resource_id: resource_id.clone(),
        report: AdhocRunReport {
            resource_id: resource_id.clone(),
            config_path,
            source_artifact_path,
            reused,
            make_permanent_command: format!(
                "cdf add {resource_id} {}",
                shell_argument(&permanent_location)
            ),
        },
    })
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

fn persist_local_adhoc_source(source: &Path, destination: &Path) -> Result<(), CliError> {
    let parent = destination.parent().ok_or_else(|| {
        CliError::mapped(
            CdfError::internal("ad-hoc staged source path has no parent"),
            error_catalog::PROJECT_IO,
        )
    })?;
    fs::create_dir_all(parent).map_err(|error| {
        CliError::mapped(
            CdfError::data(format!("create .cdf/adhoc staging directory: {error}")),
            error_catalog::PROJECT_IO,
        )
    })?;
    let temporary = destination.with_extension(format!("tmp-{}", std::process::id()));
    let _ = fs::remove_file(&temporary);
    if fs::hard_link(source, &temporary).is_err() {
        fs::copy(source, &temporary).map_err(|error| {
            CliError::mapped(
                CdfError::data(format!("stage local ad-hoc source input: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?;
    }
    if destination.exists() {
        fs::remove_file(destination).map_err(|error| {
            CliError::mapped(
                CdfError::data(format!("refresh staged ad-hoc source input: {error}")),
                error_catalog::PROJECT_IO,
            )
        })?;
    }
    fs::rename(&temporary, destination).map_err(|error| {
        CliError::mapped(
            CdfError::data(format!("publish staged ad-hoc source input: {error}")),
            error_catalog::PROJECT_IO,
        )
    })?;
    Ok(())
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

fn resolved_run_args(args: RunArgs) -> Result<ResolvedRunArgs, CliError> {
    let resource_id = args.resource_id.ok_or_else(|| {
        CliError::usage_with("run requires RESOURCE", error_catalog::RUN_ARGUMENT)
    })?;
    let suffix = minted_run_suffix(&resource_id);
    let package_id = format!("pkg-{suffix}");
    let checkpoint_id = format!("checkpoint-{suffix}");
    Ok(ResolvedRunArgs {
        resource_id: resource_id.clone(),
        pipeline_id: PipelineId::new(DEFAULT_RUN_PIPELINE_ID)?,
        destination_uri: args.destination_uri,
        target: TargetName::new(default_target_for_resource(&resource_id))?,
        package_id,
        checkpoint_id: CheckpointId::new(checkpoint_id)?,
        jobs: args.jobs,
    })
}

fn minted_run_suffix(resource_id: &str) -> String {
    let resource = resource_id.replace(|character: char| !character.is_ascii_alphanumeric(), "-");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{resource}-{}-{nanos}", std::process::id())
}

struct ResolvedRunArgs {
    resource_id: String,
    pipeline_id: PipelineId,
    destination_uri: Option<String>,
    target: TargetName,
    package_id: String,
    checkpoint_id: CheckpointId,
    jobs: Option<u16>,
}

pub(crate) fn ensure_parent_directory(path: &std::path::Path) -> Result<(), CliError> {
    let Some(parent) = path.parent() else {
        return Err(CliError::mapped(
            CdfError::internal(format!("{} has no parent directory", path.display())),
            error_catalog::RUN_ARTIFACT_INTERNAL,
        ));
    };
    fs::create_dir_all(parent).map_err(|error| {
        CliError::mapped(
            CdfError::data(format!("create {}: {error}", parent.display())),
            error_catalog::RUN_ARTIFACT_PATH,
        )
    })
}
