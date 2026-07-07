use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
};

use firn_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use firn_declarative::CompiledResourcePlan;
use firn_engine::{EnginePlan, EnginePlanInput, PlanBoundedness, Planner};
use firn_kernel::{
    CheckpointId, CheckpointStore, FirnError, OrderBy, PartitionPlan, PipelineId, PredicateId,
    ResourceId, ResourceStream, ScanPredicate, ScanRequest, ScopeKey, SortDirection, TargetName,
};
use firn_package::{MANIFEST_FILE, PackageReader};
use firn_project::{
    FileResourceSourceResolver, LocalFileDuckDbRunReport, LocalFileDuckDbRunRequest,
    PreparedReceiptSource, ResourceSourceKind, generate_lockfile,
    run_local_file_to_duckdb_checkpoint, validate_project,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    args::{
        BackfillArgs, Cli, Command, ContractCommand, InitArgs, InspectArgs, InspectNoun,
        PackageArchiveArgs, PackageCommand, ReplayPackageArgs, ResumeArgs, RunArgs, ScanArgs,
        SqlArgs, StateCommand,
    },
    context::{DestinationRuntime, DoctorProbe, ProjectContext, require_lock},
    doctor_drift::{self, DriftStatus},
    output::{CliError, CommandOutput, InvocationResult},
    status_freshness, system_sql,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const MIN_PYTHON_MAJOR: u16 = 3;
const MIN_PYTHON_MINOR: u16 = 12;
const PYTHON_INTERPRETER_PROBE: &str = r#"
import json
import platform
import sys
import sysconfig

gil_enabled = True
is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
if is_gil_enabled is not None:
    gil_enabled = bool(is_gil_enabled())

free_threaded_build = sysconfig.get_config_var("Py_GIL_DISABLED") == 1
version = sys.version_info
sys.stdout.write(json.dumps({
    "executable": sys.executable,
    "version": "{}.{}.{}".format(version.major, version.minor, version.micro),
    "major": version.major,
    "minor": version.minor,
    "micro": version.micro,
    "implementation": platform.python_implementation(),
    "gil_enabled": gil_enabled,
    "free_threaded_build": free_threaded_build,
    "can_parallelize_python": free_threaded_build and not gil_enabled,
}, sort_keys=True))
"#;

pub fn execute(cli: Cli) -> InvocationResult {
    let json_mode = cli.json;
    match dispatch(cli) {
        Ok(output) => InvocationResult::from_output(json_mode, output),
        Err(error) => InvocationResult::from_error(json_mode, error),
    }
}

fn dispatch(cli: Cli) -> Result<CommandOutput, CliError> {
    let command = cli.command.clone();
    match command {
        Command::Help => Ok(output(
            "help",
            HELP_TEXT.to_owned(),
            json!({ "help": HELP_TEXT }),
        )?),
        Command::Version => Ok(output(
            "version",
            format!("firn {VERSION}"),
            json!({ "version": VERSION }),
        )?),
        Command::Init(args) => init(args),
        Command::Validate => validate(&cli),
        Command::Plan(args) => plan_or_explain(&cli, args, "plan"),
        Command::Explain(args) => plan_or_explain(&cli, args, "explain"),
        Command::Run(args) => run(&cli, args),
        Command::Preview(args) => preview(&cli, args),
        Command::Sql(args) => sql(&cli, args),
        Command::Inspect(args) => inspect(&cli, args),
        Command::DiffSchema => diff_schema(&cli),
        Command::Contract(command) => contract(command),
        Command::State(command) => state(&cli, command),
        Command::Resume(args) => resume(&cli, args),
        Command::ReplayPackage(args) => replay_package(args),
        Command::Backfill(args) => backfill(&cli, args),
        Command::Package(command) => package(&cli, command),
        Command::Doctor => doctor(&cli),
        Command::Status => status(&cli),
    }
}

fn init(_args: InitArgs) -> Result<CommandOutput, CliError> {
    Err(CliError::not_supported(
        "init",
        "project scaffold semantics are not exposed by firn-project yet",
        "project template/write API",
    ))
}

fn validate(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let resolver = FileResourceSourceResolver::new(&context.root);
    let provider = context.secret_provider();
    let report = validate_project(
        &context.config,
        Some(&context.environment.name),
        &resolver,
        &provider,
    )?;
    let human = format!(
        "validated project {} env {}: {} declarative resource(s), {} external resource(s), {} secret reference(s)",
        context.config.project.name,
        report.environment.name,
        report.declarative_resources,
        report.external_resources,
        report.checked_secrets.len()
    );
    output("validate", human, report)
}

fn plan_or_explain(
    cli: &Cli,
    args: ScanArgs,
    command: &'static str,
) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let plan = build_engine_plan(&context, &args)?;
    let report = scan_report(&context, &plan)?;
    let human = format_scan_report(command, &report);
    output(command, human, report)
}

fn run(cli: &Cli, args: RunArgs) -> Result<CommandOutput, CliError> {
    if args.loop_mode {
        return Err(CliError::not_supported(
            "run --loop",
            "the local development loop supervisor is excluded from this explicit one-package run slice",
            "later loop/streaming supervisor",
        ));
    }
    let explicit = explicit_run_args(args)?;
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let resource = context.resource(&explicit.resource_id)?;
    ensure_run_resource_supported(resource.plan())?;
    let state_store_path = context.state_store_path()?;
    let destination_path = run_duckdb_destination_path(&context)?;
    let plan = build_engine_plan(
        &context,
        &ScanArgs {
            resource_id: explicit.resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            package_id: Some(explicit.package_id.clone()),
        },
    )?;
    let report = futures_executor::block_on(run_local_file_to_duckdb_checkpoint(
        LocalFileDuckDbRunRequest {
            resource,
            plan,
            package_root: context.package_root(),
            destination_path,
            state_store_path,
            pipeline_id: explicit.pipeline_id.clone(),
            target: explicit.target.clone(),
            package_id: explicit.package_id.clone(),
            checkpoint_id: explicit.checkpoint_id.clone(),
            after_receipt_verified: None,
        },
    ))?;
    let cli_report = RunCliReport::from_report(&report);
    output(
        "run",
        format!(
            "ran resource {} into package {} for target {}; checkpoint {} committed after DuckDB receipt verification, crossing the firn line",
            cli_report.resource_id,
            cli_report.package_hash,
            cli_report.target,
            cli_report.checkpoint_id
        ),
        cli_report,
    )
}

fn explicit_run_args(args: RunArgs) -> Result<ExplicitRunArgs, CliError> {
    Ok(ExplicitRunArgs {
        resource_id: required_run_arg(args.resource_id, "--resource")?,
        pipeline_id: PipelineId::new(required_run_arg(args.pipeline_id, "--pipeline")?)?,
        target: TargetName::new(required_run_arg(args.target, "--target")?)?,
        package_id: required_run_arg(args.package_id, "--package-id")?,
        checkpoint_id: CheckpointId::new(required_run_arg(args.checkpoint_id, "--checkpoint-id")?)?,
    })
}

fn required_run_arg(value: Option<String>, name: &str) -> Result<String, CliError> {
    value.ok_or_else(|| CliError::usage(format!("run requires {name}")))
}

fn ensure_run_resource_supported(plan: &CompiledResourcePlan) -> Result<(), CliError> {
    match plan {
        CompiledResourcePlan::Files(_) => Ok(()),
        CompiledResourcePlan::Rest(_) => Err(CliError::not_supported(
            "run",
            "REST resource execution is excluded from this explicit local file to DuckDB slice",
            "REST source runtime wired to package/checkpoint orchestration",
        )),
        CompiledResourcePlan::Sql(_) => Err(CliError::not_supported(
            "run",
            "SQL resource execution is excluded from this explicit local file to DuckDB slice",
            "SQL source runtime wired to package/checkpoint orchestration",
        )),
    }
}

fn run_duckdb_destination_path(context: &ProjectContext) -> Result<PathBuf, CliError> {
    let Some(raw_path) = context.environment.destination.strip_prefix("duckdb://") else {
        return Err(CliError::not_supported(
            "run",
            format!(
                "destination URI `{}` is unsupported for this slice; only local duckdb:// destinations are supported",
                context.environment.destination
            ),
            "local DuckDB destination runtime",
        ));
    };
    if raw_path.trim().is_empty() || raw_path.contains("://") {
        return Err(CliError::not_supported(
            "run",
            format!(
                "destination URI `{}` is malformed or non-local for this slice; expected duckdb://path",
                context.environment.destination
            ),
            "local DuckDB destination path",
        ));
    }
    let destination_path = context
        .duckdb_destination_path()
        .expect("duckdb:// prefix was checked");
    firn_dest_duckdb::DuckDbDestination::new(&destination_path)?;
    Ok(destination_path)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExplicitRunArgs {
    resource_id: String,
    pipeline_id: PipelineId,
    target: TargetName,
    package_id: String,
    checkpoint_id: CheckpointId,
}

fn preview(cli: &Cli, args: ScanArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let resource = context.resource(&args.resource_id)?;
    let plan = build_engine_plan(&context, &args)?;
    match preview_one_batch(resource, &plan) {
        Ok(report) => output(
            "preview",
            format!(
                "previewed resource {}: {} row(s), {} byte(s); wrote no package, destination data, or checkpoint",
                report.resource_id, report.row_count, report.byte_count
            ),
            report,
        ),
        Err(error) if lower_runtime_missing(&error) => Err(CliError::not_supported(
            "preview",
            error.message,
            "resource runtime open implementation",
        )),
        Err(error) => Err(error.into()),
    }
}

fn sql(cli: &Cli, args: SqlArgs) -> Result<CommandOutput, CliError> {
    let query = system_sql::read_only_query(&args.query)?;
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let report = system_sql::run(&context, query)?;
    output(
        "sql",
        format!(
            "sql returned {} row(s) from local system history",
            report.row_count()
        ),
        report,
    )
}

fn inspect(cli: &Cli, args: InspectArgs) -> Result<CommandOutput, CliError> {
    match args.noun {
        InspectNoun::Package(path) => inspect_package(path),
        noun => {
            let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
            match noun {
                InspectNoun::Project => output(
                    "inspect project",
                    format!(
                        "project {} env {}",
                        context.config.project.name, context.environment.name
                    ),
                    json!({
                        "root": context.root,
                        "config": context.config,
                        "environment": context.environment,
                    }),
                ),
                InspectNoun::Resources => {
                    let resources = resource_summaries(&context);
                    output(
                        "inspect resources",
                        format!("{} compiled resource(s)", resources.len()),
                        resources,
                    )
                }
                InspectNoun::Resource(id) => {
                    let resource = context.resource(&id)?;
                    output(
                        "inspect resource",
                        format!("resource {id}"),
                        ResourceSummary::from_resource(resource),
                    )
                }
                InspectNoun::Lock => {
                    let lock = require_lock(&context)?;
                    output(
                        "inspect lock",
                        format!(
                            "lockfile v{} for project {}",
                            lock.version, lock.project.name
                        ),
                        lock,
                    )
                }
                InspectNoun::Destinations => {
                    let runtime = context.destination_runtime();
                    output(
                        "inspect destinations",
                        "inspected destination capabilities".to_owned(),
                        json!({
                            "environment_destination": context.environment.destination,
                            "runtime": runtime,
                            "locked": context.lock.as_ref().map(|lock| &lock.destinations),
                        }),
                    )
                }
                InspectNoun::Package(_) => unreachable!("package noun handled before project load"),
            }
        }
    }
}

fn diff_schema(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let lock = require_lock(&context)?;
    let destination_sheets = lock
        .destinations
        .values()
        .map(|destination| destination.sheet.clone())
        .collect::<Vec<_>>();
    let regenerated = generate_lockfile(
        &context.config,
        &context.resources,
        lock.dependency_tuple.clone(),
        &destination_sheets,
        BTreeMap::new(),
    )?;
    let diffs = firn_project::diff_lockfiles(lock, &regenerated)?;
    output(
        "diff schema",
        format!("{} lock diff(s)", diffs.len()),
        json!({ "diffs": diffs }),
    )
}

fn contract(command: ContractCommand) -> Result<CommandOutput, CliError> {
    match command {
        ContractCommand::Show { trust } => {
            let trust = trust.unwrap_or_else(|| "governed".to_owned());
            let policy = match trust.as_str() {
                "experimental" => ContractPolicy::for_trust(firn_kernel::TrustLevel::Experimental),
                "governed" => ContractPolicy::for_trust(firn_kernel::TrustLevel::Governed),
                "financial" => ContractPolicy::for_trust(firn_kernel::TrustLevel::Financial),
                "serving" => ContractPolicy::for_trust(firn_kernel::TrustLevel::Serving),
                "evolve" => ContractPolicy::evolve(),
                "freeze" => ContractPolicy::freeze(),
                other => {
                    return Err(CliError::usage(format!(
                        "unknown contract policy `{other}`"
                    )));
                }
            };
            output(
                "contract show",
                format!("contract policy {trust}"),
                json!({ "policy": trust, "contract": policy }),
            )
        }
        ContractCommand::Freeze { contract } => Err(CliError::not_supported(
            "contract freeze",
            format!(
                "contract snapshot writes are not exposed by lower crates{}",
                contract
                    .as_ref()
                    .map(|name| format!(" for `{name}`"))
                    .unwrap_or_default()
            ),
            "contract registry/snapshot writer",
        )),
        ContractCommand::Test { contract } => Err(CliError::not_supported(
            "contract test",
            format!(
                "contract fixture execution is not exposed by lower crates{}",
                contract
                    .as_ref()
                    .map(|name| format!(" for `{name}`"))
                    .unwrap_or_default()
            ),
            "contract fixture runner",
        )),
    }
}

fn state(cli: &Cli, command: StateCommand) -> Result<CommandOutput, CliError> {
    match command {
        StateCommand::Show(args) => {
            let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
            let store = context.state_store()?;
            let scope = scope_key(args.scope_json.as_deref())?;
            let head = store.head(
                &PipelineId::new(args.pipeline_id)?,
                &ResourceId::new(args.resource_id)?,
                &scope,
            )?;
            output(
                "state show",
                if head.is_some() {
                    "state head found".to_owned()
                } else {
                    "no committed state head".to_owned()
                },
                json!({ "scope": scope, "head": head }),
            )
        }
        StateCommand::History(args) => {
            let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
            let store = context.state_store()?;
            let scope = scope_key(args.scope_json.as_deref())?;
            let history = store.history(
                &PipelineId::new(args.pipeline_id)?,
                &ResourceId::new(args.resource_id)?,
                &scope,
            )?;
            output(
                "state history",
                format!("{} checkpoint(s)", history.len()),
                json!({ "scope": scope, "history": history }),
            )
        }
        StateCommand::Rewind(args) => {
            let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
            let store = context.state_store()?;
            let report = store.rewind(firn_kernel::RewindRequest {
                marker_checkpoint_id: CheckpointId::new(args.marker_checkpoint_id)?,
                pipeline_id: PipelineId::new(args.scope.pipeline_id)?,
                resource_id: ResourceId::new(args.scope.resource_id)?,
                scope: scope_key(args.scope.scope_json.as_deref())?,
                target_checkpoint_id: CheckpointId::new(args.target_checkpoint_id)?,
            })?;
            output(
                "state rewind",
                format!(
                    "rewound to {}; {} package(s) ahead of state",
                    report.head.delta.checkpoint_id,
                    report.packages_ahead.len()
                ),
                report,
            )
        }
        StateCommand::Migrate => Err(CliError::not_supported(
            "state migrate",
            "state migration programs and fixtures are not exposed by lower crates",
            "checkpoint state migration runner",
        )),
        StateCommand::Recover => Err(CliError::not_supported(
            "state recover",
            "destination mirror recovery is not exposed by lower crates",
            "destination mirror recovery API",
        )),
    }
}

fn resume(cli: &Cli, args: ResumeArgs) -> Result<CommandOutput, CliError> {
    let _context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    Err(CliError::not_supported(
        "resume",
        format!(
            "run recovery orchestration is not exposed by lower crates{}",
            args.run_id
                .as_ref()
                .map(|id| format!(" for run `{id}`"))
                .unwrap_or_default()
        ),
        "run ledger/recovery orchestrator",
    ))
}

fn replay_package(args: ReplayPackageArgs) -> Result<CommandOutput, CliError> {
    let reader = PackageReader::open(&args.package_dir)?;
    let view = reader.replay_view()?;
    Err(CliError::not_supported(
        "replay package",
        format!(
            "package {} is replayable, but destination/checkpoint replay orchestration is not exposed",
            view.package_hash
        ),
        "destination replay API that records receipts and commits checkpoints",
    ))
}

fn backfill(cli: &Cli, args: BackfillArgs) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    if let Some(resource_id) = &args.resource_id {
        context.resource(resource_id)?;
    }
    Err(CliError::not_supported(
        "backfill",
        "bounded historical planning and checkpoint-safe replay windows are not exposed by lower crates",
        "backfill planner/orchestrator",
    ))
}

fn package(cli: &Cli, command: PackageCommand) -> Result<CommandOutput, CliError> {
    match command {
        PackageCommand::Ls { packages_dir } => {
            let root = match packages_dir {
                Some(path) => path,
                None => {
                    ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?.package_root()
                }
            };
            let packages = list_packages(root)?;
            output(
                "package ls",
                format!("{} package(s)", packages.len()),
                json!({ "packages": packages }),
            )
        }
        PackageCommand::Gc { packages_dir } => {
            let root = match packages_dir {
                Some(path) => path.display().to_string(),
                None => ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?
                    .package_root()
                    .display()
                    .to_string(),
            };
            Err(CliError::not_supported(
                "package gc",
                format!(
                    "retention-safe GC for package root `{root}` requires checkpoint proof checks"
                ),
                "package retention planner tied to CheckpointStore history",
            ))
        }
        PackageCommand::Verify { package_dir } => {
            let reader = PackageReader::open(&package_dir)?;
            let report = reader.verify()?;
            output(
                "package verify",
                format!(
                    "verified package {}: {} file(s), {} archive segment(s)",
                    report.package_hash,
                    report.checked_files.len(),
                    report.checked_archives.len()
                ),
                PackageVerifyReport {
                    package_hash: report.package_hash,
                    checked_files: report.checked_files,
                    checked_archives: report.checked_archives,
                },
            )
        }
        PackageCommand::Archive(args) => package_archive(args),
    }
}

fn package_archive(args: PackageArchiveArgs) -> Result<CommandOutput, CliError> {
    if args.format != "parquet" {
        return Err(CliError::usage(format!(
            "unsupported package archive format `{}`",
            args.format
        )));
    }

    let report = firn_package::persist_package_parquet_archive(&args.package_dir, args.force)?;
    let archive_byte_count = report
        .segments
        .iter()
        .map(|segment| segment.archive_byte_count)
        .sum::<u64>();
    output(
        "package archive",
        format!(
            "archived package {} as parquet: status {}, {} segment(s), {} byte(s), fidelity {}",
            report.package_hash,
            package_archive_status(&report.status),
            report.segments.len(),
            archive_byte_count,
            report.fidelity_report_path
        ),
        PackageArchiveCliReport {
            command: "package archive",
            package_hash: report.package_hash,
            format: report.format,
            status: report.status,
            fidelity_report_path: report.fidelity_report_path,
            fidelity_statement: report.fidelity_statement,
            segments: report.segments,
        },
    )
}

fn doctor(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let mut checks = vec![
        DoctorCheck::passed("project_file", "firn.toml parsed and environment resolved")
            .with_details(project_health_details(&context)),
        DoctorCheck::passed(
            "declarative_resources",
            format!("{} resource(s) compiled", context.resources.len()),
        ),
    ];

    let resolver = FileResourceSourceResolver::new(&context.root);
    let provider = context.secret_provider();
    match validate_project(
        &context.config,
        Some(&context.environment.name),
        &resolver,
        &provider,
    ) {
        Ok(report) => checks.push(
            DoctorCheck::passed(
                "secrets",
                format!(
                    "{} secret reference(s) resolved",
                    report.checked_secrets.len()
                ),
            )
            .with_details(secret_check_details(&report)),
        ),
        Err(error) => checks.push(DoctorCheck::failed("secrets", error.to_string())),
    }

    checks.push(python_check(&context));
    checks.extend(destination_checks(context.destination_runtime()));
    checks.push(ledger_destination_drift_check(&context));

    let failed = checks
        .iter()
        .filter(|check| matches!(check.status, CheckStatus::Failed))
        .count();
    let unsupported = checks
        .iter()
        .filter(|check| matches!(check.status, CheckStatus::Unsupported))
        .count();
    let report = DoctorReport {
        checks,
        failed,
        unsupported,
    };
    report_output(
        "doctor",
        if failed == 0 {
            format!("doctor completed with {unsupported} unsupported check(s)")
        } else {
            format!("doctor found {failed} failed check(s)")
        },
        report,
        if failed == 0 { 0 } else { 1 },
    )
}

fn project_health_details(context: &ProjectContext) -> serde_json::Value {
    json!({
        "project_root": context.root,
        "selected_environment": context.environment.name,
        "compiled_resources": context.resources.len(),
        "lockfile_present": context.lock.is_some(),
    })
}

fn secret_check_details(report: &firn_project::ProjectValidationReport) -> serde_json::Value {
    json!({
        "count": report.checked_secrets.len(),
        "references": report
            .checked_secrets
            .iter()
            .map(|check| check.uri.as_str())
            .collect::<Vec<_>>(),
    })
}

fn status(cli: &Cli) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let report = status_freshness::evaluate(&context)?;
    let exit_code = report.exit_code();
    let human = status_freshness::human_summary(&report);
    report_output("status", human, report, exit_code)
}

fn inspect_package(path: PathBuf) -> Result<CommandOutput, CliError> {
    let reader = PackageReader::open(&path)?;
    output(
        "inspect package",
        format!(
            "package {} status {}",
            reader.manifest().package_hash,
            reader.manifest().lifecycle.status.as_str()
        ),
        reader.manifest(),
    )
}

fn build_engine_plan(context: &ProjectContext, args: &ScanArgs) -> Result<EnginePlan, CliError> {
    let resource = context.resource(&args.resource_id)?;
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let validation_program = compile_validation_program(&policy, &observed_schema)?;
    let request = scan_request(resource.descriptor(), args)?;
    let input = EnginePlanInput {
        request,
        validation_program,
        boundedness: PlanBoundedness::Bounded,
        package_id: args
            .package_id
            .clone()
            .unwrap_or_else(|| format!("cli-{}", resource.descriptor().resource_id)),
    };
    Planner::new()
        .plan_tier_b(resource, input)
        .map_err(CliError::from)
}

fn scan_request(
    descriptor: &firn_kernel::ResourceDescriptor,
    args: &ScanArgs,
) -> Result<ScanRequest, CliError> {
    let filters = args
        .filters
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            Ok(ScanPredicate {
                predicate_id: PredicateId::new(format!("p{:03}", index + 1))?,
                expression: expression.clone(),
            })
        })
        .collect::<firn_kernel::Result<Vec<_>>>()?;
    Ok(ScanRequest {
        resource_id: descriptor.resource_id.clone(),
        projection: args.projection.clone(),
        filters,
        limit: args.limit,
        order_by: args
            .order_by
            .iter()
            .map(|order| parse_order_by(order))
            .collect::<Result<Vec<_>, _>>()?,
        scope: descriptor.state_scope.clone(),
    })
}

fn parse_order_by(raw: &str) -> Result<OrderBy, CliError> {
    let (field, direction) = raw.split_once(':').unwrap_or((raw, "asc"));
    let direction = match direction {
        "asc" => SortDirection::Asc,
        "desc" => SortDirection::Desc,
        other => {
            return Err(CliError::usage(format!(
                "unsupported order direction `{other}`"
            )));
        }
    };
    Ok(OrderBy {
        field: field.to_owned(),
        direction,
    })
}

fn scan_report(context: &ProjectContext, plan: &EnginePlan) -> Result<ScanPlanReport, CliError> {
    let resource = context.resource(plan.scan.request.resource_id.as_str())?;
    Ok(ScanPlanReport {
        project: context.config.project.name.clone(),
        environment: context.environment.name.clone(),
        resource_id: plan.scan.request.resource_id.to_string(),
        will_fetch: FetchReport {
            partitions: plan
                .scan
                .partitions
                .iter()
                .map(partition_report)
                .collect(),
            projection: plan.scan.request.projection.clone().unwrap_or_default(),
            filters: plan
                .scan
                .request
                .filters
                .iter()
                .map(|predicate| predicate.expression.clone())
                .collect(),
            limit: plan.scan.request.limit,
        },
        pushdown: PushdownReport {
            pushed: plan.explain.pushed_predicates.clone(),
            inexact: plan.explain.inexact_predicates.clone(),
            unsupported: plan.explain.unsupported_predicates.clone(),
        },
        ddl_preview: UnsupportedReport {
            supported: false,
            reason: "destination DDL preview requires a destination commit plan over a package schema; current lower APIs expose package commit planning, not scan-to-DDL planning".to_owned(),
            required_lower_layer: "scan/resource schema to destination DDL planning facade".to_owned(),
        },
        delivery_guarantee: format!("{:?}", plan.explain.delivery_guarantee),
        state_advancement: StateAdvancementReport {
            scope: serde_json::to_value(&resource.descriptor().state_scope)
                .map_err(json_cli_error)?,
            cursor: resource
                .descriptor()
                .cursor
                .as_ref()
                .map(|cursor| cursor.field.clone()),
            advances_after: "destination receipt is recorded and CheckpointStore::commit verifies coverage".to_owned(),
        },
        explain: plan.explain.clone(),
        package_id: plan.package_id.clone(),
    })
}

fn partition_report(partition: &PartitionPlan) -> PartitionReport {
    PartitionReport {
        partition_id: partition.partition_id.to_string(),
        scope_kind: format!("{:?}", partition.scope.kind()),
        metadata: partition.metadata.clone(),
    }
}

fn preview_one_batch(
    resource: &firn_declarative::CompiledResource,
    plan: &EnginePlan,
) -> firn_kernel::Result<PreviewReport> {
    let partition = plan
        .scan
        .partitions
        .first()
        .ok_or_else(|| FirnError::data("preview plan has no partitions"))?
        .clone();
    let mut stream = futures_executor::block_on(resource.open(partition))?;
    let batch = futures_executor::block_on(async { stream.next().await })
        .ok_or_else(|| FirnError::data("resource produced no preview batch"))??;
    Ok(PreviewReport {
        resource_id: batch.header.resource_id.to_string(),
        batch_id: batch.header.batch_id.to_string(),
        partition_id: batch.header.partition_id.to_string(),
        row_count: batch.header.row_count,
        byte_count: batch.header.byte_count,
        writes: WriteEffects::default(),
    })
}

fn list_packages(root: PathBuf) -> Result<Vec<PackageListEntry>, CliError> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut entries = fs::read_dir(&root)
        .map_err(|error| FirnError::data(format!("read {}: {error}", root.display())))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| FirnError::data(format!("read {}: {error}", root.display())))?;
    entries.sort_by_key(|entry| entry.path());

    let mut packages = Vec::new();
    for entry in entries {
        let path = entry.path();
        if path.join(MANIFEST_FILE).exists() {
            let manifest = firn_package::read_manifest(&path)?;
            packages.push(PackageListEntry {
                path: path.display().to_string(),
                package_hash: manifest.package_hash,
                status: manifest.lifecycle.status.as_str().to_owned(),
                segments: manifest.identity.segments.len(),
            });
        }
    }
    Ok(packages)
}

fn python_check(context: &ProjectContext) -> DoctorCheck {
    let require_free_threaded = context.config.python.require_free_threaded.unwrap_or(false);
    let Some(interpreter) = &context.config.python.interpreter else {
        return if has_python_resource(context) {
            DoctorCheck::failed(
                "python",
                "python.interpreter is required because at least one Python resource is configured",
            )
            .with_details(json!({
                "python_resources": python_resource_count(context),
                "require_free_threaded": require_free_threaded,
            }))
        } else {
            DoctorCheck::skipped("python", "no python.interpreter configured")
        };
    };

    let path = configured_interpreter_path(&context.root, interpreter);
    let (executable, report) = match probe_python_interpreter(&path) {
        Ok(report) => report,
        Err(message) => {
            return DoctorCheck::failed("python", message)
                .with_details(python_config_details(&path, require_free_threaded));
        }
    };
    let details = python_probe_details(&executable, &report, require_free_threaded);

    if (report.major, report.minor) < (MIN_PYTHON_MAJOR, MIN_PYTHON_MINOR) {
        return DoctorCheck::failed(
            "python",
            format!(
                "Python interpreter {} is older than required {MIN_PYTHON_MAJOR}.{MIN_PYTHON_MINOR}",
                python_version(&report)
            ),
        )
        .with_details(details);
    }

    if require_free_threaded && !python_can_parallelize(&report) {
        return DoctorCheck::failed(
            "python",
            "configured Python resources require a free-threaded interpreter with the GIL disabled",
        )
        .with_details(details);
    }

    DoctorCheck::passed(
        "python",
        format!(
            "configured interpreter {} passed Python doctor probe",
            python_version(&report)
        ),
    )
    .with_details(details)
}

fn configured_interpreter_path(root: &Path, interpreter: &str) -> PathBuf {
    let path = PathBuf::from(interpreter);
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}

fn has_python_resource(context: &ProjectContext) -> bool {
    python_resource_count(context) > 0
}

fn python_resource_count(context: &ProjectContext) -> usize {
    context
        .config
        .resources
        .values()
        .filter(|resource| matches!(resource.source_kind(), ResourceSourceKind::Python { .. }))
        .count()
}

fn probe_python_interpreter(path: &Path) -> Result<(PathBuf, PythonProbeReport), String> {
    let metadata = fs::metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!("configured interpreter is missing at {}", path.display())
        } else {
            format!(
                "configured interpreter metadata could not be read at {}: {error}",
                path.display()
            )
        }
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "configured interpreter is not a file at {}",
            path.display()
        ));
    }
    if !is_executable(&metadata) {
        return Err(format!(
            "configured interpreter is not executable at {}",
            path.display()
        ));
    }

    let executable = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let output = ProcessCommand::new(&executable)
        .arg("-I")
        .arg("-c")
        .arg(PYTHON_INTERPRETER_PROBE)
        .output()
        .map_err(|error| format!("configured interpreter could not be executed: {error}"))?;
    if !output.status.success() {
        return Err(match output.status.code() {
            Some(code) => {
                format!("configured interpreter inspection exited unsuccessfully with code {code}")
            }
            None => "configured interpreter inspection exited unsuccessfully".to_owned(),
        });
    }

    let report: PythonProbeReport = serde_json::from_slice(&output.stdout).map_err(|error| {
        format!("configured interpreter did not emit valid inspection JSON: {error}")
    })?;
    validate_python_probe_report(&report)?;
    Ok((executable, report))
}

#[cfg(unix)]
fn is_executable(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &fs::Metadata) -> bool {
    true
}

fn validate_python_probe_report(report: &PythonProbeReport) -> Result<(), String> {
    if report.version != python_version(report) {
        return Err("configured interpreter emitted inconsistent version metadata".to_owned());
    }
    if report.can_parallelize_python != python_can_parallelize(report) {
        return Err("configured interpreter emitted inconsistent GIL metadata".to_owned());
    }
    Ok(())
}

fn python_config_details(path: &Path, require_free_threaded: bool) -> serde_json::Value {
    json!({
        "executable": path.display().to_string(),
        "require_free_threaded": require_free_threaded,
    })
}

fn python_probe_details(
    executable: &Path,
    report: &PythonProbeReport,
    require_free_threaded: bool,
) -> serde_json::Value {
    json!({
        "executable": executable.display().to_string(),
        "reported_executable": report.executable,
        "version": python_version(report),
        "implementation": report.implementation,
        "gil_enabled": report.gil_enabled,
        "free_threaded_build": report.free_threaded_build,
        "can_parallelize_python": python_can_parallelize(report),
        "require_free_threaded": require_free_threaded,
    })
}

fn python_version(report: &PythonProbeReport) -> String {
    format!("{}.{}.{}", report.major, report.minor, report.micro)
}

fn python_can_parallelize(report: &PythonProbeReport) -> bool {
    report.free_threaded_build && !report.gil_enabled
}

fn destination_checks(runtime: DestinationRuntime) -> Vec<DoctorCheck> {
    match runtime {
        DestinationRuntime::DuckDb {
            database_path,
            icu_probe,
            ..
        } => {
            let mut checks = vec![
                DoctorCheck::passed("destination", "DuckDB destination capabilities loaded")
                    .with_details(json!({
                        "kind": "duck_db",
                        "database_path": database_path,
                    })),
            ];
            checks.push(match icu_probe {
                DoctorProbe::Passed => DoctorCheck::passed("duckdb_icu", "ICU probe passed")
                    .with_details(duckdb_icu_details(&database_path, true, None)),
                DoctorProbe::Failed { message } => {
                    DoctorCheck::failed("duckdb_icu", message.clone())
                        .with_details(duckdb_icu_details(&database_path, false, Some(message)))
                }
                DoctorProbe::Skipped { reason } => {
                    DoctorCheck::skipped("duckdb_icu", reason.clone()).with_details(json!({
                        "database_path": database_path,
                        "database_exists": false,
                        "probe": "icu_sort_key",
                        "reason": reason,
                    }))
                }
            });
            checks
        }
        DestinationRuntime::Postgres { .. } => vec![DoctorCheck::passed(
            "destination",
            "Postgres destination capabilities loaded",
        )],
        DestinationRuntime::Unsupported { reason, .. } => {
            vec![DoctorCheck::unsupported("destination", reason)]
        }
    }
}

fn duckdb_icu_details(
    database_path: &str,
    available: bool,
    diagnostic: Option<String>,
) -> serde_json::Value {
    json!({
        "database_path": database_path,
        "database_exists": true,
        "probe": "icu_sort_key",
        "available": available,
        "diagnostic": diagnostic,
    })
}

fn ledger_destination_drift_check(context: &ProjectContext) -> DoctorCheck {
    match doctor_drift::probe(context) {
        Ok(probe) => match probe.status {
            DriftStatus::Passed => DoctorCheck::passed("ledger_destination_drift", probe.message)
                .with_details(probe.details),
            DriftStatus::Failed => DoctorCheck::failed("ledger_destination_drift", probe.message)
                .with_details(probe.details),
            DriftStatus::Skipped => DoctorCheck::skipped("ledger_destination_drift", probe.message)
                .with_details(probe.details),
            DriftStatus::Unsupported => {
                DoctorCheck::unsupported("ledger_destination_drift", probe.message)
                    .with_details(probe.details)
            }
        },
        Err(error) => DoctorCheck::failed("ledger_destination_drift", error.message),
    }
}

fn scope_key(scope_json: Option<&str>) -> Result<ScopeKey, CliError> {
    match scope_json {
        Some(json) => serde_json::from_str(json).map_err(|error| {
            CliError::usage(format!("--scope-json must encode a ScopeKey: {error}"))
        }),
        None => Ok(ScopeKey::Resource),
    }
}

fn lower_runtime_missing(error: &FirnError) -> bool {
    error
        .message
        .contains("execution is outside the MVP compiler crate")
}

fn output<T: Serialize>(
    command: &'static str,
    human: String,
    value: T,
) -> Result<CommandOutput, CliError> {
    report_output(command, human, value, 0)
}

fn report_output<T: Serialize>(
    command: &'static str,
    human: String,
    value: T,
    exit_code: i32,
) -> Result<CommandOutput, CliError> {
    Ok(CommandOutput {
        command,
        exit_code,
        human,
        json: serde_json::to_value(value).map_err(json_cli_error)?,
    })
}

fn json_cli_error(error: serde_json::Error) -> CliError {
    CliError::from(FirnError::internal(error.to_string()))
}

fn format_scan_report(command: &str, report: &ScanPlanReport) -> String {
    let pushed = report.pushdown.pushed.len();
    let inexact = report.pushdown.inexact.len();
    let unsupported = report.pushdown.unsupported.len();
    format!(
        "{command} {}: {} partition(s), {pushed} pushed predicate(s), {inexact} inexact, {unsupported} unsupported, guarantee {}",
        report.resource_id,
        report.will_fetch.partitions.len(),
        report.delivery_guarantee
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ResourceSummary {
    descriptor: firn_kernel::ResourceDescriptor,
    capabilities: firn_kernel::ResourceCapabilities,
}

impl ResourceSummary {
    fn from_resource(resource: &firn_declarative::CompiledResource) -> Self {
        Self {
            descriptor: resource.descriptor().clone(),
            capabilities: resource.capabilities().clone(),
        }
    }
}

fn resource_summaries(context: &ProjectContext) -> Vec<ResourceSummary> {
    context
        .resources
        .iter()
        .map(ResourceSummary::from_resource)
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ScanPlanReport {
    project: String,
    environment: String,
    resource_id: String,
    will_fetch: FetchReport,
    pushdown: PushdownReport,
    ddl_preview: UnsupportedReport,
    delivery_guarantee: String,
    state_advancement: StateAdvancementReport,
    explain: firn_engine::ExplainData,
    package_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct FetchReport {
    partitions: Vec<PartitionReport>,
    projection: Vec<String>,
    filters: Vec<String>,
    limit: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PartitionReport {
    partition_id: String,
    scope_kind: String,
    metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PushdownReport {
    pushed: Vec<firn_engine::PredicateExplain>,
    inexact: Vec<firn_engine::PredicateExplain>,
    unsupported: Vec<firn_engine::PredicateExplain>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct UnsupportedReport {
    supported: bool,
    reason: String,
    required_lower_layer: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct StateAdvancementReport {
    scope: serde_json::Value,
    cursor: Option<String>,
    advances_after: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PreviewReport {
    resource_id: String,
    batch_id: String,
    partition_id: String,
    row_count: u64,
    byte_count: u64,
    writes: WriteEffects,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RunCliReport {
    command: &'static str,
    resource_id: String,
    pipeline_id: String,
    target: String,
    package_id: String,
    package_dir: String,
    package_hash: String,
    package_status: String,
    checkpoint_id: String,
    checkpoint: RunCheckpointReport,
    receipt_id: String,
    receipt_source: RunReceiptSourceReport,
    row_count: u64,
    segment_count: usize,
    writes: WriteEffects,
}

impl RunCliReport {
    fn from_report(report: &LocalFileDuckDbRunReport) -> Self {
        Self {
            command: "run",
            resource_id: report.checkpoint.delta.resource_id.to_string(),
            pipeline_id: report.checkpoint.delta.pipeline_id.to_string(),
            target: report.receipt.target.to_string(),
            package_id: report.package_id.clone(),
            package_dir: report.package_dir.display().to_string(),
            package_hash: report.package_hash.to_string(),
            package_status: report.package_status.as_str().to_owned(),
            checkpoint_id: report.checkpoint.delta.checkpoint_id.to_string(),
            checkpoint: RunCheckpointReport {
                checkpoint_id: report.checkpoint.delta.checkpoint_id.to_string(),
                status: report.checkpoint.status.as_str().to_owned(),
                committed: report.checkpoint.committed_at_ms.is_some(),
                is_head: report.checkpoint.is_head,
                committed_at_ms: report.checkpoint.committed_at_ms,
            },
            receipt_id: report.receipt.receipt_id.to_string(),
            receipt_source: RunReceiptSourceReport::from(&report.receipt_source),
            row_count: report.row_count,
            segment_count: report.segment_count,
            writes: WriteEffects {
                package: true,
                destination: true,
                checkpoint: true,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RunCheckpointReport {
    checkpoint_id: String,
    status: String,
    committed: bool,
    is_head: bool,
    committed_at_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RunReceiptSourceReport {
    DuckDbCommit {
        duplicate: bool,
        no_op: bool,
        package_receipt_recorded: bool,
    },
    SuppliedDurableReceipt,
}

impl From<&PreparedReceiptSource> for RunReceiptSourceReport {
    fn from(source: &PreparedReceiptSource) -> Self {
        match source {
            PreparedReceiptSource::DuckDbCommit {
                duplicate,
                package_receipt_recorded,
            } => Self::DuckDbCommit {
                duplicate: *duplicate,
                no_op: *duplicate,
                package_receipt_recorded: *package_receipt_recorded,
            },
            PreparedReceiptSource::SuppliedDurableReceipt => Self::SuppliedDurableReceipt,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
struct WriteEffects {
    package: bool,
    destination: bool,
    checkpoint: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageListEntry {
    path: String,
    package_hash: String,
    status: String,
    segments: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageVerifyReport {
    package_hash: String,
    checked_files: Vec<firn_package::FileEntry>,
    checked_archives: Vec<firn_package::ArchiveSegmentMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageArchiveCliReport {
    command: &'static str,
    package_hash: String,
    format: String,
    status: firn_package::PackageArchiveWriteStatus,
    fidelity_report_path: String,
    fidelity_statement: String,
    segments: Vec<firn_package::ArchiveSegmentMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct PythonProbeReport {
    executable: String,
    version: String,
    major: u16,
    minor: u16,
    micro: u16,
    implementation: String,
    gil_enabled: bool,
    free_threaded_build: bool,
    can_parallelize_python: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DoctorReport {
    checks: Vec<DoctorCheck>,
    failed: usize,
    unsupported: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DoctorCheck {
    name: String,
    status: CheckStatus,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<serde_json::Value>,
}

impl DoctorCheck {
    fn passed(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Passed,
            message: message.into(),
            details: None,
        }
    }

    fn failed(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Failed,
            message: message.into(),
            details: None,
        }
    }

    fn skipped(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Skipped,
            message: message.into(),
            details: None,
        }
    }

    fn unsupported(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Unsupported,
            message: message.into(),
            details: None,
        }
    }

    fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckStatus {
    Passed,
    Failed,
    Skipped,
    Unsupported,
}

const HELP_TEXT: &str = r#"firn 0.1.0

Usage:
  firn [--project PATH] [--env NAME] [--json] <command>

Commands:
  init [DIR] [--name NAME] [--force]
  validate
  plan <RESOURCE> [--select a,b] [--filter EXPR] [--limit N]
  explain <RESOURCE> [--select a,b] [--filter EXPR] [--limit N]
  run --resource RESOURCE --pipeline ID --target TARGET --package-id ID --checkpoint-id ID [--loop]
  preview <RESOURCE> [--select a,b] [--filter EXPR] [--limit N]
  sql <QUERY>
  inspect project|resources|resource <ID>|lock|destinations|package <DIR>
  diff schema
  contract freeze|show|test
  state show|history --pipeline ID --resource ID [--scope-json JSON]
  state rewind --pipeline ID --resource ID --target-checkpoint ID --marker-checkpoint ID [--scope-json JSON]
  state migrate|recover
  resume [RUN_ID]
  replay package <DIR>
  backfill [RESOURCE]
  package ls [DIR]
  package gc [DIR]
  package verify <DIR>
  package archive <DIR> [--format parquet] [--force]
  doctor
  status
"#;

fn package_archive_status(status: &firn_package::PackageArchiveWriteStatus) -> &'static str {
    match status {
        firn_package::PackageArchiveWriteStatus::Written => "written",
        firn_package::PackageArchiveWriteStatus::Skipped => "skipped",
        firn_package::PackageArchiveWriteStatus::Replaced => "replaced",
    }
}
