use std::{
    ffi::OsString,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::{Arg, ArgAction, ArgMatches, Command as ClapCommand, error::ErrorKind};

use crate::{output::CliError, suggestions};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const ROOT_COMMANDS: &[&str] = &[
    "help", "version", "init", "validate", "plan", "explain", "run", "preview", "sql", "inspect",
    "diff", "contract", "state", "resume", "replay", "backfill", "package", "doctor", "status",
];
const INSPECT_NOUNS: &[&str] = &[
    "project",
    "resources",
    "resource",
    "lock",
    "destinations",
    "destination",
    "package",
    "run",
];
const DIFF_SUBCOMMANDS: &[&str] = &["schema"];
const CONTRACT_SUBCOMMANDS: &[&str] = &["freeze", "show", "test"];
const STATE_SUBCOMMANDS: &[&str] = &["show", "history", "rewind", "migrate", "recover"];
const REPLAY_SUBCOMMANDS: &[&str] = &["package"];
const PACKAGE_SUBCOMMANDS: &[&str] = &["ls", "gc", "verify", "archive"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cli {
    pub json: bool,
    pub no_color: bool,
    pub project: Option<PathBuf>,
    pub env: Option<String>,
    pub command: Command,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Help(String),
    Version,
    Init(InitArgs),
    Validate,
    Plan(ScanArgs),
    Explain(ScanArgs),
    Run(RunArgs),
    Preview(ScanArgs),
    Sql(SqlArgs),
    Inspect(InspectArgs),
    DiffSchema,
    Contract(ContractCommand),
    State(StateCommand),
    Resume(ResumeArgs),
    ReplayPackage(ReplayPackageArgs),
    Backfill(BackfillArgs),
    Package(PackageCommand),
    Doctor,
    Status,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitArgs {
    pub directory: Option<PathBuf>,
    pub name: Option<String>,
    pub force: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanArgs {
    pub resource_id: String,
    pub destination_uri: Option<String>,
    pub target: Option<String>,
    pub projection: Option<Vec<String>>,
    pub filters: Vec<String>,
    pub limit: Option<u64>,
    pub order_by: Vec<String>,
    pub package_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RunArgs {
    pub resource_id: Option<String>,
    pub pipeline_id: Option<String>,
    pub destination_uri: Option<String>,
    pub target: Option<String>,
    pub package_id: Option<String>,
    pub checkpoint_id: Option<String>,
    pub loop_mode: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlArgs {
    pub query: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectArgs {
    pub noun: InspectNoun,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InspectNoun {
    Project,
    Resources,
    Resource(String),
    Lock,
    Destinations,
    Package(PathBuf),
    Run(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContractCommand {
    Freeze { contract: Option<String> },
    Show { trust: Option<String> },
    Test { contract: Option<String> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateCommand {
    Show(StateScopeArgs),
    History(StateScopeArgs),
    Rewind(RewindArgs),
    Migrate,
    Recover(StateRecoverArgs),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateScopeArgs {
    pub pipeline_id: Option<String>,
    pub resource_id: String,
    pub scope_json: Option<String>,
    pub scope: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RewindArgs {
    pub scope: StateScopeArgs,
    pub target_checkpoint_id: String,
    pub marker_checkpoint_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateRecoverArgs {
    pub package_dir: PathBuf,
    pub destination_uri: String,
    pub receipt_id: Option<String>,
    pub target: Option<String>,
    pub merge_dedup: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ResumeArgs {
    pub run_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayPackageArgs {
    pub package_dir: PathBuf,
    pub destination_uri: Option<String>,
    pub target: Option<String>,
    pub merge_dedup: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackfillArgs {
    pub resource_id: String,
    pub from: String,
    pub to: String,
    pub target: Option<String>,
    pub execute: bool,
    pub slice_size: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PackageCommand {
    Ls { packages_dir: Option<PathBuf> },
    Gc { packages_dir: Option<PathBuf> },
    Verify { package_dir: PathBuf },
    Archive(PackageArchiveArgs),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageArchiveArgs {
    pub package_dir: PathBuf,
    pub format: String,
    pub force: bool,
}

impl Cli {
    pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, CliError> {
        let mut raw = args
            .into_iter()
            .map(|arg| {
                arg.into_string()
                    .map_err(|_| CliError::usage("cdf arguments must be valid UTF-8"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if !raw.is_empty() {
            raw.remove(0);
        }

        let mut json = false;
        let mut no_color = false;
        let mut project = None;
        let mut env = None;
        let mut remaining = Vec::new();
        let mut index = 0;
        while index < raw.len() {
            match raw[index].as_str() {
                "--json" => {
                    json = true;
                    index += 1;
                }
                "--no-color" => {
                    no_color = true;
                    index += 1;
                }
                "--project" => {
                    let value = raw.get(index + 1).ok_or_else(|| {
                        CliError::usage("--project requires a project directory or cdf.toml path")
                    })?;
                    project = Some(PathBuf::from(value));
                    index += 2;
                }
                "--env" => {
                    let value = raw
                        .get(index + 1)
                        .ok_or_else(|| CliError::usage("--env requires an environment name"))?;
                    env = Some(value.clone());
                    index += 2;
                }
                other => {
                    remaining.push(other.to_owned());
                    index += 1;
                }
            }
        }

        let command = parse_command(&remaining)?;
        Ok(Self {
            json,
            no_color,
            project,
            env,
            command,
        })
    }
}

fn parse_command(args: &[String]) -> Result<Command, CliError> {
    if args.is_empty() {
        return render_help(&[]).map(Command::Help);
    }
    if args[0] == "help" {
        return render_help(&args[1..]).map(Command::Help);
    }

    let mut argv = Vec::with_capacity(args.len() + 1);
    argv.push("cdf".to_owned());
    argv.extend(args.iter().cloned());

    match cli_command().try_get_matches_from(argv) {
        Ok(matches) => command_from_matches(&matches),
        Err(error) if error.kind() == ErrorKind::DisplayHelp => {
            Ok(Command::Help(error.to_string()))
        }
        Err(error) if error.kind() == ErrorKind::DisplayVersion => Ok(Command::Version),
        Err(error) => {
            Err(CliError::usage(error.to_string()).with_suggestions(command_suggestions(args)))
        }
    }
}

fn command_from_matches(matches: &ArgMatches) -> Result<Command, CliError> {
    match matches.subcommand() {
        Some(("version", subcommand)) => {
            no_extra_values("version", &values(subcommand, "extra"))?;
            Ok(Command::Version)
        }
        Some(("init", subcommand)) => parse_init(subcommand).map(Command::Init),
        Some(("validate", subcommand)) => {
            no_extra_values("validate", &values(subcommand, "extra"))?;
            Ok(Command::Validate)
        }
        Some(("plan", subcommand)) => parse_scan("plan", subcommand, true).map(Command::Plan),
        Some(("explain", subcommand)) => {
            parse_scan("explain", subcommand, true).map(Command::Explain)
        }
        Some(("run", subcommand)) => parse_run(subcommand).map(Command::Run),
        Some(("preview", subcommand)) => {
            parse_scan("preview", subcommand, false).map(Command::Preview)
        }
        Some(("sql", subcommand)) => parse_sql(subcommand).map(Command::Sql),
        Some(("inspect", subcommand)) => parse_inspect(subcommand).map(Command::Inspect),
        Some(("diff", subcommand)) => parse_diff(subcommand),
        Some(("contract", subcommand)) => parse_contract(subcommand).map(Command::Contract),
        Some(("state", subcommand)) => parse_state(subcommand).map(Command::State),
        Some(("resume", subcommand)) => parse_resume(subcommand).map(Command::Resume),
        Some(("replay", subcommand)) => parse_replay(subcommand),
        Some(("backfill", subcommand)) => parse_backfill(subcommand).map(Command::Backfill),
        Some(("package", subcommand)) => parse_package(subcommand).map(Command::Package),
        Some(("doctor", subcommand)) => {
            no_extra_values("doctor", &values(subcommand, "extra"))?;
            Ok(Command::Doctor)
        }
        Some(("status", subcommand)) => {
            no_extra_values("status", &values(subcommand, "extra"))?;
            Ok(Command::Status)
        }
        Some((other, _)) => Err(CliError::usage(format!("unknown command `{other}`"))
            .with_suggestions(command_suggestions(&[other.to_owned()]))),
        None => render_help(&[]).map(Command::Help),
    }
}

fn parse_init(matches: &ArgMatches) -> Result<InitArgs, CliError> {
    let directories = values(matches, "directory");
    let directory = optional_path_arg("init", &directories)?;
    Ok(InitArgs {
        directory,
        name: string_value(matches, "name"),
        force: matches.get_flag("force"),
    })
}

fn parse_scan(
    command: &str,
    matches: &ArgMatches,
    accepts_target: bool,
) -> Result<ScanArgs, CliError> {
    let resource_id = resource_arg(
        command,
        &values(matches, "resource_arg"),
        string_value(matches, "resource"),
        "accepts one resource id",
    )?
    .ok_or_else(|| CliError::usage(format!("{command} requires a resource id")))?;
    let target = if accepts_target {
        string_value(matches, "target")
    } else {
        None
    };
    let destination_uri = if accepts_target {
        string_value(matches, "to")
    } else {
        None
    };
    let projection = string_value(matches, "projection").map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|field| !field.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    });
    let limit = string_value(matches, "limit")
        .map(|value| parse_u64("--limit", &value))
        .transpose()?;
    Ok(ScanArgs {
        resource_id,
        destination_uri,
        target,
        projection,
        filters: values(matches, "filter"),
        limit,
        order_by: values(matches, "order_by"),
        package_id: string_value(matches, "package_id"),
    })
}

fn parse_run(matches: &ArgMatches) -> Result<RunArgs, CliError> {
    Ok(RunArgs {
        resource_id: resource_arg(
            "run",
            &values(matches, "resource_arg"),
            string_value(matches, "resource"),
            "accepts at most one resource id",
        )?,
        pipeline_id: string_value(matches, "pipeline"),
        destination_uri: string_value(matches, "to"),
        target: string_value(matches, "target"),
        package_id: string_value(matches, "package_id"),
        checkpoint_id: string_value(matches, "checkpoint_id"),
        loop_mode: matches.get_flag("loop"),
    })
}

fn parse_sql(matches: &ArgMatches) -> Result<SqlArgs, CliError> {
    let query = values(matches, "query");
    if query.is_empty() {
        return Err(CliError::usage("sql requires a query string"));
    }
    Ok(SqlArgs {
        query: query.join(" "),
    })
}

fn parse_inspect(matches: &ArgMatches) -> Result<InspectArgs, CliError> {
    let Some((noun, subcommand)) = matches.subcommand() else {
        return Err(CliError::usage("inspect requires a noun"));
    };
    let values = values(subcommand, "values");
    let noun = match noun {
        "project" => {
            no_extra_values("inspect project", &values)?;
            InspectNoun::Project
        }
        "resources" => {
            no_extra_values("inspect resources", &values)?;
            InspectNoun::Resources
        }
        "resource" => {
            let id = required_first_value("inspect resource", &values, "requires a resource id")?;
            no_extra_values("inspect resource", &values[1..])?;
            InspectNoun::Resource(id)
        }
        "lock" => {
            no_extra_values("inspect lock", &values)?;
            InspectNoun::Lock
        }
        "destinations" | "destination" => {
            no_extra_values("inspect destinations", &values)?;
            InspectNoun::Destinations
        }
        "package" => {
            let path =
                required_first_value("inspect package", &values, "requires a package directory")?;
            no_extra_values("inspect package", &values[1..])?;
            InspectNoun::Package(PathBuf::from(path))
        }
        "run" => {
            let id = required_first_value("inspect run", &values, "requires a run id")?;
            no_extra_values("inspect run", &values[1..])?;
            InspectNoun::Run(id)
        }
        other => {
            return Err(unknown_subcommand_error(
                &["inspect"],
                other,
                INSPECT_NOUNS,
                "unknown inspect noun",
            ));
        }
    };
    Ok(InspectArgs { noun })
}

fn parse_diff(matches: &ArgMatches) -> Result<Command, CliError> {
    match matches.subcommand() {
        Some(("schema", subcommand)) => {
            no_extra_values("diff schema", &values(subcommand, "extra"))?;
            Ok(Command::DiffSchema)
        }
        Some((other, _)) => Err(unknown_subcommand_error(
            &["diff"],
            other,
            DIFF_SUBCOMMANDS,
            "unknown diff subcommand",
        )),
        None => Err(CliError::usage("diff requires subcommand `schema`")),
    }
}

fn parse_contract(matches: &ArgMatches) -> Result<ContractCommand, CliError> {
    let Some((subcommand, matches)) = matches.subcommand() else {
        return Err(CliError::usage(
            "contract requires one of freeze, show, or test",
        ));
    };
    match subcommand {
        "freeze" => Ok(ContractCommand::Freeze {
            contract: optional_named_or_positional(
                "--contract",
                string_value(matches, "contract"),
                &values(matches, "value"),
            )?,
        }),
        "show" => Ok(ContractCommand::Show {
            trust: optional_named_or_positional(
                "--trust",
                string_value(matches, "trust"),
                &values(matches, "value"),
            )?,
        }),
        "test" => Ok(ContractCommand::Test {
            contract: optional_named_or_positional(
                "--contract",
                string_value(matches, "contract"),
                &values(matches, "value"),
            )?,
        }),
        other => Err(unknown_subcommand_error(
            &["contract"],
            other,
            CONTRACT_SUBCOMMANDS,
            "unknown contract subcommand",
        )),
    }
}

fn parse_state(matches: &ArgMatches) -> Result<StateCommand, CliError> {
    let Some((subcommand, matches)) = matches.subcommand() else {
        return Err(CliError::usage(
            "state requires one of show, history, rewind, migrate, or recover",
        ));
    };
    match subcommand {
        "show" => parse_state_scope(matches).map(StateCommand::Show),
        "history" => parse_state_scope(matches).map(StateCommand::History),
        "rewind" => parse_rewind(matches).map(StateCommand::Rewind),
        "migrate" => {
            no_extra_values("state migrate", &values(matches, "extra"))?;
            Ok(StateCommand::Migrate)
        }
        "recover" => parse_state_recover(matches).map(StateCommand::Recover),
        other => Err(unknown_subcommand_error(
            &["state"],
            other,
            STATE_SUBCOMMANDS,
            "unknown state subcommand",
        )),
    }
}

fn parse_state_scope(matches: &ArgMatches) -> Result<StateScopeArgs, CliError> {
    let resource_id = resource_arg(
        "state command",
        &values(matches, "resource_arg"),
        string_value(matches, "resource"),
        "accepts at most one resource id",
    )?
    .ok_or_else(|| CliError::usage("state command requires RESOURCE or --resource"))?;
    Ok(StateScopeArgs {
        pipeline_id: string_value(matches, "pipeline"),
        resource_id,
        scope_json: string_value(matches, "scope_json"),
        scope: values(matches, "scope"),
    })
}

fn parse_rewind(matches: &ArgMatches) -> Result<RewindArgs, CliError> {
    Ok(RewindArgs {
        scope: parse_state_scope(matches)?,
        target_checkpoint_id: string_value(matches, "target_checkpoint")
            .ok_or_else(|| CliError::usage("state rewind requires --to or --target-checkpoint"))?,
        marker_checkpoint_id: string_value(matches, "marker_checkpoint")
            .unwrap_or_else(|| mint_cli_id("rewind-marker")),
    })
}

fn parse_state_recover(matches: &ArgMatches) -> Result<StateRecoverArgs, CliError> {
    reject_values_as_unknown("state recover", matches)?;
    Ok(StateRecoverArgs {
        package_dir: string_value(matches, "package")
            .map(PathBuf::from)
            .ok_or_else(|| CliError::usage("state recover requires --package"))?,
        destination_uri: string_value(matches, "to")
            .ok_or_else(|| CliError::usage("state recover requires --to"))?,
        receipt_id: string_value(matches, "receipt"),
        target: string_value(matches, "target"),
        merge_dedup: string_value(matches, "merge_dedup"),
    })
}

fn parse_resume(matches: &ArgMatches) -> Result<ResumeArgs, CliError> {
    Ok(ResumeArgs {
        run_id: resource_arg(
            "resume",
            &values(matches, "run_arg"),
            string_value(matches, "run"),
            "accepts at most one run id",
        )?,
    })
}

fn parse_replay(matches: &ArgMatches) -> Result<Command, CliError> {
    match matches.subcommand() {
        Some(("package", subcommand)) => {
            parse_replay_package(subcommand).map(Command::ReplayPackage)
        }
        Some((other, _)) => Err(unknown_subcommand_error(
            &["replay"],
            other,
            REPLAY_SUBCOMMANDS,
            "unknown replay subcommand",
        )),
        None => Err(CliError::usage("replay requires subcommand `package`")),
    }
}

fn parse_replay_package(matches: &ArgMatches) -> Result<ReplayPackageArgs, CliError> {
    let package_dir = required_single_path(
        "replay package",
        &values(matches, "package_dir"),
        "requires a package directory",
        "accepts exactly one package directory",
    )?;
    Ok(ReplayPackageArgs {
        package_dir,
        destination_uri: string_value(matches, "to"),
        target: string_value(matches, "target"),
        merge_dedup: string_value(matches, "merge_dedup"),
    })
}

fn parse_backfill(matches: &ArgMatches) -> Result<BackfillArgs, CliError> {
    let positional_resource = resource_arg(
        "backfill",
        &values(matches, "resource_arg"),
        None,
        "accepts at most one resource id",
    )?;
    let resource_option = string_value(matches, "resource");
    if let (Some(positional), Some(option)) = (&positional_resource, &resource_option)
        && positional != option
    {
        return Err(CliError::usage(
            "backfill positional RESOURCE and --resource must match when both are supplied",
        ));
    }
    let slice_size = string_value(matches, "slice_size")
        .map(|value| parse_u64("--slice-size", &value))
        .transpose()?;
    Ok(BackfillArgs {
        resource_id: positional_resource
            .or(resource_option)
            .ok_or_else(|| CliError::usage("backfill requires RESOURCE or --resource"))?,
        from: string_value(matches, "from")
            .ok_or_else(|| CliError::usage("backfill requires --from"))?,
        to: string_value(matches, "to").ok_or_else(|| CliError::usage("backfill requires --to"))?,
        target: string_value(matches, "target"),
        execute: matches.get_flag("execute"),
        slice_size,
    })
}

fn parse_package(matches: &ArgMatches) -> Result<PackageCommand, CliError> {
    let Some((subcommand, matches)) = matches.subcommand() else {
        return Err(CliError::usage(
            "package requires one of ls, gc, verify, or archive",
        ));
    };
    match subcommand {
        "ls" => Ok(PackageCommand::Ls {
            packages_dir: optional_path_arg("package ls", &values(matches, "packages_dir"))?,
        }),
        "gc" => Ok(PackageCommand::Gc {
            packages_dir: optional_path_arg("package gc", &values(matches, "packages_dir"))?,
        }),
        "verify" => Ok(PackageCommand::Verify {
            package_dir: required_single_path(
                "package verify",
                &values(matches, "package_dir"),
                "requires a package directory",
                "accepts exactly one package directory",
            )?,
        }),
        "archive" => parse_package_archive(matches).map(PackageCommand::Archive),
        other => Err(unknown_subcommand_error(
            &["package"],
            other,
            PACKAGE_SUBCOMMANDS,
            "unknown package subcommand",
        )),
    }
}

fn parse_package_archive(matches: &ArgMatches) -> Result<PackageArchiveArgs, CliError> {
    Ok(PackageArchiveArgs {
        package_dir: required_single_path(
            "package archive",
            &values(matches, "package_dir"),
            "requires a package directory",
            "accepts exactly one package directory",
        )?,
        format: string_value(matches, "format").unwrap_or_else(|| "parquet".to_owned()),
        force: matches.get_flag("force"),
    })
}

pub(crate) fn cli_command() -> ClapCommand {
    cmd("cdf")
        .version(VERSION)
        .about("Continuous Data Framework CLI")
        .arg(flag("no_color", "no-color").global(true))
        .arg_required_else_help(false)
        .disable_help_subcommand(true)
        .subcommand(cmd("help").arg(values_arg("command").value_name("COMMAND")))
        .subcommand(cmd("version").arg(values_arg("extra").hide(true)))
        .subcommand(
            cmd("init")
                .arg(values_arg("directory").value_name("DIR"))
                .arg(option("name", "name", "NAME"))
                .arg(flag("force", "force")),
        )
        .subcommand(cmd("validate").arg(values_arg("extra").hide(true)))
        .subcommand(scan_command("plan", true))
        .subcommand(scan_command("explain", true))
        .subcommand(run_command())
        .subcommand(scan_command("preview", false))
        .subcommand(
            cmd("sql").arg(
                values_arg("query")
                    .value_name("QUERY")
                    .allow_hyphen_values(true)
                    .trailing_var_arg(true),
            ),
        )
        .subcommand(inspect_command())
        .subcommand(cmd("diff").subcommand(cmd("schema").arg(values_arg("extra").hide(true))))
        .subcommand(contract_command())
        .subcommand(state_command())
        .subcommand(resume_command())
        .subcommand(
            cmd("replay").subcommand(
                cmd("package")
                    .arg(values_arg("package_dir").value_name("DIR"))
                    .arg(option("to", "to", "DEST"))
                    .arg(option("target", "target", "TARGET"))
                    .arg(option("merge_dedup", "merge-dedup", "POLICY")),
            ),
        )
        .subcommand(backfill_command())
        .subcommand(package_command())
        .subcommand(cmd("doctor").arg(values_arg("extra").hide(true)))
        .subcommand(cmd("status").arg(values_arg("extra").hide(true)))
}

fn scan_command(name: &'static str, accepts_target: bool) -> ClapCommand {
    let mut command = cmd(name)
        .arg(values_arg("resource_arg").value_name("RESOURCE"))
        .arg(option("resource", "resource", "RESOURCE"))
        .arg(option("projection", "select", "FIELDS").alias("projection"))
        .arg(append_option("filter", "filter", "EXPR"))
        .arg(option("limit", "limit", "N"))
        .arg(append_option("order_by", "order-by", "FIELD[:asc|desc]"))
        .arg(option("package_id", "package-id", "ID"));
    if accepts_target {
        command = command
            .arg(option("to", "to", "DEST"))
            .arg(option("target", "target", "TARGET"));
    }
    command
}

fn run_command() -> ClapCommand {
    cmd("run")
        .arg(values_arg("resource_arg").value_name("RESOURCE"))
        .arg(option("resource", "resource", "RESOURCE"))
        .arg(option("pipeline", "pipeline", "ID"))
        .arg(option("to", "to", "DEST"))
        .arg(option("target", "target", "TARGET"))
        .arg(option("package_id", "package-id", "ID"))
        .arg(option("checkpoint_id", "checkpoint-id", "ID"))
        .arg(flag("loop", "loop"))
}

fn inspect_command() -> ClapCommand {
    cmd("inspect")
        .subcommand(cmd("project").arg(values_arg("values").hide(true)))
        .subcommand(cmd("resources").arg(values_arg("values").hide(true)))
        .subcommand(cmd("resource").arg(values_arg("values").value_name("ID")))
        .subcommand(cmd("lock").arg(values_arg("values").hide(true)))
        .subcommand(cmd("destinations").arg(values_arg("values").hide(true)))
        .subcommand(cmd("destination").arg(values_arg("values").hide(true)))
        .subcommand(cmd("package").arg(values_arg("values").value_name("DIR")))
        .subcommand(cmd("run").arg(values_arg("values").value_name("RUN_ID")))
}

fn contract_command() -> ClapCommand {
    cmd("contract")
        .subcommand(
            cmd("freeze")
                .arg(values_arg("value").value_name("CONTRACT"))
                .arg(option("contract", "contract", "CONTRACT")),
        )
        .subcommand(
            cmd("show")
                .arg(values_arg("value").value_name("TRUST"))
                .arg(option("trust", "trust", "TRUST")),
        )
        .subcommand(
            cmd("test")
                .arg(values_arg("value").value_name("CONTRACT"))
                .arg(option("contract", "contract", "CONTRACT")),
        )
}

fn state_command() -> ClapCommand {
    cmd("state")
        .subcommand(state_scope_command("show"))
        .subcommand(state_scope_command("history"))
        .subcommand(
            state_scope_command("rewind")
                .arg(
                    option("target_checkpoint", "target-checkpoint", "CHECKPOINT")
                        .visible_alias("to"),
                )
                .arg(option(
                    "marker_checkpoint",
                    "marker-checkpoint",
                    "CHECKPOINT",
                )),
        )
        .subcommand(cmd("migrate").arg(values_arg("extra").hide(true)))
        .subcommand(
            cmd("recover")
                .arg(option("package", "package", "DIR"))
                .arg(option("to", "to", "DEST"))
                .arg(option("receipt", "receipt", "ID"))
                .arg(option("target", "target", "TARGET"))
                .arg(option("merge_dedup", "merge-dedup", "POLICY"))
                .arg(values_arg("values").hide(true)),
        )
}

fn state_scope_command(name: &'static str) -> ClapCommand {
    cmd(name)
        .arg(values_arg("resource_arg").value_name("RESOURCE"))
        .arg(option("pipeline", "pipeline", "ID"))
        .arg(option("resource", "resource", "RESOURCE"))
        .arg(append_option("scope", "scope", "KEY=VALUE"))
        .arg(option("scope_json", "scope-json", "JSON"))
}

fn resume_command() -> ClapCommand {
    cmd("resume")
        .arg(values_arg("run_arg").value_name("RUN_ID"))
        .arg(
            option("run", "run", "RUN_ID")
                .alias("run-id")
                .value_name("RUN_ID"),
        )
}

fn backfill_command() -> ClapCommand {
    cmd("backfill")
        .arg(values_arg("resource_arg").value_name("RESOURCE"))
        .arg(option("resource", "resource", "RESOURCE"))
        .arg(option("from", "from", "CURSOR"))
        .arg(option("to", "to", "CURSOR"))
        .arg(option("target", "target", "TARGET"))
        .arg(flag("execute", "execute"))
        .arg(option("slice_size", "slice-size", "N"))
}

fn package_command() -> ClapCommand {
    cmd("package")
        .subcommand(cmd("ls").arg(values_arg("packages_dir").value_name("DIR")))
        .subcommand(cmd("gc").arg(values_arg("packages_dir").value_name("DIR")))
        .subcommand(cmd("verify").arg(values_arg("package_dir").value_name("DIR")))
        .subcommand(
            cmd("archive")
                .arg(values_arg("package_dir").value_name("DIR"))
                .arg(option("format", "format", "FORMAT"))
                .arg(flag("force", "force")),
        )
}

fn cmd(name: &'static str) -> ClapCommand {
    ClapCommand::new(name).args_override_self(true)
}

fn option(id: &'static str, long: &'static str, value_name: &'static str) -> Arg {
    Arg::new(id)
        .long(long)
        .value_name(value_name)
        .num_args(1)
        .action(ArgAction::Set)
}

fn append_option(id: &'static str, long: &'static str, value_name: &'static str) -> Arg {
    Arg::new(id)
        .long(long)
        .value_name(value_name)
        .num_args(1)
        .action(ArgAction::Append)
}

fn flag(id: &'static str, long: &'static str) -> Arg {
    Arg::new(id).long(long).action(ArgAction::SetTrue)
}

fn values_arg(id: &'static str) -> Arg {
    Arg::new(id).num_args(0..).action(ArgAction::Append)
}

pub(crate) fn render_help(path: &[String]) -> Result<String, CliError> {
    if !path.is_empty() {
        let mut argv = Vec::with_capacity(path.len() + 2);
        argv.push("cdf".to_owned());
        argv.extend(path.iter().cloned());
        argv.push("--help".to_owned());
        return match cli_command().try_get_matches_from(argv) {
            Err(error) if error.kind() == ErrorKind::DisplayHelp => Ok(error.to_string()),
            Err(_) => Err(unknown_help_topic_error(path)),
            Ok(_) => Err(unknown_help_topic_error(path)),
        };
    }

    let mut command = cli_command();
    let mut buffer = Vec::new();
    command
        .write_help(&mut buffer)
        .map_err(|error| CliError::usage(format!("render help: {error}")))?;
    String::from_utf8(buffer).map_err(|_| CliError::usage("help text must be valid UTF-8"))
}

fn string_value(matches: &ArgMatches, name: &str) -> Option<String> {
    matches.get_one::<String>(name).cloned()
}

fn values(matches: &ArgMatches, name: &str) -> Vec<String> {
    matches
        .get_many::<String>(name)
        .map(|values| values.cloned().collect())
        .unwrap_or_default()
}

fn resource_arg(
    command: &str,
    positional_values: &[String],
    option_value: Option<String>,
    too_many_message: &str,
) -> Result<Option<String>, CliError> {
    if positional_values.len() > 1 {
        return Err(CliError::usage(format!("{command} {too_many_message}")));
    }
    match (positional_values.first(), option_value) {
        (Some(positional), Some(option)) if positional != &option => Err(CliError::usage(format!(
            "{command} positional RESOURCE and --resource must match when both are supplied"
        ))),
        (Some(positional), _) => Ok(Some(positional.clone())),
        (None, option) => Ok(option),
    }
}

fn optional_path_arg(command: &str, args: &[String]) -> Result<Option<PathBuf>, CliError> {
    match args {
        [] => Ok(None),
        [one] => Ok(Some(PathBuf::from(one))),
        _ => Err(CliError::usage(format!(
            "{command} accepts at most one path"
        ))),
    }
}

fn required_first_value(command: &str, args: &[String], message: &str) -> Result<String, CliError> {
    args.first()
        .cloned()
        .ok_or_else(|| CliError::usage(format!("{command} {message}")))
}

fn required_single_path(
    command: &str,
    args: &[String],
    missing_message: &str,
    too_many_message: &str,
) -> Result<PathBuf, CliError> {
    match args {
        [] => Err(CliError::usage(format!("{command} {missing_message}"))),
        [one] => Ok(PathBuf::from(one)),
        _ => Err(CliError::usage(format!("{command} {too_many_message}"))),
    }
}

fn optional_named_or_positional(
    option_name: &str,
    option_value: Option<String>,
    positional_values: &[String],
) -> Result<Option<String>, CliError> {
    if positional_values.len() > 1 {
        return Err(CliError::usage("expected at most one value"));
    }
    match (positional_values.first(), option_value) {
        (Some(positional), Some(option)) if positional != &option => Err(CliError::usage(format!(
            "{option_name} conflicts with positional value"
        ))),
        (Some(positional), _) => Ok(Some(positional.clone())),
        (None, option) => Ok(option),
    }
}

fn no_extra_values(command: &str, args: &[String]) -> Result<(), CliError> {
    if args.is_empty() {
        Ok(())
    } else {
        Err(CliError::usage(format!(
            "{command} does not accept extra argument `{}`",
            args[0]
        )))
    }
}

fn reject_values_as_unknown(command: &str, matches: &ArgMatches) -> Result<(), CliError> {
    let values = values(matches, "values");
    if let Some(value) = values.first() {
        return Err(CliError::usage(format!(
            "unknown {command} option `{value}`"
        )));
    }
    Ok(())
}

fn parse_u64(option: &str, value: &str) -> Result<u64, CliError> {
    value
        .parse()
        .map_err(|error| CliError::usage(format!("{option} must be an unsigned integer: {error}")))
}

fn unknown_subcommand_error(
    path: &[&str],
    value: &str,
    candidates: &[&str],
    message: &str,
) -> CliError {
    CliError::usage(format!("{message} `{value}`"))
        .with_suggestions(command_path_suggestions(path, value, candidates))
}

fn unknown_help_topic_error(path: &[String]) -> CliError {
    let topic = path.join(" ");
    CliError::usage(format!("unknown help topic `{topic}`"))
        .with_suggestions(command_suggestions(path))
}

fn command_suggestions(args: &[String]) -> Vec<String> {
    let Some(first) = args.first() else {
        return Vec::new();
    };
    if first.starts_with('-') {
        return Vec::new();
    }
    if !ROOT_COMMANDS.contains(&first.as_str()) {
        return command_path_suggestions(&[], first, ROOT_COMMANDS);
    }
    let Some(second) = args.get(1) else {
        return Vec::new();
    };
    if second.starts_with('-') {
        return Vec::new();
    }
    match first.as_str() {
        "inspect" => command_path_suggestions(&["inspect"], second, INSPECT_NOUNS),
        "diff" => command_path_suggestions(&["diff"], second, DIFF_SUBCOMMANDS),
        "contract" => command_path_suggestions(&["contract"], second, CONTRACT_SUBCOMMANDS),
        "state" => command_path_suggestions(&["state"], second, STATE_SUBCOMMANDS),
        "replay" => command_path_suggestions(&["replay"], second, REPLAY_SUBCOMMANDS),
        "package" => command_path_suggestions(&["package"], second, PACKAGE_SUBCOMMANDS),
        _ => Vec::new(),
    }
}

fn command_path_suggestions(path: &[&str], value: &str, candidates: &[&str]) -> Vec<String> {
    suggestions::nearest(
        value,
        candidates.iter().map(|candidate| (*candidate).to_owned()),
    )
    .into_iter()
    .take(1)
    .map(|candidate| {
        let mut command = String::from("cdf");
        for segment in path {
            command.push(' ');
            command.push_str(segment);
        }
        command.push(' ');
        command.push_str(&candidate);
        command
    })
    .collect()
}

fn mint_cli_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{prefix}-{}-{nanos}", std::process::id())
}
