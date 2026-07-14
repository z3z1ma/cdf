use std::{
    ffi::OsString,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::{Arg, ArgAction, ArgMatches, Command as ClapCommand, error::ErrorKind};

use crate::{
    output::CliError,
    suggestions,
    terminal::{PolicyMode, TerminalPolicy, Verbosity},
};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const ROOT_COMMANDS: &[&str] = &[
    "help", "version", "init", "add", "validate", "plan", "explain", "run", "preview", "sql",
    "inspect", "diff", "schema", "contract", "state", "resume", "replay", "backfill", "package",
    "doctor", "status",
];
const INSPECT_NOUNS: &[&str] = &[
    "project",
    "resources",
    "resource",
    "lock",
    "destinations",
    "package",
    "run",
];
const DIFF_SUBCOMMANDS: &[&str] = &["schema"];
const SCHEMA_SUBCOMMANDS: &[&str] = &["discover", "pin", "show", "diff", "promote"];
const CONTRACT_SUBCOMMANDS: &[&str] = &["freeze", "show", "test"];
const STATE_SUBCOMMANDS: &[&str] = &["show", "history", "rewind", "recover"];
const REPLAY_SUBCOMMANDS: &[&str] = &["package"];
const PACKAGE_SUBCOMMANDS: &[&str] = &["ls", "gc", "verify", "archive"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cli {
    pub json: bool,
    pub terminal: TerminalPolicy,
    pub project: Option<PathBuf>,
    pub env: Option<String>,
    pub command: Command,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Help(String),
    Version,
    Init(InitArgs),
    Add(AddArgs),
    Validate(ValidateArgs),
    Plan(ScanArgs),
    Explain(ScanArgs),
    Run(RunArgs),
    Preview(ScanArgs),
    Sql(SqlArgs),
    Inspect(InspectArgs),
    DiffSchema,
    Schema(SchemaCommand),
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
pub struct AddArgs {
    pub resource_id: String,
    pub location: String,
    pub dry_run: bool,
    pub records: Option<String>,
    pub cursor: Option<String>,
    pub cursor_param: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidateArgs {
    pub deep: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanArgs {
    pub resource_id: String,
    pub destination_uri: Option<String>,
    pub projection: Option<Vec<String>>,
    pub filters: Vec<String>,
    pub limit: Option<u64>,
    pub order_by: Vec<String>,
    pub no_pin: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RunArgs {
    pub resource_id: Option<String>,
    pub destination_uri: Option<String>,
    pub jobs: Option<u16>,
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
pub enum SchemaCommand {
    Discover(SchemaDiscoverArgs),
    Pin(SchemaResourceArgs),
    Show(SchemaResourceArgs),
    Diff(SchemaResourceArgs),
    Promote(SchemaPromoteArgs),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaPromoteArgs {
    pub resource_id: String,
    pub types: Vec<String>,
    pub execute: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaDiscoverArgs {
    pub resource_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaResourceArgs {
    pub resource_id: String,
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
        let mut color = PolicyMode::Auto;
        let mut progress = PolicyMode::Auto;
        let mut unicode = PolicyMode::Auto;
        let mut quiet = false;
        let mut verbose = 0_u8;
        let mut project = None;
        let mut env = None;
        let mut remaining = Vec::new();
        let mut index = 0;
        while index < raw.len() {
            match raw[index].as_str() {
                "--" => {
                    remaining.extend(raw[index..].iter().cloned());
                    break;
                }
                "--json" => {
                    json = true;
                    index += 1;
                }
                "-q" | "--quiet" => {
                    quiet = true;
                    index += 1;
                }
                "-v" | "--verbose" => {
                    verbose = verbose.saturating_add(1);
                    index += 1;
                }
                compact
                    if compact.starts_with('-')
                        && compact.len() > 2
                        && compact[1..].chars().all(|character| character == 'v') =>
                {
                    verbose =
                        verbose.saturating_add((compact.len() - 1).min(usize::from(u8::MAX)) as u8);
                    index += 1;
                }
                option if option == "--color" || option.starts_with("--color=") => {
                    let (value, consumed) = policy_value(&raw, index, "--color")?;
                    color = PolicyMode::parse("--color", value)?;
                    index += consumed;
                }
                option if option == "--progress" || option.starts_with("--progress=") => {
                    let (value, consumed) = policy_value(&raw, index, "--progress")?;
                    progress = PolicyMode::parse("--progress", value)?;
                    index += consumed;
                }
                option if option == "--unicode" || option.starts_with("--unicode=") => {
                    let (value, consumed) = policy_value(&raw, index, "--unicode")?;
                    unicode = PolicyMode::parse("--unicode", value)?;
                    index += consumed;
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

        if quiet && verbose > 0 {
            return Err(CliError::usage(
                "-q/--quiet cannot be combined with -v/--verbose; choose one",
            ));
        }
        let terminal = TerminalPolicy {
            color,
            progress,
            unicode,
            verbosity: if quiet {
                Verbosity::Quiet
            } else if verbose > 0 {
                Verbosity::Verbose(verbose)
            } else {
                Verbosity::Normal
            },
        };
        let command = parse_command(&remaining)?;
        Ok(Self {
            json,
            terminal,
            project,
            env,
            command,
        })
    }
}

fn policy_value<'a>(
    args: &'a [String],
    index: usize,
    flag: &str,
) -> Result<(&'a str, usize), CliError> {
    if let Some((_, value)) = args[index].split_once('=') {
        if value.is_empty() {
            return Err(CliError::usage(format!("{flag} requires a value")));
        }
        return Ok((value, 1));
    }
    args.get(index + 1)
        .map(|value| (value.as_str(), 2))
        .ok_or_else(|| CliError::usage(format!("{flag} requires a value")))
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
        Some(("add", subcommand)) => parse_add(subcommand).map(Command::Add),
        Some(("validate", subcommand)) => {
            no_extra_values("validate", &values(subcommand, "extra"))?;
            Ok(Command::Validate(ValidateArgs {
                deep: subcommand.get_flag("deep"),
            }))
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
        Some(("schema", subcommand)) => parse_schema(subcommand).map(Command::Schema),
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

fn parse_add(matches: &ArgMatches) -> Result<AddArgs, CliError> {
    let values = values(matches, "values");
    if values.len() != 2 {
        return Err(CliError::usage(
            "add requires RESOURCE_ID and URL_OR_PATH arguments",
        ));
    }
    Ok(AddArgs {
        resource_id: values[0].clone(),
        location: values[1].clone(),
        dry_run: matches.get_flag("dry_run"),
        records: string_value(matches, "records"),
        cursor: string_value(matches, "cursor"),
        cursor_param: string_value(matches, "cursor_param"),
    })
}

fn parse_scan(
    command: &str,
    matches: &ArgMatches,
    accepts_target: bool,
) -> Result<ScanArgs, CliError> {
    let resource_id = single_positional_arg(
        command,
        &values(matches, "resource_arg"),
        "accepts one resource id",
    )?
    .ok_or_else(|| CliError::usage(format!("{command} requires a resource id")))?;
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
        projection,
        filters: values(matches, "filter"),
        limit,
        order_by: values(matches, "order_by"),
        no_pin: accepts_target && matches.get_flag("no_pin"),
    })
}

fn parse_run(matches: &ArgMatches) -> Result<RunArgs, CliError> {
    Ok(RunArgs {
        resource_id: single_positional_arg(
            "run",
            &values(matches, "resource_arg"),
            "accepts at most one resource id",
        )?,
        destination_uri: string_value(matches, "to"),
        jobs: string_value(matches, "jobs")
            .map(|value| parse_nonzero_u16("--jobs", &value))
            .transpose()?,
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
        "destinations" => {
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

fn parse_schema(matches: &ArgMatches) -> Result<SchemaCommand, CliError> {
    let Some((subcommand, matches)) = matches.subcommand() else {
        return Err(CliError::usage(
            "schema requires one of discover, pin, show, diff, or promote",
        ));
    };
    match subcommand {
        "discover" => {
            let resource_id = parse_schema_resource("schema discover", matches)?;
            Ok(SchemaCommand::Discover(SchemaDiscoverArgs { resource_id }))
        }
        "pin" => Ok(SchemaCommand::Pin(SchemaResourceArgs {
            resource_id: parse_schema_resource("schema pin", matches)?,
        })),
        "show" => Ok(SchemaCommand::Show(SchemaResourceArgs {
            resource_id: parse_schema_resource("schema show", matches)?,
        })),
        "diff" => Ok(SchemaCommand::Diff(SchemaResourceArgs {
            resource_id: parse_schema_resource("schema diff", matches)?,
        })),
        "promote" => Ok(SchemaCommand::Promote(SchemaPromoteArgs {
            resource_id: parse_schema_resource("schema promote", matches)?,
            types: values(matches, "type"),
            execute: matches.get_flag("execute"),
        })),
        other => Err(unknown_subcommand_error(
            &["schema"],
            other,
            SCHEMA_SUBCOMMANDS,
            "unknown schema subcommand",
        )),
    }
}

fn parse_schema_resource(command: &str, matches: &ArgMatches) -> Result<String, CliError> {
    single_positional_arg(
        command,
        &values(matches, "resource_arg"),
        "accepts at most one resource id",
    )?
    .ok_or_else(|| CliError::usage(format!("{command} requires a resource id")))
}

fn parse_contract(matches: &ArgMatches) -> Result<ContractCommand, CliError> {
    let Some((subcommand, matches)) = matches.subcommand() else {
        return Err(CliError::usage(
            "contract requires one of freeze, show, or test",
        ));
    };
    match subcommand {
        "freeze" => Ok(ContractCommand::Freeze {
            contract: optional_single_value("contract freeze", &values(matches, "value"))?,
        }),
        "show" => Ok(ContractCommand::Show {
            trust: optional_single_value("contract show", &values(matches, "value"))?,
        }),
        "test" => Ok(ContractCommand::Test {
            contract: optional_single_value("contract test", &values(matches, "value"))?,
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
            "state requires one of show, history, rewind, or recover",
        ));
    };
    match subcommand {
        "show" => parse_state_scope(matches).map(StateCommand::Show),
        "history" => parse_state_scope(matches).map(StateCommand::History),
        "rewind" => parse_rewind(matches).map(StateCommand::Rewind),
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
    let resource_id = single_positional_arg(
        "state command",
        &values(matches, "resource_arg"),
        "accepts at most one resource id",
    )?
    .ok_or_else(|| CliError::usage("state command requires RESOURCE"))?;
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
            .ok_or_else(|| CliError::usage("state rewind requires --to"))?,
        marker_checkpoint_id: mint_cli_id("rewind-marker"),
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
        run_id: single_positional_arg(
            "resume",
            &values(matches, "run_arg"),
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
    let resource_id = single_positional_arg(
        "backfill",
        &values(matches, "resource_arg"),
        "accepts at most one resource id",
    )?
    .ok_or_else(|| CliError::usage("backfill requires RESOURCE"))?;
    let slice_size = string_value(matches, "slice_size")
        .map(|value| parse_u64("--slice-size", &value))
        .transpose()?;
    Ok(BackfillArgs {
        resource_id,
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
        .about("Plan, run, and inspect governed data movement")
        .long_about("Plan, run, and inspect governed data movement with durable packages, checkpoints, receipts, and schema evidence.")
        .after_long_help("Environment:\n  CDF_PROJECT       Project directory or cdf.toml path\n  CDF_ENV           Project environment name\n  CDF_TARGET        Default destination\n  NO_COLOR          Disable color unless --color always is explicit\n  CLICOLOR_FORCE    Request color when output is interactive\n  COLUMNS           Width fallback when terminal size is unavailable\n\nExamples:\n  cdf validate\n  cdf plan local.events --to duckdb://.cdf/dev.duckdb\n  cdf run local.events -v\n  cdf inspect run RUN_ID")
        .arg(flag("quiet", "quiet").short('q').global(true).conflicts_with("verbose").help("Suppress progress and non-primary success narration"))
        .arg(flag("verbose", "verbose").short('v').global(true).action(ArgAction::Count).conflicts_with("quiet").help("Show evidence detail; repeat for diagnostics"))
        .arg(policy_option("color", "color", "WHEN", "Color policy: auto, always, or never"))
        .arg(policy_option("progress", "progress", "WHEN", "Progress policy: auto, always, or never"))
        .arg(policy_option("unicode", "unicode", "WHEN", "Unicode policy: auto, always, or never"))
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
        .subcommand(
            cmd("add")
                .arg(values_arg("values").value_names(["RESOURCE_ID", "URL_OR_PATH"]))
                .arg(flag("dry_run", "dry-run"))
                .arg(option("records", "records", "SELECTOR"))
                .arg(option("cursor", "cursor", "FIELD"))
                .arg(option("cursor_param", "cursor-param", "PARAM")),
        )
        .subcommand(
            cmd("validate")
                .arg(flag("deep", "deep"))
                .arg(values_arg("extra").hide(true)),
        )
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
        .subcommand(schema_command())
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
        .arg(option("projection", "select", "FIELDS"))
        .arg(append_option("filter", "filter", "EXPR"))
        .arg(option("limit", "limit", "N"))
        .arg(append_option("order_by", "order-by", "FIELD[:asc|desc]"));
    if accepts_target {
        command = command
            .arg(option("to", "to", "DEST"))
            .arg(flag("no_pin", "no-pin"));
    }
    command
}

fn run_command() -> ClapCommand {
    cmd("run")
        .arg(values_arg("resource_arg").value_name("RESOURCE"))
        .arg(option("to", "to", "DEST"))
        .arg(option("jobs", "jobs", "N"))
        .arg(flag("loop", "loop"))
}

fn schema_command() -> ClapCommand {
    cmd("schema")
        .subcommand(schema_resource_command("discover"))
        .subcommand(schema_resource_command("pin"))
        .subcommand(schema_resource_command("show"))
        .subcommand(schema_resource_command("diff"))
        .subcommand(
            schema_resource_command("promote")
                .arg(
                    Arg::new("type")
                        .long("type")
                        .value_name("JSON_POINTER=ARROW_TYPE")
                        .action(ArgAction::Append),
                )
                .arg(flag("execute", "execute")),
        )
}

fn schema_resource_command(name: &'static str) -> ClapCommand {
    cmd(name).arg(values_arg("resource_arg").value_name("RESOURCE"))
}

fn inspect_command() -> ClapCommand {
    cmd("inspect")
        .subcommand(cmd("project").arg(values_arg("values").hide(true)))
        .subcommand(cmd("resources").arg(values_arg("values").hide(true)))
        .subcommand(cmd("resource").arg(values_arg("values").value_name("ID")))
        .subcommand(cmd("lock").arg(values_arg("values").hide(true)))
        .subcommand(cmd("destinations").arg(values_arg("values").hide(true)))
        .subcommand(cmd("package").arg(values_arg("values").value_name("DIR")))
        .subcommand(cmd("run").arg(values_arg("values").value_name("RUN_ID")))
}

fn contract_command() -> ClapCommand {
    cmd("contract")
        .subcommand(cmd("freeze").arg(values_arg("value").value_name("CONTRACT")))
        .subcommand(cmd("show").arg(values_arg("value").value_name("TRUST")))
        .subcommand(cmd("test").arg(values_arg("value").value_name("CONTRACT")))
}

fn state_command() -> ClapCommand {
    cmd("state")
        .subcommand(state_scope_command("show"))
        .subcommand(state_scope_command("history"))
        .subcommand(state_scope_command("rewind").arg(option(
            "target_checkpoint",
            "to",
            "CHECKPOINT",
        )))
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
        .arg(append_option("scope", "scope", "KEY=VALUE"))
        .arg(option("scope_json", "scope-json", "JSON"))
}

fn resume_command() -> ClapCommand {
    cmd("resume").arg(values_arg("run_arg").value_name("RUN_ID"))
}

fn backfill_command() -> ClapCommand {
    cmd("backfill")
        .arg(values_arg("resource_arg").value_name("RESOURCE"))
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
    let about = match name {
        "help" => "Show help for a command",
        "version" => "Print the cdf version",
        "init" => "Create a new cdf project",
        "add" => "Add a source resource to the project",
        "validate" => "Validate project configuration and contracts",
        "plan" => "Plan a resource run without executing it",
        "explain" => "Explain resolution, capabilities, and execution choices",
        "run" => "Execute a governed resource run",
        "preview" => "Read a bounded preview without committing data",
        "sql" => "Query cdf system metadata",
        "inspect" => "Inspect durable project and run evidence",
        "diff" => "Compare durable schemas",
        "schema" => "Discover, pin, compare, and promote schemas",
        "contract" => "Freeze, show, and test contracts",
        "state" => "Inspect and recover checkpoint state",
        "resume" => "Resume interrupted work from the run ledger",
        "replay" => "Replay a verified package",
        "backfill" => "Plan or execute a bounded cursor backfill",
        "package" => "List, verify, archive, and collect packages",
        "doctor" => "Check local runtime and destination health",
        "status" => "Summarize project freshness and run state",
        "discover" => "Discover the current physical source schema",
        "pin" => "Pin a discovered schema into the project contract",
        "show" => "Show the selected durable record",
        "promote" => "Plan or execute residual schema promotion",
        "freeze" => "Freeze a contract snapshot",
        "test" => "Test data against a contract",
        "history" => "Show checkpoint history",
        "rewind" => "Create a marker that rewinds checkpoint state",
        "recover" => "Recover state from a committed package receipt",
        "ls" => "List durable packages",
        "gc" => "Collect packages allowed by retention policy",
        "verify" => "Verify package integrity and evidence",
        "archive" => "Archive a package in a portable format",
        "project" => "Show resolved project information",
        "resources" => "List project resources",
        "resource" => "Show one resolved resource",
        "lock" => "Show the project lock",
        "destinations" => "List resolved destinations",
        _ => "Operate on cdf project evidence",
    };
    ClapCommand::new(name).about(about).args_override_self(true)
}

fn option(id: &'static str, long: &'static str, value_name: &'static str) -> Arg {
    Arg::new(id)
        .long(long)
        .value_name(value_name)
        .num_args(1)
        .action(ArgAction::Set)
        .help(option_help(long))
}

fn policy_option(
    id: &'static str,
    long: &'static str,
    value_name: &'static str,
    help: &'static str,
) -> Arg {
    option(id, long, value_name)
        .global(true)
        .value_parser(["auto", "always", "never"])
        .help(help)
}

fn append_option(id: &'static str, long: &'static str, value_name: &'static str) -> Arg {
    Arg::new(id)
        .long(long)
        .value_name(value_name)
        .num_args(1)
        .action(ArgAction::Append)
        .help(option_help(long))
}

fn flag(id: &'static str, long: &'static str) -> Arg {
    Arg::new(id)
        .long(long)
        .action(ArgAction::SetTrue)
        .help(option_help(long))
}

fn values_arg(id: &'static str) -> Arg {
    Arg::new(id)
        .num_args(0..)
        .action(ArgAction::Append)
        .help(positional_help(id))
}

fn option_help(long: &str) -> &'static str {
    match long {
        "project" => "Project directory or cdf.toml path",
        "env" => "Project environment name",
        "to" => "Destination URI or cursor upper bound, as shown in usage",
        "target" => "Destination target or table",
        "select" => "Comma-separated projected fields",
        "filter" => "Filter expression; may be repeated",
        "limit" => "Maximum rows to read",
        "order-by" => "Ordering field and optional direction",
        "pipeline" => "Pipeline identifier",
        "jobs" => "Maximum concurrent jobs",
        "loop" => "Continue polling for work",
        "deep" => "Run probes that may contact configured systems",
        "dry-run" => "Show the proposed change without writing it",
        "execute" => "Apply the planned operation",
        "force" => "Replace an existing artifact when safe",
        "scope" => "Checkpoint scope entry as key=value; may be repeated",
        "scope-json" => "Checkpoint scope encoded as JSON",
        "from" => "Inclusive cursor lower bound",
        "slice-size" => "Rows per backfill slice",
        "format" => "Archive output format",
        "no-pin" => "Do not pin newly discovered schema",
        "type" => "Residual field pointer and Arrow type",
        "contract" => "Contract name",
        "trust" => "Trust level to show",
        "merge-dedup" => "Merge deduplication policy",
        "name" => "Project name",
        "package" => "Package directory",
        "receipt" => "Receipt identifier",
        "records" => "Record selector within the source",
        "cursor" => "Cursor field",
        "cursor-param" => "Request parameter carrying the cursor",
        "color" => "Color policy: auto, always, or never",
        "progress" => "Progress policy: auto, always, or never",
        "unicode" => "Unicode policy: auto, always, or never",
        "quiet" => "Suppress progress and non-primary success narration",
        "verbose" => "Show evidence detail; repeat for diagnostics",
        _ => "Set the value named in this command's usage",
    }
}

fn positional_help(id: &str) -> &'static str {
    match id {
        "command" => "Command path to explain",
        "directory" => "Directory to initialize",
        "query" => "SQL query text",
        "resource_arg" => "Resource identifier",
        "run_arg" => "Run identifier; omit to scan interrupted work",
        "package_dir" | "packages_dir" => "Package directory",
        "value" => "Contract or trust selector shown in usage",
        "values" => "Identifiers or paths shown in usage",
        "extra" => "Unexpected operands",
        _ => "Operand named in this command's usage",
    }
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
        .write_long_help(&mut buffer)
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

fn single_positional_arg(
    command: &str,
    positional_values: &[String],
    too_many_message: &str,
) -> Result<Option<String>, CliError> {
    if positional_values.len() > 1 {
        return Err(CliError::usage(format!("{command} {too_many_message}")));
    }
    Ok(positional_values.first().cloned())
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

fn optional_single_value(command: &str, values: &[String]) -> Result<Option<String>, CliError> {
    match values {
        [] => Ok(None),
        [value] => Ok(Some(value.clone())),
        _ => Err(CliError::usage(format!(
            "{command} accepts at most one value"
        ))),
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

fn parse_nonzero_u16(option: &str, value: &str) -> Result<u16, CliError> {
    let parsed = value.parse::<u16>().map_err(|error| {
        CliError::usage(format!(
            "{option} must be an integer from 1 to 65535: {error}"
        ))
    })?;
    if parsed == 0 {
        return Err(CliError::usage(format!(
            "{option} must be an integer from 1 to 65535"
        )));
    }
    Ok(parsed)
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

#[cfg(test)]
mod run_jobs_tests {
    use std::ffi::OsString;

    use super::{Cli, Command};

    #[test]
    fn run_jobs_is_a_nonzero_user_ceiling() {
        let cli =
            Cli::parse(["cdf", "run", "local.events", "--jobs", "7"].map(OsString::from)).unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };
        assert_eq!(args.jobs, Some(7));

        let error = Cli::parse(["cdf", "run", "local.events", "--jobs", "0"].map(OsString::from))
            .unwrap_err();
        assert!(error.message.contains("--jobs must be an integer from 1"));
    }
}
