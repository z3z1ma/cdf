use std::{ffi::OsString, path::PathBuf};

use crate::output::CliError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cli {
    pub json: bool,
    pub project: Option<PathBuf>,
    pub env: Option<String>,
    pub command: Command,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Help,
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
    Recover,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateScopeArgs {
    pub pipeline_id: String,
    pub resource_id: String,
    pub scope_json: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RewindArgs {
    pub scope: StateScopeArgs,
    pub target_checkpoint_id: String,
    pub marker_checkpoint_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ResumeArgs {
    pub run_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayPackageArgs {
    pub package_dir: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackfillArgs {
    pub resource_id: Option<String>,
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
            project,
            env,
            command,
        })
    }
}

fn parse_command(args: &[String]) -> Result<Command, CliError> {
    if args.is_empty()
        || args[0] == "help"
        || args[0] == "--help"
        || args[0] == "-h"
        || args.iter().any(|arg| arg == "--help" || arg == "-h")
    {
        return Ok(Command::Help);
    }
    if args[0] == "--version" || args[0] == "-V" || args[0] == "version" {
        return Ok(Command::Version);
    }

    match args[0].as_str() {
        "init" => parse_init(&args[1..]).map(Command::Init),
        "validate" => no_extra_args("validate", &args[1..]).map(|()| Command::Validate),
        "plan" => parse_scan("plan", &args[1..]).map(Command::Plan),
        "explain" => parse_scan("explain", &args[1..]).map(Command::Explain),
        "run" => parse_run(&args[1..]).map(Command::Run),
        "preview" => parse_scan("preview", &args[1..]).map(Command::Preview),
        "sql" => parse_sql(&args[1..]).map(Command::Sql),
        "inspect" => parse_inspect(&args[1..]).map(Command::Inspect),
        "diff" => parse_diff(&args[1..]),
        "contract" => parse_contract(&args[1..]).map(Command::Contract),
        "state" => parse_state(&args[1..]).map(Command::State),
        "resume" => parse_resume(&args[1..]).map(Command::Resume),
        "replay" => parse_replay(&args[1..]),
        "backfill" => parse_backfill(&args[1..]).map(Command::Backfill),
        "package" => parse_package(&args[1..]).map(Command::Package),
        "doctor" => no_extra_args("doctor", &args[1..]).map(|()| Command::Doctor),
        "status" => no_extra_args("status", &args[1..]).map(|()| Command::Status),
        other => Err(CliError::usage(format!("unknown command `{other}`"))),
    }
}

fn parse_init(args: &[String]) -> Result<InitArgs, CliError> {
    let mut directory = None;
    let mut name = None;
    let mut force = false;
    let mut cursor = Cursor::new(args);
    while let Some(arg) = cursor.next() {
        match arg {
            "--name" => name = Some(cursor.value("--name")?),
            "--force" => force = true,
            value if value.starts_with('-') => {
                return Err(CliError::usage(format!("unknown init option `{value}`")));
            }
            value => {
                if directory.replace(PathBuf::from(value)).is_some() {
                    return Err(CliError::usage("init accepts at most one directory"));
                }
            }
        }
    }
    Ok(InitArgs {
        directory,
        name,
        force,
    })
}

fn parse_scan(command: &str, args: &[String]) -> Result<ScanArgs, CliError> {
    let mut resource_id = None;
    let mut projection = None;
    let mut filters = Vec::new();
    let mut limit = None;
    let mut order_by = Vec::new();
    let mut package_id = None;
    let mut cursor = Cursor::new(args);

    while let Some(arg) = cursor.next() {
        match arg {
            "--resource" => resource_id = Some(cursor.value("--resource")?),
            "--select" | "--projection" => {
                projection = Some(
                    cursor
                        .value(arg)?
                        .split(',')
                        .map(str::trim)
                        .filter(|field| !field.is_empty())
                        .map(ToOwned::to_owned)
                        .collect(),
                );
            }
            "--filter" => filters.push(cursor.value("--filter")?),
            "--limit" => limit = Some(parse_u64("--limit", &cursor.value("--limit")?)?),
            "--order-by" => order_by.push(cursor.value("--order-by")?),
            "--package-id" => package_id = Some(cursor.value("--package-id")?),
            value if value.starts_with('-') => {
                return Err(CliError::usage(format!(
                    "unknown {command} option `{value}`"
                )));
            }
            value => {
                if resource_id.replace(value.to_owned()).is_some() {
                    return Err(CliError::usage(format!(
                        "{command} accepts one resource id"
                    )));
                }
            }
        }
    }

    let resource_id =
        resource_id.ok_or_else(|| CliError::usage(format!("{command} requires a resource id")))?;
    Ok(ScanArgs {
        resource_id,
        projection,
        filters,
        limit,
        order_by,
        package_id,
    })
}

fn parse_run(args: &[String]) -> Result<RunArgs, CliError> {
    let mut run = RunArgs::default();
    let mut cursor = Cursor::new(args);
    while let Some(arg) = cursor.next() {
        match arg {
            "--resource" => run.resource_id = Some(cursor.value("--resource")?),
            "--pipeline" => run.pipeline_id = Some(cursor.value("--pipeline")?),
            "--target" => run.target = Some(cursor.value("--target")?),
            "--package-id" => run.package_id = Some(cursor.value("--package-id")?),
            "--checkpoint-id" => run.checkpoint_id = Some(cursor.value("--checkpoint-id")?),
            "--loop" => run.loop_mode = true,
            value if value.starts_with('-') => {
                return Err(CliError::usage(format!("unknown run option `{value}`")));
            }
            value => {
                if run.resource_id.replace(value.to_owned()).is_some() {
                    return Err(CliError::usage("run accepts at most one resource id"));
                }
            }
        }
    }
    Ok(run)
}

fn parse_sql(args: &[String]) -> Result<SqlArgs, CliError> {
    if args.is_empty() {
        return Err(CliError::usage("sql requires a query string"));
    }
    Ok(SqlArgs {
        query: args.join(" "),
    })
}

fn parse_inspect(args: &[String]) -> Result<InspectArgs, CliError> {
    let Some(noun) = args.first().map(String::as_str) else {
        return Err(CliError::usage("inspect requires a noun"));
    };
    let noun = match noun {
        "project" => {
            no_extra_args("inspect project", &args[1..])?;
            InspectNoun::Project
        }
        "resources" => {
            no_extra_args("inspect resources", &args[1..])?;
            InspectNoun::Resources
        }
        "resource" => {
            let id = args
                .get(1)
                .ok_or_else(|| CliError::usage("inspect resource requires a resource id"))?;
            no_extra_args("inspect resource", &args[2..])?;
            InspectNoun::Resource(id.clone())
        }
        "lock" => {
            no_extra_args("inspect lock", &args[1..])?;
            InspectNoun::Lock
        }
        "destinations" | "destination" => {
            no_extra_args("inspect destinations", &args[1..])?;
            InspectNoun::Destinations
        }
        "package" => {
            let path = args
                .get(1)
                .ok_or_else(|| CliError::usage("inspect package requires a package directory"))?;
            no_extra_args("inspect package", &args[2..])?;
            InspectNoun::Package(PathBuf::from(path))
        }
        other => return Err(CliError::usage(format!("unknown inspect noun `{other}`"))),
    };
    Ok(InspectArgs { noun })
}

fn parse_diff(args: &[String]) -> Result<Command, CliError> {
    match args.first().map(String::as_str) {
        Some("schema") => {
            no_extra_args("diff schema", &args[1..])?;
            Ok(Command::DiffSchema)
        }
        _ => Err(CliError::usage("diff requires subcommand `schema`")),
    }
}

fn parse_contract(args: &[String]) -> Result<ContractCommand, CliError> {
    let Some(subcommand) = args.first().map(String::as_str) else {
        return Err(CliError::usage(
            "contract requires one of freeze, show, or test",
        ));
    };
    match subcommand {
        "freeze" => Ok(ContractCommand::Freeze {
            contract: optional_named_or_positional("--contract", &args[1..])?,
        }),
        "show" => Ok(ContractCommand::Show {
            trust: optional_named_or_positional("--trust", &args[1..])?,
        }),
        "test" => Ok(ContractCommand::Test {
            contract: optional_named_or_positional("--contract", &args[1..])?,
        }),
        other => Err(CliError::usage(format!(
            "unknown contract subcommand `{other}`"
        ))),
    }
}

fn parse_state(args: &[String]) -> Result<StateCommand, CliError> {
    let Some(subcommand) = args.first().map(String::as_str) else {
        return Err(CliError::usage(
            "state requires one of show, history, rewind, migrate, or recover",
        ));
    };
    match subcommand {
        "show" => parse_state_scope(&args[1..]).map(StateCommand::Show),
        "history" => parse_state_scope(&args[1..]).map(StateCommand::History),
        "rewind" => parse_rewind(&args[1..]).map(StateCommand::Rewind),
        "migrate" => no_extra_args("state migrate", &args[1..]).map(|()| StateCommand::Migrate),
        "recover" => no_extra_args("state recover", &args[1..]).map(|()| StateCommand::Recover),
        other => Err(CliError::usage(format!(
            "unknown state subcommand `{other}`"
        ))),
    }
}

fn parse_state_scope(args: &[String]) -> Result<StateScopeArgs, CliError> {
    let mut pipeline_id = None;
    let mut resource_id = None;
    let mut scope_json = None;
    let mut cursor = Cursor::new(args);
    while let Some(arg) = cursor.next() {
        match arg {
            "--pipeline" => pipeline_id = Some(cursor.value("--pipeline")?),
            "--resource" => resource_id = Some(cursor.value("--resource")?),
            "--scope-json" => scope_json = Some(cursor.value("--scope-json")?),
            value => return Err(CliError::usage(format!("unknown state option `{value}`"))),
        }
    }
    Ok(StateScopeArgs {
        pipeline_id: pipeline_id
            .ok_or_else(|| CliError::usage("state command requires --pipeline"))?,
        resource_id: resource_id
            .ok_or_else(|| CliError::usage("state command requires --resource"))?,
        scope_json,
    })
}

fn parse_rewind(args: &[String]) -> Result<RewindArgs, CliError> {
    let mut target_checkpoint_id = None;
    let mut marker_checkpoint_id = None;
    let mut rest = Vec::new();
    let mut cursor = Cursor::new(args);
    while let Some(arg) = cursor.next() {
        match arg {
            "--target-checkpoint" => {
                target_checkpoint_id = Some(cursor.value("--target-checkpoint")?)
            }
            "--marker-checkpoint" => {
                marker_checkpoint_id = Some(cursor.value("--marker-checkpoint")?)
            }
            other => rest.push(other.to_owned()),
        }
    }
    Ok(RewindArgs {
        scope: parse_state_scope(&rest)?,
        target_checkpoint_id: target_checkpoint_id
            .ok_or_else(|| CliError::usage("state rewind requires --target-checkpoint"))?,
        marker_checkpoint_id: marker_checkpoint_id
            .ok_or_else(|| CliError::usage("state rewind requires --marker-checkpoint"))?,
    })
}

fn parse_resume(args: &[String]) -> Result<ResumeArgs, CliError> {
    let mut resume = ResumeArgs::default();
    let mut cursor = Cursor::new(args);
    while let Some(arg) = cursor.next() {
        match arg {
            "--run" | "--run-id" => resume.run_id = Some(cursor.value(arg)?),
            value if value.starts_with('-') => {
                return Err(CliError::usage(format!("unknown resume option `{value}`")));
            }
            value => {
                if resume.run_id.replace(value.to_owned()).is_some() {
                    return Err(CliError::usage("resume accepts at most one run id"));
                }
            }
        }
    }
    Ok(resume)
}

fn parse_replay(args: &[String]) -> Result<Command, CliError> {
    match args.first().map(String::as_str) {
        Some("package") => {
            let path = args
                .get(1)
                .ok_or_else(|| CliError::usage("replay package requires a package directory"))?;
            no_extra_args("replay package", &args[2..])?;
            Ok(Command::ReplayPackage(ReplayPackageArgs {
                package_dir: PathBuf::from(path),
            }))
        }
        _ => Err(CliError::usage("replay requires subcommand `package`")),
    }
}

fn parse_backfill(args: &[String]) -> Result<BackfillArgs, CliError> {
    let mut resource_id = None;
    let mut cursor = Cursor::new(args);
    while let Some(arg) = cursor.next() {
        match arg {
            "--resource" => resource_id = Some(cursor.value("--resource")?),
            value if value.starts_with('-') => {
                return Err(CliError::usage(format!(
                    "unknown backfill option `{value}`"
                )));
            }
            value => {
                if resource_id.replace(value.to_owned()).is_some() {
                    return Err(CliError::usage("backfill accepts at most one resource id"));
                }
            }
        }
    }
    Ok(BackfillArgs { resource_id })
}

fn parse_package(args: &[String]) -> Result<PackageCommand, CliError> {
    let Some(subcommand) = args.first().map(String::as_str) else {
        return Err(CliError::usage(
            "package requires one of ls, gc, verify, or archive",
        ));
    };
    match subcommand {
        "ls" => Ok(PackageCommand::Ls {
            packages_dir: optional_path_arg("package ls", &args[1..])?,
        }),
        "gc" => Ok(PackageCommand::Gc {
            packages_dir: optional_path_arg("package gc", &args[1..])?,
        }),
        "verify" => {
            let path = args
                .get(1)
                .ok_or_else(|| CliError::usage("package verify requires a package directory"))?;
            no_extra_args("package verify", &args[2..])?;
            Ok(PackageCommand::Verify {
                package_dir: PathBuf::from(path),
            })
        }
        "archive" => parse_package_archive(&args[1..]).map(PackageCommand::Archive),
        other => Err(CliError::usage(format!(
            "unknown package subcommand `{other}`"
        ))),
    }
}

fn parse_package_archive(args: &[String]) -> Result<PackageArchiveArgs, CliError> {
    let mut package_dir = None;
    let mut format = "parquet".to_owned();
    let mut force = false;
    let mut cursor = Cursor::new(args);
    while let Some(arg) = cursor.next() {
        match arg {
            "--format" => format = cursor.value("--format")?,
            "--force" => force = true,
            value if value.starts_with('-') => {
                return Err(CliError::usage(format!(
                    "unknown package archive option `{value}`"
                )));
            }
            value => {
                if package_dir.replace(PathBuf::from(value)).is_some() {
                    return Err(CliError::usage(
                        "package archive accepts exactly one package directory",
                    ));
                }
            }
        }
    }
    Ok(PackageArchiveArgs {
        package_dir: package_dir
            .ok_or_else(|| CliError::usage("package archive requires a package directory"))?,
        format,
        force,
    })
}

fn optional_path_arg(command: &str, args: &[String]) -> Result<Option<PathBuf>, CliError> {
    match args {
        [] => Ok(None),
        [one] => Ok(Some(PathBuf::from(one))),
        _ => Err(CliError::usage(format!(
            "{command} accepts at most one package root"
        ))),
    }
}

fn optional_named_or_positional(
    option_name: &str,
    args: &[String],
) -> Result<Option<String>, CliError> {
    let mut value = None;
    let mut cursor = Cursor::new(args);
    while let Some(arg) = cursor.next() {
        if arg == option_name {
            value = Some(cursor.value(option_name)?);
        } else if arg.starts_with('-') {
            return Err(CliError::usage(format!("unknown option `{arg}`")));
        } else if value.replace(arg.to_owned()).is_some() {
            return Err(CliError::usage("expected at most one value"));
        }
    }
    Ok(value)
}

fn no_extra_args(command: &str, args: &[String]) -> Result<(), CliError> {
    if args.is_empty() {
        Ok(())
    } else {
        Err(CliError::usage(format!(
            "{command} does not accept extra argument `{}`",
            args[0]
        )))
    }
}

fn parse_u64(option: &str, value: &str) -> Result<u64, CliError> {
    value
        .parse()
        .map_err(|error| CliError::usage(format!("{option} must be an unsigned integer: {error}")))
}

struct Cursor<'a> {
    args: &'a [String],
    index: usize,
}

impl<'a> Cursor<'a> {
    fn new(args: &'a [String]) -> Self {
        Self { args, index: 0 }
    }

    fn next(&mut self) -> Option<&'a str> {
        let value = self.args.get(self.index)?;
        self.index += 1;
        Some(value)
    }

    fn value(&mut self, option: &str) -> Result<String, CliError> {
        self.next()
            .map(ToOwned::to_owned)
            .ok_or_else(|| CliError::usage(format!("{option} requires a value")))
    }
}
