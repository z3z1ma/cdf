use cdf_contract::ContractPolicy;
use cdf_kernel::CdfError;
use cdf_project::{
    ContractFreezeReport, ContractTestReport, LOCK_FILE_NAME, freeze_contract_snapshots,
    lock_to_toml, write_lock_file_guarded,
};
use serde::Serialize;

use crate::{
    args::{Cli, ContractCommand},
    context::ProjectContext,
    error_catalog,
    output::{CliError, CommandOutput},
};

pub(crate) fn contract(
    cli: &Cli,
    command: ContractCommand,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    match command {
        ContractCommand::Show { trust } => {
            let trust = trust.unwrap_or_else(|| "governed".to_owned());
            let policy = match trust.as_str() {
                "experimental" => ContractPolicy::for_trust(cdf_kernel::TrustLevel::Experimental),
                "governed" => ContractPolicy::for_trust(cdf_kernel::TrustLevel::Governed),
                "financial" => ContractPolicy::for_trust(cdf_kernel::TrustLevel::Financial),
                "serving" => ContractPolicy::for_trust(cdf_kernel::TrustLevel::Serving),
                "evolve" => ContractPolicy::evolve(),
                "freeze" => ContractPolicy::freeze(),
                other => {
                    return Err(CliError::usage_with(
                        format!("unknown contract policy `{other}`"),
                        error_catalog::CONTRACT_ARGUMENT,
                    ));
                }
            };
            let report = ContractShowCliReport {
                policy: trust,
                contract: policy,
            };
            CommandOutput::rendered("contract show", render::show_document(&report), report)
        }
        ContractCommand::Freeze { contract } => freeze(cli, contract, destinations),
        ContractCommand::Test { contract } => test(cli, contract, destinations),
    }
}

fn freeze(
    cli: &Cli,
    selector: Option<String>,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load_for_command_with_destination_registry(
        "contract freeze",
        cli.project.as_ref(),
        cli.env.as_deref(),
        true,
        destinations,
    )?;
    let destination_artifacts = crate::destination_registry::inspect_destination_artifacts(
        destinations,
        &context,
        &context.environment.destination,
    )?;
    let (lock, report) = freeze_contract_snapshots(
        &context.config,
        &context.resources,
        context.lock.as_ref(),
        &destination_artifacts,
        selector.as_deref(),
        &context.semantic_catalog,
    )?;
    let encoded = lock_to_toml(&lock)?;
    let lock_path = context.root.join(LOCK_FILE_NAME);
    write_lock_file_guarded(&lock_path, context.lock_authority.as_ref(), encoded)
        .map_err(|error| CliError::mapped(error, error_catalog::CONTRACT_LOCKFILE))?;
    CommandOutput::rendered("contract freeze", render::freeze_document(&report), report)
}

fn test(
    cli: &Cli,
    selector: Option<String>,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load_for_command_with_destination_registry(
        "contract test",
        cli.project.as_ref(),
        cli.env.as_deref(),
        true,
        destinations,
    )?;
    let lock = context.lock.as_ref().ok_or_else(|| {
        CliError::mapped(
            CdfError::contract(format!(
                "{} is missing under {}; run `cdf contract freeze` before `cdf contract test`",
                LOCK_FILE_NAME,
                context.root.display()
            )),
            error_catalog::CONTRACT_LOCKFILE,
        )
    })?;
    let report = cdf_project::test_contract_snapshots_with_semantic_catalog(
        lock,
        &context.resources,
        selector.as_deref(),
        &context.semantic_catalog,
    )?;
    let exit_code = if report.counts.drifted == 0 { 0 } else { 1 };
    CommandOutput::rendered_with_exit_code(
        "contract test",
        render::test_document(&report),
        report,
        exit_code,
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ContractShowCliReport {
    policy: String,
    contract: ContractPolicy,
}

mod render;
