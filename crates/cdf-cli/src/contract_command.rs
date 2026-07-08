use std::fs;

use cdf_contract::ContractPolicy;
use cdf_kernel::CdfError;
use cdf_project::{
    LOCK_FILE_NAME, freeze_contract_snapshots, lock_to_toml, test_contract_snapshots,
};
use serde_json::json;

use crate::{
    args::{Cli, ContractCommand},
    commands::{output, report_output},
    context::ProjectContext,
    output::{CliError, CommandOutput},
};

pub(crate) fn contract(cli: &Cli, command: ContractCommand) -> Result<CommandOutput, CliError> {
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
        ContractCommand::Freeze { contract } => freeze(cli, contract),
        ContractCommand::Test { contract } => test(cli, contract),
    }
}

fn freeze(cli: &Cli, selector: Option<String>) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let (lock, report) = freeze_contract_snapshots(
        &context.config,
        &context.resources,
        context.lock.as_ref(),
        &context.environment.destination,
        selector.as_deref(),
    )?;
    let encoded = lock_to_toml(&lock)?;
    let lock_path = context.root.join(LOCK_FILE_NAME);
    fs::write(&lock_path, encoded).map_err(|error| {
        CliError::from(CdfError::contract(format!(
            "write {}: {error}",
            lock_path.display()
        )))
    })?;
    output(
        "contract freeze",
        format!(
            "froze {} contract snapshot(s) in {}",
            report.counts.frozen, LOCK_FILE_NAME
        ),
        report,
    )
}

fn test(cli: &Cli, selector: Option<String>) -> Result<CommandOutput, CliError> {
    let context = ProjectContext::load(cli.project.as_ref(), cli.env.as_deref())?;
    let lock = context.lock.as_ref().ok_or_else(|| {
        CliError::from(CdfError::contract(format!(
            "{} is missing under {}; run `cdf contract freeze` before `cdf contract test`",
            LOCK_FILE_NAME,
            context.root.display()
        )))
    })?;
    let report = test_contract_snapshots(lock, &context.resources, selector.as_deref())?;
    let exit_code = if report.counts.drifted == 0 { 0 } else { 1 };
    report_output(
        "contract test",
        format!(
            "contract test: {} passed, {} drifted",
            report.counts.passed, report.counts.drifted
        ),
        report,
        exit_code,
    )
}
