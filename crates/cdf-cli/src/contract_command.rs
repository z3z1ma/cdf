use cdf_contract::ContractPolicy;
use serde::Serialize;

use crate::{
    args::{Cli, ContractCommand},
    error_catalog,
    output::{CliError, CommandOutput},
};

pub(crate) fn contract(
    _cli: &Cli,
    command: ContractCommand,
    _destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    match command {
        ContractCommand::Show { trust } => {
            let trust = trust.unwrap_or_else(|| "governed".to_owned());
            let policy = match trust.as_str() {
                "experimental" => ContractPolicy::for_trust(cdf_kernel::TrustLevel::Experimental),
                "governed" => ContractPolicy::for_trust(cdf_kernel::TrustLevel::Governed),
                "financial" => ContractPolicy::for_trust(cdf_kernel::TrustLevel::Financial),
                "serving" => ContractPolicy::for_trust(cdf_kernel::TrustLevel::Serving),
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
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ContractShowCliReport {
    policy: String,
    contract: ContractPolicy,
}

mod render;
