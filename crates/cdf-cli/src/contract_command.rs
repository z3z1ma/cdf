use cdf_contract::ContractPolicy;
use serde_json::json;

use crate::{
    args::ContractCommand,
    commands::output,
    output::{CliError, CommandOutput},
};

pub(crate) fn contract(command: ContractCommand) -> Result<CommandOutput, CliError> {
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
