use std::fs;

use cdf_contract::ContractPolicy;
use cdf_kernel::CdfError;
use cdf_project::{
    ContractFreezeReport, ContractTestReport, LOCK_FILE_NAME, freeze_contract_snapshots,
    lock_to_toml, test_contract_snapshots,
};
use serde::Serialize;

use crate::{
    args::{Cli, ContractCommand},
    context::ProjectContext,
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
    },
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
            let report = ContractShowCliReport {
                policy: trust,
                contract: policy,
            };
            CommandOutput::rendered("contract show", report.render_document(), report)
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
    CommandOutput::rendered("contract freeze", contract_freeze_document(&report), report)
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
    CommandOutput::rendered_with_exit_code(
        "contract test",
        contract_test_document(&report),
        report,
        exit_code,
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ContractShowCliReport {
    policy: String,
    contract: ContractPolicy,
}

impl ContractShowCliReport {
    fn render_document(&self) -> RenderDocument {
        RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                StatusKind::Success,
                format!("contract policy {}", self.policy),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Policy")
                    .row("name", self.policy.clone())
                    .row(
                        "schema review",
                        yes_no(self.contract.schema.review_artifact_required),
                    )
                    .row("receipts required", yes_no(self.contract.receipts_required))
                    .row(
                        "reconciliation",
                        yes_no(self.contract.reconciliation_counts),
                    )
                    .row("retention", format!("{:?}", self.contract.retention)),
            )
            .blank_line()
            .push(NextCommand::new("cdf contract freeze"))
    }
}

fn contract_freeze_document(report: &ContractFreezeReport) -> RenderDocument {
    let table = report.snapshots.iter().fold(
        Table::new(["resource", "schema", "policy", "program"]),
        |table, (resource, snapshot)| {
            table.row([
                resource.clone(),
                optional_string(snapshot.schema_hash.clone()),
                optional_string(snapshot.policy_hash.clone()),
                optional_string(snapshot.validation_program_hash.clone()),
            ])
        },
    );

    let mut document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            StatusKind::Success,
            format!("froze {} contract snapshot(s)", report.counts.frozen),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Contract registry")
                .row("file", LOCK_FILE_NAME)
                .row("registry", report.registry.clone())
                .row("resources", report.resource_ids.len().to_string())
                .row("frozen", report.counts.frozen.to_string())
                .row("missing", report.counts.missing.to_string())
                .row("drifted", report.counts.drifted.to_string()),
        );

    if !report.snapshots.is_empty() {
        document = document.blank_line().push(table);
    }

    document
        .blank_line()
        .push(NextCommand::new("cdf contract test"))
}

fn contract_test_document(report: &ContractTestReport) -> RenderDocument {
    let drifted = report.counts.drifted > 0;
    let table = report.snapshots.iter().fold(
        Table::new(["resource", "verdict", "drift fields"]),
        |table, comparison| {
            table.row([
                comparison.resource_id.clone(),
                format!("{:?}", comparison.verdict).to_lowercase(),
                if comparison.drift_details.is_empty() {
                    "none".to_owned()
                } else {
                    comparison
                        .drift_details
                        .iter()
                        .map(|detail| detail.field.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                },
            ])
        },
    );

    let mut document = RenderDocument::new()
        .push(SectionRule::new())
        .push(StatusLine::new(
            if drifted {
                StatusKind::Warning
            } else {
                StatusKind::Success
            },
            format!(
                "contract test: {} passed, {} drifted",
                report.counts.passed, report.counts.drifted
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Contract registry")
                .row("registry", report.registry.clone())
                .row("resources", report.resource_ids.len().to_string())
                .row("passed", report.counts.passed.to_string())
                .row("drifted", report.counts.drifted.to_string())
                .row("missing", report.counts.missing.to_string()),
        );

    if !report.snapshots.is_empty() {
        document = document.blank_line().push(table);
    }

    document.blank_line().push(NextCommand::new(if drifted {
        "cdf contract freeze"
    } else {
        "cdf plan"
    }))
}

fn optional_string(value: Option<String>) -> String {
    value.unwrap_or_else(|| "none".to_owned())
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
