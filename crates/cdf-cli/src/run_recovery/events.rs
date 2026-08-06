use cdf_kernel::{PackageHash, Receipt, StateDelta};
use cdf_project::ProjectReceiptSource;
use cdf_state_sqlite::{RunEvent, RunEventAppend, RunEventDetails, RunEventKind, RunEventValue};

use super::{model::ResumePackageFacts, report::ResumeReport};

pub(super) fn run_succeeded(events: &[RunEvent]) -> bool {
    events
        .iter()
        .any(|event| event.kind == RunEventKind::RunSucceeded)
}

pub(super) fn base_package_event(
    package: &ResumePackageFacts,
    kind: RunEventKind,
    delta: Option<&StateDelta>,
    receipt: Option<&Receipt>,
) -> RunEventAppend {
    let mut event = RunEventAppend::new(kind);
    fill_package_event_fields(&mut event, package, delta, receipt);
    event
}

pub(super) fn fill_package_event_fields(
    event: &mut RunEventAppend,
    package: &ResumePackageFacts,
    delta: Option<&StateDelta>,
    receipt: Option<&Receipt>,
) {
    event.package_id = Some(package.reader.manifest().identity.package_id.clone());
    event.package_hash = Some(
        PackageHash::new(package.reader.manifest().package_hash.clone())
            .expect("loaded package manifest hash must be valid"),
    );
    event.package_path = Some(package.path.display().to_string());
    if let Some(delta) = delta {
        event.resource_id = Some(delta.resource_id.clone());
        event.scope = Some(delta.scope.clone());
        event.checkpoint_id = Some(delta.checkpoint_id.clone());
    }
    if let Some(receipt) = receipt {
        event.receipt_id = Some(receipt.receipt_id.clone());
        event.destination_id = Some(receipt.destination.clone());
    }
}

pub(super) fn resume_event_details(report: &ResumeReport) -> RunEventDetails {
    RunEventDetails::new([
        ("state", RunEventValue::String(report.state.clone())),
        ("action", RunEventValue::String(report.action.clone())),
        ("source_contact", RunEventValue::Bool(report.source_contact)),
        (
            "mutation_required",
            RunEventValue::Bool(report.mutation_required),
        ),
        ("mutated", RunEventValue::Bool(report.mutated)),
        (
            "result",
            RunEventValue::String(report.recovery.result.clone()),
        ),
        (
            "guidance",
            RunEventValue::String(report.recovery.guidance.clone()),
        ),
    ])
}

pub(super) fn receipt_source_name(source: &ProjectReceiptSource, destination: &str) -> String {
    match source {
        ProjectReceiptSource::DestinationCommit { .. } if destination == "duckdb" => {
            "duck_db_commit".to_owned()
        }
        ProjectReceiptSource::DestinationCommit { .. } => "destination_commit".to_owned(),
        ProjectReceiptSource::DestinationCommitReceiptOnly { .. } => {
            "destination_commit_receipt_only".to_owned()
        }
        ProjectReceiptSource::SuppliedDurableReceipt => "supplied_durable_receipt".to_owned(),
    }
}
