use cdf_kernel::{RunEvent, RunEventSink, RunEventSinkResult, ScopeKey};

const RUNTIME_RUN_EVENT_TRACE_TARGET: &str = "cdf_project.runtime.run_event";

#[derive(Clone, Copy, Debug, Default)]
pub struct TracingRunEventSink;

impl TracingRunEventSink {
    pub const fn new() -> Self {
        Self
    }
}

impl RunEventSink for TracingRunEventSink {
    fn try_emit(&self, event: &RunEvent) -> RunEventSinkResult {
        if event.details.validate().is_err() {
            return RunEventSinkResult::Dropped;
        }
        let Ok(details) = serde_json::to_string(&event.details.attributes) else {
            return RunEventSinkResult::Dropped;
        };
        emit_run_event(event, &details);
        RunEventSinkResult::Accepted
    }
}

fn emit_run_event(event: &RunEvent, details: &str) {
    let resource_id = optional_field(event.resource_id.as_ref());
    let scope = scope_field(event.scope.as_ref());
    let partition_id = optional_field(event.partition_id.as_ref());
    let package_id = optional_field(event.package_id.as_ref());
    let package_hash = optional_field(event.package_hash.as_ref());
    let package_path = optional_field(event.package_path.as_ref());
    let destination_id = optional_field(event.destination_id.as_ref());
    let plan_id = optional_field(event.plan_id.as_ref());
    let checkpoint_id = optional_field(event.checkpoint_id.as_ref());
    let receipt_id = optional_field(event.receipt_id.as_ref());

    tracing::info!(
        target: RUNTIME_RUN_EVENT_TRACE_TARGET,
        run_id = event.run_id.as_str(),
        resource_id = resource_id,
        scope = scope.as_str(),
        partition_id = partition_id,
        package_id = package_id,
        package_hash = package_hash,
        package_path = package_path,
        destination_id = destination_id,
        plan_id = plan_id,
        checkpoint_id = checkpoint_id,
        receipt_id = receipt_id,
        event_kind = event.kind.as_str(),
        sequence = event.sequence,
        timestamp_ms = event.timestamp_ms,
        details = details,
    );
}

fn optional_field<T: AsRef<str>>(value: Option<&T>) -> &str {
    value.map(|value| value.as_ref()).unwrap_or("")
}

fn scope_field(scope: Option<&ScopeKey>) -> String {
    scope
        .and_then(|scope| serde_json::to_string(scope).ok())
        .unwrap_or_default()
}
