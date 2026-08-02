use std::{collections::BTreeMap, time::Instant};

use cdf_kernel::{CdfError, Result, RunPhase, RunPhaseContext, RunPhaseMetric, RunPhaseStatus};

pub(super) fn elapsed_ns(started: Option<Instant>, label: &str) -> Result<u64> {
    let Some(started) = started else {
        return Ok(0);
    };
    u64::try_from(started.elapsed().as_nanos())
        .map_err(|error| CdfError::internal(format!("{label} duration overflow: {error}")))
}

#[derive(Clone, Copy, Debug, Default)]
struct PhaseAggregate {
    duration_ns: u64,
    input_bytes: u64,
    output_bytes: u64,
    operations: u64,
}

pub(super) struct PhaseMeasurements {
    pub(super) enabled: bool,
    values: BTreeMap<(RunPhase, Option<RunPhaseContext>), PhaseAggregate>,
}

impl PhaseMeasurements {
    pub(super) fn new(enabled: bool) -> Self {
        Self {
            enabled,
            values: BTreeMap::new(),
        }
    }

    pub(super) fn start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }

    pub(super) fn add(
        &mut self,
        phase: RunPhase,
        duration_ns: u64,
        input_bytes: u64,
        output_bytes: u64,
    ) {
        self.add_operations(phase, duration_ns, input_bytes, output_bytes, 1);
    }

    pub(super) fn add_operations(
        &mut self,
        phase: RunPhase,
        duration_ns: u64,
        input_bytes: u64,
        output_bytes: u64,
        operations: u64,
    ) {
        self.add_operations_with_context(
            phase,
            None,
            duration_ns,
            input_bytes,
            output_bytes,
            operations,
        );
    }

    pub(super) fn add_operations_with_context(
        &mut self,
        phase: RunPhase,
        context: Option<RunPhaseContext>,
        duration_ns: u64,
        input_bytes: u64,
        output_bytes: u64,
        operations: u64,
    ) {
        if !self.enabled {
            return;
        }
        let metric = self.values.entry((phase, context)).or_default();
        metric.duration_ns = metric.duration_ns.saturating_add(duration_ns);
        metric.input_bytes = metric.input_bytes.saturating_add(input_bytes);
        metric.output_bytes = metric.output_bytes.saturating_add(output_bytes);
        metric.operations = metric.operations.saturating_add(operations);
    }

    pub(super) fn into_metrics(self) -> Vec<RunPhaseMetric> {
        self.values
            .into_iter()
            .map(|((phase, context), metric)| RunPhaseMetric {
                phase,
                context,
                status: RunPhaseStatus::Completed,
                duration_ns: metric.duration_ns,
                input_bytes: metric.input_bytes,
                output_bytes: metric.output_bytes,
                operations: metric.operations,
            })
            .collect()
    }
}
