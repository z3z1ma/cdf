use std::{collections::BTreeMap, sync::Arc, time::Instant};

use cdf_kernel::{
    Batch, BoxFuture, CdfError, OpenedPartitionStream, PartitionCompletion, PayloadRetention,
    Result,
};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest};
use futures_util::{StreamExt, stream::FuturesUnordered};

use crate::RunCancellation;

pub type SourcePartitionOpenFuture<'a, M> =
    BoxFuture<'a, Result<(M, Option<OpenedPartitionStream>)>>;
/// Opens one planned partition under the frontier's cancellation authority.
///
/// The returned future MUST retain and join any opening lifecycle it starts. Once cancellation is
/// observed it must not return until that lifecycle has terminated; the frontier deliberately
/// continues polling admitted open futures during cleanup to uphold that barrier.
pub type SourcePartitionOpener<'a, M> =
    Box<dyn FnMut(usize, RunCancellation) -> Result<SourcePartitionOpenFuture<'a, M>> + Send + 'a>;

type PendingSourceStep<'a, M> = BoxFuture<'a, SourceStepResult<M>>;

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SourceFrontierReport {
    pub partition_count: u64,
    pub maximum_active: u64,
    pub wait_ns: u64,
    pub prefetched_batches: u64,
    pub discarded_prefetched_batches: u64,
    pub peak_ready_partitions: u64,
}

struct SourceState<M> {
    metadata: M,
    stream: Option<OpenedPartitionStream>,
    completion: Option<PartitionCompletion>,
}

enum SourceStep {
    Opened,
    BatchReady,
    Complete,
}

struct SourceStepResult<M> {
    ordinal: usize,
    state: Option<SourceState<M>>,
    batch: Option<Batch>,
    outcome: Result<SourceStep>,
}

struct CurrentSource<M> {
    state: SourceState<M>,
    prefetched: Option<Batch>,
    complete: bool,
}

/// Scheduler-owned one-step source frontier.
///
/// Every admitted ordinal owns exactly one opening/poll future, one ready batch, or the canonical
/// handle currently being consumed. Later streams are polled only until one batch or EOF is ready;
/// they cannot run away behind a stalled head. Opening and polling futures retain the source
/// invocation until terminal cleanup, and [`Self::terminate_and_join`] drains all admitted work.
pub struct CanonicalSourceFrontier<'a, M> {
    partition_count: usize,
    maximum_active: usize,
    opener: SourcePartitionOpener<'a, M>,
    maximum_batch_bytes: u64,
    memory: Option<Arc<dyn MemoryCoordinator>>,
    batch_memory: crate::SourceBatchMemoryContract,
    cancellation: RunCancellation,
    next_to_open: usize,
    canonical_ordinal: usize,
    pending: FuturesUnordered<PendingSourceStep<'a, M>>,
    ready: BTreeMap<usize, SourceStepResult<M>>,
    current: Option<CurrentSource<M>>,
    head_poll_started: bool,
    admission_stopped: bool,
    primary_failure_ordinal: Option<usize>,
    terminal_failures: BTreeMap<usize, CdfError>,
    measurement_enabled: bool,
    wait_ns: u64,
    prefetched_batches: u64,
    discarded_prefetched_batches: u64,
    peak_ready_partitions: usize,
    terminal: bool,
}

impl<M> Unpin for CanonicalSourceFrontier<'_, M> {}

impl<'a, M: Send + 'a> CanonicalSourceFrontier<'a, M> {
    pub fn new(
        partition_count: usize,
        maximum_active: usize,
        opener: SourcePartitionOpener<'a, M>,
        maximum_batch_bytes: u64,
        memory: Option<Arc<dyn MemoryCoordinator>>,
        batch_memory: crate::SourceBatchMemoryContract,
        cancellation: RunCancellation,
    ) -> Result<Self> {
        if maximum_active == 0 {
            return Err(CdfError::contract(
                "canonical source frontier requires nonzero active capacity",
            ));
        }
        if maximum_batch_bytes == 0 {
            return Err(CdfError::contract(
                "canonical source frontier requires a nonzero retained-batch bound",
            ));
        }
        if maximum_active > 1 && memory.is_none() {
            return Err(CdfError::contract(
                "parallel source frontier requires injected memory authority",
            ));
        }
        if partition_count != 0
            && batch_memory == crate::SourceBatchMemoryContract::FrontierReserved
            && memory.is_none()
        {
            return Err(CdfError::contract(
                "frontier-reserved source batches require injected memory authority",
            ));
        }
        let mut frontier = Self {
            partition_count,
            maximum_active,
            opener,
            maximum_batch_bytes,
            memory,
            batch_memory,
            cancellation,
            next_to_open: 0,
            canonical_ordinal: 0,
            pending: FuturesUnordered::new(),
            ready: BTreeMap::new(),
            current: None,
            head_poll_started: false,
            admission_stopped: false,
            primary_failure_ordinal: None,
            terminal_failures: BTreeMap::new(),
            measurement_enabled: false,
            wait_ns: 0,
            prefetched_batches: 0,
            discarded_prefetched_batches: 0,
            peak_ready_partitions: 0,
            terminal: false,
        };
        frontier.fill_head();
        Ok(frontier)
    }

    pub const fn with_measurement(mut self, enabled: bool) -> Self {
        self.measurement_enabled = enabled;
        self
    }

    pub fn report(&self) -> SourceFrontierReport {
        SourceFrontierReport {
            partition_count: u64::try_from(self.partition_count).unwrap_or(u64::MAX),
            maximum_active: u64::try_from(self.maximum_active).unwrap_or(u64::MAX),
            wait_ns: self.wait_ns,
            prefetched_batches: self.prefetched_batches,
            discarded_prefetched_batches: self.discarded_prefetched_batches,
            peak_ready_partitions: u64::try_from(self.peak_ready_partitions).unwrap_or(u64::MAX),
        }
    }

    pub async fn next_partition(&mut self) -> Result<Option<CanonicalSourcePartition<'_, 'a, M>>> {
        if self.current.is_some() {
            return Err(CdfError::internal(
                "canonical source partition was not finished before requesting its successor",
            ));
        }
        if self.terminal || self.canonical_ordinal == self.partition_count {
            self.terminal = true;
            return Ok(None);
        }
        loop {
            if let Some(ready) = self.ready.remove(&self.canonical_ordinal) {
                let SourceStepResult {
                    state,
                    batch,
                    outcome,
                    ..
                } = ready;
                let outcome = match outcome {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        if let Some(state) = state {
                            self.current = Some(CurrentSource {
                                state,
                                prefetched: None,
                                complete: false,
                            });
                        }
                        return Err(self.fail_and_join(error).await);
                    }
                };
                let state = state.ok_or_else(|| {
                    CdfError::internal("canonical source outcome omitted its partition state")
                })?;
                let (prefetched, complete) = match outcome {
                    SourceStep::Opened => (None, false),
                    SourceStep::BatchReady => match batch {
                        Some(batch) => (Some(batch), false),
                        None => {
                            self.current = Some(CurrentSource {
                                state,
                                prefetched: None,
                                complete: false,
                            });
                            return Err(CdfError::internal("ready source step omitted its batch"));
                        }
                    },
                    SourceStep::Complete => (None, true),
                };
                let head_poll_started = prefetched.is_some() || complete;
                self.current = Some(CurrentSource {
                    state,
                    prefetched,
                    complete,
                });
                self.head_poll_started = head_poll_started;
                return Ok(Some(CanonicalSourcePartition {
                    frontier: self,
                    finished: false,
                }));
            }
            let waiting = self.measurement_enabled.then(Instant::now);
            let next = self.pending.next().await;
            if let Some(waiting) = waiting {
                self.wait_ns = self.wait_ns.saturating_add(
                    u64::try_from(waiting.elapsed().as_nanos()).unwrap_or(u64::MAX),
                );
            }
            let step = next.ok_or_else(|| {
                CdfError::internal(
                    "canonical source frontier lost active work before its head opened",
                )
            })?;
            self.accept_step(step).await?;
        }
    }

    pub async fn terminate_and_join(&mut self) -> Result<()> {
        if self.terminal {
            return Ok(());
        }
        let cleanup = self.drain_and_join().await;
        match (self.canonical_terminal_error(), cleanup) {
            (Some(error), Ok(())) => Err(error),
            (Some(error), Err(cleanup)) => Err(with_cleanup_failure(
                error,
                "source frontier termination",
                cleanup,
            )),
            (None, result) => result,
        }
    }

    async fn drain_and_join(&mut self) -> Result<()> {
        self.admission_stopped = true;
        self.cancellation.cancel();
        let mut cleanup_errors = BTreeMap::<usize, Vec<CdfError>>::new();

        if let Some(current) = self.current.take() {
            record_cleanup(
                &mut cleanup_errors,
                self.canonical_ordinal,
                terminate_state(current.state).await,
            );
        }
        for (ordinal, ready) in std::mem::take(&mut self.ready) {
            if ready.batch.is_some() {
                self.discarded_prefetched_batches =
                    self.discarded_prefetched_batches.saturating_add(1);
            }
            if let Err(error) = &ready.outcome {
                self.record_terminal_failure(ordinal, error.clone());
            }
            if let Some(state) = ready.state {
                record_cleanup(&mut cleanup_errors, ordinal, terminate_state(state).await);
            }
        }
        loop {
            let waiting = self.measurement_enabled.then(Instant::now);
            let next = self.pending.next().await;
            if let Some(waiting) = waiting {
                self.wait_ns = self.wait_ns.saturating_add(
                    u64::try_from(waiting.elapsed().as_nanos()).unwrap_or(u64::MAX),
                );
            }
            let Some(step) = next else {
                break;
            };
            if step.batch.is_some() {
                self.discarded_prefetched_batches =
                    self.discarded_prefetched_batches.saturating_add(1);
            }
            if let Err(error) = &step.outcome {
                self.record_terminal_failure(step.ordinal, error.clone());
            }
            if let Some(state) = step.state {
                record_cleanup(
                    &mut cleanup_errors,
                    step.ordinal,
                    terminate_state(state).await,
                );
            }
        }
        self.terminal = true;
        canonical_cleanup_error(cleanup_errors).map_or(Ok(()), Err)
    }

    fn active(&self) -> usize {
        self.pending.len() + self.ready.len() + usize::from(self.current.is_some())
    }

    fn canonical_terminal_error(&self) -> Option<CdfError> {
        let primary_ordinal = self.primary_failure_ordinal?;
        let mut primary = self.terminal_failures.get(&primary_ordinal)?.clone();
        primary.message = format!(
            "source partition ordinal {primary_ordinal} failed: {}",
            primary.message
        );
        if self.terminal_failures.len() > 1 {
            let ordered = self
                .terminal_failures
                .iter()
                .map(|(ordinal, error)| format!("ordinal {ordinal}: {}", error.message))
                .collect::<Vec<_>>()
                .join("; ");
            primary.message = format!(
                "{}; observed source failures in canonical order: {ordered}",
                primary.message
            );
        }
        Some(primary)
    }

    fn record_terminal_failure(&mut self, ordinal: usize, error: CdfError) {
        if is_run_cancellation_error(&error) {
            return;
        }
        self.terminal_failures.entry(ordinal).or_insert(error);
        if self.primary_failure_ordinal.is_none() {
            self.primary_failure_ordinal = Some(ordinal);
            self.admission_stopped = true;
            self.cancellation.cancel();
        }
    }

    async fn fail_and_join(&mut self, fallback: CdfError) -> CdfError {
        let cleanup = self.drain_and_join().await;
        let mut error = self.canonical_terminal_error().unwrap_or(fallback);
        if let Err(cleanup) = cleanup {
            error = with_cleanup_failure(error, "source frontier termination", cleanup);
        }
        error
    }

    fn fill_active(&mut self) {
        self.fill_to(self.maximum_active);
    }

    fn fill_head(&mut self) {
        self.fill_to(1);
    }

    fn fill_to(&mut self, capacity: usize) {
        while !self.admission_stopped
            && self.next_to_open < self.partition_count
            && self.active() < capacity
        {
            let ordinal = self.next_to_open;
            self.next_to_open += 1;
            match (self.opener)(ordinal, self.cancellation.clone()) {
                Ok(opening) => {
                    self.pending.push(Box::pin(async move {
                        // The opener owns the lifecycle barrier for work that has not yet yielded
                        // an OpenedPartitionStream. Keep polling it after cancellation so it can
                        // terminate and join its in-flight PartitionOpenAttempt before returning.
                        match opening.await {
                            Ok((metadata, stream)) => SourceStepResult {
                                ordinal,
                                state: Some(SourceState {
                                    metadata,
                                    stream,
                                    completion: None,
                                }),
                                batch: None,
                                outcome: Ok(SourceStep::Opened),
                            },
                            Err(error) => SourceStepResult {
                                ordinal,
                                state: None,
                                batch: None,
                                outcome: Err(error),
                            },
                        }
                    }))
                }
                Err(error) => {
                    self.record_terminal_failure(ordinal, error.clone());
                    self.ready.insert(
                        ordinal,
                        SourceStepResult {
                            ordinal,
                            state: None,
                            batch: None,
                            outcome: Err(error),
                        },
                    );
                    self.admission_stopped = true;
                }
            }
        }
    }

    fn push_poll(
        &mut self,
        ordinal: usize,
        state: SourceState<M>,
        reservation: Option<cdf_memory::MemoryLease>,
    ) {
        let cancellation = self.cancellation.clone();
        let maximum_batch_bytes = self.maximum_batch_bytes;
        let memory = self.memory.clone();
        let batch_memory = self.batch_memory;
        self.pending.push(Box::pin(async move {
            poll_source_step(
                ordinal,
                state,
                maximum_batch_bytes,
                memory,
                batch_memory,
                reservation,
                cancellation,
            )
            .await
        }));
    }

    fn arm_ready_opened(&mut self) {
        let ordinals = self
            .ready
            .iter()
            .filter_map(|(ordinal, step)| {
                (matches!(step.outcome, Ok(SourceStep::Opened))
                    && step
                        .state
                        .as_ref()
                        .is_some_and(|state| state.stream.is_some()))
                .then_some(*ordinal)
            })
            .collect::<Vec<_>>();
        for ordinal in ordinals {
            let step = self
                .ready
                .remove(&ordinal)
                .expect("ready opened ordinal was collected from this map");
            self.push_poll(
                ordinal,
                step.state.expect("opened source step always carries state"),
                None,
            );
        }
    }

    async fn accept_step(&mut self, step: SourceStepResult<M>) -> Result<()> {
        let ordinal = step.ordinal;
        if ordinal < self.canonical_ordinal
            || self.ready.contains_key(&ordinal)
            || self
                .current
                .as_ref()
                .is_some_and(|_| ordinal == self.canonical_ordinal)
        {
            let mut error =
                CdfError::internal("canonical source frontier received duplicate or retired work");
            if let Some(state) = step.state
                && let Err(cleanup) = terminate_state(state).await
            {
                error = with_cleanup_failure(error, "duplicate source work termination", cleanup);
            }
            return Err(error);
        }
        if let Err(error) = &step.outcome {
            self.admission_stopped = true;
            // A non-head failure must wake a stalled canonical head. The owning executor drains
            // and joins every admitted invocation before returning the ordered diagnostics.
            self.record_terminal_failure(ordinal, error.clone());
        }
        if ordinal != self.canonical_ordinal
            && matches!(step.outcome, Ok(SourceStep::Opened))
            && step
                .state
                .as_ref()
                .is_some_and(|state| state.stream.is_some())
            && self.head_poll_started
        {
            self.push_poll(
                ordinal,
                step.state.expect("opened source state was checked"),
                None,
            );
            return Ok(());
        }
        if ordinal != self.canonical_ordinal && step.batch.is_some() {
            self.prefetched_batches = self.prefetched_batches.saturating_add(1);
        }
        self.ready.insert(ordinal, step);
        self.peak_ready_partitions = self.peak_ready_partitions.max(self.ready.len());
        Ok(())
    }

    async fn poll_current(&mut self) -> Result<Option<Batch>> {
        let ordinal = self.canonical_ordinal;
        let current = self.current.as_mut().ok_or_else(|| {
            CdfError::internal("canonical source frontier has no current partition")
        })?;
        if let Some(batch) = current.prefetched.take() {
            return Ok(Some(batch));
        }
        if current.complete {
            return Ok(None);
        }
        if current.state.stream.is_none() {
            return Err(CdfError::contract(
                "metadata-only source partition cannot be polled for batches",
            ));
        }
        let current = self.current.take().expect("current source was checked");
        let head_reservation = reserve_frontier_poll(
            self.maximum_batch_bytes,
            self.memory.clone(),
            self.batch_memory,
            self.cancellation.clone(),
        )
        .await?;
        self.head_poll_started = true;
        self.push_poll(ordinal, current.state, head_reservation);
        if self.batch_memory == crate::SourceBatchMemoryContract::FrontierReserved {
            self.fill_active();
            self.arm_ready_opened();
        }
        loop {
            let waiting = self.measurement_enabled.then(Instant::now);
            let next = self.pending.next().await;
            if let Some(waiting) = waiting {
                self.wait_ns = self.wait_ns.saturating_add(
                    u64::try_from(waiting.elapsed().as_nanos()).unwrap_or(u64::MAX),
                );
            }
            let step = next.ok_or_else(|| {
                CdfError::internal("canonical source frontier lost its current poll")
            })?;
            let is_current = step.ordinal == ordinal;
            self.accept_step(step).await?;
            if !is_current {
                continue;
            }
            let ready = self
                .ready
                .remove(&ordinal)
                .ok_or_else(|| CdfError::internal("canonical source poll did not become ready"))?;
            let SourceStepResult {
                state,
                batch,
                outcome,
                ..
            } = ready;
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    if let Some(state) = state {
                        self.current = Some(CurrentSource {
                            state,
                            prefetched: None,
                            complete: false,
                        });
                    }
                    return Err(self.fail_and_join(error).await);
                }
            };
            let state = state
                .ok_or_else(|| CdfError::internal("canonical source poll omitted its state"))?;
            match outcome {
                SourceStep::BatchReady => {
                    self.current = Some(CurrentSource {
                        state,
                        prefetched: None,
                        complete: false,
                    });
                    // A pre-accounted producer owns its allocation before the frontier sees the
                    // batch. Do not admit speculative producers until the canonical head has
                    // proved that its first retained outcome acquired memory successfully.
                    if self.batch_memory == crate::SourceBatchMemoryContract::Preaccounted {
                        self.fill_active();
                        self.arm_ready_opened();
                    }
                    return Ok(Some(batch.ok_or_else(|| {
                        CdfError::internal("ready source poll omitted its batch")
                    })?));
                }
                SourceStep::Complete => {
                    self.current = Some(CurrentSource {
                        state,
                        prefetched: None,
                        complete: true,
                    });
                    return Ok(None);
                }
                SourceStep::Opened => {
                    return Err(CdfError::internal(
                        "opened source step repeated after partition admission",
                    ));
                }
            }
        }
    }

    fn finish_current(
        &mut self,
        require_stream_completion: bool,
    ) -> Result<(M, Option<PartitionCompletion>)> {
        let current = self.current.take().ok_or_else(|| {
            CdfError::internal("canonical source frontier has no partition to finish")
        })?;
        if require_stream_completion && current.state.stream.is_some() && !current.complete {
            self.current = Some(current);
            return Err(CdfError::contract(
                "canonical source partition must reach EOF before completion",
            ));
        }
        if !require_stream_completion && current.state.stream.is_some() {
            self.current = Some(current);
            return Err(CdfError::contract(
                "metadata-only completion cannot discard an opened source stream",
            ));
        }
        let SourceState {
            metadata,
            completion,
            ..
        } = current.state;
        self.canonical_ordinal += 1;
        self.head_poll_started = false;
        // Retiring the canonical partition frees one frontier slot. Keep the configured open
        // frontier warm so downstream staging is not starved between partitions; speculative
        // streams still remain gated to one prefetched batch by `head_poll_started`.
        self.fill_active();
        Ok((metadata, completion))
    }

    async fn terminate_current(&mut self) -> Result<(M, Option<PartitionCompletion>)> {
        if self.maximum_active != 1 || !self.pending.is_empty() || !self.ready.is_empty() {
            return Err(CdfError::contract(
                "partial source completion requires a serial frontier with no speculative work",
            ));
        }
        let current = self.current.take().ok_or_else(|| {
            CdfError::internal("canonical source frontier has no partition to terminate")
        })?;
        let SourceState {
            metadata,
            mut stream,
            ..
        } = current.state;
        let source_io = match stream.as_mut() {
            Some(stream) => stream.terminate_and_join_with_source_io().await?,
            None => None,
        };
        self.admission_stopped = true;
        self.terminal = true;
        Ok((
            metadata,
            source_io.map(|metrics| PartitionCompletion::new(None, Some(metrics))),
        ))
    }
}

pub struct CanonicalSourcePartition<'frontier, 'a, M: Send + 'a> {
    frontier: &'frontier mut CanonicalSourceFrontier<'a, M>,
    finished: bool,
}

impl<'frontier, 'a, M: Send + 'a> CanonicalSourcePartition<'frontier, 'a, M> {
    pub fn metadata(&self) -> &M {
        &self
            .frontier
            .current
            .as_ref()
            .expect("canonical source handle always owns current metadata")
            .state
            .metadata
    }

    pub fn has_stream(&self) -> bool {
        self.frontier
            .current
            .as_ref()
            .is_some_and(|current| current.state.stream.is_some())
    }

    pub async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.frontier.poll_current().await
    }

    pub fn finish(&mut self) -> Result<(M, Option<PartitionCompletion>)> {
        let result = self.frontier.finish_current(true)?;
        self.finished = true;
        Ok(result)
    }

    pub fn finish_metadata_only(&mut self) -> Result<M> {
        let (metadata, completion) = self.frontier.finish_current(false)?;
        if completion.is_some() {
            return Err(CdfError::internal(
                "metadata-only source partition unexpectedly carried completion evidence",
            ));
        }
        self.finished = true;
        Ok(metadata)
    }

    pub async fn terminate_partial(&mut self) -> Result<(M, Option<PartitionCompletion>)> {
        let outcome = self.frontier.terminate_current().await?;
        self.finished = true;
        Ok(outcome)
    }
}

impl<M: Send> Drop for CanonicalSourcePartition<'_, '_, M> {
    fn drop(&mut self) {
        if !self.finished {
            self.frontier.admission_stopped = true;
        }
    }
}

async fn reserve_frontier_poll(
    maximum_batch_bytes: u64,
    memory: Option<Arc<dyn MemoryCoordinator>>,
    batch_memory: crate::SourceBatchMemoryContract,
    cancellation: RunCancellation,
) -> Result<Option<cdf_memory::MemoryLease>> {
    if batch_memory == crate::SourceBatchMemoryContract::Preaccounted {
        return Ok(None);
    }
    let request = ConsumerKey::new("source-frontier-poll", MemoryClass::Queue)
        .and_then(|consumer| ReservationRequest::new(consumer, maximum_batch_bytes))?;
    let memory = memory.ok_or_else(|| {
        CdfError::contract("frontier-reserved source poll omitted memory authority")
    })?;
    cancellation
        .await_or_cancel(cdf_memory::reserve(memory, request))
        .await
        .map(Some)
}

async fn poll_source_step<M>(
    ordinal: usize,
    mut state: SourceState<M>,
    maximum_batch_bytes: u64,
    memory: Option<Arc<dyn MemoryCoordinator>>,
    batch_memory: crate::SourceBatchMemoryContract,
    reservation: Option<cdf_memory::MemoryLease>,
    cancellation: RunCancellation,
) -> SourceStepResult<M> {
    let reservation = match (batch_memory, reservation) {
        (crate::SourceBatchMemoryContract::Preaccounted, None) => None,
        (crate::SourceBatchMemoryContract::Preaccounted, Some(_)) => {
            return SourceStepResult {
                ordinal,
                state: Some(state),
                batch: None,
                outcome: Err(CdfError::internal(
                    "preaccounted source poll received a competing frontier reservation",
                )),
            };
        }
        (crate::SourceBatchMemoryContract::FrontierReserved, Some(reservation)) => {
            Some(reservation)
        }
        (crate::SourceBatchMemoryContract::FrontierReserved, None) => match reserve_frontier_poll(
            maximum_batch_bytes,
            memory,
            batch_memory,
            cancellation.clone(),
        )
        .await
        {
            Ok(reservation) => reservation,
            Err(error) => {
                return SourceStepResult {
                    ordinal,
                    state: Some(state),
                    batch: None,
                    outcome: Err(error),
                };
            }
        },
    };
    let Some(stream) = state.stream.as_mut() else {
        return SourceStepResult {
            ordinal,
            state: Some(state),
            batch: None,
            outcome: Err(CdfError::internal(
                "canonical source poll omitted its opened stream",
            )),
        };
    };
    let next = cancellation
        .await_or_cancel(async {
            match stream.next().await {
                Some(batch) => batch.map(Some),
                None => Ok(None),
            }
        })
        .await;
    let (batch, outcome) = match next {
        Ok(Some(batch)) => {
            match adopt_frontier_batch(batch, maximum_batch_bytes, batch_memory, reservation) {
                Ok(batch) => (Some(batch), Ok(SourceStep::BatchReady)),
                Err(error) => (None, Err(error)),
            }
        }
        Ok(None) => match cancellation.await_or_cancel(stream.completion()).await {
            Ok(completion) => {
                state.completion = Some(completion);
                (None, Ok(SourceStep::Complete))
            }
            Err(error) => match stream.terminate_and_join().await {
                Ok(()) => (None, Err(error)),
                Err(cleanup) => (
                    None,
                    Err(with_cleanup_failure(
                        error,
                        "partition completion termination",
                        cleanup,
                    )),
                ),
            },
        },
        Err(error) => {
            let cleanup = if cancellation.is_cancelled() {
                stream.terminate_and_join().await
            } else {
                stream.join_failed_attempt().await
            };
            match cleanup {
                Ok(()) => (None, Err(error)),
                Err(cleanup) => (
                    None,
                    Err(with_cleanup_failure(
                        error,
                        "source stream termination",
                        cleanup,
                    )),
                ),
            }
        }
    };
    SourceStepResult {
        ordinal,
        state: Some(state),
        batch,
        outcome,
    }
}

fn adopt_frontier_batch(
    batch: Batch,
    maximum_batch_bytes: u64,
    batch_memory: crate::SourceBatchMemoryContract,
    reservation: Option<cdf_memory::MemoryLease>,
) -> Result<Batch> {
    let arrow_bytes = batch
        .record_batch()
        .map(cdf_memory::record_batch_retained_bytes)
        .transpose()?
        .unwrap_or(0);
    let payload_bytes = arrow_bytes
        .checked_add(batch.header.pre_contract_evidence_retained_bytes()?)
        .ok_or_else(|| CdfError::data("source frontier payload memory exceeds u64"))?;
    if payload_bytes > maximum_batch_bytes {
        return Err(CdfError::contract(format!(
            "source batch retains {payload_bytes} bytes above its compiled frontier bound {maximum_batch_bytes}"
        )));
    }
    if batch_memory == crate::SourceBatchMemoryContract::Preaccounted {
        let retained_bytes = batch.retained_bytes();
        if retained_bytes < payload_bytes {
            return Err(CdfError::contract(format!(
                "source batch retains {} accounted bytes below its {payload_bytes}-byte Arrow allocation",
                retained_bytes
            )));
        }
        if retained_bytes > maximum_batch_bytes {
            return Err(CdfError::contract(format!(
                "source batch retains {retained_bytes} accounted bytes above its compiled frontier bound {maximum_batch_bytes}"
            )));
        }
        return Ok(batch);
    }
    if batch.retained_bytes() != 0 {
        return Err(CdfError::contract(
            "frontier-reserved source batch carried a competing memory lease",
        ));
    }
    if payload_bytes == 0 {
        return Ok(batch);
    }
    let lease = reservation.ok_or_else(|| {
        CdfError::contract("unaccounted source batches require injected frontier memory authority")
    })?;
    lease.reconcile(payload_bytes)?;
    let retention = PayloadRetention::new(Arc::new(lease), payload_bytes)?;
    batch.with_retention(retention)
}

async fn terminate_state<M>(mut state: SourceState<M>) -> Result<()> {
    match state.stream.as_mut() {
        Some(stream) => stream.terminate_and_join().await,
        None => Ok(()),
    }
}

fn record_cleanup(
    failures: &mut BTreeMap<usize, Vec<CdfError>>,
    ordinal: usize,
    cleanup: Result<()>,
) {
    let Err(cleanup) = cleanup else {
        return;
    };
    failures.entry(ordinal).or_default().push(cleanup);
}

fn canonical_cleanup_error(failures: BTreeMap<usize, Vec<CdfError>>) -> Option<CdfError> {
    if failures.is_empty() {
        return None;
    }
    let message = failures
        .into_iter()
        .flat_map(|(ordinal, errors)| {
            errors
                .into_iter()
                .map(move |error| format!("ordinal {ordinal}: {}", error.message))
        })
        .collect::<Vec<_>>()
        .join("; ");
    Some(CdfError::internal(format!(
        "source termination failures in canonical order: {message}"
    )))
}

fn is_run_cancellation_error(error: &CdfError) -> bool {
    error.kind == cdf_kernel::ErrorKind::Internal
        && error
            .message
            .starts_with("run execution scope is cancelled")
}

fn with_cleanup_failure(mut primary: CdfError, context: &str, cleanup: CdfError) -> CdfError {
    primary.message = format!(
        "{}; {context} also failed: {}",
        primary.message, cleanup.message
    );
    primary
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Waker},
    };

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{
        BatchId, InvocationTermination, PartitionId, PartitionOpenAttempt, PartitionStreamPayload,
        ResourceId, SchemaHash,
    };
    use cdf_memory::DeterministicMemoryCoordinator;
    use futures_channel::oneshot;
    use futures_util::{FutureExt, StreamExt, stream};

    use super::*;

    fn batch(partition: usize, value: i64) -> Batch {
        let record_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
        Batch::from_record_batch(
            BatchId::new(format!("batch-{partition}-{value}")).unwrap(),
            ResourceId::new("test.events").unwrap(),
            PartitionId::new(format!("p{partition}")).unwrap(),
            SchemaHash::new("sha256:test").unwrap(),
            record_batch,
        )
        .unwrap()
    }

    #[test]
    fn preaccounted_batch_cannot_hide_memory_above_its_compiled_bound() {
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(4096, Default::default()).unwrap());
        let request = ReservationRequest::new(
            ConsumerKey::new("oversized-preaccounted-source", MemoryClass::Source).unwrap(),
            2048,
        )
        .unwrap();
        let lease = futures_executor::block_on(cdf_memory::reserve(memory, request)).unwrap();
        let retained = batch(0, 0)
            .with_retention(PayloadRetention::new(Arc::new(lease), 2048).unwrap())
            .unwrap();
        let error = adopt_frontier_batch(
            retained,
            1024,
            crate::SourceBatchMemoryContract::Preaccounted,
            None,
        )
        .unwrap_err();
        assert!(error.message.contains("above its compiled frontier bound"));
    }

    #[test]
    fn preaccounted_head_materializes_before_speculative_producers_open() {
        let (gate_sender, gate_receiver) = oneshot::channel::<()>();
        let mut gate_receiver = Some(gate_receiver);
        let opened_count = Arc::new(AtomicUsize::new(0));
        let opened_for_source = Arc::clone(&opened_count);
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(2048, Default::default()).unwrap());
        let head_memory = Arc::clone(&memory);
        let opener: SourcePartitionOpener<'_, usize> = Box::new(move |ordinal, _cancellation| {
            opened_for_source.fetch_add(1, Ordering::SeqCst);
            match ordinal {
                0 => {
                    let receiver = gate_receiver.take().unwrap();
                    let memory = Arc::clone(&head_memory);
                    Ok(opened(
                        ordinal,
                        stream::once(async move {
                            receiver
                                .await
                                .map_err(|_| CdfError::internal("head gate dropped"))?;
                            let request = ReservationRequest::new(
                                ConsumerKey::new("preaccounted-head", MemoryClass::Source)?,
                                1024,
                            )?;
                            let lease = cdf_memory::reserve(memory, request).await?;
                            batch(0, 0)
                                .with_retention(PayloadRetention::new(Arc::new(lease), 1024)?)
                        }),
                    ))
                }
                1 => Ok(opened(ordinal, stream::empty())),
                _ => Err(CdfError::internal("unexpected ordinal")),
            }
        });
        let mut frontier = CanonicalSourceFrontier::new(
            2,
            2,
            opener,
            1024,
            Some(memory),
            crate::SourceBatchMemoryContract::Preaccounted,
            RunCancellation::default(),
        )
        .unwrap();

        let mut head = futures_executor::block_on(frontier.next_partition())
            .unwrap()
            .unwrap();
        let mut next_head = Box::pin(head.next_batch());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            next_head.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(opened_count.load(Ordering::SeqCst), 1);

        gate_sender.send(()).unwrap();
        let first = futures_executor::block_on(next_head).unwrap().unwrap();
        assert_eq!(opened_count.load(Ordering::SeqCst), 2);
        drop(first);
        assert!(
            futures_executor::block_on(head.next_batch())
                .unwrap()
                .is_none()
        );
        head.finish().unwrap();
        drop(head);
        futures_executor::block_on(frontier.terminate_and_join()).unwrap();
    }

    fn opened<'a>(
        ordinal: usize,
        batches: impl futures_util::Stream<Item = Result<Batch>> + Send + 'static,
    ) -> SourcePartitionOpenFuture<'a, usize> {
        async move {
            let attempt = PartitionOpenAttempt::materialized(Box::pin(async move {
                Ok(PartitionStreamPayload::batches(Box::pin(batches)))
            }));
            Ok((ordinal, Some(attempt.await?)))
        }
        .boxed()
    }

    fn opened_with_lifecycle<'a>(
        ordinal: usize,
        batches: impl futures_util::Stream<Item = Result<Batch>> + Send + 'static,
        cancelled: Arc<AtomicUsize>,
        joined: Arc<AtomicUsize>,
    ) -> SourcePartitionOpenFuture<'a, usize> {
        async move {
            let cancel_count = Arc::clone(&cancelled);
            let join_count = Arc::clone(&joined);
            let termination = InvocationTermination::new(
                move || {
                    cancel_count.fetch_add(1, Ordering::SeqCst);
                },
                Box::pin(async move {
                    join_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            );
            let attempt = PartitionOpenAttempt::with_termination(
                Box::pin(async move { Ok(PartitionStreamPayload::batches(Box::pin(batches))) }),
                termination,
            );
            Ok((ordinal, Some(attempt.await?)))
        }
        .boxed()
    }

    fn pending_open_with_lifecycle<'a>(
        ordinal: usize,
        cancellation: RunCancellation,
        cancelled: Arc<AtomicUsize>,
        joined: Arc<AtomicUsize>,
    ) -> SourcePartitionOpenFuture<'a, usize> {
        async move {
            let cancel_count = Arc::clone(&cancelled);
            let join_count = Arc::clone(&joined);
            let termination = InvocationTermination::new(
                move || {
                    cancel_count.fetch_add(1, Ordering::SeqCst);
                },
                Box::pin(async move {
                    join_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            );
            let mut attempt = PartitionOpenAttempt::with_termination(
                Box::pin(futures_util::future::pending()),
                termination,
            );
            match cancellation.await_or_cancel(&mut attempt).await {
                Ok(stream) => Ok((ordinal, Some(stream))),
                Err(error) => {
                    attempt.terminate_and_join().await?;
                    Err(error)
                }
            }
        }
        .boxed()
    }

    #[test]
    fn stalled_head_polls_one_later_batch_and_adopts_its_memory() {
        let (gate_sender, gate_receiver) = oneshot::channel::<()>();
        let mut gate_receiver = Some(gate_receiver);
        let later_polls = Arc::new(AtomicUsize::new(0));
        let later_polls_for_open = Arc::clone(&later_polls);
        let opener: SourcePartitionOpener<'_, usize> =
            Box::new(move |ordinal, _cancellation| match ordinal {
                0 => {
                    let receiver = gate_receiver.take().unwrap();
                    Ok(async move {
                        let values = stream::once(async move {
                            receiver
                                .await
                                .map_err(|_| CdfError::internal("gate dropped"))?;
                            Ok(batch(0, 0))
                        });
                        let attempt = PartitionOpenAttempt::materialized(Box::pin(async move {
                            Ok(PartitionStreamPayload::batches(Box::pin(values)))
                        }));
                        Ok((ordinal, Some(attempt.await?)))
                    }
                    .boxed())
                }
                1 => {
                    let polls = Arc::clone(&later_polls_for_open);
                    Ok(async move {
                        let values =
                            stream::iter([Ok(batch(1, 10)), Ok(batch(1, 11))]).inspect(move |_| {
                                polls.fetch_add(1, Ordering::SeqCst);
                            });
                        let attempt = PartitionOpenAttempt::materialized(Box::pin(async move {
                            Ok(PartitionStreamPayload::batches(Box::pin(values)))
                        }));
                        Ok((ordinal, Some(attempt.await?)))
                    }
                    .boxed())
                }
                _ => Err(CdfError::internal("unexpected ordinal")),
            });
        let memory =
            Arc::new(DeterministicMemoryCoordinator::new(2048, Default::default()).unwrap());
        let memory_authority: Arc<dyn MemoryCoordinator> = memory.clone();
        let mut frontier = CanonicalSourceFrontier::new(
            2,
            2,
            opener,
            1024,
            Some(memory_authority),
            crate::SourceBatchMemoryContract::FrontierReserved,
            RunCancellation::default(),
        )
        .unwrap()
        .with_measurement(true);
        let mut open_head = Box::pin(frontier.next_partition());
        let mut context = Context::from_waker(Waker::noop());
        let mut head = match open_head.as_mut().poll(&mut context) {
            Poll::Ready(Ok(Some(head))) => head,
            Poll::Ready(Ok(None)) => panic!("head frontier ended before opening"),
            Poll::Ready(Err(error)) => panic!("head open failed: {error}"),
            Poll::Pending => panic!("head opening unexpectedly remained pending"),
        };
        drop(open_head);
        let mut next_head = Box::pin(head.next_batch());
        assert!(matches!(
            next_head.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(later_polls.load(Ordering::SeqCst), 1);
        assert!(memory.snapshot().current_bytes <= 2048);

        gate_sender.send(()).unwrap();
        let first = futures_executor::block_on(next_head).unwrap().unwrap();
        assert_eq!(first.header.partition_id.as_str(), "p0");
        drop(first);
        assert!(
            futures_executor::block_on(head.next_batch())
                .unwrap()
                .is_none()
        );
        head.finish().unwrap();
        drop(head);

        let mut later = futures_executor::block_on(frontier.next_partition())
            .unwrap()
            .unwrap();
        let first = futures_executor::block_on(later.next_batch())
            .unwrap()
            .unwrap();
        assert_eq!(first.header.partition_id.as_str(), "p1");
        drop(first);
        assert_eq!(later_polls.load(Ordering::SeqCst), 1);
        assert!(
            futures_executor::block_on(later.next_batch())
                .unwrap()
                .is_some()
        );
        assert!(
            futures_executor::block_on(later.next_batch())
                .unwrap()
                .is_none()
        );
        later.finish().unwrap();
        drop(later);
        assert_eq!(memory.snapshot().current_bytes, 0);
        let report = frontier.report();
        assert_eq!(report.partition_count, 2);
        assert_eq!(report.maximum_active, 2);
        assert_eq!(report.prefetched_batches, 1);
        assert_eq!(report.discarded_prefetched_batches, 0);
        assert!(report.peak_ready_partitions >= 1);
        assert!(report.wait_ns > 0);
    }

    #[test]
    fn partition_finish_replenishes_the_configured_open_frontier() {
        let (gate_sender, gate_receiver) = oneshot::channel::<()>();
        let mut gate_receiver = Some(gate_receiver);
        let opened_count = Arc::new(AtomicUsize::new(0));
        let opened_for_opener = Arc::clone(&opened_count);
        let opener: SourcePartitionOpener<'_, usize> = Box::new(move |ordinal, _cancellation| {
            opened_for_opener.fetch_add(1, Ordering::SeqCst);
            match ordinal {
                0 => {
                    let receiver = gate_receiver.take().unwrap();
                    Ok(opened(
                        ordinal,
                        stream::once(async move {
                            receiver
                                .await
                                .map_err(|_| CdfError::internal("gate dropped"))?;
                            Ok(batch(0, 0))
                        }),
                    ))
                }
                1 => Ok(opened(ordinal, stream::once(async { Ok(batch(1, 10)) }))),
                2 => Ok(opened(ordinal, stream::empty())),
                _ => Err(CdfError::internal("unexpected ordinal")),
            }
        });
        let memory =
            Arc::new(DeterministicMemoryCoordinator::new(2048, Default::default()).unwrap());
        let memory_authority: Arc<dyn MemoryCoordinator> = memory.clone();
        let mut frontier = CanonicalSourceFrontier::new(
            3,
            2,
            opener,
            1024,
            Some(memory_authority),
            crate::SourceBatchMemoryContract::FrontierReserved,
            RunCancellation::default(),
        )
        .unwrap();

        let mut head = futures_executor::block_on(frontier.next_partition())
            .unwrap()
            .unwrap();
        let mut next_head = Box::pin(head.next_batch());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            next_head.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(opened_count.load(Ordering::SeqCst), 2);

        gate_sender.send(()).unwrap();
        let first = futures_executor::block_on(next_head).unwrap().unwrap();
        assert_eq!(first.header.partition_id.as_str(), "p0");
        drop(first);
        assert!(
            futures_executor::block_on(head.next_batch())
                .unwrap()
                .is_none()
        );
        head.finish().unwrap();
        drop(head);

        assert_eq!(opened_count.load(Ordering::SeqCst), 3);
        futures_executor::block_on(frontier.terminate_and_join()).unwrap();
    }

    #[test]
    fn later_error_cancels_a_stalled_head_and_stops_admission() {
        let opened_count = Arc::new(AtomicUsize::new(0));
        let opened_for_opener = Arc::clone(&opened_count);
        let opener: SourcePartitionOpener<'_, usize> = Box::new(move |ordinal, _cancellation| {
            opened_for_opener.fetch_add(1, Ordering::SeqCst);
            match ordinal {
                0 => Ok(opened(ordinal, stream::pending())),
                1 => Ok(opened(
                    ordinal,
                    stream::iter([Err(CdfError::data("later failed"))]),
                )),
                _ => Ok(opened(ordinal, stream::iter([Ok(batch(ordinal, 99))]))),
            }
        });
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, Default::default()).unwrap());
        let mut frontier = CanonicalSourceFrontier::new(
            4,
            2,
            opener,
            1024,
            Some(memory),
            crate::SourceBatchMemoryContract::FrontierReserved,
            RunCancellation::default(),
        )
        .unwrap();
        let mut head = futures_executor::block_on(frontier.next_partition())
            .unwrap()
            .unwrap();
        let error = futures_executor::block_on(head.next_batch()).unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert_eq!(
            error.message,
            "source partition ordinal 1 failed: later failed"
        );
        drop(head);
        assert_eq!(opened_count.load(Ordering::SeqCst), 2);
        futures_executor::block_on(frontier.terminate_and_join()).unwrap();
    }

    #[test]
    fn later_failure_preserves_and_joins_an_in_flight_opening_lifecycle() {
        let opening_cancelled = Arc::new(AtomicUsize::new(0));
        let opening_joined = Arc::new(AtomicUsize::new(0));
        let opener: SourcePartitionOpener<'_, usize> = Box::new({
            let opening_cancelled = Arc::clone(&opening_cancelled);
            let opening_joined = Arc::clone(&opening_joined);
            move |ordinal, cancellation| match ordinal {
                0 => Ok(opened(ordinal, stream::pending())),
                1 => Ok(pending_open_with_lifecycle(
                    ordinal,
                    cancellation,
                    Arc::clone(&opening_cancelled),
                    Arc::clone(&opening_joined),
                )),
                2 => Ok(opened(
                    ordinal,
                    stream::iter([Err(CdfError::data("later failed"))]),
                )),
                _ => Err(CdfError::internal("unexpected ordinal")),
            }
        });
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(3072, Default::default()).unwrap());
        let mut frontier = CanonicalSourceFrontier::new(
            3,
            3,
            opener,
            1024,
            Some(memory),
            crate::SourceBatchMemoryContract::FrontierReserved,
            RunCancellation::default(),
        )
        .unwrap();

        let mut head = futures_executor::block_on(frontier.next_partition())
            .unwrap()
            .unwrap();
        let error = futures_executor::block_on(head.next_batch()).unwrap_err();
        assert!(error.message.contains("later failed"));
        drop(head);
        futures_executor::block_on(frontier.terminate_and_join()).unwrap();

        assert_eq!(opening_cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(opening_joined.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn terminal_failures_observed_during_cancellation_render_in_canonical_order() {
        let opener: SourcePartitionOpener<'_, usize> =
            Box::new(move |ordinal, cancellation| match ordinal {
                0 => Ok(opened(ordinal, stream::pending())),
                1 => Ok(async move {
                    cancellation.cancelled().await;
                    Err(CdfError::data("earlier ordinal also failed"))
                }
                .boxed()),
                2 => Ok(async move { Err(CdfError::data("primary later failure")) }.boxed()),
                _ => Err(CdfError::internal("unexpected ordinal")),
            });
        let memory: Arc<dyn MemoryCoordinator> =
            Arc::new(DeterministicMemoryCoordinator::new(3072, Default::default()).unwrap());
        let mut frontier = CanonicalSourceFrontier::new(
            3,
            3,
            opener,
            1024,
            Some(memory),
            crate::SourceBatchMemoryContract::FrontierReserved,
            RunCancellation::default(),
        )
        .unwrap();

        let mut head = futures_executor::block_on(frontier.next_partition())
            .unwrap()
            .unwrap();
        let error = futures_executor::block_on(head.next_batch()).unwrap_err();
        assert!(
            error
                .message
                .starts_with("source partition ordinal 2 failed: primary later failure")
        );
        assert!(error.message.contains(
            "observed source failures in canonical order: ordinal 1: earlier ordinal also failed; ordinal 2: primary later failure"
        ));
    }

    #[test]
    fn head_failure_terminates_every_admitted_invocation_exactly_once() {
        let (gate_sender, gate_receiver) = oneshot::channel::<()>();
        let mut gate_receiver = Some(gate_receiver);
        let later_polls = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(AtomicUsize::new(0));
        let joined = Arc::new(AtomicUsize::new(0));
        let opener: SourcePartitionOpener<'_, usize> = Box::new({
            let later_polls = Arc::clone(&later_polls);
            let cancelled = Arc::clone(&cancelled);
            let joined = Arc::clone(&joined);
            move |ordinal, _cancellation| match ordinal {
                0 => {
                    let receiver = gate_receiver.take().unwrap();
                    let values = stream::once(async move {
                        receiver
                            .await
                            .map_err(|_| CdfError::internal("gate dropped"))?;
                        Err(CdfError::data("canonical head failed"))
                    });
                    Ok(opened_with_lifecycle(
                        ordinal,
                        values,
                        Arc::clone(&cancelled),
                        Arc::clone(&joined),
                    ))
                }
                1 => {
                    let polls = Arc::clone(&later_polls);
                    let values = stream::iter([Ok(batch(1, 10))]).inspect(move |_| {
                        polls.fetch_add(1, Ordering::SeqCst);
                    });
                    Ok(opened_with_lifecycle(
                        ordinal,
                        values,
                        Arc::clone(&cancelled),
                        Arc::clone(&joined),
                    ))
                }
                _ => Err(CdfError::internal("unexpected ordinal")),
            }
        });
        let memory =
            Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, Default::default()).unwrap());
        let memory_authority: Arc<dyn MemoryCoordinator> = memory.clone();
        let mut frontier = CanonicalSourceFrontier::new(
            2,
            2,
            opener,
            1024,
            Some(memory_authority),
            crate::SourceBatchMemoryContract::FrontierReserved,
            RunCancellation::default(),
        )
        .unwrap();

        let mut head = futures_executor::block_on(frontier.next_partition())
            .unwrap()
            .unwrap();
        let mut next_head = Box::pin(head.next_batch());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            next_head.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(later_polls.load(Ordering::SeqCst), 1);
        assert!(memory.snapshot().current_bytes > 0);

        gate_sender.send(()).unwrap();
        let error = futures_executor::block_on(next_head).unwrap_err();
        assert_eq!(
            error.message,
            "source partition ordinal 0 failed: canonical head failed"
        );
        drop(head);
        futures_executor::block_on(frontier.terminate_and_join()).unwrap();

        assert_eq!(cancelled.load(Ordering::SeqCst), 2);
        assert_eq!(joined.load(Ordering::SeqCst), 2);
        assert_eq!(memory.snapshot().current_bytes, 0);
    }
}
