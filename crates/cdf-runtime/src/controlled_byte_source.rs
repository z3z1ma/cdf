use std::{sync::Arc, time::Duration};

use cdf_kernel::{BoxFuture, CdfError, ErrorKind, Result};

use crate::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    ExactRangeCoalescingPolicy, ExactRangeReadBatch, ExecutionServices, GenerationStrength,
    RunCancellation, SequentialReadRequest, SourceIoController, SourceIoControllerLimits,
    SourceRetryPolicy, format::execute_exact_range_batch,
};

/// Runtime-only controller around one immutable byte source.
///
/// The origin key is operational authority only. It is never serialized and must be the
/// credential-free transport origin supplied by the transport adapter. All sources sharing that
/// key share request admission, throttle pressure, and low-gain concurrency feedback.
pub struct ControlledByteSource {
    inner: Arc<dyn ByteSource>,
    execution: ExecutionServices,
    controller: SourceIoController,
    retry: SourceRetryPolicy,
}

impl ControlledByteSource {
    pub fn new(
        inner: Arc<dyn ByteSource>,
        origin: impl Into<String>,
        execution: ExecutionServices,
        limits: SourceIoControllerLimits,
        retry: SourceRetryPolicy,
    ) -> Result<Self> {
        inner.capabilities().validate()?;
        limits.validate()?;
        retry.validate()?;
        if !inner.capabilities().exact_ranges
            || inner.identity().strength == GenerationStrength::Weak
        {
            return Err(CdfError::contract(
                "controlled exact-range source requires enforceable strong generation ranges",
            ));
        }
        if limits.maximum_concurrency > inner.capabilities().useful_range_concurrency {
            return Err(CdfError::contract(
                "source I/O controller concurrency exceeds the provider capability ceiling",
            ));
        }
        let origin = origin.into();
        let controller = execution.resolve_source_io_controller(&origin, limits)?;
        Ok(Self {
            inner,
            execution,
            controller,
            retry,
        })
    }

    async fn read_range_with_retry(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> Result<cdf_memory::AccountedBytes> {
        let started = self.execution.monotonic_now();
        let mut attempt = 1_u16;
        loop {
            cancellation.check()?;
            let shared_delay = self.controller.backoff_remaining()?;
            if !shared_delay.is_zero() {
                self.execution
                    .delay(shared_delay, cancellation.clone())
                    .await?;
            }
            let permit = self.controller.acquire(cancellation.clone()).await?;
            let result = self
                .inner
                .read_exact_range(extent, cancellation.clone())
                .await;
            drop(permit);
            let error = match result {
                Ok(bytes) => return Ok(bytes),
                Err(error) => error,
            };
            let retryable = matches!(error.kind, ErrorKind::Transient | ErrorKind::RateLimited);
            if !retryable || attempt >= self.retry.max_total_attempts {
                return Err(error);
            }
            self.controller.observe_retry(&error)?;
            let elapsed = self.execution.monotonic_now().saturating_sub(started);
            let maximum_elapsed = Duration::from_millis(self.retry.max_elapsed_ms);
            if elapsed >= maximum_elapsed {
                return Err(error);
            }
            let exponent = u32::from(attempt.saturating_sub(1)).min(62);
            let exponential = self
                .retry
                .base_delay_ms
                .saturating_mul(1_u64 << exponent)
                .min(self.retry.max_delay_ms);
            let jitter = self.execution.entropy_u64() % exponential.saturating_add(1);
            let delay_ms = jitter.max(error.retry_after_ms.unwrap_or_default());
            let delay = Duration::from_millis(delay_ms);
            if elapsed.saturating_add(delay) >= maximum_elapsed {
                return Err(error);
            }
            if !delay.is_zero() {
                self.execution.delay(delay, cancellation.clone()).await?;
            }
            attempt = attempt.saturating_add(1);
        }
    }
}

impl std::fmt::Debug for ControlledByteSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlledByteSource")
            .field("identity", self.inner.identity())
            .field("controller", &self.controller)
            .finish_non_exhaustive()
    }
}

impl ByteSource for ControlledByteSource {
    fn identity(&self) -> &ContentIdentity {
        self.inner.identity()
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        self.inner.capabilities()
    }

    fn exact_range_coalescing_policy(&self) -> ExactRangeCoalescingPolicy {
        self.inner.exact_range_coalescing_policy()
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>> {
        self.inner.open_sequential(request)
    }

    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<cdf_memory::AccountedBytes>> {
        Box::pin(self.read_range_with_retry(extent, cancellation))
    }

    fn read_exact_ranges(
        &self,
        extents: Vec<ByteExtent>,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<ExactRangeReadBatch>> {
        Box::pin(async move {
            let started = self.execution.monotonic_now();
            let batch = execute_exact_range_batch(self, extents, cancellation).await?;
            self.controller.observe_batch(
                batch.physical_bytes(),
                self.execution.monotonic_now().saturating_sub(started),
            )?;
            Ok(batch)
        })
    }

    fn release_before(&self, frontier: u64) -> Result<()> {
        self.inner.release_before(frontier)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    };

    use bytes::Bytes;
    use cdf_memory::{
        AccountedBytes, ConsumerKey, DeterministicMemoryCoordinator, MemoryClass,
        MemoryCoordinator, ReservationRequest,
    };

    use super::*;
    use crate::{
        BlockingLaneSpec, BlockingValueTask, ExecutionHost, ExecutionHostCapabilities,
        ExecutionTaskScope, IoValue, IoValueTask,
    };

    struct RetryRangeSource {
        identity: ContentIdentity,
        capabilities: ByteSourceCapabilities,
        memory: Arc<dyn MemoryCoordinator>,
        failures_remaining: AtomicUsize,
        requests: Mutex<Vec<ByteExtent>>,
        payload: Bytes,
    }

    impl ByteSource for RetryRangeSource {
        fn identity(&self) -> &ContentIdentity {
            &self.identity
        }

        fn capabilities(&self) -> &ByteSourceCapabilities {
            &self.capabilities
        }

        fn open_sequential(
            &self,
            _request: SequentialReadRequest,
        ) -> BoxFuture<'_, Result<AccountedByteStream>> {
            Box::pin(async { Err(CdfError::internal("unused sequential retry test read")) })
        }

        fn read_exact_range(
            &self,
            extent: ByteExtent,
            cancellation: RunCancellation,
        ) -> BoxFuture<'_, Result<AccountedBytes>> {
            Box::pin(async move {
                cancellation.check()?;
                self.requests.lock().unwrap().push(extent);
                if self
                    .failures_remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
                {
                    return Err(CdfError::rate_limited(
                        "synthetic provider throttle",
                        Some(25),
                    ));
                }
                let start = usize::try_from(extent.start)
                    .map_err(|_| CdfError::data("test range start exceeds usize"))?;
                let length = usize::try_from(extent.length)
                    .map_err(|_| CdfError::data("test range length exceeds usize"))?;
                let end = start
                    .checked_add(length)
                    .ok_or_else(|| CdfError::data("test range overflows usize"))?;
                let lease = self
                    .memory
                    .try_reserve(&ReservationRequest::new(
                        ConsumerKey::new("controlled-byte-source-test", MemoryClass::Source)?,
                        extent.length,
                    )?)?
                    .ok_or_else(|| CdfError::internal("test range reservation was refused"))?;
                AccountedBytes::new(self.payload.slice(start..end), lease)
            })
        }
    }

    struct VirtualHost {
        memory: Arc<dyn MemoryCoordinator>,
        now_ms: AtomicU64,
    }

    impl ExecutionHost for VirtualHost {
        fn capabilities(&self) -> ExecutionHostCapabilities {
            ExecutionHostCapabilities {
                logical_cpu_slots: 4,
                io_workers: 4,
                blocking_lanes: Vec::new(),
            }
        }

        fn memory(&self) -> Arc<dyn MemoryCoordinator> {
            Arc::clone(&self.memory)
        }

        fn spill(&self) -> Arc<dyn crate::SpillBudgetCoordinator> {
            panic!("controlled byte-source test does not spill")
        }

        fn open_scope(&self, _run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
            panic!("controlled byte-source test does not open scopes")
        }

        fn run_io_blocking(&self, _task: IoValueTask) -> Result<IoValue> {
            panic!("controlled byte-source test does not block on I/O")
        }

        fn delay(
            &self,
            duration: Duration,
            cancellation: RunCancellation,
        ) -> BoxFuture<'static, Result<()>> {
            self.now_ms.fetch_add(
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
                Ordering::SeqCst,
            );
            Box::pin(async move { cancellation.check() })
        }

        fn monotonic_now(&self) -> Duration {
            Duration::from_millis(self.now_ms.load(Ordering::SeqCst))
        }

        fn unix_now(&self) -> Duration {
            self.monotonic_now()
        }

        fn entropy_u64(&self) -> u64 {
            0
        }

        fn ensure_blocking_lanes(&self, _lanes: &[BlockingLaneSpec]) -> Result<()> {
            Ok(())
        }

        fn run_blocking_value(&self, _lane: &str, _task: BlockingValueTask) -> Result<IoValue> {
            panic!("controlled byte-source test does not run blocking work")
        }
    }

    #[test]
    fn exact_range_retry_preserves_extent_and_reports_adaptive_feedback() {
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(1024, std::collections::BTreeMap::new()).unwrap(),
        );
        let source = Arc::new(RetryRangeSource {
            identity: ContentIdentity {
                stable_id: "https://data.example/object.parquet".to_owned(),
                size_bytes: Some(16),
                generation: Some("etag-1".to_owned()),
                checksum: None,
                strength: GenerationStrength::Strong,
            },
            capabilities: ByteSourceCapabilities {
                known_length: true,
                reopenable: true,
                seekable: true,
                exact_ranges: true,
                useful_range_concurrency: 4,
                minimum_chunk_bytes: 1,
                maximum_chunk_bytes: 16,
            },
            memory: Arc::clone(&memory),
            failures_remaining: AtomicUsize::new(1),
            requests: Mutex::new(Vec::new()),
            payload: Bytes::from_static(b"0123456789abcdef"),
        });
        let execution = ExecutionServices::new(Arc::new(VirtualHost {
            memory,
            now_ms: AtomicU64::new(0),
        }))
        .unwrap();
        let limits = SourceIoControllerLimits::automatic(4).unwrap();
        let controlled = ControlledByteSource::new(
            Arc::clone(&source) as Arc<dyn ByteSource>,
            "https://data.example",
            execution.clone(),
            limits,
            SourceRetryPolicy::default(),
        )
        .unwrap();

        let batch = futures_executor::block_on(controlled.read_exact_ranges(
            vec![
                ByteExtent::new(0, 4).unwrap(),
                ByteExtent::new(4, 4).unwrap(),
            ],
            RunCancellation::default(),
        ))
        .unwrap();
        assert_eq!(batch.logical()[0].payload(), b"0123".as_slice());
        assert_eq!(batch.logical()[1].payload(), b"4567".as_slice());
        assert_eq!(batch.physical_bytes(), 8);
        assert_eq!(
            source.requests.lock().unwrap().as_slice(),
            &[
                ByteExtent::new(0, 8).unwrap(),
                ByteExtent::new(0, 8).unwrap()
            ]
        );
        let report = execution.scheduler_report().unwrap().source_io_controller;
        assert_eq!(report.retries, 1);
        assert_eq!(report.throttles, 1);
        assert_eq!(report.physical_bytes, 8);
        assert_eq!(report.upward_adjustments, 1);
        assert_eq!(report.current_concurrency, 3);
        assert_eq!(report.acquired_requests, 2);
    }
}
