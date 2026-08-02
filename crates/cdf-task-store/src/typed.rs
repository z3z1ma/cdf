//! Typed codecs, builders, parse admission, and retained decoded tasks.

use std::io::{self, Write};
use std::sync::Arc;

use cdf_kernel::{CdfError, PlannedTaskSetReference, Result};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest,
    reserve_blocking,
};
use cdf_runtime::{RunCancellation, SpillBudgetCoordinator};

use crate::canonical::CanonicalTaskSetBuilder;
use crate::encoded::{
    DigestingWriter, ExternalTaskSetArtifact, ExternalTaskSetReader, ExternalTaskSetWriter,
};
use crate::limits::{CanonicalTaskSetLimits, TaskSetLimits, require_token};
use crate::store::ExternalTaskStore;

/// Source-owned typed decoding and validation at the external task-set boundary.
///
/// The codec owns no catalog lifecycle. It only translates already-accounted canonical bytes
/// into source types and exposes their independent authority, ordinal, and content identities for
/// the shared reader to verify.
pub trait ExternalTaskSetCodec: Send {
    type Authority: Send + Sync + 'static;
    type Task: Send + Sync + 'static;

    fn decode_authority(&self, payload: &[u8]) -> Result<Self::Authority>;
    fn authority_content_sha256(&self, authority: &Self::Authority) -> Result<String>;
    fn decode_task(&self, payload: &[u8], authority: &Self::Authority) -> Result<Self::Task>;
    fn task_canonical_ordinal(&self, task: &Self::Task) -> u64;
    fn encode_task(&self, task: &Self::Task, output: &mut dyn Write) -> Result<()>;
}

fn encoded_task_content_sha256<C>(codec: &C, task: &C::Task) -> Result<String>
where
    C: ExternalTaskSetCodec,
{
    let mut sink = io::sink();
    let mut hashing = DigestingWriter::new(&mut sink);
    codec.encode_task(task, &mut hashing)?;
    Ok(format!("sha256:{}", hex::encode(hashing.finalize())))
}

/// The source-owned typed encoding half of an external task-set boundary.
pub trait ExternalTaskPlanningCodec: ExternalTaskSetCodec {
    fn set_task_canonical_ordinal(&self, task: &mut Self::Task, ordinal: u64);
    fn encode_authority(&self, authority: &Self::Authority, output: &mut dyn Write) -> Result<()>;
}

/// Typed builder for tasks whose source planner already emits canonical order.
pub struct TypedExternalTaskSetBuilder<C>
where
    C: ExternalTaskPlanningCodec,
{
    writer: ExternalTaskSetWriter,
    codec: C,
    cancellation: RunCancellation,
    next_ordinal: u64,
}

impl<C> TypedExternalTaskSetBuilder<C>
where
    C: ExternalTaskPlanningCodec,
{
    pub fn new(
        store: &ExternalTaskStore,
        task_type: &str,
        limits: TaskSetLimits,
        memory: Arc<dyn MemoryCoordinator>,
        spill: &dyn SpillBudgetCoordinator,
        cancellation: RunCancellation,
        codec: C,
    ) -> Result<Self> {
        cancellation.check()?;
        Ok(Self {
            writer: store.writer(task_type, limits, memory, spill)?,
            codec,
            cancellation,
            next_ordinal: 0,
        })
    }

    pub fn push(&mut self, task: &mut C::Task) -> Result<u64> {
        self.cancellation.check()?;
        let ordinal = self.next_ordinal;
        self.codec.set_task_canonical_ordinal(task, ordinal);
        if self.codec.task_canonical_ordinal(task) != ordinal {
            return Err(CdfError::internal(
                "typed task codec did not install the requested canonical ordinal",
            ));
        }
        self.writer
            .push_with(ordinal, |output| self.codec.encode_task(task, output))?;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("typed task-set ordinal exceeds u64"))?;
        self.cancellation.check()?;
        Ok(ordinal)
    }

    pub fn task_count(&self) -> u64 {
        self.next_ordinal
    }

    pub fn finalize(self, authority: &C::Authority) -> Result<ExternalTaskSetArtifact> {
        self.cancellation.check()?;
        let expected_authority_sha256 = self.codec.authority_content_sha256(authority)?;
        self.writer.finalize_with_authority_hash_and_cancellation(
            &expected_authority_sha256,
            &self.cancellation,
            |output| self.codec.encode_authority(authority, output),
        )
    }
}

/// Typed builder for provider-order tasks that need spill-backed canonical sorting.
pub struct TypedCanonicalTaskSetBuilder<C>
where
    C: ExternalTaskPlanningCodec,
{
    builder: CanonicalTaskSetBuilder,
    codec: C,
    cancellation: RunCancellation,
}

impl<C> TypedCanonicalTaskSetBuilder<C>
where
    C: ExternalTaskPlanningCodec,
{
    pub fn new(
        store: &ExternalTaskStore,
        task_type: &str,
        limits: CanonicalTaskSetLimits,
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
        cancellation: RunCancellation,
        codec: C,
    ) -> Result<Self> {
        cancellation.check()?;
        Ok(Self {
            builder: store.canonical_builder(task_type, limits, memory, spill)?,
            codec,
            cancellation,
        })
    }

    pub fn push_idempotent_by<F>(&mut self, mut task: C::Task, sort_key: F) -> Result<bool>
    where
        F: for<'a> FnOnce(&'a C::Task) -> &'a [u8],
    {
        self.cancellation.check()?;
        self.codec.set_task_canonical_ordinal(&mut task, 0);
        let inserted = self
            .builder
            .push_idempotent_with(sort_key(&task), |output| {
                self.codec.encode_task(&task, output)
            })?;
        self.cancellation.check()?;
        Ok(inserted)
    }

    pub fn task_count(&self) -> u64 {
        self.builder.task_count()
    }

    pub fn finalize(self, authority: &C::Authority) -> Result<ExternalTaskSetArtifact> {
        self.cancellation.check()?;
        let expected_authority_sha256 = self.codec.authority_content_sha256(authority)?;
        let codec = &self.codec;
        let cancellation = &self.cancellation;
        self.builder.finalize_transformed_with_authority_hash(
            &expected_authority_sha256,
            &self.cancellation,
            |ordinal, payload, output| {
                cancellation.check()?;
                let mut task = codec.decode_task(payload, authority)?;
                codec.set_task_canonical_ordinal(&mut task, ordinal);
                if codec.task_canonical_ordinal(&task) != ordinal {
                    return Err(CdfError::internal(
                        "typed canonical task codec did not install the requested ordinal",
                    ));
                }
                codec.encode_task(&task, output)
            },
            |output| codec.encode_authority(authority, output),
        )
    }
}

/// Accounted parse-memory policy for one authority or task record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalTaskParseMemory {
    consumer: String,
    class: MemoryClass,
    admission: ExternalTaskParseAdmission,
    amplification_bps: u32,
    fixed_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExternalTaskParseAdmission {
    FailFast,
    Blocking,
}

impl ExternalTaskParseMemory {
    pub fn fail_fast(
        consumer: impl Into<String>,
        class: MemoryClass,
        amplification_bps: u32,
        fixed_bytes: u64,
    ) -> Result<Self> {
        Self::new(
            consumer,
            class,
            ExternalTaskParseAdmission::FailFast,
            amplification_bps,
            fixed_bytes,
        )
    }

    pub fn blocking(
        consumer: impl Into<String>,
        class: MemoryClass,
        amplification_bps: u32,
        fixed_bytes: u64,
    ) -> Result<Self> {
        Self::new(
            consumer,
            class,
            ExternalTaskParseAdmission::Blocking,
            amplification_bps,
            fixed_bytes,
        )
    }

    fn new(
        consumer: impl Into<String>,
        class: MemoryClass,
        admission: ExternalTaskParseAdmission,
        amplification_bps: u32,
        fixed_bytes: u64,
    ) -> Result<Self> {
        let consumer = consumer.into();
        require_token("external task parse-memory consumer", &consumer)?;
        if amplification_bps == 0 {
            return Err(CdfError::contract(
                "external task parse-memory amplification must be nonzero",
            ));
        }
        Ok(Self {
            consumer,
            class,
            admission,
            amplification_bps,
            fixed_bytes,
        })
    }

    pub fn reservation_bytes(&self, encoded_bytes: u64) -> Result<u64> {
        let amplified = u128::from(encoded_bytes)
            .checked_mul(u128::from(self.amplification_bps))
            .and_then(|bytes| bytes.checked_add(9_999))
            .map(|bytes| bytes / 10_000)
            .ok_or_else(|| CdfError::data("external task parse reservation overflowed"))?;
        u64::try_from(
            amplified
                .checked_add(u128::from(self.fixed_bytes))
                .ok_or_else(|| CdfError::data("external task parse reservation overflowed"))?
                .max(1),
        )
        .map_err(|_| CdfError::data("external task parse reservation exceeds u64"))
    }

    fn reserve(
        &self,
        memory: Arc<dyn MemoryCoordinator>,
        encoded_bytes: u64,
    ) -> Result<MemoryLease> {
        let consumer = ConsumerKey::new(&self.consumer, self.class)?;
        let request = ReservationRequest::new(consumer, self.reservation_bytes(encoded_bytes)?)?;
        match self.admission {
            ExternalTaskParseAdmission::FailFast => {
                memory.try_reserve(&request)?.ok_or_else(|| {
                    CdfError::data(format!(
                        "external task parsing requires {} bytes for {}, but the memory ledger cannot admit it",
                        request.bytes, self.consumer
                    ))
                })
            }
            ExternalTaskParseAdmission::Blocking => reserve_blocking(memory, &request),
        }
    }
}

/// Shared authority retained once for every typed task decoded from one task-set reader.
pub struct RetainedExternalTaskAuthority<A> {
    model: A,
    _encoded: Arc<AccountedBytes>,
    _parse: MemoryLease,
}

impl<A> RetainedExternalTaskAuthority<A> {
    pub fn model(&self) -> &A {
        &self.model
    }
}

/// One decoded source task with its exact encoded and parse-memory leases.
pub struct RetainedExternalTask<A, T> {
    inner: Arc<RetainedExternalTaskInner<A, T>>,
}

struct RetainedExternalTaskInner<A, T> {
    task: T,
    authority: Arc<RetainedExternalTaskAuthority<A>>,
    canonical_ordinal: u64,
    content_sha256: String,
    retained_bytes: u64,
    _encoded: AccountedBytes,
    _parse: MemoryLease,
}

impl<A, T> RetainedExternalTask<A, T> {
    pub fn task(&self) -> &T {
        &self.inner.task
    }

    pub fn authority(&self) -> &A {
        self.inner.authority.model()
    }

    pub fn canonical_ordinal(&self) -> u64 {
        self.inner.canonical_ordinal
    }

    pub fn content_sha256(&self) -> &str {
        &self.inner.content_sha256
    }

    pub fn retained_bytes(&self) -> u64 {
        self.inner.retained_bytes
    }
}

impl<A, T> Clone for RetainedExternalTask<A, T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Closed budgets and parse-accounting policy for one typed task-set reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypedExternalTaskSetReaderConfig {
    task_type: String,
    maximum_task_bytes: u64,
    maximum_authority_bytes: u64,
    authority_parse: ExternalTaskParseMemory,
    task_parse: ExternalTaskParseMemory,
}

impl TypedExternalTaskSetReaderConfig {
    pub fn new(
        task_type: impl Into<String>,
        maximum_task_bytes: u64,
        maximum_authority_bytes: u64,
        authority_parse: ExternalTaskParseMemory,
        task_parse: ExternalTaskParseMemory,
    ) -> Result<Self> {
        let task_type = task_type.into();
        require_token("typed external task-set type", &task_type)?;
        if maximum_task_bytes == 0 || maximum_authority_bytes == 0 {
            return Err(CdfError::contract(
                "typed external task-set budgets must be nonzero",
            ));
        }
        Ok(Self {
            task_type,
            maximum_task_bytes,
            maximum_authority_bytes,
            authority_parse,
            task_parse,
        })
    }
}

/// Typed, cancellation-aware view over one canonical external task set.
pub struct TypedExternalTaskSetReader<C>
where
    C: ExternalTaskSetCodec,
{
    reader: ExternalTaskSetReader,
    codec: C,
    authority: Arc<RetainedExternalTaskAuthority<C::Authority>>,
    memory: Arc<dyn MemoryCoordinator>,
    task_parse: ExternalTaskParseMemory,
    cancellation: RunCancellation,
}

impl<C> TypedExternalTaskSetReader<C>
where
    C: ExternalTaskSetCodec,
{
    pub fn open(
        store: &ExternalTaskStore,
        reference: PlannedTaskSetReference,
        memory: Arc<dyn MemoryCoordinator>,
        cancellation: RunCancellation,
        config: TypedExternalTaskSetReaderConfig,
        codec: C,
    ) -> Result<Self> {
        cancellation.check()?;
        let reader = store.reader(
            reference,
            &config.task_type,
            config.maximum_task_bytes,
            config.maximum_authority_bytes,
            Arc::clone(&memory),
        )?;
        cancellation.check()?;
        let encoded = reader.retained_authority();
        let encoded_bytes = u64::try_from(encoded.payload().len())
            .map_err(|_| CdfError::data("external task authority exceeds u64"))?;
        let parse = config
            .authority_parse
            .reserve(Arc::clone(&memory), encoded_bytes)?;
        let authority = codec.decode_authority(encoded.payload())?;
        cancellation.check()?;
        if codec.authority_content_sha256(&authority)? != reader.authority_sha256() {
            return Err(CdfError::data(
                "decoded task-set authority does not match its task-store identity",
            ));
        }
        Ok(Self {
            reader,
            codec,
            authority: Arc::new(RetainedExternalTaskAuthority {
                model: authority,
                _encoded: encoded,
                _parse: parse,
            }),
            memory,
            task_parse: config.task_parse,
            cancellation,
        })
    }

    pub fn authority(&self) -> &C::Authority {
        self.authority.model()
    }

    pub fn next_task(
        &mut self,
        expected_ordinal: u64,
    ) -> Result<Option<RetainedExternalTask<C::Authority, C::Task>>> {
        self.cancellation.check()?;
        let Some(record) = self.reader.next_record()? else {
            return Ok(None);
        };
        if record.canonical_ordinal != expected_ordinal {
            return Err(CdfError::data(format!(
                "external task reader returned ordinal {} while execution requested {expected_ordinal}",
                record.canonical_ordinal
            )));
        }
        let encoded_bytes = u64::try_from(record.payload.payload().len())
            .map_err(|_| CdfError::data("external task payload exceeds u64"))?;
        let parse = self
            .task_parse
            .reserve(Arc::clone(&self.memory), encoded_bytes)?;
        let task = self
            .codec
            .decode_task(record.payload.payload(), self.authority.model())?;
        let task_ordinal = self.codec.task_canonical_ordinal(&task);
        let task_content_sha256 = encoded_task_content_sha256(&self.codec, &task)?;
        if task_ordinal != record.canonical_ordinal || task_content_sha256 != record.content_sha256
        {
            return Err(CdfError::data(
                "decoded external task ordinal or content does not match its task-store record",
            ));
        }
        self.cancellation.check()?;
        let retained_bytes = encoded_bytes
            .checked_add(parse.bytes())
            .ok_or_else(|| CdfError::data("retained external task bytes overflowed u64"))?;
        Ok(Some(RetainedExternalTask {
            inner: Arc::new(RetainedExternalTaskInner {
                task,
                authority: Arc::clone(&self.authority),
                canonical_ordinal: record.canonical_ordinal,
                content_sha256: record.content_sha256,
                retained_bytes,
                _encoded: record.payload,
                _parse: parse,
            }),
        }))
    }

    pub fn observed_task_count(&self) -> u64 {
        self.reader.observed_task_count()
    }
}
