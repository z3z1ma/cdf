use std::{
    collections::BTreeMap,
    fmt,
    io::{BufRead, Read},
    pin::Pin,
    sync::Arc,
};

use arrow_schema::{Schema, SchemaRef};
use cdf_kernel::{
    Batch, BoxFuture, CdfError, PartitionId, PayloadRetention, PushdownFidelity, ResourceId,
    Result, ScanPredicate, SourcePosition,
};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease,
    record_batch_retained_bytes,
};
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::RunCancellation;

pub type AccountedByteStream = Pin<Box<dyn Stream<Item = Result<AccountedBytes>> + Send + 'static>>;
pub type PhysicalDecodeStream =
    Pin<Box<dyn Stream<Item = Result<AccountedPhysicalBatch>> + Send + 'static>>;

pub const DEFAULT_FORMAT_BATCH_ROWS: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOptions {
    pub resource_id: ResourceId,
    pub partition_id: PartitionId,
    pub batch_id_prefix: String,
    pub batch_size: usize,
}

impl ReadOptions {
    pub fn new(resource_id: ResourceId, partition_id: PartitionId) -> Self {
        let batch_id_prefix = format!(
            "{}-{}",
            sanitize_id_part(resource_id.as_str()),
            sanitize_id_part(partition_id.as_str())
        );
        Self {
            resource_id,
            partition_id,
            batch_id_prefix,
            batch_size: DEFAULT_FORMAT_BATCH_ROWS,
        }
    }

    pub fn with_batch_id_prefix(mut self, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        if prefix.trim().is_empty() {
            return Err(CdfError::contract("batch id prefix cannot be empty"));
        }
        self.batch_id_prefix = prefix;
        Ok(self)
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Result<Self> {
        if batch_size == 0 {
            return Err(CdfError::contract("batch size must be greater than zero"));
        }
        self.batch_size = batch_size;
        Ok(self)
    }
}

fn sanitize_id_part(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

pub struct AccountedChunksReader {
    chunks: Vec<AccountedBytes>,
    chunk: usize,
    offset: usize,
}

impl AccountedChunksReader {
    pub fn new(chunks: Vec<AccountedBytes>) -> Self {
        Self {
            chunks,
            chunk: 0,
            offset: 0,
        }
    }

    pub fn retained_bytes(&self) -> u64 {
        self.chunks.iter().map(|chunk| chunk.lease().bytes()).sum()
    }
}

impl Read for AccountedChunksReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let input = self.fill_buf()?;
        let copied = input.len().min(output.len());
        output[..copied].copy_from_slice(&input[..copied]);
        self.consume(copied);
        Ok(copied)
    }
}

impl BufRead for AccountedChunksReader {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        while self.chunk < self.chunks.len()
            && self.offset == self.chunks[self.chunk].payload().len()
        {
            self.chunk += 1;
            self.offset = 0;
        }
        Ok(self
            .chunks
            .get(self.chunk)
            .map(|chunk| &chunk.payload()[self.offset..])
            .unwrap_or_default())
    }

    fn consume(&mut self, amount: usize) {
        let available = self
            .chunks
            .get(self.chunk)
            .map(|chunk| chunk.payload().len().saturating_sub(self.offset))
            .unwrap_or(0);
        self.offset += amount.min(available);
    }
}

pub struct AccountedByteCursor {
    stream: AccountedByteStream,
    current: Option<AccountedBytes>,
    offset: usize,
    consumed_bytes: u64,
}

impl AccountedByteCursor {
    pub fn new(stream: AccountedByteStream) -> Self {
        Self {
            stream,
            current: None,
            offset: 0,
            consumed_bytes: 0,
        }
    }

    pub async fn ensure_current(&mut self) -> Result<bool> {
        while self
            .current
            .as_ref()
            .is_none_or(|chunk| self.offset == chunk.payload().len())
        {
            self.current = None;
            self.current = self.stream.next().await.transpose()?;
            self.offset = 0;
            if self.current.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn current_slice(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(|chunk| &chunk.payload()[self.offset..])
            .unwrap_or_default()
    }

    pub fn consume(&mut self, bytes: usize) -> Result<()> {
        let available = self.current_slice().len();
        if bytes > available {
            return Err(CdfError::internal(
                "accounted byte cursor consumed beyond its current chunk",
            ));
        }
        self.offset += bytes;
        self.consumed_bytes = self
            .consumed_bytes
            .checked_add(
                u64::try_from(bytes)
                    .map_err(|_| CdfError::data("byte cursor count exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::data("accounted byte cursor count overflowed"))?;
        Ok(())
    }

    pub async fn next_byte(&mut self) -> Result<Option<u8>> {
        if !self.ensure_current().await? {
            return Ok(None);
        }
        let byte = self.current_slice()[0];
        self.consume(1)?;
        Ok(Some(byte))
    }

    pub async fn read_exact(&mut self, length: usize, label: &str) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(length);
        while bytes.len() < length {
            if !self.ensure_current().await? {
                return Err(CdfError::data(format!(
                    "{label} ended before its declared length"
                )));
            }
            let remaining = length - bytes.len();
            let available = self.current_slice();
            let copied = remaining.min(available.len());
            bytes.extend_from_slice(&available[..copied]);
            self.consume(copied)?;
        }
        Ok(bytes)
    }

    pub async fn skip_exact(&mut self, mut length: usize, label: &str) -> Result<()> {
        while length > 0 {
            if !self.ensure_current().await? {
                return Err(CdfError::data(format!(
                    "{label} ended before its declared length"
                )));
            }
            let skipped = length.min(self.current_slice().len());
            self.consume(skipped)?;
            length -= skipped;
        }
        Ok(())
    }

    pub fn consumed_bytes(&self) -> u64 {
        self.consumed_bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FormatId(String);

impl FormatId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_registry_id("format", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FormatId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ByteTransformId(String);

impl ByteTransformId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_registry_id("byte transform", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_registry_id(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
    {
        return Err(CdfError::contract(format!(
            "{label} id must contain 1..=64 lowercase ASCII letters, digits, `-`, `_`, or `.`"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentIdentity {
    pub stable_id: String,
    pub size_bytes: Option<u64>,
    pub generation: Option<String>,
    pub checksum: Option<String>,
    pub strength: GenerationStrength,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationStrength {
    Weak,
    Strong,
    ContentAddressed,
}

impl ContentIdentity {
    pub fn validate(&self) -> Result<()> {
        if self.stable_id.is_empty() || self.stable_id.chars().any(char::is_control) {
            return Err(CdfError::contract(
                "byte-source content identity requires a non-control stable id",
            ));
        }
        if self
            .generation
            .as_ref()
            .is_some_and(|value| value.is_empty())
            || self.checksum.as_ref().is_some_and(|value| value.is_empty())
        {
            return Err(CdfError::contract(
                "byte-source generation and checksum authorities cannot be empty",
            ));
        }
        if self.generation.is_none() && self.checksum.is_none() {
            return Err(CdfError::contract(
                "byte-source content identity requires generation or checksum authority",
            ));
        }
        if self.strength == GenerationStrength::ContentAddressed && self.checksum.is_none() {
            return Err(CdfError::contract(
                "content-addressed byte identity requires a checksum",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteExtent {
    pub start: u64,
    pub length: u64,
}

impl ByteExtent {
    pub fn new(start: u64, length: u64) -> Result<Self> {
        if length == 0 || start.checked_add(length).is_none() {
            return Err(CdfError::contract(
                "byte extent requires nonzero length without u64 overflow",
            ));
        }
        Ok(Self { start, length })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteSourceCapabilities {
    pub known_length: bool,
    pub reopenable: bool,
    pub seekable: bool,
    pub exact_ranges: bool,
    pub useful_range_concurrency: u16,
    pub minimum_chunk_bytes: u64,
    pub maximum_chunk_bytes: u64,
}

impl ByteSourceCapabilities {
    pub fn validate(&self) -> Result<()> {
        if self.minimum_chunk_bytes == 0
            || self.maximum_chunk_bytes < self.minimum_chunk_bytes
            || (self.exact_ranges && self.useful_range_concurrency == 0)
            || (!self.exact_ranges && self.useful_range_concurrency != 0)
        {
            return Err(CdfError::contract(
                "byte-source capabilities require valid chunk bounds and range concurrency",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct SequentialReadRequest {
    pub preferred_chunk_bytes: u64,
    pub cancellation: RunCancellation,
}

pub trait ByteSource: Send + Sync {
    fn identity(&self) -> &ContentIdentity;
    fn capabilities(&self) -> &ByteSourceCapabilities;
    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> BoxFuture<'_, Result<AccountedByteStream>>;
    fn read_exact_range(
        &self,
        extent: ByteExtent,
        cancellation: RunCancellation,
    ) -> BoxFuture<'_, Result<AccountedBytes>>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MagicSignature {
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub strong: bool,
}

impl MagicSignature {
    fn validate(&self) -> Result<()> {
        if self.bytes.is_empty() || self.bytes.len() > 256 {
            return Err(CdfError::contract(
                "format magic signature requires 1..=256 bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormatErrorIsolation {
    DecodeUnit,
    Record,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatDriverDescriptor {
    pub format_id: FormatId,
    pub semantic_version: String,
    pub aliases: Vec<String>,
    pub extensions: Vec<String>,
    pub mime_types: Vec<String>,
    pub magic: Vec<MagicSignature>,
    pub detection_probe: FormatDetectionProbe,
    pub option_schema: serde_json::Value,
    pub projection_pushdown: PushdownFidelity,
    pub predicate_pushdown: PushdownFidelity,
    pub source_access: FormatSourceAccess,
    pub decode_unit_policy: String,
    pub error_isolation: FormatErrorIsolation,
    pub minimum_working_set_bytes: u64,
    pub maximum_working_set_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatDetectionProbe {
    pub prefix_bytes: u32,
    pub suffix_bytes: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledFormatBinding {
    pub descriptor: FormatDriverDescriptor,
    pub canonical_options: serde_json::Value,
}

impl CompiledFormatBinding {
    pub fn compile(
        registry: &FormatRegistry,
        id_or_alias: &str,
        options: serde_json::Value,
    ) -> Result<Self> {
        let driver = registry.resolve(id_or_alias)?;
        Ok(Self {
            descriptor: driver.descriptor().clone(),
            canonical_options: driver.canonical_options(options)?,
        })
    }

    pub fn verify(&self, registry: &FormatRegistry) -> Result<Arc<dyn FormatDriver>> {
        let driver = registry.resolve(self.descriptor.format_id.as_str())?;
        if driver.descriptor() != &self.descriptor
            || driver.canonical_options(self.canonical_options.clone())? != self.canonical_options
        {
            return Err(CdfError::contract(format!(
                "compiled format binding for `{}` does not match the registered driver version, capabilities, or canonical options",
                self.descriptor.format_id
            )));
        }
        Ok(driver)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormatSourceAccess {
    Sequential,
    Seekable,
    Adaptive,
}

impl FormatDriverDescriptor {
    pub fn validate(&self) -> Result<()> {
        if self.semantic_version.trim().is_empty() {
            return Err(CdfError::contract(
                "format driver semantic version cannot be empty",
            ));
        }
        if self.decode_unit_policy.trim().is_empty()
            || self.minimum_working_set_bytes == 0
            || self.maximum_working_set_bytes < self.minimum_working_set_bytes
        {
            return Err(CdfError::contract(
                "format driver requires a decode-unit policy and valid working-set bounds",
            ));
        }
        const MAX_DETECTION_PROBE_BYTES: u32 = 1024 * 1024;
        if self.detection_probe.prefix_bytes > MAX_DETECTION_PROBE_BYTES
            || self.detection_probe.suffix_bytes > MAX_DETECTION_PROBE_BYTES
        {
            return Err(CdfError::contract(
                "format detection prefix/suffix probes must each be at most 1 MiB",
            ));
        }
        for alias in &self.aliases {
            validate_registry_id("format alias", alias)?;
        }
        let mut names = std::collections::BTreeSet::from([self.format_id.as_str()]);
        for alias in &self.aliases {
            if !names.insert(alias) {
                return Err(CdfError::contract(format!(
                    "format driver {} repeats id or alias {alias}",
                    self.format_id
                )));
            }
        }
        let mut extensions = std::collections::BTreeSet::new();
        for extension in &self.extensions {
            validate_extension("format", extension)?;
            if !extensions.insert(extension) {
                return Err(CdfError::contract(format!(
                    "format driver {} repeats extension {extension}",
                    self.format_id
                )));
            }
        }
        for signature in &self.magic {
            signature.validate()?;
            let signature_end = signature
                .offset
                .checked_add(signature.bytes.len() as u64)
                .ok_or_else(|| CdfError::contract("format magic extent overflowed"))?;
            if signature_end > u64::from(self.detection_probe.prefix_bytes) {
                return Err(CdfError::contract(format!(
                    "format driver {} magic at offset {} requires {} prefix bytes but its detection probe declares {}",
                    self.format_id,
                    signature.offset,
                    signature_end,
                    self.detection_probe.prefix_bytes
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatProbe {
    pub extension: Option<String>,
    pub mime_type: Option<String>,
    pub prefix: Vec<u8>,
    pub suffix: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatDetection {
    pub confidence: FormatDetectionConfidence,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FormatDetectionConfidence {
    None,
    Weak,
    Strong,
}

#[derive(Clone)]
pub struct FormatDiscoveryRequest {
    pub options: serde_json::Value,
    pub maximum_bytes: u64,
    pub maximum_records: u64,
    pub memory: Arc<dyn MemoryCoordinator>,
    pub cancellation: RunCancellation,
}

#[derive(Clone, Debug)]
pub struct PhysicalSchemaObservation {
    pub identity: ContentIdentity,
    pub arrow_schema: SchemaRef,
    pub sampled_bytes: u64,
    pub sampled_records: u64,
    pub evidence: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecodeUnitPlan {
    pub unit_id: String,
    pub ordinal: u32,
    pub extent: Option<ByteExtent>,
    pub estimated_working_set_bytes: u64,
    pub independently_retryable: bool,
}

impl DecodeUnitPlan {
    pub fn validate(&self) -> Result<()> {
        if self.unit_id.is_empty() || self.estimated_working_set_bytes == 0 {
            return Err(CdfError::contract(
                "decode unit requires an id and nonzero working-set estimate",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct DecodePlanningRequest {
    pub options: serde_json::Value,
    pub projection: Option<Vec<String>>,
    pub predicates: Vec<ScanPredicate>,
    pub target_batch_rows: usize,
    pub target_batch_bytes: u64,
    pub cancellation: RunCancellation,
}

#[derive(Clone)]
pub struct PhysicalDecodeRequest {
    pub options: serde_json::Value,
    pub unit: DecodeUnitPlan,
    pub resource_id: ResourceId,
    pub partition_id: PartitionId,
    pub batch_id_prefix: String,
    pub schema: DecodeSchemaPlan,
    pub source_position: Option<SourcePosition>,
    pub projection: Option<Vec<String>>,
    pub predicates: Vec<ScanPredicate>,
    pub target_batch_rows: usize,
    pub target_batch_bytes: u64,
    pub memory: Arc<dyn MemoryCoordinator>,
    pub cancellation: RunCancellation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeSchemaAuthority {
    VerifiedPhysicalObservation,
    FixedAdmission,
}

#[derive(Clone, Debug)]
pub struct DecodeSchemaPlan {
    pub authority_schema: SchemaRef,
    pub decoder_schema: SchemaRef,
    pub authority: DecodeSchemaAuthority,
}

impl DecodeSchemaPlan {
    pub fn verified_physical(schema: SchemaRef) -> Self {
        Self {
            authority_schema: Arc::clone(&schema),
            decoder_schema: schema,
            authority: DecodeSchemaAuthority::VerifiedPhysicalObservation,
        }
    }

    pub fn fixed_admission(schema: SchemaRef) -> Self {
        let decoder_schema = Arc::new(Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|field| {
                    let source =
                        cdf_kernel::source_name(field.as_ref()).unwrap_or_else(|| field.name());
                    Arc::new(field.as_ref().clone().with_name(source))
                })
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        ));
        Self {
            authority_schema: schema,
            decoder_schema,
            authority: DecodeSchemaAuthority::FixedAdmission,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AccountedPhysicalBatch {
    batch: Batch,
    lease: MemoryLease,
}

impl AccountedPhysicalBatch {
    pub fn new(batch: Batch, lease: MemoryLease) -> Result<Self> {
        let record_batch = batch.record_batch().ok_or_else(|| {
            CdfError::data("format drivers must emit in-memory physical Arrow batches")
        })?;
        let bytes = record_batch_retained_bytes(record_batch)?
            .checked_add(batch.header.pre_contract_evidence_retained_bytes()?)
            .ok_or_else(|| CdfError::data("physical Arrow outcome memory overflow"))?;
        if lease.bytes() < bytes {
            return Err(CdfError::data(format!(
                "physical Arrow outcome and evidence require {bytes} bytes but their lease owns {}",
                lease.bytes()
            )));
        }
        lease.reconcile(bytes)?;
        Ok(Self { batch, lease })
    }

    pub fn batch(&self) -> &Batch {
        &self.batch
    }

    pub fn lease(&self) -> &MemoryLease {
        &self.lease
    }

    pub fn into_batch(self) -> Result<Batch> {
        let bytes = self.lease.bytes();
        self.batch
            .with_retention(PayloadRetention::new(Arc::new(self.lease), bytes)?)
    }
}

pub trait FormatDriver: Send + Sync {
    fn descriptor(&self) -> &FormatDriverDescriptor;
    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value>;
    fn detect(&self, probe: &FormatProbe) -> Result<FormatDetection>;
    fn discover(
        &self,
        source: Arc<dyn ByteSource>,
        request: FormatDiscoveryRequest,
    ) -> BoxFuture<'_, Result<PhysicalSchemaObservation>>;
    fn plan_decode_units(
        &self,
        source: Arc<dyn ByteSource>,
        request: DecodePlanningRequest,
    ) -> BoxFuture<'_, Result<Vec<DecodeUnitPlan>>>;
    fn decode(
        &self,
        source: Arc<dyn ByteSource>,
        request: PhysicalDecodeRequest,
    ) -> BoxFuture<'_, Result<PhysicalDecodeStream>>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteTransformDescriptor {
    pub transform_id: ByteTransformId,
    pub semantic_version: String,
    pub extensions: Vec<String>,
    pub magic: Vec<MagicSignature>,
    pub preserves_random_access: bool,
    pub splittable: bool,
    pub supports_concatenated_members: bool,
    pub maximum_output_chunk_bytes: u64,
    pub maximum_working_set_bytes: u64,
    pub maximum_expanded_bytes: u64,
    pub maximum_expansion_ratio: u32,
    pub checksum: TransformChecksumBehavior,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransformChecksumBehavior {
    None,
    Optional,
    Required,
}

impl ByteTransformDescriptor {
    pub fn validate(&self) -> Result<()> {
        if self.semantic_version.trim().is_empty()
            || self.maximum_working_set_bytes == 0
            || self.maximum_output_chunk_bytes == 0
            || self.maximum_output_chunk_bytes > self.maximum_working_set_bytes
            || self.maximum_expanded_bytes == 0
            || self.maximum_expansion_ratio == 0
            || (self.preserves_random_access && !self.splittable)
        {
            return Err(CdfError::contract(
                "byte-transform descriptor requires version, bounded working/expanded sizes, expansion ratio, and coherent random-access claims",
            ));
        }
        let mut extensions = std::collections::BTreeSet::new();
        for extension in &self.extensions {
            validate_extension("byte-transform", extension)?;
            if !extensions.insert(extension) {
                return Err(CdfError::contract(format!(
                    "byte-transform {} repeats extension {extension}",
                    self.transform_id.as_str()
                )));
            }
        }
        for signature in &self.magic {
            signature.validate()?;
        }
        Ok(())
    }
}

fn validate_extension(label: &str, extension: &str) -> Result<()> {
    if extension.is_empty()
        || extension.starts_with('.')
        || !extension.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
    {
        return Err(CdfError::contract(format!(
            "{label} extension {extension:?} must be lowercase and omit the leading dot"
        )));
    }
    Ok(())
}

pub trait ByteTransformDriver: Send + Sync {
    fn descriptor(&self) -> &ByteTransformDescriptor;
    fn transform(
        &self,
        input: AccountedByteStream,
        request: ByteTransformRequest,
    ) -> Result<AccountedByteStream>;
}

#[derive(Clone)]
pub struct ByteTransformRequest {
    pub preferred_output_chunk_bytes: u64,
    pub maximum_expanded_bytes: u64,
    pub maximum_expansion_ratio: u32,
    pub input_size_bytes: Option<u64>,
    pub memory: Arc<dyn MemoryCoordinator>,
    pub consumer: ConsumerKey,
    pub cancellation: RunCancellation,
}

impl ByteTransformRequest {
    pub fn validate_for(&self, descriptor: &ByteTransformDescriptor) -> Result<()> {
        descriptor.validate()?;
        if self.preferred_output_chunk_bytes == 0
            || self.preferred_output_chunk_bytes > descriptor.maximum_working_set_bytes
            || self.preferred_output_chunk_bytes > descriptor.maximum_output_chunk_bytes
            || self.maximum_expanded_bytes == 0
            || self.maximum_expanded_bytes > descriptor.maximum_expanded_bytes
            || self.maximum_expansion_ratio == 0
            || self.maximum_expansion_ratio > descriptor.maximum_expansion_ratio
            || self.input_size_bytes == Some(0)
            || self.consumer.class != MemoryClass::Transform
        {
            return Err(CdfError::contract(
                "byte-transform request requires a transform-class consumer, a nonzero output chunk within working-set authority, optional positive input length, and expansion ceilings no greater than the driver descriptor",
            ));
        }
        if let Some(input_bytes) = self.input_size_bytes {
            input_bytes
                .checked_mul(u64::from(self.maximum_expansion_ratio))
                .ok_or_else(|| {
                    CdfError::contract("byte-transform expansion authority overflowed")
                })?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct TransformExpansionGuard {
    maximum_expanded_bytes: u64,
    maximum_expansion_ratio: u32,
    streaming_grace_bytes: u64,
    expanded_bytes: u64,
}

impl TransformExpansionGuard {
    pub fn new(request: &ByteTransformRequest) -> Result<Self> {
        if request.maximum_expanded_bytes == 0
            || request.maximum_expansion_ratio == 0
            || request.preferred_output_chunk_bytes == 0
        {
            return Err(CdfError::contract(
                "transform expansion guard requires nonzero byte, ratio, and chunk bounds",
            ));
        }
        Ok(Self {
            maximum_expanded_bytes: request.maximum_expanded_bytes,
            maximum_expansion_ratio: request.maximum_expansion_ratio,
            streaming_grace_bytes: request.preferred_output_chunk_bytes,
            expanded_bytes: 0,
        })
    }

    pub fn record(
        &mut self,
        produced_bytes: usize,
        compressed_consumed_bytes: u64,
        exact_ratio_boundary: bool,
    ) -> Result<()> {
        self.expanded_bytes = self
            .expanded_bytes
            .checked_add(
                u64::try_from(produced_bytes)
                    .map_err(|_| CdfError::data("expanded-byte count exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::data("transform expanded-byte count overflowed"))?;
        if self.expanded_bytes > self.maximum_expanded_bytes {
            return Err(CdfError::data(format!(
                "transform expansion produced {} bytes, exceeding the configured {}-byte ceiling",
                self.expanded_bytes, self.maximum_expanded_bytes
            )));
        }
        let ratio_ceiling = compressed_consumed_bytes
            .checked_mul(u64::from(self.maximum_expansion_ratio))
            .ok_or_else(|| CdfError::data("transform expansion-ratio calculation overflowed"))?;
        let grace = if exact_ratio_boundary {
            0
        } else {
            self.streaming_grace_bytes
        };
        if self.expanded_bytes > ratio_ceiling.saturating_add(grace) {
            return Err(CdfError::data(format!(
                "transform expansion ratio exceeds the configured {}:1 ceiling after {compressed_consumed_bytes} compressed bytes",
                self.maximum_expansion_ratio
            )));
        }
        Ok(())
    }

    pub fn enforce_exact_ratio(&self, compressed_consumed_bytes: u64) -> Result<()> {
        let ratio_ceiling = compressed_consumed_bytes
            .checked_mul(u64::from(self.maximum_expansion_ratio))
            .ok_or_else(|| CdfError::data("transform expansion-ratio calculation overflowed"))?;
        if self.expanded_bytes > ratio_ceiling {
            return Err(CdfError::data(format!(
                "transform expansion ratio exceeds the configured {}:1 ceiling after {compressed_consumed_bytes} compressed bytes",
                self.maximum_expansion_ratio
            )));
        }
        Ok(())
    }

    pub fn expanded_bytes(&self) -> u64 {
        self.expanded_bytes
    }
}

#[derive(Clone, Default)]
pub struct FormatRegistry {
    by_id: BTreeMap<FormatId, Arc<dyn FormatDriver>>,
    aliases: BTreeMap<String, FormatId>,
    extensions: BTreeMap<String, FormatId>,
    strong_magic: BTreeMap<(u64, Vec<u8>), FormatId>,
}

impl FormatRegistry {
    pub fn register(&mut self, driver: Arc<dyn FormatDriver>) -> Result<()> {
        let descriptor = driver.descriptor();
        descriptor.validate()?;
        if self.by_id.contains_key(&descriptor.format_id)
            || self.aliases.contains_key(descriptor.format_id.as_str())
        {
            return Err(CdfError::contract(format!(
                "duplicate format id {}",
                descriptor.format_id
            )));
        }
        for alias in &descriptor.aliases {
            if self.aliases.contains_key(alias) || self.by_id.keys().any(|id| id.as_str() == alias)
            {
                return Err(CdfError::contract(format!(
                    "duplicate format alias {alias}"
                )));
            }
        }
        for extension in &descriptor.extensions {
            if let Some(existing) = self.extensions.get(extension) {
                return Err(CdfError::contract(format!(
                    "format extension {extension} conflicts between {existing} and {}",
                    descriptor.format_id
                )));
            }
        }
        for signature in descriptor.magic.iter().filter(|signature| signature.strong) {
            let key = (signature.offset, signature.bytes.clone());
            if let Some(existing) = self.strong_magic.get(&key) {
                return Err(CdfError::contract(format!(
                    "strong format magic conflicts between {existing} and {}",
                    descriptor.format_id
                )));
            }
        }
        let id = descriptor.format_id.clone();
        for alias in &descriptor.aliases {
            self.aliases.insert(alias.clone(), id.clone());
        }
        for extension in &descriptor.extensions {
            self.extensions.insert(extension.clone(), id.clone());
        }
        for signature in descriptor.magic.iter().filter(|signature| signature.strong) {
            self.strong_magic
                .insert((signature.offset, signature.bytes.clone()), id.clone());
        }
        self.by_id.insert(id, driver);
        Ok(())
    }

    pub fn resolve(&self, id_or_alias: &str) -> Result<Arc<dyn FormatDriver>> {
        let id = self
            .aliases
            .get(id_or_alias)
            .cloned()
            .unwrap_or(FormatId::new(id_or_alias)?);
        self.by_id.get(&id).cloned().ok_or_else(|| {
            CdfError::contract(format!("format driver `{id_or_alias}` is not registered"))
        })
    }

    pub fn get(&self, id_or_alias: &str) -> Option<Arc<dyn FormatDriver>> {
        let id = self
            .aliases
            .get(id_or_alias)
            .cloned()
            .or_else(|| FormatId::new(id_or_alias).ok())?;
        self.by_id.get(&id).cloned()
    }

    pub fn by_extension(&self, extension: &str) -> Option<Arc<dyn FormatDriver>> {
        let id = self.extensions.get(extension)?;
        self.by_id.get(id).cloned()
    }

    pub fn detect_strong_magic(&self, prefix: &[u8]) -> Result<Option<Arc<dyn FormatDriver>>> {
        let mut matched: Option<&FormatId> = None;
        for ((offset, signature), id) in &self.strong_magic {
            let start = usize::try_from(*offset)
                .map_err(|_| CdfError::contract("format magic offset exceeds usize"))?;
            let Some(end) = start.checked_add(signature.len()) else {
                return Err(CdfError::contract("format magic extent exceeds usize"));
            };
            if prefix.get(start..end) != Some(signature.as_slice()) {
                continue;
            }
            if matched.is_some_and(|existing| existing != id) {
                return Err(CdfError::contract(
                    "format magic is ambiguous across registered drivers",
                ));
            }
            matched = Some(id);
        }
        Ok(matched.and_then(|id| self.by_id.get(id).cloned()))
    }

    pub fn descriptors(&self) -> Vec<FormatDriverDescriptor> {
        self.by_id
            .values()
            .map(|driver| driver.descriptor().clone())
            .collect()
    }
}

#[derive(Clone, Default)]
pub struct ByteTransformRegistry {
    by_id: BTreeMap<ByteTransformId, Arc<dyn ByteTransformDriver>>,
    extensions: BTreeMap<String, ByteTransformId>,
    strong_magic: BTreeMap<(u64, Vec<u8>), ByteTransformId>,
}

impl ByteTransformRegistry {
    pub fn register(&mut self, driver: Arc<dyn ByteTransformDriver>) -> Result<()> {
        let descriptor = driver.descriptor();
        descriptor.validate()?;
        let id = descriptor.transform_id.clone();
        if self.by_id.contains_key(&id) {
            return Err(CdfError::contract(format!(
                "duplicate byte-transform id {}",
                id.as_str()
            )));
        }
        for extension in &descriptor.extensions {
            if let Some(existing) = self.extensions.get(extension) {
                return Err(CdfError::contract(format!(
                    "byte-transform extension {extension} conflicts between {} and {}",
                    existing.as_str(),
                    id.as_str()
                )));
            }
        }
        for signature in descriptor.magic.iter().filter(|signature| signature.strong) {
            let key = (signature.offset, signature.bytes.clone());
            if let Some(existing) = self.strong_magic.get(&key) {
                return Err(CdfError::contract(format!(
                    "strong byte-transform magic conflicts between {} and {}",
                    existing.as_str(),
                    id.as_str()
                )));
            }
        }
        for extension in &descriptor.extensions {
            self.extensions.insert(extension.clone(), id.clone());
        }
        for signature in descriptor.magic.iter().filter(|signature| signature.strong) {
            self.strong_magic
                .insert((signature.offset, signature.bytes.clone()), id.clone());
        }
        self.by_id.insert(id, driver);
        Ok(())
    }

    pub fn resolve(&self, id: &ByteTransformId) -> Result<Arc<dyn ByteTransformDriver>> {
        self.by_id.get(id).cloned().ok_or_else(|| {
            CdfError::contract(format!(
                "byte-transform driver `{}` is not registered",
                id.as_str()
            ))
        })
    }

    pub fn resolve_name(&self, id: &str) -> Result<Arc<dyn ByteTransformDriver>> {
        self.resolve(&ByteTransformId::new(id)?)
    }

    pub fn get(&self, id: &ByteTransformId) -> Option<Arc<dyn ByteTransformDriver>> {
        self.by_id.get(id).cloned()
    }

    pub fn by_extension(&self, extension: &str) -> Option<Arc<dyn ByteTransformDriver>> {
        let id = self.extensions.get(extension)?;
        self.by_id.get(id).cloned()
    }

    pub fn detect_strong_magic(
        &self,
        prefix: &[u8],
    ) -> Result<Option<Arc<dyn ByteTransformDriver>>> {
        let mut matched: Option<&ByteTransformId> = None;
        for ((offset, signature), id) in &self.strong_magic {
            let start = usize::try_from(*offset)
                .map_err(|_| CdfError::contract("byte-transform magic offset exceeds usize"))?;
            let Some(end) = start.checked_add(signature.len()) else {
                return Err(CdfError::contract(
                    "byte-transform magic extent exceeds usize",
                ));
            };
            if prefix.get(start..end) != Some(signature.as_slice()) {
                continue;
            }
            if matched.is_some_and(|existing| existing != id) {
                return Err(CdfError::contract(
                    "byte-transform magic is ambiguous across registered drivers",
                ));
            }
            matched = Some(id);
        }
        Ok(matched.and_then(|id| self.by_id.get(id).cloned()))
    }

    pub fn descriptors(&self) -> Vec<ByteTransformDescriptor> {
        self.by_id
            .values()
            .map(|driver| driver.descriptor().clone())
            .collect()
    }

    pub fn maximum_strong_magic_probe_bytes(&self) -> Result<u64> {
        self.strong_magic
            .keys()
            .try_fold(0_u64, |maximum, (offset, signature)| {
                let length = u64::try_from(signature.len())
                    .map_err(|_| CdfError::contract("byte-transform magic length exceeds u64"))?;
                let end = offset
                    .checked_add(length)
                    .ok_or_else(|| CdfError::contract("byte-transform magic extent exceeds u64"))?;
                Ok(maximum.max(end))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{BatchId, PartitionId, ResourceId, SchemaHash, with_source_name};
    use cdf_memory::{
        ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator,
        ReservationRequest,
    };

    struct DescriptorOnlyDriver(FormatDriverDescriptor);

    struct DescriptorOnlyTransform(ByteTransformDescriptor);

    #[test]
    fn read_options_derive_stable_batch_identity_and_reject_invalid_overrides() {
        let options = ReadOptions::new(
            ResourceId::new("events/raw").unwrap(),
            PartitionId::new("2026-07-13.json").unwrap(),
        );

        assert_eq!(options.batch_id_prefix, "events-raw-2026-07-13-json");
        assert_eq!(options.batch_size, DEFAULT_FORMAT_BATCH_ROWS);
        assert!(options.clone().with_batch_id_prefix(" ").is_err());
        assert!(options.with_batch_size(0).is_err());
    }

    impl ByteTransformDriver for DescriptorOnlyTransform {
        fn descriptor(&self) -> &ByteTransformDescriptor {
            &self.0
        }

        fn transform(
            &self,
            _input: AccountedByteStream,
            _request: ByteTransformRequest,
        ) -> Result<AccountedByteStream> {
            Err(CdfError::internal("unused test method"))
        }
    }

    impl FormatDriver for DescriptorOnlyDriver {
        fn descriptor(&self) -> &FormatDriverDescriptor {
            &self.0
        }

        fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
            Ok(options)
        }

        fn detect(&self, _probe: &FormatProbe) -> Result<FormatDetection> {
            Ok(FormatDetection {
                confidence: FormatDetectionConfidence::None,
                reason: "test".to_owned(),
            })
        }

        fn discover(
            &self,
            _source: Arc<dyn ByteSource>,
            _request: FormatDiscoveryRequest,
        ) -> BoxFuture<'_, Result<PhysicalSchemaObservation>> {
            Box::pin(async { Err(CdfError::internal("unused test method")) })
        }

        fn plan_decode_units(
            &self,
            _source: Arc<dyn ByteSource>,
            _request: DecodePlanningRequest,
        ) -> BoxFuture<'_, Result<Vec<DecodeUnitPlan>>> {
            Box::pin(async { Err(CdfError::internal("unused test method")) })
        }

        fn decode(
            &self,
            _source: Arc<dyn ByteSource>,
            _request: PhysicalDecodeRequest,
        ) -> BoxFuture<'_, Result<PhysicalDecodeStream>> {
            Box::pin(async { Err(CdfError::internal("unused test method")) })
        }
    }

    fn driver(id: &str, aliases: &[&str], magic: &[u8]) -> Arc<dyn FormatDriver> {
        Arc::new(DescriptorOnlyDriver(FormatDriverDescriptor {
            format_id: FormatId::new(id).unwrap(),
            semantic_version: "1.0.0".to_owned(),
            aliases: aliases.iter().map(|value| (*value).to_owned()).collect(),
            extensions: vec![id.to_owned()],
            mime_types: Vec::new(),
            magic: vec![MagicSignature {
                offset: 0,
                bytes: magic.to_vec(),
                strong: true,
            }],
            detection_probe: FormatDetectionProbe {
                prefix_bytes: magic.len() as u32,
                suffix_bytes: 0,
            },
            option_schema: serde_json::json!({"type": "object"}),
            projection_pushdown: PushdownFidelity::Unsupported,
            predicate_pushdown: PushdownFidelity::Unsupported,
            source_access: FormatSourceAccess::Sequential,
            decode_unit_policy: "whole_object".to_owned(),
            error_isolation: FormatErrorIsolation::DecodeUnit,
            minimum_working_set_bytes: 1,
            maximum_working_set_bytes: 1024,
        }))
    }

    #[test]
    fn format_registry_is_deterministic_and_rejects_ambiguous_authority() {
        let mut registry = FormatRegistry::default();
        registry
            .register(driver("mock", &["mock_alias"], b"MOCK"))
            .unwrap();
        assert_eq!(
            registry
                .resolve("mock_alias")
                .unwrap()
                .descriptor()
                .format_id,
            FormatId::new("mock").unwrap()
        );
        assert!(registry.register(driver("mock", &[], b"OTHER")).is_err());
        assert!(registry.register(driver("other", &[], b"MOCK")).is_err());
        assert_eq!(registry.descriptors().len(), 1);
    }

    #[test]
    fn byte_transform_registry_resolves_names_extensions_and_magic() {
        let descriptor = ByteTransformDescriptor {
            transform_id: ByteTransformId::new("mock_transform").unwrap(),
            semantic_version: "1.0.0".to_owned(),
            extensions: vec!["mockz".to_owned()],
            magic: vec![MagicSignature {
                offset: 1,
                bytes: b"MOCK".to_vec(),
                strong: true,
            }],
            preserves_random_access: false,
            splittable: false,
            supports_concatenated_members: false,
            maximum_output_chunk_bytes: 1024,
            maximum_working_set_bytes: 1024,
            maximum_expanded_bytes: 1024 * 1024,
            maximum_expansion_ratio: 10,
            checksum: TransformChecksumBehavior::Required,
        };
        let mut registry = ByteTransformRegistry::default();
        registry
            .register(Arc::new(DescriptorOnlyTransform(descriptor.clone())))
            .unwrap();

        assert_eq!(
            registry
                .resolve_name("mock_transform")
                .unwrap()
                .descriptor(),
            &descriptor
        );
        assert_eq!(
            registry.by_extension("mockz").unwrap().descriptor(),
            &descriptor
        );
        assert!(registry.by_extension("unknown").is_none());
        assert!(
            registry
                .detect_strong_magic(b"xMOCKpayload")
                .unwrap()
                .is_some()
        );
        assert!(registry.detect_strong_magic(b"xMOC").unwrap().is_none());
        assert_eq!(registry.descriptors(), vec![descriptor]);
    }

    #[test]
    fn byte_source_capabilities_fail_incoherent_range_claims() {
        assert!(
            ByteSourceCapabilities {
                known_length: true,
                reopenable: true,
                seekable: false,
                exact_ranges: false,
                useful_range_concurrency: 4,
                minimum_chunk_bytes: 1,
                maximum_chunk_bytes: 1024,
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn byte_transform_request_binds_output_allocation_and_expansion_authority() {
        let descriptor = ByteTransformDescriptor {
            transform_id: ByteTransformId::new("gzip").unwrap(),
            semantic_version: "1.0.0".to_owned(),
            extensions: vec!["gz".to_owned()],
            magic: vec![MagicSignature {
                offset: 0,
                bytes: vec![0x1f, 0x8b],
                strong: true,
            }],
            preserves_random_access: false,
            splittable: false,
            supports_concatenated_members: true,
            maximum_output_chunk_bytes: 1024 * 1024,
            maximum_working_set_bytes: 1024 * 1024,
            maximum_expanded_bytes: 1024 * 1024 * 1024,
            maximum_expansion_ratio: 100,
            checksum: TransformChecksumBehavior::Required,
        };
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            DeterministicMemoryCoordinator::new(8 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let request = ByteTransformRequest {
            preferred_output_chunk_bytes: 256 * 1024,
            maximum_expanded_bytes: 512 * 1024 * 1024,
            maximum_expansion_ratio: 50,
            input_size_bytes: Some(1024),
            memory,
            consumer: ConsumerKey::new("gzip-part-0", MemoryClass::Transform).unwrap(),
            cancellation: RunCancellation::default(),
        };
        request.validate_for(&descriptor).unwrap();

        let mut unaccounted = request.clone();
        unaccounted.consumer = ConsumerKey::new("gzip-part-0", MemoryClass::Decode).unwrap();
        assert!(unaccounted.validate_for(&descriptor).is_err());

        let mut oversized = request;
        oversized.preferred_output_chunk_bytes = descriptor.maximum_working_set_bytes + 1;
        assert!(oversized.validate_for(&descriptor).is_err());
    }

    #[test]
    fn physical_batch_retains_its_memory_lease_after_entering_kernel_stream() {
        let record_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let retained_bytes = record_batch_retained_bytes(&record_batch).unwrap();
        let memory =
            DeterministicMemoryCoordinator::new(retained_bytes * 2, BTreeMap::new()).unwrap();
        let lease = memory
            .try_reserve(
                &ReservationRequest::new(
                    ConsumerKey::new("format-test", MemoryClass::Decode).unwrap(),
                    retained_bytes,
                )
                .unwrap(),
            )
            .unwrap()
            .unwrap();
        let batch = Batch::from_record_batch(
            BatchId::new("batch-0").unwrap(),
            ResourceId::new("events").unwrap(),
            PartitionId::new("part-0").unwrap(),
            SchemaHash::new("schema-v1").unwrap(),
            record_batch,
        )
        .unwrap();
        let accounted = AccountedPhysicalBatch::new(batch, lease).unwrap();
        let batch = accounted.into_batch().unwrap();

        assert_eq!(batch.retained_bytes(), retained_bytes);
        assert_eq!(memory.snapshot().current_bytes, retained_bytes);
        drop(batch);
        assert_eq!(memory.snapshot().current_bytes, 0);
    }

    #[test]
    fn schema_bearing_empty_batch_retains_its_arrow_container_bytes() {
        let record_batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )])));
        let retained_bytes = record_batch_retained_bytes(&record_batch).unwrap();
        assert!(retained_bytes > 0);
        let memory = DeterministicMemoryCoordinator::new(4096, BTreeMap::new()).unwrap();
        let lease = memory
            .try_reserve(
                &ReservationRequest::new(
                    ConsumerKey::new("empty-format-test", MemoryClass::Decode).unwrap(),
                    4096,
                )
                .unwrap(),
            )
            .unwrap()
            .unwrap();
        let batch = Batch::from_record_batch(
            BatchId::new("empty-batch").unwrap(),
            ResourceId::new("events").unwrap(),
            PartitionId::new("part-0").unwrap(),
            SchemaHash::new("schema-empty").unwrap(),
            record_batch,
        )
        .unwrap();

        let batch = AccountedPhysicalBatch::new(batch, lease)
            .unwrap()
            .into_batch()
            .unwrap();

        assert_eq!(batch.retained_bytes(), retained_bytes);
        assert_eq!(memory.snapshot().current_bytes, retained_bytes);
        drop(batch);
        assert_eq!(memory.snapshot().current_bytes, 0);
    }

    #[test]
    fn fixed_admission_decodes_source_names_without_changing_the_pinned_schema() {
        let pinned = Arc::new(Schema::new(vec![with_source_name(
            Field::new("vendor_id", DataType::Int64, false),
            "VendorID",
        )]));
        let plan = DecodeSchemaPlan::fixed_admission(Arc::clone(&pinned));

        assert_eq!(plan.authority, DecodeSchemaAuthority::FixedAdmission);
        assert_eq!(plan.authority_schema.as_ref(), pinned.as_ref());
        assert_eq!(plan.decoder_schema.field(0).name(), "VendorID");
        assert_eq!(
            cdf_kernel::source_name(plan.decoder_schema.field(0)),
            Some("VendorID")
        );
    }
}
