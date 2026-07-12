use std::{collections::BTreeMap, fmt, pin::Pin, sync::Arc};

use arrow_schema::SchemaRef;
use cdf_contract::ObservedSchema;
use cdf_kernel::{Batch, BoxFuture, CdfError, PushdownFidelity, Result, ScanPredicate};
use cdf_memory::{AccountedBytes, MemoryLease, record_batch_retained_bytes};
use futures_util::Stream;
use serde::{Deserialize, Serialize};

use crate::RunCancellation;

pub type AccountedByteStream = Pin<Box<dyn Stream<Item = Result<AccountedBytes>> + Send + 'static>>;
pub type PhysicalDecodeStream =
    Pin<Box<dyn Stream<Item = Result<AccountedPhysicalBatch>> + Send + 'static>>;

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
    fn read_exact_range(&self, extent: ByteExtent) -> BoxFuture<'_, Result<AccountedBytes>>;
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatDriverDescriptor {
    pub format_id: FormatId,
    pub semantic_version: String,
    pub aliases: Vec<String>,
    pub extensions: Vec<String>,
    pub mime_types: Vec<String>,
    pub magic: Vec<MagicSignature>,
    pub option_schema: serde_json::Value,
    pub projection_pushdown: PushdownFidelity,
    pub predicate_pushdown: PushdownFidelity,
    pub decode_unit_policy: String,
    pub minimum_working_set_bytes: u64,
    pub maximum_working_set_bytes: u64,
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

#[derive(Clone, Debug)]
pub struct FormatDiscoveryRequest {
    pub options: serde_json::Value,
    pub maximum_bytes: u64,
    pub maximum_records: u64,
    pub cancellation: RunCancellation,
}

#[derive(Clone, Debug)]
pub struct PhysicalSchemaObservation {
    pub identity: ContentIdentity,
    pub arrow_schema: SchemaRef,
    pub observed_schema: ObservedSchema,
    pub sampled_bytes: u64,
    pub sampled_records: u64,
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

#[derive(Clone, Debug)]
pub struct PhysicalDecodeRequest {
    pub options: serde_json::Value,
    pub unit: DecodeUnitPlan,
    pub projection: Option<Vec<String>>,
    pub predicates: Vec<ScanPredicate>,
    pub target_batch_rows: usize,
    pub target_batch_bytes: u64,
    pub cancellation: RunCancellation,
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
        let bytes = record_batch_retained_bytes(record_batch)?;
        if bytes == 0 || lease.bytes() < bytes {
            return Err(CdfError::data(format!(
                "physical Arrow outcome requires {bytes} bytes but its lease owns {}",
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

    pub fn into_parts(self) -> (Batch, MemoryLease) {
        (self.batch, self.lease)
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
        cancellation: RunCancellation,
    ) -> Result<AccountedByteStream>;
}

#[derive(Clone, Default)]
pub struct FormatRegistry {
    by_id: BTreeMap<FormatId, Arc<dyn FormatDriver>>,
    aliases: BTreeMap<String, FormatId>,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DescriptorOnlyDriver(FormatDriverDescriptor);

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
            option_schema: serde_json::json!({"type": "object"}),
            projection_pushdown: PushdownFidelity::Unsupported,
            predicate_pushdown: PushdownFidelity::Unsupported,
            decode_unit_policy: "whole_object".to_owned(),
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
}
