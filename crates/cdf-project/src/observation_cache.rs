use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};

use cdf_kernel::{CanonicalArrowSchema, CdfError, Result, SchemaHash};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const OBSERVATION_CACHE_ARTIFACT_VERSION: u16 = 1;
pub const DEFAULT_OBSERVATION_CACHE_MAX_ENTRIES: usize = 4_096;
pub const DEFAULT_OBSERVATION_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_OBSERVATION_CACHE_MAX_ENTRY_BYTES: u64 = 8 * 1024 * 1024;

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

enum BoundedCacheRead {
    Complete(Vec<u8>),
    Oversized,
}

fn read_cache_entry_bounded(
    reader: &mut impl Read,
    maximum_bytes: u64,
) -> std::io::Result<BoundedCacheRead> {
    let read_limit = maximum_bytes
        .checked_add(1)
        .ok_or_else(|| std::io::Error::other("observation cache read limit overflowed"))?;
    let initial_capacity = usize::try_from(maximum_bytes.min(64 * 1024)).unwrap_or(64 * 1024);
    let mut bytes = Vec::with_capacity(initial_capacity);
    reader.take(read_limit).read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).map_or(true, |len| len > maximum_bytes) {
        Ok(BoundedCacheRead::Oversized)
    } else {
        Ok(BoundedCacheRead::Complete(bytes))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservationCachePolicy {
    max_entries: usize,
    max_bytes: u64,
    max_entry_bytes: u64,
}

impl ObservationCachePolicy {
    pub fn new(max_entries: usize, max_bytes: u64, max_entry_bytes: u64) -> Result<Self> {
        if max_entries == 0 || max_bytes == 0 || max_entry_bytes == 0 || max_entry_bytes > max_bytes
        {
            return Err(CdfError::contract(
                "observation cache policy requires positive entry/byte limits and max_entry_bytes no greater than max_bytes",
            ));
        }
        Ok(Self {
            max_entries,
            max_bytes,
            max_entry_bytes,
        })
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub fn max_entry_bytes(&self) -> u64 {
        self.max_entry_bytes
    }
}

impl Default for ObservationCachePolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_OBSERVATION_CACHE_MAX_ENTRIES,
            DEFAULT_OBSERVATION_CACHE_MAX_BYTES,
            DEFAULT_OBSERVATION_CACHE_MAX_ENTRY_BYTES,
        )
        .expect("the built-in observation cache policy is valid")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrongObservationSourceIdentity {
    pub canonical_location: String,
    pub size_bytes: u64,
    pub checksum: Option<String>,
    pub generation: BTreeMap<String, String>,
}

impl StrongObservationSourceIdentity {
    pub fn new(
        canonical_location: impl Into<String>,
        size_bytes: u64,
        checksum: Option<String>,
        generation: BTreeMap<String, String>,
    ) -> Result<Self> {
        let identity = Self {
            canonical_location: canonical_location.into(),
            size_bytes,
            checksum,
            generation,
        };
        identity.validate()?;
        Ok(identity)
    }

    fn validate(&self) -> Result<()> {
        if self.canonical_location.trim().is_empty()
            || self.checksum.as_ref().is_some_and(|value| value.is_empty())
            || self
                .generation
                .iter()
                .any(|(key, value)| key.is_empty() || value.is_empty())
            || (self.checksum.is_none() && self.generation.is_empty())
        {
            return Err(CdfError::data(
                "observation cache source identity requires location plus a checksum or non-empty strong generation token",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationCacheKey {
    pub version: u16,
    pub source: StrongObservationSourceIdentity,
    pub format_driver_id: String,
    pub format_driver_version: String,
    pub interpretation_hash: String,
    pub observation_contract_hash: String,
    pub normalizer_version: String,
    pub admission_identity: String,
}

impl ObservationCacheKey {
    pub fn new(
        source: StrongObservationSourceIdentity,
        format_driver_id: impl Into<String>,
        format_driver_version: impl Into<String>,
        interpretation_hash: impl Into<String>,
        observation_contract_hash: impl Into<String>,
        normalizer_version: impl Into<String>,
        admission_identity: impl Into<String>,
    ) -> Result<Self> {
        let key = Self {
            version: OBSERVATION_CACHE_ARTIFACT_VERSION,
            source,
            format_driver_id: format_driver_id.into(),
            format_driver_version: format_driver_version.into(),
            interpretation_hash: interpretation_hash.into(),
            observation_contract_hash: observation_contract_hash.into(),
            normalizer_version: normalizer_version.into(),
            admission_identity: admission_identity.into(),
        };
        key.validate()?;
        Ok(key)
    }

    pub fn identity_hash(&self) -> Result<String> {
        self.validate()?;
        let encoded = serde_json::to_vec(self).map_err(|error| {
            CdfError::internal(format!("encode observation cache key: {error}"))
        })?;
        Ok(format!("sha256:{}", hex::encode(Sha256::digest(encoded))))
    }

    fn validate(&self) -> Result<()> {
        self.source.validate()?;
        if self.version != OBSERVATION_CACHE_ARTIFACT_VERSION
            || [
                self.format_driver_id.as_str(),
                self.format_driver_version.as_str(),
                self.interpretation_hash.as_str(),
                self.observation_contract_hash.as_str(),
                self.normalizer_version.as_str(),
                self.admission_identity.as_str(),
            ]
            .into_iter()
            .any(str::is_empty)
        {
            return Err(CdfError::data(
                "observation cache key requires the current version and complete driver/options/normalizer/admission identity",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationCacheEntry {
    pub version: u16,
    pub key_hash: String,
    pub key: ObservationCacheKey,
    pub schema_hash: SchemaHash,
    pub schema: CanonicalArrowSchema,
    pub source_identity: BTreeMap<String, String>,
    pub observed_bytes: u64,
    pub observed_records: u64,
}

impl ObservationCacheEntry {
    pub fn new(
        key: ObservationCacheKey,
        schema: &arrow_schema::Schema,
        source_identity: BTreeMap<String, String>,
        observed_bytes: u64,
        observed_records: u64,
    ) -> Result<Self> {
        let entry = Self {
            version: OBSERVATION_CACHE_ARTIFACT_VERSION,
            key_hash: key.identity_hash()?,
            key,
            schema_hash: cdf_kernel::canonical_arrow_schema_hash(schema)?,
            schema: CanonicalArrowSchema::from_arrow(schema)?,
            source_identity,
            observed_bytes,
            observed_records,
        };
        entry.validate()?;
        Ok(entry)
    }

    pub fn arrow_schema(&self) -> Result<arrow_schema::Schema> {
        self.schema.to_arrow()
    }

    fn validate(&self) -> Result<()> {
        if self.version != OBSERVATION_CACHE_ARTIFACT_VERSION
            || self.key_hash != self.key.identity_hash()?
            || self.source_identity.is_empty()
        {
            return Err(CdfError::data(
                "observation cache entry has an unsupported version, mismatched key, or missing source evidence",
            ));
        }
        let schema = self.schema.to_arrow()?;
        let schema_hash = cdf_kernel::canonical_arrow_schema_hash(&schema)?;
        if schema_hash != self.schema_hash {
            return Err(CdfError::data(
                "observation cache entry schema does not match its canonical hash",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservationCacheMissReason {
    Absent,
    CorruptOrUnsupported,
    Oversized,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObservationCacheLookup {
    Hit(ObservationCacheEntry),
    Miss(ObservationCacheMissReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservationCacheStoreOutcome {
    Stored,
    AlreadyPresent,
    SkippedOversized,
    Unavailable,
}

#[derive(Clone, Debug)]
pub struct ObservationCacheStore {
    root: PathBuf,
    policy: ObservationCachePolicy,
    gate: Arc<Mutex<()>>,
}

impl ObservationCacheStore {
    pub fn new(project_root: impl AsRef<Path>) -> Self {
        Self::with_policy(project_root, ObservationCachePolicy::default())
    }

    pub fn with_policy(project_root: impl AsRef<Path>, policy: ObservationCachePolicy) -> Self {
        Self {
            root: project_root
                .as_ref()
                .join(".cdf/cache/schema-observations/v1"),
            policy,
            gate: Arc::new(Mutex::new(())),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn policy(&self) -> &ObservationCachePolicy {
        &self.policy
    }

    pub fn lookup(&self, key: &ObservationCacheKey) -> ObservationCacheLookup {
        let Ok(_guard) = self.gate.lock() else {
            return ObservationCacheLookup::Miss(ObservationCacheMissReason::Unavailable);
        };
        let Ok(path) = self.entry_path(key) else {
            return ObservationCacheLookup::Miss(ObservationCacheMissReason::CorruptOrUnsupported);
        };
        let mut file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return ObservationCacheLookup::Miss(ObservationCacheMissReason::Absent);
            }
            Err(_) => {
                return ObservationCacheLookup::Miss(ObservationCacheMissReason::Unavailable);
            }
        };
        if file
            .metadata()
            .is_ok_and(|metadata| metadata.len() > self.policy.max_entry_bytes)
        {
            let _ = fs::remove_file(path);
            return ObservationCacheLookup::Miss(ObservationCacheMissReason::Oversized);
        }
        let bytes = match read_cache_entry_bounded(&mut file, self.policy.max_entry_bytes) {
            Ok(BoundedCacheRead::Complete(bytes)) => bytes,
            Ok(BoundedCacheRead::Oversized) => {
                let _ = fs::remove_file(path);
                return ObservationCacheLookup::Miss(ObservationCacheMissReason::Oversized);
            }
            Err(_) => {
                return ObservationCacheLookup::Miss(ObservationCacheMissReason::Unavailable);
            }
        };
        let entry = serde_json::from_slice::<ObservationCacheEntry>(&bytes)
            .ok()
            .filter(|entry| entry.key == *key && entry.validate().is_ok());
        match entry {
            Some(entry) => ObservationCacheLookup::Hit(entry),
            None => {
                let _ = fs::remove_file(path);
                ObservationCacheLookup::Miss(ObservationCacheMissReason::CorruptOrUnsupported)
            }
        }
    }

    pub fn store(&self, entry: &ObservationCacheEntry) -> ObservationCacheStoreOutcome {
        let Ok(_guard) = self.gate.lock() else {
            return ObservationCacheStoreOutcome::Unavailable;
        };
        if entry.validate().is_err() {
            return ObservationCacheStoreOutcome::Unavailable;
        }
        let Ok(encoded) = serde_json::to_vec(entry) else {
            return ObservationCacheStoreOutcome::Unavailable;
        };
        if u64::try_from(encoded.len()).map_or(true, |len| len > self.policy.max_entry_bytes) {
            return ObservationCacheStoreOutcome::SkippedOversized;
        }
        if fs::create_dir_all(&self.root).is_err() {
            return ObservationCacheStoreOutcome::Unavailable;
        }
        let Ok(path) = self.entry_path(&entry.key) else {
            return ObservationCacheStoreOutcome::Unavailable;
        };
        if let Ok(existing) = fs::read(&path) {
            if existing == encoded {
                return ObservationCacheStoreOutcome::AlreadyPresent;
            }
            // Content-addressed observations are immutable. Different bytes
            // for one key indicate corruption or broken determinism, so the
            // existing evidence must not be replaced in place.
            return ObservationCacheStoreOutcome::Unavailable;
        }
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = self.root.join(format!(
            ".observation-cache-{}-{sequence}.tmp",
            std::process::id()
        ));
        let write_result = (|| -> std::io::Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(&encoded)?;
            file.sync_all()?;
            match fs::hard_link(&temporary, &path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    match fs::read(&path) {
                        Ok(existing) if existing == encoded => Ok(()),
                        Ok(_) => Err(std::io::Error::new(
                            std::io::ErrorKind::AlreadyExists,
                            "observation cache key was installed with different bytes",
                        )),
                        Err(read_error) => Err(read_error),
                    }
                }
                Err(error) => Err(error),
            }
        })();
        let _ = fs::remove_file(&temporary);
        if write_result.is_err() {
            return ObservationCacheStoreOutcome::Unavailable;
        }
        self.cleanup();
        ObservationCacheStoreOutcome::Stored
    }

    fn entry_path(&self, key: &ObservationCacheKey) -> Result<PathBuf> {
        let hash = key.identity_hash()?;
        let digest = hash.strip_prefix("sha256:").ok_or_else(|| {
            CdfError::internal("observation cache key hash omitted sha256 prefix")
        })?;
        Ok(self.root.join(format!("{digest}.json")))
    }

    fn cleanup(&self) {
        let Ok(entries) = fs::read_dir(&self.root) else {
            return;
        };
        let mut files = entries
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                (path.extension().and_then(|value| value.to_str()) == Some("json"))
                    .then(|| entry.metadata().ok().map(|metadata| (path, metadata)))
                    .flatten()
            })
            .map(|(path, metadata)| {
                (
                    metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                    path,
                    metadata.len(),
                )
            })
            .collect::<Vec<_>>();
        files.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        let mut bytes = files.iter().map(|(_, _, len)| *len).sum::<u64>();
        let mut count = files.len();
        for (_, path, len) in files {
            if count <= self.policy.max_entries && bytes <= self.policy.max_bytes {
                break;
            }
            if fs::remove_file(path).is_ok() {
                count = count.saturating_sub(1);
                bytes = bytes.saturating_sub(len);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::io::Cursor;

    fn key(generation: &str) -> ObservationCacheKey {
        ObservationCacheKey::new(
            StrongObservationSourceIdentity::new(
                "https://example.test/data.parquet",
                42,
                None,
                BTreeMap::from([("etag".to_owned(), generation.to_owned())]),
            )
            .unwrap(),
            "parquet",
            "1.0.0",
            "sha256:interpretation",
            "sha256:observation-contract",
            "namecase-v1",
            "sha256:admission",
        )
        .unwrap()
    }

    fn entry(key: ObservationCacheKey) -> ObservationCacheEntry {
        ObservationCacheEntry::new(
            key,
            &Schema::new(vec![Field::new("id", DataType::Int64, false)]),
            BTreeMap::from([("etag".to_owned(), "generation".to_owned())]),
            128,
            0,
        )
        .unwrap()
    }

    #[test]
    fn exact_key_hits_and_generation_mismatch_misses() {
        let temp = tempfile::tempdir().unwrap();
        let store = ObservationCacheStore::new(temp.path());
        let first_key = key("v1");
        assert_eq!(
            store.lookup(&first_key),
            ObservationCacheLookup::Miss(ObservationCacheMissReason::Absent)
        );
        assert_eq!(
            store.store(&entry(first_key.clone())),
            ObservationCacheStoreOutcome::Stored
        );
        assert!(matches!(
            store.lookup(&first_key),
            ObservationCacheLookup::Hit(_)
        ));
        assert_eq!(
            store.lookup(&key("v2")),
            ObservationCacheLookup::Miss(ObservationCacheMissReason::Absent)
        );

        let mut interpretation_mismatch = first_key.clone();
        interpretation_mismatch
            .interpretation_hash
            .push_str(":changed");
        let mut normalizer_mismatch = first_key.clone();
        normalizer_mismatch.normalizer_version.push_str(":changed");
        let mut admission_mismatch = first_key.clone();
        admission_mismatch.admission_identity.push_str(":changed");
        for mismatch in [
            interpretation_mismatch,
            normalizer_mismatch,
            admission_mismatch,
        ] {
            assert_eq!(
                store.lookup(&mismatch),
                ObservationCacheLookup::Miss(ObservationCacheMissReason::Absent)
            );
        }
    }

    #[test]
    fn corruption_and_unsupported_versions_miss_and_are_removed() {
        let temp = tempfile::tempdir().unwrap();
        let store = ObservationCacheStore::new(temp.path());
        let key = key("v1");
        let path = store.entry_path(&key).unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, br#"{"version":999}"#).unwrap();
        assert_eq!(
            store.lookup(&key),
            ObservationCacheLookup::Miss(ObservationCacheMissReason::CorruptOrUnsupported)
        );
        assert!(!path.exists());
    }

    #[test]
    fn oversized_lookup_is_bounded_before_deserialization() {
        struct CountingReader {
            inner: Cursor<Vec<u8>>,
            bytes_read: usize,
        }

        impl Read for CountingReader {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                let read = self.inner.read(buffer)?;
                self.bytes_read += read;
                Ok(read)
            }
        }

        let maximum_bytes = 32_u64;
        let mut reader = CountingReader {
            inner: Cursor::new(vec![0_u8; 1_024]),
            bytes_read: 0,
        };
        assert!(matches!(
            read_cache_entry_bounded(&mut reader, maximum_bytes).unwrap(),
            BoundedCacheRead::Oversized
        ));
        assert_eq!(reader.bytes_read, maximum_bytes as usize + 1);

        let temp = tempfile::tempdir().unwrap();
        let policy = ObservationCachePolicy::new(4, 4_096, maximum_bytes).unwrap();
        let store = ObservationCacheStore::with_policy(temp.path(), policy);
        let key = key("oversized");
        let path = store.entry_path(&key).unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, vec![0_u8; 1_024]).unwrap();
        assert_eq!(
            store.lookup(&key),
            ObservationCacheLookup::Miss(ObservationCacheMissReason::Oversized)
        );
        assert!(!path.exists());
    }

    #[test]
    fn cleanup_enforces_entry_count_bound() {
        let temp = tempfile::tempdir().unwrap();
        let policy = ObservationCachePolicy::new(1, 1024 * 1024, 512 * 1024).unwrap();
        let store = ObservationCacheStore::with_policy(temp.path(), policy);
        let first = key("v1");
        let second = key("v2");
        assert_eq!(
            store.store(&entry(first.clone())),
            ObservationCacheStoreOutcome::Stored
        );
        assert_eq!(
            store.store(&entry(second.clone())),
            ObservationCacheStoreOutcome::Stored
        );
        let hits = [first, second]
            .into_iter()
            .filter(|key| matches!(store.lookup(key), ObservationCacheLookup::Hit(_)))
            .count();
        assert_eq!(hits, 1);
    }

    #[test]
    fn same_key_with_different_evidence_is_never_overwritten() {
        let temp = tempfile::tempdir().unwrap();
        let store = ObservationCacheStore::new(temp.path());
        let key = key("v1");
        let first = entry(key.clone());
        let mut conflicting = entry(key.clone());
        conflicting.observed_bytes += 1;

        assert_eq!(store.store(&first), ObservationCacheStoreOutcome::Stored);
        assert_eq!(
            store.store(&conflicting),
            ObservationCacheStoreOutcome::Unavailable
        );
        assert_eq!(store.lookup(&key), ObservationCacheLookup::Hit(first));
    }
}
