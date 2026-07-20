#![doc = "Bounded content-addressed task-set artifacts for cdf planners."]

use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use cdf_kernel::{
    CdfError, ContentObjectKey, ContentProviderGeneration, ContentStoreNamespace,
    PLANNED_TASK_SET_REFERENCE_VERSION, PlannedTaskSetReference, Result,
};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest,
    reserve_blocking,
};
use cdf_runtime::{SpillBudgetCoordinator, SpillReservation};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

const MAGIC: &[u8; 8] = b"CDFTASK1";
const FORMAT_VERSION: u16 = 1;
const TASK_TAG: u8 = 1;
const FOOTER_TAG: u8 = u8::MAX;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskSetLimits {
    pub maximum_task_bytes: u64,
    pub maximum_authority_bytes: u64,
    pub writer_buffer_bytes: usize,
}

impl TaskSetLimits {
    pub fn validate(&self) -> Result<()> {
        if self.maximum_task_bytes == 0
            || self.maximum_authority_bytes == 0
            || self.writer_buffer_bytes == 0
        {
            return Err(CdfError::contract(
                "task-set record, shared-authority, and writer-buffer budgets must be nonzero",
            ));
        }
        usize::try_from(self.maximum_task_bytes).map_err(|_| {
            CdfError::contract("task-set maximum task bytes exceeds addressable memory")
        })?;
        usize::try_from(self.maximum_authority_bytes).map_err(|_| {
            CdfError::contract("task-set maximum authority bytes exceeds addressable memory")
        })?;
        Ok(())
    }
}

/// A local content-addressed store for canonical planned task sets.
///
/// The root is an injected planning-artifact store, not a coordinator path embedded in task
/// bytes. Temporary construction is spill-accounted. Once atomically installed, persistent
/// retention belongs to the content store and its ordinary reachability/GC authority.
#[derive(Clone, Debug)]
pub struct ExternalTaskStore {
    root: PathBuf,
    namespace: ContentStoreNamespace,
}

impl ExternalTaskStore {
    pub fn new(root: impl Into<PathBuf>, namespace: ContentStoreNamespace) -> Result<Self> {
        let root = root.into();
        if root.as_os_str().is_empty() {
            return Err(CdfError::contract("task-store root cannot be empty"));
        }
        validate_relative_component(namespace.as_str(), "task-store namespace")?;
        Ok(Self { root, namespace })
    }

    pub fn writer(
        &self,
        task_type: &str,
        limits: TaskSetLimits,
        memory: Arc<dyn MemoryCoordinator>,
        spill: &dyn SpillBudgetCoordinator,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetWriter> {
        require_token("task-set type", task_type)?;
        limits.validate()?;
        let directory = self.root.join(self.namespace.as_str()).join("task-sets");
        fs::create_dir_all(&directory)
            .map_err(|error| io_error("create task-set directory", &directory, error))?;

        let maximum_payload_bytes = usize::try_from(
            limits
                .maximum_task_bytes
                .max(limits.maximum_authority_bytes),
        )
        .map_err(|_| CdfError::contract("task-set payload budget exceeds usize"))?;
        let task_type_bytes = u64::try_from(task_type.len())
            .map_err(|_| CdfError::contract("task-set type length exceeds u64"))?;
        let reserved_memory = u64::try_from(maximum_payload_bytes)
            .map_err(|_| CdfError::contract("task-set payload budget exceeds u64"))?
            .checked_add(
                u64::try_from(limits.writer_buffer_bytes)
                    .map_err(|_| CdfError::contract("task-set writer-buffer budget exceeds u64"))?,
            )
            .and_then(|bytes| bytes.checked_add(task_type_bytes))
            .ok_or_else(|| CdfError::contract("task-set memory budget overflowed u64"))?;
        let request = ReservationRequest::new(
            ConsumerKey::new("external-task-set-writer", MemoryClass::Control)?,
            reserved_memory,
        )?;
        let memory_lease = reserve_blocking(memory, &request)?;
        let mut spill_reservation = spill.try_reserve(1)?.ok_or_else(|| {
            CdfError::data(
                "task-set planning requires spill space but the configured disk budget is exhausted",
            )
        })?;
        spill_reservation.shrink(1);

        let temporary = NamedTempFile::new_in(&directory)
            .map_err(|error| io_error("create task-set temporary file", &directory, error))?;
        let file = temporary
            .as_file()
            .try_clone()
            .map_err(|error| io_error("clone task-set temporary file", temporary.path(), error))?;
        let hashing = HashingWriter::new(file);
        let writer = BufWriter::with_capacity(limits.writer_buffer_bytes, hashing);
        let mut task_writer = ExternalTaskSetWriter {
            store: self.clone(),
            task_type: task_type.to_owned(),
            limits,
            temporary: Some(temporary),
            writer: Some(writer),
            payload: Vec::with_capacity(maximum_payload_bytes),
            authority_sha256: String::new(),
            next_ordinal: 0,
            spill_reservation: Some(spill_reservation),
            _memory_lease: memory_lease,
            poisoned: false,
        };
        task_writer.write_reserved(MAGIC)?;
        task_writer.write_reserved(&FORMAT_VERSION.to_be_bytes())?;
        let task_type_bytes = task_type.as_bytes();
        let task_type_length = u16::try_from(task_type_bytes.len())
            .map_err(|_| CdfError::contract("task-set type is too long"))?;
        task_writer.write_reserved(&task_type_length.to_be_bytes())?;
        task_writer.write_reserved(task_type_bytes)?;
        task_writer.payload.clear();
        let maximum_authority_bytes =
            usize::try_from(task_writer.limits.maximum_authority_bytes)
                .map_err(|_| CdfError::contract("task-set authority budget exceeds usize"))?;
        let mut bounded = BoundedVec::new(&mut task_writer.payload, maximum_authority_bytes);
        encode_authority(&mut bounded)?;
        if task_writer.payload.is_empty() {
            return Err(CdfError::data(
                "task-set shared authority payload cannot be empty",
            ));
        }
        let authority_length = u64::try_from(task_writer.payload.len())
            .map_err(|_| CdfError::data("task-set authority payload exceeds u64"))?;
        let authority_digest: [u8; 32] = Sha256::digest(&task_writer.payload).into();
        task_writer.write_reserved(&authority_length.to_be_bytes())?;
        task_writer.write_reserved(&authority_digest)?;
        let payload = std::mem::take(&mut task_writer.payload);
        task_writer.write_reserved(&payload)?;
        task_writer.payload = payload;
        task_writer.authority_sha256 = format!("sha256:{}", hex::encode(authority_digest));
        Ok(task_writer)
    }

    pub fn reader(
        &self,
        reference: PlannedTaskSetReference,
        expected_task_type: &str,
        maximum_task_bytes: u64,
        maximum_authority_bytes: u64,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<ExternalTaskSetReader> {
        reference.validate()?;
        if reference.store_namespace != self.namespace {
            return Err(CdfError::contract(
                "task-set artifact namespace does not match the selected store",
            ));
        }
        require_token("task-set type", expected_task_type)?;
        if reference.task_type != expected_task_type {
            return Err(CdfError::contract(
                "task-set reference type does not match the expected task decoder",
            ));
        }
        if maximum_task_bytes == 0 || maximum_authority_bytes == 0 {
            return Err(CdfError::contract(
                "task-set reader task and shared-authority budgets must be nonzero",
            ));
        }
        let path = self.path_for_reference(&reference)?;
        let file =
            File::open(&path).map_err(|error| io_error("open task-set artifact", &path, error))?;
        let mut reader = ExternalTaskSetReader {
            reference,
            file,
            path,
            hasher: Sha256::new(),
            observed_bytes: 0,
            expected_ordinal: 0,
            maximum_task_bytes,
            memory,
            authority: None,
            authority_sha256: String::new(),
            finished: false,
        };
        let magic = reader.read_array::<8>()?;
        if &magic != MAGIC {
            return Err(CdfError::data(
                "task-set artifact has invalid framing magic",
            ));
        }
        let version = u16::from_be_bytes(reader.read_array::<2>()?);
        if version != FORMAT_VERSION {
            return Err(CdfError::contract(format!(
                "task-set format version {version} is unsupported; expected {FORMAT_VERSION}"
            )));
        }
        let task_type_length = usize::from(u16::from_be_bytes(reader.read_array::<2>()?));
        let task_type_request = ReservationRequest::new(
            ConsumerKey::new("external-task-set-header", MemoryClass::Control)?,
            u64::try_from(task_type_length)
                .map_err(|_| CdfError::data("task-set type length exceeds u64"))?,
        )?;
        let task_type_lease = reserve_blocking(Arc::clone(&reader.memory), &task_type_request)?;
        let task_type = reader.read_vec(task_type_length)?;
        if task_type != expected_task_type.as_bytes() {
            return Err(CdfError::contract(format!(
                "task-set type does not match expected `{expected_task_type}`"
            )));
        }
        drop(task_type_lease);
        let authority_length = u64::from_be_bytes(reader.read_array::<8>()?);
        if authority_length == 0 || authority_length > maximum_authority_bytes {
            return Err(CdfError::data(format!(
                "task-set authority length {authority_length} exceeds the configured budget {maximum_authority_bytes}"
            )));
        }
        let expected_authority_digest = reader.read_array::<32>()?;
        let authority_request = ReservationRequest::new(
            ConsumerKey::new("external-task-set-authority", MemoryClass::Control)?,
            authority_length,
        )?;
        let authority_lease = reserve_blocking(Arc::clone(&reader.memory), &authority_request)?;
        let authority =
            reader
                .read_vec(usize::try_from(authority_length).map_err(|_| {
                    CdfError::data("task-set authority exceeds addressable memory")
                })?)?;
        let observed_authority_digest: [u8; 32] = Sha256::digest(&authority).into();
        if observed_authority_digest != expected_authority_digest {
            return Err(CdfError::data(
                "task-set shared authority does not match its content identity",
            ));
        }
        reader.authority = Some(AccountedBytes::new(
            Bytes::from(authority),
            authority_lease,
        )?);
        reader.authority_sha256 = format!("sha256:{}", hex::encode(observed_authority_digest));
        Ok(reader)
    }

    /// Creates an invocation-local workspace beside task artifacts.
    ///
    /// The workspace is never serialized into a task reference and is removed on drop. Callers
    /// remain responsible for accounting every byte written through the shared spill authority.
    pub fn temporary_workspace(&self, label: &str) -> Result<ExternalTaskWorkspace> {
        require_token("task-store workspace label", label)?;
        let directory = self.root.join(self.namespace.as_str()).join("scratch");
        fs::create_dir_all(&directory)
            .map_err(|error| io_error("create task-store scratch directory", &directory, error))?;
        let directory = tempfile::Builder::new()
            .prefix(&format!("{label}-"))
            .tempdir_in(&directory)
            .map_err(|error| {
                io_error("create task-store temporary workspace", &directory, error)
            })?;
        Ok(ExternalTaskWorkspace { directory })
    }

    fn path_for_reference(&self, reference: &PlannedTaskSetReference) -> Result<PathBuf> {
        let key = Path::new(reference.object_key.as_str());
        if key.is_absolute()
            || key
                .components()
                .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
        {
            return Err(CdfError::contract(
                "task-set object key must be a safe relative path",
            ));
        }
        Ok(self.root.join(self.namespace.as_str()).join(key))
    }
}

/// RAII ownership for invocation-local planner scratch.
pub struct ExternalTaskWorkspace {
    directory: tempfile::TempDir,
}

impl ExternalTaskWorkspace {
    pub fn path(&self) -> &Path {
        self.directory.path()
    }
}

pub struct ExternalTaskSetWriter {
    store: ExternalTaskStore,
    task_type: String,
    limits: TaskSetLimits,
    temporary: Option<NamedTempFile>,
    writer: Option<BufWriter<HashingWriter>>,
    payload: Vec<u8>,
    authority_sha256: String,
    next_ordinal: u64,
    spill_reservation: Option<SpillReservation>,
    _memory_lease: cdf_memory::MemoryLease,
    poisoned: bool,
}

impl ExternalTaskSetWriter {
    /// Appends one payload whose encoder is responsible for canonical semantic bytes.
    ///
    /// The store deliberately accepts a writer callback rather than arbitrary `Serialize`:
    /// unordered user maps cannot accidentally masquerade as canonical task identity, and the
    /// encoder cannot allocate an unbounded intermediate payload inside this authority.
    pub fn push_with(
        &mut self,
        canonical_ordinal: u64,
        encode: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<()> {
        if self.poisoned {
            return Err(CdfError::contract(
                "task-set writer cannot continue after a partial write failure",
            ));
        }
        if canonical_ordinal != self.next_ordinal {
            return Err(CdfError::contract(format!(
                "task-set canonical ordinal {canonical_ordinal} is out of order; expected {}",
                self.next_ordinal
            )));
        }
        self.payload.clear();
        let maximum = usize::try_from(self.limits.maximum_task_bytes)
            .map_err(|_| CdfError::contract("task-set task budget exceeds usize"))?;
        let mut bounded = BoundedVec::new(&mut self.payload, maximum);
        encode(&mut bounded)?;
        if self.payload.is_empty() {
            return Err(CdfError::data("canonical task payload cannot be empty"));
        }
        let payload_length = u64::try_from(self.payload.len())
            .map_err(|_| CdfError::data("canonical task payload exceeds u64"))?;
        let payload_digest: [u8; 32] = Sha256::digest(&self.payload).into();
        let frame_bytes = 1_u64
            .checked_add(8)
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(32))
            .and_then(|value| value.checked_add(payload_length))
            .ok_or_else(|| CdfError::data("task-set frame length overflowed u64"))?;
        self.reserve_spill(frame_bytes)?;
        self.write_unreserved(&[TASK_TAG], "write task-set record tag")?;
        self.write_unreserved(
            &canonical_ordinal.to_be_bytes(),
            "write task-set record ordinal",
        )?;
        self.write_unreserved(
            &payload_length.to_be_bytes(),
            "write task-set record length",
        )?;
        self.write_unreserved(&payload_digest, "write task-set record digest")?;
        let payload = std::mem::take(&mut self.payload);
        let result = self.write_unreserved(&payload, "write task-set record payload");
        self.payload = payload;
        result?;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("task-set ordinal overflowed u64"))?;
        Ok(())
    }

    pub fn finalize(mut self) -> Result<ExternalTaskSetArtifact> {
        if self.poisoned {
            return Err(CdfError::contract(
                "task-set writer cannot finalize after a partial write failure",
            ));
        }
        self.write_reserved(&[FOOTER_TAG])?;
        self.write_reserved(&self.next_ordinal.to_be_bytes())?;
        let writer = self
            .writer
            .take()
            .ok_or_else(|| CdfError::contract("task-set writer was already finalized"))?;
        let mut hashing = writer.into_inner().map_err(|error| {
            io_error(
                "flush task-set writer",
                self.temporary_path(),
                error.into_error(),
            )
        })?;
        hashing
            .flush()
            .map_err(|error| io_error("flush task-set artifact", self.temporary_path(), error))?;
        hashing
            .file
            .sync_all()
            .map_err(|error| io_error("sync task-set artifact", self.temporary_path(), error))?;
        let byte_count = hashing.bytes;
        let digest = format!("sha256:{}", hex::encode(hashing.hasher.finalize()));
        drop(hashing.file);

        let hex_digest = digest.trim_start_matches("sha256:");
        let object_key_text = format!("task-sets/sha256/{hex_digest}.cdftasks");
        let object_key = ContentObjectKey::new(object_key_text.clone())?;
        let final_path = self
            .store
            .root
            .join(self.store.namespace.as_str())
            .join(&object_key_text);
        let temporary = self
            .temporary
            .take()
            .ok_or_else(|| CdfError::contract("task-set temporary file is missing"))?;
        install_content_addressed(temporary, &final_path, byte_count, &digest)?;

        if let Some(mut reservation) = self.spill_reservation.take() {
            reservation.shrink(reservation.bytes());
        }
        let reference = PlannedTaskSetReference {
            version: PLANNED_TASK_SET_REFERENCE_VERSION,
            task_type: self.task_type.clone(),
            task_count: self.next_ordinal,
            store_namespace: self.store.namespace.clone(),
            object_key,
            byte_count,
            content_sha256: digest.clone(),
            provider_generation: ContentProviderGeneration::new(digest)?,
        };
        reference.validate()?;
        Ok(ExternalTaskSetArtifact {
            task_type: self.task_type,
            task_count: self.next_ordinal,
            authority_sha256: self.authority_sha256,
            reference,
            path: final_path,
        })
    }

    pub fn authority_sha256(&self) -> &str {
        &self.authority_sha256
    }

    fn writer_mut(&mut self) -> Result<&mut BufWriter<HashingWriter>> {
        self.writer
            .as_mut()
            .ok_or_else(|| CdfError::contract("task-set writer was already finalized"))
    }

    fn temporary_path(&self) -> &Path {
        self.temporary
            .as_ref()
            .map_or_else(|| Path::new("<finalized-task-set>"), NamedTempFile::path)
    }

    fn reserve_spill(&mut self, additional: u64) -> Result<()> {
        let reservation = self
            .spill_reservation
            .as_mut()
            .ok_or_else(|| CdfError::contract("task-set spill reservation is missing"))?;
        if !reservation.try_grow(additional)? {
            return Err(CdfError::data(
                "task-set artifact exceeded the configured disk budget; increase the spill budget or narrow the planned table extent",
            ));
        }
        Ok(())
    }

    fn write_reserved(&mut self, bytes: &[u8]) -> Result<()> {
        let length = u64::try_from(bytes.len())
            .map_err(|_| CdfError::data("task-set write length exceeds u64"))?;
        self.reserve_spill(length)?;
        self.write_unreserved(bytes, "write task-set artifact")
    }

    fn write_unreserved(&mut self, bytes: &[u8], action: &str) -> Result<()> {
        let path = self.temporary_path().to_path_buf();
        if let Err(error) = self.writer_mut()?.write_all(bytes) {
            self.poisoned = true;
            return Err(io_error(action, &path, error));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalTaskSetArtifact {
    pub task_type: String,
    pub task_count: u64,
    pub authority_sha256: String,
    pub reference: PlannedTaskSetReference,
    pub path: PathBuf,
}

pub struct ExternalTaskSetReader {
    reference: PlannedTaskSetReference,
    file: File,
    path: PathBuf,
    hasher: Sha256,
    observed_bytes: u64,
    expected_ordinal: u64,
    maximum_task_bytes: u64,
    memory: Arc<dyn MemoryCoordinator>,
    authority: Option<AccountedBytes>,
    authority_sha256: String,
    finished: bool,
}

impl ExternalTaskSetReader {
    pub fn authority(&self) -> &AccountedBytes {
        self.authority
            .as_ref()
            .expect("task-set reader constructor installs verified authority")
    }

    pub fn authority_sha256(&self) -> &str {
        &self.authority_sha256
    }
    /// Returns the next task. `None` is returned only after the footer and whole-artifact
    /// identity have been verified, so a successful drain is the caller's side-effect barrier.
    pub fn next_record(&mut self) -> Result<Option<ExternalTaskRecord>> {
        if self.finished {
            return Ok(None);
        }
        let tag = self.read_array::<1>()?[0];
        match tag {
            TASK_TAG => {
                let ordinal = u64::from_be_bytes(self.read_array::<8>()?);
                if ordinal != self.expected_ordinal {
                    return Err(CdfError::data(format!(
                        "task-set ordinal {ordinal} is noncanonical; expected {}",
                        self.expected_ordinal
                    )));
                }
                let payload_length = u64::from_be_bytes(self.read_array::<8>()?);
                if payload_length == 0 || payload_length > self.maximum_task_bytes {
                    return Err(CdfError::data(format!(
                        "task-set payload length {payload_length} exceeds the configured per-task budget {}",
                        self.maximum_task_bytes
                    )));
                }
                let expected_digest = self.read_array::<32>()?;
                let request = ReservationRequest::new(
                    ConsumerKey::new("external-task-set-record", MemoryClass::Control)?,
                    payload_length,
                )?;
                let lease = reserve_blocking(Arc::clone(&self.memory), &request)?;
                let payload_length_usize = usize::try_from(payload_length)
                    .map_err(|_| CdfError::data("task-set payload exceeds addressable memory"))?;
                let payload = self.read_vec(payload_length_usize)?;
                let observed_digest: [u8; 32] = Sha256::digest(&payload).into();
                if observed_digest != expected_digest {
                    return Err(CdfError::data(format!(
                        "task-set payload {ordinal} does not match its content identity"
                    )));
                }
                self.expected_ordinal = self
                    .expected_ordinal
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("task-set ordinal overflowed u64"))?;
                Ok(Some(ExternalTaskRecord {
                    canonical_ordinal: ordinal,
                    content_sha256: format!("sha256:{}", hex::encode(expected_digest)),
                    payload: AccountedBytes::new(Bytes::from(payload), lease)?,
                }))
            }
            FOOTER_TAG => {
                let record_count = u64::from_be_bytes(self.read_array::<8>()?);
                if record_count != self.expected_ordinal {
                    return Err(CdfError::data(format!(
                        "task-set footer count {record_count} does not match {} observed records",
                        self.expected_ordinal
                    )));
                }
                let mut trailing = [0_u8; 1];
                match self.file.read(&mut trailing) {
                    Ok(0) => {}
                    Ok(_) => return Err(CdfError::data("task-set artifact has trailing bytes")),
                    Err(error) => {
                        return Err(io_error("read task-set trailing byte", &self.path, error));
                    }
                }
                self.verify_complete()?;
                self.finished = true;
                Ok(None)
            }
            other => Err(CdfError::data(format!(
                "task-set artifact contains unknown frame tag {other}"
            ))),
        }
    }

    pub fn observed_task_count(&self) -> u64 {
        self.expected_ordinal
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut bytes = [0_u8; N];
        self.file
            .read_exact(&mut bytes)
            .map_err(|error| io_error("read task-set artifact", &self.path, error))?;
        self.observe(&bytes)?;
        Ok(bytes)
    }

    fn read_vec(&mut self, length: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0_u8; length];
        self.file
            .read_exact(&mut bytes)
            .map_err(|error| io_error("read task-set artifact", &self.path, error))?;
        self.observe(&bytes)?;
        Ok(bytes)
    }

    fn observe(&mut self, bytes: &[u8]) -> Result<()> {
        self.hasher.update(bytes);
        self.observed_bytes = self
            .observed_bytes
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| CdfError::data("task-set observed bytes exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::data("task-set observed bytes overflowed u64"))?;
        Ok(())
    }

    fn verify_complete(&self) -> Result<()> {
        let observed_digest = format!("sha256:{}", hex::encode(self.hasher.clone().finalize()));
        if self.observed_bytes != self.reference.byte_count
            || observed_digest != self.reference.content_sha256
            || self.reference.provider_generation.as_str() != self.reference.content_sha256
        {
            return Err(CdfError::data(
                "task-set artifact bytes, content identity, or provider generation changed",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ExternalTaskRecord {
    pub canonical_ordinal: u64,
    pub content_sha256: String,
    pub payload: AccountedBytes,
}

struct BoundedVec<'a> {
    bytes: &'a mut Vec<u8>,
    maximum: usize,
}

impl<'a> BoundedVec<'a> {
    fn new(bytes: &'a mut Vec<u8>, maximum: usize) -> Self {
        Self { bytes, maximum }
    }
}

impl Write for BoundedVec<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("task payload length overflow"))?;
        if next > self.maximum {
            return Err(io::Error::other(format!(
                "task payload exceeds configured {} byte budget",
                self.maximum
            )));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct HashingWriter {
    file: File,
    hasher: Sha256,
    bytes: u64,
}

impl HashingWriter {
    fn new(file: File) -> Self {
        Self {
            file,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }
}

impl Write for HashingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.file.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(written).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("task-set byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn install_content_addressed(
    temporary: NamedTempFile,
    final_path: &Path,
    expected_bytes: u64,
    expected_sha256: &str,
) -> Result<()> {
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| io_error("create task-set content directory", parent, error))?;
    }
    match temporary.persist_noclobber(final_path) {
        Ok(_) => {
            sync_parent(final_path)?;
            Ok(())
        }
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
            verify_file(final_path, expected_bytes, expected_sha256)
        }
        Err(error) => Err(io_error(
            "install task-set content address",
            final_path,
            error.error,
        )),
    }
}

fn verify_file(path: &Path, expected_bytes: u64, expected_sha256: &str) -> Result<()> {
    let mut file =
        File::open(path).map_err(|error| io_error("verify task-set artifact", path, error))?;
    let mut hasher = Sha256::new();
    let bytes = io::copy(&mut file, &mut hasher)
        .map_err(|error| io_error("hash task-set artifact", path, error))?;
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));
    if bytes != expected_bytes || digest != expected_sha256 {
        return Err(CdfError::contract(format!(
            "content-addressed task-set path {} contains different bytes",
            path.display()
        )));
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error("sync task-set directory", parent, error))?;
    }
    Ok(())
}

fn validate_relative_component(value: &str, label: &str) -> Result<()> {
    let path = Path::new(value);
    if path.components().count() != 1
        || !matches!(path.components().next(), Some(Component::Normal(_)))
    {
        return Err(CdfError::contract(format!(
            "{label} must be one safe path component"
        )));
    }
    Ok(())
}

fn require_token(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > usize::from(u16::MAX)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(CdfError::contract(format!(
            "{label} must be a nonempty canonical ASCII token"
        )));
    }
    Ok(())
}

fn io_error(action: &str, path: &Path, error: io::Error) -> CdfError {
    CdfError::data(format!("{action} {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
    use cdf_runtime::{FixedSpillBudget, SpillBudgetCoordinator};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct SyntheticTask {
        partition: u64,
        path: String,
    }

    fn authorities(
        memory_bytes: u64,
        spill_bytes: u64,
    ) -> (Arc<dyn MemoryCoordinator>, FixedSpillBudget) {
        (
            Arc::new(DeterministicMemoryCoordinator::new(memory_bytes, BTreeMap::new()).unwrap()),
            FixedSpillBudget::new(spill_bytes).unwrap(),
        )
    }

    fn store(root: &TempDir) -> ExternalTaskStore {
        ExternalTaskStore::new(
            root.path(),
            ContentStoreNamespace::new("planner-artifacts").unwrap(),
        )
        .unwrap()
    }

    fn limits() -> TaskSetLimits {
        TaskSetLimits {
            maximum_task_bytes: 4096,
            maximum_authority_bytes: 4096,
            writer_buffer_bytes: 8192,
        }
    }

    fn encode_authority(output: &mut dyn Write) -> Result<()> {
        output
            .write_all(br#"{"version":1}"#)
            .map_err(|error| CdfError::data(format!("encode synthetic authority: {error}")))
    }

    fn push_task(
        writer: &mut ExternalTaskSetWriter,
        ordinal: u64,
        task: &SyntheticTask,
    ) -> Result<()> {
        writer.push_with(ordinal, |output| {
            serde_json::to_writer(output, task)
                .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
        })
    }

    #[test]
    fn canonical_task_set_round_trips_with_bounded_memory_and_spill() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
        let mut writer = store
            .writer(
                "synthetic-v1",
                limits(),
                Arc::clone(&memory),
                &spill,
                encode_authority,
            )
            .unwrap();
        for ordinal in 0..100 {
            push_task(
                &mut writer,
                ordinal,
                &SyntheticTask {
                    partition: ordinal,
                    path: format!("s3://bucket/{ordinal:08}.parquet"),
                },
            )
            .unwrap();
        }
        let artifact = writer.finalize().unwrap();
        assert_eq!(artifact.task_count, 100);
        assert_eq!(artifact.reference.task_count, 100);
        assert_eq!(artifact.authority_sha256, writer_authority_hash());
        let portable = cdf_runtime::WorkerArtifactReference::from(&artifact.reference);
        portable.validate().unwrap();
        assert_eq!(
            portable.kind,
            cdf_runtime::WorkerArtifactKind::PlannedTaskSet
        );
        assert_eq!(spill.snapshot().current_bytes, 0);
        assert!(spill.snapshot().peak_bytes <= 1024 * 1024);
        assert!(memory.snapshot().peak_bytes <= 64 * 1024);

        let mut reader = store
            .reader(
                artifact.reference.clone(),
                "synthetic-v1",
                4096,
                4096,
                Arc::clone(&memory),
            )
            .unwrap();
        assert_eq!(reader.authority().payload(), br#"{"version":1}"#);
        assert_eq!(reader.authority_sha256(), writer_authority_hash());
        let mut count = 0;
        while let Some(record) = reader.next_record().unwrap() {
            let task: SyntheticTask = serde_json::from_slice(record.payload.payload()).unwrap();
            assert_eq!(record.canonical_ordinal, count);
            assert_eq!(task.partition, count);
            count += 1;
        }
        assert_eq!(count, 100);
        assert_eq!(reader.observed_task_count(), 100);
    }

    #[test]
    fn jobs_timing_and_store_location_do_not_change_identity() {
        let first_root = TempDir::new().unwrap();
        let second_root = TempDir::new().unwrap();
        let mut references = Vec::new();
        for root in [&first_root, &second_root] {
            let store = store(root);
            let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
            let mut writer = store
                .writer("synthetic-v1", limits(), memory, &spill, encode_authority)
                .unwrap();
            for ordinal in 0..32 {
                push_task(
                    &mut writer,
                    ordinal,
                    &SyntheticTask {
                        partition: ordinal,
                        path: format!("s3://bucket/{ordinal:08}.parquet"),
                    },
                )
                .unwrap();
            }
            references.push(writer.finalize().unwrap().reference);
        }
        assert_eq!(references[0], references[1]);
    }

    #[test]
    fn tamper_and_noncanonical_order_fail_closed() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
        let mut writer = store
            .writer(
                "synthetic-v1",
                limits(),
                Arc::clone(&memory),
                &spill,
                encode_authority,
            )
            .unwrap();
        let task = SyntheticTask {
            partition: 0,
            path: "file:///zero.parquet".to_owned(),
        };
        assert!(
            push_task(&mut writer, 1, &task)
                .unwrap_err()
                .message
                .contains("out of order")
        );
        push_task(&mut writer, 0, &task).unwrap();
        let artifact = writer.finalize().unwrap();

        let mut bytes = fs::read(&artifact.path).unwrap();
        let last_payload_byte = bytes.len() - 10;
        bytes[last_payload_byte] ^= 1;
        fs::write(&artifact.path, bytes).unwrap();
        let mut reader = store
            .reader(
                artifact.reference,
                "synthetic-v1",
                4096,
                4096,
                Arc::clone(&memory),
            )
            .unwrap();
        let error = loop {
            match reader.next_record() {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("tampered task set passed verification"),
                Err(error) => break error,
            }
        };
        assert!(
            error.message.contains("content identity")
                || error.message.contains("changed")
                || error.message.contains("footer")
        );
    }

    #[test]
    fn configured_task_and_spill_budgets_fail_cleanly() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(64 * 1024, 96);
        let mut writer = store
            .writer("synthetic-v1", limits(), memory, &spill, encode_authority)
            .unwrap();
        let oversized = SyntheticTask {
            partition: 0,
            path: "x".repeat(5000),
        };
        assert!(
            push_task(&mut writer, 0, &oversized)
                .unwrap_err()
                .message
                .contains("configured")
        );

        let small = SyntheticTask {
            partition: 0,
            path: "file:///zero.parquet".to_owned(),
        };
        let error = push_task(&mut writer, 0, &small).unwrap_err();
        assert!(error.message.contains("disk budget"));
    }

    #[test]
    #[ignore = "slow million-task constant-memory conformance"]
    fn million_tasks_hold_the_configured_metadata_budget() {
        let root = TempDir::new().unwrap();
        let store = store(&root);
        let (memory, spill) = authorities(64 * 1024, 256 * 1024 * 1024);
        let mut writer = store
            .writer(
                "million-v1",
                limits(),
                Arc::clone(&memory),
                &spill,
                encode_authority,
            )
            .unwrap();
        for ordinal in 0..1_000_000 {
            push_task(
                &mut writer,
                ordinal,
                &SyntheticTask {
                    partition: ordinal,
                    path: format!("s3://b/{ordinal:08}"),
                },
            )
            .unwrap();
        }
        let artifact = writer.finalize().unwrap();
        assert_eq!(artifact.task_count, 1_000_000);
        assert!(memory.snapshot().peak_bytes <= 64 * 1024);
        assert!(spill.snapshot().peak_bytes <= 256 * 1024 * 1024);
    }

    fn writer_authority_hash() -> &'static str {
        "sha256:2430f1a2ad2982d0067885488a4c89e21ad1d7c83b115ba8f1b20acc88dfaea8"
    }
}
