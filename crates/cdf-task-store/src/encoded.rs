//! Canonical encoded task-set reader and writer.

use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use cdf_kernel::{
    CdfError, ContentObjectKey, ContentProviderGeneration, PLANNED_TASK_SET_REFERENCE_VERSION,
    PlannedTaskSetReference, Result,
};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest,
    reserve_blocking,
};
use cdf_runtime::{RunCancellation, SpillBudgetCoordinator, SpillReservation};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::limits::{TaskSetLimits, require_token, task_writer_memory_requirements};
use crate::publication::{artifact_io_error, install_content_addressed, io_error};
use crate::store::ExternalTaskStore;

const MAGIC: &[u8; 8] = b"CDFTASK1";
const FORMAT_VERSION: u16 = 2;
const TASK_TAG: u8 = 1;
const AUTHORITY_TAG: u8 = 2;
const FOOTER_TAG: u8 = u8::MAX;
const FOOTER_BYTES: u64 = 1 + 8 + 8;

impl ExternalTaskStore {
    pub fn writer(
        &self,
        task_type: &str,
        limits: TaskSetLimits,
        memory: Arc<dyn MemoryCoordinator>,
        spill: &dyn SpillBudgetCoordinator,
    ) -> Result<ExternalTaskSetWriter> {
        require_token("task-set type", task_type)?;
        limits.validate()?;
        let (maximum_payload_bytes, reserved_memory) =
            task_writer_memory_requirements(task_type, &limits)?;
        let memory_lease = reserve_blocking(
            memory,
            &ReservationRequest::new(
                ConsumerKey::new("external-task-set-writer", MemoryClass::Control)?,
                reserved_memory,
            )?,
        )?;
        self.writer_with_memory_lease(
            task_type,
            limits,
            spill,
            maximum_payload_bytes,
            memory_lease,
        )
    }

    pub(crate) fn writer_with_memory_lease(
        &self,
        task_type: &str,
        limits: TaskSetLimits,
        spill: &dyn SpillBudgetCoordinator,
        maximum_payload_bytes: usize,
        memory_lease: MemoryLease,
    ) -> Result<ExternalTaskSetWriter> {
        let (_, required_memory) = task_writer_memory_requirements(task_type, &limits)?;
        if memory_lease.bytes() != required_memory {
            return Err(CdfError::contract(format!(
                "task-set writer lease owns {} bytes but its working set requires {required_memory}",
                memory_lease.bytes()
            )));
        }
        let directory = self.task_set_directory();
        fs::create_dir_all(&directory)
            .map_err(|error| io_error("create task-set directory", &directory, error))?;
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
            next_ordinal: 0,
            spill_reservation: Some(spill_reservation),
            _memory_lease: memory_lease,
            poisoned: false,
        };
        task_writer.write_reserved(MAGIC)?;
        task_writer.write_reserved(&FORMAT_VERSION.to_be_bytes())?;
        let task_type_length = u16::try_from(task_type.len())
            .map_err(|_| CdfError::contract("task-set type is too long"))?;
        task_writer.write_reserved(&task_type_length.to_be_bytes())?;
        task_writer.write_reserved(task_type.as_bytes())?;
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
        if &reference.store_namespace != self.namespace() {
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
        let file = File::open(&path)
            .map_err(|error| artifact_io_error("open task-set artifact", &path, error))?;
        let mut cursor = ExternalTaskSetReadCursor {
            file,
            path,
            hasher: Sha256::new(),
            observed_bytes: 0,
        };
        let magic = cursor.read_array::<8>()?;
        if &magic != MAGIC {
            return Err(CdfError::data(
                "task-set artifact has invalid framing magic",
            ));
        }
        let version = u16::from_be_bytes(cursor.read_array::<2>()?);
        if version != FORMAT_VERSION {
            return Err(CdfError::contract(format!(
                "task-set format version {version} is unsupported; expected {FORMAT_VERSION}"
            )));
        }
        let task_type_length = usize::from(u16::from_be_bytes(cursor.read_array::<2>()?));
        let task_type_request = ReservationRequest::new(
            ConsumerKey::new("external-task-set-header", MemoryClass::Control)?,
            u64::try_from(task_type_length)
                .map_err(|_| CdfError::data("task-set type length exceeds u64"))?,
        )?;
        let task_type_lease = reserve_blocking(Arc::clone(&memory), &task_type_request)?;
        let task_type = cursor.read_vec(task_type_length)?;
        if task_type != expected_task_type.as_bytes() {
            return Err(CdfError::contract(format!(
                "task-set type does not match expected `{expected_task_type}`"
            )));
        }
        drop(task_type_lease);

        let task_start = cursor.observed_bytes;
        let footer_offset = reference
            .byte_count
            .checked_sub(FOOTER_BYTES)
            .ok_or_else(|| CdfError::data("task-set artifact is shorter than its footer"))?;
        let mut tail = File::open(&cursor.path)
            .map_err(|error| artifact_io_error("open task-set trailer", &cursor.path, error))?;
        tail.seek(SeekFrom::Start(footer_offset))
            .map_err(|error| artifact_io_error("seek task-set footer", &cursor.path, error))?;
        let mut footer = [0_u8; FOOTER_BYTES as usize];
        tail.read_exact(&mut footer)
            .map_err(|error| artifact_io_error("read task-set footer", &cursor.path, error))?;
        if footer[0] != FOOTER_TAG {
            return Err(CdfError::data("task-set artifact has invalid footer tag"));
        }
        let footer_task_count = u64::from_be_bytes(
            footer[1..9]
                .try_into()
                .map_err(|_| CdfError::internal("task-set footer count slice is invalid"))?,
        );
        if footer_task_count != reference.task_count {
            return Err(CdfError::data(format!(
                "task-set footer count {footer_task_count} does not match referenced count {}",
                reference.task_count
            )));
        }
        let authority_offset = u64::from_be_bytes(
            footer[9..17]
                .try_into()
                .map_err(|_| CdfError::internal("task-set authority offset slice is invalid"))?,
        );
        if authority_offset < task_start || authority_offset >= footer_offset {
            return Err(CdfError::data(
                "task-set authority offset is outside the canonical task body",
            ));
        }
        tail.seek(SeekFrom::Start(authority_offset))
            .map_err(|error| artifact_io_error("seek task-set authority", &cursor.path, error))?;
        let mut authority_tag = [0_u8; 1];
        tail.read_exact(&mut authority_tag).map_err(|error| {
            artifact_io_error("read task-set authority tag", &cursor.path, error)
        })?;
        if authority_tag[0] != AUTHORITY_TAG {
            return Err(CdfError::data(
                "task-set artifact has invalid authority tag",
            ));
        }
        let mut authority_length_bytes = [0_u8; 8];
        tail.read_exact(&mut authority_length_bytes)
            .map_err(|error| {
                artifact_io_error("read task-set authority length", &cursor.path, error)
            })?;
        let authority_length = u64::from_be_bytes(authority_length_bytes);
        if authority_length == 0 || authority_length > maximum_authority_bytes {
            return Err(CdfError::data(format!(
                "task-set authority length {authority_length} exceeds the configured budget {maximum_authority_bytes}"
            )));
        }
        let expected_authority_end = authority_offset
            .checked_add(1 + 8 + 32)
            .and_then(|offset| offset.checked_add(authority_length))
            .ok_or_else(|| CdfError::data("task-set authority bounds overflowed u64"))?;
        if expected_authority_end != footer_offset {
            return Err(CdfError::data(
                "task-set authority frame does not end at the canonical footer",
            ));
        }
        let mut expected_authority_digest = [0_u8; 32];
        tail.read_exact(&mut expected_authority_digest)
            .map_err(|error| {
                artifact_io_error("read task-set authority digest", &cursor.path, error)
            })?;
        let authority_request = ReservationRequest::new(
            ConsumerKey::new("external-task-set-authority", MemoryClass::Control)?,
            authority_length,
        )?;
        let authority_lease = reserve_blocking(Arc::clone(&memory), &authority_request)?;
        let mut authority = vec![
            0_u8;
            usize::try_from(authority_length).map_err(|_| {
                CdfError::data("task-set authority exceeds addressable memory")
            })?
        ];
        tail.read_exact(&mut authority)
            .map_err(|error| artifact_io_error("read task-set authority", &cursor.path, error))?;
        let observed_authority_digest: [u8; 32] = Sha256::digest(&authority).into();
        if observed_authority_digest != expected_authority_digest {
            return Err(CdfError::data(
                "task-set shared authority does not match its content identity",
            ));
        }
        Ok(ExternalTaskSetReader {
            reference,
            cursor,
            expected_ordinal: 0,
            maximum_task_bytes,
            memory,
            authority: Arc::new(AccountedBytes::new(
                Bytes::from(authority),
                authority_lease,
            )?),
            authority_sha256: format!("sha256:{}", hex::encode(observed_authority_digest)),
            task_end: authority_offset,
            footer_task_count,
            finished: false,
        })
    }
}

pub struct ExternalTaskSetWriter {
    store: ExternalTaskStore,
    task_type: String,
    limits: TaskSetLimits,
    temporary: Option<NamedTempFile>,
    writer: Option<BufWriter<HashingWriter>>,
    payload: Vec<u8>,
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
        self.push_checked(canonical_ordinal, encode)
    }

    fn push_checked(
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
        let mut hashing = DigestingWriter::new(&mut bounded);
        encode(&mut hashing)?;
        let payload_digest = hashing.finalize();
        if self.payload.is_empty() {
            return Err(CdfError::data("canonical task payload cannot be empty"));
        }
        let payload_length = u64::try_from(self.payload.len())
            .map_err(|_| CdfError::data("canonical task payload exceeds u64"))?;
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

    pub fn finalize(
        self,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetArtifact> {
        self.finalize_checked(None, None, encode_authority)
    }

    pub fn finalize_with_authority_hash(
        self,
        expected_authority_sha256: &str,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetArtifact> {
        cdf_runtime::validate_artifact_hash(
            "expected task-set authority",
            expected_authority_sha256,
        )?;
        self.finalize_checked(None, Some(expected_authority_sha256), encode_authority)
    }

    pub fn finalize_with_authority_hash_and_cancellation(
        self,
        expected_authority_sha256: &str,
        cancellation: &RunCancellation,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetArtifact> {
        cdf_runtime::validate_artifact_hash(
            "expected task-set authority",
            expected_authority_sha256,
        )?;
        cancellation.check()?;
        self.finalize_checked(
            Some(cancellation),
            Some(expected_authority_sha256),
            encode_authority,
        )
    }

    fn finalize_checked(
        mut self,
        cancellation: Option<&RunCancellation>,
        expected_authority_sha256: Option<&str>,
        encode_authority: impl FnOnce(&mut dyn Write) -> Result<()>,
    ) -> Result<ExternalTaskSetArtifact> {
        if self.poisoned {
            return Err(CdfError::contract(
                "task-set writer cannot finalize after a partial write failure",
            ));
        }
        self.payload.clear();
        let maximum_authority_bytes = usize::try_from(self.limits.maximum_authority_bytes)
            .map_err(|_| CdfError::contract("task-set authority budget exceeds usize"))?;
        encode_authority(&mut BoundedVec::new(
            &mut self.payload,
            maximum_authority_bytes,
        ))?;
        if self.payload.is_empty() {
            return Err(CdfError::data(
                "task-set shared authority payload cannot be empty",
            ));
        }
        let authority_length = u64::try_from(self.payload.len())
            .map_err(|_| CdfError::data("task-set authority payload exceeds u64"))?;
        let authority_digest: [u8; 32] = Sha256::digest(&self.payload).into();
        let authority_sha256 = format!("sha256:{}", hex::encode(authority_digest));
        if expected_authority_sha256.is_some_and(|expected| expected != authority_sha256) {
            return Err(CdfError::data(
                "encoded task-set authority does not match its typed content identity",
            ));
        }

        self.writer_mut()?
            .flush()
            .map_err(|error| io_error("flush task-set body", self.temporary_path(), error))?;
        let authority_offset = self.writer_mut()?.get_ref().bytes;
        let tail_bytes = 1_u64
            .checked_add(8 + 32)
            .and_then(|bytes| bytes.checked_add(authority_length))
            .and_then(|bytes| bytes.checked_add(FOOTER_BYTES))
            .ok_or_else(|| CdfError::data("task-set trailer length overflowed u64"))?;
        self.reserve_spill(tail_bytes)?;
        self.write_unreserved(&[AUTHORITY_TAG], "write task-set authority tag")?;
        self.write_unreserved(
            &authority_length.to_be_bytes(),
            "write task-set authority length",
        )?;
        self.write_unreserved(&authority_digest, "write task-set authority digest")?;
        let payload = std::mem::take(&mut self.payload);
        let result = self.write_unreserved(&payload, "write task-set authority payload");
        self.payload = payload;
        result?;
        self.write_unreserved(&[FOOTER_TAG], "write task-set footer tag")?;
        self.write_unreserved(
            &self.next_ordinal.to_be_bytes(),
            "write task-set footer count",
        )?;
        self.write_unreserved(
            &authority_offset.to_be_bytes(),
            "write task-set authority offset",
        )?;
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
        if let Some(cancellation) = cancellation {
            cancellation.check()?;
        }
        let byte_count = hashing.bytes;
        let digest = format!("sha256:{}", hex::encode(hashing.hasher.finalize()));
        drop(hashing.file);

        let hex_digest = digest.trim_start_matches("sha256:");
        let object_key_text = format!("task-sets/sha256/{hex_digest}.cdftasks");
        let object_key = ContentObjectKey::new(object_key_text.clone())?;
        let final_path = self.store.object_path(&object_key_text);
        let reference = PlannedTaskSetReference {
            version: PLANNED_TASK_SET_REFERENCE_VERSION,
            task_type: self.task_type.clone(),
            task_count: self.next_ordinal,
            store_namespace: self.store.namespace().clone(),
            object_key,
            byte_count,
            content_sha256: digest.clone(),
            provider_generation: ContentProviderGeneration::new(digest.clone())?,
        };
        reference.validate()?;
        let temporary = self
            .temporary
            .take()
            .ok_or_else(|| CdfError::contract("task-set temporary file is missing"))?;
        install_content_addressed(temporary, &final_path, byte_count, &digest)?;

        if let Some(mut reservation) = self.spill_reservation.take() {
            reservation.shrink(reservation.bytes());
        }
        Ok(ExternalTaskSetArtifact {
            task_type: self.task_type,
            task_count: self.next_ordinal,
            authority_sha256,
            reference,
            path: final_path,
        })
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

    fn write_unreserved(&mut self, bytes: &[u8], action: &str) -> Result<()> {
        let path = self.temporary_path().to_path_buf();
        if let Err(error) = self.writer_mut()?.write_all(bytes) {
            self.poisoned = true;
            return Err(io_error(action, &path, error));
        }
        Ok(())
    }

    fn write_reserved(&mut self, bytes: &[u8]) -> Result<()> {
        let length = u64::try_from(bytes.len())
            .map_err(|_| CdfError::data("task-set write length exceeds u64"))?;
        self.reserve_spill(length)?;
        self.write_unreserved(bytes, "write task-set artifact")
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

struct ExternalTaskSetReadCursor {
    file: File,
    path: PathBuf,
    hasher: Sha256,
    observed_bytes: u64,
}

pub struct ExternalTaskSetReader {
    reference: PlannedTaskSetReference,
    cursor: ExternalTaskSetReadCursor,
    expected_ordinal: u64,
    maximum_task_bytes: u64,
    memory: Arc<dyn MemoryCoordinator>,
    authority: Arc<AccountedBytes>,
    authority_sha256: String,
    task_end: u64,
    footer_task_count: u64,
    finished: bool,
}

impl ExternalTaskSetReader {
    pub fn authority(&self) -> &AccountedBytes {
        self.authority.as_ref()
    }

    pub fn retained_authority(&self) -> Arc<AccountedBytes> {
        Arc::clone(&self.authority)
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
        if self.cursor.observed_bytes == self.task_end {
            return self.finish_tail();
        }
        if self.cursor.observed_bytes > self.task_end {
            return Err(CdfError::data(
                "task-set task body crossed the authority boundary",
            ));
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
                let remaining = self
                    .task_end
                    .checked_sub(self.cursor.observed_bytes)
                    .ok_or_else(|| {
                        CdfError::data("task-set task frame crossed the authority boundary")
                    })?;
                if payload_length > remaining {
                    return Err(CdfError::data(
                        "task-set task payload crosses the authority boundary",
                    ));
                }
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
            other => Err(CdfError::data(format!(
                "task-set task body contains unknown frame tag {other}"
            ))),
        }
    }

    pub fn observed_task_count(&self) -> u64 {
        self.expected_ordinal
    }

    fn finish_tail(&mut self) -> Result<Option<ExternalTaskRecord>> {
        if self.expected_ordinal != self.footer_task_count {
            return Err(CdfError::data(format!(
                "task-set footer count {} does not match {} observed records",
                self.footer_task_count, self.expected_ordinal
            )));
        }
        if self.read_array::<1>()?[0] != AUTHORITY_TAG {
            return Err(CdfError::data("task-set authority tag changed"));
        }
        let authority_length = u64::from_be_bytes(self.read_array::<8>()?);
        let retained_authority_length = self.authority.payload().len();
        if authority_length
            != u64::try_from(retained_authority_length)
                .map_err(|_| CdfError::data("task-set authority exceeds u64"))?
        {
            return Err(CdfError::data("task-set authority length changed"));
        }
        let authority_digest = self.read_array::<32>()?;
        if format!("sha256:{}", hex::encode(authority_digest)) != self.authority_sha256 {
            return Err(CdfError::data("task-set authority identity changed"));
        }
        let authority = self.read_vec(
            usize::try_from(authority_length)
                .map_err(|_| CdfError::data("task-set authority exceeds addressable memory"))?,
        )?;
        if authority.as_slice() != self.authority.payload() {
            return Err(CdfError::data("task-set authority payload changed"));
        }
        if self.read_array::<1>()?[0] != FOOTER_TAG {
            return Err(CdfError::data("task-set footer tag changed"));
        }
        let record_count = u64::from_be_bytes(self.read_array::<8>()?);
        let authority_offset = u64::from_be_bytes(self.read_array::<8>()?);
        if record_count != self.footer_task_count || authority_offset != self.task_end {
            return Err(CdfError::data("task-set footer authority changed"));
        }
        let mut trailing = [0_u8; 1];
        match self.cursor.file.read(&mut trailing) {
            Ok(0) => {}
            Ok(_) => return Err(CdfError::data("task-set artifact has trailing bytes")),
            Err(error) => {
                return Err(artifact_io_error(
                    "read task-set trailing byte",
                    &self.cursor.path,
                    error,
                ));
            }
        }
        self.verify_complete()?;
        self.finished = true;
        Ok(None)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.cursor.read_array()
    }

    fn read_vec(&mut self, length: usize) -> Result<Vec<u8>> {
        self.cursor.read_vec(length)
    }

    fn verify_complete(&self) -> Result<()> {
        let observed_digest = format!(
            "sha256:{}",
            hex::encode(self.cursor.hasher.clone().finalize())
        );
        if self.cursor.observed_bytes != self.reference.byte_count
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

impl ExternalTaskSetReadCursor {
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut bytes = [0_u8; N];
        self.file
            .read_exact(&mut bytes)
            .map_err(|error| artifact_io_error("read task-set artifact", &self.path, error))?;
        self.observe(&bytes)?;
        Ok(bytes)
    }

    fn read_vec(&mut self, length: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0_u8; length];
        self.file
            .read_exact(&mut bytes)
            .map_err(|error| artifact_io_error("read task-set artifact", &self.path, error))?;
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
}

#[derive(Clone, Debug)]
pub struct ExternalTaskRecord {
    pub canonical_ordinal: u64,
    pub content_sha256: String,
    pub payload: AccountedBytes,
}

pub(crate) struct BoundedVec<'a> {
    bytes: &'a mut Vec<u8>,
    maximum: usize,
}

impl<'a> BoundedVec<'a> {
    pub(crate) fn new(bytes: &'a mut Vec<u8>, maximum: usize) -> Self {
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

pub(crate) struct DigestingWriter<'a> {
    output: &'a mut dyn Write,
    hasher: Sha256,
}

impl<'a> DigestingWriter<'a> {
    pub(crate) fn new(output: &'a mut dyn Write) -> Self {
        Self {
            output,
            hasher: Sha256::new(),
        }
    }

    pub(crate) fn finalize(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

impl Write for DigestingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.output.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
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
            .checked_add(u64::try_from(written).map_err(|_| {
                io::Error::other(CdfError::internal("task-set byte count exceeds u64"))
            })?)
            .ok_or_else(|| io::Error::other(CdfError::internal("task-set byte count overflow")))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
