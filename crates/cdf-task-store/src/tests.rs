use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;

use cdf_kernel::{CdfError, ContentStoreNamespace, Result};
use cdf_memory::{DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator};
use cdf_runtime::{FixedSpillBudget, RunCancellation, SpillBudgetCoordinator};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use super::*;
use crate::limits::task_writer_memory_requirements;
use crate::publication::artifact_io_error;
use crate::sqlite_capacity::{SQLITE_PAGE_BYTES, sqlite_error};

#[test]
fn sqlite_host_failure_is_environment_but_scratch_invariant_is_internal() {
    let host = sqlite_error(
        "open canonical task index",
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
            None,
        ),
    );
    assert_eq!(host.kind, cdf_kernel::ErrorKind::Environment);
    assert!(host.message.contains("temporary storage"));

    let invariant = sqlite_error("decode canonical task row", rusqlite::Error::InvalidQuery);
    assert_eq!(invariant.kind, cdf_kernel::ErrorKind::Internal);
}

#[test]
fn task_artifact_io_separates_missing_data_from_host_failure() {
    let path = Path::new("tasks.cdf");
    let missing = artifact_io_error(
        "open task-set artifact",
        path,
        io::Error::new(io::ErrorKind::NotFound, "missing"),
    );
    assert_eq!(missing.kind, cdf_kernel::ErrorKind::Data);

    let directory = artifact_io_error(
        "read task-set artifact",
        path,
        io::Error::new(io::ErrorKind::IsADirectory, "is a directory"),
    );
    assert_eq!(directory.kind, cdf_kernel::ErrorKind::Data);

    let host = artifact_io_error(
        "open task-set artifact",
        path,
        io::Error::new(io::ErrorKind::PermissionDenied, "denied"),
    );
    assert_eq!(host.kind, cdf_kernel::ErrorKind::Environment);
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SyntheticTask {
    partition: u64,
    path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SyntheticAuthority {
    version: u32,
}

#[derive(Default)]
struct SyntheticCodec {
    authority_hash_override: Option<String>,
    task_hash_override: Option<String>,
}

impl ExternalTaskSetCodec for SyntheticCodec {
    type Authority = SyntheticAuthority;
    type Task = SyntheticTask;

    fn decode_authority(&self, payload: &[u8]) -> Result<Self::Authority> {
        let authority: SyntheticAuthority = serde_json::from_slice(payload)
            .map_err(|error| CdfError::data(format!("decode synthetic authority: {error}")))?;
        if authority.version != 1 {
            return Err(CdfError::data(
                "synthetic authority has an unsupported version",
            ));
        }
        Ok(authority)
    }

    fn authority_content_sha256(&self, authority: &Self::Authority) -> Result<String> {
        if let Some(hash) = &self.authority_hash_override {
            return Ok(hash.clone());
        }
        canonical_json_hash(authority)
    }

    fn decode_task(&self, payload: &[u8], authority: &Self::Authority) -> Result<Self::Task> {
        if authority.version != 1 {
            return Err(CdfError::data(
                "synthetic task authority changed during decode",
            ));
        }
        serde_json::from_slice(payload)
            .map_err(|error| CdfError::data(format!("decode synthetic task: {error}")))
    }

    fn task_canonical_ordinal(&self, task: &Self::Task) -> u64 {
        task.partition
    }

    fn encode_task(&self, task: &Self::Task, output: &mut dyn Write) -> Result<()> {
        serde_json::to_writer(&mut *output, task)
            .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))?;
        if let Some(suffix) = &self.task_hash_override {
            output.write_all(suffix.as_bytes()).map_err(|error| {
                CdfError::data(format!("encode synthetic task identity suffix: {error}"))
            })?;
        }
        Ok(())
    }
}

impl ExternalTaskPlanningCodec for SyntheticCodec {
    fn set_task_canonical_ordinal(&self, task: &mut Self::Task, ordinal: u64) {
        task.partition = ordinal;
    }

    fn encode_authority(&self, authority: &Self::Authority, output: &mut dyn Write) -> Result<()> {
        serde_json::to_writer(output, authority)
            .map_err(|error| CdfError::data(format!("encode synthetic authority: {error}")))
    }
}

struct RejectDecodedTaskCodec;

impl ExternalTaskSetCodec for RejectDecodedTaskCodec {
    type Authority = SyntheticAuthority;
    type Task = SyntheticTask;

    fn decode_authority(&self, payload: &[u8]) -> Result<Self::Authority> {
        serde_json::from_slice(payload)
            .map_err(|error| CdfError::data(format!("decode synthetic authority: {error}")))
    }

    fn authority_content_sha256(&self, authority: &Self::Authority) -> Result<String> {
        canonical_json_hash(authority)
    }

    fn decode_task(&self, _payload: &[u8], _authority: &Self::Authority) -> Result<Self::Task> {
        Err(CdfError::data("synthetic malformed planning record"))
    }

    fn task_canonical_ordinal(&self, task: &Self::Task) -> u64 {
        task.partition
    }

    fn encode_task(&self, task: &Self::Task, output: &mut dyn Write) -> Result<()> {
        serde_json::to_writer(output, task)
            .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
    }
}

impl ExternalTaskPlanningCodec for RejectDecodedTaskCodec {
    fn set_task_canonical_ordinal(&self, task: &mut Self::Task, ordinal: u64) {
        task.partition = ordinal;
    }

    fn encode_authority(&self, authority: &Self::Authority, output: &mut dyn Write) -> Result<()> {
        serde_json::to_writer(output, authority)
            .map_err(|error| CdfError::data(format!("encode synthetic authority: {error}")))
    }
}

fn canonical_json_hash(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| CdfError::data(format!("encode synthetic model: {error}")))?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

fn typed_config(task_type: &str) -> TypedExternalTaskSetReaderConfig {
    TypedExternalTaskSetReaderConfig::new(
        task_type,
        4096,
        4096,
        ExternalTaskParseMemory::blocking(
            "synthetic-authority-parse",
            MemoryClass::Control,
            10_000,
            0,
        )
        .unwrap(),
        ExternalTaskParseMemory::blocking("synthetic-task-parse", MemoryClass::Control, 10_000, 0)
            .unwrap(),
    )
    .unwrap()
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

#[test]
fn unavailable_temporary_directory_is_environment_owned() {
    let root = TempDir::new().unwrap();
    fs::write(root.path().join("planner-artifacts"), b"not a directory").unwrap();
    let store = ExternalTaskStore::new(
        root.path(),
        ContentStoreNamespace::new("planner-artifacts").unwrap(),
    )
    .unwrap();

    let error = match store.temporary_workspace("audit") {
        Ok(_) => panic!("a file in place of the temporary root must fail"),
        Err(error) => error,
    };

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Environment);
    assert!(error.message.contains("temporary"));
    assert!(error.message.contains("process file limits"));
}

fn limits() -> TaskSetLimits {
    TaskSetLimits {
        maximum_task_bytes: 4096,
        maximum_authority_bytes: 4096,
        writer_buffer_bytes: 8192,
    }
}

fn canonical_limits() -> CanonicalTaskSetLimits {
    CanonicalTaskSetLimits {
        tasks: limits(),
        maximum_sort_key_bytes: 1024,
        index_cache_bytes: 16 * 1024,
        spill_growth_bytes: 16 * 1024,
        minimum_initial_spill_bytes: SQLITE_PAGE_BYTES * 2,
    }
}

fn encode_authority(output: &mut dyn Write) -> Result<()> {
    output
        .write_all(br#"{"version":1}"#)
        .map_err(|error| CdfError::data(format!("encode synthetic authority: {error}")))
}

fn push_task(writer: &mut ExternalTaskSetWriter, ordinal: u64, task: &SyntheticTask) -> Result<()> {
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
        .writer("synthetic-v1", limits(), Arc::clone(&memory), &spill)
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
    let artifact = writer.finalize(encode_authority).unwrap();
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
fn typed_reader_retains_one_accounted_authority_and_task_lifecycle() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
    let mut writer = store
        .writer("synthetic-v1", limits(), Arc::clone(&memory), &spill)
        .unwrap();
    let task = SyntheticTask {
        partition: 0,
        path: "file:///zero.parquet".to_owned(),
    };
    push_task(&mut writer, 0, &task).unwrap();
    let artifact = writer.finalize(encode_authority).unwrap();
    assert_eq!(memory.snapshot().current_bytes, 0);

    let authority_bytes = u64::try_from(br#"{"version":1}"#.len()).unwrap();
    let task_bytes = u64::try_from(serde_json::to_vec(&task).unwrap().len()).unwrap();
    let mut reader = TypedExternalTaskSetReader::open(
        &store,
        artifact.reference,
        Arc::clone(&memory),
        RunCancellation::default(),
        typed_config("synthetic-v1"),
        SyntheticCodec::default(),
    )
    .unwrap();
    assert_eq!(reader.authority(), &SyntheticAuthority { version: 1 });
    assert_eq!(
        memory.snapshot().current_bytes,
        authority_bytes * 2,
        "authority encoded and parse memory must each be leased once"
    );

    let retained = reader.next_task(0).unwrap().unwrap();
    assert_eq!(retained.task(), &task);
    assert_eq!(retained.canonical_ordinal(), 0);
    assert_eq!(
        retained.content_sha256(),
        canonical_json_hash(&task).unwrap()
    );
    assert_eq!(retained.retained_bytes(), task_bytes * 2);
    assert_eq!(
        memory.snapshot().current_bytes,
        authority_bytes * 2 + task_bytes * 2
    );
    let retained_task_address = std::ptr::from_ref(retained.task());
    let retained_clone = retained.clone();
    assert_eq!(
        std::ptr::from_ref(retained_clone.task()),
        retained_task_address,
        "scheduler lookahead clones must share one decoded task model"
    );
    assert!(reader.next_task(1).unwrap().is_none());
    drop(reader);
    assert_eq!(
        memory.snapshot().current_bytes,
        authority_bytes * 2 + task_bytes * 2,
        "retained task must keep its one shared authority alive"
    );
    drop(retained);
    assert_eq!(
        memory.snapshot().current_bytes,
        authority_bytes * 2 + task_bytes * 2,
        "one clone must retain the singular authority/task leases"
    );
    drop(retained_clone);
    assert_eq!(memory.snapshot().current_bytes, 0);
}

#[test]
fn typed_reader_fails_closed_on_type_authority_ordinal_content_decode_and_cancellation() {
    fn artifact_with(
        store: &ExternalTaskStore,
        memory: Arc<dyn MemoryCoordinator>,
        spill: &dyn SpillBudgetCoordinator,
        encode: impl FnOnce(&mut ExternalTaskSetWriter) -> Result<()>,
    ) -> ExternalTaskSetArtifact {
        let mut writer = store
            .writer("synthetic-v1", limits(), memory, spill)
            .unwrap();
        encode(&mut writer).unwrap();
        writer.finalize(encode_authority).unwrap()
    }

    let task = SyntheticTask {
        partition: 0,
        path: "file:///zero.parquet".to_owned(),
    };

    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
    let artifact = artifact_with(&store, Arc::clone(&memory), &spill, |writer| {
        push_task(writer, 0, &task)
    });
    let error = TypedExternalTaskSetReader::open(
        &store,
        artifact.reference.clone(),
        Arc::clone(&memory),
        RunCancellation::default(),
        typed_config("wrong-v1"),
        SyntheticCodec::default(),
    )
    .err()
    .unwrap();
    assert!(error.message.contains("type"));

    let error = TypedExternalTaskSetReader::open(
        &store,
        artifact.reference.clone(),
        Arc::clone(&memory),
        RunCancellation::default(),
        typed_config("synthetic-v1"),
        SyntheticCodec {
            authority_hash_override: Some(format!("sha256:{}", "00".repeat(32))),
            task_hash_override: None,
        },
    )
    .err()
    .unwrap();
    assert!(error.message.contains("authority"));

    let mut reader = TypedExternalTaskSetReader::open(
        &store,
        artifact.reference.clone(),
        Arc::clone(&memory),
        RunCancellation::default(),
        typed_config("synthetic-v1"),
        SyntheticCodec::default(),
    )
    .unwrap();
    assert!(
        reader
            .next_task(1)
            .err()
            .unwrap()
            .message
            .contains("execution requested")
    );
    drop(reader);

    let mut wrong_content = TypedExternalTaskSetReader::open(
        &store,
        artifact.reference.clone(),
        Arc::clone(&memory),
        RunCancellation::default(),
        typed_config("synthetic-v1"),
        SyntheticCodec {
            authority_hash_override: None,
            task_hash_override: Some(format!("sha256:{}", "11".repeat(32))),
        },
    )
    .unwrap();
    assert!(
        wrong_content
            .next_task(0)
            .err()
            .unwrap()
            .message
            .contains("ordinal or content")
    );
    drop(wrong_content);

    let wrong_ordinal = SyntheticTask {
        partition: 9,
        path: "file:///nine.parquet".to_owned(),
    };
    let ordinal_artifact = artifact_with(&store, Arc::clone(&memory), &spill, |writer| {
        push_task(writer, 0, &wrong_ordinal)
    });
    let mut ordinal_reader = TypedExternalTaskSetReader::open(
        &store,
        ordinal_artifact.reference,
        Arc::clone(&memory),
        RunCancellation::default(),
        typed_config("synthetic-v1"),
        SyntheticCodec::default(),
    )
    .unwrap();
    assert!(
        ordinal_reader
            .next_task(0)
            .err()
            .unwrap()
            .message
            .contains("ordinal or content")
    );
    drop(ordinal_reader);

    let decode_artifact = artifact_with(&store, Arc::clone(&memory), &spill, |writer| {
        writer.push_with(0, |output| {
            output
                .write_all(b"not-json")
                .map_err(|error| CdfError::data(format!("write invalid task: {error}")))
        })
    });
    let mut decode_reader = TypedExternalTaskSetReader::open(
        &store,
        decode_artifact.reference,
        Arc::clone(&memory),
        RunCancellation::default(),
        typed_config("synthetic-v1"),
        SyntheticCodec::default(),
    )
    .unwrap();
    assert!(
        decode_reader
            .next_task(0)
            .err()
            .unwrap()
            .message
            .contains("decode synthetic task")
    );
    drop(decode_reader);

    let cancellation = RunCancellation::default();
    let mut cancelled_reader = TypedExternalTaskSetReader::open(
        &store,
        artifact.reference,
        Arc::clone(&memory),
        cancellation.clone(),
        typed_config("synthetic-v1"),
        SyntheticCodec::default(),
    )
    .unwrap();
    cancellation.cancel();
    assert!(
        cancelled_reader
            .next_task(0)
            .err()
            .unwrap()
            .message
            .contains("cancelled")
    );
    drop(cancelled_reader);
    assert_eq!(
        memory.snapshot().current_bytes,
        0,
        "every failed/cancelled decode must release encoded and parse leases"
    );
}

#[test]
fn typed_reader_parse_policy_rejects_overflow() {
    let policy = ExternalTaskParseMemory::fail_fast(
        "synthetic-overflow",
        MemoryClass::Discovery,
        u32::MAX,
        u64::MAX,
    )
    .unwrap();
    assert!(
        policy
            .reservation_bytes(u64::MAX)
            .unwrap_err()
            .message
            .contains("u64")
    );
}

#[test]
fn typed_reader_fail_fast_pressure_and_cancellation_never_wait() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (writer_memory, spill) = authorities(64 * 1024, 1024 * 1024);
    let task = SyntheticTask {
        partition: 0,
        path: "file:///pressure.parquet".to_owned(),
    };
    let mut writer = store
        .writer("synthetic-v1", limits(), Arc::clone(&writer_memory), &spill)
        .unwrap();
    push_task(&mut writer, 0, &task).unwrap();
    let artifact = writer.finalize(encode_authority).unwrap();

    let authority_bytes = u64::try_from(br#"{"version":1}"#.len()).unwrap();
    let task_bytes = u64::try_from(serde_json::to_vec(&task).unwrap().len()).unwrap();
    let constrained_bytes = authority_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(task_bytes))
        .unwrap();
    let constrained: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(constrained_bytes, BTreeMap::new()).unwrap());
    let fail_fast_config = || {
        TypedExternalTaskSetReaderConfig::new(
            "synthetic-v1",
            4096,
            4096,
            ExternalTaskParseMemory::fail_fast(
                "synthetic-authority-parse",
                MemoryClass::Discovery,
                10_000,
                0,
            )
            .unwrap(),
            ExternalTaskParseMemory::fail_fast(
                "synthetic-task-parse",
                MemoryClass::Discovery,
                10_000,
                0,
            )
            .unwrap(),
        )
        .unwrap()
    };

    let mut pressure_reader = TypedExternalTaskSetReader::open(
        &store,
        artifact.reference.clone(),
        Arc::clone(&constrained),
        RunCancellation::default(),
        fail_fast_config(),
        SyntheticCodec::default(),
    )
    .unwrap();
    let error = pressure_reader.next_task(0).err().unwrap();
    assert!(error.message.contains("cannot admit"));
    drop(pressure_reader);
    assert_eq!(constrained.snapshot().current_bytes, 0);

    let cancellation = RunCancellation::default();
    let mut cancelled_reader = TypedExternalTaskSetReader::open(
        &store,
        artifact.reference,
        Arc::clone(&constrained),
        cancellation.clone(),
        fail_fast_config(),
        SyntheticCodec::default(),
    )
    .unwrap();
    cancellation.cancel();
    let error = cancelled_reader.next_task(0).err().unwrap();
    assert!(error.message.contains("cancelled"));
    drop(cancelled_reader);
    assert_eq!(constrained.snapshot().current_bytes, 0);
}

#[test]
fn typed_ordered_and_spill_sorted_builders_publish_one_canonical_identity() {
    let ordered_root = TempDir::new().unwrap();
    let canonical_root = TempDir::new().unwrap();
    let authority = SyntheticAuthority { version: 1 };

    let ordered_store = store(&ordered_root);
    let (ordered_memory, ordered_spill) = authorities(64 * 1024, 1024 * 1024);
    let mut ordered = TypedExternalTaskSetBuilder::new(
        &ordered_store,
        "synthetic-v1",
        limits(),
        Arc::clone(&ordered_memory),
        &ordered_spill,
        RunCancellation::default(),
        SyntheticCodec::default(),
    )
    .unwrap();
    for path in ["s3://bucket/a", "s3://bucket/b"] {
        ordered
            .push(&mut SyntheticTask {
                partition: u64::MAX,
                path: path.to_owned(),
            })
            .unwrap();
    }
    let ordered_artifact = ordered.finalize(&authority).unwrap();

    let canonical_store = store(&canonical_root);
    let (canonical_memory, canonical_spill) = authorities(256 * 1024, 1024 * 1024);
    let canonical_spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(canonical_spill);
    let mut canonical = TypedCanonicalTaskSetBuilder::new(
        &canonical_store,
        "synthetic-v1",
        canonical_limits(),
        Arc::clone(&canonical_memory),
        Arc::clone(&canonical_spill),
        RunCancellation::default(),
        SyntheticCodec::default(),
    )
    .unwrap();
    for path in ["s3://bucket/b", "s3://bucket/a"] {
        let task = SyntheticTask {
            partition: u64::MAX,
            path: path.to_owned(),
        };
        assert!(
            canonical
                .push_idempotent_by(task, |task| task.path.as_bytes())
                .unwrap()
        );
    }
    let canonical_artifact = canonical.finalize(&authority).unwrap();

    assert_eq!(ordered_artifact.reference, canonical_artifact.reference);
    for (store, reference, memory) in [
        (
            &ordered_store,
            ordered_artifact.reference,
            Arc::clone(&ordered_memory),
        ),
        (
            &canonical_store,
            canonical_artifact.reference,
            Arc::clone(&canonical_memory),
        ),
    ] {
        let mut reader = TypedExternalTaskSetReader::open(
            store,
            reference,
            Arc::clone(&memory),
            RunCancellation::default(),
            typed_config("synthetic-v1"),
            SyntheticCodec::default(),
        )
        .unwrap();
        for (ordinal, path) in ["s3://bucket/a", "s3://bucket/b"].into_iter().enumerate() {
            let task = reader
                .next_task(u64::try_from(ordinal).unwrap())
                .unwrap()
                .unwrap();
            assert_eq!(task.task().path, path);
        }
        assert!(reader.next_task(2).unwrap().is_none());
    }
    assert_eq!(ordered_memory.snapshot().current_bytes, 0);
    assert_eq!(ordered_spill.snapshot().current_bytes, 0);
    assert_eq!(canonical_memory.snapshot().current_bytes, 0);
    assert_eq!(canonical_spill.snapshot().current_bytes, 0);
}

#[test]
fn planning_workspace_and_builder_failures_release_every_authority() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (memory, spill) = authorities(64 * 1024, 64 * 1024);
    let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
    let workspace = store
        .accounted_workspace(
            "synthetic-index",
            ExternalTaskWorkspaceLimits::new(
                "synthetic-index",
                MemoryClass::Control,
                8192,
                8192,
                8192,
            )
            .unwrap(),
            Arc::clone(&memory),
            Arc::clone(&spill),
        )
        .unwrap();
    let workspace_path = workspace.path().to_path_buf();
    assert_eq!(memory.snapshot().current_bytes, 8192);
    assert_eq!(spill.snapshot().current_bytes, 8192);
    drop(workspace);
    assert_eq!(memory.snapshot().current_bytes, 0);
    assert_eq!(spill.snapshot().current_bytes, 0);
    assert!(!workspace_path.exists());

    let constrained_spill: Arc<dyn SpillBudgetCoordinator> =
        Arc::new(FixedSpillBudget::new(4096).unwrap());
    assert!(
        store
            .accounted_workspace(
                "constrained-index",
                ExternalTaskWorkspaceLimits::new(
                    "constrained-index",
                    MemoryClass::Control,
                    8192,
                    8192,
                    8192,
                )
                .unwrap(),
                Arc::clone(&memory),
                Arc::clone(&constrained_spill),
            )
            .err()
            .unwrap()
            .message
            .contains("free spill")
    );
    assert_eq!(memory.snapshot().current_bytes, 0);
    assert_eq!(constrained_spill.snapshot().current_bytes, 0);

    let cancellation = RunCancellation::default();
    let mut cancelled = TypedExternalTaskSetBuilder::new(
        &store,
        "synthetic-v1",
        limits(),
        Arc::clone(&memory),
        spill.as_ref(),
        cancellation.clone(),
        SyntheticCodec::default(),
    )
    .unwrap();
    cancellation.cancel();
    assert!(
        cancelled
            .push(&mut SyntheticTask {
                partition: 0,
                path: "s3://bucket/cancelled".to_owned(),
            })
            .err()
            .unwrap()
            .message
            .contains("cancelled")
    );
    drop(cancelled);
    assert_eq!(memory.snapshot().current_bytes, 0);
    assert_eq!(spill.snapshot().current_bytes, 0);

    let mut mismatched = TypedExternalTaskSetBuilder::new(
        &store,
        "synthetic-v1",
        limits(),
        Arc::clone(&memory),
        spill.as_ref(),
        RunCancellation::default(),
        SyntheticCodec {
            authority_hash_override: Some(format!("sha256:{}", "00".repeat(32))),
            task_hash_override: None,
        },
    )
    .unwrap();
    mismatched
        .push(&mut SyntheticTask {
            partition: 0,
            path: "s3://bucket/mismatch".to_owned(),
        })
        .unwrap();
    assert!(
        mismatched
            .finalize(&SyntheticAuthority { version: 1 })
            .err()
            .unwrap()
            .message
            .contains("typed content identity")
    );
    assert_eq!(memory.snapshot().current_bytes, 0);
    assert_eq!(spill.snapshot().current_bytes, 0);
    let task_directory = root.path().join("planner-artifacts/task-sets");
    assert_eq!(fs::read_dir(task_directory).unwrap().count(), 0);
}

#[test]
fn malformed_canonical_record_and_empty_inventory_are_fail_closed_and_deterministic() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (memory, spill) = authorities(256 * 1024, 1024 * 1024);
    let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
    let mut malformed = TypedCanonicalTaskSetBuilder::new(
        &store,
        "synthetic-v1",
        canonical_limits(),
        Arc::clone(&memory),
        Arc::clone(&spill),
        RunCancellation::default(),
        RejectDecodedTaskCodec,
    )
    .unwrap();
    malformed
        .push_idempotent_by(
            SyntheticTask {
                partition: 0,
                path: "s3://bucket/malformed".to_owned(),
            },
            |_| b"malformed",
        )
        .unwrap();
    assert!(
        malformed
            .finalize(&SyntheticAuthority { version: 1 })
            .err()
            .unwrap()
            .message
            .contains("malformed planning record")
    );
    assert_eq!(memory.snapshot().current_bytes, 0);
    assert_eq!(spill.snapshot().current_bytes, 0);

    let empty = TypedCanonicalTaskSetBuilder::new(
        &store,
        "synthetic-v1",
        canonical_limits(),
        Arc::clone(&memory),
        Arc::clone(&spill),
        RunCancellation::default(),
        SyntheticCodec::default(),
    )
    .unwrap()
    .finalize(&SyntheticAuthority { version: 1 })
    .unwrap();
    assert_eq!(empty.task_count, 0);
    assert_eq!(empty.reference.task_count, 0);
    assert_eq!(memory.snapshot().current_bytes, 0);
    assert_eq!(spill.snapshot().current_bytes, 0);
}

#[test]
fn canonical_builder_admits_the_complete_finalize_overlap_once() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let limits = canonical_limits();
    let scratch_bytes = limits
        .index_cache_bytes
        .checked_add(limits.tasks.maximum_task_bytes * 2)
        .and_then(|bytes| bytes.checked_add(limits.maximum_sort_key_bytes * 2))
        .unwrap();
    let (_, writer_bytes) = task_writer_memory_requirements("synthetic-v1", &limits.tasks).unwrap();
    let combined_bytes = scratch_bytes + writer_bytes;

    let constrained: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(combined_bytes - 1, BTreeMap::new()).unwrap());
    let constrained_spill: Arc<dyn SpillBudgetCoordinator> =
        Arc::new(FixedSpillBudget::new(1024 * 1024).unwrap());
    assert!(
        TypedCanonicalTaskSetBuilder::new(
            &store,
            "synthetic-v1",
            limits.clone(),
            Arc::clone(&constrained),
            Arc::clone(&constrained_spill),
            RunCancellation::default(),
            SyntheticCodec::default(),
        )
        .err()
        .unwrap()
        .message
        .contains("exceeds managed budget")
    );
    assert_eq!(constrained.snapshot().current_bytes, 0);
    assert_eq!(constrained_spill.snapshot().current_bytes, 0);

    let admitted: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(combined_bytes, BTreeMap::new()).unwrap());
    let admitted_spill: Arc<dyn SpillBudgetCoordinator> =
        Arc::new(FixedSpillBudget::new(1024 * 1024).unwrap());
    let mut builder = TypedCanonicalTaskSetBuilder::new(
        &store,
        "synthetic-v1",
        limits,
        Arc::clone(&admitted),
        Arc::clone(&admitted_spill),
        RunCancellation::default(),
        SyntheticCodec::default(),
    )
    .unwrap();
    builder
        .push_idempotent_by(
            SyntheticTask {
                partition: 0,
                path: "s3://bucket/admitted".to_owned(),
            },
            |task| task.path.as_bytes(),
        )
        .unwrap();
    builder
        .finalize(&SyntheticAuthority { version: 1 })
        .unwrap();
    assert_eq!(admitted.snapshot().peak_bytes, combined_bytes);
    assert_eq!(admitted.snapshot().current_bytes, 0);
    assert_eq!(admitted_spill.snapshot().current_bytes, 0);
}

#[test]
fn canonical_builder_configured_spill_exhaustion_is_data_and_discards_scratch() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (memory, spill) = authorities(256 * 1024, 256 * 1024);
    let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
    let mut builder = store
        .canonical_builder(
            "synthetic-v1",
            canonical_limits(),
            Arc::clone(&memory),
            Arc::clone(&spill),
        )
        .unwrap();
    let first = SyntheticTask {
        partition: 0,
        path: format!("s3://bucket/{:08}/{}", 0, "x".repeat(1000)),
    };
    assert!(
        builder
            .push_idempotent_with(first.path.as_bytes(), |output| {
                serde_json::to_writer(output, &first)
                    .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
            })
            .unwrap()
    );
    let error = (1_u64..10_000)
        .find_map(|ordinal| {
            let task = SyntheticTask {
                partition: ordinal,
                path: format!("s3://bucket/{ordinal:08}/{}", "x".repeat(1000)),
            };
            builder
                .push_idempotent_with(task.path.as_bytes(), |output| {
                    serde_json::to_writer(output, &task)
                        .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
                })
                .err()
        })
        .expect("bounded spill must terminate canonical insertion");
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("spill"));
    assert!(
        !builder
            .push_idempotent_with(first.path.as_bytes(), |output| {
                serde_json::to_writer(output, &first)
                    .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
            })
            .unwrap(),
        "an exact duplicate must not need fresh spill admission"
    );
    let conflicting = SyntheticTask {
        partition: u64::MAX,
        path: first.path.clone(),
    };
    assert!(
        builder
            .push_idempotent_with(conflicting.path.as_bytes(), |output| {
                serde_json::to_writer(output, &conflicting)
                    .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
            })
            .unwrap_err()
            .message
            .contains("conflicting payloads"),
        "conflicting duplicate detection must not need fresh spill admission"
    );
    drop(builder);
    assert_eq!(memory.snapshot().current_bytes, 0);
    assert_eq!(spill.snapshot().current_bytes, 0);
    let scratch = root.path().join("planner-artifacts/scratch");
    assert_eq!(fs::read_dir(scratch).unwrap().count(), 0);
}

#[test]
fn canonical_builder_admits_multi_page_insert_before_sqlite_mutation() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (memory, spill) = authorities(512 * 1024, 1024 * 1024);
    let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
    let limits = CanonicalTaskSetLimits {
        tasks: TaskSetLimits {
            maximum_task_bytes: 64 * 1024,
            maximum_authority_bytes: 4096,
            writer_buffer_bytes: 8192,
        },
        ..canonical_limits()
    };
    let mut builder = store
        .canonical_builder(
            "multi-page-v1",
            limits,
            Arc::clone(&memory),
            Arc::clone(&spill),
        )
        .unwrap();
    let initial_reservation = builder.reserved_spill_bytes();
    assert_eq!(initial_reservation, 16 * 1024);
    let payload = vec![7_u8; 50 * 1024];

    builder
        .push_with(b"multi-page", |output| {
            output
                .write_all(&payload)
                .map_err(|error| CdfError::data(format!("encode multi-page task: {error}")))
        })
        .unwrap();

    assert!(builder.reserved_spill_bytes() > initial_reservation);
    assert_eq!(builder.task_count(), 1);
}

#[test]
fn canonical_builder_two_page_minimum_accepts_a_tiny_insert() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (memory, spill) = authorities(128 * 1024, SQLITE_PAGE_BYTES * 2);
    let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
    let limits = CanonicalTaskSetLimits {
        spill_growth_bytes: SQLITE_PAGE_BYTES * 2,
        minimum_initial_spill_bytes: SQLITE_PAGE_BYTES * 2,
        ..canonical_limits()
    };
    let mut builder = store
        .canonical_builder("tiny-v1", limits, Arc::clone(&memory), Arc::clone(&spill))
        .unwrap();

    builder
        .push_with(b"k", |output| {
            output
                .write_all(b"x")
                .map_err(|error| CdfError::data(format!("encode tiny task: {error}")))
        })
        .unwrap();

    assert_eq!(builder.reserved_spill_bytes(), SQLITE_PAGE_BYTES * 2);
    assert_eq!(builder.task_count(), 1);

    let oversized_local = vec![7_u8; 1200];
    let error = builder
        .push_with(b"overflow", |output| {
            output
                .write_all(&oversized_local)
                .map_err(|error| CdfError::data(format!("encode overflow task: {error}")))
        })
        .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("spill budget"));
    assert_eq!(builder.task_count(), 1);
}

#[test]
fn cancellation_during_empty_finalization_prevents_atomic_install() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
    let cancellation = RunCancellation::default();
    let writer = store
        .writer("synthetic-v1", limits(), Arc::clone(&memory), &spill)
        .unwrap();
    let error = writer
        .finalize_with_authority_hash_and_cancellation(
            writer_authority_hash(),
            &cancellation,
            |output| {
                encode_authority(output)?;
                cancellation.cancel();
                Ok(())
            },
        )
        .unwrap_err();
    assert!(error.message.contains("cancelled"));
    assert_eq!(memory.snapshot().current_bytes, 0);
    assert_eq!(spill.snapshot().current_bytes, 0);
    let task_directory = root.path().join("planner-artifacts/task-sets");
    assert_eq!(fs::read_dir(task_directory).unwrap().count(), 0);
}

#[test]
fn provider_order_is_externalized_into_one_canonical_identity() {
    let first_root = TempDir::new().unwrap();
    let second_root = TempDir::new().unwrap();
    let mut references = Vec::new();
    for (root, order) in [
        (&first_root, vec![9_u64, 1, 7, 0, 8, 2, 6, 3, 5, 4]),
        (&second_root, (0_u64..10).collect()),
    ] {
        let store = store(root);
        let (memory, spill) = authorities(256 * 1024, 4 * 1024 * 1024);
        let mut builder = store
            .canonical_builder(
                "synthetic-v1",
                canonical_limits(),
                Arc::clone(&memory),
                Arc::new(spill),
            )
            .unwrap();
        for partition in order {
            let task = SyntheticTask {
                partition,
                path: format!("s3://bucket/{partition:08}.parquet"),
            };
            builder
                .push_with(task.path.as_bytes(), |output| {
                    serde_json::to_writer(output, &task)
                        .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
                })
                .unwrap();
        }
        let artifact = builder.finalize(encode_authority).unwrap();
        let mut reader = store
            .reader(
                artifact.reference.clone(),
                "synthetic-v1",
                4096,
                4096,
                Arc::clone(&memory),
            )
            .unwrap();
        let mut expected = 0_u64;
        while let Some(record) = reader.next_record().unwrap() {
            let task: SyntheticTask = serde_json::from_slice(record.payload.payload()).unwrap();
            assert_eq!(task.partition, expected);
            expected += 1;
        }
        assert_eq!(expected, 10);
        references.push(artifact.reference);
    }
    assert_eq!(references[0], references[1]);
}

#[test]
fn canonical_builder_rejects_duplicate_keys_and_releases_authorities() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (memory, spill) = authorities(256 * 1024, 4 * 1024 * 1024);
    let spill: Arc<dyn SpillBudgetCoordinator> = Arc::new(spill);
    let mut builder = store
        .canonical_builder(
            "synthetic-v1",
            canonical_limits(),
            Arc::clone(&memory),
            Arc::clone(&spill),
        )
        .unwrap();
    let task = SyntheticTask {
        partition: 0,
        path: "s3://bucket/same.parquet".to_owned(),
    };
    for expected_ok in [true, false] {
        let result = builder.push_with(task.path.as_bytes(), |output| {
            serde_json::to_writer(output, &task)
                .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
        });
        assert_eq!(result.is_ok(), expected_ok);
    }
    drop(builder);
    assert_eq!(memory.snapshot().current_bytes, 0);
    assert_eq!(spill.snapshot().current_bytes, 0);
}

#[test]
fn idempotent_provider_input_collapses_only_identical_observations() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (memory, spill) = authorities(256 * 1024, 4 * 1024 * 1024);
    let mut builder = store
        .canonical_builder(
            "synthetic-v1",
            canonical_limits(),
            Arc::clone(&memory),
            Arc::new(spill),
        )
        .unwrap();
    let task = SyntheticTask {
        partition: 0,
        path: "s3://bucket/same.parquet".to_owned(),
    };
    assert!(
        builder
            .push_idempotent_with(task.path.as_bytes(), |output| {
                serde_json::to_writer(output, &task)
                    .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
            })
            .unwrap()
    );
    assert!(
        !builder
            .push_idempotent_with(task.path.as_bytes(), |output| {
                serde_json::to_writer(output, &task)
                    .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
            })
            .unwrap()
    );
    let conflicting = SyntheticTask {
        partition: 1,
        path: task.path.clone(),
    };
    let error = builder
        .push_idempotent_with(conflicting.path.as_bytes(), |output| {
            serde_json::to_writer(output, &conflicting)
                .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
        })
        .unwrap_err();
    assert!(error.message.contains("conflicting payloads"));
    assert_eq!(builder.task_count(), 1);
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
            .writer("synthetic-v1", limits(), memory, &spill)
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
        references.push(writer.finalize(encode_authority).unwrap().reference);
    }
    assert_eq!(references[0], references[1]);
}

#[test]
fn tamper_and_noncanonical_order_fail_closed() {
    let root = TempDir::new().unwrap();
    let store = store(&root);
    let (memory, spill) = authorities(64 * 1024, 1024 * 1024);
    let mut writer = store
        .writer("synthetic-v1", limits(), Arc::clone(&memory), &spill)
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
    let artifact = writer.finalize(encode_authority).unwrap();

    let mut bytes = fs::read(&artifact.path).unwrap();
    let payload_offset = bytes
        .windows(b"file:///zero.parquet".len())
        .position(|window| window == b"file:///zero.parquet")
        .unwrap();
    bytes[payload_offset] ^= 1;
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
        .writer("synthetic-v1", limits(), memory, &spill)
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
    let (memory, spill) = authorities(16 * 1024 * 1024, 512 * 1024 * 1024);
    let spill = Arc::new(spill);
    let production_limits = CanonicalTaskSetLimits {
        tasks: limits(),
        maximum_sort_key_bytes: 64 * 1024,
        index_cache_bytes: 8 * 1024 * 1024,
        spill_growth_bytes: 64 * 1024 * 1024,
        minimum_initial_spill_bytes: SQLITE_PAGE_BYTES * 2,
    };
    let mut builder = store
        .canonical_builder(
            "million-v1",
            production_limits.clone(),
            Arc::clone(&memory),
            spill.clone(),
        )
        .unwrap();
    for partition in (0..1_000_000).rev() {
        let task = SyntheticTask {
            partition,
            path: format!("s3://b/{partition:08}"),
        };
        builder
            .push_with(task.path.as_bytes(), |output| {
                serde_json::to_writer(output, &task)
                    .map_err(|error| CdfError::data(format!("encode synthetic task: {error}")))
            })
            .unwrap();
    }
    let artifact = builder.finalize(encode_authority).unwrap();
    assert_eq!(artifact.task_count, 1_000_000);
    let mut reader = store
        .reader(
            artifact.reference,
            "million-v1",
            production_limits.tasks.maximum_task_bytes,
            production_limits.tasks.maximum_authority_bytes,
            Arc::clone(&memory),
        )
        .unwrap();
    let mut expected = 0_u64;
    while let Some(record) = reader.next_record().unwrap() {
        let task: SyntheticTask = serde_json::from_slice(record.payload.payload()).unwrap();
        assert_eq!(record.canonical_ordinal, expected);
        assert_eq!(task.partition, expected);
        expected += 1;
    }
    assert_eq!(expected, 1_000_000);
    assert!(memory.snapshot().peak_bytes <= 16 * 1024 * 1024);
    assert!(spill.snapshot().peak_bytes <= 512 * 1024 * 1024);
}

fn writer_authority_hash() -> &'static str {
    "sha256:2430f1a2ad2982d0067885488a4c89e21ad1d7c83b115ba8f1b20acc88dfaea8"
}
