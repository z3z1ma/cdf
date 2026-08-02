use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    Batch, BatchId, CdfError, PartitionId, ResourceId, Result, SchemaHash, SegmentId,
};
use cdf_package::PackageBuilder;
use futures_core::Stream;
use futures_executor::block_on;
use futures_util::{StreamExt, TryStreamExt, stream};

use super::*;

#[test]
fn descriptor_validates_capabilities_without_concrete_runtime_types() {
    let descriptor = mock_descriptor(ForeignTransferMode::ArrowCData);
    descriptor.validate().unwrap();
    assert!(descriptor.supports_transfer_mode(ForeignTransferMode::ArrowCData));
    assert!(!descriptor.supports_transfer_mode(ForeignTransferMode::RowCompat));
}

#[test]
fn cancellation_wakes_and_unregisters_pending_foreign_work() {
    let cancellation = ForeignCancellation::default();
    let mut pending = Box::pin(cancellation.cancelled());
    let mut context = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(matches!(pending.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(cancellation.waiter_count_for_test(), 1);
    drop(pending);
    assert_eq!(cancellation.waiter_count_for_test(), 0);

    cancellation.cancel();
    block_on(cancellation.cancelled());
    assert!(cancellation.check().is_err());
}

#[test]
fn mock_transfer_modes_traverse_as_incremental_batches() {
    for mode in [
        ForeignTransferMode::ArrowCData,
        ForeignTransferMode::ArrowIpcStream,
        ForeignTransferMode::RowCompat,
    ] {
        let stream = Box::pin(stream::iter(vec![
            Ok(ForeignStreamEvent::Control(
                ForeignControlEvent::new(1, ForeignControlKind::Progress { rows: 0, bytes: 0 })
                    .unwrap(),
            )),
            Ok(ForeignStreamEvent::Outcome(mock_outcome(2, mode))),
            Ok(ForeignStreamEvent::Outcome(mock_outcome(3, mode))),
            Ok(ForeignStreamEvent::Terminal(
                ForeignTerminalStatus::Succeeded {
                    final_position: None,
                },
            )),
        ])) as ForeignEventStream;
        let batches =
            block_on(batch_stream_from_foreign_events(stream).try_collect::<Vec<_>>()).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].header.row_count, 2);
        assert_eq!(batches[1].header.row_count, 2);
    }
}

#[test]
fn projection_preserves_actual_transfer_copy_and_control_evidence_at_eof() {
    let stream = Box::pin(stream::iter(vec![
        Ok(ForeignStreamEvent::Control(
            ForeignControlEvent::new(1, ForeignControlKind::Progress { rows: 0, bytes: 0 })
                .unwrap(),
        )),
        Ok(ForeignStreamEvent::Outcome(mock_outcome(
            2,
            ForeignTransferMode::ArrowCData,
        ))),
        Ok(ForeignStreamEvent::Outcome(mock_outcome(
            3,
            ForeignTransferMode::RowCompat,
        ))),
        Ok(ForeignStreamEvent::Terminal(
            ForeignTerminalStatus::Succeeded {
                final_position: None,
            },
        )),
    ])) as ForeignEventStream;
    let projection = project_foreign_events(stream);
    let batches = block_on(projection.batches.try_collect::<Vec<_>>()).unwrap();
    let report = block_on(projection.completion).unwrap();

    assert_eq!(batches.len(), 2);
    assert_eq!(report.control_events, 1);
    assert_eq!(report.modes.len(), 2);
    assert_eq!(report.modes[0].mode, ForeignTransferMode::ArrowCData);
    assert_eq!(report.modes[0].batches, 1);
    assert_eq!(report.modes[0].rows, 2);
    assert_eq!(report.modes[0].zero_copy_verified_batches, 1);
    assert_eq!(report.modes[0].known_copy_batches, 0);
    assert_eq!(report.modes[1].mode, ForeignTransferMode::RowCompat);
    assert_eq!(report.modes[1].batches, 1);
    assert_eq!(report.modes[1].rows, 2);
    assert_eq!(report.modes[1].known_copy_batches, 1);
    assert_eq!(report.modes[1].known_copy_bytes, 64);
    assert_eq!(report.modes[1].unknown_copy_batches, 0);
}

#[test]
fn mock_stream_reaches_package_segments_without_whole_stream_collection() {
    let stream = Box::pin(stream::iter(vec![
        Ok(ForeignStreamEvent::Outcome(mock_outcome(
            1,
            ForeignTransferMode::ArrowIpcStream,
        ))),
        Ok(ForeignStreamEvent::Outcome(mock_outcome(
            2,
            ForeignTransferMode::ArrowIpcStream,
        ))),
        Ok(ForeignStreamEvent::Terminal(
            ForeignTerminalStatus::Succeeded {
                final_position: None,
            },
        )),
    ])) as ForeignEventStream;
    let temp = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(
        temp.path(),
        "foreign-mock-package",
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    let mut batches = batch_stream_from_foreign_events(stream);
    let mut segment_count = 0_u64;
    let mut package_row_ord_start = 0_u64;
    while let Some(batch) = block_on(batches.next()).transpose().unwrap() {
        segment_count += 1;
        let record_batch = batch.record_batch().unwrap().clone();
        let row_count = record_batch.num_rows() as u64;
        let record_batch =
            cdf_package_contract::append_package_row_ord(vec![record_batch], package_row_ord_start)
                .unwrap();
        builder
            .write_segment(
                SegmentId::new(format!("seg-{segment_count:06}")).unwrap(),
                package_row_ord_start,
                &record_batch,
            )
            .unwrap();
        package_row_ord_start += row_count;
    }
    builder.finish().unwrap();
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let mut row_counts = Vec::new();
    reader
        .for_each_identity_segment(&mut |segment| {
            row_counts.push(segment.row_count);
            Ok(())
        })
        .unwrap();
    assert_eq!(segment_count, 2);
    assert_eq!(row_counts, [2, 2]);
}

#[test]
fn batch_projection_does_not_collect_before_first_output() {
    let polls = Arc::new(AtomicUsize::new(0));
    let stream = CountingForeignStream {
        polls: Arc::clone(&polls),
        next: 0,
    };
    let mut batches = batch_stream_from_foreign_events(Box::pin(stream));
    let first = block_on(batches.next()).unwrap().unwrap();
    assert_eq!(first.header.row_count, 2);
    assert!(polls.load(Ordering::SeqCst) <= 1);
}

#[test]
fn stream_summary_requires_exactly_one_terminal_status() {
    let missing_terminal = Box::pin(stream::iter(vec![Ok(ForeignStreamEvent::Outcome(
        mock_outcome(1, ForeignTransferMode::ArrowIpcStream),
    ))])) as ForeignEventStream;
    assert!(block_on(summarize_foreign_events(missing_terminal)).is_err());

    let duplicate_terminal = Box::pin(stream::iter(vec![
        Ok(ForeignStreamEvent::Terminal(
            ForeignTerminalStatus::Succeeded {
                final_position: None,
            },
        )),
        Ok(ForeignStreamEvent::Terminal(
            ForeignTerminalStatus::Succeeded {
                final_position: None,
            },
        )),
    ])) as ForeignEventStream;
    assert!(block_on(summarize_foreign_events(duplicate_terminal)).is_err());

    let post_terminal_outcome = Box::pin(stream::iter(vec![
        Ok(ForeignStreamEvent::Terminal(
            ForeignTerminalStatus::Succeeded {
                final_position: None,
            },
        )),
        Ok(ForeignStreamEvent::Outcome(mock_outcome(
            2,
            ForeignTransferMode::ArrowCData,
        ))),
    ])) as ForeignEventStream;
    assert!(block_on(summarize_foreign_events(post_terminal_outcome)).is_err());
}

#[test]
fn production_batch_projection_rejects_every_post_terminal_event() {
    let post_terminal_outcome = Box::pin(stream::iter(vec![
        Ok(ForeignStreamEvent::Terminal(
            ForeignTerminalStatus::Succeeded {
                final_position: None,
            },
        )),
        Ok(ForeignStreamEvent::Outcome(mock_outcome(
            2,
            ForeignTransferMode::ArrowCData,
        ))),
    ])) as ForeignEventStream;
    assert!(
        block_on(batch_stream_from_foreign_events(post_terminal_outcome).try_collect::<Vec<_>>())
            .is_err()
    );

    let duplicate_terminal = Box::pin(stream::iter(vec![
        Ok(ForeignStreamEvent::Terminal(
            ForeignTerminalStatus::Succeeded {
                final_position: None,
            },
        )),
        Ok(ForeignStreamEvent::Terminal(
            ForeignTerminalStatus::Succeeded {
                final_position: None,
            },
        )),
    ])) as ForeignEventStream;
    assert!(
        block_on(batch_stream_from_foreign_events(duplicate_terminal).try_collect::<Vec<_>>())
            .is_err()
    );

    let task_failure = Box::pin(stream::iter(vec![Err(CdfError::transient(
        "producer task failed",
    ))])) as ForeignEventStream;
    let error = block_on(batch_stream_from_foreign_events(task_failure).try_collect::<Vec<_>>())
        .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Transient);
    assert_eq!(error.message, "producer task failed");
}

#[test]
fn crate_contract_stays_executor_neutral_and_non_collecting() {
    let manifest = std::fs::read_to_string(manifest_path()).unwrap();
    let source = source_text();
    for forbidden in forbidden_runtime_tokens() {
        assert!(
            !manifest.contains(&forbidden),
            "manifest must not depend on concrete runtime `{forbidden}`"
        );
        assert!(
            !source.to_ascii_lowercase().contains(&forbidden),
            "contract source must not expose concrete runtime `{forbidden}`"
        );
    }
    for forbidden in forbidden_collection_tokens() {
        assert!(
            !source.contains(&forbidden),
            "foreign stream contract must not expose eager batch collection `{forbidden}`"
        );
    }
}

#[test]
fn production_source_scan_recurses_and_excludes_test_sources() {
    let root = tempfile::tempdir().unwrap();
    let nested = root.path().join("producer/nested");
    let nested_tests = root.path().join("producer/tests");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir_all(&nested_tests).unwrap();
    for relative in [
        "lib.rs",
        "producer/mod.rs",
        "producer/nested/adapter.rs",
        "tests.rs",
        "producer/tests.rs",
        "producer/tests/adapter.rs",
    ] {
        std::fs::write(root.path().join(relative), relative).unwrap();
    }
    std::fs::write(root.path().join("producer/README.md"), "not Rust").unwrap();

    let paths = production_rust_source_paths(root.path())
        .into_iter()
        .map(|path| path.strip_prefix(root.path()).unwrap().to_path_buf())
        .collect::<Vec<_>>();

    assert_eq!(
        paths,
        [
            PathBuf::from("lib.rs"),
            PathBuf::from("producer/mod.rs"),
            PathBuf::from("producer/nested/adapter.rs"),
        ]
    );
}

struct CountingForeignStream {
    polls: Arc<AtomicUsize>,
    next: u8,
}

impl Stream for CountingForeignStream {
    type Item = Result<ForeignStreamEvent>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        let item = match self.next {
            0 => Some(Ok(ForeignStreamEvent::Outcome(mock_outcome(
                1,
                ForeignTransferMode::ArrowCData,
            )))),
            1 => Some(Ok(ForeignStreamEvent::Terminal(
                ForeignTerminalStatus::Succeeded {
                    final_position: None,
                },
            ))),
            _ => None,
        };
        self.next = self.next.saturating_add(1);
        Poll::Ready(item)
    }
}

fn mock_descriptor(mode: ForeignTransferMode) -> ForeignProducerDescriptor {
    ForeignProducerDescriptor {
        producer_id: ForeignProducerId::new("mock_foreign").unwrap(),
        protocol_version: ForeignProtocolVersion::new("1").unwrap(),
        transfer_modes: vec![mode],
        schema_acquisition: ForeignSchemaAcquisition::DeclaredHandshake,
        startup: ForeignStartupModel::InProcessAttached,
        lanes: ForeignLaneCapabilities {
            execution_lane: ForeignExecutionLane::Cpu,
            maximum_internal_parallelism: 1,
            backpressure: ForeignBackpressure::Pull,
        },
        memory: ForeignMemoryContract {
            payload_window_bytes: Some(4096),
            control_queue_bytes: Some(1024),
            diagnostic_queue_bytes: Some(1024),
            native_scratch_bytes: None,
            child_process_bytes: None,
        },
        cancellation: ForeignCancellationContract {
            cooperative_stop: true,
            interrupt_safe: true,
            force_termination_authorized: false,
            drains_on_cancel: true,
        },
        state: ForeignStateContract {
            emits_positions: true,
            emits_watermarks: false,
            emits_foreign_state: false,
            terminal_state_required: false,
        },
        security: ForeignSecurityContract {
            ambient_network: false,
            ambient_filesystem: false,
            secret_names: Vec::new(),
        },
    }
}

fn mock_outcome(sequence: u64, mode: ForeignTransferMode) -> ForeignBatchOutcome {
    ForeignBatchOutcome::new(
        sequence,
        mock_batch(sequence),
        mode,
        match mode {
            ForeignTransferMode::ArrowCData => ForeignCopyClassification::PayloadZeroCopyVerified,
            ForeignTransferMode::ArrowIpcStream | ForeignTransferMode::RowCompat => {
                ForeignCopyClassification::payload_copy_known(64).unwrap()
            }
        },
    )
    .unwrap()
}

fn mock_batch(sequence: u64) -> Batch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![
            sequence as i64,
            sequence as i64 + 1,
        ]))],
    )
    .unwrap();
    Batch::from_record_batch(
        BatchId::new(format!("batch-{sequence}")).unwrap(),
        ResourceId::new("mock.resource").unwrap(),
        PartitionId::new("partition-0").unwrap(),
        SchemaHash::new("sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
            .unwrap(),
        batch,
    )
    .unwrap()
}

fn manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")
}

fn source_text() -> String {
    let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    production_rust_source_paths(&source_root)
        .into_iter()
        .map(|path| std::fs::read_to_string(path).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

fn production_rust_source_paths(source_root: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    collect_production_rust_source_paths(source_root, &mut paths);
    paths.sort();
    paths
}

fn collect_production_rust_source_paths(directory: &Path, paths: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let file_type = entry.file_type().unwrap();
        if file_type.is_dir() {
            if path.file_name().and_then(|name| name.to_str()) != Some("tests") {
                collect_production_rust_source_paths(&path, paths);
            }
        } else if file_type.is_file()
            && path.extension().and_then(|extension| extension.to_str()) == Some("rs")
            && path.file_name().and_then(|name| name.to_str()) != Some("tests.rs")
        {
            paths.push(path);
        }
    }
}

fn forbidden_runtime_tokens() -> Vec<String> {
    vec![
        ["py", "o3"].concat(),
        ["to", "kio"].concat(),
        ["wasm", "time"].concat(),
        ["data", "fusion"].concat(),
        ["cdf", "_cli"].concat(),
        ["cdf", "-runtime"].concat(),
        ["std", "::", "process"].concat(),
    ]
}

fn forbidden_collection_tokens() -> Vec<String> {
    vec![
        ["Vec", "<", "Batch", ">"].concat(),
        ["Vec", "<", "Record", "Batch", ">"].concat(),
        ["Vec", " < ", "Batch", " >"].concat(),
    ]
}
