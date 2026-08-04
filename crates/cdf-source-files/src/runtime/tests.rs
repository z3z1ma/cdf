use super::*;
use super::{
    decode::*, discovery::*, input::*, model::*, planning::*, resolution::*, validation::*,
};

use std::{
    fs,
    io::Write,
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    BoxFuture, CompiledScanIntent, DeclarativeExpressionNode, PartitionId, ResourceId, ScopeKey,
    SourceReadMode,
};
use cdf_memory::{MemoryClass, MemoryCoordinator};
use cdf_object_access::{
    FileIdentityMetadata, FileTransportLocation, FileTransportResource, LocalByteSource,
};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    FormatDetectionConfidence, FormatDriver, ReadOptions, SequentialReadRequest,
};
use flate2::{Compression, write::GzEncoder};
use futures_util::TryStreamExt;
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

use crate::FileCompressionDeclaration;

#[test]
fn file_source_blocking_lane_matches_advertised_parallelism() {
    let lane = file_source_blocking_lane();
    assert_eq!(lane.lane_id, FILE_SOURCE_BLOCKING_LANE_ID);
    assert_eq!(lane.maximum_concurrency, FILE_SOURCE_ADVERTISED_PARALLELISM);
    assert_eq!(lane.cpu_slot_cost, 1);
    assert_eq!(lane.native_internal_parallelism, 1);
}

#[test]
fn decode_unit_width_uses_budget_not_transient_free_memory() {
    const BUDGET: u64 = 64 * 1024 * 1024;
    let memory =
        cdf_memory::DeterministicMemoryCoordinator::new(BUDGET, std::collections::BTreeMap::new())
            .unwrap();
    let held = memory
        .try_reserve(
            &cdf_memory::ReservationRequest::new(
                cdf_memory::ConsumerKey::new(
                    "transient-sibling",
                    cdf_memory::MemoryClass::Transform,
                )
                .unwrap(),
                BUDGET,
            )
            .unwrap(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(memory.snapshot().current_bytes, BUDGET);
    assert_eq!(stable_decode_memory_budget(&memory), BUDGET);
    drop(held);
}

#[test]
fn nested_decode_fanout_shares_the_run_cpu_authority() {
    assert_eq!(per_partition_decode_unit_ceiling(16, Some(16)), 1);
    assert_eq!(per_partition_decode_unit_ceiling(16, Some(4)), 4);
    assert_eq!(per_partition_decode_unit_ceiling(16, Some(1)), 16);
    assert_eq!(per_partition_decode_unit_ceiling(16, None), 16);
    assert_eq!(per_partition_decode_unit_ceiling(1, Some(16)), 1);
}

fn physical_runtime(
    descriptor: &ResourceDescriptor,
    effective_schema: SchemaRef,
    physical_schema: SchemaRef,
    observation_id: impl Into<String>,
    observation_binding: cdf_kernel::SchemaObservationBinding,
) -> EffectiveSchemaRuntime {
    let effective_hash =
        cdf_kernel::canonical_arrow_schema_hash(effective_schema.as_ref()).unwrap();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let evidence = cdf_kernel::EffectiveSchemaEvidence::new(
        descriptor.schema_source.baseline_reference().unwrap(),
        effective_hash,
        cdf_kernel::DiscoveryManifestReference {
            manifest_hash: cdf_kernel::DiscoveryManifestHash::new("test-exact-physical-manifest")
                .unwrap(),
            path: ".cdf/discovery/test-exact-physical.json".to_owned(),
        },
        vec![cdf_kernel::EffectiveSchemaObservationEvidence::new(
            observation_id,
            physical_hash.clone(),
            observation_binding,
        )],
    )
    .unwrap();
    EffectiveSchemaRuntime::new(
        evidence,
        vec![cdf_kernel::EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap()
}

#[derive(Debug)]
struct ExternalMockFormat {
    descriptor: cdf_runtime::FormatDriverDescriptor,
    batches_per_unit: usize,
}

impl ExternalMockFormat {
    fn new() -> Self {
        Self {
            descriptor: cdf_runtime::FormatDriverDescriptor {
                format_id: cdf_runtime::FormatId::new("external_mock").unwrap(),
                semantic_version: "1.0.0".to_owned(),
                aliases: Vec::new(),
                extensions: vec!["mock".to_owned()],
                mime_types: Vec::new(),
                magic: Vec::new(),
                detection_probe: cdf_runtime::FormatDetectionProbe {
                    prefix_bytes: 4,
                    suffix_bytes: 0,
                },
                option_schema: serde_json::json!({
                    "type": "object",
                    "additionalProperties": false
                }),
                projection_pushdown: cdf_kernel::PushdownFidelity::Unsupported,
                predicate_pushdown: cdf_kernel::PushdownFidelity::Unsupported,
                predicate_operators: Vec::new(),
                source_access: cdf_runtime::FormatSourceAccess::Sequential,
                discovery: cdf_runtime::FormatDiscoveryCapabilities::only(
                    cdf_runtime::FormatDiscoveryKind::BoundedContent,
                ),
                decode_unit_policy: "whole_mock_file".to_owned(),
                error_isolation: cdf_runtime::FormatErrorIsolation::DecodeUnit,
                decode_cpu: cdf_runtime::CpuTaskSpec {
                    task_kind: "format.external_mock.decode".to_owned(),
                    cpu_slot_cost: 1,
                    native_internal_parallelism: 1,
                },
                minimum_working_set_bytes: 64,
                maximum_working_set_bytes: 1024 * 1024,
            },
            batches_per_unit: 1,
        }
    }

    fn with_batches_per_unit(mut self, batches_per_unit: usize) -> Self {
        self.batches_per_unit = batches_per_unit;
        self
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }
}

impl cdf_runtime::FormatDriver for ExternalMockFormat {
    fn descriptor(&self) -> &cdf_runtime::FormatDriverDescriptor {
        &self.descriptor
    }

    fn canonical_options(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        if options.as_object().is_some_and(serde_json::Map::is_empty) {
            Ok(options)
        } else {
            Err(CdfError::contract("external mock options must be empty"))
        }
    }

    fn detect(&self, probe: &cdf_runtime::FormatProbe) -> Result<cdf_runtime::FormatDetection> {
        Ok(cdf_runtime::FormatDetection {
            confidence: if probe.prefix.starts_with(b"MOCK") {
                cdf_runtime::FormatDetectionConfidence::Strong
            } else {
                cdf_runtime::FormatDetectionConfidence::None
            },
            reason: "external mock framing".to_owned(),
        })
    }

    fn discover(
        &self,
        source: Arc<dyn cdf_runtime::ByteSource>,
        request: cdf_runtime::FormatDiscoveryRequest,
    ) -> cdf_kernel::BoxFuture<'_, Result<cdf_runtime::PhysicalSchemaObservation>> {
        Box::pin(async move {
            request.cancellation.check()?;
            let preferred_chunk_bytes = (8 * 1024_u64).clamp(
                source.capabilities().minimum_chunk_bytes,
                source.capabilities().maximum_chunk_bytes,
            );
            let input = source
                .open_sequential(cdf_runtime::SequentialReadRequest {
                    preferred_chunk_bytes,
                    cancellation: request.cancellation,
                })
                .await?;
            let mut cursor = cdf_runtime::AccountedByteCursor::new(input);
            if cursor.read_exact(4, "external mock magic").await? != b"MOCK" {
                return Err(CdfError::data("external mock magic mismatch"));
            }
            let schema = Self::schema();
            Ok(cdf_runtime::PhysicalSchemaObservation {
                identity: source.identity().clone(),
                arrow_schema: schema,
                sampled_bytes: 4,
                sampled_records: 0,
                evidence: BTreeMap::new(),
            })
        })
    }

    fn prepare_decode(
        &self,
        source: Arc<dyn cdf_runtime::ByteSource>,
        request: cdf_runtime::DecodePlanningRequest,
    ) -> cdf_kernel::BoxFuture<'_, Result<Arc<dyn cdf_runtime::FormatDecodeSession>>> {
        Box::pin(async move {
            request.cancellation.check()?;
            let units = vec![cdf_runtime::DecodeUnitPlan {
                unit_id: "mock-file".to_owned(),
                ordinal: 0,
                extent: None,
                estimated_working_set_bytes: 64,
                independently_retryable: true,
            }];
            Ok(Arc::new(ExternalMockDecodeSession {
                source,
                units,
                batches_per_unit: self.batches_per_unit,
            }) as Arc<dyn cdf_runtime::FormatDecodeSession>)
        })
    }
}

struct ExternalMockDecodeSession {
    source: Arc<dyn cdf_runtime::ByteSource>,
    units: Vec<cdf_runtime::DecodeUnitPlan>,
    batches_per_unit: usize,
}

impl cdf_runtime::FormatDecodeSession for ExternalMockDecodeSession {
    fn units(&self) -> &[cdf_runtime::DecodeUnitPlan] {
        &self.units
    }

    fn decode(
        &self,
        request: cdf_runtime::PhysicalDecodeRequest,
    ) -> cdf_kernel::BoxFuture<'_, Result<cdf_runtime::PhysicalDecodeStream>> {
        Box::pin(async move {
            request.cancellation.check()?;
            self.validate_unit(&request.unit)?;
            let preferred_chunk_bytes = (8 * 1024_u64).clamp(
                self.source.capabilities().minimum_chunk_bytes,
                self.source.capabilities().maximum_chunk_bytes,
            );
            let input = self
                .source
                .open_sequential(cdf_runtime::SequentialReadRequest {
                    preferred_chunk_bytes,
                    cancellation: request.cancellation.clone(),
                })
                .await?;
            let mut cursor = cdf_runtime::AccountedByteCursor::new(input);
            if cursor.read_exact(5, "external mock payload").await? != b"MOCK\n" {
                return Err(CdfError::data("external mock payload mismatch"));
            }
            let schema_hash =
                cdf_kernel::canonical_arrow_schema_hash(request.schema.decoder_schema.as_ref())?;
            let mut batches = Vec::with_capacity(self.batches_per_unit);
            for index in 0..self.batches_per_unit {
                let record_batch = RecordBatch::try_new(
                    ExternalMockFormat::schema(),
                    vec![Arc::new(Int64Array::from(vec![42]))],
                )
                .map_err(|error| CdfError::data(format!("external mock batch: {error}")))?;
                let lease = cdf_memory::reserve(
                    Arc::clone(&request.memory),
                    cdf_memory::ReservationRequest::new(
                        cdf_memory::ConsumerKey::new(
                            "external-mock-decode",
                            cdf_memory::MemoryClass::Decode,
                        )?,
                        1024,
                    )?,
                )
                .await?;
                let mut batch = cdf_kernel::Batch::from_record_batch(
                    cdf_kernel::BatchId::new(format!("external-mock-batch-{index}"))?,
                    request.resource_id.clone(),
                    request.partition_id.clone(),
                    schema_hash.clone(),
                    record_batch,
                )?;
                batch.header.source_position = request.source_position.clone();
                batches.push(cdf_runtime::AccountedPhysicalBatch::new(batch, lease));
            }
            Ok(Box::pin(futures_util::stream::iter(batches)) as cdf_runtime::PhysicalDecodeStream)
        })
    }
}

#[test]
fn blocked_decode_publication_releases_shared_run_work() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("events.mock");
    std::fs::write(&path, b"MOCK\n").unwrap();
    let mut formats = cdf_runtime::FormatRegistry::default();
    formats
        .register(Arc::new(ExternalMockFormat::new().with_batches_per_unit(8)))
        .unwrap();
    let services = crate::test_execution_services()
        .with_run_job_ceiling(1)
        .unwrap();
    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        services.clone(),
        Arc::new(formats),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    );
    let driver = dependencies.formats().resolve("external_mock").unwrap();
    let open = |partition: &str| {
        stream_registered_format(
            RegisteredFormatStreamRequest {
                source: Arc::new(
                    LocalByteSource::open(&path, dependencies.execution().memory()).unwrap(),
                ),
                payload_retention: None,
                driver: Arc::clone(&driver),
                scan_intent: CompiledScanIntent::full_scan(),
                options: ReadOptions::new(
                    ResourceId::new("events").unwrap(),
                    PartitionId::new(partition).unwrap(),
                ),
                admission_schema: ExternalMockFormat::schema(),
                canonical_format_options: serde_json::json!({}),
                source_position: None,
                physical_schema_authority: PhysicalSchemaAuthority::default(),
            },
            &dependencies,
        )
        .unwrap()
    };
    let mut first = open("first");
    let second = open("second");
    let first_batch = futures_executor::block_on(first.next())
        .expect("first stream must publish")
        .unwrap();

    // Let the first producer fill both bounded publication channels. It may retain decoded
    // bytes there, but it must not retain the sole run-work permit while waiting for demand.
    std::thread::sleep(Duration::from_millis(100));
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        sender
            .send(futures_executor::block_on(second.into_future()).0)
            .unwrap();
    });
    let second_batch = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("a blocked later publication must not monopolize shared run work")
        .expect("second stream must publish")
        .unwrap();
    drop(second_batch);
    drop(first_batch);
    drop(first);
    worker.join().unwrap();
    assert_eq!(services.run_job_ceiling().unwrap(), Some(1));
}

#[derive(Debug)]
struct ExternalPassthroughTransform(cdf_runtime::ByteTransformDescriptor);

impl ExternalPassthroughTransform {
    fn new() -> Self {
        Self(cdf_runtime::ByteTransformDescriptor {
            transform_id: cdf_runtime::ByteTransformId::new("external_passthrough").unwrap(),
            semantic_version: "1.0.0".to_owned(),
            extensions: vec!["mt".to_owned()],
            magic: Vec::new(),
            preserves_random_access: false,
            splittable: false,
            supports_concatenated_members: false,
            maximum_output_chunk_bytes: 1024 * 1024,
            maximum_working_set_bytes: 1024 * 1024,
            maximum_expanded_bytes: 1024 * 1024,
            maximum_expansion_ratio: 1,
            checksum: cdf_runtime::TransformChecksumBehavior::None,
        })
    }
}

impl cdf_runtime::ByteTransformDriver for ExternalPassthroughTransform {
    fn descriptor(&self) -> &cdf_runtime::ByteTransformDescriptor {
        &self.0
    }

    fn transform(
        &self,
        input: cdf_runtime::AccountedByteStream,
        request: cdf_runtime::ByteTransformRequest,
    ) -> Result<cdf_runtime::AccountedByteStream> {
        request.validate_for(&self.0)?;
        Ok(input)
    }
}

struct PayloadOpenCountingTransport {
    inner: FileTransportFacade,
    payload_opens: Arc<AtomicUsize>,
    metadata_reads: Arc<AtomicUsize>,
    listings: Arc<AtomicUsize>,
}

struct ExternalSchemeTransport {
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
}

impl FileTransport for ExternalSchemeTransport {
    fn metadata(
        &self,
        _egress: &cdf_runtime::SourceEgressScope,
        _resource: &FileTransportResource,
        _control: &FileTransportControl,
    ) -> Result<FileMetadataObservation> {
        Err(CdfError::internal(
            "external scheme fixture does not use metadata",
        ))
    }

    fn list(
        &self,
        _egress: &cdf_runtime::SourceEgressScope,
        resource: &FileTransportResource,
        _maximum_results: usize,
        _control: &FileTransportControl,
    ) -> Result<FileIdentityStream> {
        assert!(matches!(
            &resource.location,
            FileTransportLocation::RemoteUrl { url } if url.starts_with("mock://")
        ));
        let lease = futures_executor::block_on(cdf_memory::reserve(
            Arc::clone(&self.memory),
            cdf_memory::ReservationRequest::new(
                cdf_memory::ConsumerKey::new(
                    "external-file-transport-metadata",
                    cdf_memory::MemoryClass::Discovery,
                )?,
                FILE_IDENTITY_MEMORY_ENVELOPE_BYTES,
            )?,
        ))?;
        let identity = AccountedFileIdentity::new(
            FileIdentityMetadata {
                location: "mock://catalog/data/events.parquet".to_owned(),
                size_bytes: Some(4),
                checksum: None,
                etag: Some("\"mock-generation\"".to_owned()),
                version: None,
                modified: None,
                exact_ranges: true,
            },
            lease,
        )?;
        Ok(FileIdentityStream::materialized(
            futures_util::stream::iter([Ok(identity)]),
        ))
    }

    fn open_byte_source(
        &self,
        _egress: &cdf_runtime::SourceEgressScope,
        _resource: &FileTransportResource,
        _expected: &FileIdentityMetadata,
        _memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    ) -> Result<Arc<dyn cdf_runtime::ByteSource>> {
        Err(CdfError::internal(
            "external scheme fixture does not open payload",
        ))
    }
}

impl FileTransport for PayloadOpenCountingTransport {
    fn metadata(
        &self,
        egress: &cdf_runtime::SourceEgressScope,
        resource: &FileTransportResource,
        control: &FileTransportControl,
    ) -> Result<FileMetadataObservation> {
        self.metadata_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.metadata(egress, resource, control)
    }

    fn metadata_if_exists(
        &self,
        egress: &cdf_runtime::SourceEgressScope,
        resource: &FileTransportResource,
        control: &FileTransportControl,
    ) -> Result<Option<FileMetadataObservation>> {
        self.inner.metadata_if_exists(egress, resource, control)
    }

    fn list(
        &self,
        egress: &cdf_runtime::SourceEgressScope,
        resource: &FileTransportResource,
        maximum_results: usize,
        control: &FileTransportControl,
    ) -> Result<FileIdentityStream> {
        self.listings.fetch_add(1, Ordering::Relaxed);
        self.inner.list(egress, resource, maximum_results, control)
    }

    fn open_byte_source(
        &self,
        egress: &cdf_runtime::SourceEgressScope,
        resource: &FileTransportResource,
        expected: &FileIdentityMetadata,
        memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    ) -> Result<Arc<dyn cdf_runtime::ByteSource>> {
        self.payload_opens.fetch_add(1, Ordering::Relaxed);
        self.inner
            .open_byte_source(egress, resource, expected, memory)
    }
}

#[test]
fn external_remote_scheme_requires_no_file_runtime_dispatch_branch() {
    let coordinator = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(
            FILE_IDENTITY_MEMORY_ENVELOPE_BYTES,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let transport = ExternalSchemeTransport {
        memory: coordinator.clone(),
    };
    let plan = FileResourcePlan {
        source: "events".to_owned(),
        root: "mock://catalog/data".to_owned(),
        glob: "*.parquet".to_owned(),
        format: Some(FileFormatDeclaration::parquet()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };

    let matches = resolve_remote_matches(
        &ResourceId::new("events.raw").unwrap(),
        &plan,
        &transport,
        &crate::test_egress_scope(),
        crate::test_format_registry().as_ref(),
        crate::test_transform_registry().as_ref(),
    )
    .unwrap();

    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].path_text, "mock://catalog/data/events.parquet");
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn external_format_and_transform_compose_without_runtime_dispatch_edits() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("events.mock.mt");
    std::fs::write(&path, b"MOCK\n").unwrap();
    let mut formats = cdf_runtime::FormatRegistry::default();
    formats
        .register(Arc::new(ExternalMockFormat::new()))
        .unwrap();
    let mut transforms = cdf_runtime::ByteTransformRegistry::default();
    transforms
        .register(Arc::new(ExternalPassthroughTransform::new()))
        .unwrap();
    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        Arc::new(formats),
        Arc::new(transforms),
        crate::test_egress_scope(),
    );
    let plan = FileResourcePlan {
        source: "external".to_owned(),
        root: root.path().to_string_lossy().into_owned(),
        glob: "events.mock.mt".to_owned(),
        format: Some(FileFormatDeclaration::named("external_mock").unwrap()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::auto(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("external.events").unwrap();
    let resolved = dependencies
        .with_transport(|transport, egress| {
            resolve_file_matches(
                &resource_id,
                &plan,
                transport,
                egress,
                dependencies.formats(),
                dependencies.transforms(),
            )
        })
        .unwrap();
    assert_eq!(resolved[0].compression.mode_name(), "external_passthrough");
    let probe = discover_local_binary_schema(
        &path,
        "events.mock.mt",
        &dependencies,
        0,
        SchemaDiscoveryRequest {
            resource_id: &resource_id,
            format: plan.resolved_format().unwrap(),
            format_declared: plan.format_declared,
            format_options: &plan.format_options,
            discovery_kind: cdf_runtime::FormatDiscoveryKind::BoundedContent,
            transform_name: "external_passthrough",
            maximum_bytes: 1024,
            maximum_records: 1_000,
            cancellation: cdf_runtime::RunCancellation::default(),
        },
    )
    .unwrap();
    assert_eq!(probe.schema.as_ref(), ExternalMockFormat::schema().as_ref());
    let stream = stream_file_match_blocking(
        &resolved[0],
        plan.resolved_format().unwrap(),
        ReadOptions::new(resource_id, PartitionId::new("external-file").unwrap()),
        &dependencies,
        Arc::clone(&probe.schema),
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].record_batch().unwrap().num_rows(), 1);
    assert!(matches!(
        batches[0].header.source_position,
        Some(SourcePosition::FileManifest(_))
    ));
    drop(batches);
    assert_eq!(
        dependencies.execution().memory().snapshot().current_bytes,
        0
    );
}

#[test]
fn format_discovery_evidence_cannot_override_source_identity() {
    let mut identity = BTreeMap::from([("format".to_owned(), "parquet".to_owned())]);
    let error = merge_discovery_evidence(
        &mut identity,
        BTreeMap::from([("format".to_owned(), "forged".to_owned())]),
    )
    .unwrap_err();
    assert!(error.message.contains("conflicts with source identity"));
    assert_eq!(identity["format"], "parquet");
}

#[test]
fn shared_transport_dependency_does_not_serialize_independent_io() {
    let dependencies = Arc::new(FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    ));
    let start = Arc::new(Barrier::new(3));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let workers = (0..2)
        .map(|_| {
            let dependencies = Arc::clone(&dependencies);
            let start = Arc::clone(&start);
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            std::thread::spawn(move || {
                start.wait();
                dependencies
                    .with_transport(|_, _| {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(current, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(50));
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .unwrap();
            })
        })
        .collect::<Vec<_>>();
    start.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(peak.load(Ordering::SeqCst), 2);
}

#[test]
fn local_parquet_uses_registered_native_driver_as_bounded_stream() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, Arc::clone(&schema), None).unwrap();
    for start in [0_i64, 50_000, 100_000] {
        let record_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(start..start + 50_000)) as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    (start..start + 50_000).map(|value| format!("name-{value}")),
                )) as ArrayRef,
            ],
        )
        .unwrap();
        writer.write(&record_batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), bytes).unwrap();
    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    );
    let driver = dependencies.formats().resolve("parquet").unwrap();
    let stream = stream_registered_format(
        RegisteredFormatStreamRequest {
            source: Arc::new(
                LocalByteSource::open(temp.path(), dependencies.execution().memory()).unwrap(),
            ),
            payload_retention: None,
            driver,
            scan_intent: CompiledScanIntent::full_scan(),
            options: ReadOptions::new(
                ResourceId::new("events").unwrap(),
                PartitionId::new("file-0").unwrap(),
            ),
            canonical_format_options: serde_json::json!({}),
            source_position: None,
            admission_schema: Arc::clone(&schema),
            physical_schema_authority: PhysicalSchemaAuthority::default(),
        },
        &dependencies,
    )
    .unwrap();

    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(batches.len(), 3);
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        150_000
    );
    assert!(
        batches
            .iter()
            .all(|batch| batch.header.row_count <= NATIVE_TARGET_BATCH_ROWS as u64)
    );
    assert!(dependencies.execution().memory().snapshot().current_bytes > 0);
    drop(batches);
    assert_eq!(
        dependencies.execution().memory().snapshot().current_bytes,
        0
    );
}

#[test]
fn negotiated_parquet_projection_and_predicate_reach_production_decode() {
    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("VendorID", DataType::Int64, false),
        Field::new("Name", DataType::Utf8, false),
        Field::new("Ignored", DataType::Int64, false),
    ]));
    let schema = Arc::new(Schema::new(vec![
        cdf_kernel::with_source_name(Field::new("vendor_id", DataType::Int64, false), "VendorID"),
        cdf_kernel::with_source_name(Field::new("name", DataType::Utf8, false), "Name"),
        cdf_kernel::with_source_name(Field::new("ignored", DataType::Int64, false), "Ignored"),
    ]));
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("events.parquet");
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        Arc::clone(&physical_schema),
        None,
    )
    .unwrap();
    writer
        .write(
            &RecordBatch::try_new(
                Arc::clone(&physical_schema),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .unwrap();
    writer.close().unwrap();

    let formats = crate::test_format_registry();
    let unresolved = FileResourcePlan {
        source: "events".to_owned(),
        root: temp.path().to_string_lossy().into_owned(),
        glob: "events.parquet".to_owned(),
        format: Some(FileFormatDeclaration::parquet()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let (plan, compiled_format) =
        crate::compile_file_resource_plan(&unresolved, formats.as_ref()).unwrap();
    let descriptor = ResourceDescriptor {
        resource_id: ResourceId::new("events").unwrap(),
        schema_source: cdf_kernel::SchemaSource::Declared {
            schema_hash: cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap(),
            source: "test".to_owned(),
        },
        primary_key: Vec::new(),
        merge_key: Vec::new(),
        cursor: None,
        write_disposition: WriteDisposition::Append,
        deduplication: None,
        contract: None,
        state_scope: ScopeKey::Resource,
        freshness: None,
        trust_level: cdf_kernel::TrustLevel::Governed,
    };
    assert_eq!(
        file_resource_capabilities(&compiled_format.descriptor).projection,
        CapabilitySupport::Supported
    );
    assert_eq!(
        file_resource_capabilities(&compiled_format.descriptor)
            .filters
            .default_fidelity,
        PushdownFidelity::Exact
    );
    let task_store_root = TempDir::new().unwrap();
    let transforms = crate::test_transform_registry();
    let observed_file = resolved_file_match(
        &descriptor.resource_id,
        temp.path(),
        path.clone(),
        &plan,
        formats.as_ref(),
        transforms.as_ref(),
    )
    .unwrap();
    let observation_binding = cdf_kernel::SchemaObservationBinding::new(
        file_schema_observation_binding(&FileInventoryRecord::from(&observed_file)),
    )
    .unwrap();
    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        formats,
        transforms,
        crate::test_egress_scope(),
    )
    .with_task_store(
        ExternalTaskStore::new(
            task_store_root.path(),
            cdf_kernel::ContentStoreNamespace::new("file-plans").unwrap(),
        )
        .unwrap(),
        FileTaskStoreOptions {
            maximum_task_bytes: 1024 * 1024,
            maximum_authority_bytes: 1024 * 1024,
            maximum_sort_key_bytes: 64 * 1024,
            index_cache_bytes: 1024 * 1024,
            writer_buffer_bytes: 64 * 1024,
            spill_growth_bytes: 1024 * 1024,
            metadata_parse_amplification_bps: 40_000,
        },
    )
    .unwrap();
    let resource = FileResource::new_for_test(
        FileResourceDefinition {
            descriptor: descriptor.clone(),
            schema: Arc::clone(&schema),
            plan,
            type_policy_allowances: TypePolicyAllowances::default(),
            effective_schema_runtime: Some(physical_runtime(
                &descriptor,
                Arc::clone(&schema),
                Arc::clone(&physical_schema),
                "events.parquet",
                observation_binding.clone(),
            )),
            baseline_observation_schema_catalog: Vec::new(),
            compiled_format,
        },
        dependencies,
        cdf_kernel::SourceDiscoveryBinding::new(
            cdf_runtime::artifact_hash(&serde_json::json!({"discovery": "events"})).unwrap(),
        )
        .unwrap(),
        cdf_kernel::CompiledSourcePlanHash::new(
            cdf_runtime::artifact_hash(&serde_json::json!({"resource": "events"})).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let request = ScanRequest {
        resource_id: descriptor.resource_id.clone(),
        projection: Some(vec!["vendor_id".to_owned()]),
        filters: vec![
            cdf_kernel::ScanPredicate::new(
                cdf_kernel::PredicateId::new("id-is-greater-than-one").unwrap(),
                "vendor_id > 1",
            )
            .unwrap(),
            cdf_kernel::ScanPredicate::new(
                cdf_kernel::PredicateId::new("name-is-two").unwrap(),
                "name = 'two'",
            )
            .unwrap(),
        ],
        limit: None,
        order_by: Vec::new(),
        scope: ScopeKey::Resource,
    };
    let eager_error = resource.plan_partitions(&request).unwrap_err();
    assert!(
        eager_error
            .message
            .contains("canonical partition authority")
    );
    let scan = resource.negotiate(&request).unwrap();
    assert_eq!(
        scan.planned_source_bytes.unwrap().get(),
        std::fs::metadata(&path).unwrap().len()
    );
    let task_reference = scan.external_task_set().unwrap();
    let mut task_reader = resource.planned_partition_reader(task_reference).unwrap();
    let executable = task_reader.next_partition(0).unwrap().unwrap();
    let planned_partitions = vec![executable.plan().clone()];
    assert!(task_reader.next_partition(1).unwrap().is_none());
    assert_eq!(
        planned_partitions[0].scan_intent.projection.as_deref(),
        Some(["vendor_id".to_owned()].as_slice())
    );
    assert_eq!(planned_partitions[0].scan_intent.predicates.len(), 2);
    assert_eq!(scan.pushed_predicates.len(), 2);
    assert!(scan.unsupported_predicates.is_empty());
    cdf_kernel::validate_compiled_scan_intents(&scan).unwrap();

    let widened_physical = Arc::new(Schema::new(vec![
        Field::new("VendorID", DataType::Int32, false),
        Field::new("Name", DataType::Utf8, false),
        Field::new("Ignored", DataType::Int64, false),
    ]));
    let widened_runtime = physical_runtime(
        &descriptor,
        Arc::clone(&schema),
        widened_physical,
        "events.parquet",
        observation_binding,
    );
    assert!(
        !exact_predicate_is_partition_equivalent(
            &request.filters[0],
            &planned_partitions,
            schema.as_ref(),
            Some(&widened_runtime),
        )
        .unwrap()
    );

    let opened = futures_executor::block_on(resource.open_executable(executable)).unwrap();
    let batches = futures_executor::block_on(opened.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(batches.len(), 1);
    let projected = batches[0].record_batch().unwrap();
    assert_eq!(projected.schema().fields().len(), 1);
    assert_eq!(projected.schema().field(0).name(), "VendorID");
    assert_eq!(projected.num_rows(), 1);
    assert_eq!(
        projected
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );

    let partial_scan = resource.negotiate(&request).unwrap();
    let mut partial_reader = resource
        .planned_partition_reader(partial_scan.external_task_set().unwrap())
        .unwrap();
    let partial_executable = partial_reader.next_partition(0).unwrap().unwrap();
    let mut partial =
        futures_executor::block_on(resource.open_executable(partial_executable)).unwrap();
    assert!(
        futures_executor::block_on(partial.try_next())
            .unwrap()
            .is_some()
    );
    let partial_io = futures_executor::block_on(partial.terminate_and_join_with_source_io())
        .unwrap()
        .unwrap();
    assert!(partial_io.physical_bytes > 0);
    assert!(partial_io.requests > 0);
}

#[test]
fn compiled_logical_projection_maps_to_physical_source_names() {
    let schema = Schema::new(vec![cdf_kernel::with_source_name(
        Field::new("vendor_id", DataType::Int64, false),
        "VendorID",
    )]);
    assert_eq!(
        physical_projection_names(&schema, Some(&["vendor_id".to_owned()])).unwrap(),
        Some(vec!["VendorID".to_owned()])
    );
}

#[test]
fn compiled_logical_predicate_maps_to_physical_source_names() {
    let schema = Schema::new(vec![cdf_kernel::with_source_name(
        Field::new("vendor_id", DataType::Int64, false),
        "VendorID",
    )]);
    let predicate = cdf_kernel::ScanPredicate::new(
        cdf_kernel::PredicateId::new("vendor-filter").unwrap(),
        "vendor_id = 7",
    )
    .unwrap();
    let physical = physical_predicates(&schema, &[predicate]).unwrap();
    assert_eq!(
        physical[0]
            .canonical_expression
            .comparison()
            .map(|(name, _, _)| name),
        Some("VendorID")
    );
}

#[test]
fn adaptive_range_policy_requires_a_strict_subset_projection() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]);
    assert_eq!(
        planned_file_access_coverage(&CompiledScanIntent::full_scan(), &schema),
        PlannedFileAccessCoverage::Full
    );
    let predicate = cdf_kernel::ScanPredicate::new(
        cdf_kernel::PredicateId::new("id-filter").unwrap(),
        "id > 7",
    )
    .unwrap();
    let predicate_only = CompiledScanIntent {
        version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
        projection: None,
        predicates: vec![PushedPredicate {
            predicate,
            fidelity: PushdownFidelity::Exact,
        }],
        limit: None,
        order_by: Vec::new(),
    };
    assert_eq!(
        planned_file_access_coverage(&predicate_only, &schema),
        PlannedFileAccessCoverage::Full
    );
    let all_columns = CompiledScanIntent {
        projection: Some(vec!["id".to_owned(), "payload".to_owned()]),
        ..CompiledScanIntent::full_scan()
    };
    assert_eq!(
        planned_file_access_coverage(&all_columns, &schema),
        PlannedFileAccessCoverage::Full
    );
    let subset = CompiledScanIntent {
        projection: Some(vec!["id".to_owned()]),
        ..CompiledScanIntent::full_scan()
    };
    assert_eq!(
        planned_file_access_coverage(&subset, &schema),
        PlannedFileAccessCoverage::Selective
    );
}

#[test]
fn exact_predicate_negotiation_requires_the_shared_physical_lowering() {
    let descriptor = cdf_format_parquet::ParquetFormatDriver::new()
        .unwrap()
        .descriptor()
        .clone();
    let integer_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let integer_request = ScanRequest {
        resource_id: ResourceId::new("events").unwrap(),
        projection: None,
        filters: vec![
            cdf_kernel::ScanPredicate::new(
                cdf_kernel::PredicateId::new("id-filter").unwrap(),
                "id > 7",
            )
            .unwrap(),
        ],
        limit: None,
        order_by: Vec::new(),
        scope: ScopeKey::Resource,
    };
    let integer = compile_file_scan(&integer_request, &descriptor, &integer_schema).unwrap();
    assert_eq!(integer.pushed_predicates.len(), 1);
    assert!(integer.unsupported_predicates.is_empty());

    let timestamp_schema = Schema::new(vec![Field::new(
        "observed_at",
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        false,
    )]);
    let timestamp_request = ScanRequest {
        filters: vec![
            cdf_kernel::ScanPredicate::new(
                cdf_kernel::PredicateId::new("time-filter").unwrap(),
                "observed_at >= '2026-07-16T00:00:00Z'",
            )
            .unwrap(),
        ],
        ..integer_request.clone()
    };
    let timestamp = compile_file_scan(&timestamp_request, &descriptor, &timestamp_schema).unwrap();
    assert!(timestamp.pushed_predicates.is_empty());
    assert_eq!(timestamp.unsupported_predicates.len(), 1);

    let hostile = cdf_kernel::ScanPredicate::from_expression(
        cdf_kernel::PredicateId::new("hostile-version").unwrap(),
        "id = 7",
        cdf_kernel::DeclarativeExpression::new(DeclarativeExpressionNode::Call {
            function: cdf_kernel::DeclarativeFunctionReference {
                namespace: "other".to_owned(),
                name: "eq".to_owned(),
                version: "999".to_owned(),
            },
            arguments: vec![
                DeclarativeExpressionNode::Column {
                    name: "id".to_owned(),
                },
                DeclarativeExpressionNode::Literal {
                    value: cdf_kernel::DeclarativeExpressionLiteral::Signed(7),
                },
            ],
        }),
    )
    .unwrap();
    let hostile_request = ScanRequest {
        filters: vec![hostile],
        ..integer_request
    };
    let hostile = compile_file_scan(&hostile_request, &descriptor, &integer_schema).unwrap();
    assert!(hostile.pushed_predicates.is_empty());
    assert_eq!(hostile.unsupported_predicates.len(), 1);
}

#[test]
fn gzip_parquet_composes_transform_spool_with_registered_format_driver() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(0..10_000))],
    )
    .unwrap();
    let parquet = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&parquet).unwrap();
    let compressed = encoder.finish().unwrap();
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("events.parquet.gz"), compressed).unwrap();
    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    );
    let plan = FileResourcePlan {
        source: "events".to_owned(),
        root: root.path().to_string_lossy().into_owned(),
        glob: "events.parquet.gz".to_owned(),
        format: Some(FileFormatDeclaration::parquet()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::auto(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("events.raw").unwrap();
    let resolved = dependencies
        .with_transport(|transport, egress| {
            resolve_file_matches(
                &resource_id,
                &plan,
                transport,
                egress,
                dependencies.formats(),
                dependencies.transforms(),
            )
        })
        .unwrap();
    assert_eq!(resolved[0].compression.mode_name(), "gzip");
    assert_eq!(resolved[0].format.extension.as_deref(), Some("parquet"));
    let probe = discover_local_binary_schema(
        root.path().join("events.parquet.gz"),
        "events.parquet.gz",
        &dependencies,
        0,
        SchemaDiscoveryRequest {
            resource_id: &resource_id,
            format: plan.resolved_format().unwrap(),
            format_declared: plan.format_declared,
            format_options: &plan.format_options,
            discovery_kind: cdf_runtime::FormatDiscoveryKind::FormatMetadata,
            transform_name: "gzip",
            maximum_bytes: 64 * 1024 * 1024,
            maximum_records: 1_000,
            cancellation: cdf_runtime::RunCancellation::default(),
        },
    )
    .unwrap();
    assert_eq!(probe.schema.as_ref(), schema.as_ref());
    assert_eq!(probe.source_identity.get("compression").unwrap(), "gzip");
    let stable_id = probe.source_identity.get("stable_id").unwrap();
    assert!(
        stable_id.ends_with("events.parquet.gz") && !stable_id.contains('#'),
        "unexpected transformed stable id: {stable_id}"
    );
    assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 1);
    std::fs::remove_file(root.path().join("events.parquet.gz")).unwrap();
    let stream = stream_file_match_blocking(
        &resolved[0],
        plan.resolved_format().unwrap(),
        ReadOptions::new(resource_id, PartitionId::new("file-events").unwrap()),
        &dependencies,
        Arc::clone(&probe.schema),
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 0);
    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        10_000
    );
    drop(batches);
    assert_eq!(
        dependencies.execution().memory().snapshot().current_bytes,
        0
    );
}

#[test]
fn remote_arrow_ipc_file_streams_directly_through_registered_driver() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let store = Arc::new(InMemory::new());
    futures_executor::block_on(store.put(
        &ObjectPath::from("prod/events.arrow"),
        PutPayload::from(bytes.clone()),
    ))
    .unwrap();
    let facade = FileTransportFacade::new()
        .with_object_store("s3://ipc", store)
        .with_execution_services(crate::test_execution_services());
    let dependencies = FileRuntimeDependencies::new(
        facade,
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    )
    .with_max_spool_bytes(1)
    .unwrap();
    let plan = FileResourcePlan {
        source: "ipc".to_owned(),
        root: "s3://ipc/prod".to_owned(),
        glob: "events.arrow".to_owned(),
        format: Some(FileFormatDeclaration::arrow_ipc()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("ipc.events").unwrap();
    let resolved = dependencies
        .with_transport(|transport, egress| {
            resolve_remote_matches(
                &resource_id,
                &plan,
                transport,
                egress,
                crate::test_format_registry().as_ref(),
                crate::test_transform_registry().as_ref(),
            )
        })
        .unwrap();
    let stream = stream_file_match_blocking(
        &resolved[0],
        plan.resolved_format().unwrap(),
        ReadOptions::new(resource_id, PartitionId::new("file-ipc").unwrap()),
        &dependencies,
        Arc::clone(&schema),
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].header.row_count, 3);
}

#[test]
fn local_open_rejects_planned_generation_mismatch_before_hashing() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("events.ndjson");
    fs::write(&path, b"{\"id\":1}\n").unwrap();
    let plan = FileResourcePlan {
        source: "local".to_owned(),
        root: root.path().to_string_lossy().into_owned(),
        glob: "events.ndjson".to_owned(),
        format: Some(FileFormatDeclaration::ndjson()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    );
    let resource_id = ResourceId::new("local.events").unwrap();
    let mut resolved = resolved_file_match(
        &resource_id,
        root.path(),
        fs::canonicalize(&path).unwrap(),
        &plan,
        dependencies.formats(),
        dependencies.transforms(),
    )
    .unwrap();
    resolved.source_generation = Some("local-v1:stale-planned-generation".to_owned());
    let driver = dependencies.formats().resolve("ndjson").unwrap();
    let canonical_options = driver.canonical_options(serde_json::json!({})).unwrap();

    let error = prepare_file_input(PrepareFileInputRequest {
        resource_id: &resource_id,
        resolved: &resolved,
        source_access: cdf_runtime::FormatSourceAccess::Sequential,
        access_coverage: PlannedFileAccessCoverage::Full,
        driver: driver.as_ref(),
        canonical_format_options: &canonical_options,
        dependencies: &dependencies,
        cancellation: &cdf_runtime::RunCancellation::default(),
    })
    .err()
    .expect("stale local plan must fail before extraction hashing");

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("changed between planning and open"));
}

#[test]
fn remote_parquet_uses_admitted_spool_or_generation_bound_ranges() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..100_000)),
            Arc::new(Int64Array::from_iter_values(100_000..200_000)),
        ],
    )
    .unwrap();
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    futures_executor::block_on(store.put(
        &ObjectPath::from("prod/events.parquet"),
        PutPayload::from(bytes.clone()),
    ))
    .unwrap();
    let facade = FileTransportFacade::new()
        .with_object_store("s3://parquet", Arc::clone(&store))
        .with_execution_services(crate::test_execution_services());
    let dependencies = FileRuntimeDependencies::new(
        facade,
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    )
    .with_max_spool_bytes(bytes.len() as u64)
    .unwrap();
    let plan = FileResourcePlan {
        source: "parquet".to_owned(),
        root: "s3://parquet/prod".to_owned(),
        glob: "events.parquet".to_owned(),
        format: Some(FileFormatDeclaration::parquet()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("parquet.events").unwrap();
    let resolved = dependencies
        .with_transport(|transport, egress| {
            resolve_remote_matches(
                &resource_id,
                &plan,
                transport,
                egress,
                dependencies.formats(),
                dependencies.transforms(),
            )
        })
        .unwrap();
    let driver = dependencies.formats().resolve("parquet").unwrap();
    let canonical_options = driver.canonical_options(serde_json::json!({})).unwrap();
    assert!(matches!(
        prepare_file_input(PrepareFileInputRequest {
            resource_id: &resource_id,
            resolved: &resolved[0],
            source_access: cdf_runtime::FormatSourceAccess::Adaptive,
            access_coverage: PlannedFileAccessCoverage::Selective,
            driver: driver.as_ref(),
            canonical_format_options: &canonical_options,
            dependencies: &dependencies,
            cancellation: &cdf_runtime::RunCancellation::default(),
        })
        .unwrap()
        .input,
        PreparedFileInput::Source(_)
    ));
    assert!(matches!(
        prepare_file_input(PrepareFileInputRequest {
            resource_id: &resource_id,
            resolved: &resolved[0],
            source_access: cdf_runtime::FormatSourceAccess::Adaptive,
            access_coverage: PlannedFileAccessCoverage::Full,
            driver: driver.as_ref(),
            canonical_format_options: &canonical_options,
            dependencies: &dependencies,
            cancellation: &cdf_runtime::RunCancellation::default(),
        })
        .unwrap()
        .input,
        PreparedFileInput::SpoolSource { .. }
    ));

    let prepared = prepare_file_input(PrepareFileInputRequest {
        resource_id: &resource_id,
        resolved: &resolved[0],
        source_access: cdf_runtime::FormatSourceAccess::Adaptive,
        access_coverage: PlannedFileAccessCoverage::Full,
        driver: driver.as_ref(),
        canonical_format_options: &canonical_options,
        dependencies: &dependencies,
        cancellation: &cdf_runtime::RunCancellation::default(),
    })
    .unwrap();
    let source_io = prepared.source_io.clone();
    let PreparedFileInput::SpoolSource { source, size_bytes } = prepared.input else {
        panic!("full remote Parquet scan must select a seekable spool input")
    };
    let dependencies_for_complete = dependencies.clone();
    dependencies
        .execution()
        .run_io(async move {
            ready_spooled_file_input(SpoolInputRequest {
                source,
                size_bytes,
                mode: crate::FileSpoolMode::Complete,
                source_io: source_io.clone(),
                payload_cache_key: None,
                dependencies: &dependencies_for_complete,
                cancellation: cdf_runtime::RunCancellation::default(),
            })
            .await
            .map(|ready| (ready, source_io))
        })
        .map(|(ready, source_io)| {
            assert_eq!(source_io.snapshot().mode, Some(SourceReadMode::FullSpool));
            assert!(ready.source_completion.is_none());
        })
        .unwrap();

    struct WeakHttpTransport {
        path: PathBuf,
    }

    impl HttpFileTransport for WeakHttpTransport {
        fn send_headers(
            &self,
            _request: HttpFileRequest,
        ) -> BoxFuture<'static, Result<HttpFileResponse>> {
            Box::pin(async { Err(CdfError::internal("unused weak HTTP metadata probe")) })
        }

        fn open_byte_source(
            &self,
            _resource: &FileTransportResource,
            _expected: &FileIdentityMetadata,
            _auth: Option<ResolvedHttpAuth>,
            memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        ) -> Result<Arc<dyn ByteSource>> {
            Ok(Arc::new(LocalByteSource::open(&self.path, memory)?))
        }
    }

    let weak_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(weak_file.path(), &bytes).unwrap();
    let weak_dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new().with_http_transport(WeakHttpTransport {
            path: weak_file.path().to_path_buf(),
        }),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    )
    .with_max_spool_bytes(bytes.len() as u64)
    .unwrap();
    let weak = ResolvedFileMatch {
        open: ResolvedFileOpen::Transport(FileTransportResource::http_url(
            "https://weak.example/events.parquet",
        )),
        path_text: "https://weak.example/events.parquet".to_owned(),
        size_bytes: bytes.len() as u64,
        source_generation: None,
        identity_strength: GenerationStrength::Weak,
        sha256: None,
        etag: None,
        version: None,
        modified_ms: None,
        exact_ranges: false,
        bytes_loaded: None,
        compression: resolved[0].compression.clone(),
        format: resolved[0].format.clone(),
    };
    let weak_driver = weak_dependencies.formats().resolve("parquet").unwrap();
    let weak_options = weak_driver
        .canonical_options(serde_json::json!({}))
        .unwrap();
    let weak_input = prepare_file_input(PrepareFileInputRequest {
        resource_id: &resource_id,
        resolved: &weak,
        source_access: cdf_runtime::FormatSourceAccess::Adaptive,
        access_coverage: PlannedFileAccessCoverage::Selective,
        driver: weak_driver.as_ref(),
        canonical_format_options: &weak_options,
        dependencies: &weak_dependencies,
        cancellation: &cdf_runtime::RunCancellation::default(),
    })
    .unwrap();
    assert!(matches!(
        &weak_input.input,
        PreparedFileInput::SpoolSource { .. }
    ));
    let weak_partition = PreparedFilePartition {
        resolved: weak,
        input: weak_input.input,
        scan_intent: CompiledScanIntent {
            projection: Some(vec!["id".to_owned()]),
            ..CompiledScanIntent::full_scan()
        },
        options: ReadOptions::new(
            resource_id.clone(),
            PartitionId::new("file-parquet-weak").unwrap(),
        ),
        admission_schema: Arc::clone(&schema),
        physical_schema_authority: PhysicalSchemaAuthority::default(),
        canonical_format_options: weak_options,
        driver: weak_driver,
        source_io: weak_input.source_io,
        extraction_content_hash: weak_input.extraction_content_hash,
        hash_sweep_source: weak_input.hash_sweep_source,
        payload_retention: weak_input.payload_retention,
        payload_cache_key: weak_input.payload_cache_key,
        spool_mode: crate::FileSpoolMode::Overlap,
    };
    let dependencies_for_weak = weak_dependencies.clone();
    let weak_stream = weak_dependencies
        .execution()
        .run_io(async move {
            stream_prepared_file_match(
                weak_partition,
                &dependencies_for_weak,
                cdf_runtime::RunCancellation::default(),
            )
            .await
        })
        .unwrap();
    assert!(weak_stream.source_completion.is_none());
    let weak_batches = futures_executor::block_on(weak_stream.batches.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        weak_batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        100_000
    );
    assert!(weak_batches.iter().all(|batch| {
        batch
            .record_batch()
            .is_some_and(|batch| batch.schema().fields().len() == 1)
    }));
    drop(weak_batches);
    assert_eq!(
        weak_dependencies
            .execution()
            .memory()
            .snapshot()
            .current_bytes,
        0
    );
    let weak_spill = weak_dependencies.execution().spill().snapshot();
    assert!(weak_spill.peak_bytes >= bytes.len() as u64);
    assert_eq!(weak_spill.current_bytes, 0);
    let stream = stream_file_match_blocking(
        &resolved[0],
        plan.resolved_format().unwrap(),
        ReadOptions::new(
            resource_id.clone(),
            PartitionId::new("file-parquet").unwrap(),
        ),
        &dependencies,
        Arc::clone(&schema),
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        100_000
    );
    drop(batches);
    assert_eq!(
        dependencies.execution().memory().snapshot().current_bytes,
        0
    );
    let spill = dependencies.execution().spill().snapshot();
    assert!(spill.peak_bytes >= bytes.len() as u64);
    assert_eq!(spill.current_bytes, 0);

    let constrained = FileRuntimeDependencies::new(
        FileTransportFacade::new()
            .with_object_store("s3://parquet", Arc::clone(&store))
            .with_execution_services(crate::test_execution_services()),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    )
    .with_max_spool_bytes(1)
    .unwrap();
    let constrained_matches = constrained
        .with_transport(|transport, egress| {
            resolve_remote_matches(
                &resource_id,
                &plan,
                transport,
                egress,
                constrained.formats(),
                constrained.transforms(),
            )
        })
        .unwrap();
    let driver = constrained.formats().resolve("parquet").unwrap();
    let canonical_options = driver.canonical_options(serde_json::json!({})).unwrap();
    assert!(matches!(
        prepare_file_input(PrepareFileInputRequest {
            resource_id: &resource_id,
            resolved: &constrained_matches[0],
            source_access: cdf_runtime::FormatSourceAccess::Adaptive,
            access_coverage: PlannedFileAccessCoverage::Full,
            driver: driver.as_ref(),
            canonical_format_options: &canonical_options,
            dependencies: &constrained,
            cancellation: &cdf_runtime::RunCancellation::default(),
        })
        .unwrap()
        .input,
        PreparedFileInput::SpoolSource { .. }
    ));
    let stream = stream_file_match_blocking(
        &constrained_matches[0],
        plan.resolved_format().unwrap(),
        ReadOptions::new(resource_id, PartitionId::new("file-parquet-range").unwrap()),
        &constrained,
        schema,
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        100_000
    );
    drop(batches);
    let spill = constrained.execution().spill().snapshot();
    assert_eq!(spill.current_bytes, 0);
    assert_eq!(spill.peak_bytes, 0);

    let contended = FileRuntimeDependencies::new(
        FileTransportFacade::new()
            .with_object_store("s3://parquet", store)
            .with_execution_services(crate::test_execution_services()),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    )
    .with_max_spool_bytes(bytes.len() as u64)
    .unwrap();
    let contended_matches = contended
        .with_transport(|transport, egress| {
            resolve_remote_matches(
                &ResourceId::new("parquet.contended").unwrap(),
                &plan,
                transport,
                egress,
                contended.formats(),
                contended.transforms(),
            )
        })
        .unwrap();
    let spill = contended.execution().spill();
    let budget = spill.snapshot().budget_bytes;
    let remaining = u64::try_from(bytes.len()).unwrap().saturating_sub(1);
    let held = spill
        .try_reserve(budget.saturating_sub(remaining))
        .unwrap()
        .unwrap();
    let stream = stream_file_match_blocking(
        &contended_matches[0],
        plan.resolved_format().unwrap(),
        ReadOptions::new(
            ResourceId::new("parquet.contended").unwrap(),
            PartitionId::new("file-parquet-contended").unwrap(),
        ),
        &contended,
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        100_000
    );
    drop(batches);
    assert_eq!(spill.snapshot().current_bytes, held.bytes());
    drop(held);
    assert_eq!(spill.snapshot().current_bytes, 0);
}

#[test]
fn opt_in_payload_cache_reuses_strong_remote_generation_and_misses_after_change() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(0..100_000))],
    )
    .unwrap();
    let first_bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    let changed_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(100_000..200_000))],
    )
    .unwrap();
    let changed_bytes =
        cdf_package::transcode_record_batches_to_parquet_bytes(&[changed_batch]).unwrap();
    let store = Arc::new(InMemory::new());
    let object_path = ObjectPath::from("prod/events.parquet");
    futures_executor::block_on(store.put(&object_path, PutPayload::from(first_bytes.clone())))
        .unwrap();
    let payload_opens = Arc::new(AtomicUsize::new(0));
    let listings = Arc::new(AtomicUsize::new(0));
    let transport = PayloadOpenCountingTransport {
        inner: FileTransportFacade::new()
            .with_object_store("s3://cache", store.clone())
            .with_execution_services(crate::test_execution_services()),
        payload_opens: Arc::clone(&payload_opens),
        metadata_reads: Arc::new(AtomicUsize::new(0)),
        listings: Arc::clone(&listings),
    };
    let cache_root = tempfile::tempdir().unwrap();
    let maximum_object_bytes = first_bytes.len().max(changed_bytes.len()) as u64;
    let dependencies = FileRuntimeDependencies::new(
        transport,
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    )
    .with_max_spool_bytes(maximum_object_bytes)
    .unwrap()
    .with_payload_cache(
        FilePayloadCache::new(
            cache_root.path().join("v1"),
            4,
            maximum_object_bytes.saturating_mul(4),
        )
        .unwrap(),
    );
    let plan = FileResourcePlan {
        source: "cache".to_owned(),
        root: "s3://cache/prod".to_owned(),
        glob: "events.parquet".to_owned(),
        format: Some(FileFormatDeclaration::parquet()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("cache.events").unwrap();
    let resolve_current = || {
        dependencies
            .with_transport(|transport, egress| {
                resolve_remote_matches(
                    &resource_id,
                    &plan,
                    transport,
                    egress,
                    dependencies.formats(),
                    dependencies.transforms(),
                )
            })
            .unwrap()
            .remove(0)
    };
    let run = |resolved: &ResolvedFileMatch, partition: &str| {
        let stream = stream_file_match_blocking(
            resolved,
            plan.resolved_format().unwrap(),
            ReadOptions::new(resource_id.clone(), PartitionId::new(partition).unwrap()),
            &dependencies,
            Arc::clone(&schema),
            PhysicalSchemaAuthority::default(),
        )
        .unwrap();
        futures_executor::block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap()
    };

    let first_resolved = resolve_current();
    let first = run(&first_resolved, "file-cache-first");
    assert_eq!(payload_opens.load(Ordering::Relaxed), 1);

    let second_resolved = resolve_current();
    let driver = dependencies.formats().resolve("parquet").unwrap();
    let canonical_options = driver.canonical_options(serde_json::json!({})).unwrap();
    let cached_input = prepare_file_input(PrepareFileInputRequest {
        resource_id: &resource_id,
        resolved: &second_resolved,
        source_access: cdf_runtime::FormatSourceAccess::Adaptive,
        access_coverage: PlannedFileAccessCoverage::Full,
        driver: driver.as_ref(),
        canonical_format_options: &canonical_options,
        dependencies: &dependencies,
        cancellation: &cdf_runtime::RunCancellation::default(),
    })
    .unwrap();
    assert_eq!(
        cached_input.source_io.snapshot().mode,
        Some(SourceReadMode::PayloadCache)
    );
    assert!(matches!(cached_input.input, PreparedFileInput::Source(_)));
    let second = run(&second_resolved, "file-cache-second");
    assert_eq!(payload_opens.load(Ordering::Relaxed), 1);
    assert!(listings.load(Ordering::Relaxed) >= 2);
    assert_eq!(first.len(), second.len());
    for (left, right) in first.iter().zip(&second) {
        assert_eq!(left.record_batch(), right.record_batch());
    }

    let cache_object = std::fs::read_dir(cache_root.path().join("v1/objects"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&cache_object, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    std::fs::write(&cache_object, vec![0_u8; first_bytes.len()]).unwrap();
    let corrupt_resolved = resolve_current();
    let recovered = run(&corrupt_resolved, "file-cache-corrupt-fallback");
    assert_eq!(payload_opens.load(Ordering::Relaxed), 2);
    assert_eq!(first.len(), recovered.len());
    for (left, right) in first.iter().zip(&recovered) {
        assert_eq!(left.record_batch(), right.record_batch());
    }

    futures_executor::block_on(store.put(&object_path, PutPayload::from(changed_bytes))).unwrap();
    let changed_resolved = resolve_current();
    assert_ne!(first_resolved.etag, changed_resolved.etag);
    let changed = run(&changed_resolved, "file-cache-changed");
    assert_eq!(payload_opens.load(Ordering::Relaxed), 3);
    let first_value = first[0]
        .record_batch()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let changed_value = changed[0]
        .record_batch()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_ne!(first_value, changed_value);
    drop((first, second, recovered, changed));
    assert_eq!(
        dependencies.execution().memory().snapshot().current_bytes,
        0
    );
    assert_eq!(dependencies.execution().spill().snapshot().current_bytes, 0);
}

#[test]
fn disabled_payload_cache_adds_no_spool_hash_pass() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), b"uncached-hot-path").unwrap();
    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    );
    let source: Arc<dyn ByteSource> =
        Arc::new(LocalByteSource::open(file.path(), dependencies.execution().memory()).unwrap());
    let execution = dependencies.execution().clone();
    let spool_dependencies = dependencies.clone();
    let spool = execution
        .run_io(async move {
            spool_byte_source_async(
                source,
                Some(17),
                None,
                &spool_dependencies,
                cdf_runtime::RunCancellation::default(),
            )
            .await
        })
        .unwrap();

    assert_eq!(spool.bytes(), 17);
    assert!(spool.sha256().is_none());
}

#[test]
fn object_store_recursive_glob_resolves_stable_multi_file_partitions() {
    let store = Arc::new(InMemory::new());
    for path in [
        "prod/2026/01/events.parquet",
        "prod/2026/02/nested/events.parquet",
        "prod/2025/events.parquet",
    ] {
        futures_executor::block_on(store.put(
            &ObjectPath::from(path),
            PutPayload::from_static(b"PAR1payloadPAR1"),
        ))
        .unwrap();
    }
    let transport = FileTransportFacade::new()
        .with_object_store("s3://acme-events", store)
        .with_execution_services(crate::test_execution_services());
    let plan = FileResourcePlan {
        source: "events".to_owned(),
        root: "s3://acme-events/prod".to_owned(),
        glob: "2026/**/*.parquet".to_owned(),
        format: Some(FileFormatDeclaration::parquet()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("events.raw").unwrap();

    let matches = resolve_remote_matches(
        &resource_id,
        &plan,
        &transport,
        &crate::test_egress_scope(),
        crate::test_format_registry().as_ref(),
        crate::test_transform_registry().as_ref(),
    )
    .unwrap();
    assert_eq!(matches.len(), 2);
    assert_eq!(
        matches
            .iter()
            .map(|file| file.path_text.as_str())
            .collect::<Vec<_>>(),
        vec![
            "s3://acme-events/prod/2026/01/events.parquet",
            "s3://acme-events/prod/2026/02/nested/events.parquet",
        ]
    );
    assert!(matches.iter().all(|file| file.etag.is_some()));
}

struct StreamingOnlyRemoteListingTransport {
    memory: Arc<cdf_memory::DeterministicMemoryCoordinator>,
    locations: Vec<String>,
}

impl FileTransport for StreamingOnlyRemoteListingTransport {
    fn metadata(
        &self,
        _egress: &cdf_runtime::SourceEgressScope,
        _resource: &FileTransportResource,
        _control: &FileTransportControl,
    ) -> Result<FileMetadataObservation> {
        Err(CdfError::internal(
            "streaming-listing fixture does not support metadata",
        ))
    }

    fn list(
        &self,
        _egress: &cdf_runtime::SourceEgressScope,
        _resource: &FileTransportResource,
        _maximum_results: usize,
        _control: &FileTransportControl,
    ) -> Result<FileIdentityStream> {
        let memory = Arc::clone(&self.memory);
        let stream = futures_util::stream::iter(self.locations.clone().into_iter().enumerate())
            .then(move |(index, location)| {
                let memory = Arc::clone(&memory);
                async move {
                    let lease = cdf_memory::reserve(
                        memory,
                        cdf_memory::ReservationRequest::new(
                            cdf_memory::ConsumerKey::new(
                                format!("streaming-listing-{index}"),
                                cdf_memory::MemoryClass::Discovery,
                            )?,
                            FILE_IDENTITY_MEMORY_ENVELOPE_BYTES,
                        )?,
                    )
                    .await?;
                    AccountedFileIdentity::new(
                        FileIdentityMetadata {
                            location,
                            size_bytes: Some(4),
                            checksum: None,
                            etag: Some(format!("\"listing-{index}\"")),
                            version: None,
                            modified: None,
                            exact_ranges: true,
                        },
                        lease,
                    )
                }
            });
        Ok(FileIdentityStream::materialized(stream))
    }

    fn open_byte_source(
        &self,
        _egress: &cdf_runtime::SourceEgressScope,
        _resource: &FileTransportResource,
        _expected: &FileIdentityMetadata,
        _memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    ) -> Result<Arc<dyn ByteSource>> {
        Err(CdfError::internal(
            "streaming-listing fixture does not open payload",
        ))
    }
}

#[test]
fn remote_listing_filters_without_materializing_all_metadata() {
    let memory = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(
            FILE_IDENTITY_MEMORY_ENVELOPE_BYTES,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let mut locations = (0..64)
        .map(|index| format!("s3://acme-events/prod/2025/nonmatch-{index:02}.parquet"))
        .collect::<Vec<_>>();
    locations.push("s3://acme-events/prod/2026/keep.parquet".to_owned());
    locations.extend(
        (0..64).map(|index| format!("s3://acme-events/prod/2027/nonmatch-{index:02}.parquet")),
    );
    let transport = StreamingOnlyRemoteListingTransport {
        memory: Arc::clone(&memory),
        locations,
    };
    let plan = FileResourcePlan {
        source: "events".to_owned(),
        root: "s3://acme-events/prod".to_owned(),
        glob: "2026/*.parquet".to_owned(),
        format: Some(FileFormatDeclaration::parquet()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("events.raw").unwrap();

    let matches = resolve_remote_matches(
        &resource_id,
        &plan,
        &transport,
        &crate::test_egress_scope(),
        crate::test_format_registry().as_ref(),
        crate::test_transform_registry().as_ref(),
    )
    .unwrap();

    assert_eq!(matches.len(), 1);
    assert_eq!(
        matches[0].path_text,
        "s3://acme-events/prod/2026/keep.parquet"
    );
    assert_eq!(memory.snapshot().current_bytes, 0);
}

#[test]
fn remote_inventory_never_reads_payload_for_format_or_compression_detection() {
    let store = Arc::new(InMemory::new());
    futures_executor::block_on(store.put(
        &ObjectPath::from("prod/events.ndjson.gz"),
        PutPayload::from_static(b"not payload CDF should inspect during inventory"),
    ))
    .unwrap();
    let payload_opens = Arc::new(AtomicUsize::new(0));
    let metadata_reads = Arc::new(AtomicUsize::new(0));
    let listings = Arc::new(AtomicUsize::new(0));
    let transport = PayloadOpenCountingTransport {
        inner: FileTransportFacade::new()
            .with_object_store("s3://events", store)
            .with_execution_services(crate::test_execution_services()),
        payload_opens: Arc::clone(&payload_opens),
        metadata_reads,
        listings,
    };
    let plan = FileResourcePlan {
        source: "events".to_owned(),
        root: "s3://events/prod".to_owned(),
        glob: "events.ndjson.gz".to_owned(),
        format: Some(FileFormatDeclaration::ndjson()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::auto(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };

    let matches = resolve_remote_matches(
        &ResourceId::new("events.raw").unwrap(),
        &plan,
        &transport,
        &crate::test_egress_scope(),
        crate::test_format_registry().as_ref(),
        crate::test_transform_registry().as_ref(),
    )
    .unwrap();

    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].compression.mode_name(), "gzip");
    assert_eq!(matches[0].format.format_id, "ndjson");
    assert_eq!(
        matches[0].format.detection.confidence,
        FormatDetectionConfidence::None
    );
    assert_eq!(payload_opens.load(Ordering::Relaxed), 0);
}

#[test]
fn planned_object_partitions_revalidate_exact_objects_without_relisting_the_glob() {
    let store = Arc::new(InMemory::new());
    for path in ["prod/2026/01/events.parquet", "prod/2026/02/events.parquet"] {
        futures_executor::block_on(store.put(
            &ObjectPath::from(path),
            PutPayload::from_static(b"PAR1fixture"),
        ))
        .unwrap();
    }
    let listings = Arc::new(AtomicUsize::new(0));
    let metadata_reads = Arc::new(AtomicUsize::new(0));
    let transport = PayloadOpenCountingTransport {
        inner: FileTransportFacade::new()
            .with_object_store("s3://events", store)
            .with_execution_services(crate::test_execution_services()),
        payload_opens: Arc::new(AtomicUsize::new(0)),
        metadata_reads: Arc::clone(&metadata_reads),
        listings: Arc::clone(&listings),
    };
    let plan = FileResourcePlan {
        source: "events".to_owned(),
        root: "s3://events/prod".to_owned(),
        glob: "2026/**/*.parquet".to_owned(),
        format: Some(FileFormatDeclaration::parquet()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let empty_schema = Schema::empty();
    let descriptor = ResourceDescriptor {
        resource_id: ResourceId::new("events.raw").unwrap(),
        schema_source: cdf_kernel::SchemaSource::Declared {
            schema_hash: cdf_kernel::canonical_arrow_schema_hash(&empty_schema).unwrap(),
            source: "test".to_owned(),
        },
        primary_key: Vec::new(),
        merge_key: Vec::new(),
        cursor: None,
        write_disposition: WriteDisposition::Append,
        deduplication: None,
        contract: None,
        state_scope: ScopeKey::Resource,
        freshness: None,
        trust_level: cdf_kernel::TrustLevel::Governed,
    };
    let formats = crate::test_format_registry();
    let transforms = crate::test_transform_registry();
    let partitions = file_partitions_for_plan_with_transport(
        &descriptor,
        &plan,
        &CompiledScanIntent::full_scan(),
        FilePlanningContext {
            transport: &transport,
            egress: &crate::test_egress_scope(),
            formats: formats.as_ref(),
            transforms: transforms.as_ref(),
            maximum_matches: usize::MAX,
            control: &FileTransportControl::default(),
            execution: crate::test_execution_services(),
        },
    )
    .unwrap();
    assert_eq!(partitions.len(), 2);
    assert_eq!(listings.load(Ordering::Relaxed), 1);

    for partition in &partitions {
        let egress = crate::test_egress_scope();
        let control = FileTransportControl::default();
        validate_partition(
            &descriptor,
            &plan,
            partition,
            FileResolutionContext {
                transport: &transport,
                egress: &egress,
                formats: formats.as_ref(),
                transforms: transforms.as_ref(),
                control: &control,
            },
        )
        .unwrap();
    }
    assert_eq!(listings.load(Ordering::Relaxed), 1);
    assert_eq!(metadata_reads.load(Ordering::Relaxed), 2);
}

#[test]
fn http_numeric_template_expands_finitely_and_preserves_width() {
    let resource_id = ResourceId::new("tlc.yellow").unwrap();
    assert_eq!(
        expand_http_glob(&resource_id, "yellow_tripdata_2024-{01..03}.parquet").unwrap(),
        vec![
            "yellow_tripdata_2024-01.parquet",
            "yellow_tripdata_2024-02.parquet",
            "yellow_tripdata_2024-03.parquet",
        ]
    );
    assert_eq!(
        expand_http_glob(&resource_id, "yellow_tripdata_2024-*.parquet").unwrap(),
        (1..=12)
            .map(|month| format!("yellow_tripdata_2024-{month:02}.parquet"))
            .collect::<Vec<_>>()
    );
    let error = expand_http_glob(&resource_id, "yellow_tripdata_*.parquet").unwrap_err();
    assert!(error.message.contains("HTTP has no LIST operation"));
}

#[test]
fn http_numeric_template_membership_revalidates_one_path_without_expansion() {
    let resource_id = ResourceId::new("archive.events").unwrap();
    let glob = "part-{000000..999999}.parquet";

    assert!(http_glob_contains(&resource_id, glob, "part-000000.parquet").unwrap());
    assert!(http_glob_contains(&resource_id, glob, "part-999999.parquet").unwrap());
    assert!(!http_glob_contains(&resource_id, glob, "part-1000000.parquet").unwrap());
    assert!(!http_glob_contains(&resource_id, glob, "part-1.parquet").unwrap());
    assert!(!http_glob_contains(&resource_id, glob, "other-000001.parquet").unwrap());
}

#[test]
fn object_store_gzip_ndjson_streams_without_spill_and_preserves_remote_position() {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(b"{\"id\":1}\n{\"id\":2}\n").unwrap();
    let encoded = encoder.finish().unwrap();
    let store = Arc::new(InMemory::new());
    futures_executor::block_on(store.put(
        &ObjectPath::from("prod/2026/events.ndjson.gz"),
        PutPayload::from(encoded.clone()),
    ))
    .unwrap();
    let facade = FileTransportFacade::new()
        .with_object_store("s3://acme-events", store)
        .with_execution_services(crate::test_execution_services());
    let dependencies = FileRuntimeDependencies::new(
        facade,
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    )
    .with_max_spool_bytes(encoded.len() as u64)
    .unwrap();
    let transport = dependencies.transport();
    let plan = FileResourcePlan {
        source: "events".to_owned(),
        root: "s3://acme-events/prod".to_owned(),
        glob: "2026/**/*.ndjson.gz".to_owned(),
        format: Some(FileFormatDeclaration::ndjson()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::auto(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("events.raw").unwrap();
    let resolved = resolve_remote_matches(
        &resource_id,
        &plan,
        transport.as_ref(),
        &crate::test_egress_scope(),
        crate::test_format_registry().as_ref(),
        crate::test_transform_registry().as_ref(),
    )
    .unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].compression.mode_name(), "gzip");
    let options = ReadOptions::new(resource_id, PartitionId::new("file-events").unwrap());
    let admission_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let stream = stream_file_match_blocking(
        &resolved[0],
        plan.resolved_format().unwrap(),
        options.clone(),
        &dependencies,
        Arc::clone(&admission_schema),
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        2
    );
    let SourcePosition::FileManifest(position) =
        batches[0].header.source_position.as_ref().unwrap()
    else {
        panic!("expected remote file manifest position")
    };
    assert_eq!(
        position.files[0].path,
        "s3://acme-events/prod/2026/events.ndjson.gz"
    );

    let constrained = dependencies.with_max_spool_bytes(1).unwrap();
    let stream = stream_file_match_blocking(
        &resolved[0],
        plan.resolved_format().unwrap(),
        options,
        &constrained,
        admission_schema,
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    let constrained_batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        constrained_batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        2
    );
    drop(constrained_batches);
    let spill = constrained.execution().spill().snapshot();
    assert_eq!(spill.current_bytes, 0);
    assert_eq!(spill.peak_bytes, 0);
}

#[test]
fn local_csv_discovers_and_streams_through_registered_driver() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("events.csv");
    std::fs::write(&path, b"id,name\n1,alpha\n2,beta\n").unwrap();
    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    );
    let plan = FileResourcePlan {
        source: "events".to_owned(),
        root: root.path().to_string_lossy().into_owned(),
        glob: "events.csv".to_owned(),
        format: Some(FileFormatDeclaration::csv()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("events.csv").unwrap();
    let resolved = dependencies
        .with_transport(|transport, egress| {
            resolve_file_matches(
                &resource_id,
                &plan,
                transport,
                egress,
                dependencies.formats(),
                dependencies.transforms(),
            )
        })
        .unwrap();
    let probe = discover_local_binary_schema(
        &path,
        "events.csv",
        &dependencies,
        0,
        SchemaDiscoveryRequest {
            resource_id: &resource_id,
            format: plan.resolved_format().unwrap(),
            format_declared: plan.format_declared,
            format_options: &plan.format_options,
            discovery_kind: cdf_runtime::FormatDiscoveryKind::BoundedContent,
            transform_name: "none",
            maximum_bytes: 1024 * 1024,
            maximum_records: 1_000,
            cancellation: cdf_runtime::RunCancellation::default(),
        },
    )
    .unwrap();
    assert_eq!(probe.schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(probe.schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 1);
    std::fs::remove_file(&path).unwrap();
    let stream = stream_file_match_blocking(
        &resolved[0],
        plan.resolved_format().unwrap(),
        ReadOptions::new(resource_id, PartitionId::new("csv-file").unwrap()),
        &dependencies,
        Arc::clone(&probe.schema),
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        2
    );
    assert!(matches!(
        batches[0].header.source_position,
        Some(SourcePosition::FileManifest(_))
    ));
    assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 0);
    drop(batches);
    assert_eq!(
        dependencies.execution().memory().snapshot().current_bytes,
        0
    );
}

#[test]
fn local_fixed_width_streams_through_registered_driver() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("events.fixed"),
        b"0001 Alice\n0002 Bob  \n",
    )
    .unwrap();
    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    );
    let plan = FileResourcePlan {
        source: "events".to_owned(),
        root: root.path().to_string_lossy().into_owned(),
        glob: "events.fixed".to_owned(),
        format: Some(FileFormatDeclaration::named("fixed_width").unwrap()),
        format_declared: true,
        format_options: serde_json::json!({
            "layout_version": 1,
            "unit": "bytes",
            "encoding": "utf8",
            "line_ending": "lf",
            "trim": "ascii",
            "null_tokens": [],
            "record_width": 10,
            "fields": [
                {"name": "id", "start": 0, "end": 4},
                {"name": "name", "start": 5, "end": 10}
            ],
            "required_gaps": [{"start": 4, "end": 5}],
            "max_record_bytes": 1024
        }),
        schema_discovery: Some(cdf_runtime::FormatDiscoveryKind::FormatMetadata),
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("events.fixed").unwrap();
    let resolved = dependencies
        .with_transport(|transport, egress| {
            resolve_file_matches(
                &resource_id,
                &plan,
                transport,
                egress,
                dependencies.formats(),
                dependencies.transforms(),
            )
        })
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let stream = stream_file_match_with_options_blocking(
        &resolved[0],
        plan.resolved_format().unwrap(),
        plan.format_options.clone(),
        ReadOptions::new(resource_id, PartitionId::new("fixed-file").unwrap()),
        &dependencies,
        schema,
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        2
    );
    let record_batch = batches[0].record_batch().unwrap();
    assert_eq!(
        record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1),
        2
    );
    assert_eq!(
        record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(1),
        "Bob"
    );
    drop(batches);
    assert_eq!(
        dependencies.execution().memory().snapshot().current_bytes,
        0
    );
}

#[test]
fn local_ndjson_full_content_discovery_replays_the_same_source_for_extraction() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("events.ndjson");
    std::fs::write(
        &path,
        b"{\"id\":1,\"name\":\"alpha\"}\n{\"id\":2,\"name\":\"beta\",\"late\":true}\n",
    )
    .unwrap();
    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    );
    let plan = FileResourcePlan {
        source: "events".to_owned(),
        root: root.path().to_string_lossy().into_owned(),
        glob: "events.ndjson".to_owned(),
        format: Some(FileFormatDeclaration::ndjson()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: Some(cdf_runtime::FormatDiscoveryKind::FullContent),
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("events.ndjson").unwrap();
    let resolved = dependencies
        .with_transport(|transport, egress| {
            resolve_file_matches(
                &resource_id,
                &plan,
                transport,
                egress,
                dependencies.formats(),
                dependencies.transforms(),
            )
        })
        .unwrap();
    let probe = discover_local_binary_schema(
        &path,
        "events.ndjson",
        &dependencies,
        0,
        SchemaDiscoveryRequest {
            resource_id: &resource_id,
            format: plan.resolved_format().unwrap(),
            format_declared: plan.format_declared,
            format_options: &plan.format_options,
            discovery_kind: cdf_runtime::FormatDiscoveryKind::FullContent,
            transform_name: "none",
            maximum_bytes: 8,
            maximum_records: 1,
            cancellation: cdf_runtime::RunCancellation::default(),
        },
    )
    .unwrap();
    assert_eq!(probe.schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(probe.schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(probe.schema.field(2).data_type(), &DataType::Boolean);
    assert_eq!(probe.source_identity["content_coverage"], "full_content");
    assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 1);
    std::fs::remove_file(&path).unwrap();

    let stream = stream_file_match_blocking(
        &resolved[0],
        plan.resolved_format().unwrap(),
        ReadOptions::new(resource_id, PartitionId::new("ndjson-file").unwrap()),
        &dependencies,
        Arc::clone(&probe.schema),
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();

    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        2
    );
    assert!(matches!(
        batches[0].header.source_position,
        Some(SourcePosition::FileManifest(_))
    ));
    assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 0);
    drop(batches);
    assert_eq!(
        dependencies.execution().memory().snapshot().current_bytes,
        0
    );
}

#[test]
fn retained_sequential_window_replays_then_continues_one_source_invocation() {
    struct ChunkedTestSource {
        identity: ContentIdentity,
        capabilities: ByteSourceCapabilities,
        payload: Arc<Vec<u8>>,
        memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        opens: Arc<AtomicUsize>,
        chunk_bytes: usize,
    }

    impl ByteSource for ChunkedTestSource {
        fn identity(&self) -> &ContentIdentity {
            &self.identity
        }

        fn capabilities(&self) -> &ByteSourceCapabilities {
            &self.capabilities
        }

        fn open_sequential(
            &self,
            request: SequentialReadRequest,
        ) -> BoxFuture<'_, Result<AccountedByteStream>> {
            let payload = Arc::clone(&self.payload);
            let memory = Arc::clone(&self.memory);
            let opens = Arc::clone(&self.opens);
            let chunk_bytes = self.chunk_bytes;
            Box::pin(async move {
                request.cancellation.check()?;
                if opens.fetch_add(1, Ordering::Relaxed) != 0 {
                    return Err(CdfError::data("test source was opened more than once"));
                }
                let state = (0_usize, payload, memory, request.cancellation);
                Ok(Box::pin(futures_util::stream::try_unfold(
                    state,
                    move |(offset, payload, memory, cancellation)| async move {
                        cancellation.check()?;
                        if offset == payload.len() {
                            return Ok(None);
                        }
                        let end = offset.saturating_add(chunk_bytes).min(payload.len());
                        let bytes = bytes::Bytes::copy_from_slice(&payload[offset..end]);
                        let lease = cdf_memory::reserve(
                            Arc::clone(&memory),
                            cdf_memory::ReservationRequest::new(
                                cdf_memory::ConsumerKey::new(
                                    "retained-window-test-source",
                                    MemoryClass::Source,
                                )?,
                                u64::try_from(bytes.len())
                                    .map_err(|_| CdfError::data("test source chunk exceeds u64"))?,
                            )?,
                        )
                        .await?;
                        let chunk = cdf_memory::AccountedBytes::new(bytes, lease)?;
                        Ok(Some((chunk, (end, payload, memory, cancellation))))
                    },
                )) as AccountedByteStream)
            })
        }

        fn read_exact_range(
            &self,
            _extent: ByteExtent,
            _cancellation: cdf_runtime::RunCancellation,
        ) -> BoxFuture<'_, Result<cdf_memory::AccountedBytes>> {
            Box::pin(async {
                Err(CdfError::contract(
                    "sequential test source does not support ranges",
                ))
            })
        }
    }

    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    );
    let payload = b"first-window|second-window|live-continuation".to_vec();
    let opens = Arc::new(AtomicUsize::new(0));
    let source: Arc<dyn ByteSource> = Arc::new(ChunkedTestSource {
        identity: ContentIdentity {
            stable_id: "retained-window-test".to_owned(),
            size_bytes: Some(payload.len() as u64),
            generation: Some("test-generation".to_owned()),
            checksum: None,
            strength: GenerationStrength::Strong,
        },
        capabilities: ByteSourceCapabilities {
            known_length: true,
            reopenable: false,
            seekable: false,
            exact_ranges: false,
            useful_range_concurrency: 0,
            minimum_chunk_bytes: 1,
            maximum_chunk_bytes: 1024,
        },
        payload: Arc::new(payload.clone()),
        memory: dependencies.execution().memory(),
        opens: Arc::clone(&opens),
        chunk_bytes: 13,
    });
    let observed = dependencies
        .execution()
        .run_io({
            let dependencies = dependencies.clone();
            async move {
                let capture = SequentialPayloadCapture::new(source, &dependencies).await?;
                let discovery_source = capture.discovery_source();
                assert!(!discovery_source.capabilities().reopenable);
                let mut discovery = discovery_source
                    .open_sequential(SequentialReadRequest {
                        preferred_chunk_bytes: 13,
                        cancellation: cdf_runtime::RunCancellation::default(),
                    })
                    .await?;
                let first = discovery.try_next().await?.ok_or_else(|| {
                    CdfError::internal("test discovery stream omitted first chunk")
                })?;
                assert_eq!(first.payload(), b"first-window|");
                drop(first);
                drop(discovery);

                let prepared = capture.finish(None).await?;
                let (prepared, retention) =
                    prepared.into_typed::<PreparedFilePayload>("retained-window test execution")?;
                assert_eq!(retention.bytes(), 13);
                assert!(prepared.source_content_digest.is_none());
                let mut execution = prepared
                    .source
                    .open_sequential(SequentialReadRequest {
                        preferred_chunk_bytes: 7,
                        cancellation: cdf_runtime::RunCancellation::default(),
                    })
                    .await?;
                let mut observed = Vec::new();
                while let Some(chunk) = execution.try_next().await? {
                    observed.extend_from_slice(chunk.payload());
                }
                drop(execution);
                drop(retention);
                Ok::<_, CdfError>(observed)
            }
        })
        .unwrap();
    assert_eq!(observed, payload);
    assert_eq!(opens.load(Ordering::Relaxed), 1);
    assert_eq!(
        dependencies.execution().memory().snapshot().current_bytes,
        0
    );
    assert_eq!(dependencies.execution().spill().snapshot().current_bytes, 0);
}

#[test]
fn bounded_and_full_content_drivers_share_the_retained_stream_handoff() {
    let mut descriptor = ExternalMockFormat::new().descriptor().clone();
    assert!(retains_sequential_discovery_payload(
        &descriptor,
        cdf_runtime::FormatDiscoveryKind::BoundedContent
    ));

    descriptor.discovery = cdf_runtime::FormatDiscoveryCapabilities::only(
        cdf_runtime::FormatDiscoveryKind::FullContent,
    );
    assert!(retains_sequential_discovery_payload(
        &descriptor,
        cdf_runtime::FormatDiscoveryKind::FullContent
    ));

    descriptor.discovery = cdf_runtime::FormatDiscoveryCapabilities::only(
        cdf_runtime::FormatDiscoveryKind::FormatMetadata,
    );
    assert!(!retains_sequential_discovery_payload(
        &descriptor,
        cdf_runtime::FormatDiscoveryKind::FormatMetadata
    ));
    descriptor.discovery = cdf_runtime::FormatDiscoveryCapabilities::only(
        cdf_runtime::FormatDiscoveryKind::FullContent,
    );
    descriptor.source_access = cdf_runtime::FormatSourceAccess::Adaptive;
    assert!(!retains_sequential_discovery_payload(
        &descriptor,
        cdf_runtime::FormatDiscoveryKind::FullContent
    ));
}

#[test]
fn local_json_document_discovers_and_streams_through_registered_driver() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("events.json");
    std::fs::write(
        &path,
        br#"[{"id":1,"name":"alpha },["},{"id":2,"name":"beta"}]"#,
    )
    .unwrap();
    let dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new(),
        crate::test_execution_services(),
        crate::test_format_registry(),
        crate::test_transform_registry(),
        crate::test_egress_scope(),
    );
    let plan = FileResourcePlan {
        source: "events".to_owned(),
        root: root.path().to_string_lossy().into_owned(),
        glob: "events.json".to_owned(),
        format: Some(FileFormatDeclaration::json()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };
    let resource_id = ResourceId::new("events.json").unwrap();
    let resolved = dependencies
        .with_transport(|transport, egress| {
            resolve_file_matches(
                &resource_id,
                &plan,
                transport,
                egress,
                dependencies.formats(),
                dependencies.transforms(),
            )
        })
        .unwrap();
    let probe = discover_local_binary_schema(
        &path,
        "events.json",
        &dependencies,
        0,
        SchemaDiscoveryRequest {
            resource_id: &resource_id,
            format: plan.resolved_format().unwrap(),
            format_declared: plan.format_declared,
            format_options: &plan.format_options,
            discovery_kind: cdf_runtime::FormatDiscoveryKind::BoundedContent,
            transform_name: "none",
            maximum_bytes: 1024 * 1024,
            maximum_records: 1_000,
            cancellation: cdf_runtime::RunCancellation::default(),
        },
    )
    .unwrap();
    assert_eq!(probe.schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(probe.schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 1);
    std::fs::remove_file(&path).unwrap();
    let stream = stream_file_match_blocking(
        &resolved[0],
        plan.resolved_format().unwrap(),
        ReadOptions::new(resource_id, PartitionId::new("json-file").unwrap()),
        &dependencies,
        Arc::clone(&probe.schema),
        PhysicalSchemaAuthority::default(),
    )
    .unwrap();
    let batches = futures_executor::block_on(stream.collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        2
    );
    assert!(matches!(
        batches[0].header.source_position,
        Some(SourcePosition::FileManifest(_))
    ));
    assert_eq!(dependencies.prepared_payloads().pending_count().unwrap(), 0);
    drop(batches);
    assert_eq!(
        dependencies.execution().memory().snapshot().current_bytes,
        0
    );
}

#[test]
fn explicit_format_wins_over_an_ambiguous_extension() {
    let formats = crate::test_format_registry();
    let resource_id = ResourceId::new("events.rows").unwrap();
    let mut plan = FileResourcePlan {
        source: "events".to_owned(),
        root: ".".to_owned(),
        glob: "events.json".to_owned(),
        format: Some(FileFormatDeclaration::ndjson()),
        format_declared: true,
        format_options: serde_json::json!({}),
        schema_discovery: None,
        compression: FileCompressionDeclaration::none(),
        spool_mode: crate::FileSpoolMode::Overlap,
        auth: None,
        credentials: None,
        allowlist: cdf_http::EgressAllowlist::allow_any(),
    };

    validate_format_extension(
        &resource_id,
        &plan,
        "events.json",
        Some("json"),
        formats.as_ref(),
    )
    .unwrap();

    plan.format_declared = false;
    let error = validate_format_extension(
        &resource_id,
        &plan,
        "events.json",
        Some("json"),
        formats.as_ref(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("extension `.json` selects `json`")
    );
}
#[test]
fn format_confirmation_shares_the_configured_discovery_byte_budget() {
    let context = FormatConfirmationContext {
        resource_id: ResourceId::new("events.raw").unwrap(),
        location: "https://example.test/events.parquet".to_owned(),
        format_declared: false,
        transform_name: "none".to_owned(),
    };
    assert_eq!(
        discovery_budget_after_confirmation(1_024, 24, &context).unwrap(),
        1_000
    );
    for confirmation_bytes in [1_024, 1_025] {
        assert!(discovery_budget_after_confirmation(1_024, confirmation_bytes, &context).is_err());
    }
    assert_eq!(
        schema_observation_byte_limit(
            1,
            1_024,
            &context,
            cdf_runtime::FormatDiscoveryKind::FullContent,
        )
        .unwrap(),
        1
    );
}
