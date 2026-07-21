use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::Poll,
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampMillisecondArray,
    builder::{Int32Builder, MapBuilder, StringBuilder, StringDictionaryBuilder},
    types::Int32Type,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_contract::{
    ContractPolicy, DedupKeep, Expression, FieldCoercionDecision, NestedDataPolicy, ObservedSchema,
    RESIDUAL_ENCODING_METADATA_KEY, RESIDUAL_ENCODING_NAME, RedactionDecision, RowRule,
    SchemaChangeKind, SchemaEvolutionMode, VARIANT_COLUMN_NAME, VARIANT_SEMANTIC_TAG,
    VerdictAction, compile_resource_validation_program, compile_validation_program,
    reconcile_schema,
};
use cdf_kernel::{
    BackpressureSupport, Batch, BatchHeader, BatchId, BatchStream, CapabilitySupport, ContractRef,
    CursorPosition, CursorValue, DeduplicationSpec, DeliveryGuarantee,
    DiscoveryExecutorBudgetEvidence, DiscoveryManifestHash, DiscoveryManifestReference,
    DrainTermination, EXECUTION_EXTENT_VERSION, EffectiveSchemaCatalogEntry,
    EffectiveSchemaEvidence, EffectiveSchemaObservationEvidence, EffectiveSchemaRuntime,
    EpochClosureTrigger, EstimateSupport, EventTimeDomain, ExecutionExtent, FileManifest,
    FilePosition, FilterCapabilities, FreshnessSpec, IncrementalShape, LateDataAction,
    PLAN_PHYSICAL_SCHEMA_HASH_KEY, PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionAttestation, PartitionAuthority, PartitionId,
    PartitionPlan, PartitioningCapabilities, PreContractObservedValue, PreContractQuarantineFact,
    PreContractResidualCandidate, PredicateId, PushdownFidelity, QueryableResource,
    ResourceCapabilities, ResourceDescriptor, ResourceId, ResourceStream, Result, RunId, RunPhase,
    RunPhaseStatus, STRATIFIED_HASH_SELECTOR_V1, STREAM_EPOCH_POLICY_VERSION, SafeFrontierPolicy,
    ScanPlan, ScanPredicate, ScanRequest, SchemaBaselineReference, SchemaHash,
    SchemaObservationFieldQuarantine, SchemaObservationPolicy, SchemaSnapshotReference,
    SchemaSource, ScopeKey, SourcePosition, StreamEpochPolicy, TerminalSchemaObservationQuarantine,
    TrustLevel, WATERMARK_CLAIM_VERSION, WatermarkAuthority, WatermarkClaim,
    WatermarkObservationContext, WatermarkPolicy, WatermarkValue, WriteDisposition, source_name,
    with_semantic,
};
use cdf_package_contract::{
    DEDUP_SUMMARY_FILE, LATE_DATA_PAYLOAD_CATALOG_FILE, LateDataPayloadCatalog,
    LateDataPayloadLocation, PackageStatus, QuarantineObservedValue, SegmentEntry,
};
use datafusion::{
    catalog::TableProvider, physical_plan::common::collect as collect_stream, prelude::*,
};
use futures_executor::block_on;
use futures_util::{StreamExt, stream};
use tempfile::TempDir;
use tracing::{
    Event, Id, Metadata, Subscriber,
    field::{Field as TracingField, Visit},
    span::{Attributes, Record},
};

fn collect_quarantine_records(
    reader: &cdf_package::PackageReader,
) -> Vec<cdf_package_contract::QuarantineRecord> {
    let mut records = Vec::new();
    reader
        .for_each_quarantine_record(&mut |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
    records
}

fn collect_dedup_dropped_provenance(reader: &cdf_package::PackageReader) -> Vec<(u64, u64)> {
    let mut rows = Vec::new();
    reader
        .for_each_dedup_dropped_provenance(&mut |dropped, kept| {
            rows.push((dropped, kept));
            Ok(())
        })
        .unwrap();
    rows
}

fn package_identity_file_paths(reader: &cdf_package::PackageReader) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    reader
        .for_each_identity_file(&mut |entry| {
            paths.insert(entry.path);
            Ok(())
        })
        .unwrap();
    paths
}

fn package_identity_segments(reader: &cdf_package::PackageReader) -> Vec<SegmentEntry> {
    let mut segments = Vec::new();
    reader
        .for_each_identity_segment(&mut |entry| {
            segments.push(entry);
            Ok(())
        })
        .unwrap();
    segments
}

fn read_package_segment(
    reader: &cdf_package::PackageReader,
    segment_id: &cdf_kernel::SegmentId,
) -> Vec<RecordBatch> {
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new())
            .unwrap(),
    );
    reader
        .verified_canonical_segment_stream(memory, 128 * 1024 * 1024)
        .unwrap()
        .find_map(|segment| {
            let segment = segment.unwrap();
            (segment.entry.segment_id == *segment_id).then_some(segment.batches)
        })
        .unwrap_or_else(|| panic!("segment {segment_id} is not in the verified package"))
}

fn executable_mock_plan(plan: &EnginePlan, resource: &MockResource) -> Result<EnginePlan> {
    if plan.compiled_source_execution.is_some() {
        return Ok(plan.clone());
    }
    let source = match resource.compiled_source_plan.get() {
        Some(source) => source.clone(),
        None => {
            let source = mock_compiled_source_plan(resource, None);
            resource.bind_compiled_source(&source);
            source
        }
    };
    plan.clone().bind_compiled_source(&source)
}

fn executable_mock_options(config: EngineExecutionConfig) -> Result<EngineExecutionInvocation> {
    let config = if config.services.is_some() {
        config
    } else {
        let (_, services) =
            StandaloneExecutionHost::default_services(cdf_memory::DEFAULT_PROCESS_BUDGET_BYTES)?;
        config.with_execution_services(services)
    };
    Ok(config.new_invocation())
}

fn datafusion_test_services() -> cdf_runtime::ExecutionServices {
    StandaloneExecutionHost::default_services(64 * 1024 * 1024)
        .unwrap()
        .1
}

#[test]
fn reusable_engine_execution_config_creates_isolated_invocation_state() {
    let config = EngineExecutionConfig::default();
    let first = config.new_invocation();
    let second = config.new_invocation();

    first.cancellation.cancel();

    assert!(first.cancellation.is_cancelled());
    assert!(!second.cancellation.is_cancelled());
    assert!(first.source_retry_evidence().snapshot().unwrap().is_empty());
    assert!(
        second
            .source_retry_evidence()
            .snapshot()
            .unwrap()
            .is_empty()
    );
}

async fn execute_to_package(
    plan: &EnginePlan,
    resource: &MockResource,
    package_dir: impl AsRef<std::path::Path>,
) -> Result<EngineRunOutput> {
    let plan = executable_mock_plan(plan, resource)?;
    super::execute_to_package(&plan, resource, package_dir).await
}

async fn preview_resource(
    plan: &EnginePlan,
    resource: &MockResource,
    limits: EnginePreviewLimits,
) -> Result<EnginePreviewOutput> {
    let plan = executable_mock_plan(plan, resource)?;
    super::preview_resource(&plan, resource, limits).await
}

async fn execute_to_package_with_run_id(
    run_id: &RunId,
    plan: &EnginePlan,
    resource: &MockResource,
    package_dir: impl AsRef<std::path::Path>,
) -> Result<EngineRunOutput> {
    let plan = executable_mock_plan(plan, resource)?;
    super::execute_to_package_with_run_id(run_id, &plan, resource, package_dir).await
}

async fn execute_to_package_with_segment_positions(
    plan: &EnginePlan,
    resource: &MockResource,
    package_dir: impl AsRef<std::path::Path>,
) -> Result<EngineRunOutputWithSegmentPositions> {
    let plan = executable_mock_plan(plan, resource)?;
    super::execute_to_package_with_segment_positions(&plan, resource, package_dir).await
}

async fn execute_to_package_with_segment_positions_and_pre_finalize(
    plan: &EnginePlan,
    resource: &MockResource,
    package_dir: impl AsRef<std::path::Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
    options: EngineExecutionConfig,
) -> Result<EngineRunOutputWithSegmentPositions> {
    let plan = executable_mock_plan(plan, resource)?;
    let options = executable_mock_options(options)?;
    super::execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        resource,
        package_dir,
        pre_finalize,
        options,
    )
    .await
}

async fn execute_to_package_with_streaming_hooks<'a>(
    plan: &EnginePlan,
    resource: &MockResource,
    package_dir: impl AsRef<std::path::Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
    durable_segment: &'a mut DurableSegmentHook<'a>,
    stream_finalize: &'a mut StreamingFinalizeHook<'a>,
    options: EngineExecutionConfig,
) -> Result<EngineRunOutputWithSegmentPositions> {
    let plan = executable_mock_plan(plan, resource)?;
    let options = executable_mock_options(options)?;
    super::execute_to_package_with_streaming_hooks(
        &plan,
        resource,
        package_dir,
        pre_finalize,
        durable_segment,
        stream_finalize,
        options,
    )
    .await
}

use super::*;

struct MemoryWorkerCompilerArtifacts {
    values: BTreeMap<cdf_runtime::WorkerArtifactKind, Vec<u8>>,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
}

impl Default for MemoryWorkerCompilerArtifacts {
    fn default() -> Self {
        Self {
            values: BTreeMap::new(),
            memory: Arc::new(
                cdf_memory::DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new())
                    .unwrap(),
            ),
        }
    }
}

impl WorkerCompilerArtifactWriter for MemoryWorkerCompilerArtifacts {
    fn write(
        &mut self,
        kind: cdf_runtime::WorkerArtifactKind,
        canonical_bytes: &[u8],
    ) -> Result<cdf_runtime::WorkerArtifactReference> {
        let content_sha256 = format!(
            "sha256:{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(canonical_bytes)
        );
        self.values.insert(kind, canonical_bytes.to_vec());
        Ok(cdf_runtime::WorkerArtifactReference {
            kind,
            store_namespace: cdf_kernel::ContentStoreNamespace::new("engine-worker-test")?,
            object_key: cdf_kernel::ContentObjectKey::new(format!("compiler/{kind:?}.json"))?,
            byte_count: u64::try_from(canonical_bytes.len())
                .map_err(|_| cdf_kernel::CdfError::contract("test artifact exceeds u64"))?,
            content_sha256,
            provider_generation: Some(cdf_kernel::ContentProviderGeneration::new("generation-1")?),
        })
    }
}

impl EngineWorkerArtifactAuthority for MemoryWorkerCompilerArtifacts {
    fn memory(&self) -> Arc<dyn cdf_memory::MemoryCoordinator> {
        Arc::clone(&self.memory)
    }

    fn read_compiler_artifact(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_bytes: u64,
    ) -> Result<VerifiedWorkerCompilerArtifact> {
        let lease = reserve_worker_artifact_memory(
            &self.memory,
            reference.byte_count,
            maximum_bytes,
            cdf_memory::MemoryClass::Control,
        )?;
        let bytes =
            self.values.get(&reference.kind).cloned().ok_or_else(|| {
                cdf_kernel::CdfError::contract("test compiler artifact is missing")
            })?;
        VerifiedWorkerCompilerArtifact::new(
            reference,
            cdf_memory::AccountedBytes::new(bytes::Bytes::from(bytes), lease)?,
            reference.provider_generation.as_ref(),
            maximum_bytes,
        )
    }

    fn verify_output_artifact(
        &self,
        _reference: &cdf_runtime::WorkerArtifactReference,
        _maximum_encoded_bytes: u64,
        _maximum_decoded_bytes: u64,
    ) -> Result<cdf_runtime::VerifiedWorkerArtifactFacts> {
        Err(cdf_kernel::CdfError::internal(
            "compiler fixture contains no worker output artifacts",
        ))
    }

    fn read_prepared_segment(
        &self,
        _reference: &cdf_runtime::WorkerArtifactReference,
        _maximum_encoded_bytes: u64,
        _maximum_decoded_bytes: u64,
    ) -> Result<VerifiedPreparedSegmentArtifact> {
        Err(cdf_kernel::CdfError::internal(
            "compiler fixture contains no prepared segment artifacts",
        ))
    }

    fn read_canonical_segment(
        &self,
        _reference: &cdf_runtime::WorkerArtifactReference,
        _maximum_encoded_bytes: u64,
        _maximum_decoded_bytes: u64,
    ) -> Result<VerifiedCanonicalSegmentArtifact> {
        Err(cdf_kernel::CdfError::internal(
            "compiler fixture contains no canonical segment artifacts",
        ))
    }

    fn read_partition_evidence(
        &self,
        _reference: &cdf_runtime::WorkerArtifactReference,
        _maximum_bytes: u64,
    ) -> Result<VerifiedEnginePartitionEvidenceArtifact> {
        Err(cdf_kernel::CdfError::internal(
            "compiler fixture contains no partition evidence artifacts",
        ))
    }
}

type SharedEngineWorkerArtifactMap = BTreeMap<(String, String), bytes::Bytes>;
type SharedEngineWorkerLeaseMap =
    BTreeMap<(cdf_kernel::LeaseAuthorityDomainId, String), (cdf_runtime::WorkerLeaseState, i64)>;

#[derive(Clone)]
struct SharedEngineWorkerArtifacts {
    values: Arc<Mutex<SharedEngineWorkerArtifactMap>>,
    leases: Arc<Mutex<SharedEngineWorkerLeaseMap>>,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
}

impl Default for SharedEngineWorkerArtifacts {
    fn default() -> Self {
        Self {
            values: Arc::new(Mutex::new(BTreeMap::new())),
            leases: Arc::new(Mutex::new(BTreeMap::new())),
            memory: Arc::new(
                cdf_memory::DeterministicMemoryCoordinator::new(
                    2 * 1024 * 1024 * 1024,
                    BTreeMap::new(),
                )
                .unwrap(),
            ),
        }
    }
}

impl SharedEngineWorkerArtifacts {
    fn lease_key(
        domain: &cdf_kernel::LeaseAuthorityDomainId,
        scope: &cdf_kernel::ScopeKey,
    ) -> Result<(cdf_kernel::LeaseAuthorityDomainId, String)> {
        Ok((domain.clone(), cdf_runtime::artifact_hash(scope)?))
    }

    fn admit_lease(&self, lease: cdf_runtime::WorkerLeaseState, now_ms: i64) -> Result<()> {
        lease.validate()?;
        let key = Self::lease_key(&lease.lease_authority_domain_id, &lease.lease_scope)?;
        let mut leases = self.leases.lock().unwrap();
        match leases.get(&key) {
            Some((current, current_now_ms)) if current == &lease && *current_now_ms == now_ms => {}
            Some((current, _)) if current.fencing_token.get() < lease.fencing_token.get() => {
                leases.insert(key, (lease, now_ms));
            }
            Some(_) => {
                return Err(cdf_kernel::CdfError::contract(
                    "worker artifact lease authority cannot regress or rewrite an existing fence",
                ));
            }
            None => {
                leases.insert(key, (lease, now_ms));
            }
        }
        Ok(())
    }

    fn bytes_for(&self, reference: &cdf_runtime::WorkerArtifactReference) -> Result<bytes::Bytes> {
        self.values
            .lock()
            .unwrap()
            .get(&(
                reference.store_namespace.as_str().to_owned(),
                reference.object_key.as_str().to_owned(),
            ))
            .cloned()
            .ok_or_else(|| cdf_kernel::CdfError::contract("worker artifact is missing"))
    }

    fn accounted_bytes_for(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_bytes: u64,
        class: cdf_memory::MemoryClass,
    ) -> Result<cdf_memory::AccountedBytes> {
        let lease = reserve_worker_artifact_memory(
            &self.memory,
            reference.byte_count,
            maximum_bytes,
            class,
        )?;
        cdf_memory::AccountedBytes::new(self.bytes_for(reference)?, lease)
    }

    fn decoded_lease(&self, maximum_bytes: u64) -> Result<cdf_memory::MemoryLease> {
        reserve_worker_artifact_memory(
            &self.memory,
            maximum_bytes,
            maximum_bytes,
            cdf_memory::MemoryClass::Decode,
        )
    }

    fn object_state(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
    ) -> cdf_runtime::WorkerArtifactObjectState {
        match self.bytes_for(reference) {
            Ok(bytes) => cdf_runtime::WorkerArtifactObjectState::Present {
                content_sha256: format!(
                    "sha256:{:x}",
                    <sha2::Sha256 as sha2::Digest>::digest(bytes.as_ref())
                ),
                provider_generation: reference
                    .provider_generation
                    .clone()
                    .expect("memory worker artifacts always bind a generation"),
            },
            Err(_) => cdf_runtime::WorkerArtifactObjectState::Absent,
        }
    }

    fn atomic_write_authorized(
        &self,
        authorization: cdf_runtime::WorkerArtifactWriteAuthorization<'_>,
        bytes: cdf_memory::AccountedBytes,
    ) -> Result<cdf_runtime::VerifiedWorkerArtifactFacts> {
        let reference = &authorization.receipt().artifact;
        let observed = Self::reference(
            reference.kind,
            reference.store_namespace.as_str(),
            reference.object_key.as_str().to_owned(),
            bytes.payload(),
        )?;
        if &observed != reference {
            return Err(cdf_kernel::CdfError::contract(
                "authorized worker output bytes do not match their receipt",
            ));
        }

        // Test-provider analogue of an object-store conditional transaction. Lease/fence updates
        // use the same first lock, and object inspection plus mutation use the same second lock.
        let leases = self.leases.lock().unwrap();
        let (current_lease, now_ms) = leases
            .get(&Self::lease_key(
                &authorization.permit().lease_authority_domain_id,
                &authorization.permit().lease_scope,
            )?)
            .ok_or_else(|| {
                cdf_kernel::CdfError::contract(
                    "worker artifact provider has no current lease authority",
                )
            })?;
        let key = (
            reference.store_namespace.as_str().to_owned(),
            reference.object_key.as_str().to_owned(),
        );
        let mut values = self.values.lock().unwrap();
        let object_state =
            values
                .get(&key)
                .map_or(cdf_runtime::WorkerArtifactObjectState::Absent, |existing| {
                    cdf_runtime::WorkerArtifactObjectState::Present {
                        content_sha256: format!(
                            "sha256:{:x}",
                            <sha2::Sha256 as sha2::Digest>::digest(existing.as_ref())
                        ),
                        provider_generation: cdf_kernel::ContentProviderGeneration::new(
                            "memory-generation-1",
                        )
                        .unwrap(),
                    }
                });
        authorization.validate_provider_preconditions(current_lease, &object_state, *now_ms)?;
        if matches!(object_state, cdf_runtime::WorkerArtifactObjectState::Absent) {
            values.insert(key, bytes.into_retained_bytes());
        }
        drop(values);
        drop(leases);

        let row_count = match &authorization.receipt().role {
            cdf_runtime::WorkerArtifactRole::PreparedSegment { row_count, .. }
            | cdf_runtime::WorkerArtifactRole::CanonicalSegment { row_count, .. } => {
                Some(*row_count)
            }
            _ => None,
        };
        cdf_runtime::VerifiedWorkerArtifactFacts::new(reference.clone(), row_count)
    }

    fn reference(
        kind: cdf_runtime::WorkerArtifactKind,
        namespace: &str,
        key: String,
        bytes: &[u8],
    ) -> Result<cdf_runtime::WorkerArtifactReference> {
        Ok(cdf_runtime::WorkerArtifactReference {
            kind,
            store_namespace: cdf_kernel::ContentStoreNamespace::new(namespace)?,
            object_key: cdf_kernel::ContentObjectKey::new(key)?,
            byte_count: u64::try_from(bytes.len())
                .map_err(|_| cdf_kernel::CdfError::contract("worker artifact exceeds u64"))?,
            content_sha256: format!("sha256:{:x}", <sha2::Sha256 as sha2::Digest>::digest(bytes)),
            provider_generation: Some(cdf_kernel::ContentProviderGeneration::new(
                "memory-generation-1",
            )?),
        })
    }

    fn observed_output_rows(
        bytes: cdf_memory::AccountedBytes,
        decoded_lease: cdf_memory::MemoryLease,
        maximum_decoded_bytes: u64,
    ) -> Result<u64> {
        let bytes = bytes.into_retained_bytes();
        let mut reader = arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(cdf_kernel::CdfError::from)?;
        let rows =
            reader.try_fold(0_u64, |rows, batch| {
                let batch = batch?;
                if cdf_memory::record_batch_retained_bytes(&batch)? > maximum_decoded_bytes {
                    return Err(cdf_kernel::CdfError::data(
                        "worker segment batch exceeds its admitted decoded-memory budget",
                    ));
                }
                rows.checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    cdf_kernel::CdfError::data("worker segment row count exceeds u64")
                })?)
                .ok_or_else(|| cdf_kernel::CdfError::data("worker segment row count overflow"))
            })?;
        drop(decoded_lease);
        Ok(rows)
    }
}

fn reserve_worker_artifact_memory(
    memory: &Arc<dyn cdf_memory::MemoryCoordinator>,
    bytes: u64,
    maximum_bytes: u64,
    class: cdf_memory::MemoryClass,
) -> Result<cdf_memory::MemoryLease> {
    if bytes == 0 || bytes > maximum_bytes {
        return Err(cdf_kernel::CdfError::data(
            "worker artifact exceeds its admitted memory window",
        ));
    }
    let request = cdf_memory::ReservationRequest::new(
        cdf_memory::ConsumerKey::new("isolated-worker-artifact", class)?,
        bytes,
    )?;
    memory.try_reserve(&request)?.ok_or_else(|| {
        cdf_kernel::CdfError::data(
            "isolated worker artifact memory is exhausted; reduce jobs or raise the worker memory budget",
        )
    })
}

fn account_worker_artifact_bytes(
    memory: &Arc<dyn cdf_memory::MemoryCoordinator>,
    bytes: Vec<u8>,
    maximum_bytes: u64,
    class: cdf_memory::MemoryClass,
) -> Result<cdf_memory::AccountedBytes> {
    let byte_count = u64::try_from(bytes.len())
        .map_err(|_| cdf_kernel::CdfError::data("worker artifact exceeds u64"))?;
    let lease = reserve_worker_artifact_memory(memory, byte_count, maximum_bytes, class)?;
    cdf_memory::AccountedBytes::new(bytes::Bytes::from(bytes), lease)
}

fn prepared_segment_bytes(
    canonical_bytes: Vec<u8>,
    package_row_ord_start: u64,
    row_count: u64,
) -> Result<Vec<u8>> {
    let reader =
        arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(canonical_bytes), None)
            .map_err(cdf_kernel::CdfError::from)?;
    let canonical = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(cdf_kernel::CdfError::from)?;
    cdf_package_contract::validate_package_row_ord_batches(
        &canonical,
        package_row_ord_start,
        row_count,
    )?;
    let first = canonical
        .first()
        .ok_or_else(|| cdf_kernel::CdfError::data("canonical fixture segment is empty"))?;
    let logical_schema = Arc::new(cdf_package_contract::logical_output_schema(
        first.schema().as_ref(),
    )?);
    let logical = canonical
        .into_iter()
        .map(|batch| {
            arrow_array::RecordBatch::try_new(
                Arc::clone(&logical_schema),
                batch.columns()[..batch.num_columns() - 1].to_vec(),
            )
            .map_err(cdf_kernel::CdfError::from)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut bytes = Vec::new();
    cdf_package::encode_canonical_segment_ipc(&mut bytes, logical_schema.as_ref(), &logical)?;
    Ok(bytes)
}

impl WorkerCompilerArtifactWriter for SharedEngineWorkerArtifacts {
    fn write(
        &mut self,
        kind: cdf_runtime::WorkerArtifactKind,
        canonical_bytes: &[u8],
    ) -> Result<cdf_runtime::WorkerArtifactReference> {
        let digest = format!(
            "{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(canonical_bytes)
        );
        let reference = Self::reference(
            kind,
            "engine-worker-compiler",
            format!("compiler/{kind:?}/{digest}.json"),
            canonical_bytes,
        )?;
        self.values.lock().unwrap().insert(
            (
                reference.store_namespace.as_str().to_owned(),
                reference.object_key.as_str().to_owned(),
            ),
            bytes::Bytes::copy_from_slice(canonical_bytes),
        );
        Ok(reference)
    }
}

impl EngineWorkerArtifactAuthority for SharedEngineWorkerArtifacts {
    fn memory(&self) -> Arc<dyn cdf_memory::MemoryCoordinator> {
        Arc::clone(&self.memory)
    }

    fn read_compiler_artifact(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_bytes: u64,
    ) -> Result<VerifiedWorkerCompilerArtifact> {
        VerifiedWorkerCompilerArtifact::new(
            reference,
            self.accounted_bytes_for(reference, maximum_bytes, cdf_memory::MemoryClass::Control)?,
            reference.provider_generation.as_ref(),
            maximum_bytes,
        )
    }

    fn verify_output_artifact(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_encoded_bytes: u64,
        maximum_decoded_bytes: u64,
    ) -> Result<cdf_runtime::VerifiedWorkerArtifactFacts> {
        let bytes = self.accounted_bytes_for(
            reference,
            maximum_encoded_bytes,
            cdf_memory::MemoryClass::Package,
        )?;
        let observed = Self::reference(
            reference.kind,
            reference.store_namespace.as_str(),
            reference.object_key.as_str().to_owned(),
            bytes.payload(),
        )?;
        if &observed != reference {
            return Err(cdf_kernel::CdfError::contract(
                "stored worker output bytes do not match their result reference",
            ));
        }
        let row_count = matches!(
            reference.kind,
            cdf_runtime::WorkerArtifactKind::PreparedSegment
                | cdf_runtime::WorkerArtifactKind::CanonicalSegment
        )
        .then(|| {
            Self::observed_output_rows(
                bytes,
                self.decoded_lease(maximum_decoded_bytes)?,
                maximum_decoded_bytes,
            )
        })
        .transpose()?;
        cdf_runtime::VerifiedWorkerArtifactFacts::new(reference.clone(), row_count)
    }

    fn read_prepared_segment(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_encoded_bytes: u64,
        maximum_decoded_bytes: u64,
    ) -> Result<VerifiedPreparedSegmentArtifact> {
        VerifiedPreparedSegmentArtifact::new(
            reference,
            self.accounted_bytes_for(
                reference,
                maximum_encoded_bytes,
                cdf_memory::MemoryClass::Package,
            )?,
            self.decoded_lease(maximum_decoded_bytes)?,
            reference.provider_generation.as_ref(),
            maximum_encoded_bytes,
            maximum_decoded_bytes,
        )
    }

    fn read_canonical_segment(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_encoded_bytes: u64,
        maximum_decoded_bytes: u64,
    ) -> Result<VerifiedCanonicalSegmentArtifact> {
        VerifiedCanonicalSegmentArtifact::new(
            reference,
            self.accounted_bytes_for(
                reference,
                maximum_encoded_bytes,
                cdf_memory::MemoryClass::Package,
            )?,
            self.decoded_lease(maximum_decoded_bytes)?,
            reference.provider_generation.as_ref(),
            maximum_encoded_bytes,
            maximum_decoded_bytes,
        )
    }

    fn read_partition_evidence(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
        maximum_bytes: u64,
    ) -> Result<VerifiedEnginePartitionEvidenceArtifact> {
        VerifiedEnginePartitionEvidenceArtifact::new(
            reference,
            self.accounted_bytes_for(reference, maximum_bytes, cdf_memory::MemoryClass::Control)?,
            reference.provider_generation.as_ref(),
            maximum_bytes,
        )
    }
}

impl EngineWorkerOutputAuthority for SharedEngineWorkerArtifacts {
    fn reference_for_bytes(
        &self,
        kind: cdf_runtime::WorkerArtifactKind,
        namespace: &cdf_kernel::ContentStoreNamespace,
        object_key: cdf_kernel::ContentObjectKey,
        bytes: &[u8],
    ) -> Result<cdf_runtime::WorkerArtifactReference> {
        Self::reference(
            kind,
            namespace.as_str(),
            object_key.as_str().to_owned(),
            bytes,
        )
    }

    fn object_state(
        &self,
        reference: &cdf_runtime::WorkerArtifactReference,
    ) -> Result<cdf_runtime::WorkerArtifactObjectState> {
        Ok(Self::object_state(self, reference))
    }

    fn write_authorized_bytes(
        &self,
        authorization: cdf_runtime::WorkerArtifactWriteAuthorization<'_>,
        bytes: cdf_memory::AccountedBytes,
    ) -> Result<cdf_runtime::VerifiedWorkerArtifactFacts> {
        self.atomic_write_authorized(authorization, bytes)
    }
}

struct PendingEngineWorkerWrite<'a> {
    store: &'a SharedEngineWorkerArtifacts,
    bytes: Option<cdf_memory::AccountedBytes>,
}

impl cdf_runtime::WorkerAuthorizedArtifactSink for PendingEngineWorkerWrite<'_> {
    fn write_authorized(
        &mut self,
        authorization: cdf_runtime::WorkerArtifactWriteAuthorization<'_>,
    ) -> Result<cdf_runtime::VerifiedWorkerArtifactFacts> {
        let bytes = self
            .bytes
            .take()
            .ok_or_else(|| cdf_kernel::CdfError::internal("worker output was already consumed"))?;
        self.store.atomic_write_authorized(authorization, bytes)
    }
}

fn isolated_mock_option_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "source": {"type": "object", "additionalProperties": false, "properties": {}},
        "resource": {"type": "object", "additionalProperties": false, "properties": {}}
    })
}

#[derive(Clone)]
struct IsolatedEngineMockDriver {
    descriptor: cdf_runtime::SourceDriverDescriptor,
    option_schema: serde_json::Value,
    dataset_id: String,
    resource: MockResource,
}

impl cdf_runtime::SourceDriver for IsolatedEngineMockDriver {
    fn descriptor(&self) -> &cdf_runtime::SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn compile(
        &self,
        _request: cdf_runtime::SourceCompileRequest,
    ) -> Result<cdf_runtime::CompiledSourcePlan> {
        Err(cdf_kernel::CdfError::internal(
            "isolated engine fixture compiles its source explicitly",
        ))
    }

    fn validate_portable_plan(&self, plan: &cdf_runtime::CompiledSourcePlan) -> Result<()> {
        plan.validate()?;
        let expected_source_bytes =
            self.resource
                .batches
                .iter()
                .try_fold(0_u64, |total, batch| {
                    total.checked_add(batch.header.byte_count).ok_or_else(|| {
                        cdf_kernel::CdfError::data("isolated mock source byte count overflow")
                    })
                })?;
        if plan.physical_plan["dataset_id"].as_str() != Some(self.dataset_id.as_str())
            || plan.physical_plan["source_bytes"].as_u64() != Some(expected_source_bytes)
        {
            return Err(cdf_kernel::CdfError::contract(
                "isolated mock source authority does not match the worker-owned dataset",
            ));
        }
        Ok(())
    }

    fn verify_worker_source(
        &self,
        task: &cdf_runtime::PortablePartitionTask,
        plan: &cdf_runtime::CompiledSourcePlan,
        partition: &PartitionPlan,
        attestation: &cdf_runtime::WorkerSourceAttestation,
        observations: &[cdf_runtime::WorkerProcessedObservation],
    ) -> Result<cdf_runtime::VerifiedWorkerSourceFacts> {
        self.validate_portable_plan(plan)?;
        let batches = self
            .resource
            .batches
            .iter()
            .filter(|batch| batch.header.partition_id == partition.partition_id)
            .collect::<Vec<_>>();
        let first = batches.first().ok_or_else(|| {
            cdf_kernel::CdfError::contract(
                "isolated mock driver cannot verify an unknown partition",
            )
        })?;
        let physical_schema_hash = first.header.observed_schema_hash.clone();
        let processed_position = first.header.source_position.clone().ok_or_else(|| {
            cdf_kernel::CdfError::contract(
                "isolated mock driver requires an exact processed source position",
            )
        })?;
        if batches.iter().any(|batch| {
            batch.header.source_position.as_ref() != Some(&processed_position)
                || batch.header.observed_schema_hash != physical_schema_hash
        }) {
            return Err(cdf_kernel::CdfError::contract(
                "isolated mock partition batches disagree on position or schema",
            ));
        }
        let expected_observation_id = partition
            .metadata
            .get(PLAN_SCHEMA_OBSERVATION_ID_KEY)
            .map(String::as_str)
            .unwrap_or_else(|| partition.partition_id.as_str());
        let input_rows = batches.iter().try_fold(0_u64, |total, batch| {
            total
                .checked_add(batch.header.row_count)
                .ok_or_else(|| cdf_kernel::CdfError::data("mock input rows overflowed u64"))
        })?;
        let source_bytes = batches.iter().try_fold(0_u64, |total, batch| {
            total
                .checked_add(batch.header.byte_count)
                .ok_or_else(|| cdf_kernel::CdfError::data("mock source bytes overflowed u64"))
        })?;
        if task.partition.partition_id != partition.partition_id
            || attestation.processed_position
                != cdf_runtime::WorkerPosition::inline(processed_position.clone())?
            || attestation.physical_schema_hash != physical_schema_hash
            || observations.len() != 1
            || observations[0].observation_id != expected_observation_id
            || observations[0].source_position
                != cdf_runtime::WorkerPosition::inline(processed_position.clone())?
        {
            return Err(cdf_kernel::CdfError::contract(format!(
                "isolated worker source result exceeds registered driver authority: task_partition_match={}, position_match={}, schema_match={}, observation_count={}, observation_id={:?}, expected_observation_id={expected_observation_id:?}, observation_position_match={}",
                task.partition.partition_id == partition.partition_id,
                attestation.processed_position
                    == cdf_runtime::WorkerPosition::inline(processed_position.clone())?,
                attestation.physical_schema_hash == physical_schema_hash,
                observations.len(),
                observations
                    .first()
                    .map(|observation| observation.observation_id.as_str()),
                observations
                    .first()
                    .is_some_and(|observation| observation.source_position
                        == cdf_runtime::WorkerPosition::inline(processed_position.clone())
                            .expect("validated fixture position")),
            )));
        }
        cdf_runtime::VerifiedWorkerSourceFacts::new(
            cdf_runtime::WorkerPosition::inline(processed_position)?,
            physical_schema_hash,
            input_rows,
            source_bytes,
            true,
        )
    }

    fn health(
        &self,
        _request: cdf_runtime::SourceHealthRequest,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
        _output: &mut dyn cdf_runtime::SourceHealthSink,
    ) -> Result<()> {
        Err(cdf_kernel::CdfError::internal(
            "isolated engine fixture does not probe source health",
        ))
    }

    fn discovery_session(
        &self,
        _plan: &cdf_runtime::CompiledSourcePlan,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
    ) -> Result<Box<dyn cdf_runtime::SourceDiscoverySession>> {
        Err(cdf_kernel::CdfError::internal(
            "isolated engine fixture does not discover sources",
        ))
    }

    fn resolve(
        &self,
        plan: &cdf_runtime::CompiledSourcePlan,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        self.validate_portable_plan(plan)?;
        let resource = self.resource.clone();
        resource.bind_compiled_source(plan);
        Ok(Arc::new(resource))
    }
}

struct RejectingEngineWorkerSecrets;

impl cdf_http::SecretProvider for RejectingEngineWorkerSecrets {
    fn resolve(&self, _uri: &cdf_http::SecretUri) -> Result<cdf_http::SecretValue> {
        Err(cdf_kernel::CdfError::contract(
            "isolated mock source does not accept secrets",
        ))
    }
}

struct ActualEngineIsolatedExecutor<'a> {
    registry: &'a cdf_runtime::SourceRegistry,
    source_context: &'a cdf_runtime::SourceResolutionContext<'a>,
    services: cdf_runtime::ExecutionServices,
    artifacts: SharedEngineWorkerArtifacts,
    lease: cdf_runtime::WorkerLeaseState,
    package_root: &'a std::path::Path,
    now_ms: i64,
}

impl cdf_runtime::IsolatedPartitionExecutor for ActualEngineIsolatedExecutor<'_> {
    fn execute(
        &self,
        invocation: cdf_runtime::IsolatedPartitionInvocation,
    ) -> cdf_kernel::BoxFuture<'_, Result<cdf_runtime::PartitionWorkerResult>> {
        let result = (|| {
            let (task, attempt, authority) = invocation.into_parts();
            let program = authority.execution_program::<ReconstructedEngineWorkerProgram>()?;
            if program
                .plan()
                .scan
                .inline_partitions()
                .expect("isolated execution uses inline partition authority")
                .get(task.partition.canonical_partition_ordinal as usize)
                != Some(authority.partition())
            {
                return Err(cdf_kernel::CdfError::contract(
                    "local isolated execution partition does not match its full plan authority",
                ));
            }
            let resource = self
                .registry
                .resolve(authority.source(), self.source_context)?;
            let source_bytes = authority.source().physical_plan["partition_source_bytes"]
                [authority.partition().partition_id.as_str()]
            .as_u64()
            .ok_or_else(|| {
                cdf_kernel::CdfError::contract(
                    "isolated mock source plan omits its partition source byte count",
                )
            })?;
            let scheduler = cdf_runtime::resolve_runtime_scheduler(
                1,
                &authority.source().execution_capabilities,
                &cdf_runtime::DestinationRuntimeCapabilities::default(),
                &self.services,
                Some(task.resources.cpu_slots),
            )?;
            let plan = program.partition_execution_plan()?;
            let package_dir = self.package_root.join(&attempt.attempt_id);
            let execution_plan = plan.clone();
            let execution_package_dir = package_dir.clone();
            let execution_services = self.services.clone();
            let output = std::thread::scope(|scope| {
                scope
                    .spawn(move || {
                        let pre_finalize =
                            |_builder: &cdf_package::PackageBuilder,
                             _draft: EnginePackageDraft<'_>| Ok(());
                        let options = EngineExecutionConfig::default()
                            .with_execution_services(execution_services)
                            .with_scheduler_resolution(scheduler)
                            .new_invocation();
                        match &execution_plan.execution_extent {
                            ExecutionExtent::Bounded { .. } => block_on(
                                super::execute_to_package_with_segment_positions_and_pre_finalize(
                                    &execution_plan,
                                    resource.as_ref(),
                                    execution_package_dir,
                                    &pre_finalize,
                                    options,
                                ),
                            ),
                            ExecutionExtent::Drain { .. } => {
                                let mut controller = cdf_runtime::DrainEpochController::new(
                                    &execution_plan.execution_extent,
                                )?;
                                block_on(super::execute_drain_epoch_with_hooks(
                                    &execution_plan,
                                    resource.as_ref(),
                                    execution_package_dir,
                                    &pre_finalize,
                                    super::DrainEpochExecution::new(&mut controller),
                                    options,
                                ))?
                                .into_package()
                            }
                            ExecutionExtent::Resident { .. } => {
                                Err(cdf_kernel::CdfError::contract(
                                    "isolated resident execution is not enabled",
                                ))
                            }
                        }
                    })
                    .join()
                    .map_err(|_| {
                        cdf_kernel::CdfError::internal("isolated engine execution thread panicked")
                    })?
            })?;
            let processed = output.execution_evidence().processed_observations();
            let processed_position = if processed.is_empty() {
                output
                    .segment_positions
                    .iter()
                    .rev()
                    .find_map(|position| position.output_position.clone())
                    .ok_or_else(|| {
                        cdf_kernel::CdfError::data(
                            "isolated worker produced no exact processed source position",
                        )
                    })?
            } else {
                cdf_kernel::aggregate_processed_observation_positions(
                    None,
                    processed,
                    &plan.write_disposition,
                )?
            };
            let stream_admission_bytes = std::fs::read(
                package_dir.join("schema/stream-admission-evidence.json"),
            )
            .map_err(|error| {
                cdf_kernel::CdfError::internal(format!(
                    "read isolated partition stream-admission evidence: {error}"
                ))
            })?;
            let stream_admission =
                serde_json::from_slice(&stream_admission_bytes).map_err(|error| {
                    cdf_kernel::CdfError::contract(format!(
                        "decode isolated partition stream-admission evidence: {error}"
                    ))
                })?;
            let schema_quarantine_path =
                package_dir.join("quarantine/schema-admission-evidence.json");
            let schema_quarantine_evidence = schema_quarantine_path
                .exists()
                .then(|| {
                    std::fs::read(&schema_quarantine_path)
                        .map_err(|error| {
                            cdf_kernel::CdfError::internal(format!(
                                "read isolated partition schema-quarantine evidence: {error}"
                            ))
                        })
                        .and_then(|bytes| {
                            serde_json::from_slice(&bytes).map_err(|error| {
                                cdf_kernel::CdfError::contract(format!(
                                    "decode isolated partition schema-quarantine evidence: {error}"
                                ))
                            })
                        })
                })
                .transpose()?;
            let partition_evidence = EnginePartitionEvidence::from_execution(
                &task,
                &plan,
                &output,
                stream_admission,
                schema_quarantine_evidence,
            )?;
            let mut outcome_tamper = partition_evidence.clone();
            if let Some(observation) = outcome_tamper.processed_observations.first_mut() {
                observation.outcome = match observation.outcome {
                    cdf_kernel::ProcessedObservationOutcome::Admitted => {
                        cdf_kernel::ProcessedObservationOutcome::Quarantined
                    }
                    cdf_kernel::ProcessedObservationOutcome::Quarantined => {
                        cdf_kernel::ProcessedObservationOutcome::Admitted
                    }
                    _ => {
                        return Err(cdf_kernel::CdfError::contract(
                            "fixture encountered an unsupported processed-observation outcome",
                        ));
                    }
                };
                let error = outcome_tamper.validate(&task, &plan, None).unwrap_err();
                if !error.message.contains("outcomes") {
                    return Err(cdf_kernel::CdfError::contract(
                        "partition outcome tamper did not fail at engine evidence admission",
                    ));
                }
            }
            let mut physical_schema_hashes = partition_evidence
                .stream_admission
                .physical_observation_catalog
                .values()
                .map(PhysicalObservationEvidence::identity_hash)
                .chain(
                    partition_evidence
                        .schema_quarantine_evidence
                        .iter()
                        .flat_map(|evidence| evidence.physical_observation_catalog.values())
                        .map(PhysicalObservationEvidence::identity_hash),
                )
                .collect::<Result<Vec<_>>>()?;
            physical_schema_hashes.sort();
            physical_schema_hashes.dedup();
            let [physical_schema_hash] = physical_schema_hashes.as_slice() else {
                return Err(cdf_kernel::CdfError::contract(
                    "isolated partition source attestation requires one exact physical-schema identity",
                ));
            };
            let physical_schema_hash = physical_schema_hash.clone();
            let partition_evidence_bytes = cdf_package::canonical_json_bytes(&partition_evidence)?;
            let mut write_session = cdf_runtime::WorkerArtifactWriteSession::new(
                &task,
                &attempt,
                &self.lease,
                self.now_ms,
            )?;
            let mut receipts = Vec::with_capacity(output.output.identity_segments().len());
            for (segment_ordinal, segment) in output.output.identity_segments().iter().enumerate() {
                let segment_ordinal = u32::try_from(segment_ordinal).map_err(|_| {
                    cdf_kernel::CdfError::data("isolated worker segment ordinal exceeds u32")
                })?;
                let segment_id = program
                    .plan()
                    .segmentation_policy()?
                    .segment_id(task.partition.canonical_partition_ordinal, segment_ordinal)?;
                let canonical_bytes =
                    std::fs::read(package_dir.join(&segment.path)).map_err(|error| {
                        cdf_kernel::CdfError::internal(format!(
                            "read isolated worker segment {}: {error}",
                            segment.path
                        ))
                    })?;
                let bytes = prepared_segment_bytes(
                    canonical_bytes,
                    segment.package_row_ord_start,
                    segment.row_count,
                )?;
                let reference = SharedEngineWorkerArtifacts::reference(
                    cdf_runtime::WorkerArtifactKind::PreparedSegment,
                    attempt.write_permit.output.store_namespace.as_str(),
                    format!(
                        "{}prepared/{}.arrow",
                        attempt.write_permit.output.object_key_prefix,
                        segment_id.as_str()
                    ),
                    &bytes,
                )?;
                let receipt = cdf_runtime::WorkerArtifactReceipt {
                    role: cdf_runtime::WorkerArtifactRole::PreparedSegment {
                        segment_id,
                        partition_ordinal: task.partition.canonical_partition_ordinal,
                        segment_ordinal,
                        row_count: segment.row_count,
                    },
                    artifact: reference,
                };
                let object_state = self.artifacts.object_state(&receipt.artifact);
                let maximum_bytes = task
                    .resources
                    .memory_bytes
                    .min(task.output_policy.maximum_artifact_bytes)
                    .min(attempt.write_permit.output.maximum_bytes);
                let mut sink = PendingEngineWorkerWrite {
                    store: &self.artifacts,
                    bytes: Some(account_worker_artifact_bytes(
                        &self.artifacts.memory,
                        bytes,
                        maximum_bytes,
                        cdf_memory::MemoryClass::Package,
                    )?),
                };
                write_session.write(&receipt, &object_state, self.now_ms, &mut sink)?;
                receipts.push(receipt);
            }
            let evidence_reference = SharedEngineWorkerArtifacts::reference(
                cdf_runtime::WorkerArtifactKind::PartitionEvidence,
                attempt.write_permit.output.store_namespace.as_str(),
                format!(
                    "{}evidence/partition-{:08}.json",
                    attempt.write_permit.output.object_key_prefix,
                    task.partition.canonical_partition_ordinal
                ),
                &partition_evidence_bytes,
            )?;
            let evidence_receipt = cdf_runtime::WorkerArtifactReceipt {
                role: cdf_runtime::WorkerArtifactRole::PartitionEvidence {
                    partition_ordinal: task.partition.canonical_partition_ordinal,
                },
                artifact: evidence_reference,
            };
            let evidence_state = self.artifacts.object_state(&evidence_receipt.artifact);
            let maximum_bytes = task
                .resources
                .memory_bytes
                .min(task.output_policy.maximum_artifact_bytes)
                .min(attempt.write_permit.output.maximum_bytes);
            let mut evidence_sink = PendingEngineWorkerWrite {
                store: &self.artifacts,
                bytes: Some(account_worker_artifact_bytes(
                    &self.artifacts.memory,
                    partition_evidence_bytes,
                    maximum_bytes,
                    cdf_memory::MemoryClass::Control,
                )?),
            };
            write_session.write(
                &evidence_receipt,
                &evidence_state,
                self.now_ms,
                &mut evidence_sink,
            )?;
            receipts.push(evidence_receipt);
            receipts.sort_by(|left, right| left.artifact.cmp(&right.artifact));
            let artifact_bytes = receipts.iter().try_fold(0_u64, |total, receipt| {
                total
                    .checked_add(receipt.artifact.byte_count)
                    .ok_or_else(|| cdf_kernel::CdfError::data("worker artifact bytes overflow"))
            })?;
            let result = cdf_runtime::PartitionWorkerResult::new(
                &attempt,
                cdf_runtime::PartitionWorkerResultInput {
                    status: cdf_runtime::WorkerTerminalStatus::Succeeded,
                    source_attestation: Some(cdf_runtime::WorkerSourceAttestation {
                        processed_position: cdf_runtime::WorkerPosition::inline(
                            processed_position,
                        )?,
                        physical_schema_hash,
                    }),
                    artifacts: receipts,
                    counts: cdf_runtime::WorkerResultCounts {
                        input_rows: output.output.lineage.input_rows,
                        output_rows: output.output.profile.output_rows,
                        quarantined_rows: 0,
                        source_bytes,
                        artifact_bytes,
                    },
                    telemetry: cdf_runtime::WorkerTelemetry::default(),
                },
            )?;
            Ok(result)
        })();
        Box::pin(async move { result })
    }
}

#[test]
fn lineage_summary_rejects_superseded_duplicate_identity_fields() {
    for field in ["input_partitions", "output_segments"] {
        let mut value = serde_json::json!({
            "input_rows": 0,
            "input_observations": []
        });
        value[field] = serde_json::json!([]);
        let error = serde_json::from_value::<crate::LineageSummary>(value).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }
}

#[test]
fn engine_partition_task_compiles_every_authority_as_typed_artifacts() {
    let resource = MockResource::tier_a(sample_batches());
    let source = mock_compiled_source_plan(&resource, None);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let partition = plan.scan.inline_partitions().unwrap().first().unwrap();
    let mut artifacts = MemoryWorkerCompilerArtifacts::default();
    let task = compile_engine_partition_task(
        EnginePartitionTaskInput {
            compatibility: cdf_runtime::WorkerCompatibility {
                cdf_version: "0.1.0".to_owned(),
                artifact_version: "package-v1".to_owned(),
                arrow_version: "58.3.0".to_owned(),
                relational_engine: cdf_runtime::WorkerComponentVersion {
                    component: "datafusion".to_owned(),
                    version: "54.0.0".to_owned(),
                },
                normalizer_version: plan.validation_program.normalizer_version.clone(),
            },
            pipeline_id: cdf_kernel::PipelineId::new("engine-worker-test").unwrap(),
            source: &source,
            plan: &plan,
            partition,
            canonical_partition_ordinal: 0,
            epoch_ordinal: None,
            input_checkpoint: None,
            secret_references: Vec::new(),
            input_artifacts: Vec::new(),
            resources: cdf_runtime::WorkerResourceBudget {
                memory_bytes: 64 * 1024 * 1024,
                disk_bytes: 64 * 1024 * 1024,
                cpu_slots: 1,
                io_slots: 1,
                control: cdf_runtime::WorkerControlBudget {
                    maximum_task_bytes: 64 * 1024,
                    maximum_attempt_bytes: 16 * 1024,
                    maximum_result_bytes: 64 * 1024,
                    maximum_input_artifacts: 32,
                    maximum_output_artifacts: 32,
                    maximum_secret_references: 8,
                },
            },
            attempt_policy: cdf_runtime::WorkerAttemptPolicy {
                maximum_attempts: 3,
                maximum_attempt_duration_ms: 30_000,
            },
            capabilities: cdf_runtime::WorkerCapabilityRequirements {
                required_blocking_lanes: Vec::new(),
                services: Vec::new(),
            },
            output_policy: cdf_runtime::WorkerOutputPolicy {
                allowed_kinds: vec![
                    cdf_runtime::WorkerArtifactKind::CanonicalSegment,
                    cdf_runtime::WorkerArtifactKind::Quarantine,
                    cdf_runtime::WorkerArtifactKind::Residual,
                    cdf_runtime::WorkerArtifactKind::Verdict,
                    cdf_runtime::WorkerArtifactKind::Lineage,
                ],
                maximum_artifact_bytes: 64 * 1024 * 1024,
            },
        },
        &mut artifacts,
    )
    .unwrap();

    assert_eq!(artifacts.values.len(), 12);
    assert_eq!(task.partition.partition_id, partition.partition_id);
    assert_eq!(
        task.execution.project_identity_hash,
        cdf_runtime::artifact_hash(&plan).unwrap()
    );
    assert_eq!(
        task.partition.unit_authority_hash,
        task.execution.artifacts.decode_unit_plan.content_sha256
    );
    assert_eq!(
        task.partition.segment_authority_hash,
        task.execution.artifacts.segment_plan.content_sha256
    );
    let serialized = serde_json::to_string(&task).unwrap();
    assert!(!serialized.contains("operator_chain"));
    assert!(!serialized.contains("compiled_source_execution"));

    let verifier = EngineWorkerAdmissionVerifier::new(&artifacts);
    let reconstructed =
        cdf_runtime::WorkerAdmissionVerifier::reconstruct_task_authority(&verifier, &task).unwrap();
    assert_eq!(reconstructed.source(), &source);
    assert_eq!(reconstructed.partition(), partition);

    artifacts
        .values
        .get_mut(&cdf_runtime::WorkerArtifactKind::ProjectPlan)
        .unwrap()[0] ^= 1;
    let verifier = EngineWorkerAdmissionVerifier::new(&artifacts);
    let error = cdf_runtime::WorkerAdmissionVerifier::reconstruct_task_authority(&verifier, &task)
        .unwrap_err();
    assert!(error.message.contains("bytes or generation"));
}

#[test]
fn engine_partition_task_rejects_package_global_work_before_writing_control_artifacts() {
    let resource = MockResource::tier_a(sample_batches())
        .without_control_keys()
        .with_partition_count(2);
    let source = mock_compiled_source_plan(&resource, None);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut artifacts = MemoryWorkerCompilerArtifacts::default();
    let error = compile_engine_partition_task(
        EnginePartitionTaskInput {
            compatibility: cdf_runtime::WorkerCompatibility {
                cdf_version: "0.1.0".to_owned(),
                artifact_version: "package-v2".to_owned(),
                arrow_version: "58.3.0".to_owned(),
                relational_engine: cdf_runtime::WorkerComponentVersion {
                    component: "datafusion".to_owned(),
                    version: "54.0.0".to_owned(),
                },
                normalizer_version: plan.validation_program.normalizer_version.clone(),
            },
            pipeline_id: cdf_kernel::PipelineId::new("global-operator-guard").unwrap(),
            source: &source,
            plan: &plan,
            partition: &plan.scan.inline_partitions().unwrap()[0],
            canonical_partition_ordinal: 0,
            epoch_ordinal: None,
            input_checkpoint: None,
            secret_references: Vec::new(),
            input_artifacts: Vec::new(),
            resources: cdf_runtime::WorkerResourceBudget {
                memory_bytes: 64 * 1024 * 1024,
                disk_bytes: 64 * 1024 * 1024,
                cpu_slots: 1,
                io_slots: 1,
                control: cdf_runtime::WorkerControlBudget {
                    maximum_task_bytes: 64 * 1024,
                    maximum_attempt_bytes: 16 * 1024,
                    maximum_result_bytes: 64 * 1024,
                    maximum_input_artifacts: 32,
                    maximum_output_artifacts: 32,
                    maximum_secret_references: 8,
                },
            },
            attempt_policy: cdf_runtime::WorkerAttemptPolicy {
                maximum_attempts: 2,
                maximum_attempt_duration_ms: 30_000,
            },
            capabilities: cdf_runtime::WorkerCapabilityRequirements {
                required_blocking_lanes: Vec::new(),
                services: Vec::new(),
            },
            output_policy: cdf_runtime::WorkerOutputPolicy {
                allowed_kinds: vec![cdf_runtime::WorkerArtifactKind::PreparedSegment],
                maximum_artifact_bytes: 64 * 1024 * 1024,
            },
        },
        &mut artifacts,
    )
    .unwrap_err();

    assert!(
        error
            .message
            .contains("canonical global-operator or epoch task")
    );
    assert!(artifacts.values.is_empty());
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

fn isolated_engine_services(cpu_slots: u16) -> cdf_runtime::ExecutionServices {
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new())
            .unwrap(),
    );
    cdf_runtime::ExecutionServices::new(Arc::new(
        StandaloneExecutionHost::new(
            cdf_runtime::ExecutionHostCapabilities {
                logical_cpu_slots: cpu_slots,
                io_workers: cpu_slots.max(1),
                blocking_lanes: Vec::new(),
            },
            memory,
        )
        .unwrap(),
    ))
    .unwrap()
}

fn isolated_engine_source_plan(
    resource: &MockResource,
    descriptor: cdf_runtime::SourceDriverDescriptor,
    dataset_id: &str,
    batches: &[Batch],
    extent: &ExecutionExtent,
) -> cdf_runtime::CompiledSourcePlan {
    let source_bytes = batches
        .iter()
        .map(|batch| batch.header.byte_count)
        .sum::<u64>();
    let partition_source_bytes = batches
        .iter()
        .map(|batch| {
            (
                batch.header.partition_id.as_str().to_owned(),
                batch.header.byte_count,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut source = if extent.is_bounded() {
        mock_compiled_source_plan(resource, None)
    } else {
        mock_unbounded_source_plan(resource)
    };
    source.driver = descriptor;
    source.redacted_options = serde_json::json!({});
    source.redacted_options_hash = cdf_runtime::artifact_hash(&source.redacted_options).unwrap();
    source.physical_plan = serde_json::json!({
        "dataset_id": dataset_id,
        "source_bytes": source_bytes,
        "partition_source_bytes": partition_source_bytes,
    });
    source.physical_plan_hash = cdf_kernel::PhysicalSourcePlanHash::new(
        cdf_runtime::artifact_hash(&source.physical_plan).unwrap(),
    )
    .unwrap();
    source.validate().unwrap();
    source
}

fn isolated_engine_attempt<T: cdf_runtime::PortableWorkerTask>(
    task: &T,
    attempt_id: &str,
) -> (
    cdf_runtime::PartitionAttemptEnvelope,
    cdf_runtime::WorkerLeaseState,
) {
    let domain = cdf_kernel::LeaseAuthorityDomainId::new("isolated-engine-test").unwrap();
    let fence = cdf_kernel::FencingToken::new(7).unwrap();
    let attempt = cdf_runtime::PartitionAttemptEnvelope {
        version: cdf_runtime::PARTITION_ATTEMPT_VERSION,
        attempt_id: attempt_id.to_owned(),
        retry_ordinal: 0,
        trace_id: format!("trace-{attempt_id}"),
        write_permit: cdf_runtime::WorkerArtifactWritePermit {
            task_sha256: task.task_sha256().to_owned(),
            lease_authority_domain_id: domain.clone(),
            lease_scope: task.lease_scope().clone(),
            fencing_token: fence,
            issued_at_ms: 1_000,
            expires_at_ms: 30_000,
            output: cdf_runtime::WorkerArtifactWriteScope {
                store_namespace: cdf_kernel::ContentStoreNamespace::new("engine-worker-output")
                    .unwrap(),
                object_key_prefix: format!("attempts/{attempt_id}/"),
                maximum_bytes: 128 * 1024 * 1024,
            },
            generation_precondition: cdf_runtime::WorkerObjectGenerationPrecondition::CreateOnly,
        },
    };
    let lease = cdf_runtime::WorkerLeaseState {
        lease_authority_domain_id: domain,
        lease_scope: task.lease_scope().clone(),
        fencing_token: fence,
        expires_at_ms: 30_000,
    };
    (attempt, lease)
}

fn assert_engine_store_rechecks_fence_at_mutation(
    task: &cdf_runtime::PortablePartitionTask,
    attempt: &cdf_runtime::PartitionAttemptEnvelope,
    lease: &cdf_runtime::WorkerLeaseState,
) -> Result<()> {
    let artifacts = SharedEngineWorkerArtifacts::default();
    artifacts.admit_lease(lease.clone(), 2_000)?;
    let payload = bytes::Bytes::from_static(b"fence-probe");
    let reference = SharedEngineWorkerArtifacts::reference(
        cdf_runtime::WorkerArtifactKind::PartitionEvidence,
        attempt.write_permit.output.store_namespace.as_str(),
        format!(
            "{}fence-probe.json",
            attempt.write_permit.output.object_key_prefix
        ),
        &payload,
    )?;
    let receipt = cdf_runtime::WorkerArtifactReceipt {
        role: cdf_runtime::WorkerArtifactRole::PartitionEvidence {
            partition_ordinal: task.partition.canonical_partition_ordinal,
        },
        artifact: reference.clone(),
    };
    let memory_lease = reserve_worker_artifact_memory(
        &artifacts.memory,
        reference.byte_count,
        reference.byte_count,
        cdf_memory::MemoryClass::Control,
    )?;
    let accounted = cdf_memory::AccountedBytes::new(payload, memory_lease)?;
    let mut session = cdf_runtime::WorkerArtifactWriteSession::new(task, attempt, lease, 2_000)?;
    artifacts.admit_lease(
        cdf_runtime::WorkerLeaseState {
            fencing_token: cdf_kernel::FencingToken::new(
                lease.fencing_token.get().checked_add(1).ok_or_else(|| {
                    cdf_kernel::CdfError::contract("test fencing token overflowed")
                })?,
            )?,
            ..lease.clone()
        },
        2_000,
    )?;
    let mut sink = PendingEngineWorkerWrite {
        store: &artifacts,
        bytes: Some(accounted),
    };
    let error = session
        .write(
            &receipt,
            &cdf_runtime::WorkerArtifactObjectState::Absent,
            2_000,
            &mut sink,
        )
        .unwrap_err();
    if !error.message.contains("stale") || artifacts.bytes_for(&reference).is_ok() {
        return Err(cdf_kernel::CdfError::contract(
            "engine artifact provider mutated under a stale fence",
        ));
    }
    let rollback = artifacts.admit_lease(lease.clone(), 2_000).unwrap_err();
    if !rollback.message.contains("cannot regress") {
        return Err(cdf_kernel::CdfError::contract(
            "engine artifact provider admitted a fencing rollback",
        ));
    }
    Ok(())
}

fn run_actual_isolated_engine_equivalence(
    cpu_slots: u16,
    partition_count: usize,
    extent: ExecutionExtent,
) -> (
    RetainedEngineRun,
    RetainedEngineRun,
    Vec<cdf_runtime::PartitionWorkerResult>,
    Vec<cdf_runtime::SegmentWorkerResult>,
) {
    let mut batches = sample_batches()
        .into_iter()
        .take(partition_count)
        .collect::<Vec<_>>();
    for batch in &mut batches {
        batch.header.source_position = Some(terminal_file_position());
    }
    let direct_resource = MockResource::tier_a(batches.clone())
        .without_control_keys()
        .with_partition_count(partition_count);
    run_actual_isolated_engine_equivalence_for_resource(cpu_slots, direct_resource, extent)
}

fn run_actual_isolated_engine_equivalence_for_resource(
    cpu_slots: u16,
    direct_resource: MockResource,
    extent: ExecutionExtent,
) -> (
    RetainedEngineRun,
    RetainedEngineRun,
    Vec<cdf_runtime::PartitionWorkerResult>,
    Vec<cdf_runtime::SegmentWorkerResult>,
) {
    let batches = direct_resource.batches.clone();
    let partition_count = direct_resource.partition_count;
    let dataset_id = "isolated-orders-v1";
    let option_schema = isolated_mock_option_schema();
    let descriptor = cdf_runtime::SourceDriverDescriptor {
        driver_id: cdf_runtime::SourceDriverId::new("isolated_engine_mock").unwrap(),
        driver_version: "1.0.0".to_owned(),
        option_schema_hash: cdf_runtime::artifact_hash(&option_schema).unwrap(),
        kinds: vec!["isolated_engine_mock".to_owned()],
        schemes: vec!["isolated-engine-mock".to_owned()],
    };
    let source = isolated_engine_source_plan(
        &direct_resource,
        descriptor.clone(),
        dataset_id,
        &batches,
        &extent,
    );
    direct_resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &direct_resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    assert_eq!(plan.scan.partition_count().unwrap(), partition_count as u64);

    let direct_services = isolated_engine_services(cpu_slots);
    let direct_scheduler = cdf_runtime::resolve_runtime_scheduler(
        partition_count,
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &direct_services,
        Some(cpu_slots),
    )
    .unwrap();
    let direct_root = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let direct_options = EngineExecutionConfig::default()
        .with_execution_services(direct_services)
        .with_scheduler_resolution(direct_scheduler)
        .new_invocation();
    let direct = match &plan.execution_extent {
        ExecutionExtent::Bounded { .. } => block_on(
            super::execute_to_package_with_segment_positions_and_pre_finalize(
                &plan,
                &direct_resource,
                direct_root.path(),
                &pre_finalize,
                direct_options,
            ),
        )
        .unwrap(),
        ExecutionExtent::Drain { .. } => {
            let mut controller =
                cdf_runtime::DrainEpochController::new(&plan.execution_extent).unwrap();
            block_on(super::execute_drain_epoch_with_hooks(
                &plan,
                &direct_resource,
                direct_root.path(),
                &pre_finalize,
                super::DrainEpochExecution::new(&mut controller),
                direct_options,
            ))
            .unwrap()
            .into_package()
            .unwrap()
        }
        ExecutionExtent::Resident { .. } => unreachable!("resident plans do not compile"),
    };

    let compatibility = cdf_runtime::WorkerCompatibility {
        cdf_version: "0.1.0".to_owned(),
        artifact_version: "package-v2".to_owned(),
        arrow_version: "58.3.0".to_owned(),
        relational_engine: cdf_runtime::WorkerComponentVersion {
            component: "datafusion".to_owned(),
            version: "54.0.0".to_owned(),
        },
        normalizer_version: plan.validation_program.normalizer_version.clone(),
    };
    let control = cdf_runtime::WorkerControlBudget {
        maximum_task_bytes: 128 * 1024,
        maximum_attempt_bytes: 32 * 1024,
        maximum_result_bytes: 128 * 1024,
        maximum_input_artifacts: 32,
        maximum_output_artifacts: 64,
        maximum_secret_references: 8,
    };
    let mut artifacts = SharedEngineWorkerArtifacts::default();
    let resources = cdf_runtime::WorkerResourceBudget {
        memory_bytes: 512 * 1024 * 1024,
        disk_bytes: 512 * 1024 * 1024,
        cpu_slots,
        io_slots: cpu_slots,
        control: control.clone(),
    };
    let attempt_policy = cdf_runtime::WorkerAttemptPolicy {
        maximum_attempts: 2,
        maximum_attempt_duration_ms: 29_000,
    };
    let partition_tasks = plan
        .scan
        .inline_partitions()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(ordinal, partition)| {
            compile_engine_partition_task(
                EnginePartitionTaskInput {
                    compatibility: compatibility.clone(),
                    pipeline_id: cdf_kernel::PipelineId::new("isolated-engine-test").unwrap(),
                    source: &source,
                    plan: &plan,
                    partition,
                    canonical_partition_ordinal: u32::try_from(ordinal).unwrap(),
                    epoch_ordinal: None,
                    input_checkpoint: None,
                    secret_references: Vec::new(),
                    input_artifacts: Vec::new(),
                    resources: resources.clone(),
                    attempt_policy: attempt_policy.clone(),
                    capabilities: cdf_runtime::WorkerCapabilityRequirements {
                        required_blocking_lanes: Vec::new(),
                        services: Vec::new(),
                    },
                    output_policy: cdf_runtime::WorkerOutputPolicy {
                        allowed_kinds: vec![
                            cdf_runtime::WorkerArtifactKind::PreparedSegment,
                            cdf_runtime::WorkerArtifactKind::PartitionEvidence,
                        ],
                        maximum_artifact_bytes: 128 * 1024 * 1024,
                    },
                },
                &mut artifacts,
            )
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let worker_services = isolated_engine_services(cpu_slots);
    let worker_resource = direct_resource.clone();
    let worker_capabilities = cdf_runtime::WorkerRuntimeCapabilities {
        host: worker_services.capabilities().clone(),
        memory_bytes: 512 * 1024 * 1024,
        disk_bytes: 512 * 1024 * 1024,
        control,
        services: Vec::new(),
    };

    struct PartitionFixture {
        task: cdf_runtime::PortablePartitionTask,
    }

    let fixtures = partition_tasks
        .into_iter()
        .map(|task| PartitionFixture { task })
        .collect::<Vec<_>>();

    let mut preparations = Vec::with_capacity(fixtures.len());
    let mut partition_evidence = Vec::with_capacity(fixtures.len());
    let jobs = usize::from(cpu_slots).max(1);
    let plan_authority = &plan;
    for chunk in fixtures.chunks(jobs) {
        let completed = std::thread::scope(|scope| {
            chunk
                .iter()
                .enumerate()
                .map(|(chunk_ordinal, fixture)| {
                    let artifacts = artifacts.clone();
                    let compatibility = compatibility.clone();
                    let worker_capabilities = worker_capabilities.clone();
                    let worker_services = worker_services.clone();
                    let descriptor = descriptor.clone();
                    let option_schema = option_schema.clone();
                    let worker_resource = worker_resource.clone();
                    scope.spawn(move || {
                        let attempt_id = format!(
                            "jobs-{cpu_slots}-partition-{}",
                            fixture.task.partition.canonical_partition_ordinal
                        );
                        let (attempt, lease) = isolated_engine_attempt(&fixture.task, &attempt_id);
                        artifacts.admit_lease(lease.clone(), 2_000)?;
                        if fixture.task.partition.canonical_partition_ordinal == 0 {
                            assert_engine_store_rechecks_fence_at_mutation(
                                &fixture.task,
                                &attempt,
                                &lease,
                            )?;
                        }
                        let mut registry = cdf_runtime::SourceRegistry::new();
                        registry.register(IsolatedEngineMockDriver {
                            descriptor,
                            option_schema,
                            dataset_id: dataset_id.to_owned(),
                            resource: worker_resource,
                        })?;
                        let worker_root = TempDir::new().map_err(|error| {
                            cdf_kernel::CdfError::internal(format!(
                                "create isolated worker root: {error}"
                            ))
                        })?;
                        let secrets: Arc<dyn cdf_http::SecretProvider + Send + Sync> =
                            Arc::new(RejectingEngineWorkerSecrets);
                        let source_context = cdf_runtime::SourceResolutionContext::new(
                            worker_root.path(),
                            secrets,
                            &worker_services,
                            Arc::new(cdf_http::EgressAllowlist::allow_any()),
                        );
                        let worker_verifier = EngineWorkerAdmissionVerifier::new(&artifacts);
                        let coordinator_verifier = EngineWorkerAdmissionVerifier::new(&artifacts);
                        let executor = ActualEngineIsolatedExecutor {
                            registry: &registry,
                            source_context: &source_context,
                            services: worker_services.clone(),
                            artifacts: artifacts.clone(),
                            lease: lease.clone(),
                            package_root: worker_root.path(),
                            now_ms: 2_000,
                        };
                        let host = cdf_runtime::LocalIsolatedWorkerHost::new(
                            &compatibility,
                            &worker_capabilities,
                            &registry,
                            &worker_verifier,
                            &executor,
                        )?;
                        let admitted = block_on(cdf_runtime::execute_local_isolated_partition(
                            &fixture.task,
                            &attempt,
                            &host,
                            &registry,
                            &coordinator_verifier,
                            &lease,
                            2_000,
                        ))?;
                        let evidence = coordinator_verifier.read_partition_evidence(
                            &fixture.task,
                            plan_authority,
                            &admitted,
                        )?;
                        Ok::<_, cdf_kernel::CdfError>((chunk_ordinal, admitted, evidence))
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        cdf_kernel::CdfError::internal("isolated partition thread panicked")
                    })?
                })
                .collect::<Result<Vec<_>>>()
        })
        .unwrap();
        for (_, admitted, evidence) in completed {
            preparations.push(admitted);
            partition_evidence.push(evidence);
        }
    }

    let mut package_row_ord_start = 0_u64;
    let mut finalized = Vec::with_capacity(direct.output.identity_segments().len());
    let segment_verifier = EngineWorkerAdmissionVerifier::new(&artifacts);
    for (fixture, preparation) in fixtures.iter().zip(&preparations) {
        let mut prepared_segments = preparation
            .result()
            .artifacts
            .iter()
            .filter_map(|receipt| match receipt.role {
                cdf_runtime::WorkerArtifactRole::PreparedSegment {
                    segment_ordinal,
                    row_count,
                    ..
                } => Some((segment_ordinal, row_count)),
                _ => None,
            })
            .collect::<Vec<_>>();
        prepared_segments.sort_unstable_by_key(|(segment_ordinal, _)| *segment_ordinal);
        for (segment_ordinal, row_count) in prepared_segments {
            let segment_task = compile_engine_segment_task(EngineSegmentTaskInput {
                plan: &plan,
                preparation_task: &fixture.task,
                preparation_result: preparation,
                segment_ordinal,
                package_row_ord_start,
                resources: fixture.task.resources.clone(),
                attempt_policy: fixture.task.attempt_policy.clone(),
                capabilities: cdf_runtime::WorkerCapabilityRequirements {
                    required_blocking_lanes: Vec::new(),
                    services: Vec::new(),
                },
                output_policy: cdf_runtime::WorkerOutputPolicy {
                    allowed_kinds: vec![cdf_runtime::WorkerArtifactKind::CanonicalSegment],
                    maximum_artifact_bytes: fixture.task.output_policy.maximum_artifact_bytes,
                },
            })
            .unwrap();
            let (segment_attempt, segment_lease) = isolated_engine_attempt(
                &segment_task,
                &format!(
                    "jobs-{cpu_slots}-partition-{}-segment-{segment_ordinal}",
                    fixture.task.partition.canonical_partition_ordinal
                ),
            );
            artifacts.admit_lease(segment_lease.clone(), 2_000).unwrap();
            let segment_executor =
                EngineIsolatedSegmentExecutor::new(&artifacts, &segment_lease, 2_000);
            let segment_host = cdf_runtime::LocalIsolatedSegmentHost::new(
                &compatibility,
                &worker_capabilities,
                &segment_verifier,
                &segment_executor,
            )
            .unwrap();
            finalized.push(
                block_on(cdf_runtime::execute_local_isolated_segment(
                    &segment_task,
                    &segment_attempt,
                    &segment_host,
                    &segment_verifier,
                    &segment_lease,
                    2_000,
                ))
                .unwrap(),
            );
            package_row_ord_start = package_row_ord_start.checked_add(row_count).unwrap();
        }
    }
    let assembled_root = TempDir::new().unwrap();
    if cpu_slots == 1 && partition_count == 1 && plan.execution_extent.is_bounded() {
        let mut replay_plan = plan.clone();
        replay_plan.package_id.push_str("-cross-plan-replay");
        let replay_root = TempDir::new().unwrap();
        let replay_error = assemble_isolated_worker_package(
            &replay_plan,
            replay_root.path(),
            partition_evidence.clone(),
            &finalized,
            &artifacts,
            &resources,
            &worker_services,
        )
        .unwrap_err();
        assert!(replay_error.message.contains("different engine plan"));
    }
    let assembled = assemble_isolated_worker_package(
        &plan,
        assembled_root.path(),
        partition_evidence,
        &finalized,
        &artifacts,
        &resources,
        &worker_services,
    )
    .unwrap();
    (
        RetainedEngineRun {
            run: direct,
            _package: direct_root,
        },
        RetainedEngineRun {
            run: assembled,
            _package: assembled_root,
        },
        preparations
            .into_iter()
            .map(cdf_runtime::AdmittedPartitionWorkerResult::into_result)
            .collect(),
        finalized
            .into_iter()
            .map(cdf_runtime::AdmittedSegmentWorkerResult::into_result)
            .collect(),
    )
}

#[test]
fn actual_engine_capsule_publishes_direct_segments_across_cpu_budgets() {
    for cpu_slots in [1, 4] {
        let (direct, isolated, admitted, finalized) =
            run_actual_isolated_engine_equivalence(cpu_slots, 1, ExecutionExtent::bounded());
        let admitted = &admitted[0];
        assert_eq!(isolated.output.manifest, direct.output.manifest);
        assert_eq!(
            isolated.output.verification.package_hash(),
            direct.output.verification.package_hash()
        );
        assert_eq!(
            isolated.output.identity_segments(),
            direct.output.identity_segments()
        );
        assert_eq!(isolated.output.profile, direct.output.profile);
        assert_eq!(isolated.output.lineage, direct.output.lineage);
        assert_eq!(isolated.segment_positions, direct.segment_positions);
        assert_eq!(isolated.execution_evidence(), direct.execution_evidence());
        assert_eq!(admitted.counts.input_rows, direct.output.lineage.input_rows);
        assert_eq!(
            admitted.counts.output_rows,
            direct.output.profile.output_rows
        );
        assert_eq!(
            admitted.artifacts.len(),
            direct.output.identity_segments().len() + 1
        );
        assert_eq!(
            finalized
                .iter()
                .map(|result| {
                    result
                        .artifact
                        .as_ref()
                        .unwrap()
                        .artifact
                        .content_sha256
                        .as_str()
                })
                .collect::<Vec<_>>(),
            direct
                .output
                .identity_segments()
                .iter()
                .map(|segment| format!("sha256:{}", segment.sha256))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn actual_engine_capsules_are_jobs_invariant_for_multiple_partitions() {
    let (direct_serial, isolated_serial, admitted_serial, finalized_serial) =
        run_actual_isolated_engine_equivalence(1, 2, ExecutionExtent::bounded());
    let (direct_parallel, isolated_parallel, admitted_parallel, finalized_parallel) =
        run_actual_isolated_engine_equivalence(4, 2, ExecutionExtent::bounded());

    assert_eq!(
        direct_parallel.output.identity_segments(),
        direct_serial.output.identity_segments()
    );
    assert_eq!(
        direct_parallel.output.manifest,
        direct_serial.output.manifest
    );
    assert_eq!(
        direct_parallel.output.verification.package_hash(),
        direct_serial.output.verification.package_hash()
    );
    assert_eq!(direct_parallel.output.profile, direct_serial.output.profile);
    assert_eq!(direct_parallel.output.lineage, direct_serial.output.lineage);
    assert_eq!(
        direct_parallel.segment_positions,
        direct_serial.segment_positions
    );
    assert_eq!(
        direct_parallel.execution_evidence(),
        direct_serial.execution_evidence()
    );
    assert_eq!(
        isolated_serial.output.manifest,
        direct_serial.output.manifest
    );
    assert_eq!(
        isolated_parallel.output.manifest,
        direct_parallel.output.manifest
    );
    assert_eq!(
        admitted_serial
            .iter()
            .map(|result| result.counts.output_rows)
            .sum::<u64>(),
        direct_serial.output.profile.output_rows
    );
    assert_eq!(
        admitted_parallel
            .iter()
            .map(|result| result.counts.output_rows)
            .sum::<u64>(),
        direct_parallel.output.profile.output_rows
    );

    let finalized_hashes = |results: &[cdf_runtime::SegmentWorkerResult]| {
        results
            .iter()
            .map(|result| {
                result
                    .artifact
                    .as_ref()
                    .unwrap()
                    .artifact
                    .content_sha256
                    .clone()
            })
            .collect::<Vec<_>>()
    };
    let direct_hashes = direct_serial
        .output
        .identity_segments()
        .iter()
        .map(|segment| format!("sha256:{}", segment.sha256))
        .collect::<Vec<_>>();
    assert_eq!(finalized_hashes(&finalized_serial), direct_hashes);
    assert_eq!(finalized_hashes(&finalized_parallel), direct_hashes);
}

#[test]
fn actual_engine_capsule_preserves_a_finite_drain_epoch() {
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 64 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: cdf_kernel::LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 3 },
    };
    let (direct, isolated, admitted, finalized) =
        run_actual_isolated_engine_equivalence(4, 1, extent);
    assert_eq!(isolated.output.manifest, direct.output.manifest);
    assert_eq!(
        isolated.output.verification.package_hash(),
        direct.output.verification.package_hash()
    );
    assert_eq!(
        isolated.output.identity_segments(),
        direct.output.identity_segments()
    );
    assert_eq!(isolated.output.profile, direct.output.profile);
    assert_eq!(isolated.output.lineage, direct.output.lineage);
    assert_eq!(isolated.segment_positions, direct.segment_positions);
    assert_eq!(isolated.execution_evidence(), direct.execution_evidence());
    let direct_epoch = direct.drain_epoch.as_ref().unwrap();
    let isolated_epoch = isolated.drain_epoch.as_ref().unwrap();
    assert_eq!(
        isolated_epoch.closure.frontier,
        direct_epoch.closure.frontier
    );
    assert_eq!(
        isolated_epoch.closure.evidence,
        direct_epoch.closure.evidence
    );
    assert_eq!(isolated_epoch.consumed_partition_count, 1);
    assert_eq!(
        admitted[0].counts.output_rows,
        direct.output.profile.output_rows
    );
    assert_eq!(
        finalized[0]
            .artifact
            .as_ref()
            .unwrap()
            .artifact
            .content_sha256,
        format!("sha256:{}", direct.output.identity_segments()[0].sha256)
    );
}

#[test]
fn actual_engine_capsule_preserves_terminal_schema_quarantine_evidence() {
    let mut batch =
        missing_control_field_batch("isolated-schema-quarantine", "part-0", vec!["one", "two"]);
    batch.header.source_position = Some(terminal_file_position());
    let resource = MockResource::tier_a(vec![batch])
        .with_schema(sample_schema())
        .without_control_keys();
    let (direct, isolated, admitted, finalized) =
        run_actual_isolated_engine_equivalence_for_resource(
            4,
            resource,
            ExecutionExtent::bounded(),
        );

    assert_eq!(isolated.output.manifest, direct.output.manifest);
    assert_eq!(
        isolated.output.verification.package_hash(),
        direct.output.verification.package_hash()
    );
    assert_eq!(
        isolated.output.terminal_schema_quarantines,
        direct.output.terminal_schema_quarantines
    );
    assert_eq!(isolated.execution_evidence(), direct.execution_evidence());
    assert_eq!(admitted[0].counts.output_rows, 0);
    assert!(finalized.is_empty());
}

#[test]
fn isolated_worker_artifact_reads_hold_and_release_real_memory_leases() {
    let store = SharedEngineWorkerArtifacts::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(0..1_024)) as ArrayRef],
    )
    .unwrap();
    let mut encoded = Vec::new();
    cdf_package::encode_canonical_segment_ipc(&mut encoded, schema.as_ref(), &[batch]).unwrap();
    let reference = SharedEngineWorkerArtifacts::reference(
        cdf_runtime::WorkerArtifactKind::PreparedSegment,
        "worker-memory-test",
        "prepared/segment.arrow".to_owned(),
        &encoded,
    )
    .unwrap();
    store.values.lock().unwrap().insert(
        (
            reference.store_namespace.as_str().to_owned(),
            reference.object_key.as_str().to_owned(),
        ),
        bytes::Bytes::from(encoded),
    );

    let artifact = store
        .read_prepared_segment(&reference, reference.byte_count, 64 * 1_024)
        .unwrap();
    assert_eq!(
        artifact
            .batches()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1_024
    );
    assert!(store.memory.snapshot().current_bytes > 0);
    drop(artifact);
    assert_eq!(store.memory.snapshot().current_bytes, 0);

    let error = match store.read_prepared_segment(&reference, reference.byte_count, 1) {
        Ok(_) => panic!("oversized decoded artifact unexpectedly acquired a lease"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("exceeds its admitted decoded-memory budget")
    );
    assert_eq!(store.memory.snapshot().current_bytes, 0);
}

#[test]
fn tier_a_resource_runs_engine_projection_filter_limit_into_package() {
    let mut batches = sample_batches();
    for batch in &mut batches {
        batch.header.source_position = Some(terminal_file_position());
    }
    let resource = MockResource::tier_a(batches);
    let input = plan_input(
        vec!["id > 1", "active = true"],
        Some(vec!["name".to_owned()]),
        Some(1),
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();

    assert_eq!(plan.explain.pushed_predicates, Vec::new());
    assert_eq!(plan.explain.unsupported_predicates.len(), 2);

    let temp = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let output = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        temp.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_statistics_profile(true),
    ))
    .unwrap()
    .output;

    assert_eq!(output.manifest.lifecycle.status, PackageStatus::Packaged);
    assert_eq!(output.profile.output_rows, 1);
    assert!(output.profile.output_bytes > 0);
    assert_eq!(output.identity_segments().len(), 1);
    assert_eq!(output.profile.statistics.columns[0].field_path.len(), 1);
    assert_eq!(
        output.profile.statistics.columns[0].field_path[0].as_ref(),
        "name"
    );
    assert_eq!(
        output.profile.statistics.columns[0].minimum,
        Some(cdf_kernel::TypedScalar::Utf8("two".into()))
    );
    assert!(!temp.path().join("stats/profile.json").exists());
    assert!(
        temp.path()
            .join(cdf_package::STATISTICS_PROFILE_FILE)
            .exists()
    );
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let verified = reader.verify_for_consumption().unwrap();
    let mut profile_rows = Vec::new();
    reader
        .for_each_verified_statistics_profile(&verified, &mut |row| {
            profile_rows.push(row);
            Ok(())
        })
        .unwrap();
    assert!(profile_rows.iter().any(|row| {
        row.grain == cdf_package::StatisticsProfileGrain::Package
            && row.field_path[0].as_ref() == "name"
            && row.minimum == Some(cdf_kernel::TypedScalar::Utf8("two".into()))
    }));

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema().field(0).name(), "name");
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "two");
}

#[test]
fn tier_a_rejects_partition_intent_that_claims_source_pushdown() {
    let intent = cdf_kernel::CompiledScanIntent {
        version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
        projection: Some(vec!["name".to_owned()]),
        predicates: Vec::new(),
        limit: Some(1),
        order_by: Vec::new(),
    };
    let resource = MockResource::tier_a(sample_batches()).with_tier_a_intent(intent);
    let error = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(
                Vec::new(),
                Some(vec!["name".to_owned()]),
                Some(1),
                ExecutionExtent::bounded(),
            ),
        )
        .unwrap_err();
    assert!(error.message.contains("Tier-A partition"));
    assert!(error.message.contains("full-scan intent"));
}

#[test]
fn residual_limit_is_consumed_across_partitions() {
    let mut batches = sample_batches();
    for batch in &mut batches {
        batch.header.source_position = Some(terminal_file_position());
    }
    let resource = MockResource::tier_b(batches);
    let input = plan_input(
        vec!["active = true"],
        Some(vec!["name".to_owned()]),
        Some(1),
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    assert_eq!(output.profile.output_batches, 1);
    assert_eq!(output.identity_segments().len(), 1);
}

#[test]
fn zero_limit_finalizes_an_empty_package_without_source_contact() {
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, Some(0), ExecutionExtent::bounded()),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(output.profile.output_rows, 0);
    assert!(output.identity_segments().is_empty());
}

#[test]
fn validation_program_rebind_atomically_rebuilds_compiled_output_schema() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let before = plan.output_arrow_schema().unwrap();
    assert!(before.field_with_name("name").is_ok());
    let mut rebound = plan.validation_program.clone();
    rename_column_program_output(&mut rebound, "name", "customer_name");

    plan.rebind_validation_program(rebound, resource.schema().as_ref())
        .unwrap();

    let after = plan.output_arrow_schema().unwrap();
    assert!(after.field_with_name("name").is_err());
    assert!(after.field_with_name("customer_name").is_ok());
    crate::planning::validate_plan_schema_authority(&resource, &plan).unwrap();
}

#[test]
fn engine_plan_requires_recorded_schema_authorities() {
    let resource = MockResource::tier_a(Vec::new());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    assert!(
        plan.validate_partition_schedule()
            .unwrap_err()
            .message
            .contains("requires compiled source")
    );
    let source = mock_compiled_source_plan(&resource, None);
    let mut unbounded = source.clone();
    unbounded.execution_capabilities.bounded = false;
    unbounded.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
        quiescence: false,
        watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Drop,
        watermark: None,
        safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
        source_frontiers: vec![cdf_runtime::SourceFrontierCapability::Cursor {
            fields: vec!["id".to_owned()],
        }],
        idleness_capabilities: Vec::new(),
    });
    assert!(
        plan.clone()
            .bind_compiled_source(&unbounded)
            .unwrap_err()
            .message
            .contains("declare a complete drain policy")
    );
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    assert_eq!(
        plan.compiled_stream_policy,
        plan.explain.compiled_stream_policy
    );
    assert!(plan.compiled_stream_policy.is_none());
    let serialized = serde_json::to_value(&plan).unwrap();
    assert!(serialized.get("compiled_stream_policy").is_none());
    assert!(
        serialized["explain"]
            .get("compiled_stream_policy")
            .is_none()
    );
    for required in [
        "schema_authority",
        "output_schema",
        "compiled_schema_admission",
    ] {
        let mut incomplete = serde_json::to_value(&plan).unwrap();
        incomplete.as_object_mut().unwrap().remove(required);
        let error = serde_json::from_value::<EnginePlan>(incomplete).unwrap_err();
        assert!(error.to_string().contains(required));
    }
    for required in ["compiled_source_execution", "partition_schedule"] {
        let mut incomplete = serde_json::to_value(&plan).unwrap();
        incomplete.as_object_mut().unwrap().remove(required);
        let incomplete: EnginePlan = serde_json::from_value(incomplete).unwrap();
        let error = incomplete.validate_partition_schedule().unwrap_err();
        assert!(
            error.message.contains("must be present together")
                || error
                    .message
                    .contains("does not match its recorded explain")
        );
    }
}

#[test]
fn compiled_stream_admission_is_replay_verifiable_and_rejects_mismatched_evidence() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let physical_schema_hash =
        cdf_kernel::canonical_arrow_schema_hash(resource.schema().as_ref()).unwrap();
    let coercion_plan = plan
        .compiled_schema_admission
        .instantiate(resource.schema().as_ref(), &physical_schema_hash)
        .unwrap();
    let physical_observation =
        crate::PhysicalObservationEvidence::arrow_schema(resource.schema().as_ref()).unwrap();
    let physical_observation_hash = physical_observation.identity_hash().unwrap();
    let evidence = CompiledStreamAdmissionEvidence {
        compiled_admission_hash: cdf_runtime::artifact_hash(&plan.compiled_schema_admission)
            .unwrap(),
        baseline_schema_hash: plan.schema_authority.baseline_schema_hash.to_string(),
        effective_schema_hash: plan.schema_authority.effective_schema_hash.to_string(),
        physical_observation_catalog: BTreeMap::from([(
            physical_observation_hash.to_string(),
            physical_observation,
        )]),
        observations: vec![
            StreamAdmissionObservationEvidence::new(
                "partition-1",
                physical_observation_hash,
                coercion_plan,
                crate::StreamAdmissionCompletion::Complete {
                    source_position: terminal_file_position(),
                    partition_binding: cdf_kernel::SchemaObservationBinding::new(format!(
                        "sha256:{:064x}",
                        1
                    ))
                    .unwrap(),
                },
            )
            .unwrap(),
        ],
    };

    evidence.validate(&plan.compiled_schema_admission).unwrap();
    let mut unbound = evidence.clone();
    unbound.observations[0].physical_observation_hash = "sha256:unbound".to_owned();
    let error = unbound
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("absent physical observation"),
        "{error}"
    );
    let mut unauthorized = evidence.clone();
    unauthorized.observations[0].coercion_plan.fields[0].decision =
        FieldCoercionDecision::LossyAllowed;
    let error = unauthorized
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("compiled coercion verdict"),
        "{error}"
    );

    let mut forged = evidence.clone();
    forged.observations[0].coercion_plan.fields[0].decision = FieldCoercionDecision::Missing;
    let error = forged
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("compiled coercion verdict"),
        "{error}"
    );

    let mut with_unused_catalog_entry = evidence.clone();
    let unused = crate::PhysicalObservationEvidence::materialized_output(
        resource.schema().as_ref(),
        resource.schema().as_ref(),
        Vec::<String>::new(),
    )
    .unwrap();
    with_unused_catalog_entry
        .physical_observation_catalog
        .insert(unused.identity_hash().unwrap().to_string(), unused);
    let error = with_unused_catalog_entry
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("exact referenced set"),
        "{error}"
    );

    let mut mismatched = evidence;
    mismatched.compiled_admission_hash = "sha256:mismatched".to_owned();
    let error = mismatched
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(error.to_string().contains("does not match"), "{error}");
}

#[test]
fn compiled_stream_admission_enforces_unknown_and_widening_verdicts() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    assert!(
        plan.compiled_schema_admission
            .captures_unknown_fields()
            .unwrap(),
        "a fixed evolve schema must preserve admitted unknown fields in its residual capture"
    );

    plan.compiled_schema_admission
        .schema_verdicts
        .iter_mut()
        .find(|rule| rule.change == SchemaChangeKind::UnknownField)
        .unwrap()
        .verdict = VerdictAction::RejectRun;
    let unknown = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("unexpected", DataType::Int64, true),
    ]);
    let unknown_hash = cdf_kernel::canonical_arrow_schema_hash(&unknown).unwrap();
    let error = plan
        .compiled_schema_admission
        .instantiate(&unknown, &unknown_hash)
        .unwrap_err();
    assert!(error.to_string().contains("unknown field"), "{error}");

    plan.compiled_schema_admission
        .schema_verdicts
        .iter_mut()
        .find(|rule| rule.change == SchemaChangeKind::UnknownField)
        .unwrap()
        .verdict = VerdictAction::AdmitAsVariant;
    plan.compiled_schema_admission
        .schema_verdicts
        .iter_mut()
        .find(|rule| rule.change == SchemaChangeKind::TypeWidening)
        .unwrap()
        .verdict = VerdictAction::RejectBatch;
    let narrow = Schema::new(vec![
        Field::new("id", DataType::Int16, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]);
    let narrow_hash = cdf_kernel::canonical_arrow_schema_hash(&narrow).unwrap();
    let error = plan
        .compiled_schema_admission
        .instantiate(&narrow, &narrow_hash)
        .unwrap_err();
    assert!(error.to_string().contains("width coercion"), "{error}");
}

#[test]
fn pinned_baseline_admission_projects_full_physical_catalog_before_hashing() {
    let baseline_physical = sample_schema();
    let baseline_hash =
        cdf_kernel::canonical_arrow_schema_hash(baseline_physical.as_ref()).unwrap();
    let effective = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let resource = MockResource::tier_b(sample_batches())
        .with_schema(effective.clone())
        .with_baseline_observation_schema_catalog(vec![EffectiveSchemaCatalogEntry::new(
            baseline_hash,
            baseline_physical,
        )]);
    let mut input = plan_input_for_schema(
        effective,
        Vec::new(),
        Some(vec!["id".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    input
        .validation_program
        .schema_verdicts
        .iter_mut()
        .find(|rule| rule.change == SchemaChangeKind::TypeWidening)
        .unwrap()
        .verdict = VerdictAction::RejectBatch;

    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    assert_eq!(
        plan.compiled_schema_admission.baseline_projection,
        Some(vec!["id".to_owned()])
    );
    let projected_baseline = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let projected_hash = cdf_kernel::canonical_arrow_schema_hash(&projected_baseline).unwrap();
    plan.compiled_schema_admission
        .instantiate(&projected_baseline, &projected_hash)
        .unwrap();

    let new_drift = Schema::new(vec![Field::new("id", DataType::Int16, false)]);
    let drift_hash = cdf_kernel::canonical_arrow_schema_hash(&new_drift).unwrap();
    assert!(
        plan.compiled_schema_admission
            .instantiate(&new_drift, &drift_hash)
            .unwrap_err()
            .message
            .contains("width coercion")
    );
}

#[test]
fn materialized_stream_admission_rejects_noncanonical_provenance_and_nullable_claims() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let output_schema = plan
        .compiled_schema_admission
        .constraint_schema
        .to_arrow()
        .unwrap();
    let output_hash = cdf_kernel::canonical_arrow_schema_hash(output_schema.as_ref()).unwrap();
    let coercion = plan
        .compiled_schema_admission
        .instantiate(output_schema.as_ref(), &output_hash)
        .unwrap();
    let physical = crate::PhysicalObservationEvidence::materialized_output(
        output_schema.as_ref(),
        output_schema.as_ref(),
        Vec::<String>::new(),
    )
    .unwrap();
    let physical_hash = physical.identity_hash().unwrap();
    let evidence = CompiledStreamAdmissionEvidence::new(
        &plan.compiled_schema_admission,
        BTreeMap::from([(physical_hash.to_string(), physical)]),
        vec![
            StreamAdmissionObservationEvidence::new(
                "part-0",
                physical_hash,
                coercion,
                crate::StreamAdmissionCompletion::CompleteUnpositioned {
                    partition_binding: cdf_kernel::SchemaObservationBinding::new(format!(
                        "sha256:{:064x}",
                        2
                    ))
                    .unwrap(),
                },
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let mut forged_reason = evidence.clone();
    forged_reason.observations[0].coercion_plan.fields[0].reason = "forged".to_owned();
    let error = forged_reason
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("typed physical observation"),
        "{error}"
    );

    let mut forged_relation = evidence.clone();
    let forged_relation_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]);
    let forged_physical = crate::PhysicalObservationEvidence::materialized_output(
        &forged_relation_schema,
        output_schema.as_ref(),
        Vec::<String>::new(),
    )
    .unwrap();
    let forged_hash = forged_physical.identity_hash().unwrap();
    forged_relation.physical_observation_catalog =
        BTreeMap::from([(forged_hash.to_string(), forged_physical)]);
    forged_relation.observations[0].physical_observation_hash = forged_hash.to_string();
    let error = forged_relation
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("typed physical observation"),
        "{error}"
    );

    let mut forged_nullable = evidence;
    let forged_physical = crate::PhysicalObservationEvidence::materialized_output(
        output_schema.as_ref(),
        output_schema.as_ref(),
        ["id".to_owned()],
    )
    .unwrap();
    let forged_hash = forged_physical.identity_hash().unwrap();
    forged_nullable.physical_observation_catalog =
        BTreeMap::from([(forged_hash.to_string(), forged_physical)]);
    forged_nullable.observations[0].physical_observation_hash = forged_hash.to_string();
    let error = forged_nullable
        .validate(&plan.compiled_schema_admission)
        .unwrap_err();
    assert!(
        error.to_string().contains("nullable residual identities"),
        "{error}"
    );
}

#[test]
fn preobserved_baseline_widening_survives_the_drift_reject_verdict() {
    let physical_schema = sample_schema();
    let effective_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let evidence = bound_effective_schema_evidence(
        SchemaHash::new("effective-preobserved-v1").unwrap(),
        "manifest-preobserved-v1",
        ".cdf/schemas/orders@manifest-preobserved-v1.discovery.json",
        vec![EffectiveSchemaObservationEvidence::new(
            "input-0",
            physical_hash.clone(),
            schema_observation_binding("input-0"),
        )],
    );
    let runtime = EffectiveSchemaRuntime::new(
        evidence,
        vec![EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap();
    let resource = MockResource::tier_b(sample_batches())
        .with_effective_schema_runtime(effective_schema.clone(), runtime);
    let mut input = plan_input_for_schema(
        effective_schema,
        vec![],
        None,
        None,
        ExecutionExtent::bounded(),
    );
    input
        .validation_program
        .schema_verdicts
        .iter_mut()
        .find(|rule| rule.change == SchemaChangeKind::TypeWidening)
        .unwrap()
        .verdict = VerdictAction::RejectBatch;

    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let widening = plan
        .effective_schema_evidence
        .as_ref()
        .unwrap()
        .observations[0]
        .coercion_plan
        .fields
        .iter()
        .find(|field| field.source_name == "id")
        .unwrap();
    assert_eq!(widening.decision, FieldCoercionDecision::Widened);
}

#[test]
fn planning_rejects_one_schema_observation_identity_across_partitions() {
    let schema = sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap();
    let evidence = bound_effective_schema_evidence(
        SchemaHash::new("effective-unique-observations-v1").unwrap(),
        "manifest-unique-observations-v1",
        ".cdf/schemas/orders@manifest-unique-observations-v1.discovery.json",
        vec![EffectiveSchemaObservationEvidence::new(
            "input-0",
            physical_hash.clone(),
            schema_observation_binding("input-0"),
        )],
    );
    let runtime = EffectiveSchemaRuntime::new(
        evidence,
        vec![EffectiveSchemaCatalogEntry::new(
            physical_hash,
            schema.clone(),
        )],
    )
    .unwrap();
    let resource = MockResource::tier_b(sample_batches())
        .with_effective_schema_runtime(schema, runtime)
        .with_duplicate_observation_identity();

    let error = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap_err();

    assert!(
        error.to_string().contains("assigned to planned partitions"),
        "{error}"
    );
}

#[test]
fn dynamic_planning_rejects_duplicate_observation_identity_without_runtime_evidence() {
    let resource = MockResource::tier_b(sample_batches()).with_duplicate_observation_identity();

    let error = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap_err();

    assert!(
        error.to_string().contains("assigned to planned partitions"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn execution_rejects_duplicate_planned_observations_before_staged_ingress() {
    let resource = MockResource::tier_b(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    for partition in plan.scan.inline_partitions_mut().unwrap() {
        partition.metadata.insert(
            PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
            "forged-shared-observation".to_owned(),
        );
    }
    let package_dir = TempDir::new().unwrap();
    let durable_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls = Arc::clone(&durable_calls);
    let mut durable_segment = move |_entry: &SegmentEntry, _payload: DurableSegmentPayload| {
        hook_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    };
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let mut stream_finalize = || Ok(());

    let error = block_on(execute_to_package_with_streaming_hooks(
        &plan,
        &resource,
        package_dir.path(),
        &pre_finalize,
        &mut durable_segment,
        &mut stream_finalize,
        EngineExecutionConfig::default(),
    ))
    .unwrap_err();

    assert!(
        error.to_string().contains("assigned to planned partitions"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(durable_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn execution_rejects_batch_labeled_for_another_partition_before_admission() {
    let resource = MockResource::tier_a(vec![batch_for_partition_with_schema(
        "misrouted-batch",
        "part-1",
        sample_schema(),
        vec![1],
        vec!["one"],
        vec![true],
    )])
    .with_misrouted_batches();
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let package_dir = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, package_dir.path())).unwrap_err();

    assert!(
        error.to_string().contains("planned partition")
            && error.to_string().contains("received batch")
            && error.to_string().contains("part-1"),
        "{error}"
    );
}

#[test]
fn execution_evidence_rejects_repeated_observation_identity_even_when_identical() {
    let observation = cdf_kernel::ProcessedObservationPosition::new(
        "input-0",
        cdf_kernel::ProcessedObservationOutcome::Admitted,
        terminal_file_position(),
    )
    .unwrap();

    let error = EngineExecutionEvidence::new(
        vec![observation.clone(), observation],
        Vec::new(),
        None,
        true,
    )
    .unwrap_err();

    assert!(
        error.to_string().contains("more than one partition"),
        "{error}"
    );
}

#[test]
fn missing_control_critical_field_becomes_a_named_schema_quarantine() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let physical = Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]);
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(&physical).unwrap();

    let outcome = plan
        .compiled_schema_admission
        .instantiate_or_quarantine("missing-id", &physical, &physical_hash)
        .unwrap();
    let CompiledSchemaAdmissionOutcome::Quarantined(quarantine) = outcome else {
        panic!("control-critical missing field must not be admitted");
    };
    assert_eq!(
        quarantine.rule_id(),
        "schema-observation:control-critical-missing"
    );
    assert_eq!(quarantine.error_code(), "schema_control_field_missing");
}

#[test]
fn recorded_schema_quarantine_must_match_the_compiled_admission_action() {
    let resource = MockResource::tier_b(Vec::new());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let physical = incompatible_sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical.as_ref()).unwrap();
    let quarantine = TerminalSchemaObservationQuarantine::new(
        "input-0",
        physical_hash,
        "schema-observation:freeze-deviation",
        "schema_observation_quarantined",
        SchemaObservationPolicy::Freeze,
        "restore the pinned schema for this input",
        vec![
            SchemaObservationFieldQuarantine::whole_schema("incompatible physical schema").unwrap(),
        ],
    )
    .unwrap();
    let physical_evidence = PhysicalObservationEvidence::arrow_schema(physical.as_ref()).unwrap();

    let error = plan
        .compiled_schema_admission
        .validate_quarantined_observation(&quarantine, &physical_evidence)
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not match the compiled admission action"),
        "{error}"
    );
}

#[test]
fn validation_program_rebind_rejects_new_physical_dependencies_without_mutating_plan() {
    let resource = MockResource::tier_b(sample_batches());
    let mut input = plan_input(
        Vec::new(),
        Some(vec!["name".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    input.validation_program = compile_validation_program(
        &ContractPolicy::evolve(),
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    input.validation_program.row_rules.clear();
    let mut plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    assert_eq!(
        plan.scan.request.projection,
        Some(vec!["id".to_owned(), "name".to_owned()])
    );
    let original = plan.clone();
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Nullability {
        column: "active".to_owned(),
    }];
    let replacement = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();

    let error = plan
        .rebind_validation_program(replacement, resource.schema().as_ref())
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("outside the compiled physical projection")
    );
    assert_eq!(plan, original);
}

#[test]
fn tier_b_exact_temporal_pushdown_selects_recorded_source_lowering_without_residual() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    let resource = MockResource::tier_b(Vec::new()).with_schema(schema.clone());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input_for_schema(
                schema,
                vec!["updated_at >= '2026-07-12T00:00:00Z'"],
                None,
                None,
                ExecutionExtent::bounded(),
            ),
        )
        .unwrap();

    assert!(plan.residual_predicates.is_empty());
    assert!(plan.compiled_expression_plan.residuals.is_empty());
    assert_eq!(
        plan.compiled_expression_plan.predicates[0].optimizer.name,
        cdf_contract::SOURCE_EXACT_PUSHDOWN_OPTIMIZER
    );
    plan.validate_compiled_expression_plan().unwrap();
}

#[test]
fn preview_traverses_every_planned_partition_through_the_engine_front_end() {
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let limits = EnginePreviewLimits::default();

    let preview = block_on(preview_resource(&plan, &resource, limits.clone())).unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(preview.planned_partition_count, 2);
    assert_eq!(preview.payload_opened_partition_count, 2);
    assert_eq!(preview.attested_partition_count, 0);
    assert_eq!(preview.inspected_partition_count, 2);
    assert_eq!(preview.inspected_batch_count, 2);
    assert_eq!(preview.selected_partition_count, 2);
    assert_eq!(
        preview.selection.policy,
        PREVIEW_POLICY_BALANCED_STRATIFIED_V1
    );
    assert_eq!(preview.selection.selector, STRATIFIED_HASH_SELECTOR_V1);
    assert_eq!(preview.selection.selected.len(), 2);
    assert_eq!(preview.selection.selected[0].batch_quota, 32);
    assert_eq!(preview.selection.selected[1].batch_quota, 32);
    assert_eq!(preview.row_count, 6);
    assert_eq!(preview.fields, vec!["id", "name", "active", "_cdf_variant"]);
    assert_eq!(preview.limits, limits);
    assert!(!preview.truncated);
}

#[test]
fn preview_rejects_stale_compiled_expression_plan_before_source_contact() {
    let resource = MockResource::tier_b(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(vec!["id >= 1"], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    plan.compiled_expression_plan.native_filter_lowering_version = "stale".to_owned();

    let error = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::default(),
    ))
    .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn preview_applies_explicit_row_limit_globally_without_opening_later_payloads() {
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, Some(2), ExecutionExtent::bounded()),
        )
        .unwrap();
    let limits = EnginePreviewLimits::default().with_max_rows(2).unwrap();

    let preview = block_on(preview_resource(&plan, &resource, limits)).unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 1);
    assert_eq!(preview.payload_opened_partition_count, 1);
    assert_eq!(preview.attested_partition_count, 0);
    assert_eq!(preview.inspected_partition_count, 1);
    assert_eq!(preview.inspected_batch_count, 1);
    assert_eq!(preview.row_count, 2);
    assert_eq!(
        preview
            .selection
            .selected_but_uninspected_partition_ids
            .len(),
        1
    );
    assert_eq!(preview.payload_uninspected_partition_count, 1);
    assert!(preview.truncated);
}

#[test]
fn preview_configured_byte_limit_accounts_decoded_input_separately_from_output() {
    let baseline_resource = MockResource::tier_b(sample_batches());
    let baseline_plan = Planner::new()
        .plan_tier_b(
            &baseline_resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap();
    let one_row = block_on(preview_resource(
        &baseline_plan,
        &baseline_resource,
        EnginePreviewLimits::default().with_max_rows(1).unwrap(),
    ))
    .unwrap();

    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::new(500, one_row.byte_count, 64).unwrap(),
    ))
    .unwrap();

    assert_eq!(preview.byte_count, one_row.byte_count);
    assert!(preview.output_byte_count > 0);
    assert_eq!(preview.inspected_batch_count, 1);
    assert_eq!(preview.payload_opened_partition_count, 1);
    assert_eq!(preview.payload_uninspected_partition_count, 1);
    assert!(preview.truncated);
}

#[test]
fn preview_rejects_an_oversized_batch_atomically() {
    let baseline_resource = MockResource::tier_b(sample_batches());
    let baseline_plan = Planner::new()
        .plan_tier_b(
            &baseline_resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let baseline = block_on(preview_resource(
        &baseline_plan,
        &baseline_resource,
        EnginePreviewLimits::default().with_max_rows(1).unwrap(),
    ))
    .unwrap();
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();

    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::new(500, baseline.byte_count - 1, 64).unwrap(),
    ))
    .unwrap();

    assert_eq!(preview.payload_opened_partition_count, 2);
    assert_eq!(preview.inspected_partition_count, 0);
    assert_eq!(preview.inspected_batch_count, 0);
    assert_eq!(preview.row_count, 0);
    assert_eq!(preview.byte_count, 0);
    assert_eq!(preview.output_byte_count, 0);
    assert_eq!(preview.payload_uninspected_partition_count, 2);
    assert!(preview.truncated);
}

#[test]
fn preview_fair_batch_quotas_are_fixed_before_payload_io() {
    let resource = MockResource::tier_b(sample_batches()).with_partition_count(3);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();

    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::new(500, DEFAULT_PREVIEW_MAX_BYTES, 8).unwrap(),
    ))
    .unwrap();

    assert_eq!(preview.selected_partition_count, 3);
    assert_eq!(
        preview
            .selection
            .selected
            .iter()
            .map(|partition| partition.batch_quota)
            .collect::<Vec<_>>(),
        vec![3, 3, 2]
    );
}

#[test]
fn preview_large_plan_selects_and_opens_at_most_the_global_batch_budget() {
    let resource = MockResource::tier_b(Vec::new()).with_partition_count(10_000);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();

    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::default(),
    ))
    .unwrap();

    assert_eq!(preview.planned_partition_count, 10_000);
    assert_eq!(preview.payload_eligible_partition_count, 10_000);
    assert_eq!(preview.selected_partition_count, 64);
    assert_eq!(preview.payload_opened_partition_count, 64);
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 64);
    assert_eq!(preview.selection.selected.len(), 64);
    assert!(
        preview
            .selection
            .selected
            .iter()
            .all(|partition| partition.batch_quota == 1)
    );
    assert_eq!(preview.inspected_partition_count, 64);
    assert_eq!(preview.payload_uninspected_partition_count, 9_936);
    assert!(preview.truncated);
}

#[test]
fn preview_terminal_quarantine_uses_run_attestation_without_opening_payloads() {
    let effective_schema = sample_schema();
    let physical_schema = incompatible_sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash.clone());
    let resource = MockResource::tier_b(sample_batches())
        .with_effective_schema_runtime(effective_schema, runtime)
        .with_attestation(PartitionAttestation::new(
            terminal_file_position(),
            Some(physical_hash),
        ));
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();

    let preview = block_on(preview_resource(
        &plan,
        &resource,
        EnginePreviewLimits::default(),
    ))
    .unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 2);
    assert_eq!(preview.planned_partition_count, 2);
    assert_eq!(preview.payload_opened_partition_count, 0);
    assert_eq!(preview.attested_partition_count, 2);
    assert_eq!(preview.terminal_quarantine_count, 2);
    assert_eq!(preview.row_count, 0);
}

#[test]
fn execution_returns_segment_source_position_evidence() {
    let resource = MockResource::tier_a(vec![batch_with_file_position()]);
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert_eq!(output.output.identity_segments().len(), 1);
    assert_eq!(output.segment_positions.len(), 1);
    assert_eq!(
        output.segment_positions[0].segment_id,
        output.output.identity_segments()[0].segment_id
    );
    let Some(SourcePosition::FileManifest(manifest)) = &output.segment_positions[0].output_position
    else {
        panic!("expected file manifest position evidence");
    };
    assert_eq!(manifest.files[0].path, "/tmp/cdf/events.ndjson");
}

#[test]
fn tier_b_negotiates_pushdown_fidelity_without_io() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true", "name != 'missing'"],
        Some(vec!["name".to_owned()]),
        Some(10),
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();

    assert_eq!(resource.negotiate_count.load(Ordering::SeqCst), 1);
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(plan.scan.pushed_predicates.len(), 2);
    assert_eq!(
        plan.scan.pushed_predicates[0].fidelity,
        PushdownFidelity::Exact
    );
    assert_eq!(
        datafusion_filter_pushdown(&plan.scan.pushed_predicates[0].fidelity),
        datafusion::logical_expr::TableProviderFilterPushDown::Exact
    );
    assert_eq!(
        plan.scan.pushed_predicates[1].fidelity,
        PushdownFidelity::Inexact
    );
    assert_eq!(plan.scan.unsupported_predicates.len(), 1);
    assert_eq!(plan.residual_predicates.len(), 2);
    assert!(plan.explain.projection_pushed);
    assert!(plan.explain.limit_pushed);
    assert_eq!(plan.explain.inexact_predicates.len(), 1);
    assert_eq!(plan.explain.unsupported_predicates.len(), 1);
    assert_eq!(plan.explain.partitions.len(), 2);
    assert_eq!(plan.explain.estimates.rows, Some(3));
    assert_eq!(
        plan.explain.delivery_guarantee,
        DeliveryGuarantee::EffectivelyOncePerKey
    );
}

#[test]
fn tier_b_explain_serializes_honest_cdf_native_operator_metadata() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true", "name != 'missing'"],
        Some(vec!["name".to_owned()]),
        Some(10),
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let explain_json = serde_json::to_value(&plan.explain).unwrap();

    assert_honest_cdf_native_operator_metadata(&plan);
    assert_explain_carries_required_fields(&explain_json);
    assert_eq!(plan.explain.pushed_predicates.len(), 2);
    assert_eq!(plan.explain.inexact_predicates.len(), 1);
    assert_eq!(plan.explain.unsupported_predicates.len(), 1);
    assert!(plan.explain.projection_pushed);
    assert!(plan.explain.limit_pushed);
    assert_eq!(plan.explain.partitions.len(), 2);
    assert_eq!(plan.explain.estimates.rows, Some(3));
    assert_eq!(
        plan.explain.delivery_guarantee,
        DeliveryGuarantee::EffectivelyOncePerKey
    );
}

#[test]
fn engine_plan_deserialization_rejects_missing_required_execution_policy() {
    let resource =
        MockResource::tier_a(sample_batches()).with_write_disposition(WriteDisposition::Append);
    let input = plan_input(Vec::new(), None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let mut plan_json = serde_json::to_value(&plan).unwrap();
    plan_json
        .as_object_mut()
        .unwrap()
        .remove("execution_extent");
    let error = serde_json::from_value::<EnginePlan>(plan_json).unwrap_err();
    assert!(error.to_string().contains("execution_extent"));

    let mut plan_json = serde_json::to_value(&plan).unwrap();
    plan_json
        .as_object_mut()
        .unwrap()
        .remove("write_disposition");
    let error = serde_json::from_value::<EnginePlan>(plan_json).unwrap_err();
    assert!(error.to_string().contains("write_disposition"));

    let mut plan_json = serde_json::to_value(&plan).unwrap();
    for operator in plan_json["operator_chain"].as_array_mut().unwrap() {
        if operator["kind"] == "package_sink" {
            operator.as_object_mut().unwrap().remove("segmentation");
        }
    }
    let error = serde_json::from_value::<EnginePlan>(plan_json).unwrap_err();
    assert!(error.to_string().contains("segmentation"));
}

#[test]
fn effective_schema_binds_only_the_attempted_partition_observation_under_limit() {
    let effective_schema = sample_schema();
    let physical_schema = sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let mut batches = vec![
        batch_for_partition_with_schema(
            "batch-limit-0",
            "part-0",
            physical_schema.clone(),
            vec![1, 2, 3],
            vec!["one", "two", "three"],
            vec![true, true, true],
        ),
        batch_for_partition_with_schema(
            "batch-limit-1",
            "part-1",
            physical_schema.clone(),
            vec![4, 5, 6],
            vec!["four", "five", "six"],
            vec![true, true, true],
        ),
    ];
    for batch in &mut batches {
        batch.header.observed_schema_hash = physical_hash.clone();
        batch.header.source_position = Some(terminal_file_position());
    }
    let evidence = bound_effective_schema_evidence(
        SchemaHash::new("effective-snapshot-v1").unwrap(),
        "manifest-v1",
        ".cdf/schemas/orders@manifest-v1.discovery.json",
        vec![EffectiveSchemaObservationEvidence::new(
            "input-0",
            physical_hash.clone(),
            schema_observation_binding("input-0"),
        )],
    );
    let runtime = EffectiveSchemaRuntime::new(
        evidence,
        vec![EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap()
    .with_discovery_executor_budget(
        DiscoveryExecutorBudgetEvidence::new(64, 1_000, 128, 2).unwrap(),
    )
    .unwrap();
    let resource =
        MockResource::tier_b(batches).with_effective_schema_runtime(effective_schema, runtime);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap();
    assert_eq!(
        plan.effective_schema_evidence().unwrap().observations.len(),
        1
    );

    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 1);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 0);
    let witnessed: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(witnessed["observations"].as_array().unwrap().len(), 1);
    assert_eq!(
        witnessed["observations"][0]["completion"]["kind"],
        "partial"
    );

    let mut tampered = plan.clone();
    tampered
        .effective_schema_evidence
        .as_mut()
        .unwrap()
        .discovery_executor_budget =
        Some(DiscoveryExecutorBudgetEvidence::new(32, 1_000, 128, 2).unwrap());
    let tampered_package = TempDir::new().unwrap();
    let error = block_on(execute_to_package(
        &tampered,
        &resource,
        tampered_package.path(),
    ))
    .unwrap_err();
    assert!(
        error.to_string().contains("discovery executor budget"),
        "{error}"
    );
}

#[test]
fn pushed_projection_rebinds_preobserved_physical_evidence_before_execution() {
    let effective_schema = sample_schema();
    let physical_schema = sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let projected_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let projected_hash =
        cdf_kernel::canonical_arrow_schema_hash(projected_schema.as_ref()).unwrap();
    let record_batch = RecordBatch::try_new(
        projected_schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-projected-preobserved").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        projected_hash.clone(),
        record_batch,
    )
    .unwrap();
    batch.header.source_position = Some(terminal_file_position());

    let evidence = bound_effective_schema_evidence(
        SchemaHash::new("effective-projected-v1").unwrap(),
        "manifest-projected-v1",
        ".cdf/schemas/orders@manifest-projected-v1.discovery.json",
        vec![EffectiveSchemaObservationEvidence::new(
            "input-0",
            physical_hash.clone(),
            schema_observation_binding("input-0"),
        )],
    );
    let runtime = EffectiveSchemaRuntime::new(
        evidence,
        vec![EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap();
    let resource = MockResource::tier_b(vec![batch])
        .with_partition_count(1)
        .with_effective_schema_runtime(effective_schema, runtime);
    let mut input = plan_input(
        Vec::new(),
        Some(vec!["id".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    input.validation_program.row_rules.clear();
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let planned = &plan.effective_schema_evidence().unwrap().observations[0];
    assert_eq!(
        planned.physical_schema_hash,
        projected_hash,
        "compiled projection: {:?}",
        plan.scan.inline_partitions().unwrap()[0]
            .scan_intent
            .projection
    );
    assert_eq!(
        plan.scan.inline_partitions().unwrap()[0]
            .metadata
            .get(PLAN_PHYSICAL_SCHEMA_HASH_KEY),
        Some(&projected_hash.to_string())
    );

    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
}

#[test]
fn limited_multi_batch_partition_records_exact_non_checkpointing_partial_attempt() {
    let mut batches = sample_batches();
    for batch in &mut batches {
        batch.header.partition_id = PartitionId::new("part-0").unwrap();
        batch.header.source_position = Some(terminal_file_position());
    }
    let resource = MockResource::tier_a(batches);
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, None);
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert_eq!(resource.batch_poll_count.load(Ordering::SeqCst), 1);
    assert!(!output.execution_evidence().checkpoint_eligible());

    let evidence: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let [observation] = evidence.observations.as_slice() else {
        panic!("expected exactly one partial schema observation");
    };
    match &observation.completion {
        crate::StreamAdmissionCompletion::Partial {
            attempted_position: Some(position),
            observed_rows,
            partition_binding,
        } => {
            assert_eq!(position, &terminal_file_position());
            assert_eq!(*observed_rows, 3);
            assert!(partition_binding.as_str().starts_with("sha256:"));
        }
        other => panic!("expected exact partial attempt, got {other:?}"),
    }
    assert!(
        !temp
            .path()
            .join(cdf_package_contract::PROCESSED_OBSERVATIONS_FILE)
            .exists()
    );
}

#[test]
fn limited_cursor_batch_never_assigns_unsliced_position_to_output_segments() {
    let attempted = SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: "id".to_owned(),
        value: CursorValue::I64(3),
    });
    let mut batches = sample_batches();
    for batch in &mut batches {
        batch.header.partition_id = PartitionId::new("part-0").unwrap();
        batch.header.source_position = Some(attempted.clone());
    }
    let resource = MockResource::tier_a(batches);
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, None);
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert!(!output.execution_evidence().checkpoint_eligible());
    assert_eq!(output.segment_positions.len(), 1);
    assert_eq!(output.segment_positions[0].output_position, None);

    let evidence: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let [observation] = evidence.observations.as_slice() else {
        panic!("expected exactly one partial schema observation");
    };
    match &observation.completion {
        crate::StreamAdmissionCompletion::Partial {
            attempted_position: Some(position),
            ..
        } => assert_eq!(position, &attempted),
        other => panic!("expected exact partial attempt, got {other:?}"),
    }
}

#[test]
fn terminal_schema_observation_quarantine_processes_distinct_partitions_without_opening_data() {
    let effective_schema = sample_schema();
    let physical_schema = incompatible_sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash.clone());
    let processed_position = terminal_file_position();
    let secret_batches = vec![batch_for_partition_with_schema(
        "secret-batch",
        "part-0",
        effective_schema.clone(),
        vec![1],
        vec!["super-secret-row-value"],
        vec![true],
    )];
    let resource = MockResource::tier_b(secret_batches)
        .with_effective_schema_runtime(effective_schema, runtime)
        .with_attestation(PartitionAttestation::new(
            processed_position.clone(),
            Some(physical_hash),
        ));
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert!(output.output.identity_segments().is_empty());
    assert!(output.segment_positions.is_empty());
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 2);
    let processed = output.execution_evidence().processed_observations();
    assert_eq!(processed.len(), 2);
    assert!(
        processed
            .iter()
            .all(|observation| observation.source_position == processed_position)
    );
    assert!(
        temp.path()
            .join("quarantine/schema-observations.json")
            .is_file()
    );
    assert!(
        temp.path()
            .join("quarantine/schema-admission-evidence.json")
            .is_file()
    );
    assert!(!temp.path().join("quarantine/records.parquet").is_file());
    let terminal_json =
        std::fs::read_to_string(temp.path().join("quarantine/schema-observations.json")).unwrap();
    assert!(!terminal_json.contains("super-secret-row-value"));

    let mut conflicting = plan.clone();
    conflicting.scan.inline_partitions_mut().unwrap()[1]
        .metadata
        .insert(
            PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
            "conflicting-binding".to_owned(),
        );
    let conflicting_package = TempDir::new().unwrap();
    let error = block_on(execute_to_package(
        &conflicting,
        &resource,
        conflicting_package.path(),
    ))
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("missing or spoofed cdf:schema_observation_binding"),
        "{error}"
    );
}

#[test]
fn terminal_schema_observation_attestation_change_aborts_before_processed_evidence() {
    let effective_schema = sample_schema();
    let physical_schema = incompatible_sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash);
    let resource = MockResource::tier_b(Vec::new())
        .with_effective_schema_runtime(effective_schema, runtime)
        .with_attestation(PartitionAttestation::new(
            terminal_file_position(),
            Some(SchemaHash::new("changed-physical-schema").unwrap()),
        ));
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(
        error.to_string().contains("changed physical schema"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    assert!(
        !temp
            .path()
            .join("state/processed-observations.json")
            .exists()
    );
    assert!(
        !temp
            .path()
            .join("quarantine/schema-observations.json")
            .exists()
    );
}

#[test]
fn terminal_schema_observation_identity_attestation_failure_aborts_before_processed_evidence() {
    let effective_schema = sample_schema();
    let physical_schema = incompatible_sample_schema();
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let runtime = terminal_effective_schema_runtime(physical_schema, physical_hash);
    let resource = MockResource::tier_b(Vec::new())
        .with_effective_schema_runtime(effective_schema, runtime)
        .with_attestation_error("file identity changed between planning and execution");
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(
        error.to_string().contains("file identity changed"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    assert!(
        !temp
            .path()
            .join("state/processed-observations.json")
            .exists()
    );
}

fn terminal_effective_schema_runtime(
    physical_schema: SchemaRef,
    physical_hash: SchemaHash,
) -> EffectiveSchemaRuntime {
    let authority_plan = Planner::new()
        .plan_tier_b(
            &MockResource::tier_b(Vec::new()),
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let CompiledSchemaAdmissionOutcome::Quarantined(terminal_0) = authority_plan
        .compiled_schema_admission
        .instantiate_or_quarantine("input-0", physical_schema.as_ref(), &physical_hash)
        .unwrap()
    else {
        panic!("incompatible fixture must compile to terminal quarantine");
    };
    let CompiledSchemaAdmissionOutcome::Quarantined(terminal_1) = authority_plan
        .compiled_schema_admission
        .instantiate_or_quarantine("input-1", physical_schema.as_ref(), &physical_hash)
        .unwrap()
    else {
        panic!("incompatible fixture must compile to terminal quarantine");
    };
    let evidence = bound_effective_schema_evidence(
        SchemaHash::new("effective-snapshot-v1").unwrap(),
        "manifest-v1",
        ".cdf/schemas/orders@manifest-v1.discovery.json",
        vec![
            EffectiveSchemaObservationEvidence::new(
                "input-0",
                physical_hash.clone(),
                schema_observation_binding("input-0"),
            ),
            EffectiveSchemaObservationEvidence::new(
                "input-1",
                physical_hash.clone(),
                schema_observation_binding("input-1"),
            ),
        ],
    );
    EffectiveSchemaRuntime::new(
        evidence,
        vec![EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap()
    .with_terminal_quarantines(vec![*terminal_0, *terminal_1])
    .unwrap()
    .with_discovery_executor_budget(
        DiscoveryExecutorBudgetEvidence::new(64, 1_000, 128, 2).unwrap(),
    )
    .unwrap()
}

fn terminal_file_position() -> SourcePosition {
    SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "input-0".to_owned(),
            size_bytes: 10,
            source_generation: None,
            etag: Some("etag-0".to_owned()),
            object_version: None,
            sha256: Some(format!("sha256:{}", "ab".repeat(32))),
        }],
    })
}

#[test]
fn inexact_and_unsupported_predicates_are_reapplied_during_execution() {
    let resource = MockResource::tier_b(sample_batches());
    let input = plan_input(
        vec!["id > 1", "active = true", "name != 'three'"],
        Some(vec!["name".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new())
            .unwrap(),
    );
    for segment in reader
        .verified_canonical_segment_stream(memory, 128 * 1024 * 1024)
        .unwrap()
    {
        let batches = segment.unwrap().batches;
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "two");
    }
}

#[test]
fn durable_segment_hook_runs_after_publish_with_exact_entry_and_batch() {
    let resource = MockResource::tier_b(sample_batches());
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(vec![], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let package_dir = TempDir::new().unwrap();
    let durable_root = package_dir.path().to_path_buf();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let hook_observed = Arc::clone(&observed);
    let retained_payloads = Arc::new(Mutex::new(Vec::new()));
    let hook_payloads = Arc::clone(&retained_payloads);
    let mut durable_segment = move |entry: &SegmentEntry, payload: DurableSegmentPayload| {
        assert!(durable_root.join(&entry.path).is_file());
        hook_observed.lock().unwrap().push((
            entry.segment_id.clone(),
            entry.sha256.clone(),
            entry.row_count,
            payload
                .batches()
                .iter()
                .map(|batch| batch.num_rows() as u64)
                .sum::<u64>(),
        ));
        hook_payloads.lock().unwrap().push(payload);
        Ok(())
    };
    fn pre_finalize(
        _builder: &cdf_package::PackageBuilder,
        _draft: EnginePackageDraft<'_>,
    ) -> Result<()> {
        Ok(())
    }
    let mut stream_finalize = || Ok(());

    let (_, services) = StandaloneExecutionHost::default_services(512 * 1024 * 1024).unwrap();
    let output = block_on(execute_to_package_with_streaming_hooks(
        &plan,
        &resource,
        package_dir.path(),
        &pre_finalize,
        &mut durable_segment,
        &mut stream_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();

    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), output.output.identity_segments().len());
    for (actual, expected) in observed.iter().zip(output.output.identity_segments()) {
        assert_eq!(&actual.0, &expected.segment_id);
        assert_eq!(&actual.1, &expected.sha256);
        assert_eq!(actual.2, actual.3);
        assert_eq!(actual.2, expected.row_count);
    }
    assert!(services.memory().snapshot().current_bytes > 0);
    retained_payloads.lock().unwrap().clear();
    assert_eq!(services.memory().snapshot().current_bytes, 0);
}

#[test]
fn canonical_segment_releases_construction_peak_before_durable_ingress() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let package_dir = TempDir::new().unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let hook_observed = Arc::clone(&observed);
    let mut durable_segment = move |_entry: &SegmentEntry, payload: DurableSegmentPayload| {
        let (_durable_local_file, batches, memory_leases) = payload.into_parts();
        let output_bytes =
            batches.iter().try_fold(0_u64, |total, batch| {
                total
                    .checked_add(u64::try_from(batch.get_array_memory_size()).map_err(|_| {
                        cdf_kernel::CdfError::data("durable payload bytes exceed u64")
                    })?)
                    .ok_or_else(|| cdf_kernel::CdfError::data("durable payload bytes overflow"))
            })?;
        let scratch_bytes = memory_leases
            .last()
            .ok_or_else(|| cdf_kernel::CdfError::internal("canonical scratch lease is absent"))?
            .bytes();
        hook_observed
            .lock()
            .unwrap()
            .push((scratch_bytes, output_bytes.max(1)));
        Ok(())
    };
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let mut stream_finalize = || Ok(());

    block_on(execute_to_package_with_streaming_hooks(
        &plan,
        &resource,
        package_dir.path(),
        &pre_finalize,
        &mut durable_segment,
        &mut stream_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();

    let observed = observed.lock().unwrap();
    assert!(!observed.is_empty());
    assert!(observed.iter().all(|(scratch, output)| scratch == output));
    assert_eq!(services.memory().snapshot().current_bytes, 0);
}

#[test]
fn resident_execution_plan_is_rejected_until_supervisor_exists() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(
        vec![],
        None,
        None,
        ExecutionExtent::Resident {
            version: EXECUTION_EXTENT_VERSION,
            policy: sample_stream_epoch_policy(),
        },
    );
    let error = Planner::new().plan_tier_a(&resource, input).unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(error.message.contains("resident execution is not enabled"));
}

#[test]
fn execution_extent_is_not_redefined_by_the_engine() {
    let source_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for entry in std::fs::read_dir(source_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs")
            || path.file_name().and_then(std::ffi::OsStr::to_str) == Some("tests.rs")
        {
            continue;
        }
        let source = std::fs::read_to_string(&path).unwrap();
        for forbidden in ["PlanBoundedness", "UnboundedLive", "UnboundedDrain"] {
            assert!(
                !source.contains(forbidden),
                "{} contains obsolete execution-extent authority {forbidden}",
                path.display()
            );
        }
        for line in source.lines() {
            let tokens = line.split_whitespace().collect::<Vec<_>>();
            for declaration in tokens.windows(2) {
                assert!(
                    !matches!(declaration[0], "enum" | "struct")
                        || !declaration[1].trim_end_matches('{').contains("Extent"),
                    "{} defines engine-owned extent type on line `{}`",
                    path.display(),
                    line.trim()
                );
            }
        }
    }
}

#[test]
fn execution_rejects_resident_extent_before_source_contact() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    plan.execution_extent = ExecutionExtent::Resident {
        version: EXECUTION_EXTENT_VERSION,
        policy: sample_stream_epoch_policy(),
    };
    plan.explain.execution_extent = plan.execution_extent.clone();

    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(error.message.contains("resident execution is not enabled"));
    assert!(std::fs::read_dir(temp.path()).unwrap().next().is_none());
}

#[test]
fn execution_rejects_drain_extent_before_source_contact() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let drain = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: sample_stream_epoch_policy(),
        termination: DrainTermination::Records { count: 10 },
    };
    plan.execution_extent = drain.clone();
    plan.explain.execution_extent = drain;

    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(
        error
            .message
            .contains("bounded and cannot use drain execution")
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
    assert!(std::fs::read_dir(temp.path()).unwrap().next().is_none());
}

#[test]
fn execution_rejects_divergent_recorded_extent_before_source_contact() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    plan.explain.execution_extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: sample_stream_epoch_policy(),
        termination: DrainTermination::Records { count: 10 },
    };

    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(
        error
            .message
            .contains("does not match its recorded explain extent")
    );
    assert!(std::fs::read_dir(temp.path()).unwrap().next().is_none());
}

#[test]
fn explain_and_operator_chain_carry_contract_package_details() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(
        vec!["active = true"],
        Some(vec!["id".to_owned(), "name".to_owned()]),
        Some(2),
        ExecutionExtent::Drain {
            version: EXECUTION_EXTENT_VERSION,
            policy: sample_stream_epoch_policy(),
            termination: DrainTermination::Records { count: 10 },
        },
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let explain_json = serde_json::to_value(&plan.explain).unwrap();

    assert_honest_cdf_native_operator_metadata(&plan);
    assert_explain_carries_required_fields(&explain_json);
    assert!(plan.operator_chain.iter().any(|operator| {
        matches!(
            operator,
            OperatorNode::ContractExec {
                normalizer_version,
                ..
            } if normalizer_version == cdf_contract::NORMALIZER_NAMECASE_V1
        )
    }));
    assert!(plan.operator_chain.iter().any(|operator| {
        matches!(
            operator,
            OperatorNode::PackageSink { package_id, segmentation }
                if package_id == "pkg-engine-test"
                    && segmentation == &CanonicalSegmentationPolicy::p3_v2()
        )
    }));
}

fn mock_compiled_source_plan(
    resource: &MockResource,
    retry_policy: Option<cdf_runtime::SourceRetryPolicy>,
) -> cdf_runtime::CompiledSourcePlan {
    mock_compiled_source_plan_with_speculation(resource, retry_policy, true)
}

fn mock_compiled_source_plan_with_speculation(
    resource: &MockResource,
    retry_policy: Option<cdf_runtime::SourceRetryPolicy>,
    speculative_safe: bool,
) -> cdf_runtime::CompiledSourcePlan {
    let retry_enabled = retry_policy.is_some();
    cdf_runtime::CompiledSourcePlan::new(
        cdf_runtime::SourceDriverDescriptor {
            driver_id: cdf_runtime::SourceDriverId::new("external_mock").unwrap(),
            driver_version: "mock-v1".to_owned(),
            option_schema_hash:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            kinds: vec!["external_mock".to_owned()],
            schemes: vec!["mock".to_owned()],
        },
        resource.capabilities().clone(),
        cdf_runtime::SourceExecutionCapabilities {
            minimum_poll_bytes: 1024,
            maximum_poll_bytes: 1024 * 1024,
            minimum_decode_bytes: 1024,
            maximum_decode_bytes: 8 * 1024 * 1024,
            maximum_concurrency: 8,
            useful_concurrency: 4,
            executor_class: cdf_runtime::SourceExecutorClass::Io,
            blocking_lane: None,
            pausable: true,
            spillable: false,
            idempotent_reads: true,
            reopenable: true,
            resumable: true,
            speculative_safe,
            retry_granularity: if retry_enabled {
                cdf_runtime::SourceRetryGranularity::Partition
            } else {
                cdf_runtime::SourceRetryGranularity::None
            },
            retryable_errors: retry_enabled
                .then_some(cdf_kernel::ErrorKind::Transient)
                .into_iter()
                .collect(),
            retry_policy,
            attestation: if retry_enabled || speculative_safe {
                cdf_runtime::SourceAttestationStrength::ImmutableContent
            } else {
                cdf_runtime::SourceAttestationStrength::None
            },
            rate_limit: None,
            quota_authority: None,
            canonical_order: true,
            bounded: true,
            batch_memory: if retry_enabled {
                cdf_runtime::SourceBatchMemoryContract::Preaccounted
            } else {
                cdf_runtime::SourceBatchMemoryContract::FrontierReserved
            },
            telemetry_version: "mock-v1".to_owned(),
        },
        cdf_runtime::CompiledSourcePlanInput {
            descriptor: resource.descriptor().clone(),
            schema: resource.schema().as_ref().clone(),
            type_policy_allowances: resource.type_policy_allowances,
            effective_schema_runtime: resource.effective_schema_runtime.clone(),
            baseline_observation_schema_catalog: resource
                .baseline_observation_schema_catalog
                .clone(),
            redacted_options: serde_json::json!({"endpoint": "redacted"}),
            physical_plan: serde_json::json!({"partitioning": "mock"}),
        },
    )
    .unwrap()
}

fn mock_unbounded_source_plan(resource: &MockResource) -> cdf_runtime::CompiledSourcePlan {
    let mut source = mock_compiled_source_plan(resource, None);
    source.execution_capabilities.bounded = false;
    source.execution_capabilities.speculative_safe = false;
    source.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
        quiescence: true,
        watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Drop,
        watermark: None,
        safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
        source_frontiers: vec![cdf_runtime::SourceFrontierCapability::FileManifest],
        idleness_capabilities: Vec::new(),
    });
    source.validate().unwrap();
    source
}

fn mock_unbounded_cursor_source_plan(resource: &MockResource) -> cdf_runtime::CompiledSourcePlan {
    let mut source = mock_unbounded_source_plan(resource);
    source.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
        quiescence: true,
        watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Drop,
        watermark: None,
        safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
        source_frontiers: vec![cdf_runtime::SourceFrontierCapability::Cursor {
            fields: vec!["id".to_owned()],
        }],
        idleness_capabilities: Vec::new(),
    });
    source.validate().unwrap();
    source
}

#[test]
fn drain_epochs_stop_at_canonical_partition_frontiers_and_require_settlement() {
    let mut batches = sample_batches();
    for (ordinal, batch) in batches.iter_mut().enumerate() {
        batch.header.source_position = Some(SourcePosition::FileManifest(FileManifest {
            version: 1,
            files: vec![FilePosition {
                path: format!("input-{ordinal}.arrow"),
                size_bytes: batch.header.byte_count,
                source_generation: Some(format!("generation-{ordinal}")),
                etag: None,
                object_version: None,
                sha256: None,
            }],
        }));
    }
    let resource = MockResource::tier_b(batches).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 3 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 6 },
    };
    let source = mock_unbounded_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let root = TempDir::new().unwrap();
    let first_dir = root.path().join("epoch-0");
    let first = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        &first_dir,
        &pre_finalize,
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap()
    .into_package()
    .unwrap();
    let first_epoch = first.drain_epoch.as_ref().unwrap();
    assert_eq!(first_epoch.consumed_partition_count, 1);
    assert_eq!(first.output.profile.output_rows, 3);
    assert!(matches!(
        first_epoch.closure.evidence.cause,
        cdf_kernel::EpochClosureCause::CheckpointCadence { .. }
    ));
    assert!(first_dir.join("plan/epoch-frontier.json").is_file());
    let opened_after_first = resource.open_count.load(Ordering::SeqCst);

    let selected = BTreeSet::from([PartitionId::new("part-1").unwrap()]);
    let second_plan = plan
        .clone()
        .select_partitions(&selected)
        .unwrap()
        .rebind_package_id("pkg-engine-test-e000001")
        .unwrap();
    let blocked_dir = root.path().join("blocked");
    let blocked = block_on(super::execute_drain_epoch_with_hooks(
        &second_plan,
        &resource,
        &blocked_dir,
        &pre_finalize,
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap_err();
    assert!(blocked.message.contains("before frontier settlement"));
    assert_eq!(
        resource.open_count.load(Ordering::SeqCst),
        opened_after_first
    );
    controller
        .acknowledge_settlement(&first_epoch.closure.frontier.frontier)
        .unwrap();

    let second_dir = root.path().join("epoch-1");
    let second = block_on(super::execute_drain_epoch_with_hooks(
        &second_plan,
        &resource,
        &second_dir,
        &pre_finalize,
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap()
    .into_package()
    .unwrap();
    let second_epoch = second.drain_epoch.as_ref().unwrap();
    assert_eq!(second_epoch.consumed_partition_count, 1);
    assert!(second_epoch.closure.terminate_after_settlement);
    assert!(matches!(
        second_epoch.closure.evidence.cause,
        cdf_kernel::EpochClosureCause::DrainTermination {
            termination: DrainTermination::Records { count: 6 }
        }
    ));
    controller
        .acknowledge_settlement(&second_epoch.closure.frontier.frontier)
        .unwrap();
    assert!(controller.is_finished());
}

#[test]
fn drain_epochs_resume_one_unbounded_partition_from_each_settled_batch_frontier() {
    let mut batches = [1_i64, 2, 3]
        .into_iter()
        .map(|position| {
            let mut batch = batch_for_partition(
                &format!("batch-{position}"),
                "part-0",
                vec![i32::try_from(position).unwrap()],
                vec!["event"],
                vec![true],
            );
            batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(position),
            }));
            batch
        })
        .collect::<Vec<_>>();
    let resource = MockResource::tier_a(std::mem::take(&mut batches)).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 1 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 3 },
    };
    let source = mock_unbounded_cursor_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let root = TempDir::new().unwrap();

    let mut fresh_process_plan = plan.clone();
    fresh_process_plan
        .rebind_initial_committed_frontier(
            &resource,
            &SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(1),
            }),
        )
        .unwrap();
    assert_eq!(
        fresh_process_plan.scan.inline_partitions().unwrap()[0].start_position,
        Some(SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(1),
        }))
    );

    for epoch in 0..3_u64 {
        let package_id = format!("pkg-cursor-epoch-{epoch}");
        plan = plan.rebind_package_id(package_id).unwrap();
        let output = block_on(super::execute_drain_epoch_with_hooks(
            &plan,
            &resource,
            root.path().join(format!("epoch-{epoch}")),
            &pre_finalize,
            super::DrainEpochExecution::new(&mut controller),
            executable_mock_options(EngineExecutionConfig::default()).unwrap(),
        ))
        .unwrap()
        .into_package()
        .unwrap();
        let drain = output.drain_epoch.as_ref().unwrap();
        assert_eq!(output.output.profile.output_rows, 1);
        assert_eq!(drain.consumed_partition_count, 0);
        assert_eq!(
            drain
                .resume_partition
                .as_deref()
                .map(|resume| &resume.start_position),
            Some(&SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(i64::try_from(epoch + 1).unwrap()),
            }))
        );
        assert_eq!(
            output.execution_evidence().processed_observations().len(),
            1
        );
        assert!(output.execution_evidence().checkpoint_eligible());
        controller
            .acknowledge_settlement(&drain.closure.frontier.frontier)
            .unwrap();
        plan.advance_committed_drain_frontier(
            drain.consumed_partition_count,
            drain.resume_partition.as_deref(),
        )
        .unwrap();
    }

    assert!(controller.is_finished());
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 3);
    assert_eq!(resource.batch_poll_count.load(Ordering::SeqCst), 3);
}

#[test]
fn duration_drain_closes_while_the_next_batch_poll_is_silent() {
    let mut batch = batch_for_partition("batch-1", "part-0", vec![1], vec!["event"], vec![true]);
    batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(1),
    }));
    let resource = MockResource::tier_a(vec![batch])
        .without_control_keys()
        .with_stall_after_batches();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Elapsed {
                milliseconds: 60_000,
            },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Duration { milliseconds: 25 },
    };
    let source = mock_unbounded_cursor_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();
    let package_dir = root.path().join("duration-epoch");

    let output = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        &package_dir,
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap()
    .into_package()
    .unwrap();

    assert_eq!(output.output.profile.output_rows, 1);
    let drain = output.drain_epoch.unwrap();
    assert!(matches!(
        drain.closure.evidence.cause,
        cdf_kernel::EpochClosureCause::DrainTermination {
            termination: DrainTermination::Duration { milliseconds: 25 }
        }
    ));
    assert_eq!(
        drain
            .resume_partition
            .as_deref()
            .map(|resume| &resume.start_position),
        Some(&SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(1),
        }))
    );
}

#[test]
fn duration_drain_discards_an_empty_package_while_source_open_is_silent() {
    let resource = MockResource::tier_a(Vec::new())
        .without_control_keys()
        .with_stall_after_batches();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Elapsed {
                milliseconds: 60_000,
            },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Duration { milliseconds: 20 },
    };
    let source = mock_unbounded_cursor_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();
    let package_dir = root.path().join("empty-duration-epoch");

    let outcome = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        &package_dir,
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap();

    assert!(matches!(
        outcome,
        EngineDrainEpochOutcome::FinishedNoOp { .. }
    ));
    assert!(!package_dir.exists());
    assert!(controller.is_finished());
}

#[test]
fn immediately_exhausted_drain_is_a_no_op_without_a_package() {
    let resource = MockResource::tier_a(Vec::new()).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 1 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Duration {
            milliseconds: 60_000,
        },
    };
    let source = mock_unbounded_cursor_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();
    let package_dir = root.path().join("empty");

    let outcome = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        &package_dir,
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap();

    assert!(matches!(
        outcome,
        EngineDrainEpochOutcome::FinishedNoOp { .. }
    ));
    assert!(controller.is_finished());
    assert!(!package_dir.exists());
}

#[test]
fn drain_partition_resume_stays_local_when_resource_frontier_is_a_larger_cursor() {
    let batches = [("part-0", 100_i64), ("part-1", 1), ("part-1", 2)]
        .into_iter()
        .map(|(partition, position)| {
            let mut batch = batch_for_partition(
                &format!("{partition}-{position}"),
                partition,
                vec![i32::try_from(position).unwrap()],
                vec!["event"],
                vec![true],
            );
            batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(position),
            }));
            batch
        })
        .collect::<Vec<_>>();
    let resource = MockResource::tier_b(batches).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 2 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Disabled,
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 3 },
    };
    let source = mock_unbounded_cursor_source_plan(&resource);
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();
    let output = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        root.path().join("epoch-0"),
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap()
    .into_package()
    .unwrap();
    let drain = output.drain_epoch.unwrap();
    assert_eq!(drain.consumed_partition_count, 1);
    assert_eq!(
        drain.closure.frontier.frontier,
        SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(100),
        })
    );
    assert_eq!(
        drain
            .resume_partition
            .as_deref()
            .map(|resume| &resume.start_position),
        Some(&SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(1),
        }))
    );
    let SourcePosition::Composite(continuation) = drain
        .closure
        .frontier
        .carryover
        .as_ref()
        .expect("durable partition continuation")
    else {
        panic!("multi-partition drain continuation must be partition-keyed");
    };
    assert_eq!(
        continuation.positions.get("part-0"),
        Some(&SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(100),
        }))
    );
    assert_eq!(
        continuation.positions.get("part-1"),
        Some(&SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(1),
        }))
    );

    let mut restarted = plan.clone();
    restarted
        .rebind_initial_committed_frontier(
            &resource,
            &SourcePosition::Composite(continuation.clone()),
        )
        .unwrap();
    assert_eq!(
        restarted.scan.inline_partitions().unwrap()[1].start_position,
        continuation.positions.get("part-1").cloned()
    );
}

#[test]
fn drain_epoch_records_the_minimum_partition_watermark_not_the_latest_claim() {
    fn claim(partition: &str, value: i64) -> WatermarkClaim {
        WatermarkClaim {
            version: WATERMARK_CLAIM_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            event_time_field: "id".into(),
            domain: EventTimeDomain::SignedInteger,
            value: WatermarkValue::Signed(value),
            partition_id: PartitionId::new(partition).unwrap(),
            source_position: SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(value),
            }),
            authority: WatermarkAuthority::Source,
            observation_context: WatermarkObservationContext::SourcePoll,
        }
    }

    let mut batches = [("part-0", 100_i64), ("part-1", 5_i64)]
        .into_iter()
        .map(|(partition, value)| {
            let mut batch = batch_for_partition(
                &format!("{partition}-{value}"),
                partition,
                vec![i32::try_from(value).unwrap()],
                vec!["event"],
                vec![true],
            );
            batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(value),
            }));
            batch.header.watermarks.push(claim(partition, value));
            batch
        })
        .collect::<Vec<_>>();
    let resource = MockResource::tier_b(std::mem::take(&mut batches)).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 2 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Enabled {
                event_time_field: "id".into(),
                domain: EventTimeDomain::SignedInteger,
                authority: WatermarkAuthority::Source,
                partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumAll,
            },
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 2 },
    };
    let mut source = mock_unbounded_cursor_source_plan(&resource);
    source
        .stream_capabilities
        .as_mut()
        .unwrap()
        .watermark_behavior = cdf_kernel::OperatorWatermarkBehavior::Preserve;
    source.stream_capabilities.as_mut().unwrap().watermark =
        Some(cdf_runtime::SourceWatermarkCapability {
            event_time_field: "id".into(),
            domain: EventTimeDomain::SignedInteger,
            authority: WatermarkAuthority::Source,
        });
    source.validate().unwrap();
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();
    let output = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        root.path().join("epoch-0"),
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap()
    .into_package()
    .unwrap();

    assert_eq!(
        output
            .drain_epoch
            .unwrap()
            .closure
            .frontier
            .watermark
            .unwrap()
            .value,
        WatermarkValue::Signed(5)
    );
}

#[test]
fn late_rows_are_quarantined_or_admitted_with_identity_evidence() {
    for action in [
        LateDataAction::Quarantine,
        LateDataAction::RecaptureNextEpoch,
        LateDataAction::AdmitWithAnnotation,
    ] {
        let claim = |value: i64, offset: i64| WatermarkClaim {
            version: WATERMARK_CLAIM_VERSION,
            policy_version: STREAM_EPOCH_POLICY_VERSION,
            event_time_field: "id".into(),
            domain: EventTimeDomain::SignedInteger,
            value: WatermarkValue::Signed(value),
            partition_id: PartitionId::new("part-0").unwrap(),
            source_position: SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: CursorValue::I64(offset),
            }),
            authority: WatermarkAuthority::Source,
            observation_context: WatermarkObservationContext::SourcePoll,
        };
        let mut batches = [20_i64, 10]
            .into_iter()
            .enumerate()
            .map(|(ordinal, value)| {
                let offset = i64::try_from(ordinal + 1).unwrap();
                let mut batch = batch_for_partition(
                    &format!("batch-{value}"),
                    "part-0",
                    vec![i32::try_from(value).unwrap()],
                    vec!["event"],
                    vec![true],
                );
                batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
                    version: cdf_kernel::SOURCE_POSITION_VERSION,
                    field: "id".to_owned(),
                    value: CursorValue::I64(offset),
                }));
                batch.header.watermarks.push(claim(20, offset));
                batch
            })
            .collect::<Vec<_>>();
        let resource = MockResource::tier_a(std::mem::take(&mut batches)).without_control_keys();
        let extent = ExecutionExtent::Drain {
            version: EXECUTION_EXTENT_VERSION,
            policy: StreamEpochPolicy {
                version: STREAM_EPOCH_POLICY_VERSION,
                checkpoint_cadence: EpochClosureTrigger::Rows { count: 1 },
                package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
                watermark: WatermarkPolicy::Enabled {
                    event_time_field: "id".into(),
                    domain: EventTimeDomain::SignedInteger,
                    authority: WatermarkAuthority::Source,
                    partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumAll,
                },
                late_data: action,
                safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
            },
            termination: DrainTermination::Records { count: 3 },
        };
        let mut source = mock_unbounded_cursor_source_plan(&resource);
        source
            .stream_capabilities
            .as_mut()
            .unwrap()
            .watermark_behavior = cdf_kernel::OperatorWatermarkBehavior::Preserve;
        source.stream_capabilities.as_mut().unwrap().watermark =
            Some(cdf_runtime::SourceWatermarkCapability {
                event_time_field: "id".into(),
                domain: EventTimeDomain::SignedInteger,
                authority: WatermarkAuthority::Source,
            });
        source.validate().unwrap();
        resource.bind_compiled_source(&source);
        let mut plan = Planner::new()
            .plan_tier_a(
                &resource,
                plan_input(Vec::new(), None, None, extent.clone()),
            )
            .unwrap()
            .bind_compiled_source(&source)
            .unwrap()
            .bind_operator_graph(
                &source,
                &cdf_runtime::DestinationRuntimeCapabilities::default(),
            )
            .unwrap();
        let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
        let root = TempDir::new().unwrap();

        let first = block_on(super::execute_drain_epoch_with_hooks(
            &plan,
            &resource,
            root.path().join(format!("{action:?}-epoch-0")),
            &|_, _| Ok(()),
            super::DrainEpochExecution::new(&mut controller),
            executable_mock_options(EngineExecutionConfig::default()).unwrap(),
        ))
        .unwrap()
        .into_package()
        .unwrap();
        let first_drain = first.drain_epoch.as_ref().unwrap();
        assert_eq!(first.output.profile.output_rows, 1);
        assert_eq!(
            first_drain
                .closure
                .frontier
                .watermark
                .as_ref()
                .unwrap()
                .value,
            WatermarkValue::Signed(20)
        );
        controller
            .acknowledge_settlement(&first_drain.closure.frontier.frontier)
            .unwrap();
        plan.advance_committed_drain_frontier(
            first_drain.consumed_partition_count,
            first_drain.resume_partition.as_deref(),
        )
        .unwrap();
        plan = plan
            .rebind_package_id(format!("pkg-late-{action:?}"))
            .unwrap();
        let second_dir = root.path().join(format!("{action:?}-epoch-1"));
        let second = block_on(super::execute_drain_epoch_with_hooks(
            &plan,
            &resource,
            &second_dir,
            &|_, _| Ok(()),
            super::DrainEpochExecution::new(&mut controller),
            executable_mock_options(EngineExecutionConfig::default()).unwrap(),
        ))
        .unwrap()
        .into_package()
        .unwrap();
        let evidence: cdf_package_contract::LateDataEvidence = serde_json::from_slice(
            &std::fs::read(second_dir.join(cdf_package_contract::LATE_DATA_EVIDENCE_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(evidence.batches.len(), 1);
        let evidence_batch = &evidence.batches[0];
        assert_eq!(evidence_batch.rows.len(), 1);
        assert_eq!(
            evidence_batch.rows[0].event_time,
            WatermarkValue::Signed(10)
        );
        assert_eq!(
            evidence_batch.effective_watermark.value,
            WatermarkValue::Signed(20)
        );
        let package = cdf_package::PackageReader::open(&second_dir).unwrap();
        let verified = package.verify_for_consumption().unwrap();
        let (joined_evidence, _) = package
            .late_data_evidence_verified(&verified)
            .unwrap()
            .expect("late-data evidence");
        assert_eq!(joined_evidence, evidence);
        let quarantine = collect_quarantine_records(&package);
        match action {
            LateDataAction::Quarantine => {
                assert_eq!(second.output.profile.output_rows, 0);
                assert_eq!(quarantine.len(), 1);
                assert_eq!(quarantine[0].rule_id, "cdf.late_data");
                let catalog: LateDataPayloadCatalog = serde_json::from_slice(
                    &std::fs::read(second_dir.join(LATE_DATA_PAYLOAD_CATALOG_FILE)).unwrap(),
                )
                .unwrap();
                catalog.validate().unwrap();
                assert_eq!(catalog.artifacts.len(), 1);
                assert_eq!(catalog.artifacts[0].action, action);
                assert_eq!(catalog.artifacts[0].row_count, 1);
                assert!(matches!(
                    evidence_batch.rows[0].payload,
                    LateDataPayloadLocation::ArtifactRow {
                        artifact_ordinal: 0,
                        row_ordinal: 0,
                    }
                ));
                let file =
                    std::fs::File::open(second_dir.join(&catalog.artifacts[0].path)).unwrap();
                let mut payload = arrow_ipc::reader::FileReader::try_new(file, None).unwrap();
                let batch = payload.next().unwrap().unwrap();
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(
                    batch
                        .column_by_name("id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(0),
                    10
                );
                assert!(payload.next().is_none());
                let summary: cdf_contract::VerdictSummary = serde_json::from_slice(
                    &std::fs::read(second_dir.join("stats/verdict-summary.json")).unwrap(),
                )
                .unwrap();
                assert_eq!(summary.input_rows, 1);
                assert_eq!(summary.accepted_rows, 0);
                assert_eq!(summary.quarantined_rows, 1);
            }
            LateDataAction::AdmitWithAnnotation => {
                assert_eq!(second.output.profile.output_rows, 1);
                assert!(quarantine.is_empty());
                assert!(matches!(
                    evidence_batch.rows[0].payload,
                    LateDataPayloadLocation::AdmittedOutput {
                        package_row_ordinal: 0
                    }
                ));
                assert!(!second_dir.join(LATE_DATA_PAYLOAD_CATALOG_FILE).exists());
            }
            LateDataAction::RecaptureNextEpoch => {
                assert_eq!(second.output.profile.output_rows, 0);
                assert!(quarantine.is_empty());
                let catalog: LateDataPayloadCatalog = serde_json::from_slice(
                    &std::fs::read(second_dir.join(LATE_DATA_PAYLOAD_CATALOG_FILE)).unwrap(),
                )
                .unwrap();
                catalog.validate().unwrap();
                assert_eq!(catalog.artifacts.len(), 1);
                assert_eq!(catalog.artifacts[0].action, action);
                assert!(matches!(
                    evidence_batch.rows[0].payload,
                    LateDataPayloadLocation::ArtifactRow {
                        artifact_ordinal: 0,
                        row_ordinal: 0,
                    }
                ));
                let second_drain = second.drain_epoch.as_ref().unwrap();
                assert_eq!(second_drain.late_data_carryover.len(), 1);
                controller
                    .acknowledge_settlement(&second_drain.closure.frontier.frontier)
                    .unwrap();
                plan.advance_committed_drain_frontier(
                    second_drain.consumed_partition_count,
                    second_drain.resume_partition.as_deref(),
                )
                .unwrap();
                plan = plan
                    .rebind_package_id("pkg-late-recapture-epoch-2")
                    .unwrap();
                let reader = cdf_package::PackageReader::open(&second_dir).unwrap();
                let verified = Arc::new(reader.verify_for_consumption().unwrap());
                let carryover = second_drain
                    .late_data_carryover
                    .iter()
                    .map(|reference| {
                        let object = reader
                            .verified_identity_object(
                                Arc::clone(&verified),
                                &reference.relative_path,
                            )
                            .unwrap();
                        super::LateDataCarryoverInput::new(reference.clone(), object).unwrap()
                    })
                    .collect();
                let third_dir = root.path().join("RecaptureNextEpoch-epoch-2");
                let third = block_on(super::execute_drain_epoch_with_hooks(
                    &plan,
                    &resource,
                    &third_dir,
                    &|_, _| Ok(()),
                    super::DrainEpochExecution::new(&mut controller)
                        .with_late_data_carryover(carryover),
                    executable_mock_options(EngineExecutionConfig::default()).unwrap(),
                ))
                .unwrap()
                .into_package()
                .unwrap();
                let third_drain = third.drain_epoch.as_ref().unwrap();
                assert_eq!(third.output.profile.output_rows, 1);
                assert_eq!(third_drain.consumed_late_data_carryover.len(), 1);
                assert!(third_drain.late_data_carryover.is_empty());
                assert!(third_drain.closure.terminate_after_settlement);
                assert!(
                    third_dir
                        .join("plan/late-data-carryover-input.json")
                        .is_file()
                );
            }
        }
    }
}

#[test]
fn drain_rejects_an_earlier_regressing_claim_even_when_the_batch_tail_recovers() {
    let claim = |value: i64| WatermarkClaim {
        version: WATERMARK_CLAIM_VERSION,
        policy_version: STREAM_EPOCH_POLICY_VERSION,
        event_time_field: "id".into(),
        domain: EventTimeDomain::SignedInteger,
        value: WatermarkValue::Signed(value),
        partition_id: PartitionId::new("part-0").unwrap(),
        source_position: SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(1),
        }),
        authority: WatermarkAuthority::Source,
        observation_context: WatermarkObservationContext::SourcePoll,
    };
    let mut batch = batch_for_partition("batch-1", "part-0", vec![100], vec!["event"], vec![true]);
    batch.header.source_position = Some(SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(1),
    }));
    batch.header.watermarks = vec![claim(100), claim(90), claim(110)];
    let resource = MockResource::tier_a(vec![batch]).without_control_keys();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 1 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Enabled {
                event_time_field: "id".into(),
                domain: EventTimeDomain::SignedInteger,
                authority: WatermarkAuthority::Source,
                partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumAll,
            },
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 1 },
    };
    let mut source = mock_unbounded_cursor_source_plan(&resource);
    source
        .stream_capabilities
        .as_mut()
        .unwrap()
        .watermark_behavior = cdf_kernel::OperatorWatermarkBehavior::Preserve;
    source.stream_capabilities.as_mut().unwrap().watermark =
        Some(cdf_runtime::SourceWatermarkCapability {
            event_time_field: "id".into(),
            domain: EventTimeDomain::SignedInteger,
            authority: WatermarkAuthority::Source,
        });
    source.validate().unwrap();
    resource.bind_compiled_source(&source);
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let root = TempDir::new().unwrap();

    let error = block_on(super::execute_drain_epoch_with_hooks(
        &plan,
        &resource,
        root.path().join("regressing-watermark"),
        &|_, _| Ok(()),
        super::DrainEpochExecution::new(&mut controller),
        executable_mock_options(EngineExecutionConfig::default()).unwrap(),
    ))
    .unwrap_err();
    assert!(error.message.contains("watermark regressed"));
}

type FixedDrainEpochEvidence = (
    String,
    Vec<cdf_package_contract::SegmentEntry>,
    cdf_kernel::EpochClosureEvidence,
    bool,
);

fn run_fixed_drain_epochs_with_jobs(jobs: u16) -> (Vec<FixedDrainEpochEvidence>, u64) {
    let mut batches = sample_batches();
    for (ordinal, batch) in batches.iter_mut().enumerate() {
        batch.header.partition_id = PartitionId::new(format!("part-{}", ordinal + 1)).unwrap();
    }
    batches.insert(
        0,
        batch_for_partition("batch-idle", "part-0", Vec::new(), Vec::new(), Vec::new()),
    );
    for (ordinal, batch) in batches.iter_mut().enumerate() {
        let source_position = SourcePosition::FileManifest(FileManifest {
            version: 1,
            files: vec![FilePosition {
                path: format!("input-{ordinal}.arrow"),
                size_bytes: batch.header.byte_count,
                source_generation: Some(format!("generation-{ordinal}")),
                etag: None,
                object_version: None,
                sha256: None,
            }],
        });
        batch.header.source_position = Some(source_position.clone());
        if batch.header.partition_id.as_str() == "part-0" {
            batch.header.partition_idleness = Some(cdf_kernel::PartitionIdlenessClaim {
                version: cdf_kernel::PARTITION_IDLENESS_CLAIM_VERSION,
                partition_id: batch.header.partition_id.clone(),
                source_position,
                capability_id: "source-idleness-v1".into(),
                idle_for_milliseconds: 10,
            });
        } else {
            batch.header.watermarks.push(WatermarkClaim {
                version: WATERMARK_CLAIM_VERSION,
                policy_version: STREAM_EPOCH_POLICY_VERSION,
                event_time_field: "id".into(),
                domain: EventTimeDomain::SignedInteger,
                value: WatermarkValue::Signed(i64::try_from((ordinal + 1) * 10).unwrap()),
                partition_id: batch.header.partition_id.clone(),
                source_position,
                authority: WatermarkAuthority::Source,
                observation_context: WatermarkObservationContext::SourcePoll,
            });
        }
    }
    let resource = MockResource::tier_b(batches)
        .with_partition_count(3)
        .without_control_keys()
        .with_dynamic_attestation();
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: StreamEpochPolicy {
            version: STREAM_EPOCH_POLICY_VERSION,
            checkpoint_cadence: EpochClosureTrigger::Rows { count: 3 },
            package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
            watermark: WatermarkPolicy::Enabled {
                event_time_field: "id".into(),
                domain: EventTimeDomain::SignedInteger,
                authority: WatermarkAuthority::Source,
                partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumEligible {
                    idle_after_milliseconds: 10,
                    capability_id: "source-idleness-v1".into(),
                },
            },
            late_data: LateDataAction::Quarantine,
            safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
        },
        termination: DrainTermination::Records { count: 6 },
    };
    let mut source = mock_unbounded_source_plan(&resource);
    source.execution_capabilities.speculative_safe = true;
    source.execution_capabilities.attestation =
        cdf_runtime::SourceAttestationStrength::ImmutableContent;
    source
        .stream_capabilities
        .as_mut()
        .unwrap()
        .watermark_behavior = cdf_kernel::OperatorWatermarkBehavior::Preserve;
    source.stream_capabilities.as_mut().unwrap().watermark =
        Some(cdf_runtime::SourceWatermarkCapability {
            event_time_field: "id".into(),
            domain: EventTimeDomain::SignedInteger,
            authority: WatermarkAuthority::Source,
        });
    source
        .stream_capabilities
        .as_mut()
        .unwrap()
        .idleness_capabilities = vec!["source-idleness-v1".to_owned()];
    source.validate().unwrap();
    resource.bind_compiled_source(&source);
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, extent.clone()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(512 * 1024 * 1024).unwrap();
    let services = services.with_run_job_ceiling(jobs).unwrap();
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        usize::try_from(plan.scan.partition_count().unwrap()).unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(jobs),
    )
    .unwrap();
    let mut controller = cdf_runtime::DrainEpochController::new(&extent).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let root = TempDir::new().unwrap();
    let mut outputs = Vec::new();
    let mut maximum_active = 0_u64;
    let mut observed_global_watermark = false;
    let mut observed_source_idleness = false;
    let mut epoch = 0_u64;
    while !controller.is_finished() {
        plan = plan
            .rebind_package_id(format!("pkg-drain-jobs-{epoch}"))
            .unwrap();
        let output = block_on(super::execute_drain_epoch_with_hooks(
            &plan,
            &resource,
            root.path().join(format!("epoch-{epoch}")),
            &pre_finalize,
            super::DrainEpochExecution::new(&mut controller),
            EngineExecutionConfig::default()
                .with_execution_services(services.clone())
                .with_scheduler_resolution(scheduler.narrow_to_partition_count(
                    usize::try_from(plan.scan.partition_count().unwrap()).unwrap(),
                ))
                .new_invocation(),
        ))
        .unwrap()
        .into_package()
        .unwrap();
        let drain = output.drain_epoch.as_ref().unwrap();
        observed_global_watermark |= drain.closure.frontier.watermark.is_some();
        observed_source_idleness |= drain
            .partition_watermarks
            .iter()
            .any(|state| state.partition_id.as_str() == "part-0" && state.idleness.is_some());
        maximum_active = maximum_active.max(output.source_frontier.maximum_active);
        outputs.push((
            output.output.manifest.package_hash.clone(),
            output.output.identity_segments().to_vec(),
            drain.closure.evidence.clone(),
            drain.closure.terminate_after_settlement,
        ));
        controller
            .acknowledge_settlement(&drain.closure.frontier.frontier)
            .unwrap();
        plan.advance_committed_drain_frontier(
            drain.consumed_partition_count,
            drain.resume_partition.as_deref(),
        )
        .unwrap();
        epoch += 1;
    }
    assert!(observed_global_watermark);
    assert!(observed_source_idleness);
    assert_eq!(services.memory().snapshot().current_bytes, 0);
    (outputs, maximum_active)
}

#[test]
fn fixed_drain_epoch_packages_are_jobs_invariant() {
    let jobs_one = run_fixed_drain_epochs_with_jobs(1);
    let jobs_many = run_fixed_drain_epochs_with_jobs(8);
    assert_eq!(jobs_one.0, jobs_many.0);
    assert_eq!(jobs_one.0.len(), 2);
    assert_eq!(jobs_one.1, 1);
    assert!(jobs_many.1 > jobs_one.1);
}

#[test]
fn operator_graph_compiles_from_capabilities_without_driver_name_dispatch() {
    let resource = MockResource::tier_b(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    for operator in &mut plan.operator_chain {
        if let OperatorNode::PackageSink { segmentation, .. } = operator {
            segmentation.target_rows = 1;
            segmentation.maximum_rows = 1;
            segmentation.microbatch_minimum_rows = 1;
            segmentation.microbatch_maximum_rows = 1;
        }
    }
    let source = mock_compiled_source_plan(&resource, None);
    resource.bind_compiled_source(&source);

    let graph = compile_operator_graph(
        &plan,
        &source,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
    )
    .unwrap();

    graph.validate().unwrap();
    graph
        .validate_destination_join(&cdf_runtime::DestinationRuntimeCapabilities::default())
        .unwrap();
    let stale_staged = cdf_runtime::DestinationRuntimeCapabilities {
        ingress_mode: cdf_runtime::DestinationIngressMode::StagedDurableSegments,
        staged_ingress: Some(cdf_runtime::StagedIngressCapabilities {
            recovery: cdf_runtime::StagingRecoveryMode::RollbackRedrive,
            visibility: cdf_runtime::StagingVisibility::IsolatedUntilFinalBinding,
            abort_idempotent: true,
            lifecycle_cleanup: true,
            final_binding_requires_exclusive_writer: false,
        }),
        max_in_flight_bytes: Some(64 * 1024 * 1024),
        ..Default::default()
    };
    assert!(graph.validate_destination_join(&stale_staged).is_err());
    assert_eq!(graph.nodes[0].implementation_version, "mock-v1");
    assert_eq!(graph.execution_extent, plan.execution_extent);
    assert!(
        graph
            .nodes
            .iter()
            .all(|node| node.execution_extent_hash.is_none())
    );
    assert!(
        graph
            .nodes
            .iter()
            .all(|node| node.node_id != "external_mock")
    );
    assert!(graph.edges.iter().any(|edge| {
        edge.transfer == cdf_runtime::GraphEdgeTransfer::Fused
            && edge.producer == "reconcile"
            && edge.consumer == "transform"
    }));
    assert!(graph.edges.iter().any(|edge| {
        edge.transfer == cdf_runtime::GraphEdgeTransfer::Durable
            && edge.producer == "segment_persist"
    }));
    plan = plan.bind_compiled_source(&source).unwrap();
    plan.operator_graph = Some(graph.clone());
    let temp = TempDir::new().unwrap();
    let serial = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let packaged: cdf_runtime::CompiledOperatorGraph = serde_json::from_slice(
        &std::fs::read(temp.path().join("plan/operator-graph.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(packaged, graph);

    let parallel_temp = TempDir::new().unwrap();
    // The encoder conservatively charges 3x the 64 MiB segment ceiling. A 512 MiB logical
    // coordinator budget admits two workers, so jobs=4 can complete segments out of order while
    // the canonical registration frontier remains deterministic.
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new())
            .unwrap(),
    );
    let host = Arc::new(
        StandaloneExecutionHost::new(
            cdf_runtime::ExecutionHostCapabilities {
                logical_cpu_slots: 4,
                io_workers: 2,
                blocking_lanes: Vec::new(),
            },
            memory,
        )
        .unwrap(),
    );
    let services = cdf_runtime::ExecutionServices::new(host).unwrap();
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        usize::try_from(plan.scan.partition_count().unwrap()).unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(4),
    )
    .unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let parallel = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        parallel_temp.path(),
        &pre_finalize,
        EngineExecutionConfig::default()
            .with_execution_services(services.clone())
            .with_scheduler_resolution(scheduler),
    ))
    .unwrap();
    assert!(serial.identity_segments().len() > 1);
    assert_eq!(
        parallel.output.identity_segments().len(),
        serial.identity_segments().len()
    );
    assert_eq!(parallel.output.manifest.identity, serial.manifest.identity);
    assert_eq!(parallel.output.lineage, serial.lineage);
    assert_eq!(
        parallel.output.profile.statistics,
        serial.profile.statistics
    );
    assert_eq!(services.memory().snapshot().current_bytes, 0);

    let mut stale_scheduler = cdf_runtime::resolve_runtime_scheduler(
        usize::try_from(plan.scan.partition_count().unwrap()).unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(4),
    )
    .unwrap();
    stale_scheduler.source_bounded = !source.execution_capabilities.bounded;
    let stale_temp = TempDir::new().unwrap();
    let error = block_on(
        super::execute_to_package_with_segment_positions_and_pre_finalize(
            &plan,
            &resource,
            stale_temp.path(),
            &pre_finalize,
            EngineExecutionConfig::default()
                .with_execution_services(services.clone())
                .with_scheduler_resolution(stale_scheduler)
                .new_invocation(),
        ),
    )
    .unwrap_err();
    assert!(error.message.contains("scheduler source authority"));

    let destination = cdf_runtime::DestinationRuntimeCapabilities {
        blocking_lanes: vec![
            cdf_runtime::BlockingLaneSpec {
                lane_id: "mock.maintenance".to_owned(),
                binding: cdf_runtime::BlockingLaneBinding::Static,
                maximum_concurrency: 1,
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
                affinity: cdf_runtime::LaneAffinity::Shared,
                interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
            },
            cdf_runtime::BlockingLaneSpec {
                lane_id: "mock.commit".to_owned(),
                binding: cdf_runtime::BlockingLaneBinding::Static,
                maximum_concurrency: 1,
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
                affinity: cdf_runtime::LaneAffinity::Pinned,
                interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
            },
        ],
        final_binding_lane: Some("mock.commit".to_owned()),
        ..cdf_runtime::DestinationRuntimeCapabilities::default()
    };
    let graph = compile_operator_graph(&plan, &source, &destination).unwrap();
    graph.validate_destination_join(&destination).unwrap();
    let binding = graph
        .nodes
        .iter()
        .find(|node| node.node_id == "destination_bind")
        .unwrap();
    assert_eq!(binding.blocking_lane.as_deref(), Some("mock.commit"));
}

#[test]
fn watermark_projection_fails_at_graph_compilation_before_source_contact() {
    let resource = MockResource::tier_b(sample_batches());
    let mut policy = sample_stream_epoch_policy();
    policy.watermark = WatermarkPolicy::Enabled {
        event_time_field: "id".into(),
        domain: cdf_kernel::EventTimeDomain::SignedInteger,
        authority: cdf_kernel::WatermarkAuthority::Source,
        partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumAll,
    };
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy,
        termination: DrainTermination::Records { count: 100 },
    };
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), Some(vec!["name".to_owned()]), None, extent),
        )
        .unwrap();
    let mut source = mock_compiled_source_plan(&resource, None);
    source.execution_capabilities.bounded = false;
    source.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
        quiescence: false,
        watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Preserve,
        watermark: Some(cdf_runtime::SourceWatermarkCapability {
            event_time_field: "id".into(),
            domain: cdf_kernel::EventTimeDomain::SignedInteger,
            authority: WatermarkAuthority::Source,
        }),
        safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
        source_frontiers: vec![cdf_runtime::SourceFrontierCapability::Cursor {
            fields: vec!["id".to_owned()],
        }],
        idleness_capabilities: Vec::new(),
    });
    source.validate().unwrap();
    let plan = plan.bind_compiled_source(&source).unwrap();

    let error = plan
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap_err();

    assert!(error.message.contains("event-time field `id` is removed"));
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn watermark_compilation_proves_field_domain_authority_and_column_preservation() {
    fn extent(field: &str, domain: cdf_kernel::EventTimeDomain) -> ExecutionExtent {
        let mut policy = sample_stream_epoch_policy();
        policy.watermark = WatermarkPolicy::Enabled {
            event_time_field: field.into(),
            domain,
            authority: WatermarkAuthority::Source,
            partition_aggregation: cdf_kernel::PartitionWatermarkAggregation::MinimumAll,
        };
        ExecutionExtent::Drain {
            version: EXECUTION_EXTENT_VERSION,
            policy,
            termination: DrainTermination::Records { count: 100 },
        }
    }

    fn unbounded_source(resource: &MockResource) -> cdf_runtime::CompiledSourcePlan {
        let mut source = mock_compiled_source_plan(resource, None);
        source.execution_capabilities.bounded = false;
        source.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
            quiescence: false,
            watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Preserve,
            watermark: Some(cdf_runtime::SourceWatermarkCapability {
                event_time_field: "id".into(),
                domain: cdf_kernel::EventTimeDomain::SignedInteger,
                authority: WatermarkAuthority::Source,
            }),
            safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
            source_frontiers: vec![cdf_runtime::SourceFrontierCapability::Cursor {
                fields: vec!["id".to_owned()],
            }],
            idleness_capabilities: Vec::new(),
        });
        source.validate().unwrap();
        source
    }

    let destination = cdf_runtime::DestinationRuntimeCapabilities::default();

    let resource = MockResource::tier_b(sample_batches());
    let source = unbounded_source(&resource);
    let error = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(
                Vec::new(),
                None,
                None,
                extent("missing", cdf_kernel::EventTimeDomain::SignedInteger),
            ),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap_err();
    assert!(
        error
            .message
            .contains("watermark field/domain/authority does not match")
    );

    let error = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(
                Vec::new(),
                None,
                None,
                extent("id", cdf_kernel::EventTimeDomain::Date32),
            ),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap_err();
    assert!(
        error
            .message
            .contains("watermark field/domain/authority does not match"),
        "{}",
        error.message
    );

    let mut redacted = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(
                Vec::new(),
                None,
                None,
                extent("id", cdf_kernel::EventTimeDomain::SignedInteger),
            ),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap();
    redacted
        .validation_program
        .column_programs
        .iter_mut()
        .find(|column| column.output_name == "id")
        .unwrap()
        .redaction = RedactionDecision::Omit;
    let error = redacted
        .bind_operator_graph(&source, &destination)
        .unwrap_err();
    assert!(error.message.contains("is redacted"));

    let mut derived_extent = extent("id", cdf_kernel::EventTimeDomain::SignedInteger);
    let ExecutionExtent::Drain { policy, .. } = &mut derived_extent else {
        unreachable!()
    };
    let WatermarkPolicy::Enabled { authority, .. } = &mut policy.watermark else {
        unreachable!()
    };
    *authority = WatermarkAuthority::Derived {
        mapping_id: "missing-mapping".into(),
    };
    let derived = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, derived_extent),
        )
        .unwrap();
    let error = derived.bind_compiled_source(&source).unwrap_err();
    assert!(
        error
            .message
            .contains("watermark field/domain/authority does not match")
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn operator_graph_binds_the_plan_source_and_drain_policy_exactly() {
    let resource = MockResource::tier_b(sample_batches());
    let source = mock_compiled_source_plan(&resource, None);
    let plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap();
    let mut other_source = source.clone();
    other_source.physical_plan = serde_json::json!({"partitioning": "other"});
    other_source.physical_plan_hash = cdf_kernel::PhysicalSourcePlanHash::new(
        cdf_runtime::artifact_hash(&other_source.physical_plan).unwrap(),
    )
    .unwrap();
    other_source.validate().unwrap();
    let error = plan
        .bind_operator_graph(
            &other_source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap_err();
    assert!(
        error
            .message
            .contains("differs from the source already bound")
    );

    let mut drain_source = source.clone();
    drain_source.execution_capabilities.bounded = false;
    drain_source.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
        quiescence: false,
        watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Drop,
        watermark: None,
        safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
        source_frontiers: vec![cdf_runtime::SourceFrontierCapability::Cursor {
            fields: vec!["id".to_owned()],
        }],
        idleness_capabilities: Vec::new(),
    });
    drain_source.validate().unwrap();
    resource.bind_compiled_source(&drain_source);
    let drain = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(
                Vec::new(),
                None,
                None,
                ExecutionExtent::Drain {
                    version: EXECUTION_EXTENT_VERSION,
                    policy: sample_stream_epoch_policy(),
                    termination: DrainTermination::Records { count: 100 },
                },
            ),
        )
        .unwrap()
        .bind_compiled_source(&drain_source)
        .unwrap();
    let error = drain
        .validate_compiled_source_resource(&resource)
        .unwrap_err();
    assert!(error.message.contains("requires a compiled operator graph"));
}

#[test]
fn non_pausable_unbounded_execution_requires_runtime_replay_retention() {
    let resource = MockResource::tier_a(sample_batches()).without_control_keys();
    let mut source = mock_unbounded_cursor_source_plan(&resource);
    source.resource_capabilities.backpressure = BackpressureSupport::SpillRequired;
    source.execution_capabilities.pausable = false;
    source.execution_capabilities.spillable = true;
    source.validate().unwrap();
    resource.bind_compiled_source(&source);
    let extent = ExecutionExtent::Drain {
        version: EXECUTION_EXTENT_VERSION,
        policy: sample_stream_epoch_policy(),
        termination: DrainTermination::Records { count: 3 },
    };
    let plan = Planner::new()
        .plan_tier_a(&resource, plan_input(Vec::new(), None, None, extent))
        .unwrap()
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();

    let error = plan
        .validate_compiled_source_resource(&resource)
        .unwrap_err();
    assert!(error.message.contains("replay-retention authority"));
    assert!(error.message.contains("byte, age, and unit-count knobs"));
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn engine_parallel_frontier_polls_later_partition_while_head_is_stalled() {
    let (head_sender, head_receiver) = tokio::sync::oneshot::channel::<()>();
    let later_polls = Arc::new(AtomicUsize::new(0));
    let resource = StalledHeadResource {
        inner: MockResource::tier_b(sample_batches()),
        head_gate: Arc::new(Mutex::new(Some(head_receiver))),
        later_polls: Arc::clone(&later_polls),
    };
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource.inner, None);
    resource.inner.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    plan.operator_graph = Some(
        compile_operator_graph(
            &plan,
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap(),
    );

    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new())
            .unwrap(),
    );
    let host = Arc::new(
        StandaloneExecutionHost::new(
            cdf_runtime::ExecutionHostCapabilities {
                logical_cpu_slots: 2,
                io_workers: 2,
                blocking_lanes: Vec::new(),
            },
            memory,
        )
        .unwrap(),
    );
    let services = cdf_runtime::ExecutionServices::new(host).unwrap();
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        usize::try_from(plan.scan.partition_count().unwrap()).unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(2),
    )
    .unwrap();
    assert_eq!(scheduler.effective_jobs.jobs, 2);

    let parallel_plan = plan.clone();
    let parallel_resource = resource.clone();
    let parallel_services = services.clone();
    let run = std::thread::spawn(move || {
        let package = TempDir::new().unwrap();
        let pre_finalize =
            |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
        block_on(
            super::execute_to_package_with_segment_positions_and_pre_finalize(
                &parallel_plan,
                &parallel_resource,
                package.path(),
                &pre_finalize,
                EngineExecutionConfig::default()
                    .with_execution_services(parallel_services)
                    .with_scheduler_resolution(scheduler)
                    .new_invocation(),
            ),
        )
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while later_polls.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    let later_polled_while_head_stalled = later_polls.load(Ordering::SeqCst) == 1;
    head_sender.send(()).unwrap();
    let parallel = run.join().unwrap().unwrap();

    assert!(
        later_polled_while_head_stalled,
        "jobs=2 did not poll the later partition before the canonical head was released"
    );
    assert_eq!(later_polls.load(Ordering::SeqCst), 1);
    assert_eq!(resource.inner.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(services.memory().snapshot().current_bytes, 0);

    let serial_resource = MockResource::tier_b(sample_batches());
    serial_resource.bind_compiled_source(&source);
    let serial_package = TempDir::new().unwrap();
    let serial = block_on(execute_to_package(
        &plan,
        &serial_resource,
        serial_package.path(),
    ))
    .unwrap();
    assert_eq!(parallel.output.manifest.identity, serial.manifest.identity);
    assert_eq!(parallel.output.lineage, serial.lineage);
    assert_eq!(
        parallel.output.profile.statistics,
        serial.profile.statistics
    );
}

#[test]
fn engine_keeps_non_speculative_source_frontier_serial() {
    let resource = MockResource::tier_b(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan_with_speculation(&resource, None, false);
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    plan.operator_graph = Some(
        compile_operator_graph(
            &plan,
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap(),
    );

    let (_, services) = StandaloneExecutionHost::default_services(512 * 1024 * 1024).unwrap();
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        usize::try_from(plan.scan.partition_count().unwrap()).unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(4),
    )
    .unwrap();
    assert!(scheduler.effective_jobs.jobs > 1);
    let options = EngineExecutionConfig::default()
        .with_execution_services(services)
        .with_scheduler_resolution(scheduler)
        .new_invocation();

    assert_eq!(crate::execution::partition_open_jobs(&plan, &options), 1);
}

fn skewed_resource(
    seed: usize,
    terminal_failure_partitions: impl IntoIterator<Item = usize>,
) -> SkewedMockResource {
    let partition_count = 8;
    let batches = (0..partition_count)
        .map(|ordinal| {
            let row_count = 1 + ((seed * 17 + ordinal * 5) % 7);
            let first_id = i32::try_from(seed * 10_000 + ordinal * 100).unwrap();
            let mut batch = batch_for_partition(
                &format!("skew-{seed}-{ordinal}"),
                &format!("part-{ordinal}"),
                (0..row_count)
                    .map(|row| first_id + i32::try_from(row).unwrap())
                    .collect(),
                vec!["skew-value"; row_count],
                (0..row_count)
                    .map(|row| !(seed + ordinal + row).is_multiple_of(3))
                    .collect(),
            );
            batch.header.source_position = Some(terminal_file_position());
            batch
        })
        .collect();
    let inner = MockResource::tier_b(batches).with_partition_count(partition_count);
    SkewedMockResource {
        inner,
        poll_delays: Arc::new(
            (0..partition_count)
                .map(|ordinal| (seed * 29 + ordinal * 11) % 9)
                .collect(),
        ),
        terminal_failure_partitions: terminal_failure_partitions.into_iter().collect(),
    }
}

fn skewed_plan(
    resource: &SkewedMockResource,
    seed: usize,
) -> (EnginePlan, cdf_runtime::CompiledSourcePlan) {
    let limits = [None, Some(0), Some(1), Some(3), Some(7), Some(19)];
    let filters = if seed.is_multiple_of(2) {
        vec!["active = true"]
    } else {
        Vec::new()
    };
    let projection = seed
        .is_multiple_of(3)
        .then(|| vec!["id".to_owned(), "name".to_owned()]);
    let mut plan = Planner::new()
        .plan_tier_b(
            resource,
            plan_input(
                filters,
                projection,
                limits[seed % limits.len()],
                ExecutionExtent::bounded(),
            ),
        )
        .unwrap();
    for operator in &mut plan.operator_chain {
        if let OperatorNode::PackageSink { segmentation, .. } = operator {
            segmentation.target_rows = 1;
            segmentation.maximum_rows = 1;
            segmentation.microbatch_minimum_rows = 1;
            segmentation.microbatch_maximum_rows = 1;
        }
    }
    let source = mock_compiled_source_plan(&resource.inner, None);
    resource.inner.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    plan = plan
        .bind_operator_graph(
            &source,
            &cdf_runtime::DestinationRuntimeCapabilities::default(),
        )
        .unwrap();
    (plan, source)
}

#[derive(Debug)]
struct RetainedEngineRun {
    run: EngineRunOutputWithSegmentPositions,
    _package: TempDir,
}

impl std::ops::Deref for RetainedEngineRun {
    type Target = EngineRunOutputWithSegmentPositions;

    fn deref(&self) -> &Self::Target {
        &self.run
    }
}

fn run_skewed_jobs(
    resource: &SkewedMockResource,
    plan: &EnginePlan,
    source: &cdf_runtime::CompiledSourcePlan,
    jobs: u16,
) -> Result<RetainedEngineRun> {
    let (_, services) = StandaloneExecutionHost::default_services(4 * 1024 * 1024 * 1024)?;
    let services = services.with_run_job_ceiling(jobs)?;
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        usize::try_from(plan.scan.partition_count().unwrap()).unwrap(),
        &source.execution_capabilities,
        &cdf_runtime::DestinationRuntimeCapabilities::default(),
        &services,
        Some(jobs),
    )?;
    services.tighten_run_job_ceiling(scheduler.effective_jobs.jobs)?;
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let run = block_on(
        super::execute_to_package_with_segment_positions_and_pre_finalize(
            plan,
            resource,
            package.path(),
            &pre_finalize,
            EngineExecutionConfig::default()
                .with_execution_services(services)
                .with_scheduler_resolution(scheduler)
                .new_invocation(),
        ),
    )?;
    Ok(RetainedEngineRun {
        run,
        _package: package,
    })
}

#[test]
fn randomized_skew_limit_projection_and_filter_matrix_is_jobs_invariant() {
    for seed in 0..12 {
        let resource = skewed_resource(seed, []);
        let (plan, source) = skewed_plan(&resource, seed);
        let runs = [1, 2, 4, 8]
            .into_iter()
            .map(|jobs| run_skewed_jobs(&resource, &plan, &source, jobs).unwrap())
            .collect::<Vec<_>>();
        for run in &runs[1..] {
            assert_eq!(
                run.output.manifest.package_hash,
                runs[0].output.manifest.package_hash
            );
            assert_eq!(
                run.output.identity_segments(),
                runs[0].output.identity_segments()
            );
            assert_eq!(run.output.lineage, runs[0].output.lineage);
            assert_eq!(run.output.profile, runs[0].output.profile);
            assert_eq!(run.segment_positions, runs[0].segment_positions);
            assert_eq!(
                run.output.terminal_schema_quarantines,
                runs[0].output.terminal_schema_quarantines
            );
        }
    }
}

#[test]
fn randomized_skew_terminal_failure_is_canonical_across_jobs() {
    let mut successful_cases = 0;
    let mut canonical_failures = BTreeSet::new();
    for seed in 12..36 {
        let first_failure = (seed * 5 + 1) % 8;
        let mut second_failure = (seed * 3 + 4) % 8;
        if second_failure == first_failure {
            second_failure = (second_failure + 1) % 8;
        }
        let resource = skewed_resource(seed, [first_failure, second_failure]);
        let (plan, source) = skewed_plan(&resource, seed);
        let outcomes = [1, 2, 4, 8]
            .into_iter()
            .map(|jobs| run_skewed_jobs(&resource, &plan, &source, jobs))
            .collect::<Vec<_>>();
        match &outcomes[0] {
            Ok(expected) => {
                successful_cases += 1;
                for outcome in &outcomes[1..] {
                    let actual = outcome.as_ref().unwrap();
                    assert_eq!(
                        actual.output.manifest.package_hash,
                        expected.output.manifest.package_hash
                    );
                    assert_eq!(
                        actual.output.identity_segments(),
                        expected.output.identity_segments()
                    );
                    assert_eq!(actual.output.lineage, expected.output.lineage);
                    assert_eq!(actual.segment_positions, expected.segment_positions);
                }
            }
            Err(expected) => {
                canonical_failures.insert(expected.message.clone());
                for outcome in &outcomes[1..] {
                    let actual = outcome.as_ref().unwrap_err();
                    assert_eq!(actual.kind, expected.kind);
                    assert_eq!(actual.message, expected.message);
                }
                assert!(
                    expected
                        .message
                        .contains(&format!("partition {first_failure}"))
                        || expected
                            .message
                            .contains(&format!("partition {second_failure}")),
                    "{expected}"
                );
            }
        }
    }
    assert!(
        successful_cases > 0,
        "limits never stopped before source failure"
    );
    assert!(
        canonical_failures.len() >= 4,
        "failure matrix did not vary canonical error authority: {canonical_failures:?}"
    );
}

fn fast_test_retry_policy() -> cdf_runtime::SourceRetryPolicy {
    cdf_runtime::SourceRetryPolicy {
        max_total_attempts: 3,
        max_elapsed_ms: 30_000,
        base_delay_ms: 1,
        max_delay_ms: 1,
    }
}

fn retry_positioned_batches(position: &SourcePosition) -> Vec<Batch> {
    sample_batches()
        .into_iter()
        .map(|mut batch| {
            batch.header.source_position = Some(position.clone());
            let retained_bytes = batch
                .record_batch()
                .map(cdf_memory::record_batch_retained_bytes)
                .transpose()
                .unwrap()
                .unwrap_or(0)
                + batch.header.pre_contract_evidence_retained_bytes().unwrap();
            batch
                .with_retention(
                    cdf_kernel::PayloadRetention::new(Arc::new(()), retained_bytes).unwrap(),
                )
                .unwrap()
        })
        .collect()
}

#[test]
fn scheduler_retries_atomic_open_and_records_one_canonical_success() {
    let mut position = terminal_file_position();
    let SourcePosition::FileManifest(planned_manifest) = &mut position else {
        unreachable!("fixture is a file manifest")
    };
    planned_manifest.files[0].sha256 = None;
    let attestation = PartitionAttestation::new(position.clone(), None);
    let mut completed_position = position.clone();
    let SourcePosition::FileManifest(completed_manifest) = &mut completed_position else {
        unreachable!("fixture is a file manifest")
    };
    completed_manifest.files[0].sha256 = Some(format!("sha256:{}", "a".repeat(64)));
    let resource = MockResource::tier_b(retry_positioned_batches(&position))
        .with_partition_count(1)
        .with_transient_open_failures(1)
        .with_attestation(attestation)
        .with_completion_attestation(PartitionAttestation::new(completed_position, None));
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());

    let output = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        package.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services),
    ))
    .unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    assert_eq!(output.output.profile.output_rows, 3);
    let retries = output.execution_evidence().source_retries();
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].partition_ordinal(), 0);
    assert_eq!(retries[0].history().len(), 1);
    assert_eq!(retries[0].history()[0].failed_attempt, 1);
    assert_eq!(
        retries[0].history()[0].cause,
        cdf_kernel::ErrorKind::Transient
    );
}

#[test]
fn scheduler_retries_lazy_stream_failure_before_first_batch() {
    let position = terminal_file_position();
    let attestation = PartitionAttestation::new(position.clone(), None);
    let resource = MockResource::tier_b(retry_positioned_batches(&position))
        .with_partition_count(1)
        .with_transient_stream_failures(1)
        .with_attestation(attestation.clone())
        .with_completion_attestation(attestation);
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());

    let output = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        package.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services),
    ))
    .unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 1);
    assert_eq!(output.output.profile.output_rows, 3);
    assert_eq!(output.execution_evidence().source_retries().len(), 1);
}

#[test]
fn scheduler_reattests_a_retried_partial_limit_without_requiring_eof() {
    let position = terminal_file_position();
    let attestation = PartitionAttestation::new(position.clone(), None);
    let mut batches = retry_positioned_batches(&position);
    batches[1].header.partition_id = PartitionId::new("part-0").unwrap();
    let resource = MockResource::tier_b(batches)
        .with_partition_count(1)
        .with_transient_open_failures(1)
        .with_attestation(attestation.clone())
        .with_completion_attestation(attestation);
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, Some(1), ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());

    let output = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        package.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services),
    ))
    .unwrap();

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(resource.attest_count.load(Ordering::SeqCst), 2);
    assert_eq!(output.output.profile.output_rows, 1);
    assert_eq!(output.execution_evidence().source_retries().len(), 1);
}

#[test]
fn exhausted_retry_history_survives_a_failed_engine_return() {
    let position = terminal_file_position();
    let attestation = PartitionAttestation::new(position.clone(), None);
    let resource = MockResource::tier_b(retry_positioned_batches(&position))
        .with_partition_count(1)
        .with_transient_open_failures(3)
        .with_attestation(attestation.clone())
        .with_completion_attestation(attestation);
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let options = EngineExecutionConfig::default()
        .with_execution_services(services)
        .new_invocation();
    let retry_evidence = options.source_retry_evidence();

    let error = block_on(
        super::execute_to_package_with_segment_positions_and_pre_finalize(
            &plan,
            &resource,
            package.path(),
            &pre_finalize,
            options,
        ),
    )
    .unwrap_err();

    assert!(error.message.contains("attempt limit exhausted"), "{error}");
    let history = retry_evidence.snapshot().unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].history().len(), 3);
    assert_eq!(
        history[0].history()[2].exhaustion,
        Some(cdf_runtime::SourceRetryExhaustion::AttemptLimit)
    );
}

#[test]
fn nonretryable_reattest_failure_preserves_primary_error_and_history() {
    let position = terminal_file_position();
    let resource = MockResource::tier_b(retry_positioned_batches(&position))
        .with_partition_count(1)
        .with_transient_open_failures(1)
        .with_attestation_error("planned identity cannot be reattested");
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let options = EngineExecutionConfig::default()
        .with_execution_services(services)
        .new_invocation();
    let retry_evidence = options.source_retry_evidence();

    let error = block_on(
        super::execute_to_package_with_segment_positions_and_pre_finalize(
            &plan,
            &resource,
            package.path(),
            &pre_finalize,
            options,
        ),
    )
    .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data, "{error}");
    assert!(error.message.contains("cannot be reattested"), "{error}");
    let history = retry_evidence.snapshot().unwrap();
    assert_eq!(history[0].history().len(), 2);
    assert_eq!(
        history[0].history()[1].exhaustion,
        Some(cdf_runtime::SourceRetryExhaustion::Ineligible)
    );
    assert_eq!(history[0].history()[1].cause, cdf_kernel::ErrorKind::Data);
}

#[test]
fn execution_rejects_tampered_retry_schedule_before_source_contact() {
    let resource = MockResource::tier_b(sample_batches()).with_partition_count(1);
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let mut forged_retry =
        cdf_runtime::CompiledSourceRetry::from_capabilities(&source.execution_capabilities)
            .unwrap();
    forged_retry.as_mut().unwrap().policy.max_total_attempts += 1;
    plan.partition_schedule.as_mut().unwrap().admission.retry = forged_retry;
    plan.explain.partition_schedule = plan.partition_schedule.clone();
    let package = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, package.path())).unwrap_err();

    assert!(
        error
            .message
            .contains("partition schedule differs from its scan or compiled source execution plan"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn execution_rejects_coherently_widened_source_ceiling_and_schedule() {
    let resource = MockResource::tier_b(sample_batches())
        .with_partition_count(1)
        .with_transient_open_failures(0);
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let mut forged_compiler_source = source.clone();
    forged_compiler_source
        .execution_capabilities
        .retry_policy
        .as_mut()
        .unwrap()
        .max_total_attempts = 4;
    forged_compiler_source.validate().unwrap();
    let forged_source =
        cdf_runtime::CompiledSourceExecutionPlan::compile(&forged_compiler_source).unwrap();
    let forged_schedule =
        cdf_runtime::CanonicalPartitionSchedule::compile(&forged_source, &plan.scan).unwrap();
    plan.compiled_schema_admission.source =
        Some(cdf_runtime::CompiledSourceCompilerBinding::compile(&forged_compiler_source).unwrap());
    plan.compiled_source_execution = Some(forged_source);
    plan.partition_schedule = Some(forged_schedule.clone());
    plan.explain.partition_schedule = Some(forged_schedule);
    plan.compiled_stream_policy = None;
    plan.explain.compiled_stream_policy = None;
    let package = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, package.path())).unwrap_err();

    assert!(
        error
            .message
            .contains("resolved source does not match the compiler source artifact"),
        "{error}"
    );
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn scheduler_rejects_generation_change_after_retried_open() {
    let initial_position = terminal_file_position();
    let pre_open = PartitionAttestation::new(initial_position.clone(), None);
    let mut changed_position = initial_position.clone();
    let SourcePosition::FileManifest(changed_manifest) = &mut changed_position else {
        unreachable!("fixture is a file manifest")
    };
    changed_manifest.files[0].etag = Some("etag-changed".to_owned());
    let resource = MockResource::tier_b(retry_positioned_batches(&initial_position))
        .with_partition_count(1)
        .with_transient_open_failures(1)
        .with_attestation(pre_open)
        .with_completion_attestation(PartitionAttestation::new(changed_position, None));
    let mut plan = Planner::new()
        .plan_tier_b(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let source = mock_compiled_source_plan(&resource, Some(fast_test_retry_policy()));
    resource.bind_compiled_source(&source);
    plan = plan.bind_compiled_source(&source).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let package = TempDir::new().unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());

    let error = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        package.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services),
    ))
    .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data, "{error:?}");
    assert!(
        error
            .message
            .contains("changed source generation or schema")
    );
    assert!(error.message.contains("re-plan"));
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
}

#[test]
fn validation_program_source_name_can_cover_and_rename_batch_field() {
    let resource = MockResource::tier_a(sample_batches()).without_control_keys();
    let mut input = plan_input(
        vec![],
        Some(vec!["name".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    rename_column_program_output(&mut input.validation_program, "name", "customer_name");
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let schema = batches[0].schema();
    let field = schema.field(0);
    assert_eq!(field.name(), "customer_name");
    assert_eq!(source_name(field), Some("name"));
}

#[test]
fn validation_program_output_name_can_cover_already_normalized_batch_field() {
    let resource = MockResource::tier_a(output_name_batches()).without_control_keys();
    let mut input = plan_input_for_schema(
        output_name_schema(),
        vec![],
        Some(vec!["customer_name".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    rename_column_program_source(&mut input.validation_program, "customer_name", "name");
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let schema = batches[0].schema();
    let field = schema.field(0);
    assert_eq!(field.name(), "customer_name");
    assert_eq!(source_name(field), Some("name"));
}

#[test]
fn package_artifacts_record_schema_coercion_evidence_and_physical_type_metadata() {
    let resource = MockResource::tier_a(vec![parquet_reconciled_batch()]);
    let input = plan_input_for_schema(
        parquet_reconciled_schema(),
        vec![],
        None,
        None,
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);

    let plan_evidence = stream_admission_coercion(temp.path());
    let widened = coercion_decision(&plan_evidence, "id");
    assert_eq!(widened.decision, FieldCoercionDecision::Widened);
    assert_eq!(widened.observed_type.as_deref(), Some("Int32"));
    assert_eq!(widened.constraint_type.as_deref(), Some("Int64"));

    let preserved = coercion_decision(&plan_evidence, "name");
    assert_eq!(preserved.decision, FieldCoercionDecision::Preserved);
    assert_eq!(preserved.observed_type.as_deref(), Some("Utf8"));
    assert_eq!(preserved.constraint_type.as_deref(), Some("Utf8"));

    assert!(!temp.path().join("schema/coercion-plan.json").exists());

    let output_schema: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("schema/output.json")).unwrap())
            .unwrap();
    assert_eq!(
        output_schema["fields"][0]["metadata"]["cdf:physical_type"],
        "Int32"
    );
    assert_eq!(
        output_schema["fields"][0]["metadata"]["cdf:source_name"],
        "id"
    );
    assert_eq!(
        output_schema["fields"][1]["metadata"]["cdf:source_name"],
        "name"
    );

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(
        batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2]
    );
}

#[test]
fn compiled_output_schema_strips_runtime_provenance_only_after_serializing_evidence() {
    let observed = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let constraint = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let reconciliation = reconcile_schema(
        &observed,
        constraint.as_ref(),
        &ContractPolicy::default().types,
    )
    .unwrap();
    let serialized_plan = serde_json::to_string(&reconciliation.plan).unwrap();
    let runtime_schema = Arc::new(reconciliation.schema);
    assert_eq!(
        runtime_schema
            .field(0)
            .metadata()
            .get("cdf:physical_type")
            .map(String::as_str),
        Some("Int32")
    );
    let record_batch = RecordBatch::try_new(
        runtime_schema,
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-runtime-provenance").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-runtime-provenance").unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.schema_coercion_plan = Some(serialized_plan);
    batch.header.mark_materialized_output(&observed).unwrap();
    let resource = MockResource::tier_a(vec![batch]).with_schema(constraint.clone());
    let input = plan_input_for_schema(constraint, vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    reader.verify().unwrap();
    let runtime_output = reader.runtime_arrow_schema().unwrap();
    assert_eq!(runtime_output, plan.output_arrow_schema().unwrap());
    assert!(
        !runtime_output
            .field(0)
            .metadata()
            .contains_key("cdf:physical_type")
    );
    assert_eq!(
        runtime_output
            .field(0)
            .metadata()
            .get("cdf:source_name")
            .map(String::as_str),
        Some("id")
    );
    let evidence = stream_admission_coercion(temp.path());
    let widened = coercion_decision(&evidence, "id");
    assert_eq!(widened.observed_type.as_deref(), Some("Int32"));
    assert_eq!(widened.constraint_type.as_deref(), Some("Int64"));
    assert_eq!(widened.decision, FieldCoercionDecision::Widened);
}

#[test]
fn package_artifacts_preserve_exact_embedded_lossy_and_extra_reconciliation_decisions() {
    let observed = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("source_only", DataType::Utf8, true),
    ]);
    let constraint = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let mut type_policy = ContractPolicy::default().types;
    type_policy.allow_lossy_mapping = true;
    let reconciliation = reconcile_schema(&observed, &constraint, &type_policy).unwrap();
    let serialized_plan = serde_json::to_string(&reconciliation.plan).unwrap();
    let schema = Arc::new(reconciliation.schema);
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-json-reconciled").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-json-reconciled").unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.schema_coercion_plan = Some(serialized_plan);
    batch.header.mark_materialized_output(&observed).unwrap();
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            1,
            1,
            vec!["source_only".to_owned()],
            Field::new("source_only", DataType::Utf8, true),
            None,
            Arc::new(StringArray::from(vec!["present-on-one-row"])) as ArrayRef,
            0,
        )
        .unwrap(),
    );
    let incomplete_residual_evidence = batch.clone();
    batch.header.mark_materialized_residuals_complete();
    let resource = MockResource::tier_a(vec![batch]).with_type_policy_allowances(
        cdf_kernel::TypePolicyAllowances {
            coerce_types: false,
            allow_lossy_mapping: true,
        },
    );
    let input = plan_input_for_schema(
        Arc::new(constraint),
        vec![],
        None,
        None,
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    let evidence = stream_admission_coercion(temp.path());
    assert_eq!(
        coercion_decision(&evidence, "id").decision,
        FieldCoercionDecision::LossyAllowed
    );
    assert_eq!(
        coercion_decision(&evidence, "source_only").decision,
        FieldCoercionDecision::Extra
    );

    let incomplete_resource = MockResource::tier_a(vec![incomplete_residual_evidence])
        .with_type_policy_allowances(cdf_kernel::TypePolicyAllowances {
            coerce_types: false,
            allow_lossy_mapping: true,
        });
    let incomplete_plan = Planner::new()
        .plan_tier_a(
            &incomplete_resource,
            plan_input_for_schema(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                vec![],
                None,
                None,
                ExecutionExtent::bounded(),
            ),
        )
        .unwrap();
    let incomplete_package = TempDir::new().unwrap();
    let error = block_on(execute_to_package(
        &incomplete_plan,
        &incomplete_resource,
        incomplete_package.path(),
    ))
    .unwrap_err();
    assert!(
        error.to_string().contains("absent from its physical batch"),
        "{error}"
    );
}

#[test]
fn package_execution_rejects_source_carried_coercion_metadata_without_trusted_header() {
    let injected_plan = serde_json::json!({
        "fields": [{
            "source_name": "id",
            "observed_name": "id",
            "output_name": "id",
            "observed_type": "Int64",
            "constraint_type": "Int64",
            "decision": "preserved",
            "outcome": "pass",
            "reason": "observed type already satisfies the constraint"
        }]
    })
    .to_string();
    let injected_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int64, false)],
        HashMap::from([("cdf:schema_coercion_plan".to_owned(), injected_plan)]),
    ));
    let record_batch =
        RecordBatch::try_new(injected_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let batch = Batch::from_record_batch(
        BatchId::new("batch-injected-coercion").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-injected-coercion").unwrap(),
        record_batch,
    )
    .unwrap();
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input_for_schema(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![],
        None,
        None,
        ExecutionExtent::bounded(),
    );
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(error.to_string().contains("without trusted batch evidence"));
}

#[test]
fn package_execution_rejects_malformed_trusted_coercion_header() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-malformed-coercion").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-malformed-coercion").unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.schema_coercion_plan = Some("{not-json".to_owned());
    batch
        .header
        .mark_materialized_output(schema.as_ref())
        .unwrap();
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(error.to_string().contains("not a valid coercion plan"));
}

#[test]
fn package_execution_rejects_valid_header_only_coercion_injection() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-header-only-coercion").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        SchemaHash::new("schema-header-only-coercion").unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.schema_coercion_plan = Some(
        serde_json::json!({
            "fields": [{
                "source_name": "fabricated_extra",
                "observed_name": "fabricated_extra",
                "observed_type": "Utf8",
                "decision": "extra",
                "outcome": "admitted_as_variant",
                "reason": "observed field is outside the constraint projection"
            }]
        })
        .to_string(),
    );
    batch
        .header
        .mark_materialized_output(schema.as_ref())
        .unwrap();
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("has no matching reserved Arrow schema metadata")
    );
}

#[test]
fn contract_exec_filters_quarantined_rows_before_normalize() {
    let resource = MockResource::tier_a(sample_batches());
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Domain {
        column: "name".to_owned(),
        allowed: vec!["two".to_owned(), "three".to_owned()],
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let names = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "two");
    assert_eq!(names.value(1), "three");
}

#[test]
fn fused_and_unfused_transform_modes_produce_identical_packages() {
    let resource = MockResource::tier_a(sample_batches());
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    policy.rows.rules = vec![RowRule::Domain {
        column: "name".to_owned(),
        allowed: vec!["two".to_owned(), "three".to_owned()],
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let fused_dir = TempDir::new().unwrap();
    let unfused_dir = TempDir::new().unwrap();
    let pre_finalize =
        |_: &cdf_package::PackageBuilder, _: EnginePackageDraft<'_>| -> Result<()> { Ok(()) };

    let fused = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        fused_dir.path(),
        &pre_finalize,
        EngineExecutionConfig::default(),
    ))
    .unwrap();
    let unfused = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        unfused_dir.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_unfused_transform_for_conformance(true),
    ))
    .unwrap();

    assert_eq!(fused.source_frontier.wait_ns, 0);
    assert_eq!(unfused.source_frontier.wait_ns, 0);
    assert_eq!(fused, unfused);
    assert_eq!(
        std::fs::read(fused_dir.path().join("quarantine/part-000001.parquet")).unwrap(),
        std::fs::read(unfused_dir.path().join("quarantine/part-000001.parquet")).unwrap()
    );
    cdf_package::PackageReader::open(fused_dir.path())
        .unwrap()
        .verify()
        .unwrap();
    cdf_package::PackageReader::open(unfused_dir.path())
        .unwrap()
        .verify()
        .unwrap();
}

#[test]
fn fused_transform_reserves_before_allocation_and_releases_after_persist() {
    let resource = MockResource::tier_a(sample_batches());
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let pre_finalize =
        |_: &cdf_package::PackageBuilder, _: EnginePackageDraft<'_>| -> Result<()> { Ok(()) };
    let (_, services) =
        StandaloneExecutionHost::default_services_with_spill(64 * 1024 * 1024, 1024 * 1024)
            .unwrap();
    let output_dir = TempDir::new().unwrap();
    block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        output_dir.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();
    let memory = services.memory().snapshot();
    assert!(memory.consumers.iter().any(|(consumer, usage)| {
        consumer.class == cdf_memory::MemoryClass::Transform && usage.peak_bytes > 0
    }));
    assert_eq!(memory.current_bytes, 0);

    let (_, tiny_services) =
        StandaloneExecutionHost::default_services_with_spill(64, 1024).unwrap();
    let failed_dir = TempDir::new().unwrap();
    let error = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        failed_dir.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(tiny_services.clone()),
    ))
    .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("exceeds managed budget"));
    assert_eq!(tiny_services.memory().snapshot().current_bytes, 0);
}

#[test]
fn contract_exec_writes_redacted_quarantine_artifact_and_keeps_accepted_rows() {
    let raw_pii = "pii-fixture-sensitive";
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        with_semantic(Field::new("name", DataType::Utf8, false), "pii:email"),
        Field::new("active", DataType::Boolean, false),
    ]));
    let mut batch = batch_for_partition_with_schema(
        "batch-pii",
        "part-0",
        schema.clone(),
        vec![1, 2],
        vec!["ok@example.test", raw_pii],
        vec![true, true],
    );
    batch.header.source_position = Some(SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "/tmp/cdf/pii.ndjson".to_owned(),
            size_bytes: 64,
            source_generation: None,
            etag: None,
            object_version: None,
            sha256: Some(
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            ),
        }],
    }));
    let resource = MockResource::tier_a(vec![batch]);
    let mut input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Regex {
        column: "name".to_owned(),
        pattern: r"^[^@]+@example\.test$".to_owned(),
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let accepted = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted.value(0), "ok@example.test");

    let quarantine = collect_quarantine_records(&reader);
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].source_row_ordinal, 1);
    assert_eq!(quarantine[0].error_code, "regex_violation");
    assert!(matches!(
        quarantine[0].source_position,
        Some(SourcePosition::FileManifest(_))
    ));
    let QuarantineObservedValue::Hashed { algorithm, value } =
        &quarantine[0].observed_value_redacted
    else {
        panic!("pii semantic field must be hash-redacted");
    };
    assert_eq!(algorithm, "sha256");
    assert_eq!(
        value,
        "sha256:0a08d503e0f6794940fd8e6a1f547999622742616551894946ba6dc0489cf184"
    );

    let files = package_identity_file_paths(&reader);
    assert!(files.contains("stats/verdict-summary.json"));
    assert!(files.contains("stats/quarantine-summary.json"));

    let verdict_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/verdict-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(verdict_summary["input_rows"], 2);
    assert_eq!(verdict_summary["accepted_rows"], 1);
    assert_eq!(verdict_summary["quarantined_rows"], 1);
    assert_eq!(verdict_summary["violation_count"], 1);
    assert_eq!(verdict_summary["quarantine_candidate_count"], 1);
    assert!(
        verdict_summary["rule_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary
                == &serde_json::json!({
                    "rule_id": "row-rule-0000-regex",
                    "error_code": "regex_violation",
                    "checked_rows": 2,
                    "violation_count": 1
                }))
    );

    let quarantine_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/quarantine-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine_summary["quarantined_rows"], 1);
    assert_eq!(quarantine_summary["quarantine_candidate_count"], 1);
    assert_eq!(quarantine_summary["artifact_count"], 1);
    assert_eq!(
        quarantine_summary["artifacts"],
        serde_json::json!(["quarantine/part-000001.parquet"])
    );

    let quarantine_path = temp.path().join("quarantine/part-000001.parquet");
    let artifact = std::fs::read(quarantine_path).unwrap();
    assert!(!String::from_utf8_lossy(&artifact).contains(raw_pii));
    assert!(package_identity_file_paths(&reader).contains("quarantine/part-000001.parquet"));
}

#[test]
fn contract_quarantine_preserves_source_ordinal_after_transform_filter() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let batch = batch_for_partition_with_schema(
        "batch-transform-quarantine",
        "part-0",
        schema.clone(),
        vec![1, 2],
        vec!["ignored", "bad"],
        vec![true, true],
    );
    let resource = MockResource::tier_a(vec![batch]);
    let mut input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.transforms = vec![cdf_contract::TransformDescription::Filter {
        expression: Expression::parse_comparison("id >= 2").unwrap(),
    }];
    policy.rows.rules = vec![RowRule::Regex {
        column: "name".to_owned(),
        pattern: "^ok$".to_owned(),
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let quarantine =
        collect_quarantine_records(&cdf_package::PackageReader::open(temp.path()).unwrap());
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].error_code, "regex_violation");
    assert_eq!(quarantine[0].source_row_ordinal, 1);
}

#[test]
fn contract_quarantine_preserves_source_ordinal_after_residual_quarantine() {
    let id_field = Field::new("id", DataType::Int32, true);
    let schema = Arc::new(Schema::new(vec![
        id_field.clone(),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![None, Some(2)])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ignored", "bad"])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true, true])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-residual-contract-quarantine").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
        record_batch,
    )
    .unwrap();
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            0,
            0,
            vec!["id".to_owned()],
            Field::new("id", DataType::Utf8, true),
            Some(id_field),
            Arc::new(StringArray::from(vec!["bad-id"])) as ArrayRef,
            0,
        )
        .unwrap(),
    );
    let resource = MockResource::tier_a(vec![batch]);
    let mut input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    policy.rows.rules = vec![RowRule::Regex {
        column: "name".to_owned(),
        pattern: "^ok$".to_owned(),
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let quarantine =
        collect_quarantine_records(&cdf_package::PackageReader::open(temp.path()).unwrap());
    let contract = quarantine
        .iter()
        .find(|record| record.error_code == "regex_violation")
        .unwrap();
    assert_eq!(contract.source_row_ordinal, 1);
}

#[test]
fn source_decode_quarantine_facts_fold_into_package_artifacts() {
    let mut batch = batch_for_partition(
        "batch-source-drift",
        "part-0",
        vec![3],
        vec!["three"],
        vec![true],
    );
    batch.header.source_position = Some(SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "/tmp/cdf/source-drift.ndjson".to_owned(),
            size_bytes: 96,
            source_generation: None,
            etag: None,
            object_version: None,
            sha256: Some(
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
            ),
        }],
    }));
    batch.header.pre_contract_quarantine = vec![PreContractQuarantineFact {
        source_row_ordinal: 1,
        rule_id: "source-decode:event_type:type-mismatch".to_owned(),
        error_code: "source_type_mismatch".to_owned(),
        source_position: batch.header.source_position.clone(),
        observed_value_redacted: PreContractObservedValue::Preserved {
            value: "42".to_owned(),
        },
    }];
    let resource = MockResource::tier_a(vec![batch]);
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let accepted = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&accepted[0], "id"), vec![3]);
    assert_eq!(batch_strings(&accepted, "name"), vec!["three"]);
    let quarantine = collect_quarantine_records(&reader);
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].source_row_ordinal, 1);
    assert_eq!(
        quarantine[0].rule_id,
        "source-decode:event_type:type-mismatch"
    );
    assert_eq!(quarantine[0].error_code, "source_type_mismatch");
    assert!(matches!(
        quarantine[0].source_position,
        Some(SourcePosition::FileManifest(_))
    ));
    assert_eq!(
        quarantine[0].observed_value_redacted,
        QuarantineObservedValue::Preserved {
            value: "42".to_owned()
        }
    );

    let verdict_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/verdict-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(verdict_summary["input_rows"], 2);
    assert_eq!(verdict_summary["accepted_rows"], 1);
    assert_eq!(verdict_summary["quarantined_rows"], 1);
    assert_eq!(verdict_summary["violation_count"], 1);
    assert_eq!(verdict_summary["quarantine_candidate_count"], 1);
    assert!(
        verdict_summary["rule_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary
                == &serde_json::json!({
                    "rule_id": "source-decode:event_type:type-mismatch",
                    "error_code": "source_type_mismatch",
                    "checked_rows": 1,
                    "violation_count": 1
                }))
    );

    let quarantine_summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("stats/quarantine-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine_summary["quarantined_rows"], 1);
    assert_eq!(quarantine_summary["quarantine_candidate_count"], 1);
    assert_eq!(
        quarantine_summary["artifacts"],
        serde_json::json!(["quarantine/part-000001.parquet"])
    );
    reader.verify().unwrap();
}

#[test]
fn variant_capture_materializes_nested_values_and_contract_evolution_evidence() {
    let resource = MockResource::tier_a(vec![nested_variant_batch()]);
    let mut input = plan_input_for_schema(
        resource.schema(),
        vec![],
        None,
        None,
        ExecutionExtent::bounded(),
    );
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.normalization.nested = NestedDataPolicy::VariantCapture(Default::default());
    policy.rows.rules = vec![RowRule::Regex {
        column: "email".to_owned(),
        pattern: r"^[^@]+@example\.test$".to_owned(),
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let batch = &batches[0];
    assert_eq!(batch.schema().fields().len(), 4);
    assert!(batch.schema().field_with_name("payload").is_err());
    assert!(batch.schema().field_with_name("tags").is_err());
    assert!(batch.schema().field_with_name("attributes").is_err());
    let batch_schema = batch.schema();
    let variant_field = batch_schema.field_with_name(VARIANT_COLUMN_NAME).unwrap();
    assert_eq!(
        cdf_kernel::semantic(variant_field),
        Some(VARIANT_SEMANTIC_TAG)
    );
    assert_eq!(
        variant_field
            .metadata()
            .get(RESIDUAL_ENCODING_METADATA_KEY)
            .map(String::as_str),
        Some(RESIDUAL_ENCODING_NAME)
    );
    let variants = batch
        .column_by_name(VARIANT_COLUMN_NAME)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        variants.value(0),
        r#"{"v":1,"fields":{"/attributes":{"arrow_type":{"kind":"map","field":{"name":"entries","data_type":{"kind":"struct","fields":[{"name":"keys","data_type":{"kind":"utf8","offset_width":32},"nullable":false,"metadata":{}},{"name":"values","data_type":{"kind":"int","signed":true,"bits":32},"nullable":true,"metadata":{}}]},"nullable":false,"metadata":{}},"sorted":false},"encoding":"nested","value":[{"key":"tier","value":"1"}]},"/payload":{"arrow_type":{"kind":"struct","fields":[{"name":"kind","data_type":{"kind":"utf8","offset_width":32},"nullable":false,"metadata":{}},{"name":"count","data_type":{"kind":"int","signed":true,"bits":32},"nullable":false,"metadata":{}}]},"encoding":"nested","value":{"count":"7","kind":"alpha"}},"/tags":{"arrow_type":{"kind":"list","field":{"name":"item","data_type":{"kind":"int","signed":true,"bits":32},"nullable":true,"metadata":{}},"offset_width":32,"view":false},"encoding":"nested","value":["1","2"]}}}"#
    );
    let decoded = cdf_contract::decode_residual_json_v1(variants.value(0).as_bytes()).unwrap();
    assert_eq!(
        decoded
            .iter()
            .map(|field| field.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/attributes", "/payload", "/tags"]
    );
    let source_schema = resource.schema();
    assert_eq!(
        decoded[0].array.data_type(),
        source_schema
            .field_with_name("attributes")
            .unwrap()
            .data_type()
    );
    assert_eq!(
        decoded[1].array.data_type(),
        source_schema
            .field_with_name("payload")
            .unwrap()
            .data_type()
    );
    assert_eq!(
        decoded[2].array.data_type(),
        source_schema.field_with_name("tags").unwrap().data_type()
    );

    let output_schema: serde_json::Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("schema/output.json")).unwrap())
            .unwrap();
    assert_eq!(
        output_schema["fields"][2],
        serde_json::json!({
            "name": VARIANT_COLUMN_NAME,
            "data_type": "Utf8",
            "nullable": true,
            "semantic": VARIANT_SEMANTIC_TAG,
            "metadata": {
                (RESIDUAL_ENCODING_METADATA_KEY): RESIDUAL_ENCODING_NAME
            }
        })
    );
    let evolution_path = temp.path().join("schema/contract-evolution.json");
    let evolution_bytes = std::fs::read(&evolution_path).unwrap();
    let evolution: serde_json::Value = serde_json::from_slice(&evolution_bytes).unwrap();
    assert_eq!(evolution["implicit_promotion_count"], 0);
    assert_eq!(evolution["promotion_events"], serde_json::json!([]));
    assert_eq!(
        evolution["variant_capture"],
        serde_json::json!([
            {
                "source_field": "attributes",
                "variant_column": VARIANT_COLUMN_NAME,
                "semantic": VARIANT_SEMANTIC_TAG
            },
            {
                "source_field": "payload",
                "variant_column": VARIANT_COLUMN_NAME,
                "semantic": VARIANT_SEMANTIC_TAG
            },
            {
                "source_field": "tags",
                "variant_column": VARIANT_COLUMN_NAME,
                "semantic": VARIANT_SEMANTIC_TAG
            }
        ])
    );
    assert_eq!(
        evolution_bytes,
        cdf_package::canonical_json_bytes(&evolution).unwrap()
    );
    assert!(package_identity_file_paths(&reader).contains("schema/contract-evolution.json"));
    assert_eq!(reader.manifest().identity.segment_count, 1);

    let quarantine = collect_quarantine_records(&reader);
    assert_eq!(quarantine.len(), 1);
    let QuarantineObservedValue::Hashed { value, .. } = &quarantine[0].observed_value_redacted
    else {
        panic!("pii variant interaction must keep quarantine observed value hashed");
    };
    assert!(value.starts_with("sha256:"));
    let quarantine_artifact =
        std::fs::read(temp.path().join("quarantine/part-000001.parquet")).unwrap();
    assert!(!String::from_utf8_lossy(&quarantine_artifact).contains("raw-secret"));
}

#[test]
fn residual_contract_exec_captures_safe_values_redacts_pii_and_quarantines_controls() {
    let id_field = Field::new("id", DataType::Int32, true);
    let note_field = with_semantic(Field::new("note", DataType::Int32, true), "pii:note");
    let schema = Arc::new(Schema::new(vec![id_field.clone(), note_field.clone()]));
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-residual").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
        record_batch,
    )
    .unwrap();
    let note_values = Arc::new(StringArray::from(vec!["alice@example.test"])) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            1,
            1,
            vec!["note".to_owned()],
            with_semantic(Field::new("note", DataType::Utf8, true), "pii:note"),
            Some(note_field),
            note_values,
            0,
        )
        .unwrap(),
    );
    let unknown_values = Arc::new(StringArray::from(vec!["top-secret"])) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            1,
            1,
            vec!["new_secret".to_owned()],
            with_semantic(Field::new("new_secret", DataType::Utf8, true), "pii:secret"),
            None,
            unknown_values,
            0,
        )
        .unwrap(),
    );
    let id_values = Arc::new(StringArray::from(vec!["bad-id"])) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            2,
            2,
            vec!["id".to_owned()],
            Field::new("id", DataType::Utf8, true),
            Some(id_field),
            id_values,
            0,
        )
        .unwrap(),
    );

    let resource =
        MockResource::tier_a(vec![batch]).with_write_disposition(WriteDisposition::Append);
    let mut input = plan_input_for_schema(
        schema,
        vec![],
        Some(vec!["id".to_owned(), "note".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let planned_schema = plan.output_arrow_schema().unwrap();
    assert_eq!(planned_schema.fields().len(), 3);
    assert_eq!(planned_schema.field(2).name(), VARIANT_COLUMN_NAME);
    assert_ne!(
        plan.schema_authority().effective_schema_hash,
        plan.output_schema.arrow_schema_hash
    );

    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    let output = &batches[0];
    assert_eq!(output.num_rows(), 2);
    let variants = output
        .column_by_name(VARIANT_COLUMN_NAME)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(variants.is_null(0));
    assert!(variants.value(1).contains("sha256:"));
    assert!(!variants.value(1).contains("alice@example.test"));
    assert!(!variants.value(1).contains("top-secret"));

    let quarantine = collect_quarantine_records(&reader);
    assert_eq!(quarantine.len(), 1);
    assert_eq!(quarantine[0].error_code, "cdf.residual_control_critical");
    let evolution_bytes =
        std::fs::read(temp.path().join("schema/contract-evolution.json")).unwrap();
    let evolution_text = String::from_utf8(evolution_bytes.clone()).unwrap();
    assert!(!evolution_text.contains("alice@example.test"));
    assert!(!evolution_text.contains("top-secret"));
    let evolution: serde_json::Value = serde_json::from_slice(&evolution_bytes).unwrap();
    assert_eq!(evolution["version"], 1);
    assert_eq!(evolution["residual_decisions"].as_array().unwrap().len(), 3);
    reader.verify().unwrap();
    assert_eq!(reader.runtime_arrow_schema().unwrap(), planned_schema);
}

#[test]
fn residual_multi_partition_decisions_share_verified_effective_schema_and_keep_identity() {
    const CAPTURE_SENTINEL: &str = "rp2-captured-pii-sentinel";
    const QUARANTINE_SENTINEL: &str = "rp2-quarantined-pii-sentinel";

    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        with_semantic(Field::new("note", DataType::Int32, true), "pii:note"),
    ]));
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let reconciliation = reconcile_schema(
        physical_schema.as_ref(),
        physical_schema.as_ref(),
        &ContractPolicy::default().types,
    )
    .unwrap();
    let serialized_coercion = serde_json::to_string(&reconciliation.plan).unwrap();
    let schema = Arc::new(reconciliation.schema);
    let id_field = schema.field(0).as_ref().clone();
    let note_field = schema.field(1).as_ref().clone();

    let captured_record = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut captured_batch = Batch::from_record_batch(
        BatchId::new("batch-residual-captured").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(captured_record.schema().as_ref()).unwrap(),
        captured_record,
    )
    .unwrap();
    captured_batch.header.observed_schema_hash = physical_hash.clone();
    captured_batch.header.schema_coercion_plan = Some(serialized_coercion.clone());
    captured_batch
        .header
        .mark_materialized_output(physical_schema.as_ref())
        .unwrap();
    captured_batch.header.source_position = Some(terminal_file_position());
    captured_batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            10,
            0,
            vec!["note".to_owned()],
            with_semantic(Field::new("note", DataType::Utf8, true), "pii:note"),
            Some(note_field),
            Arc::new(StringArray::from(vec![CAPTURE_SENTINEL])) as ArrayRef,
            0,
        )
        .unwrap(),
    );

    let quarantined_record = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(30)])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut quarantined_batch = Batch::from_record_batch(
        BatchId::new("batch-residual-quarantined").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-1").unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(quarantined_record.schema().as_ref()).unwrap(),
        quarantined_record,
    )
    .unwrap();
    quarantined_batch.header.observed_schema_hash = physical_hash.clone();
    quarantined_batch.header.schema_coercion_plan = Some(serialized_coercion);
    quarantined_batch
        .header
        .mark_materialized_output(physical_schema.as_ref())
        .unwrap();
    quarantined_batch.header.source_position = Some(terminal_file_position());
    quarantined_batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            20,
            0,
            vec!["id".to_owned()],
            Field::new("id", DataType::Utf8, true),
            Some(id_field),
            Arc::new(StringArray::from(vec!["bad-control-id"])) as ArrayRef,
            0,
        )
        .unwrap(),
    );
    quarantined_batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            20,
            0,
            vec!["new_secret".to_owned()],
            with_semantic(Field::new("new_secret", DataType::Utf8, true), "pii:secret"),
            None,
            Arc::new(StringArray::from(vec![QUARANTINE_SENTINEL])) as ArrayRef,
            0,
        )
        .unwrap(),
    );

    let effective_schema_hash = SchemaHash::new("effective-snapshot-v1").unwrap();
    let evidence = bound_effective_schema_evidence(
        effective_schema_hash.clone(),
        "manifest-residual-mixed",
        ".cdf/schemas/orders@manifest-residual-mixed.discovery.json",
        vec![EffectiveSchemaObservationEvidence::new(
            "input-0",
            physical_hash.clone(),
            schema_observation_binding("input-0"),
        )],
    );
    let runtime = EffectiveSchemaRuntime::new(
        evidence,
        vec![EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap()
    .with_discovery_executor_budget(
        DiscoveryExecutorBudgetEvidence::new(64, 1_000, 128, 2).unwrap(),
    )
    .unwrap();
    let resource = MockResource::tier_b(vec![captured_batch, quarantined_batch])
        .with_effective_schema_runtime(schema.clone(), runtime)
        .with_write_disposition(WriteDisposition::Append);
    let mut input = plan_input_for_schema(
        schema,
        vec![],
        Some(vec!["id".to_owned(), "note".to_owned()]),
        None,
        ExecutionExtent::bounded(),
    );
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_b(&resource, input).unwrap();
    let planned_schema = plan.output_arrow_schema().unwrap();
    assert_eq!(
        plan.schema_authority().effective_schema_hash,
        effective_schema_hash
    );

    let temp = TempDir::new().unwrap();
    let plain = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();
    let managed_temp = TempDir::new().unwrap();
    let (_, services) =
        StandaloneExecutionHost::default_services_with_spill(64 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let managed = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        managed_temp.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();
    assert_eq!(managed.output.manifest.identity, plain.manifest.identity);
    assert_eq!(
        managed.output.manifest.package_hash,
        plain.manifest.package_hash
    );
    assert!(services.spill().snapshot().peak_bytes > 0);
    assert_eq!(services.spill().snapshot().current_bytes, 0);
    assert_eq!(services.memory().snapshot().current_bytes, 0);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    reader.verify().unwrap();
    assert_eq!(reader.runtime_arrow_schema().unwrap(), planned_schema);

    let evolution: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/contract-evolution.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        evolution["baseline_schema_hash"],
        plan.schema_authority().baseline_schema_hash.as_str()
    );
    assert_eq!(
        evolution["effective_schema_hash"],
        effective_schema_hash.as_str()
    );
    let decisions = evolution["residual_decisions"].as_array().unwrap();
    assert_eq!(decisions.len(), 3);
    assert!(decisions.iter().all(|decision| decision["version"] == 1));
    let captured = decisions
        .iter()
        .filter(|decision| decision["batch_id"] == "batch-residual-captured")
        .collect::<Vec<_>>();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0]["observation_id"], "input-0");
    assert_eq!(captured[0]["verdict"], "captured");
    assert_eq!(captured[0]["source_path"], serde_json::json!(["note"]));
    let quarantined = decisions
        .iter()
        .filter(|decision| decision["batch_id"] == "batch-residual-quarantined")
        .collect::<Vec<_>>();
    assert_eq!(quarantined.len(), 2);
    assert!(
        quarantined
            .iter()
            .all(|decision| decision["observation_id"] == "unobserved-part-1")
    );
    assert!(
        quarantined
            .iter()
            .all(|decision| decision["verdict"] == "quarantined")
    );
    assert_package_tree_excludes(temp.path(), &[CAPTURE_SENTINEL, QUARANTINE_SENTINEL]);
}

#[test]
fn residual_unsupported_encoding_becomes_named_quarantine() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef],
    )
    .unwrap();
    let mut batch = Batch::from_record_batch(
        BatchId::new("batch-unsupported-residual").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
        record_batch,
    )
    .unwrap();
    let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
    dictionary.append("value").unwrap();
    let dictionary = Arc::new(dictionary.finish()) as ArrayRef;
    batch.header.push_residual_candidate(
        PreContractResidualCandidate::new(
            0,
            0,
            vec!["unsupported".to_owned()],
            Field::new("unsupported", dictionary.data_type().clone(), true),
            None,
            dictionary,
            0,
        )
        .unwrap(),
    );
    let resource =
        MockResource::tier_a(vec![batch]).with_write_disposition(WriteDisposition::Append);
    let mut input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.schema.mode = SchemaEvolutionMode::Evolve;
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let quarantine = collect_quarantine_records(&reader);
    assert_eq!(quarantine.len(), 1);
    assert_eq!(
        quarantine[0].error_code,
        cdf_contract::RESIDUAL_ENCODE_UNSUPPORTED_CODE
    );
    let evolution: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temp.path().join("schema/contract-evolution.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        evolution["residual_decisions"][0]["observed_physical_type"]["kind"],
        "dictionary"
    );
}

#[test]
fn execution_rejects_schema_authority_and_zero_row_output_schema_tampering() {
    let resource =
        MockResource::tier_a(Vec::new()).with_write_disposition(WriteDisposition::Append);
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();

    let mut authority_tamper = plan.clone();
    authority_tamper.schema_authority.effective_schema_hash =
        SchemaHash::new("sha256:forged-authority").unwrap();
    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(
        &authority_tamper,
        &resource,
        temp.path(),
    ))
    .unwrap_err();
    assert!(error.to_string().contains("schema authority"));

    let mut output_tamper = plan;
    let output = &mut output_tamper.output_schema;
    output.fields.pop();
    let forged_schema = Schema::new(
        output
            .fields
            .iter()
            .map(|field| field.to_arrow().unwrap())
            .collect::<Vec<_>>(),
    );
    output.arrow_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&forged_schema).unwrap();
    let temp = TempDir::new().unwrap();
    let error = block_on(execute_to_package(&output_tamper, &resource, temp.path())).unwrap_err();
    assert!(error.to_string().contains("compiled output schema"));
}

#[test]
fn reject_batch_contract_abort_prevents_packaged_manifest() {
    let resource = MockResource::tier_a(sample_batches());
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.verdicts.violation = VerdictAction::RejectBatch;
    policy.rows.rules = vec![RowRule::Domain {
        column: "name".to_owned(),
        allowed: vec!["missing".to_owned()],
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(error.to_string().contains("reject_batch"));
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    assert_ne!(reader.manifest().lifecycle.status, PackageStatus::Packaged);
}

#[test]
fn merge_dedup_keep_last_runs_after_contract_filtering_and_before_normalize() {
    let batches = vec![
        batch_for_partition(
            "batch-dedup-0",
            "part-0",
            vec![1, 2],
            vec!["one-first", "two"],
            vec![true, true],
        ),
        batch_for_partition(
            "batch-dedup-1",
            "part-0",
            vec![1, 3, 1],
            vec!["one-last", "three", "one-invalid"],
            vec![true, true, true],
        ),
    ];
    let resource = MockResource::tier_a(batches);
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![
        RowRule::Domain {
            column: "name".to_owned(),
            allowed: vec![
                "one-first".to_owned(),
                "one-last".to_owned(),
                "two".to_owned(),
                "three".to_owned(),
            ],
        },
        RowRule::Dedup {
            keys: vec!["id".to_owned()],
            keep: DedupKeep::Last,
        },
    ];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    rename_column_program_output(&mut input.validation_program, "name", "customer_name");
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let segment = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&segment[0], "id"), vec![2, 1, 3]);
    assert_eq!(
        batch_strings(&segment, "customer_name"),
        vec!["two", "one-last", "three"]
    );

    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["rule_id"], "row-rule-0001-dedup");
    assert_eq!(summary["keep"], "last");
    assert_eq!(summary["input_rows"], 4);
    assert_eq!(summary["output_rows"], 3);
    assert_eq!(summary["duplicate_key_count"], 1);
    assert_eq!(summary["dropped_row_count"], 1);
    assert_eq!(collect_dedup_dropped_provenance(&reader), vec![(0, 2)]);
    assert!(package_identity_file_paths(&reader).contains(DEDUP_SUMMARY_FILE));
}

#[test]
fn terminal_attestation_enriches_segments_after_package_dedup() {
    let initial_file = FilePosition {
        path: "/tmp/cdf/events.ndjson".to_owned(),
        size_bytes: 42,
        source_generation: Some("local-v1:generation".to_owned()),
        etag: None,
        object_version: None,
        sha256: None,
    };
    let initial_position = SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![initial_file.clone()],
    });
    let mut terminal_file = initial_file;
    terminal_file.sha256 = Some("sha256:terminal-content".to_owned());
    let terminal_position = SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![terminal_file],
    });
    let mut batches = vec![
        batch_for_partition(
            "batch-terminal-0",
            "part-0",
            vec![1, 2],
            vec!["one-first", "two"],
            vec![true, true],
        ),
        batch_for_partition(
            "batch-terminal-1",
            "part-0",
            vec![1, 3],
            vec!["one-last", "three"],
            vec![true, true],
        ),
    ];
    for batch in &mut batches {
        batch.header.source_position = Some(initial_position.clone());
    }
    let resource = MockResource::tier_a(batches)
        .with_completion_attestation(PartitionAttestation::new(terminal_position.clone(), None));
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Last,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert_eq!(output.output.profile.output_rows, 3);
    assert_eq!(output.segment_positions.len(), 1);
    assert_eq!(output.segment_positions[0].partition_ordinal, 0);
    assert_eq!(
        output.segment_positions[0].output_position.as_ref(),
        Some(&terminal_position)
    );
}

#[test]
fn dynamic_schema_quarantine_drains_to_eof_and_commits_terminal_content_identity() {
    let initial_file = FilePosition {
        path: "https://data.example.test/events.ndjson".to_owned(),
        size_bytes: 42,
        source_generation: Some("weak:last-modified".to_owned()),
        etag: None,
        object_version: None,
        sha256: None,
    };
    let initial_position = SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![initial_file.clone()],
    });
    let mut terminal_file = initial_file;
    terminal_file.sha256 = Some("sha256:terminal-content".to_owned());
    let terminal_position = SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![terminal_file],
    });
    let mut batches = vec![
        missing_control_field_batch("schema-quarantine-0", "part-0", vec!["one"]),
        missing_control_field_batch("schema-quarantine-1", "part-0", vec!["two", "three"]),
    ];
    for batch in &mut batches {
        batch.header.source_position = Some(initial_position.clone());
    }
    let resource = MockResource::tier_a(batches)
        .with_schema(sample_schema())
        .with_completion_attestation(PartitionAttestation::new(terminal_position.clone(), None));
    let plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let temp = TempDir::new().unwrap();

    let output = block_on(execute_to_package_with_segment_positions(
        &plan,
        &resource,
        temp.path(),
    ))
    .unwrap();

    assert!(output.output.identity_segments().is_empty());
    assert_eq!(output.output.lineage.input_rows, 3);
    assert_eq!(output.output.terminal_schema_quarantines.len(), 1);
    let processed = output.execution_evidence().processed_observations();
    assert_eq!(processed.len(), 1);
    assert_eq!(
        processed[0].outcome,
        cdf_kernel::ProcessedObservationOutcome::Quarantined
    );
    assert_eq!(processed[0].source_position, terminal_position);
}

#[test]
fn merge_dedup_keep_first_uses_package_order() {
    let batches = vec![
        batch_for_partition(
            "batch-dedup-first-0",
            "part-0",
            vec![1, 2],
            vec!["one-first", "two"],
            vec![true, true],
        ),
        batch_for_partition(
            "batch-dedup-first-1",
            "part-0",
            vec![1, 3],
            vec!["one-last", "three"],
            vec![true, true],
        ),
    ];
    let resource = MockResource::tier_a(batches);
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::First,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 3);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let segment = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&segment[0], "id"), vec![1, 2, 3]);
    assert_eq!(
        batch_strings(&segment, "name"),
        vec!["one-first", "two", "three"]
    );

    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["keep"], "first");
    assert_eq!(summary["input_rows"], 4);
    assert_eq!(summary["output_rows"], 3);
    assert_eq!(summary["duplicate_key_count"], 1);
    assert_eq!(summary["dropped_row_count"], 1);
    assert_eq!(collect_dedup_dropped_provenance(&reader), vec![(2, 0)]);
}

#[test]
fn package_identity_is_invariant_to_source_batch_rechunking() {
    let one = MockResource::tier_a(vec![batch_for_partition(
        "source-page-one",
        "part-0",
        vec![1, 2, 3, 4],
        vec!["one", "two", "three", "four"],
        vec![true; 4],
    )]);
    let many = MockResource::tier_a(vec![
        batch_for_partition("source-page-a", "part-0", vec![1], vec!["one"], vec![true]),
        batch_for_partition(
            "source-page-b",
            "part-0",
            vec![2, 3],
            vec!["two", "three"],
            vec![true; 2],
        ),
        batch_for_partition("source-page-c", "part-0", vec![4], vec!["four"], vec![true]),
    ]);
    let input = plan_input(Vec::new(), None, None, ExecutionExtent::bounded());
    let one_plan = Planner::new().plan_tier_a(&one, input.clone()).unwrap();
    let many_plan = Planner::new().plan_tier_a(&many, input).unwrap();
    assert_eq!(one_plan, many_plan);
    let one_dir = TempDir::new().unwrap();
    let many_dir = TempDir::new().unwrap();
    let one_output = block_on(execute_to_package(&one_plan, &one, one_dir.path())).unwrap();
    let many_output = block_on(execute_to_package(&many_plan, &many, many_dir.path())).unwrap();
    assert_eq!(
        one_output.identity_segments(),
        many_output.identity_segments()
    );
    assert_eq!(one_output.lineage, many_output.lineage);
    assert_eq!(one_output.manifest.identity, many_output.manifest.identity);
    assert_eq!(
        one_output.manifest.package_hash,
        many_output.manifest.package_hash
    );
    assert_eq!(
        one_output.manifest.package_hash,
        "sha256:423853b61a607518d8cd966d9f36599935b6768efab1e989211c8da11fcbfd78"
    );
}

#[test]
fn append_plan_with_compiled_dedup_rule_does_not_change_rows_or_write_summary() {
    let resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-append-dedup",
        "part-0",
        vec![1, 1],
        vec!["one-first", "one-last"],
        vec![true, true],
    )])
    .with_write_disposition(WriteDisposition::Append);
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Last,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&batches[0], "id"), vec![1, 1]);
    assert_eq!(
        batch_strings(&batches, "name"),
        vec!["one-first", "one-last"]
    );
    assert!(reader.read_dedup_summary_json().unwrap().is_none());
}

#[test]
fn append_exact_row_dedup_compiles_and_drops_only_complete_duplicates() {
    let mut resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-append-exact-row-dedup",
        "part-0",
        vec![1, 1, 1],
        vec!["same", "same", "different"],
        vec![true, true, true],
    )])
    .with_write_disposition(WriteDisposition::Append);
    resource.descriptor.deduplication = Some(DeduplicationSpec::ExactRow);
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    input.validation_program = compile_resource_validation_program(
        &ContractPolicy::for_trust(TrustLevel::Governed),
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
        resource.descriptor(),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&batches[0], "id"), vec![1, 1]);
    assert_eq!(batch_strings(&batches, "name"), vec!["same", "different"]);
    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["keep"], "first");
    assert_eq!(summary["input_rows"], 3);
    assert_eq!(summary["output_rows"], 2);
    assert_eq!(summary["dropped_row_count"], 1);
    assert_eq!(summary["version"], 3);
    assert_eq!(summary["provenance_format"], "parquet");
    assert_eq!(summary["provenance_path"], "stats/dedup-dropped/");
    assert_eq!(summary["provenance_shard_row_target"], 65_536);
    assert_eq!(summary["shard_count"], 1);
    assert!(summary.get("dropped_rows").is_none());
    assert!(
        temp.path()
            .join("stats/dedup-dropped/part-00000000000000000001.parquet")
            .is_file()
    );

    let spill_temp = TempDir::new().unwrap();
    let (_, services) =
        StandaloneExecutionHost::default_services_with_spill(64 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let spilled = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        spill_temp.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();

    assert_eq!(spilled.output.manifest.identity, output.manifest.identity);
    assert_eq!(
        spilled.output.manifest.package_hash,
        output.manifest.package_hash
    );
    let spill = services.spill().snapshot();
    assert!(spill.peak_bytes > 0);
    assert_eq!(spill.current_bytes, 0);
    let memory = services.memory().snapshot();
    assert!(memory.peak_bytes > 0);
    assert_eq!(memory.current_bytes, 0);
}

#[test]
fn replace_plan_with_compiled_dedup_rule_does_not_change_rows_or_write_summary() {
    let resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-replace-dedup",
        "part-0",
        vec![1, 1],
        vec!["one-first", "one-last"],
        vec![true, true],
    )])
    .with_write_disposition(WriteDisposition::Replace);
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::First,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 2);
    assert_eq!(output.identity_segments().len(), 1);
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    let batches = read_package_segment(&reader, &output.identity_segments()[0].segment_id);
    assert_eq!(batch_i32s(&batches[0], "id"), vec![1, 1]);
    assert_eq!(
        batch_strings(&batches, "name"),
        vec!["one-first", "one-last"]
    );
    assert!(reader.read_dedup_summary_json().unwrap().is_none());
}

#[test]
fn merge_dedup_fail_aborts_before_package_finalization() {
    let resource = MockResource::tier_a(vec![batch_for_partition(
        "batch-dedup-fail",
        "part-0",
        vec![1, 1],
        vec!["one-first", "one-last"],
        vec![true, true],
    )]);
    let mut input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Fail,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(sample_schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();

    let error = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap_err();

    assert!(error.to_string().contains("keep=fail aborts"));
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    assert_ne!(reader.manifest().lifecycle.status, PackageStatus::Packaged);
    assert!(package_identity_segments(&reader).is_empty());
    assert!(reader.read_dedup_summary_json().unwrap().is_none());
}

#[test]
fn freshness_contract_writes_observed_at_context_when_rule_requires_it() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "updated_at",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    )]));
    let batch = Batch::from_record_batch(
        BatchId::new("freshness-batch").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("part-0").unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0]).with_timezone("UTC")) as ArrayRef,
            ],
        )
        .unwrap(),
    )
    .unwrap();
    let mut resource = MockResource::tier_a(vec![batch]);
    resource.descriptor.primary_key.clear();
    resource.descriptor.merge_key.clear();
    resource.descriptor.write_disposition = WriteDisposition::Append;
    let mut input = plan_input_for_schema(schema, vec![], None, None, ExecutionExtent::bounded());
    let mut policy = ContractPolicy::for_trust(TrustLevel::Governed);
    policy.rows.rules = vec![RowRule::Freshness {
        column: "updated_at".to_owned(),
        max_age_ms: 1,
    }];
    input.validation_program = compile_validation_program(
        &policy,
        &ObservedSchema::from_arrow(resource.schema().as_ref()),
    )
    .unwrap();
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let temp = TempDir::new().unwrap();
    let output = block_on(execute_to_package(&plan, &resource, temp.path())).unwrap();

    assert_eq!(output.profile.output_rows, 0);
    assert!(output.identity_segments().is_empty());
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    assert!(package_identity_file_paths(&reader).contains("plan/contract-evaluation-context.json"));
}

#[test]
fn traced_execution_emits_run_resource_package_and_partition_spans() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let run_id = RunId::new("run-engine-trace-test").unwrap();
    let temp = TempDir::new().unwrap();
    let subscriber = CapturingSubscriber::default();

    let output = tracing::subscriber::with_default(subscriber.clone(), || {
        block_on(execute_to_package_with_run_id(
            &run_id,
            &plan,
            &resource,
            temp.path(),
        ))
    })
    .unwrap();

    assert_eq!(output.profile.output_batches, 1);
    let spans = subscriber.captured_spans();
    let package_span = spans
        .iter()
        .find(|span| span.name == "cdf_engine.package_execution")
        .expect("package execution span is emitted");
    assert_span_fields(
        package_span,
        &[
            ("run_id", "run-engine-trace-test"),
            ("resource_id", "orders"),
            ("package_id", "pkg-engine-test"),
        ],
    );

    let partition_span = spans
        .iter()
        .find(|span| span.name == "cdf_engine.partition_execution")
        .expect("partition execution span is emitted");
    assert_span_fields(
        partition_span,
        &[
            ("run_id", "run-engine-trace-test"),
            ("resource_id", "orders"),
            ("package_id", "pkg-engine-test"),
            ("partition_id", "part-0"),
        ],
    );
}

#[test]
fn traced_execution_preserves_manifest_identity_hash() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let untraced_temp = TempDir::new().unwrap();
    let traced_temp = TempDir::new().unwrap();

    let untraced = block_on(execute_to_package(&plan, &resource, untraced_temp.path())).unwrap();
    let traced = block_on(execute_to_package_with_run_id(
        &RunId::new("run-engine-hash-test").unwrap(),
        &plan,
        &resource,
        traced_temp.path(),
    ))
    .unwrap();

    assert_eq!(traced.manifest.identity, untraced.manifest.identity);
    assert_eq!(traced.manifest.package_hash, untraced.manifest.package_hash);
    assert_eq!(traced.manifest.signature, untraced.manifest.signature);
}

#[test]
fn phase_telemetry_is_additive_and_preserves_manifest_identity() {
    let resource = MockResource::tier_a(sample_batches());
    let input = plan_input(vec![], None, None, ExecutionExtent::bounded());
    let plan = Planner::new().plan_tier_a(&resource, input).unwrap();
    let plain_temp = TempDir::new().unwrap();
    let measured_temp = TempDir::new().unwrap();
    let plain = block_on(execute_to_package(&plan, &resource, plain_temp.path())).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());

    let measured = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        measured_temp.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_phase_metrics(true),
    ))
    .unwrap();

    assert_eq!(measured.output.manifest.identity, plain.manifest.identity);
    assert_eq!(
        measured.output.manifest.package_hash,
        plain.manifest.package_hash
    );
    assert_eq!(measured.output.manifest.signature, plain.manifest.signature);
    assert!(!measured.phase_metrics.is_empty());
    assert!(measured.phase_metrics.iter().all(|metric| {
        metric.status == RunPhaseStatus::Completed
            && metric.duration_ns > 0
            && metric.operations > 0
    }));
    let phases = measured
        .phase_metrics
        .iter()
        .map(|metric| metric.phase)
        .collect::<std::collections::BTreeSet<_>>();
    for phase in [
        RunPhase::Decode,
        RunPhase::ValidationNormalization,
        RunPhase::SegmentEncode,
        RunPhase::PersistHash,
        RunPhase::PackageFinalize,
    ] {
        assert!(phases.contains(&phase), "missing {phase:?}");
    }
}

#[test]
fn parallel_segment_encoding_is_identical_to_inline_canonical_registration() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    for operator in &mut plan.operator_chain {
        if let OperatorNode::PackageSink { segmentation, .. } = operator {
            segmentation.target_rows = 2;
            segmentation.maximum_rows = 2;
            segmentation.microbatch_minimum_rows = 1;
            segmentation.microbatch_maximum_rows = 2;
        }
    }
    let inline_dir = TempDir::new().unwrap();
    let parallel_dir = TempDir::new().unwrap();
    let inline = block_on(execute_to_package(&plan, &resource, inline_dir.path())).unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let parallel = block_on(execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        &resource,
        parallel_dir.path(),
        &pre_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap();

    assert_eq!(parallel.output.manifest.identity, inline.manifest.identity);
    let parallel_segments = parallel.output.identity_segments();
    assert_eq!(
        parallel_segments
            .iter()
            .map(|segment| segment.package_row_ord_start)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    let parallel_reader = cdf_package::PackageReader::open(parallel_dir.path()).unwrap();
    let persisted_ordinals = parallel_reader
        .verified_canonical_segment_stream(services.memory(), 64 * 1024 * 1024)
        .unwrap()
        .flat_map(|segment| {
            segment.unwrap().batches.into_iter().flat_map(|batch| {
                cdf_package_contract::package_row_ord_array(&batch)
                    .unwrap()
                    .values()
                    .to_vec()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(persisted_ordinals, vec![0, 1, 2]);
    assert_eq!(
        parallel.output.identity_segments(),
        inline.identity_segments()
    );
    assert_eq!(parallel.output.lineage, inline.lineage);
    assert_eq!(
        parallel.segment_positions,
        inline
            .identity_segments()
            .iter()
            .map(|segment| {
                EngineSegmentPosition {
                    segment_id: segment.segment_id.clone(),
                    partition_ordinal: 0,
                    output_position: None,
                }
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(services.memory().snapshot().current_bytes, 0);
}

#[test]
fn parallel_segment_frontier_failure_joins_workers_and_prevents_finalization() {
    let resource = MockResource::tier_a(sample_batches());
    let mut plan = Planner::new()
        .plan_tier_a(
            &resource,
            plan_input(vec![], None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    for operator in &mut plan.operator_chain {
        if let OperatorNode::PackageSink { segmentation, .. } = operator {
            segmentation.target_rows = 2;
            segmentation.maximum_rows = 2;
            segmentation.microbatch_minimum_rows = 1;
            segmentation.microbatch_maximum_rows = 2;
        }
    }
    let package_dir = TempDir::new().unwrap();
    let (_, services) = StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let pre_finalize =
        |_builder: &cdf_package::PackageBuilder, _draft: EnginePackageDraft<'_>| Ok(());
    let mut durable_segment =
        |_entry: &SegmentEntry, _payload: DurableSegmentPayload| -> Result<()> {
            Err(cdf_kernel::CdfError::internal(
                "stop at canonical segment frontier",
            ))
        };
    let mut stream_finalize =
        || -> Result<()> { panic!("failed segment frontier must not reach stream finalization") };

    let error = block_on(execute_to_package_with_streaming_hooks(
        &plan,
        &resource,
        package_dir.path(),
        &pre_finalize,
        &mut durable_segment,
        &mut stream_finalize,
        EngineExecutionConfig::default().with_execution_services(services.clone()),
    ))
    .unwrap_err();

    assert!(error.message.contains("canonical segment frontier"));
    assert_eq!(
        cdf_package::PackageReader::open(package_dir.path())
            .unwrap()
            .manifest()
            .lifecycle
            .status,
        PackageStatus::Extracting
    );
    assert_eq!(services.memory().snapshot().current_bytes, 0);
}

#[test]
fn datafusion_table_provider_pushdown_classification_delegates_to_resource() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
    let filters = [
        col("id").gt(lit(1_i32)),
        col("active").eq(lit(true)),
        col("name").not_eq(lit("three")),
        col("id").add(lit(1_i32)).gt(lit(2_i32)),
    ];
    let filter_refs = filters.iter().collect::<Vec<_>>();

    let pushdown = provider.supports_filters_pushdown(&filter_refs).unwrap();

    assert_eq!(resource.negotiate_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        pushdown,
        vec![
            datafusion::logical_expr::TableProviderFilterPushDown::Exact,
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact,
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported,
        ]
    );
    let requests = resource.requests.lock().unwrap();
    assert_eq!(requests[0].filters.len(), 3);
    assert_eq!(requests[0].filters[0].expression, "id > 1");
    assert_eq!(requests[0].filters[1].expression, "active = true");
    assert_eq!(requests[0].filters[2].expression, "name != 'three'");
}

#[test]
fn datafusion_registered_table_executes_with_residuals_and_projection() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = queryable_resource_table_provider(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
    let ctx = SessionContext::new();
    ctx.register_table("orders", provider).unwrap();

    let batches = block_on(async {
        let provider = ctx.table_provider("orders").await.unwrap();
        let projection = vec![1];
        let filters = vec![col("id").gt(lit(1_i32))];
        let plan = provider
            .scan(&ctx.state(), Some(&projection), &filters, None)
            .await
            .unwrap();
        collect_execution_plan_partitions(plan, ctx.task_ctx()).await
    });

    assert_eq!(resource.open_count.load(Ordering::SeqCst), 2);
    assert_eq!(
        batch_strings(&batches, "name"),
        vec!["two", "three", "two", "three"]
    );
    assert_eq!(batches[0].schema().fields().len(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "name");
    let poll_threads = resource.poll_threads.lock().unwrap();
    assert!(!poll_threads.is_empty());
    assert!(
        poll_threads
            .iter()
            .all(|thread| thread.starts_with("cdf-cpu-")),
        "CDF source polling and adaptation bypassed CPU admission: {poll_threads:?}"
    );
}

#[test]
fn datafusion_unsupported_expression_stays_residual() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
    let unsupported = col("id").add(lit(1_i32)).gt(lit(2_i32));
    let filter_refs = vec![&unsupported];
    let pushdown = provider.supports_filters_pushdown(&filter_refs).unwrap();

    assert_eq!(
        pushdown,
        vec![datafusion::logical_expr::TableProviderFilterPushDown::Unsupported]
    );
    let requests = resource.requests.lock().unwrap();
    assert!(requests.iter().all(|request| request.filters.is_empty()));
}

#[test]
fn datafusion_limit_pushdown_is_disabled_for_inexact_filters() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
    let ctx = SessionContext::new();
    let filters = vec![col("active").eq(lit(true))];

    let _plan = block_on(provider.scan(&ctx.state(), None, &filters, Some(1))).unwrap();

    let requests = resource.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].limit, None);
    assert_eq!(requests[1].limit, None);
}

#[test]
fn datafusion_limit_pushdown_remains_enabled_for_exact_filters() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
    let ctx = SessionContext::new();
    let filters = vec![col("id").gt(lit(1_i32))];

    let _plan = block_on(provider.scan(&ctx.state(), None, &filters, Some(1))).unwrap();

    let requests = resource.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].limit, None);
    assert_eq!(requests[1].limit, Some(1));
}

#[test]
fn datafusion_zero_fetch_never_opens_a_source_partition() {
    let resource = Arc::new(DataFusionMockResource::new());
    let provider = QueryableResourceTableProvider::new(
        resource.clone(),
        ScopeKey::Resource,
        datafusion_test_services(),
    );
    let ctx = SessionContext::new();

    let batches = block_on(async {
        let plan = provider
            .scan(&ctx.state(), None, &[], Some(0))
            .await
            .unwrap();
        collect_execution_plan_partitions(plan, ctx.task_ctx()).await
    });

    assert!(batches.is_empty());
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct MockResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    batches: Vec<Batch>,
    partition_count: usize,
    negotiate_count: Arc<AtomicUsize>,
    open_count: Arc<AtomicUsize>,
    batch_poll_count: Arc<AtomicUsize>,
    attest_count: Arc<AtomicUsize>,
    attestation: Option<PartitionAttestation>,
    completion_attestation: Option<PartitionAttestation>,
    attestation_error: Option<String>,
    dynamic_attestation: bool,
    transient_open_failures: Arc<AtomicUsize>,
    transient_stream_failures: Arc<AtomicUsize>,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
    type_policy_allowances: cdf_kernel::TypePolicyAllowances,
    duplicate_observation_identity: bool,
    misroute_batches: bool,
    retry_safety: cdf_kernel::PartitionRetrySafety,
    stall_after_batches: bool,
    tier_a_intent: cdf_kernel::CompiledScanIntent,
    compiled_source_plan: Arc<OnceLock<cdf_runtime::CompiledSourcePlan>>,
    compiled_source_plan_hash: Arc<OnceLock<cdf_kernel::CompiledSourcePlanHash>>,
}

#[derive(Clone)]
struct StalledHeadResource {
    inner: MockResource,
    head_gate: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
    later_polls: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct SkewedMockResource {
    inner: MockResource,
    poll_delays: Arc<Vec<usize>>,
    terminal_failure_partitions: BTreeSet<usize>,
}

#[derive(Clone)]
struct DataFusionMockResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    batches: Vec<Batch>,
    negotiate_count: Arc<AtomicUsize>,
    open_count: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<ScanRequest>>>,
    poll_threads: Arc<Mutex<Vec<String>>>,
}

impl DataFusionMockResource {
    fn new() -> Self {
        Self {
            descriptor: descriptor(),
            schema: sample_schema(),
            batches: sample_batches(),
            negotiate_count: Arc::new(AtomicUsize::new(0)),
            open_count: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(Mutex::new(Vec::new())),
            poll_threads: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl ResourceStream for DataFusionMockResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        unreachable!("DataFusion adapter must use QueryableResource::negotiate")
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.open_count.fetch_add(1, Ordering::SeqCst);
        let exact_filters = partition
            .metadata
            .get("exact_filters")
            .map(|filters| filters.split('\n').map(str::to_owned).collect::<Vec<_>>())
            .unwrap_or_default();
        let batches = self
            .batches
            .iter()
            .filter(|batch| batch.header.partition_id == partition.partition_id)
            .map(|batch| apply_mock_exact_filters(batch.clone(), &exact_filters))
            .collect::<Result<Vec<_>>>();
        let poll_threads = Arc::clone(&self.poll_threads);
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            let mut batches = batches?.into_iter();
            let stream = Box::pin(stream::poll_fn(move |_| {
                poll_threads
                    .lock()
                    .unwrap()
                    .push(std::thread::current().name().unwrap_or_default().to_owned());
                Poll::Ready(batches.next().map(Ok))
            })) as BatchStream;
            Ok(cdf_kernel::PartitionStreamPayload::batches(stream))
        }))
    }
}

impl QueryableResource for DataFusionMockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        static CAPABILITIES: std::sync::OnceLock<ResourceCapabilities> = std::sync::OnceLock::new();
        CAPABILITIES.get_or_init(|| ResourceCapabilities {
            projection: CapabilitySupport::Supported,
            filters: FilterCapabilities {
                default_fidelity: PushdownFidelity::Unsupported,
                supported_operators: vec![">".to_owned(), "=".to_owned(), "!=".to_owned()],
            },
            limits: CapabilitySupport::Supported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![cdf_kernel::ScopeKind::Partition],
            },
            incremental: IncrementalShape::Full,
            replay: cdf_kernel::ReplaySupport::ExactRecordedBatches,
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::Rows,
        })
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.negotiate_count.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());

        let mut pushed_predicates = Vec::new();
        let mut unsupported_predicates = Vec::new();
        for predicate in &request.filters {
            match predicate.expression.as_str() {
                "id > 1" => pushed_predicates.push(cdf_kernel::PushedPredicate {
                    predicate: predicate.clone(),
                    fidelity: PushdownFidelity::Exact,
                }),
                "active = true" => pushed_predicates.push(cdf_kernel::PushedPredicate {
                    predicate: predicate.clone(),
                    fidelity: PushdownFidelity::Inexact,
                }),
                _ => unsupported_predicates.push(predicate.clone()),
            }
        }

        let exact_filters = pushed_predicates
            .iter()
            .filter(|pushed| pushed.fidelity == PushdownFidelity::Exact)
            .map(|pushed| pushed.predicate.expression.clone())
            .collect::<Vec<_>>()
            .join("\n");
        let scan_intent = cdf_kernel::CompiledScanIntent {
            version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
            projection: request.projection.clone(),
            predicates: pushed_predicates.clone(),
            limit: request.limit,
            order_by: Vec::new(),
        };
        scan_intent.validate()?;
        let partitions = ["part-0", "part-1"]
            .into_iter()
            .map(|partition| {
                let partition_id = PartitionId::new(partition)?;
                Ok(PartitionPlan {
                    partition_id: partition_id.clone(),
                    scope: ScopeKey::Partition { partition_id },
                    planned_position: None,
                    start_position: None,
                    scan_intent: scan_intent.clone(),
                    retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
                    metadata: BTreeMap::from([("exact_filters".to_owned(), exact_filters.clone())]),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ScanPlan::new(
            cdf_kernel::PlanId::new(format!(
                "df-plan-{}-{}",
                request.resource_id.as_str(),
                self.negotiate_count.load(Ordering::SeqCst)
            ))?,
            request.clone(),
            PartitionAuthority::Inline(partitions),
            pushed_predicates,
            unsupported_predicates,
            Some(6),
            None,
            DeliveryGuarantee::EffectivelyOncePerKey,
        ))
    }
}

impl MockResource {
    fn tier_a(batches: Vec<Batch>) -> Self {
        Self::new(batches, false)
    }

    fn tier_b(batches: Vec<Batch>) -> Self {
        Self::new(batches, true)
    }

    fn new(batches: Vec<Batch>, tier_b: bool) -> Self {
        let schema = batches
            .first()
            .and_then(Batch::record_batch)
            .map(RecordBatch::schema)
            .unwrap_or_else(sample_schema);
        Self {
            descriptor: descriptor(),
            schema,
            batches,
            partition_count: if tier_b { 2 } else { 1 },
            negotiate_count: Arc::new(AtomicUsize::new(0)),
            open_count: Arc::new(AtomicUsize::new(0)),
            batch_poll_count: Arc::new(AtomicUsize::new(0)),
            attest_count: Arc::new(AtomicUsize::new(0)),
            attestation: None,
            completion_attestation: None,
            attestation_error: None,
            dynamic_attestation: false,
            transient_open_failures: Arc::new(AtomicUsize::new(0)),
            transient_stream_failures: Arc::new(AtomicUsize::new(0)),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
            type_policy_allowances: cdf_kernel::TypePolicyAllowances::default(),
            duplicate_observation_identity: false,
            misroute_batches: false,
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            stall_after_batches: false,
            tier_a_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            compiled_source_plan: Arc::new(OnceLock::new()),
            compiled_source_plan_hash: Arc::new(OnceLock::new()),
        }
    }

    fn with_write_disposition(mut self, write_disposition: WriteDisposition) -> Self {
        self.descriptor.write_disposition = write_disposition;
        self
    }

    fn without_control_keys(mut self) -> Self {
        self.descriptor.primary_key.clear();
        self.descriptor.merge_key.clear();
        self.descriptor.cursor = None;
        self.descriptor.write_disposition = WriteDisposition::Append;
        self
    }

    fn with_partition_count(mut self, partition_count: usize) -> Self {
        self.partition_count = partition_count;
        self
    }

    fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
        self
    }

    fn with_effective_schema_runtime(
        mut self,
        schema: SchemaRef,
        runtime: EffectiveSchemaRuntime,
    ) -> Self {
        let SchemaBaselineReference::Pinned { snapshot } = &runtime.evidence.baseline else {
            panic!("engine effective-schema fixtures require a pinned discovery baseline");
        };
        self.schema = schema;
        self.descriptor.schema_source = SchemaSource::Discovered {
            snapshot: snapshot.clone(),
        };
        self.baseline_observation_schema_catalog = runtime.schema_catalog.clone();
        self.effective_schema_runtime = Some(runtime);
        self
    }

    fn with_baseline_observation_schema_catalog(
        mut self,
        mut catalog: Vec<EffectiveSchemaCatalogEntry>,
    ) -> Self {
        catalog.sort_by(|left, right| left.physical_schema_hash.cmp(&right.physical_schema_hash));
        self.baseline_observation_schema_catalog = catalog;
        self
    }

    fn with_attestation(mut self, attestation: PartitionAttestation) -> Self {
        self.attestation = Some(attestation);
        self
    }

    fn with_dynamic_attestation(mut self) -> Self {
        self.dynamic_attestation = true;
        self
    }

    fn with_completion_attestation(mut self, attestation: PartitionAttestation) -> Self {
        self.completion_attestation = Some(attestation);
        self
    }

    fn with_attestation_error(mut self, error: impl Into<String>) -> Self {
        self.attestation_error = Some(error.into());
        self
    }

    fn with_transient_open_failures(mut self, failures: usize) -> Self {
        self.transient_open_failures
            .store(failures, Ordering::SeqCst);
        self.retry_safety = cdf_kernel::PartitionRetrySafety::ImmutableContent;
        self
    }

    fn with_transient_stream_failures(mut self, failures: usize) -> Self {
        self.transient_stream_failures
            .store(failures, Ordering::SeqCst);
        self.retry_safety = cdf_kernel::PartitionRetrySafety::ImmutableContent;
        self
    }

    fn with_type_policy_allowances(mut self, allowances: cdf_kernel::TypePolicyAllowances) -> Self {
        self.type_policy_allowances = allowances;
        self
    }

    fn with_duplicate_observation_identity(mut self) -> Self {
        self.duplicate_observation_identity = true;
        self
    }

    fn with_misrouted_batches(mut self) -> Self {
        self.misroute_batches = true;
        self
    }

    fn with_tier_a_intent(mut self, intent: cdf_kernel::CompiledScanIntent) -> Self {
        self.tier_a_intent = intent;
        self
    }

    fn with_stall_after_batches(mut self) -> Self {
        self.stall_after_batches = true;
        self
    }

    fn bind_compiled_source(&self, source: &cdf_runtime::CompiledSourcePlan) {
        let hash = source.compiled_source_plan_hash().unwrap();
        match self.compiled_source_plan.set(source.clone()) {
            Ok(()) => {}
            Err(source) => assert_eq!(
                self.compiled_source_plan.get(),
                Some(&source),
                "mock source compiler binding is single-assignment"
            ),
        }
        match self.compiled_source_plan_hash.set(hash) {
            Ok(()) => {}
            Err(hash) => assert_eq!(
                self.compiled_source_plan_hash.get(),
                Some(&hash),
                "mock source hash binding is single-assignment"
            ),
        }
    }
}

impl ResourceStream for StalledHeadResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.inner.descriptor()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
        self.inner.compiled_source_plan_hash()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        self.inner.plan_partitions(request)
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.inner.open_count.fetch_add(1, Ordering::SeqCst);
        let batch = self
            .inner
            .batches
            .iter()
            .find(|batch| batch.header.partition_id == partition.partition_id)
            .cloned()
            .expect("stalled-head fixture covers every partition");
        if partition.partition_id.as_str() == "part-0" {
            let receiver = self
                .head_gate
                .lock()
                .unwrap()
                .take()
                .expect("head gate is single-use");
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
                let stream = stream::once(async move {
                    receiver
                        .await
                        .map_err(|_| cdf_kernel::CdfError::internal("head gate dropped"))?;
                    Ok(batch)
                });
                Ok(cdf_kernel::PartitionStreamPayload::batches(Box::pin(
                    stream,
                )))
            }));
        }
        let later_polls = Arc::clone(&self.later_polls);
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            let stream = stream::iter([Ok(batch)]).inspect(move |_| {
                later_polls.fetch_add(1, Ordering::SeqCst);
            });
            Ok(cdf_kernel::PartitionStreamPayload::batches(Box::pin(
                stream,
            )))
        }))
    }

    fn attest_partition(
        &self,
        partition: PartitionPlan,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.inner.attest_partition(partition)
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.inner.effective_schema_runtime()
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.inner.type_policy_allowances()
    }
}

impl ResourceStream for SkewedMockResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.inner.descriptor()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
        self.inner.compiled_source_plan_hash()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        self.inner.plan_partitions(request)
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.inner.open_count.fetch_add(1, Ordering::SeqCst);
        let ordinal = partition
            .metadata
            .get("ordinal")
            .and_then(|value| value.parse::<usize>().ok())
            .expect("skew fixture partition has an ordinal");
        let mut delay = self.poll_delays[ordinal];
        let mut batches = self
            .inner
            .batches
            .iter()
            .filter(|batch| batch.header.partition_id == partition.partition_id)
            .cloned()
            .map(Ok)
            .collect::<Vec<Result<Batch>>>()
            .into_iter();
        if self.terminal_failure_partitions.contains(&ordinal) {
            batches = vec![Err(cdf_kernel::CdfError::data(format!(
                "skew fixture terminal failure at partition {ordinal}"
            )))]
            .into_iter();
        }
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            let stream = stream::poll_fn(move |context| {
                if delay > 0 {
                    delay -= 1;
                    context.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(batches.next())
            });
            Ok(cdf_kernel::PartitionStreamPayload::batches(Box::pin(
                stream,
            )))
        }))
    }

    fn attest_partition(
        &self,
        partition: PartitionPlan,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.inner.attest_partition(partition)
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.inner.effective_schema_runtime()
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.inner.type_policy_allowances()
    }
}

impl QueryableResource for SkewedMockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.inner.negotiate(request)
    }
}

impl QueryableResource for StalledHeadResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.inner.negotiate(request)
    }
}

impl ResourceStream for MockResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
        self.compiled_source_plan_hash.get()
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        (0..self.partition_count)
            .map(|index| {
                let mut metadata = BTreeMap::from([("ordinal".to_owned(), index.to_string())]);
                if self.duplicate_observation_identity {
                    metadata.insert(
                        PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
                        "duplicate-observation".to_owned(),
                    );
                } else if let Some(runtime) = &self.effective_schema_runtime {
                    let observation_id = runtime
                        .evidence
                        .observations
                        .get(index)
                        .map(|observation| observation.observation_id.clone())
                        .unwrap_or_else(|| format!("unobserved-part-{index}"));
                    metadata.insert(PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(), observation_id);
                }
                if self.effective_schema_runtime.is_some() {
                    metadata.insert(
                        PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
                        schema_observation_binding(&format!("input-{index}")).to_string(),
                    );
                }
                Ok(PartitionPlan {
                    partition_id: PartitionId::new(format!("part-{index}"))?,
                    scope: ScopeKey::Partition {
                        partition_id: PartitionId::new(format!("part-{index}"))?,
                    },
                    planned_position: None,
                    start_position: None,
                    scan_intent: self.tier_a_intent.clone(),
                    retry_safety: self.retry_safety,
                    metadata,
                })
            })
            .collect()
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.open_count.fetch_add(1, Ordering::SeqCst);
        if self
            .transient_open_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
                Err(cdf_kernel::CdfError::transient("mock open unavailable"))
            }));
        }
        let start_position = partition.start_position.clone();
        let batches = self
            .batches
            .iter()
            .filter(|batch| {
                self.misroute_batches || batch.header.partition_id == partition.partition_id
            })
            .filter(|batch| {
                start_position.as_ref().is_none_or(|start| {
                    batch
                        .header
                        .source_position
                        .as_ref()
                        .is_some_and(|position| cursor_position_is_after(position, start))
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let transient_stream_failure = self
            .transient_stream_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        let completion_attestation = self.completion_attestation.clone();
        let batch_poll_count = Arc::clone(&self.batch_poll_count);
        let stall_after_batches = self.stall_after_batches;
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            let stream = if transient_stream_failure {
                Box::pin(stream::iter([Err(cdf_kernel::CdfError::transient(
                    "mock lazy stream unavailable",
                ))])) as BatchStream
            } else {
                let batches = stream::iter(batches.into_iter().map(Ok));
                if stall_after_batches {
                    Box::pin(batches.chain(stream::pending())) as BatchStream
                } else {
                    Box::pin(batches) as BatchStream
                }
            };
            let stream = Box::pin(stream.inspect(move |_| {
                batch_poll_count.fetch_add(1, Ordering::SeqCst);
            })) as BatchStream;
            match completion_attestation {
                Some(attestation) => Ok(cdf_kernel::PartitionStreamPayload::new(
                    stream,
                    Box::pin(async move {
                        Ok(cdf_kernel::PartitionCompletion::new(
                            Some(attestation),
                            None,
                        ))
                    }),
                )),
                None => Ok(cdf_kernel::PartitionStreamPayload::batches(stream)),
            }
        }))
    }

    fn attest_partition(
        &self,
        partition: PartitionPlan,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.attest_count.fetch_add(1, Ordering::SeqCst);
        let attestation = if self.dynamic_attestation {
            self.batches
                .iter()
                .filter(|batch| batch.header.partition_id == partition.partition_id)
                .filter_map(|batch| batch.header.source_position.clone())
                .next_back()
                .map(|position| PartitionAttestation::new(position, None))
        } else {
            self.attestation.clone()
        };
        let error = self.attestation_error.clone();
        cdf_kernel::PartitionAttestationAttempt::materialized(Box::pin(async move {
            if let Some(error) = error {
                return Err(cdf_kernel::CdfError::data(error));
            }
            Ok(attestation)
        }))
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_ref()
    }

    fn baseline_observation_schema_catalog(&self) -> &[EffectiveSchemaCatalogEntry] {
        &self.baseline_observation_schema_catalog
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.type_policy_allowances
    }
}

fn cursor_position_is_after(position: &SourcePosition, start: &SourcePosition) -> bool {
    match (position, start) {
        (SourcePosition::Cursor(position), SourcePosition::Cursor(start))
            if position.field == start.field =>
        {
            match (&position.value, &start.value) {
                (CursorValue::I64(position), CursorValue::I64(start)) => position > start,
                (CursorValue::U64(position), CursorValue::U64(start)) => position > start,
                _ => false,
            }
        }
        _ => false,
    }
}

impl QueryableResource for MockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        static CAPABILITIES: std::sync::OnceLock<ResourceCapabilities> = std::sync::OnceLock::new();
        CAPABILITIES.get_or_init(|| ResourceCapabilities {
            projection: CapabilitySupport::Supported,
            filters: FilterCapabilities {
                default_fidelity: PushdownFidelity::Inexact,
                supported_operators: vec![">".to_owned(), ">=".to_owned(), "=".to_owned()],
            },
            limits: CapabilitySupport::Supported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![cdf_kernel::ScopeKind::Partition],
            },
            incremental: IncrementalShape::Cursor,
            replay: cdf_kernel::ReplaySupport::ExactRecordedBatches,
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::RowsAndBytes,
        })
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.negotiate_count.fetch_add(1, Ordering::SeqCst);
        let mut plan = negotiate_scan_plan(
            request.resource_id.clone(),
            request.clone(),
            self.capabilities(),
            self.plan_partitions(request)?,
            Some(3),
            Some(256),
            DeliveryGuarantee::EffectivelyOncePerKey,
        )?;
        for pushed in &mut plan.pushed_predicates {
            if pushed.predicate.expression == "id > 1"
                || pushed.predicate.expression == "updated_at >= '2026-07-12T00:00:00Z'"
            {
                pushed.fidelity = PushdownFidelity::Exact;
            }
        }
        let pushed_predicates = plan.pushed_predicates.clone();
        for partition in plan.inline_partitions_mut().unwrap() {
            partition.scan_intent.predicates = pushed_predicates.clone();
        }
        Ok(plan)
    }
}

#[derive(Clone, Default)]
struct CapturingSubscriber {
    next_id: Arc<AtomicU64>,
    spans: Arc<Mutex<Vec<CapturedSpan>>>,
}

impl CapturingSubscriber {
    fn captured_spans(&self) -> Vec<CapturedSpan> {
        self.spans.lock().unwrap().clone()
    }
}

impl Subscriber for CapturingSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        self.spans.lock().unwrap().push(CapturedSpan {
            name: attrs.metadata().name().to_owned(),
            fields: visitor.fields,
        });
        Id::from_u64(self.next_id.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, _event: &Event<'_>) {}

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

#[derive(Clone, Debug)]
struct CapturedSpan {
    name: String,
    fields: BTreeMap<String, String>,
}

#[derive(Default)]
struct FieldVisitor {
    fields: BTreeMap<String, String>,
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &TracingField, value: &str) {
        self.fields
            .insert(field.name().to_owned(), value.to_owned());
    }

    fn record_bool(&mut self, field: &TracingField, value: bool) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_i64(&mut self, field: &TracingField, value: i64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_u64(&mut self, field: &TracingField, value: u64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_debug(&mut self, field: &TracingField, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), format!("{value:?}"));
    }
}

fn assert_span_fields(span: &CapturedSpan, expected: &[(&str, &str)]) {
    let expected = expected
        .iter()
        .map(|(field, value)| ((*field).to_owned(), (*value).to_owned()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        span.fields, expected,
        "span {} should record the exact field set",
        span.name
    );
}

fn assert_package_tree_excludes(root: &std::path::Path, sentinels: &[&str]) {
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            assert_package_tree_excludes(&path, sentinels);
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        for sentinel in sentinels {
            assert!(
                !bytes
                    .windows(sentinel.len())
                    .any(|window| window == sentinel.as_bytes()),
                "package artifact {} contains raw sentinel {sentinel:?}",
                path.display()
            );
        }
    }
}

fn assert_honest_cdf_native_operator_metadata(plan: &EnginePlan) {
    let plan_json = serde_json::to_value(plan).unwrap();
    let plan_text = serde_json::to_string(&plan_json).unwrap();
    assert!(!plan_text.contains("data_fusion_table_provider"));
    assert!(!plan_text.contains("data_fusion_scan_exec"));
    assert!(!plan_text.contains("datafusion_table_provider"));

    assert_cdf_native_operator_kinds(&plan_json["operator_chain"]);
    assert_cdf_native_operator_kinds(&plan_json["explain"]["operator_chain"]);
    assert_eq!(
        plan_json["operator_chain"][0]["adapter_kind"],
        "cdf_native_resource_adapter"
    );
    assert_eq!(
        plan_json["explain"]["operator_chain"][0]["adapter_kind"],
        "cdf_native_resource_adapter"
    );
}

fn assert_cdf_native_operator_kinds(operator_chain: &serde_json::Value) {
    let actual = operator_chain
        .as_array()
        .unwrap()
        .iter()
        .map(|operator| operator["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            "cdf_resource_adapter",
            "cdf_native_scan",
            "schema_fingerprint_exec",
            "contract_exec",
            "normalize_exec",
            "profile_exec",
            "lineage_exec",
            "package_sink",
        ]
    );
}

fn assert_explain_carries_required_fields(explain_json: &serde_json::Value) {
    for field in [
        "pushed_predicates",
        "inexact_predicates",
        "unsupported_predicates",
        "partitions",
        "estimates",
        "delivery_guarantee",
        "execution_extent",
    ] {
        assert!(explain_json.get(field).is_some(), "missing {field}");
    }
}

fn batch_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let index = batch.schema().index_of(column).unwrap();
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn batch_i32s(batch: &RecordBatch, column: &str) -> Vec<i32> {
    let index = batch.schema().index_of(column).unwrap();
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    (0..array.len()).map(|row| array.value(row)).collect()
}

async fn collect_execution_plan_partitions(
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    task_ctx: Arc<datafusion::execution::TaskContext>,
) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    for partition in 0..plan.properties().partitioning.partition_count() {
        let stream = plan.execute(partition, Arc::clone(&task_ctx)).unwrap();
        batches.extend(collect_stream(stream).await.unwrap());
    }
    batches
}

fn apply_mock_exact_filters(batch: Batch, filters: &[String]) -> Result<Batch> {
    if filters.is_empty() {
        return Ok(batch);
    }
    let Some(record_batch) = batch.record_batch() else {
        return Ok(batch);
    };
    let mut keep = vec![true; record_batch.num_rows()];
    for filter in filters {
        if filter == "id > 1" {
            let id_index = record_batch.schema().index_of("id").unwrap();
            let ids = record_batch
                .column(id_index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for (row, keep_row) in keep.iter_mut().enumerate().take(ids.len()) {
                *keep_row &= ids.value(row) > 1;
            }
        }
    }
    let filtered =
        arrow_select::filter::filter_record_batch(record_batch, &BooleanArray::from(keep))
            .map_err(cdf_kernel::CdfError::from)?;
    let mut header = batch.header;
    header.set_payload_counts(
        filtered.num_rows() as u64,
        filtered.get_array_memory_size() as u64,
    );
    Ok(Batch {
        header,
        payload: cdf_kernel::BatchPayload::in_memory(filtered),
    })
}

fn plan_input(
    filters: Vec<&str>,
    projection: Option<Vec<String>>,
    limit: Option<u64>,
    execution_extent: ExecutionExtent,
) -> EnginePlanInput {
    plan_input_for_schema(
        sample_schema(),
        filters,
        projection,
        limit,
        execution_extent,
    )
}

fn sample_stream_epoch_policy() -> StreamEpochPolicy {
    StreamEpochPolicy {
        version: STREAM_EPOCH_POLICY_VERSION,
        checkpoint_cadence: EpochClosureTrigger::Rows { count: 5 },
        package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
        watermark: WatermarkPolicy::Disabled,
        late_data: LateDataAction::Quarantine,
        safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
    }
}

fn plan_input_for_schema(
    schema: SchemaRef,
    filters: Vec<&str>,
    projection: Option<Vec<String>>,
    limit: Option<u64>,
    execution_extent: ExecutionExtent,
) -> EnginePlanInput {
    let observed = ObservedSchema::from_arrow(schema.as_ref());
    let validation_program =
        compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Governed), &observed)
            .unwrap();
    EnginePlanInput {
        request: ScanRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            projection,
            filters: filters
                .into_iter()
                .enumerate()
                .map(|(index, expression)| {
                    ScanPredicate::new(PredicateId::new(format!("p{index}")).unwrap(), expression)
                        .unwrap()
                })
                .collect(),
            limit,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        },
        validation_program,
        execution_extent,
        package_id: "pkg-engine-test".to_owned(),
    }
}

fn rename_column_program_output(
    program: &mut cdf_contract::ValidationProgram,
    source_name: &str,
    output_name: &str,
) {
    let column = program
        .column_programs
        .iter_mut()
        .find(|column| column.source_name == source_name)
        .unwrap();
    column.output_name = output_name.to_owned();
}

fn rename_column_program_source(
    program: &mut cdf_contract::ValidationProgram,
    output_name: &str,
    source_name: &str,
) {
    let column = program
        .column_programs
        .iter_mut()
        .find(|column| column.output_name == output_name)
        .unwrap();
    column.source_name = source_name.to_owned();
}

fn coercion_decision<'a>(
    plan: &'a cdf_contract::SchemaCoercionPlan,
    source_name: &str,
) -> &'a cdf_contract::FieldCoercion {
    plan.fields
        .iter()
        .find(|field| field.source_name == source_name)
        .unwrap()
}

fn stream_admission_coercion(package_dir: &std::path::Path) -> cdf_contract::SchemaCoercionPlan {
    let evidence: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &std::fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(evidence.observations.len(), 1);
    evidence
        .observations
        .into_iter()
        .next()
        .unwrap()
        .coercion_plan
}

fn descriptor() -> ResourceDescriptor {
    let schema_hash = SchemaHash::new("schema-v1").unwrap();
    ResourceDescriptor {
        resource_id: ResourceId::new("orders").unwrap(),
        schema_source: SchemaSource::Discovered {
            snapshot: SchemaSnapshotReference {
                schema_hash,
                path: ".cdf/schemas/orders@schema-v1.json".to_owned(),
                metadata: BTreeMap::from([("probe".to_owned(), "engine-test".to_owned())]),
            },
        },
        primary_key: vec!["id".to_owned()],
        merge_key: vec!["id".to_owned()],
        cursor: None,
        write_disposition: WriteDisposition::Merge,
        deduplication: None,
        contract: Some(ContractRef::new("contract-orders").unwrap()),
        state_scope: ScopeKey::Resource,
        freshness: Some(FreshnessSpec { max_age_ms: 60_000 }),
        trust_level: TrustLevel::Governed,
    }
}

fn bound_effective_schema_evidence(
    effective_schema_hash: SchemaHash,
    manifest_hash: &str,
    manifest_path: &str,
    observations: Vec<EffectiveSchemaObservationEvidence>,
) -> EffectiveSchemaEvidence {
    let discovery_manifest = DiscoveryManifestReference {
        manifest_hash: DiscoveryManifestHash::new(manifest_hash).unwrap(),
        path: manifest_path.to_owned(),
    };
    let snapshot = descriptor()
        .schema_source
        .pinned_snapshot()
        .unwrap()
        .clone()
        .with_discovery_manifest(&discovery_manifest)
        .unwrap();
    EffectiveSchemaEvidence::new(
        SchemaBaselineReference::Pinned { snapshot },
        effective_schema_hash,
        discovery_manifest,
        observations,
    )
    .unwrap()
}

fn schema_observation_binding(observation_id: &str) -> cdf_kernel::SchemaObservationBinding {
    cdf_kernel::SchemaObservationBinding::new(
        cdf_runtime::artifact_hash(&("engine-test-schema-observation", observation_id)).unwrap(),
    )
    .unwrap()
}

fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn incompatible_sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn output_name_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn parquet_reconciled_schema() -> SchemaRef {
    Arc::new(parquet_reconciliation().schema)
}

fn parquet_reconciliation() -> cdf_contract::SchemaReconciliation {
    reconcile_schema(
        &Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]),
        &Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]),
        &ContractPolicy::default().types,
    )
    .unwrap()
}

fn sample_batches() -> Vec<Batch> {
    vec![
        batch_for_partition(
            "batch-0",
            "part-0",
            vec![1, 2, 3],
            vec!["one", "two", "three"],
            vec![false, true, true],
        ),
        batch_for_partition(
            "batch-1",
            "part-1",
            vec![1, 2, 3],
            vec!["one", "two", "three"],
            vec![false, true, true],
        ),
    ]
}

fn output_name_batches() -> Vec<Batch> {
    vec![batch_for_partition_with_schema(
        "batch-0",
        "part-0",
        output_name_schema(),
        vec![1, 2, 3],
        vec!["one", "two", "three"],
        vec![false, true, true],
    )]
}

fn parquet_reconciled_batch() -> Batch {
    let reconciliation = parquet_reconciliation();
    let serialized_plan = serde_json::to_string(&reconciliation.plan).unwrap();
    let schema = Arc::new(reconciliation.schema);
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
        ],
    )
    .unwrap();

    Batch {
        header: {
            let mut header = BatchHeader::new(
                BatchId::new("batch-parquet-reconciled").unwrap(),
                ResourceId::new("orders").unwrap(),
                PartitionId::new("part-0").unwrap(),
                cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
                record_batch.num_rows() as u64,
                record_batch.get_array_memory_size() as u64,
            );
            header.schema_coercion_plan = Some(serialized_plan);
            header
                .mark_materialized_output(&Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                ]))
                .unwrap();
            header
        },
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    }
}

fn batch_with_file_position() -> Batch {
    let mut batch = batch_for_partition(
        "batch-file",
        "part-0",
        vec![1, 2],
        vec!["one", "two"],
        vec![true, true],
    );
    batch.header.source_position = Some(SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "/tmp/cdf/events.ndjson".to_owned(),
            size_bytes: 42,
            source_generation: None,
            etag: None,
            object_version: None,
            sha256: Some(
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_owned(),
            ),
        }],
    }));
    batch
}

fn nested_variant_batch() -> Batch {
    let payload = StructArray::from(vec![
        (
            Arc::new(Field::new("kind", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("count", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![7, 9])) as ArrayRef,
        ),
    ]);
    let tags = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        Some(vec![Some(3), None]),
    ]);
    let mut attributes = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    attributes.keys().append_value("tier");
    attributes.values().append_value(1);
    attributes.append(true).unwrap();
    attributes.keys().append_value("score");
    attributes.values().append_value(5);
    attributes.append(true).unwrap();
    let attributes = attributes.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        with_semantic(Field::new("email", DataType::Utf8, false), "pii:email"),
        Field::new("payload", payload.data_type().clone(), true),
        Field::new("tags", tags.data_type().clone(), true),
        Field::new("attributes", attributes.data_type().clone(), true),
    ]));
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ok@example.test", "raw-secret"])) as ArrayRef,
            Arc::new(payload) as ArrayRef,
            Arc::new(tags) as ArrayRef,
            Arc::new(attributes) as ArrayRef,
        ],
    )
    .unwrap();

    Batch {
        header: BatchHeader::new(
            BatchId::new("batch-variant").unwrap(),
            ResourceId::new("orders").unwrap(),
            PartitionId::new("part-0").unwrap(),
            cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
            record_batch.num_rows() as u64,
            record_batch.get_array_memory_size() as u64,
        ),
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    }
}

fn batch_for_partition(
    batch_id: &str,
    partition_id: &str,
    ids: Vec<i32>,
    names: Vec<&str>,
    active: Vec<bool>,
) -> Batch {
    batch_for_partition_with_schema(batch_id, partition_id, sample_schema(), ids, names, active)
}

fn batch_for_partition_with_schema(
    batch_id: &str,
    partition_id: &str,
    schema: SchemaRef,
    ids: Vec<i32>,
    names: Vec<&str>,
    active: Vec<bool>,
) -> Batch {
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(BooleanArray::from(active)) as ArrayRef,
        ],
    )
    .unwrap();

    Batch {
        header: BatchHeader::new(
            BatchId::new(batch_id).unwrap(),
            ResourceId::new("orders").unwrap(),
            PartitionId::new(partition_id).unwrap(),
            cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
            record_batch.num_rows() as u64,
            record_batch.get_array_memory_size() as u64,
        ),
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    }
}

fn missing_control_field_batch(batch_id: &str, partition_id: &str, names: Vec<&str>) -> Batch {
    let row_count = names.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true; row_count])) as ArrayRef,
        ],
    )
    .unwrap();
    Batch {
        header: BatchHeader::new(
            BatchId::new(batch_id).unwrap(),
            ResourceId::new("orders").unwrap(),
            PartitionId::new(partition_id).unwrap(),
            cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
            record_batch.num_rows() as u64,
            record_batch.get_array_memory_size() as u64,
        ),
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    }
}
