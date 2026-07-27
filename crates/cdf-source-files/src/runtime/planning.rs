use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use cdf_kernel::{
    BatchStream, CdfError, EffectiveSchemaRuntime, PLAN_SCHEMA_OBSERVATION_ID_KEY,
    PartitionAttestation, PartitionCompletion, PartitionPlan, PayloadRetention, ResourceDescriptor,
    ResourceId, Result, SourcePosition,
};
use cdf_object_access::FileTransportControl;
use cdf_runtime::{
    ByteSource, CompiledFormatBinding, GenerationStrength, ReadOptions, SequentialReadRequest,
    SourceIoObserver,
};
use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};

use crate::FileResourcePlan;

use super::{
    FILE_SOURCE_BLOCKING_LANE_ID, FileResource, FileRuntimeDependencies, NATIVE_STREAM_ITEMS,
    decode::{PreparedFormatStream, stream_prepared_file_match},
    file_source_blocking_lane,
    input::{
        PhysicalSchemaAuthority, PrepareFileInputRequest, PreparedFilePartition,
        planned_file_access_coverage, prepare_file_input,
    },
    resolution::{
        FilePlanningContext, FileResolutionContext, no_file_matches_error,
        resolve_file_matches_bounded,
    },
    task::{FileInventoryTaskBuilder, PlannedFileInventory},
    validation::{validate_partition, validate_partition_plan_shape},
};
#[cfg(test)]
use super::{model::FileInventoryRecord, validation::partition_for_file_record};
#[cfg(test)]
use cdf_kernel::CompiledScanIntent;

pub(super) struct FilePartitionPreparation<'a> {
    admission_schema: SchemaRef,
    dependencies: &'a FileRuntimeDependencies,
    effective_schema_runtime: Option<&'a EffectiveSchemaRuntime>,
    compiled_format: &'a CompiledFormatBinding,
    control: &'a FileTransportControl,
}

pub(super) fn planned_physical_schema_authority(
    runtime: Option<&EffectiveSchemaRuntime>,
    partition: &PartitionPlan,
) -> Result<PhysicalSchemaAuthority> {
    let Some(runtime) = runtime else {
        return Ok(PhysicalSchemaAuthority::default());
    };
    let Some(observation_id) = partition.metadata.get(PLAN_SCHEMA_OBSERVATION_ID_KEY) else {
        return Ok(PhysicalSchemaAuthority::default());
    };
    let Some(observation) = runtime.evidence.observation(observation_id) else {
        return Ok(PhysicalSchemaAuthority::default());
    };
    let binding = cdf_kernel::partition_schema_observation_binding(partition)?;
    if binding != observation.schema_observation_binding {
        return Err(CdfError::contract(format!(
            "file partition `{}` schema-observation binding does not match observation {observation_id:?}",
            partition.partition_id
        )));
    }
    let hash = observation.physical_schema_hash.clone();
    Ok(PhysicalSchemaAuthority {
        schema: runtime.physical_schema(&hash).cloned(),
        hash: Some(hash),
    })
}

#[cfg(test)]
pub(super) fn file_partitions_for_plan_with_transport(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    scan_intent: &CompiledScanIntent,
    context: FilePlanningContext<'_>,
) -> Result<Vec<PartitionPlan>> {
    let matches = resolve_file_matches_bounded(&descriptor.resource_id, plan, context, Vec::new())?;
    if matches.is_empty() {
        return Err(no_file_matches_error(&descriptor.resource_id, plan));
    }

    let total_matches = matches.len();
    matches
        .iter()
        .map(|file| {
            partition_for_file_record(
                descriptor,
                plan,
                scan_intent,
                &FileInventoryRecord::from(file),
                u64::try_from(total_matches)
                    .map_err(|_| CdfError::data("file match count exceeds u64"))?,
            )
        })
        .collect()
}

pub(super) fn build_file_inventory_with_transport(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    context: FilePlanningContext<'_>,
    mut builder: FileInventoryTaskBuilder,
) -> Result<PlannedFileInventory> {
    builder = resolve_file_matches_bounded(resource_id, plan, context, builder)?;
    if builder.task_count() == 0 {
        return Err(no_file_matches_error(resource_id, plan));
    }
    builder.finalize()
}

pub(super) fn open_file_resource_with_dependencies(
    resource: FileResource,
    partition: PartitionPlan,
    task_retention: Option<PayloadRetention>,
) -> cdf_kernel::PartitionOpenAttempt<'static> {
    let FileResource {
        descriptor,
        schema,
        capabilities: _,
        plan,
        type_policy_allowances: _,
        effective_schema_runtime,
        baseline_observation_schema_catalog: _,
        compiled_format,
        dependencies,
        prepared_inventory_key: _,
        source_discovery_binding_hash: _,
        compiled_source_plan_hash: _,
        transport_control: _,
    } = resource;
    if let Err(error) = validate_partition_plan_shape(&descriptor, &plan, &partition) {
        return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move { Err(error) }));
    }
    let execution = dependencies.execution().clone();
    if let Err(error) = execution.ensure_blocking_lanes(&[file_source_blocking_lane()]) {
        return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move { Err(error) }));
    }
    let (completion_sender, completion_receiver) = tokio::sync::oneshot::channel();
    let source_io_observer = Arc::new(Mutex::new(None::<SourceIoObserver>));
    let prepared_source_io_observer = Arc::clone(&source_io_observer);
    let mut scope_hasher = Sha256::new();
    scope_hasher.update(descriptor.resource_id.as_str().as_bytes());
    scope_hasher.update([0]);
    scope_hasher.update(partition.partition_id.as_str().as_bytes());
    let scope_id = format!("file-open-{}", &hex::encode(scope_hasher.finalize())[..16]);
    let prepare_dependencies = dependencies.clone();
    let stream_dependencies = dependencies;
    let stream = execution.spawn_blocking_prepared_io_stream(
        &scope_id,
        FILE_SOURCE_BLOCKING_LANE_ID,
        NATIVE_STREAM_ITEMS,
        move |cancellation| {
            cancellation.check()?;
            let control = FileTransportControl::new(cancellation.clone(), None);
            let prepared = prepare_file_partition(
                &descriptor,
                &plan,
                &partition,
                FilePartitionPreparation {
                    admission_schema: schema,
                    dependencies: &prepare_dependencies,
                    effective_schema_runtime: effective_schema_runtime.as_deref(),
                    compiled_format: &compiled_format,
                    control: &control,
                },
            )?;
            *prepared_source_io_observer.lock().map_err(|_| {
                CdfError::internal("file source I/O observation state was poisoned")
            })? = Some(prepared.source_io.clone());
            cancellation.check()?;
            Ok((prepared, task_retention))
        },
        move |(prepared, task_retention), mut sender, cancellation| async move {
            let _task_retention = task_retention;
            let source_io = prepared.source_io.clone();
            let extraction_content_hash = prepared.extraction_content_hash.clone();
            let hash_sweep_source = prepared.hash_sweep_source.clone();
            let completed_position = cdf_kernel::FilePosition {
                path: prepared.resolved.path_text.clone(),
                size_bytes: prepared.resolved.size_bytes,
                source_generation: prepared.resolved.source_generation.clone(),
                etag: prepared.resolved.etag.clone(),
                object_version: prepared.resolved.version.clone(),
                sha256: prepared.resolved.sha256.clone(),
            };
            let planned_physical_schema_hash = prepared.physical_schema_authority.hash.clone();
            let strong_generation = prepared.resolved.identity_strength != GenerationStrength::Weak;
            let decode = async {
                let prepared_stream = stream_prepared_file_match(
                    prepared,
                    &stream_dependencies,
                    cancellation.clone(),
                )
                .await?;
                let PreparedFormatStream {
                    mut batches,
                    source_completion,
                    post_decode_completion,
                } = prepared_stream;
                let forward = async {
                    let mut observed_schema_hash = None;
                    while let Some(batch) = batches.try_next().await? {
                        if observed_schema_hash.is_none() {
                            observed_schema_hash = Some(batch.header.observed_schema_hash.clone());
                        }
                        sender.send(batch).await?;
                    }
                    Ok::<_, CdfError>(observed_schema_hash)
                };
                let observed_schema_hash = if let Some(source_completion) = source_completion {
                    let (observed_schema_hash, ()) = tokio::try_join!(forward, source_completion)?;
                    observed_schema_hash
                } else {
                    forward.await?
                };
                if let Some(post_decode_completion) = post_decode_completion {
                    post_decode_completion.await?;
                }
                Ok::<_, CdfError>(observed_schema_hash)
            };
            let hash_sweep = complete_hash_sweep(hash_sweep_source, cancellation.clone());
            let (observed_schema_hash, ()) = tokio::try_join!(decode, hash_sweep)?;
            let mut completed_position = completed_position;
            if let Some(extraction_content_hash) = extraction_content_hash {
                completed_position.sha256 = Some(extraction_content_hash.completed()?);
            }
            let attestation = Some(PartitionAttestation::new(
                SourcePosition::FileManifest(cdf_kernel::FileManifest {
                    version: 1,
                    files: vec![completed_position],
                }),
                observed_schema_hash.or_else(|| {
                    strong_generation
                        .then_some(planned_physical_schema_hash)
                        .flatten()
                }),
            ));
            let completion = PartitionCompletion::new(attestation, Some(source_io.snapshot()));
            let _ = completion_sender.send(completion);
            Ok(())
        },
    );
    let stream = match stream {
        Ok(stream) => stream,
        Err(error) => {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(
                async move { Err(error) },
            ));
        }
    };
    let termination = stream.termination();
    let opening = Box::pin(async move {
        let stream = Box::pin(stream) as BatchStream;
        let completion = Box::pin(async move {
            completion_receiver.await.map_err(|_| {
                CdfError::internal(
                    "partition stream ended without publishing its invocation completion",
                )
            })
        });
        let snapshot = Arc::new(move || {
            source_io_observer
                .lock()
                .ok()
                .and_then(|observer| observer.as_ref().map(SourceIoObserver::snapshot))
                .unwrap_or_default()
        });
        Ok(cdf_kernel::PartitionStreamPayload::new(stream, completion)
            .with_source_io_snapshot(snapshot))
    });
    cdf_kernel::PartitionOpenAttempt::with_termination(opening, termination)
}

pub(super) async fn complete_hash_sweep(
    source: Option<Arc<dyn ByteSource>>,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<()> {
    let Some(source) = source else {
        return Ok(());
    };
    let preferred_chunk_bytes = (4 * 1024 * 1024_u64).clamp(
        source.capabilities().minimum_chunk_bytes,
        source.capabilities().maximum_chunk_bytes,
    );
    let mut stream = source
        .open_sequential(SequentialReadRequest {
            preferred_chunk_bytes,
            cancellation: cancellation.clone(),
        })
        .await?;
    while stream.try_next().await?.is_some() {
        cancellation.check()?;
    }
    Ok(())
}

pub(super) fn prepare_file_partition(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    partition: &PartitionPlan,
    preparation: FilePartitionPreparation<'_>,
) -> Result<PreparedFilePartition> {
    partition.scan_intent.validate()?;
    let resolved = preparation
        .dependencies
        .with_transport(|transport, egress| {
            validate_partition(
                descriptor,
                plan,
                partition,
                FileResolutionContext {
                    transport,
                    egress,
                    formats: preparation.dependencies.formats(),
                    transforms: preparation.dependencies.transforms(),
                    control: preparation.control,
                },
            )
        })?;
    let physical_schema_authority =
        planned_physical_schema_authority(preparation.effective_schema_runtime, partition)?;
    let options = ReadOptions::new(
        descriptor.resource_id.clone(),
        partition.partition_id.clone(),
    );
    let driver = preparation
        .compiled_format
        .verify(preparation.dependencies.formats())?;
    let source_access = driver.descriptor().source_access;
    let access_coverage =
        planned_file_access_coverage(&partition.scan_intent, &preparation.admission_schema);
    let prepared_input = prepare_file_input(PrepareFileInputRequest {
        resource_id: &descriptor.resource_id,
        resolved: &resolved,
        source_access,
        access_coverage,
        driver: driver.as_ref(),
        canonical_format_options: &preparation.compiled_format.canonical_options,
        dependencies: preparation.dependencies,
        cancellation: &preparation.control.cancellation(),
    })?;
    Ok(PreparedFilePartition {
        resolved,
        input: prepared_input.input,
        scan_intent: partition.scan_intent.clone(),
        options,
        admission_schema: preparation.admission_schema,
        physical_schema_authority,
        canonical_format_options: preparation.compiled_format.canonical_options.clone(),
        driver,
        source_io: prepared_input.source_io,
        extraction_content_hash: prepared_input.extraction_content_hash,
        hash_sweep_source: prepared_input.hash_sweep_source,
        payload_retention: prepared_input.payload_retention,
        payload_cache_key: prepared_input.payload_cache_key,
        spool_mode: plan.spool_mode,
    })
}
