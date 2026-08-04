use std::sync::Arc;

use arrow_schema::SchemaRef;
use cdf_kernel::{
    Batch, BatchStream, BoxFuture, CdfError, CompiledScanIntent, PayloadRetention, Result,
    SourcePosition,
};
use cdf_runtime::{
    ByteSource, CanonicalStreamCompletion, CanonicalStreamOpener, DecodePlanningRequest,
    FormatDriver, PhysicalDecodeRequest, ReadOptions, canonical_stream_frontier_with_completion,
    decode_unit_no_lookback_frontiers, resolve_decode_unit_concurrency,
};
use futures_util::TryStreamExt;

#[cfg(test)]
use crate::FileFormatDeclaration;

use super::{
    FileRuntimeDependencies, NATIVE_STREAM_ITEMS, NATIVE_TARGET_BATCH_BYTES,
    NATIVE_TARGET_BATCH_ROWS, NATIVE_UNIT_BUFFERED_BATCHES, NATIVE_UNIT_STREAM_ITEMS,
    input::{
        PhysicalSchemaAuthority, PreparedFileInput, PreparedFilePartition, ReadyFileInput,
        SpoolInputRequest, ready_spooled_file_input,
    },
    validation::{
        per_partition_decode_unit_ceiling, physical_predicates, physical_projection_names,
        stable_decode_memory_budget,
    },
};
#[cfg(test)]
use super::{
    input::{PlannedFileAccessCoverage, PrepareFileInputRequest, prepare_file_input},
    model::ResolvedFileMatch,
};

pub(super) async fn stream_prepared_file_match(
    prepared: PreparedFilePartition,
    dependencies: &FileRuntimeDependencies,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<PreparedFormatStream> {
    let PreparedFilePartition {
        resolved,
        input: prepared,
        scan_intent,
        options,
        admission_schema,
        physical_schema_authority,
        canonical_format_options,
        driver,
        source_io,
        extraction_content_hash: _,
        hash_sweep_source: _,
        payload_retention,
        payload_cache_key,
        spool_mode,
    } = prepared;
    // Weak metadata identifies planned work, not a checkpoint-safe source frontier. Keep batch
    // positions absent until EOF supplies the content digest; the completion attestation then
    // enriches every durable segment through the generic terminal-position path.
    let position = (resolved.source_generation.is_some()
        || resolved.etag.is_some()
        || resolved.version.is_some()
        || resolved.sha256.is_some())
    .then(|| {
        SourcePosition::FileManifest(cdf_kernel::FileManifest {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            files: vec![cdf_kernel::FilePosition {
                path: resolved.path_text.clone(),
                size_bytes: resolved.size_bytes,
                source_generation: resolved.source_generation.clone(),
                etag: resolved.etag.clone(),
                object_version: resolved.version.clone(),
                sha256: resolved.sha256.clone(),
            }],
        })
    });
    let ReadyFileInput {
        source,
        payload_retention,
        source_completion,
        post_decode_completion,
    } = match prepared {
        PreparedFileInput::Source(source) => ReadyFileInput {
            source,
            payload_retention,
            source_completion: None,
            post_decode_completion: None,
        },
        PreparedFileInput::SpoolSource { source, size_bytes } => {
            if payload_retention.is_some() {
                return Err(CdfError::internal(
                    "prepared source payload cannot request a second spool",
                ));
            }
            ready_spooled_file_input(SpoolInputRequest {
                source,
                size_bytes,
                mode: spool_mode,
                source_io,
                payload_cache_key,
                dependencies,
                cancellation,
            })
            .await?
        }
    };

    let batches = stream_registered_format(
        RegisteredFormatStreamRequest {
            source,
            payload_retention,
            driver,
            scan_intent,
            options,
            admission_schema,
            canonical_format_options,
            source_position: position,
            physical_schema_authority,
        },
        dependencies,
    )?;
    Ok(PreparedFormatStream {
        batches,
        source_completion,
        post_decode_completion,
    })
}

pub(super) struct PreparedFormatStream {
    pub(super) batches: BatchStream,
    pub(super) source_completion: Option<BoxFuture<'static, Result<()>>>,
    pub(super) post_decode_completion: Option<BoxFuture<'static, Result<()>>>,
}

#[cfg(test)]
pub(super) fn stream_file_match_blocking(
    resolved: &ResolvedFileMatch,
    declaration: &FileFormatDeclaration,
    options: ReadOptions,
    dependencies: &FileRuntimeDependencies,
    admission_schema: SchemaRef,
    physical_schema_authority: PhysicalSchemaAuthority,
) -> Result<BatchStream> {
    stream_file_match_with_options_blocking(
        resolved,
        declaration,
        serde_json::json!({}),
        options,
        dependencies,
        admission_schema,
        physical_schema_authority,
    )
}

#[cfg(test)]
pub(super) fn stream_file_match_with_options_blocking(
    resolved: &ResolvedFileMatch,
    declaration: &FileFormatDeclaration,
    format_options: serde_json::Value,
    options: ReadOptions,
    dependencies: &FileRuntimeDependencies,
    admission_schema: SchemaRef,
    physical_schema_authority: PhysicalSchemaAuthority,
) -> Result<BatchStream> {
    let driver = dependencies.formats().resolve(declaration.as_str())?;
    let canonical_format_options = driver.canonical_options(format_options)?;
    let prepared_input = prepare_file_input(PrepareFileInputRequest {
        resource_id: &options.resource_id,
        resolved,
        source_access: driver.descriptor().source_access,
        access_coverage: PlannedFileAccessCoverage::Full,
        driver: driver.as_ref(),
        canonical_format_options: &canonical_format_options,
        dependencies,
        cancellation: &cdf_runtime::RunCancellation::default(),
    })?;
    let prepared = PreparedFilePartition {
        resolved: resolved.clone(),
        input: prepared_input.input,
        scan_intent: CompiledScanIntent::full_scan(),
        options,
        admission_schema,
        physical_schema_authority,
        canonical_format_options,
        driver,
        source_io: prepared_input.source_io,
        extraction_content_hash: prepared_input.extraction_content_hash,
        hash_sweep_source: prepared_input.hash_sweep_source,
        payload_retention: prepared_input.payload_retention,
        payload_cache_key: prepared_input.payload_cache_key,
        spool_mode: crate::FileSpoolMode::Overlap,
    };
    let dependencies = dependencies.clone();
    let execution = dependencies.execution().clone();
    let prepared_stream = execution.run_io(async move {
        stream_prepared_file_match(
            prepared,
            &dependencies,
            cdf_runtime::RunCancellation::default(),
        )
        .await
    })?;
    let PreparedFormatStream {
        mut batches,
        source_completion,
        post_decode_completion,
    } = prepared_stream;
    if source_completion.is_none() && post_decode_completion.is_none() {
        return Ok(batches);
    }
    let stream = execution.spawn_io_stream(
        "file-test-spool-completion",
        NATIVE_STREAM_ITEMS,
        move |mut sender, _| async move {
            let forward = async {
                while let Some(batch) = batches.try_next().await? {
                    sender.send(batch).await?;
                }
                Ok::<_, CdfError>(())
            };
            if let Some(source_completion) = source_completion {
                tokio::try_join!(forward, source_completion)?;
            } else {
                forward.await?;
            }
            if let Some(post_decode_completion) = post_decode_completion {
                post_decode_completion.await?;
            }
            Ok(())
        },
    )?;
    Ok(Box::pin(stream))
}

pub(super) struct RegisteredFormatStreamRequest {
    pub(super) source: Arc<dyn ByteSource>,
    pub(super) payload_retention: Option<PayloadRetention>,
    pub(super) driver: Arc<dyn FormatDriver>,
    pub(super) scan_intent: CompiledScanIntent,
    pub(super) options: ReadOptions,
    pub(super) admission_schema: SchemaRef,
    pub(super) canonical_format_options: serde_json::Value,
    pub(super) source_position: Option<SourcePosition>,
    pub(super) physical_schema_authority: PhysicalSchemaAuthority,
}

pub(super) fn stream_registered_format(
    request: RegisteredFormatStreamRequest,
    dependencies: &FileRuntimeDependencies,
) -> Result<BatchStream> {
    let RegisteredFormatStreamRequest {
        source,
        payload_retention,
        driver,
        scan_intent,
        options,
        admission_schema,
        canonical_format_options,
        source_position,
        physical_schema_authority,
    } = request;
    let execution = dependencies.execution().clone();
    let memory = execution.memory();
    let scope_id = format!(
        "format-{}-{}",
        driver.descriptor().format_id,
        options.batch_id_prefix
    );
    let unit_execution = execution.clone();
    let unit_scope_prefix = scope_id.clone();
    scan_intent.validate()?;
    let stream = execution.spawn_io_stream(
        &scope_id,
        NATIVE_STREAM_ITEMS,
        move |mut sender, cancellation| async move {
            let _payload_retention = payload_retention;
            let options_json = driver.canonical_options(canonical_format_options)?;
            let decode_cpu = driver.descriptor().decode_cpu.clone();
            let projection = physical_projection_names(
                admission_schema.as_ref(),
                scan_intent.projection.as_deref(),
            )?;
            let predicates = physical_predicates(
                admission_schema.as_ref(),
                &scan_intent.pushed_predicates(),
            )?;
            let decode_schema = match physical_schema_authority.schema {
                Some(schema) => {
                    let schema_hash =
                        cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?;
                    if let Some(planned_hash) = &physical_schema_authority.hash
                        && planned_hash != &schema_hash
                    {
                        return Err(CdfError::data(format!(
                            "plan physical schema catalog hash {schema_hash} does not match partition authority {planned_hash}"
                        )));
                    }
                    cdf_runtime::DecodeSchemaPlan::verified_physical(schema)
                }
                None => cdf_runtime::DecodeSchemaPlan::fixed_admission(admission_schema),
            };
            let session = driver
                .prepare_decode(
                    source.clone(),
                    DecodePlanningRequest {
                        options: options_json.clone(),
                        projection: projection.clone(),
                        predicates: predicates.clone(),
                        target_batch_rows: NATIVE_TARGET_BATCH_ROWS,
                        target_batch_bytes: NATIVE_TARGET_BATCH_BYTES,
                        cancellation: cancellation.clone(),
                    },
                )
                .await?;
            let units = session.units().to_vec();
            if units.is_empty() {
                return Err(CdfError::contract(
                    "prepared format session must contain at least one decode unit",
                ));
            }
            let no_lookback_frontiers = decode_unit_no_lookback_frontiers(&units)?;
            // Decode-unit width is stable plan tuning. Live allocations may temporarily consume
            // the entire ledger while sibling partitions or downstream stages make progress;
            // each decoder's cancellable reservation already waits on that shared coordinator.
            // Using instantaneous free bytes here turns normal backpressure into a nondeterministic
            // planning failure and makes concurrency depend on scheduling order.
            let managed_memory_budget = stable_decode_memory_budget(memory.as_ref());
            let unit_jobs = usize::from(resolve_decode_unit_concurrency(
                &units,
                &unit_execution.capabilities(),
                &decode_cpu,
                managed_memory_budget,
                source.capabilities().useful_range_concurrency.max(1),
                NATIVE_TARGET_BATCH_BYTES,
                NATIVE_UNIT_BUFFERED_BATCHES,
            )?
            .jobs)
            .min(per_partition_decode_unit_ceiling(
                unit_execution.capabilities().logical_cpu_slots,
                unit_execution.run_job_ceiling()?,
            ));

            let units = Arc::new(units);
            let unit_count = units.len();
            let opener_session = Arc::clone(&session);
            let opener_units = Arc::clone(&units);
            let opener_execution = unit_execution.clone();
            let opener_memory = Arc::clone(&memory);
            let opener_options = options.clone();
            let opener_schema = decode_schema.clone();
            let opener_position = source_position.clone();
            let opener_projection = projection.clone();
            let opener_predicates = predicates.clone();
            let opener_scope_prefix = unit_scope_prefix.clone();
            let opener_cpu = decode_cpu;
            let opener: CanonicalStreamOpener<Batch> = Box::new(move |ordinal| {
                let unit = opener_units.get(ordinal).cloned().ok_or_else(|| {
                    CdfError::internal("decode-unit frontier ordinal is outside its session")
                })?;
                let session = Arc::clone(&opener_session);
                let memory = Arc::clone(&opener_memory);
                let options = opener_options.clone();
                let schema = opener_schema.clone();
                let source_position = opener_position.clone();
                let projection = opener_projection.clone();
                let predicates = opener_predicates.clone();
                let work_execution = opener_execution.clone();
                let cpu = opener_cpu.clone();
                let unit_stream = opener_execution.spawn_cpu_stream(
                    &format!("{opener_scope_prefix}-unit-{ordinal:08}"),
                    cpu,
                    NATIVE_UNIT_STREAM_ITEMS,
                    move |mut unit_sender, unit_cancellation| async move {
                        let mut decoded = {
                            let _work = work_execution
                                .acquire_run_work(unit_cancellation.clone())
                                .await?;
                            session
                                .decode(PhysicalDecodeRequest {
                                    unit,
                                    resource_id: options.resource_id,
                                    partition_id: options.partition_id,
                                    batch_id_prefix: options.batch_id_prefix,
                                    schema,
                                    source_position,
                                    projection,
                                    predicates,
                                    target_batch_rows: NATIVE_TARGET_BATCH_ROWS,
                                    target_batch_bytes: NATIVE_TARGET_BATCH_BYTES,
                                    memory,
                                    cancellation: unit_cancellation.clone(),
                                })
                                .await?
                        };
                        loop {
                            let next = {
                                let _work = work_execution
                                    .acquire_run_work(unit_cancellation.clone())
                                    .await?;
                                decoded.try_next().await?
                            };
                            let Some(batch) = next else {
                                break;
                            };
                            // A run-work permit owns active leaf computation, not bounded-channel
                            // residence. Releasing it before publication prevents a later
                            // canonical unit from monopolizing every run slot while its output is
                            // intentionally held behind an earlier partition.
                            unit_sender.send(batch.into_batch()?).await?;
                        }
                        Ok(())
                    },
                )?;
                Ok(Box::pin(unit_stream))
            });
            let release_source = Arc::clone(&source);
            let release_frontiers = no_lookback_frontiers;
            let completion: CanonicalStreamCompletion = Box::new(move |ordinal| {
                if let Some(frontiers) = &release_frontiers {
                    let frontier = frontiers.get(ordinal).copied().ok_or_else(|| {
                        CdfError::internal("decode-unit release frontier ordinal is outside its session")
                    })?;
                    release_source.release_before(frontier)?;
                }
                Ok(())
            });
            let mut decoded = canonical_stream_frontier_with_completion(
                unit_count,
                unit_jobs,
                opener,
                completion,
            )?;
            while let Some(batch) = decoded.try_next().await? {
                cancellation.check()?;
                sender.send(batch).await?;
            }
            if let Some(size_bytes) = source.identity().size_bytes {
                source.release_before(size_bytes)?;
            }
            Ok(())
        },
    )?;
    Ok(Box::pin(stream))
}
