use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, mpsc::sync_channel},
};

use apache_avro::{Reader as AvroReader, from_value as from_avro_value};
use cdf_kernel::{
    CdfError, CompiledScanIntent, DeliveryGuarantee, PlanId, Result, ScanPlan, ScanRequest,
    WriteDisposition,
};
use cdf_memory::{AccountedBytes, MemoryLease};
use cdf_runtime::artifact_hash;
use cdf_task_store::{ExternalTaskSetWriter, ExternalTaskStore, TaskSetLimits};
use iceberg::spec::{
    DEFAULT_SCHEMA_NAME_MAPPING, DataContentType, DataFileFormat, FormatVersion, Manifest,
    ManifestContentType, ManifestStatus,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{
    ICEBERG_SCAN_TASK_VERSION, ICEBERG_SOURCE_BLOCKING_LANE_ID, ICEBERG_SOURCE_DRIVER_VERSION,
    ICEBERG_TASK_SET_AUTHORITY_VERSION, ICEBERG_TASK_SET_TYPE, IcebergDataFile,
    IcebergDeleteContent, IcebergDeleteFile, IcebergFileFormat, IcebergJsonAuthority,
    IcebergReaderRequirements, IcebergScanTask, IcebergSourceOptions, IcebergTaskSetAuthority,
    LoadedIcebergTable, ValidatedIcebergTaskSetAuthority,
    catalog::{load_catalog_object, reserve_parse_memory},
    planning_index::{
        IcebergPlanningIndex, IcebergPlanningManifest, IcebergPlanningManifestReader,
    },
};

pub(crate) struct IcebergPlanningContext<'a> {
    pub catalog: &'a crate::IcebergCatalogContext,
    pub task_store: &'a ExternalTaskStore,
    pub cancellation: cdf_runtime::RunCancellation,
}

pub(crate) fn plan_snapshot_scan(
    descriptor: &cdf_kernel::ResourceDescriptor,
    source: &IcebergSourceOptions,
    table: &LoadedIcebergTable,
    request: &ScanRequest,
    context: IcebergPlanningContext<'_>,
) -> Result<ScanPlan> {
    if request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "scan request resource `{}` does not match compiled Iceberg resource `{}`",
            request.resource_id, descriptor.resource_id
        )));
    }
    request
        .filters
        .iter()
        .try_for_each(|predicate| predicate.canonical_expression.validate())?;
    let output_schema_id = table.selected.as_ref().map_or_else(
        || table.metadata.current_schema_id(),
        |selected| selected.schema_id,
    );
    let output_schema = table
        .metadata
        .schema_by_id(output_schema_id)
        .ok_or_else(|| {
            CdfError::data(format!(
                "Iceberg output schema id {output_schema_id} is absent from table metadata"
            ))
        })?;
    let projected_field_ids = projected_field_ids(output_schema, request.projection.as_deref())?;
    let scan_intent = CompiledScanIntent {
        version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
        projection: request.projection.clone(),
        predicates: Vec::new(),
        limit: None,
        order_by: Vec::new(),
    };
    let mut planning_index = None;
    if let Some(selected) = &table.selected {
        let manifest_list = load_catalog_object(
            context.catalog,
            source,
            &selected.manifest_list,
            None,
            context.cancellation.clone(),
        )?;
        let manifest_list_parse = reserve_parse_memory(
            context.catalog.execution.memory(),
            u64::try_from(manifest_list.payload.payload().len()).unwrap_or(u64::MAX),
            source.metadata_parse_amplification_bps,
            "iceberg-manifest-list-parse",
        )?;
        let mut index = IcebergPlanningIndex::create(
            context.task_store,
            source,
            context.catalog.execution.memory(),
            context.catalog.execution.spill(),
        )?;
        parse_manifest_list(
            manifest_list.payload.payload(),
            table.metadata.format_version(),
            source.maximum_metadata_files,
            &mut index,
            &context.cancellation,
        )?;
        drop(manifest_list_parse);
        planning_index = Some(index);
    }
    let has_deletes = planning_index
        .as_ref()
        .is_some_and(|index| index.manifest_count(ManifestContentType::Deletes) > 0);
    let mut required_authority = RequiredTaskAuthority {
        schema_ids: BTreeSet::from([output_schema_id]),
        partition_spec_ids: BTreeSet::from([table.metadata.default_partition_spec().spec_id()]),
        sort_order_ids: BTreeSet::from([table.metadata.default_sort_order_id()]),
    };
    let spill = context.catalog.execution.spill();
    let mut writer = context.task_store.writer(
        ICEBERG_TASK_SET_TYPE,
        TaskSetLimits {
            maximum_task_bytes: source.maximum_task_bytes,
            maximum_authority_bytes: source.maximum_task_authority_bytes,
            writer_buffer_bytes: source.task_writer_buffer_bytes,
        },
        context.catalog.execution.memory(),
        spill.as_ref(),
    )?;

    let mut estimated_rows = 0_u64;
    let mut estimated_bytes = 0_u64;
    if let Some(mut index) = planning_index {
        if has_deletes {
            let mut manifests = index.manifest_cursor(ManifestContentType::Deletes)?;
            process_manifests_canonical(
                manifests.manifest_count(),
                &mut manifests,
                source,
                context.catalog,
                context.cancellation.clone(),
                |work| emit_delete_manifest(work, table, &mut index),
            )?;
        }
        let mut task_ordinal = 0_u64;
        let manifests = index.manifest_reader(ManifestContentType::Data)?;
        process_indexed_manifests(
            manifests,
            source,
            context.catalog,
            context.cancellation.clone(),
            |work| {
                emit_manifest_tasks(
                    work,
                    table,
                    &mut writer,
                    &mut required_authority,
                    &mut task_ordinal,
                    &mut estimated_rows,
                    &mut estimated_bytes,
                    has_deletes.then_some(&index),
                    source.maximum_task_bytes,
                )
            },
        )?;
    }
    let authority = task_authority(
        table,
        output_schema_id,
        projected_field_ids,
        scan_intent,
        has_deletes,
        &required_authority,
    )?;
    let artifact = writer.finalize(|output| authority.encode_to(output))?;
    if artifact.authority_sha256 != authority.content_sha256() {
        return Err(CdfError::internal(
            "Iceberg task-store authority hash does not match its canonical model",
        ));
    }
    let reference = artifact.reference;
    reference.validate()?;
    Ok(ScanPlan {
        plan_id: PlanId::new(format!("plan-{}", descriptor.resource_id))?,
        request: request.clone(),
        partitions: Vec::new(),
        planned_task_set: Some(reference),
        pushed_predicates: Vec::new(),
        unsupported_predicates: request.filters.clone(),
        estimated_rows: Some(estimated_rows),
        estimated_bytes: Some(estimated_bytes),
        delivery_guarantee: delivery_guarantee(descriptor.write_disposition.clone()),
    })
}

#[derive(Deserialize)]
struct ManifestListEntryV1 {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,
    added_snapshot_id: i64,
    #[serde(default)]
    key_metadata: Option<Vec<u8>>,
}

#[derive(Deserialize)]
struct ManifestListEntryV2 {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,
    content: i32,
    sequence_number: i64,
    added_snapshot_id: i64,
    #[serde(default)]
    key_metadata: Option<Vec<u8>>,
}

fn parse_manifest_list(
    payload: &[u8],
    version: FormatVersion,
    maximum_manifests: usize,
    index: &mut IcebergPlanningIndex,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<()> {
    if version == FormatVersion::V3 {
        return Err(CdfError::contract(
            "Iceberg format v3 scan planning is not compiled; use a v1/v2 table",
        ));
    }
    loop {
        index.begin_manifest_ingest()?;
        let attempt =
            parse_manifest_list_attempt(payload, version, maximum_manifests, index, cancellation);
        match attempt {
            Ok(true) => match index.finish_manifest_ingest() {
                Ok(true) => return Ok(()),
                Ok(false) => index.restart_manifest_ingest_after_spill_full()?,
                Err(error) => {
                    index.abort_manifest_ingest()?;
                    return Err(error);
                }
            },
            Ok(false) => index.restart_manifest_ingest_after_spill_full()?,
            Err(error) => {
                index.abort_manifest_ingest()?;
                return Err(error);
            }
        }
    }
}

fn parse_manifest_list_attempt(
    payload: &[u8],
    version: FormatVersion,
    maximum_manifests: usize,
    index: &mut IcebergPlanningIndex,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<bool> {
    let reader = AvroReader::new(payload)
        .map_err(|error| CdfError::data(format!("open Iceberg manifest list: {error}")))?;
    let mut manifest_count = 0_usize;
    for value in reader {
        cancellation.check()?;
        let value = value
            .map_err(|error| CdfError::data(format!("read Iceberg manifest list: {error}")))?;
        let (manifest, key_metadata) = match version {
            FormatVersion::V1 => {
                let listed: ManifestListEntryV1 = from_avro_value(&value).map_err(|error| {
                    CdfError::data(format!("decode Iceberg v1 manifest-list entry: {error}"))
                })?;
                (
                    IcebergPlanningManifest {
                        manifest_path: listed.manifest_path,
                        manifest_length: listed.manifest_length,
                        partition_spec_id: listed.partition_spec_id,
                        content: ManifestContentType::Data,
                        sequence_number: 0,
                        added_snapshot_id: listed.added_snapshot_id,
                    },
                    listed.key_metadata,
                )
            }
            FormatVersion::V2 => {
                let listed: ManifestListEntryV2 = from_avro_value(&value).map_err(|error| {
                    CdfError::data(format!("decode Iceberg v2 manifest-list entry: {error}"))
                })?;
                let content = match listed.content {
                    0 => ManifestContentType::Data,
                    1 => ManifestContentType::Deletes,
                    value => {
                        return Err(CdfError::data(format!(
                            "Iceberg manifest list contains unknown content type {value}"
                        )));
                    }
                };
                (
                    IcebergPlanningManifest {
                        manifest_path: listed.manifest_path,
                        manifest_length: listed.manifest_length,
                        partition_spec_id: listed.partition_spec_id,
                        content,
                        sequence_number: listed.sequence_number,
                        added_snapshot_id: listed.added_snapshot_id,
                    },
                    listed.key_metadata,
                )
            }
            FormatVersion::V3 => unreachable!("v3 returned before opening the manifest list"),
        };
        if key_metadata.is_some() {
            return Err(CdfError::contract(
                "encrypted Iceberg manifests require a configured KMS capability; plaintext key metadata is never admitted",
            ));
        }
        u64::try_from(manifest.manifest_length)
            .map_err(|_| CdfError::data("Iceberg manifest length is negative or exceeds u64"))?;
        if manifest.partition_spec_id < 0 {
            return Err(CdfError::data(
                "Iceberg manifest partition spec id must be nonnegative",
            ));
        }
        manifest_count = manifest_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Iceberg manifest count exceeds usize"))?;
        if manifest_count > maximum_manifests {
            return Err(CdfError::data(format!(
                "Iceberg snapshot contains more than maximum_metadata_files={maximum_manifests} manifests"
            )));
        }
        if !index.insert_manifest(&manifest)? {
            return Ok(false);
        }
    }
    Ok(true)
}

struct ManifestWork {
    listed: IcebergPlanningManifest,
    manifest_sha256: String,
    manifest: Manifest,
    header: ManifestHeaderAuthority,
    _payload: AccountedBytes,
    _parse_lease: MemoryLease,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ManifestHeaderAuthority {
    schema_id: Option<i32>,
    partition_spec_id: Option<i32>,
}

#[allow(clippy::too_many_arguments)]
fn process_indexed_manifests(
    manifests: IcebergPlanningManifestReader,
    source: &IcebergSourceOptions,
    catalog: &crate::IcebergCatalogContext,
    cancellation: cdf_runtime::RunCancellation,
    emit: impl FnMut(ManifestWork) -> Result<()>,
) -> Result<()> {
    manifests.consume(|manifest_count, manifests| {
        process_manifests_canonical(
            manifest_count,
            manifests,
            source,
            catalog,
            cancellation,
            emit,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn process_manifests_canonical(
    manifest_count: u64,
    manifests: &mut dyn Iterator<Item = Result<IcebergPlanningManifest>>,
    source: &IcebergSourceOptions,
    catalog: &crate::IcebergCatalogContext,
    cancellation: cdf_runtime::RunCancellation,
    mut emit: impl FnMut(ManifestWork) -> Result<()>,
) -> Result<()> {
    if manifest_count == 0 {
        return Ok(());
    }
    let manifest_count = usize::try_from(manifest_count)
        .map_err(|_| CdfError::data("Iceberg manifest count exceeds addressable usize"))?;
    let host_slots = usize::from(catalog.execution.capabilities().logical_cpu_slots.max(1));
    let worker_count = manifest_count
        .min(usize::from(source.maximum_concurrency))
        .min(host_slots)
        .max(1);
    let (job_sender, job_receiver) = sync_channel::<(usize, IcebergPlanningManifest)>(worker_count);
    let job_receiver = Arc::new(Mutex::new(job_receiver));
    let (result_sender, result_receiver) =
        sync_channel::<(usize, Result<ManifestWork>)>(worker_count);
    let mut scope = catalog.execution.open_scope("iceberg-manifest-planning")?;

    for _ in 0..worker_count {
        let jobs = Arc::clone(&job_receiver);
        let results = result_sender.clone();
        let worker_catalog = catalog.clone();
        let source = source.clone();
        let cancellation = cancellation.clone();
        if let Err(error) = scope.spawn_blocking(
            ICEBERG_SOURCE_BLOCKING_LANE_ID,
            Box::new(move || {
                loop {
                    cancellation.check()?;
                    let (index, manifest) = match jobs
                        .lock()
                        .map_err(|_| CdfError::internal("Iceberg manifest work queue is poisoned"))?
                        .recv()
                    {
                        Ok(index) => index,
                        Err(_) => return Ok(()),
                    };
                    let result = load_manifest_work(
                        &worker_catalog,
                        &source,
                        manifest,
                        cancellation.clone(),
                    );
                    if results.send((index, result)).is_err() {
                        return Ok(());
                    }
                }
            }),
        ) {
            scope.cancel();
            drop(job_sender);
            drop(result_receiver);
            let join = scope.join();
            let _ = catalog.execution.run_io(join);
            return Err(error);
        }
    }
    drop(result_sender);

    let drain_result = (|| -> Result<()> {
        let initially_assigned = worker_count.min(manifest_count);
        for index in 0..initially_assigned {
            let manifest = next_indexed_manifest(manifests, index, manifest_count)?;
            job_sender.send((index, manifest)).map_err(|_| {
                CdfError::internal("Iceberg manifest workers stopped before accepting initial work")
            })?;
        }
        let mut next_assignment = initially_assigned;
        let mut next_canonical_manifest = 0_usize;
        let mut pending = BTreeMap::<usize, Result<ManifestWork>>::new();
        while next_canonical_manifest < manifest_count {
            cancellation.check()?;
            let (index, result) = result_receiver.recv().map_err(|_| {
                CdfError::internal(
                    "Iceberg manifest workers stopped before publishing complete results",
                )
            })?;
            if index < next_canonical_manifest || pending.insert(index, result).is_some() {
                return Err(CdfError::internal(
                    "Iceberg manifest workers published a duplicate canonical result",
                ));
            }
            while let Some(work) = pending.remove(&next_canonical_manifest) {
                emit(work?)?;
                next_canonical_manifest += 1;
                if next_assignment < manifest_count {
                    let manifest =
                        next_indexed_manifest(manifests, next_assignment, manifest_count)?;
                    job_sender.send((next_assignment, manifest)).map_err(|_| {
                        CdfError::internal(
                            "Iceberg manifest workers stopped before accepting bounded work",
                        )
                    })?;
                    next_assignment += 1;
                }
            }
        }
        if let Some(extra) = manifests.next() {
            extra?;
            return Err(CdfError::data(
                "Iceberg manifest index emitted more entries than its counted authority",
            ));
        }
        Ok(())
    })();

    drop(job_sender);
    drop(result_receiver);
    if drain_result.is_err() {
        cancellation.cancel();
        scope.cancel();
    }
    let join = scope.join();
    let join_result = catalog.execution.run_io(join);
    match (drain_result, join_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(_)) => Ok(()),
    }
}

fn next_indexed_manifest(
    manifests: &mut dyn Iterator<Item = Result<IcebergPlanningManifest>>,
    index: usize,
    manifest_count: usize,
) -> Result<IcebergPlanningManifest> {
    manifests.next().transpose()?.ok_or_else(|| {
        CdfError::data(format!(
            "Iceberg manifest index ended at {index} before its {manifest_count}-entry authority"
        ))
    })
}

fn load_manifest_work(
    catalog: &crate::IcebergCatalogContext,
    source: &IcebergSourceOptions,
    listed: IcebergPlanningManifest,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<ManifestWork> {
    let expected_size = u64::try_from(listed.manifest_length)
        .map_err(|_| CdfError::data("Iceberg manifest length is negative or exceeds u64"))?;
    let loaded = load_catalog_object(
        catalog,
        source,
        &listed.manifest_path,
        Some(expected_size),
        cancellation,
    )?;
    let parse_lease = reserve_parse_memory(
        catalog.execution.memory(),
        expected_size,
        source.metadata_parse_amplification_bps,
        "iceberg-manifest-parse",
    )?;
    let manifest_sha256 = sha256_bytes(loaded.payload.payload());
    let header = manifest_header_authority(loaded.payload.payload())?;
    let manifest = Manifest::parse_avro(loaded.payload.payload())
        .map_err(|error| CdfError::data(format!("parse Iceberg manifest: {error}")))?;
    Ok(ManifestWork {
        listed,
        manifest_sha256,
        manifest,
        header,
        _payload: loaded.payload,
        _parse_lease: parse_lease,
    })
}

fn manifest_header_authority(payload: &[u8]) -> Result<ManifestHeaderAuthority> {
    let reader = AvroReader::new(payload)
        .map_err(|error| CdfError::data(format!("parse Iceberg manifest header: {error}")))?;
    Ok(ManifestHeaderAuthority {
        schema_id: optional_manifest_header_id(reader.user_metadata(), "schema-id")?,
        partition_spec_id: optional_manifest_header_id(
            reader.user_metadata(),
            "partition-spec-id",
        )?,
    })
}

fn optional_manifest_header_id(
    metadata: &std::collections::HashMap<String, Vec<u8>>,
    key: &str,
) -> Result<Option<i32>> {
    metadata
        .get(key)
        .map(|value| {
            let value = std::str::from_utf8(value).map_err(|error| {
                CdfError::data(format!(
                    "Iceberg manifest metadata `{key}` is not UTF-8: {error}"
                ))
            })?;
            let id = value.parse::<i32>().map_err(|error| {
                CdfError::data(format!(
                    "Iceberg manifest metadata `{key}` is not an i32: {error}"
                ))
            })?;
            if id < 0 {
                return Err(CdfError::data(format!(
                    "Iceberg manifest metadata `{key}` must be nonnegative"
                )));
            }
            Ok(id)
        })
        .transpose()
}

#[allow(clippy::too_many_arguments)]
fn emit_manifest_tasks(
    work: ManifestWork,
    table: &LoadedIcebergTable,
    writer: &mut ExternalTaskSetWriter,
    required_authority: &mut RequiredTaskAuthority,
    ordinal: &mut u64,
    estimated_rows: &mut u64,
    estimated_bytes: &mut u64,
    delete_index: Option<&IcebergPlanningIndex>,
    maximum_task_bytes: u64,
) -> Result<()> {
    let file_schema_id =
        validate_manifest_authority(table, &work.manifest, &work.listed, work.header)?;
    required_authority.schema_ids.insert(file_schema_id);
    for (entry_index, entry) in work.manifest.entries().iter().enumerate() {
        if !entry.is_alive() {
            continue;
        }
        if entry.content_type() != DataContentType::Data {
            return Err(CdfError::data(format!(
                "Iceberg data manifest `{}` contains a delete entry",
                work.listed.manifest_path
            )));
        }
        if entry.file_format() != DataFileFormat::Parquet {
            return Err(CdfError::contract(format!(
                "Iceberg data file `{}` uses unsupported format {}; v1/v2 Parquet is required",
                entry.file_path(),
                entry.file_format()
            )));
        }
        if entry.data_file().key_metadata().is_some() {
            return Err(CdfError::contract(format!(
                "Iceberg data file `{}` is encrypted but no KMS reader capability is compiled",
                entry.file_path()
            )));
        }
        let task = data_task(
            *ordinal,
            entry_index,
            entry,
            DataTaskContext {
                manifest_sha256: &work.manifest_sha256,
                manifest_file: &work.listed,
                manifest: &work.manifest,
                file_schema_id,
                delete_index,
                maximum_task_bytes,
            },
        )?;
        required_authority
            .partition_spec_ids
            .insert(task.partition_spec_id);
        required_authority
            .partition_spec_ids
            .extend(task.deletes.iter().map(|delete| delete.partition_spec_id));
        if let Some(sort_order_id) = task.data_file.sort_order_id {
            required_authority
                .sort_order_ids
                .insert(i64::from(sort_order_id));
        }
        *estimated_rows = estimated_rows
            .checked_add(entry.record_count())
            .ok_or_else(|| CdfError::data("Iceberg row estimate exceeds u64"))?;
        *estimated_bytes = estimated_bytes
            .checked_add(entry.file_size_in_bytes())
            .ok_or_else(|| CdfError::data("Iceberg byte estimate exceeds u64"))?;
        task.append_to(writer)?;
        *ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Iceberg task ordinal exceeds u64"))?;
    }
    Ok(())
}

fn emit_delete_manifest(
    work: ManifestWork,
    table: &LoadedIcebergTable,
    index: &mut IcebergPlanningIndex,
) -> Result<()> {
    validate_manifest_authority(table, &work.manifest, &work.listed, work.header)?;
    for (entry_index, entry) in work.manifest.entries().iter().enumerate() {
        if !entry.is_alive() {
            continue;
        }
        if entry.content_type() == DataContentType::Data {
            return Err(CdfError::data(format!(
                "Iceberg delete manifest `{}` contains a data entry",
                work.listed.manifest_path
            )));
        }
        let (delete, partition_values) = delete_file(
            &work.manifest_sha256,
            entry_index,
            &work.listed,
            &work.manifest,
            entry,
        )?;
        let global_equality = delete.content == IcebergDeleteContent::Equality
            && work
                .manifest
                .metadata()
                .partition_spec()
                .fields()
                .is_empty();
        let partition_key = if global_equality {
            Vec::new()
        } else {
            serde_json::to_vec(&partition_values).map_err(|error| {
                CdfError::internal(format!("encode Iceberg delete partition key: {error}"))
            })?
        };
        index.insert(&delete, &partition_key, global_equality)?;
    }
    Ok(())
}

struct RequiredTaskAuthority {
    schema_ids: BTreeSet<i32>,
    partition_spec_ids: BTreeSet<i32>,
    sort_order_ids: BTreeSet<i64>,
}

fn task_authority(
    table: &LoadedIcebergTable,
    output_schema_id: i32,
    projected_field_ids: Vec<i32>,
    scan_intent: CompiledScanIntent,
    has_deletes: bool,
    required: &RequiredTaskAuthority,
) -> Result<ValidatedIcebergTaskSetAuthority> {
    let schemas = table
        .metadata
        .schemas_iter()
        .filter(|schema| required.schema_ids.contains(&schema.schema_id()))
        .map(|schema| {
            Ok((
                schema.schema_id(),
                IcebergJsonAuthority::new(serde_json::to_value(schema.as_ref()).map_err(
                    |error| CdfError::internal(format!("serialize Iceberg schema: {error}")),
                )?)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let partition_specs = table
        .metadata
        .partition_specs_iter()
        .filter(|spec| required.partition_spec_ids.contains(&spec.spec_id()))
        .map(|spec| {
            Ok((
                spec.spec_id(),
                IcebergJsonAuthority::new(serde_json::to_value(spec.as_ref()).map_err(
                    |error| {
                        CdfError::internal(format!("serialize Iceberg partition spec: {error}"))
                    },
                )?)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let name_mapping = table
        .metadata
        .properties()
        .get(DEFAULT_SCHEMA_NAME_MAPPING)
        .map(|encoded| {
            serde_json::from_str(encoded)
                .map_err(|error| CdfError::data(format!("decode Iceberg name mapping: {error}")))
                .and_then(IcebergJsonAuthority::new)
        })
        .transpose()?;
    let sort_orders = table
        .metadata
        .sort_orders_iter()
        .filter(|order| required.sort_order_ids.contains(&order.order_id))
        .map(|order| {
            Ok((
                order.order_id,
                IcebergJsonAuthority::new(serde_json::to_value(order.as_ref()).map_err(
                    |error| CdfError::internal(format!("serialize Iceberg sort order: {error}")),
                )?)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut required_capabilities = BTreeSet::from([
        "field-id-projection".to_owned(),
        "partition-evolution".to_owned(),
        "schema-evolution".to_owned(),
    ]);
    if name_mapping.is_some() {
        required_capabilities.insert("name-mapping".to_owned());
    }
    if has_deletes {
        required_capabilities.insert("equality-delete".to_owned());
        required_capabilities.insert("position-delete".to_owned());
    }
    let authority = IcebergTaskSetAuthority {
        version: ICEBERG_TASK_SET_AUTHORITY_VERSION,
        table: table.table_identity(),
        snapshot: table
            .selected
            .as_ref()
            .map(|selected| selected.position.clone()),
        table_format_version: match table.metadata.format_version() {
            FormatVersion::V1 => 1,
            FormatVersion::V2 => 2,
            FormatVersion::V3 => 3,
        },
        schemas,
        output_schema_id,
        projected_field_ids,
        partition_specs,
        sort_orders,
        default_sort_order_id: table.metadata.default_sort_order_id(),
        name_mapping,
        case_sensitive: true,
        scan_intent,
        reader: IcebergReaderRequirements {
            reader_protocol: "cdf-iceberg-parquet".to_owned(),
            reader_version: ICEBERG_SOURCE_DRIVER_VERSION.to_owned(),
            required_capabilities,
        },
    };
    authority.into_validated()
}

fn projected_field_ids(
    schema: &iceberg::spec::Schema,
    projection: Option<&[String]>,
) -> Result<Vec<i32>> {
    let top_level = schema.as_struct().fields();
    let mut ids = match projection {
        Some(projection) => projection
            .iter()
            .map(|name| {
                top_level
                    .iter()
                    .find(|field| field.name == *name)
                    .map(|field| field.id)
                    .ok_or_else(|| {
                        CdfError::contract(format!(
                            "Iceberg projection field `{name}` is absent from the top-level selected schema {}",
                            schema.schema_id()
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?,
        None => top_level.iter().map(|field| field.id).collect(),
    };
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn validate_manifest_authority(
    table: &LoadedIcebergTable,
    manifest: &Manifest,
    listed: &IcebergPlanningManifest,
    header: ManifestHeaderAuthority,
) -> Result<i32> {
    let metadata = manifest.metadata();
    if metadata.content() != &listed.content {
        return Err(CdfError::data(format!(
            "Iceberg manifest `{}` content does not match its manifest-list authority",
            listed.manifest_path
        )));
    }
    let embedded_schema_id = metadata.schema().schema_id();
    let schema_id =
        manifest_schema_id(&listed.manifest_path, header.schema_id, embedded_schema_id)?;
    let schema = table.metadata.schema_by_id(schema_id).ok_or_else(|| {
        CdfError::data(format!(
            "Iceberg manifest `{}` references absent schema id {}",
            listed.manifest_path, schema_id
        ))
    })?;
    if schema.as_ref() != metadata.schema().as_ref() {
        return Err(CdfError::data(format!(
            "Iceberg manifest `{}` schema does not match table metadata",
            listed.manifest_path
        )));
    }
    validate_manifest_partition_spec_id(
        &listed.manifest_path,
        header.partition_spec_id,
        listed.partition_spec_id,
    )?;
    let spec = table
        .metadata
        .partition_spec_by_id(listed.partition_spec_id)
        .ok_or_else(|| {
            CdfError::data(format!(
                "Iceberg manifest `{}` references absent partition spec id {}",
                listed.manifest_path, listed.partition_spec_id
            ))
        })?;
    if spec.fields() != metadata.partition_spec().fields() {
        return Err(CdfError::data(format!(
            "Iceberg manifest `{}` partition spec does not match table metadata",
            listed.manifest_path
        )));
    }
    Ok(schema_id)
}

fn manifest_schema_id(
    manifest_path: &str,
    header_schema_id: Option<i32>,
    embedded_schema_id: i32,
) -> Result<i32> {
    // Iceberg v1 permits the header to be absent, and manifests written under v1 remain legal
    // after a table upgrades to v2. The embedded schema is therefore the fallback authority;
    // when both encodings are present they must agree rather than silently selecting either one.
    match header_schema_id {
        Some(schema_id) if schema_id != embedded_schema_id => Err(CdfError::data(format!(
            "Iceberg manifest `{manifest_path}` schema-id header {schema_id} does not match embedded schema id {embedded_schema_id}"
        ))),
        Some(schema_id) => Ok(schema_id),
        None => Ok(embedded_schema_id),
    }
}

fn validate_manifest_partition_spec_id(
    manifest_path: &str,
    header_partition_spec_id: Option<i32>,
    listed_partition_spec_id: i32,
) -> Result<()> {
    // The manifest list always binds the applicable spec. An optional manifest header is useful
    // corroboration, but absence is not a reason to invent spec 0 (the upstream model's default).
    if let Some(partition_spec_id) = header_partition_spec_id
        && partition_spec_id != listed_partition_spec_id
    {
        return Err(CdfError::data(format!(
            "Iceberg manifest `{manifest_path}` partition-spec-id header {partition_spec_id} does not match manifest-list partition spec id {listed_partition_spec_id}"
        )));
    }
    Ok(())
}

struct DataTaskContext<'a> {
    manifest_sha256: &'a str,
    manifest_file: &'a IcebergPlanningManifest,
    manifest: &'a Manifest,
    file_schema_id: i32,
    delete_index: Option<&'a IcebergPlanningIndex>,
    maximum_task_bytes: u64,
}

fn data_task(
    ordinal: u64,
    entry_index: usize,
    entry: &iceberg::spec::ManifestEntry,
    context: DataTaskContext<'_>,
) -> Result<IcebergScanTask> {
    let DataTaskContext {
        manifest_sha256,
        manifest_file,
        manifest,
        file_schema_id,
        delete_index,
        maximum_task_bytes,
    } = context;
    let data_file = entry.data_file();
    let file_size_bytes = data_file.file_size_in_bytes();
    if file_size_bytes == 0 {
        return Err(CdfError::data(format!(
            "Iceberg data file `{}` has zero bytes",
            data_file.file_path()
        )));
    }
    let partition_values = encoded_partition_values(manifest, data_file)?;
    let (inherited_sequence, inherited_file_sequence) = inherited_sequences(entry, manifest_file);
    let object_generation = artifact_hash(&serde_json::json!({
        "version": 1,
        "manifest_sha256": manifest_sha256,
        "entry_index": entry_index,
        "path": data_file.file_path(),
        "size_bytes": file_size_bytes,
        "sequence_number": inherited_sequence,
        "file_sequence_number": inherited_file_sequence,
    }))?;
    let deletes = match delete_index {
        Some(index) => {
            let partition_key = serde_json::to_vec(&partition_values).map_err(|error| {
                CdfError::internal(format!("encode Iceberg data partition key: {error}"))
            })?;
            index.applicable(
                manifest_file.partition_spec_id,
                &partition_key,
                data_file.file_path(),
                inherited_sequence,
                maximum_task_bytes,
            )?
        }
        None => Vec::new(),
    };
    Ok(IcebergScanTask {
        version: ICEBERG_SCAN_TASK_VERSION,
        canonical_ordinal: ordinal,
        data_file: IcebergDataFile {
            path: data_file.file_path().to_owned(),
            format: IcebergFileFormat::Parquet,
            file_size_bytes,
            range_start: 0,
            range_length: file_size_bytes,
            object_generation,
            content_sha256: None,
            record_count: Some(data_file.record_count()),
            sequence_number: inherited_sequence,
            file_sequence_number: inherited_file_sequence,
            sort_order_id: data_file.sort_order_id(),
            first_row_id: data_file.first_row_id(),
        },
        file_schema_id,
        partition_spec_id: manifest_file.partition_spec_id,
        partition_values,
        deletes,
    })
}

fn delete_file(
    manifest_sha256: &str,
    entry_index: usize,
    manifest_file: &IcebergPlanningManifest,
    manifest: &Manifest,
    entry: &iceberg::spec::ManifestEntry,
) -> Result<(IcebergDeleteFile, Vec<Option<serde_json::Value>>)> {
    let data_file = entry.data_file();
    if data_file.file_format() != DataFileFormat::Parquet {
        return Err(CdfError::contract(format!(
            "Iceberg delete file `{}` uses unsupported format {}; v1/v2 Parquet is required",
            data_file.file_path(),
            data_file.file_format()
        )));
    }
    if data_file.key_metadata().is_some() {
        return Err(CdfError::contract(format!(
            "Iceberg delete file `{}` is encrypted but no KMS reader capability is compiled",
            data_file.file_path()
        )));
    }
    if data_file.content_offset().is_some() || data_file.content_size_in_bytes().is_some() {
        return Err(CdfError::contract(format!(
            "Iceberg delete file `{}` is a v3 deletion vector; v1/v2 Parquet deletes are required",
            data_file.file_path()
        )));
    }
    let file_size_bytes = data_file.file_size_in_bytes();
    if file_size_bytes == 0 {
        return Err(CdfError::data(format!(
            "Iceberg delete file `{}` has zero bytes",
            data_file.file_path()
        )));
    }
    let partition_values = encoded_partition_values(manifest, data_file)?;
    let (sequence_number, file_sequence_number) = inherited_sequences(entry, manifest_file);
    let content = match entry.content_type() {
        DataContentType::PositionDeletes => IcebergDeleteContent::Position,
        DataContentType::EqualityDeletes => IcebergDeleteContent::Equality,
        DataContentType::Data => {
            return Err(CdfError::data(
                "Iceberg delete descriptor was requested for a data file",
            ));
        }
    };
    let mut equality_field_ids = data_file.equality_ids().unwrap_or_default();
    equality_field_ids.sort_unstable();
    equality_field_ids.dedup();
    let referenced_data_file = data_file.referenced_data_file();
    let object_generation = artifact_hash(&serde_json::json!({
        "version": 1,
        "manifest_sha256": manifest_sha256,
        "entry_index": entry_index,
        "path": data_file.file_path(),
        "size_bytes": file_size_bytes,
        "sequence_number": sequence_number,
        "file_sequence_number": file_sequence_number,
    }))?;
    let delete = IcebergDeleteFile {
        path: data_file.file_path().to_owned(),
        format: IcebergFileFormat::Parquet,
        content,
        file_size_bytes,
        object_generation,
        content_sha256: None,
        partition_spec_id: manifest_file.partition_spec_id,
        record_count: Some(data_file.record_count()),
        sequence_number,
        file_sequence_number,
        equality_field_ids,
        referenced_data_file,
    };
    delete.validate()?;
    Ok((delete, partition_values))
}

fn encoded_partition_values(
    manifest: &Manifest,
    data_file: &iceberg::spec::DataFile,
) -> Result<Vec<Option<serde_json::Value>>> {
    let partition_type = manifest
        .metadata()
        .partition_spec()
        .partition_type(manifest.metadata().schema().as_ref())
        .map_err(|error| CdfError::data(format!("bind Iceberg partition values: {error}")))?;
    let partition_values = data_file
        .partition()
        .iter()
        .zip(partition_type.fields())
        .map(|(value, field)| {
            value
                .cloned()
                .map(|literal| {
                    literal
                        .try_into_json(field.field_type.as_ref())
                        .map_err(|error| {
                            CdfError::data(format!(
                                "encode Iceberg partition field {}: {error}",
                                field.id
                            ))
                        })
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    if partition_values.len() != data_file.partition().fields().len() {
        return Err(CdfError::data(
            "Iceberg file partition tuple does not match its manifest partition type",
        ));
    }
    Ok(partition_values)
}

fn inherited_sequences(
    entry: &iceberg::spec::ManifestEntry,
    manifest_file: &IcebergPlanningManifest,
) -> (Option<i64>, Option<i64>) {
    let sequence = entry.sequence_number.or_else(|| {
        (entry.status == ManifestStatus::Added || manifest_file.sequence_number == 0)
            .then_some(manifest_file.sequence_number)
    });
    let file_sequence = entry.file_sequence_number.or_else(|| {
        (entry.status == ManifestStatus::Added || manifest_file.sequence_number == 0)
            .then_some(manifest_file.sequence_number)
    });
    (sequence, file_sequence)
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn delivery_guarantee(disposition: WriteDisposition) -> DeliveryGuarantee {
    match disposition {
        WriteDisposition::Append => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
        WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, sync::Arc};

    use cdf_kernel::ContentStoreNamespace;
    use cdf_memory::DeterministicMemoryCoordinator;
    use cdf_runtime::{FixedSpillBudget, SpillBudgetCoordinator};
    use iceberg::{
        io::FileIO,
        spec::{ManifestFile, ManifestListWriter},
    };

    use super::*;

    #[test]
    fn projection_passes_only_top_level_field_ids_to_the_arrow_reader() {
        let schema: iceberg::spec::Schema = serde_json::from_value(serde_json::json!({
            "type": "struct",
            "schema-id": 7,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {
                    "id": 2,
                    "name": "tags",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 3,
                        "element": "string",
                        "element-required": false
                    }
                }
            ]
        }))
        .unwrap();

        assert_eq!(projected_field_ids(&schema, None).unwrap(), vec![1, 2]);
        assert_eq!(
            projected_field_ids(&schema, Some(&["tags".to_owned()])).unwrap(),
            vec![2]
        );
        assert!(
            projected_field_ids(&schema, Some(&["element".to_owned()]))
                .unwrap_err()
                .to_string()
                .contains("top-level")
        );
    }

    #[test]
    fn omitted_manifest_ids_use_embedded_and_manifest_list_authorities() {
        let schema = apache_avro::Schema::parse_str(r#"{"type":"int"}"#).unwrap();
        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        writer
            .add_user_metadata("partition-spec-id".to_owned(), "11")
            .unwrap();
        writer.append(apache_avro::types::Value::Int(1)).unwrap();
        let payload = writer.into_inner().unwrap();
        assert_eq!(
            manifest_header_authority(&payload).unwrap(),
            ManifestHeaderAuthority {
                schema_id: None,
                partition_spec_id: Some(11),
            }
        );

        assert_eq!(manifest_schema_id("manifest.avro", None, 7).unwrap(), 7);
        validate_manifest_partition_spec_id("manifest.avro", None, 11).unwrap();

        assert!(
            manifest_schema_id("manifest.avro", Some(0), 7)
                .unwrap_err()
                .to_string()
                .contains("schema-id header 0 does not match embedded schema id 7")
        );
        assert!(
            validate_manifest_partition_spec_id("manifest.avro", Some(0), 11)
                .unwrap_err()
                .to_string()
                .contains(
                    "partition-spec-id header 0 does not match manifest-list partition spec id 11"
                )
        );
    }

    #[test]
    fn v1_manifest_list_streams_into_canonical_planning_index() {
        let root = tempfile::tempdir().unwrap();
        let manifest_list = root.path().join("manifest-list-v1.avro");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let output = FileIO::new_with_fs()
            .new_output(manifest_list.to_string_lossy())
            .unwrap();
        let file_writer = runtime.block_on(output.writer()).unwrap();
        let mut writer = ManifestListWriter::v1(file_writer, 41, Some(0));
        writer
            .add_manifests(
                [ManifestFile {
                    manifest_path: "data-manifest.avro".to_owned(),
                    manifest_length: 512,
                    partition_spec_id: 0,
                    content: ManifestContentType::Data,
                    sequence_number: 0,
                    min_sequence_number: 0,
                    added_snapshot_id: 41,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: None,
                    key_metadata: None,
                    first_row_id: None,
                }]
                .into_iter(),
            )
            .unwrap();
        runtime.block_on(writer.close()).unwrap();

        let store = ExternalTaskStore::new(
            root.path(),
            ContentStoreNamespace::new("iceberg-manifest-v1-test").unwrap(),
        )
        .unwrap();
        let memory = Arc::new(
            DeterministicMemoryCoordinator::new(8 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(FixedSpillBudget::new(16 * 1024 * 1024).unwrap());
        let mut source: IcebergSourceOptions = serde_json::from_value(serde_json::json!({
            "catalog": {"kind": "filesystem", "warehouse": ".warehouse"}
        }))
        .unwrap();
        source.planning_index_cache_bytes = 1024 * 1024;
        source.planning_index_spill_growth_bytes = 4 * 1024 * 1024;
        let mut index = IcebergPlanningIndex::create(&store, &source, memory, spill).unwrap();
        parse_manifest_list(
            &fs::read(manifest_list).unwrap(),
            FormatVersion::V1,
            1,
            &mut index,
            &cdf_runtime::RunCancellation::default(),
        )
        .unwrap();
        assert_eq!(index.manifest_count(ManifestContentType::Data), 1);
        let reader = index.manifest_reader(ManifestContentType::Data).unwrap();
        reader
            .consume(|count, manifests| {
                assert_eq!(count, 1);
                let manifest = manifests.next().unwrap()?;
                assert_eq!(manifest.manifest_path, "data-manifest.avro");
                assert_eq!(manifest.sequence_number, 0);
                assert!(manifests.next().is_none());
                Ok(())
            })
            .unwrap();
    }
}
