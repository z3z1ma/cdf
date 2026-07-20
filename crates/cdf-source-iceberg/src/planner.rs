use std::collections::{BTreeMap, BTreeSet};

use cdf_kernel::{
    CdfError, CompiledScanIntent, DeliveryGuarantee, PlanId, Result, ScanPlan, ScanRequest,
    WriteDisposition,
};
use cdf_runtime::artifact_hash;
use cdf_task_store::{ExternalTaskStore, TaskSetLimits};
use iceberg::spec::{
    DEFAULT_SCHEMA_NAME_MAPPING, DataContentType, DataFileFormat, FormatVersion, Manifest,
    ManifestContentType, ManifestList, ManifestStatus,
};
use sha2::{Digest, Sha256};

use crate::{
    ICEBERG_SCAN_TASK_VERSION, ICEBERG_SOURCE_DRIVER_VERSION, ICEBERG_TASK_SET_AUTHORITY_VERSION,
    ICEBERG_TASK_SET_TYPE, IcebergDataFile, IcebergFileFormat, IcebergJsonAuthority,
    IcebergReaderRequirements, IcebergScanTask, IcebergSourceOptions, IcebergTaskSetAuthority,
    LoadedIcebergTable,
    catalog::{load_catalog_object, reserve_parse_memory},
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
    let authority = task_authority(table, output_schema_id, projected_field_ids, scan_intent)?;
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
        |output| authority.encode_to(output),
    )?;
    let authority_sha256 = writer.authority_sha256().to_owned();
    if authority_sha256 != authority.content_sha256()? {
        return Err(CdfError::internal(
            "Iceberg task-store authority hash does not match its canonical model",
        ));
    }

    let mut estimated_rows = 0_u64;
    let mut estimated_bytes = 0_u64;
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
        let list = ManifestList::parse_with_version(
            manifest_list.payload.payload(),
            table.metadata.format_version(),
        )
        .map_err(|error| CdfError::data(format!("parse Iceberg manifest list: {error}")))?;
        let mut manifests = list.consume_entries().into_iter().collect::<Vec<_>>();
        if manifests.len() > source.maximum_metadata_files {
            return Err(CdfError::data(format!(
                "Iceberg snapshot contains {} manifests but maximum_metadata_files is {}",
                manifests.len(),
                source.maximum_metadata_files
            )));
        }
        manifests.sort_by(|left, right| {
            left.manifest_path
                .cmp(&right.manifest_path)
                .then_with(|| left.partition_spec_id.cmp(&right.partition_spec_id))
                .then_with(|| left.added_snapshot_id.cmp(&right.added_snapshot_id))
        });
        if manifests
            .windows(2)
            .any(|pair| pair[0].manifest_path == pair[1].manifest_path)
        {
            return Err(CdfError::data(
                "Iceberg manifest list contains a duplicate manifest path",
            ));
        }
        let mut ordinal = 0_u64;
        for manifest_file in manifests {
            if manifest_file.content == ManifestContentType::Deletes {
                return Err(CdfError::contract(
                    "Iceberg delete manifests require the delete-planning capability owned by I2; no data task was admitted",
                ));
            }
            if manifest_file.key_metadata.is_some() {
                return Err(CdfError::contract(
                    "encrypted Iceberg manifests require a configured KMS capability; plaintext key metadata is never admitted",
                ));
            }
            let expected_size = u64::try_from(manifest_file.manifest_length).map_err(|_| {
                CdfError::data("Iceberg manifest length is negative or exceeds u64")
            })?;
            let loaded = load_catalog_object(
                context.catalog,
                source,
                &manifest_file.manifest_path,
                Some(expected_size),
                context.cancellation.clone(),
            )?;
            let parse_lease = reserve_parse_memory(
                context.catalog.execution.memory(),
                expected_size,
                source.metadata_parse_amplification_bps,
                "iceberg-manifest-parse",
            )?;
            let manifest_sha256 = sha256_bytes(loaded.payload.payload());
            let manifest = Manifest::parse_avro(loaded.payload.payload())
                .map_err(|error| CdfError::data(format!("parse Iceberg manifest: {error}")))?;
            validate_manifest_authority(table, &manifest, &manifest_file)?;
            for (entry_index, entry) in manifest.entries().iter().enumerate() {
                if !entry.is_alive() {
                    continue;
                }
                if entry.content_type() != DataContentType::Data {
                    return Err(CdfError::contract(
                        "Iceberg delete entries require the delete-planning capability owned by I2; no incomplete task set was published",
                    ));
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
                    ordinal,
                    &authority_sha256,
                    &manifest_sha256,
                    entry_index,
                    &manifest_file,
                    &manifest,
                    entry,
                )?;
                estimated_rows = estimated_rows
                    .checked_add(entry.record_count())
                    .ok_or_else(|| CdfError::data("Iceberg row estimate exceeds u64"))?;
                estimated_bytes = estimated_bytes
                    .checked_add(entry.file_size_in_bytes())
                    .ok_or_else(|| CdfError::data("Iceberg byte estimate exceeds u64"))?;
                task.append_to(&authority, &mut writer)?;
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("Iceberg task ordinal exceeds u64"))?;
            }
            drop(parse_lease);
        }
        drop(manifest_list_parse);
    }
    let artifact = writer.finalize()?;
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

fn task_authority(
    table: &LoadedIcebergTable,
    output_schema_id: i32,
    projected_field_ids: Vec<i32>,
    scan_intent: CompiledScanIntent,
) -> Result<IcebergTaskSetAuthority> {
    let schemas = table
        .metadata
        .schemas_iter()
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
    let mut required_capabilities = BTreeSet::from([
        "field-id-projection".to_owned(),
        "partition-evolution".to_owned(),
        "schema-evolution".to_owned(),
    ]);
    if name_mapping.is_some() {
        required_capabilities.insert("name-mapping".to_owned());
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
        name_mapping,
        case_sensitive: true,
        scan_intent,
        reader: IcebergReaderRequirements {
            reader_protocol: "cdf-iceberg-parquet".to_owned(),
            reader_version: ICEBERG_SOURCE_DRIVER_VERSION.to_owned(),
            required_capabilities,
        },
    };
    authority.validate()?;
    Ok(authority)
}

fn projected_field_ids(
    schema: &iceberg::spec::Schema,
    projection: Option<&[String]>,
) -> Result<Vec<i32>> {
    let mut ids =
        match projection {
            Some(projection) => projection
                .iter()
                .map(|name| {
                    schema.field_by_name(name).map(|field| field.id).ok_or_else(|| {
                    CdfError::contract(format!(
                        "Iceberg projection field `{name}` is absent from selected schema {}",
                        schema.schema_id()
                    ))
                })
                })
                .collect::<Result<Vec<_>>>()?,
            None => (1..=schema.highest_field_id())
                .filter(|field_id| schema.field_by_id(*field_id).is_some())
                .collect(),
        };
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn validate_manifest_authority(
    table: &LoadedIcebergTable,
    manifest: &Manifest,
    listed: &iceberg::spec::ManifestFile,
) -> Result<()> {
    let metadata = manifest.metadata();
    if metadata.content() != &listed.content
        || metadata.partition_spec().spec_id() != listed.partition_spec_id
    {
        return Err(CdfError::data(format!(
            "Iceberg manifest `{}` metadata does not match its manifest-list authority",
            listed.manifest_path
        )));
    }
    let schema = table
        .metadata
        .schema_by_id(metadata.schema_id())
        .ok_or_else(|| {
            CdfError::data(format!(
                "Iceberg manifest `{}` references absent schema id {}",
                listed.manifest_path,
                metadata.schema_id()
            ))
        })?;
    if artifact_hash(schema.as_ref())? != artifact_hash(metadata.schema().as_ref())? {
        return Err(CdfError::data(format!(
            "Iceberg manifest `{}` schema does not match table metadata",
            listed.manifest_path
        )));
    }
    let spec = table
        .metadata
        .partition_spec_by_id(listed.partition_spec_id)
        .ok_or_else(|| {
            CdfError::data(format!(
                "Iceberg manifest `{}` references absent partition spec id {}",
                listed.manifest_path, listed.partition_spec_id
            ))
        })?;
    if artifact_hash(spec.as_ref())? != artifact_hash(metadata.partition_spec())? {
        return Err(CdfError::data(format!(
            "Iceberg manifest `{}` partition spec does not match table metadata",
            listed.manifest_path
        )));
    }
    Ok(())
}

fn data_task(
    ordinal: u64,
    authority_sha256: &str,
    manifest_sha256: &str,
    entry_index: usize,
    manifest_file: &iceberg::spec::ManifestFile,
    manifest: &Manifest,
    entry: &iceberg::spec::ManifestEntry,
) -> Result<IcebergScanTask> {
    let data_file = entry.data_file();
    let file_size_bytes = data_file.file_size_in_bytes();
    if file_size_bytes == 0 {
        return Err(CdfError::data(format!(
            "Iceberg data file `{}` has zero bytes",
            data_file.file_path()
        )));
    }
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
            "Iceberg data-file partition tuple does not match its manifest partition type",
        ));
    }
    let inherited_sequence = entry.sequence_number.or_else(|| {
        (entry.status == ManifestStatus::Added || manifest_file.sequence_number == 0)
            .then_some(manifest_file.sequence_number)
    });
    let inherited_file_sequence = entry.file_sequence_number.or_else(|| {
        (entry.status == ManifestStatus::Added || manifest_file.sequence_number == 0)
            .then_some(manifest_file.sequence_number)
    });
    let object_generation = artifact_hash(&serde_json::json!({
        "version": 1,
        "manifest_sha256": manifest_sha256,
        "entry_index": entry_index,
        "path": data_file.file_path(),
        "size_bytes": file_size_bytes,
        "sequence_number": inherited_sequence,
        "file_sequence_number": inherited_file_sequence,
    }))?;
    Ok(IcebergScanTask {
        version: ICEBERG_SCAN_TASK_VERSION,
        canonical_ordinal: ordinal,
        authority_sha256: authority_sha256.to_owned(),
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
        file_schema_id: manifest.metadata().schema_id(),
        partition_spec_id: manifest_file.partition_spec_id,
        partition_values,
        deletes: Vec::new(),
    })
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
