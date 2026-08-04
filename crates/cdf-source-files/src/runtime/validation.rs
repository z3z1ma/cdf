use std::{collections::BTreeMap, fs, path::PathBuf};

use arrow_schema::Schema;
use cdf_kernel::{
    CdfError, CompiledScanIntent, DeclarativeExpressionNode, PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionId, PartitionPlan, ResourceDescriptor, ResourceId,
    Result, ScopeKey, SourcePosition, source_name,
};
use cdf_memory::MemoryCoordinator;
use cdf_object_access::{FileTransportResource, file_url_path};
use cdf_runtime::{ByteTransformRegistry, FormatRegistry, GenerationStrength};
use sha2::{Digest, Sha256};

use crate::{
    FileCompressionDeclaration, FileResourcePlan,
    driver::{FileTransportScheme, file_transport_scheme},
};

use super::{
    model::{FileInventoryRecord, ResolvedFileMatch},
    resolution::{
        FileResolutionContext, file_partition_id, format_detection_confidence_name,
        glob_path_matches, http_glob_contains, identity_strength_name, pattern_components,
        records_compression_evidence, remote_relative_path, resolve_transport_compression,
        resolve_transport_format, resolved_file_match, resolved_transport_file_match,
    },
};

pub(super) fn per_partition_decode_unit_ceiling(
    logical_cpu_slots: u16,
    run_partition_jobs: Option<u16>,
) -> usize {
    let logical_cpu_slots = usize::from(logical_cpu_slots.max(1));
    let run_partition_jobs = usize::from(run_partition_jobs.unwrap_or(1).max(1));
    logical_cpu_slots.div_ceil(run_partition_jobs).max(1)
}

pub(super) fn stable_decode_memory_budget(memory: &dyn MemoryCoordinator) -> u64 {
    memory.snapshot().budget_bytes
}

pub(super) fn physical_projection_names(
    effective_schema: &Schema,
    projection: Option<&[String]>,
) -> Result<Option<Vec<String>>> {
    projection
        .map(|fields| {
            fields
                .iter()
                .map(|logical_name| {
                    let field = effective_schema.field_with_name(logical_name).map_err(|_| {
                        CdfError::contract(format!(
                            "compiled file projection field {logical_name:?} is absent from the effective schema"
                        ))
                    })?;
                    Ok(source_name(field).unwrap_or_else(|| field.name()).to_owned())
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()
}

pub(super) fn physical_predicates(
    effective_schema: &Schema,
    predicates: &[cdf_kernel::ScanPredicate],
) -> Result<Vec<cdf_kernel::ScanPredicate>> {
    predicates
        .iter()
        .map(|predicate| {
            let mut physical = predicate.clone();
            physical.canonical_expression = cdf_kernel::DeclarativeExpression::new(
                physical_expression_node(effective_schema, &predicate.canonical_expression.root)?,
            );
            physical.canonical_expression.validate()?;
            Ok(physical)
        })
        .collect()
}

pub(super) fn physical_expression_node(
    effective_schema: &Schema,
    node: &DeclarativeExpressionNode,
) -> Result<DeclarativeExpressionNode> {
    match node {
        DeclarativeExpressionNode::Column { name } => {
            let field = effective_schema.field_with_name(name).map_err(|_| {
                CdfError::contract(format!(
                    "compiled file predicate field {name:?} is absent from the effective schema"
                ))
            })?;
            Ok(DeclarativeExpressionNode::Column {
                name: source_name(field)
                    .unwrap_or_else(|| field.name())
                    .to_owned(),
            })
        }
        DeclarativeExpressionNode::Literal { value } => Ok(DeclarativeExpressionNode::Literal {
            value: value.clone(),
        }),
        DeclarativeExpressionNode::Call {
            function,
            arguments,
        } => Ok(DeclarativeExpressionNode::Call {
            function: function.clone(),
            arguments: arguments
                .iter()
                .map(|argument| physical_expression_node(effective_schema, argument))
                .collect::<Result<Vec<_>>>()?,
        }),
        _ => Err(CdfError::contract(
            "compiled file predicate contains an unsupported expression node",
        )),
    }
}

pub(super) fn validate_partition(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    partition: &PartitionPlan,
    context: FileResolutionContext<'_>,
) -> Result<ResolvedFileMatch> {
    let (path, match_count) = validate_partition_plan_shape(descriptor, plan, partition)?;
    let resolved = resolve_planned_file_match(descriptor, plan, path, context)?;
    let planned = partition.planned_file()?.ok_or_else(|| {
        CdfError::contract(format!(
            "file partition `{}` omitted its typed planned position",
            partition.partition_id
        ))
    })?;
    let observed = cdf_kernel::FilePosition {
        path: resolved.path_text.clone(),
        size_bytes: resolved.size_bytes,
        source_generation: resolved.source_generation.clone(),
        etag: resolved.etag.clone(),
        object_version: resolved.version.clone(),
        sha256: resolved.sha256.clone(),
    };
    cdf_kernel::merge_file_position_evidence(planned, &observed)?;
    debug_assert!(match_count > 0);
    validate_resolved_partition_metadata(partition, &resolved, plan, path)?;
    Ok(resolved)
}

pub(super) fn validate_partition_plan_shape<'a>(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    partition: &'a PartitionPlan,
) -> Result<(&'a str, u64)> {
    if partition.metadata.get("kind").map(String::as_str) != Some("files") {
        return Err(CdfError::contract(format!(
            "declarative file resource `{}` expected a file partition plan",
            descriptor.resource_id
        )));
    }
    if partition.metadata.get("resource_id").map(String::as_str)
        != Some(descriptor.resource_id.as_str())
    {
        return Err(CdfError::contract(format!(
            "declarative file partition resource id does not match `{}`",
            descriptor.resource_id
        )));
    }
    if partition.metadata.get("glob").map(String::as_str) != Some(plan.glob.as_str()) {
        return Err(CdfError::contract(format!(
            "declarative file partition glob does not match `{}`",
            plan.glob
        )));
    }
    let path = partition
        .planned_file()?
        .ok_or_else(|| {
            CdfError::contract(format!(
                "file partition `{}` omitted its typed planned position",
                partition.partition_id
            ))
        })?
        .path
        .as_str();
    let expected_scope = ScopeKey::File {
        path: path.to_owned(),
    };
    if partition.scope != expected_scope {
        return Err(CdfError::contract(format!(
            "declarative file partition scope does not match file path `{path}`",
        )));
    }
    let match_count = partition
        .metadata
        .get("match_count")
        .ok_or_else(|| CdfError::contract("file partition omitted planned match count"))?
        .parse::<u64>()
        .map_err(|_| CdfError::contract("file partition match count is invalid"))?;
    if match_count == 0 {
        return Err(CdfError::contract(
            "file partition match count must be greater than zero",
        ));
    }
    let expected_partition_id = if match_count == 1 {
        "files".to_owned()
    } else {
        file_partition_id(path)
    };
    if partition.partition_id.as_str() != expected_partition_id.as_str() {
        return Err(CdfError::contract(format!(
            "declarative file partition id `{}` does not match file path `{path}`",
            partition.partition_id
        )));
    }
    let expected_path_binding =
        planned_file_path_binding(&descriptor.resource_id, &plan.glob, path, match_count)?;
    if partition
        .metadata
        .get("plan_path_binding")
        .map(String::as_str)
        != Some(expected_path_binding.as_str())
    {
        return Err(CdfError::contract(format!(
            "declarative file partition path `{path}` was not produced by glob `{}` under `{}` or does not match its compiled plan binding",
            plan.glob, plan.root
        )));
    }
    Ok((path, match_count))
}

pub(super) fn validate_resolved_partition_metadata(
    partition: &PartitionPlan,
    resolved: &ResolvedFileMatch,
    plan: &FileResourcePlan,
    path: &str,
) -> Result<()> {
    if resolved.identity_strength != GenerationStrength::Weak
        && resolved.sha256.is_none()
        && resolved.etag.is_none()
        && resolved.version.is_none()
        && resolved.source_generation.is_none()
    {
        return Err(CdfError::internal(format!(
            "declarative file partition `{path}` omitted generation evidence despite non-weak identity"
        )));
    }
    validate_partition_metadata_value(
        partition,
        "identity_strength",
        identity_strength_name(resolved.identity_strength),
        path,
    )?;
    validate_compression_metadata(partition, resolved, &plan.compression, path)?;
    validate_partition_metadata_value(partition, "format", &resolved.format.format_id, path)?;
    validate_partition_metadata_value(
        partition,
        "format_driver_version",
        &resolved.format.driver_version,
        path,
    )?;
    validate_partition_metadata_value(
        partition,
        "format_declared",
        if plan.format_declared {
            "true"
        } else {
            "false"
        },
        path,
    )?;
    validate_partition_metadata_value(
        partition,
        "format_extension",
        resolved.format.extension.as_deref().unwrap_or("none"),
        path,
    )?;
    validate_partition_metadata_value(
        partition,
        "format_detection",
        format_detection_confidence_name(resolved.format.detection.confidence),
        path,
    )?;
    validate_partition_metadata_value(
        partition,
        "format_detection_reason",
        &resolved.format.detection.reason,
        path,
    )?;
    Ok(())
}

pub(super) fn validate_compression_metadata(
    partition: &PartitionPlan,
    resolved: &ResolvedFileMatch,
    declared: &FileCompressionDeclaration,
    path: &str,
) -> Result<()> {
    let expects_metadata = records_compression_evidence(&resolved.compression, declared);
    if expects_metadata {
        validate_partition_metadata_value(
            partition,
            "compression",
            resolved.compression.mode_name(),
            path,
        )?;
        validate_partition_metadata_value(
            partition,
            "compression_declared",
            declared.as_str(),
            path,
        )?;
        validate_partition_metadata_value(
            partition,
            "compression_extension",
            resolved.compression.extension_signal.as_str(),
            path,
        )?;
        validate_partition_metadata_value(
            partition,
            "compression_magic",
            resolved.compression.magic_signal.as_str(),
            path,
        )?;
        return Ok(());
    }

    if partition.metadata.contains_key("compression") {
        validate_partition_metadata_value(
            partition,
            "compression",
            resolved.compression.mode_name(),
            path,
        )?;
    }
    Ok(())
}

pub(super) fn validate_partition_metadata_value(
    partition: &PartitionPlan,
    key: &str,
    expected: &str,
    path: &str,
) -> Result<()> {
    if partition.metadata.get(key).map(String::as_str) != Some(expected) {
        return Err(CdfError::data(format!(
            "declarative file partition `{path}` changed {key} after planning"
        )));
    }
    Ok(())
}

pub(super) fn partition_for_file_record(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    scan_intent: &CompiledScanIntent,
    file: &FileInventoryRecord,
    total_matches: u64,
) -> Result<PartitionPlan> {
    let mut metadata = BTreeMap::new();
    metadata.insert("kind".to_owned(), "files".to_owned());
    metadata.insert("glob".to_owned(), plan.glob.clone());
    metadata.insert("resource_id".to_owned(), descriptor.resource_id.to_string());
    metadata.insert("match_count".to_owned(), total_matches.to_string());
    metadata.insert(
        "plan_path_binding".to_owned(),
        planned_file_path_binding(
            &descriptor.resource_id,
            &plan.glob,
            &file.path_text,
            total_matches,
        )?,
    );
    metadata.insert(
        PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
        file.path_text.clone(),
    );
    metadata.insert(
        PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
        file_schema_observation_binding(file),
    );
    metadata.insert(
        "identity_strength".to_owned(),
        identity_strength_name(file.identity_strength).to_owned(),
    );
    if let Some(modified_ms) = &file.modified_ms {
        metadata.insert("modified_ms".to_owned(), modified_ms.clone());
    }
    if let Some(bytes_loaded) = file.bytes_loaded {
        metadata.insert("bytes_loaded".to_owned(), bytes_loaded.to_string());
    }
    metadata.insert("format".to_owned(), file.format.format_id.clone());
    metadata.insert(
        "format_driver_version".to_owned(),
        file.format.driver_version.clone(),
    );
    metadata.insert(
        "format_declared".to_owned(),
        plan.format_declared.to_string(),
    );
    metadata.insert(
        "format_extension".to_owned(),
        file.format
            .extension
            .clone()
            .unwrap_or_else(|| "none".to_owned()),
    );
    metadata.insert(
        "format_detection".to_owned(),
        format_detection_confidence_name(file.format.detection.confidence).to_owned(),
    );
    metadata.insert(
        "format_detection_reason".to_owned(),
        file.format.detection.reason.clone(),
    );
    if records_compression_evidence(&file.compression, &plan.compression) {
        metadata.insert(
            "compression".to_owned(),
            file.compression.mode_name().to_owned(),
        );
        metadata.insert(
            "compression_declared".to_owned(),
            plan.compression.as_str().to_owned(),
        );
        metadata.insert(
            "compression_extension".to_owned(),
            file.compression.extension_signal.as_str().to_owned(),
        );
        metadata.insert(
            "compression_magic".to_owned(),
            file.compression.magic_signal.as_str().to_owned(),
        );
    }

    let partition_id = if total_matches == 1 {
        "files".to_owned()
    } else {
        file_partition_id(&file.path_text)
    };

    Ok(PartitionPlan {
        partition_id: PartitionId::new(partition_id)?,
        scope: ScopeKey::File {
            path: file.path_text.clone(),
        },
        planned_position: Some(SourcePosition::FileManifest(cdf_kernel::FileManifest {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            files: vec![cdf_kernel::FilePosition {
                path: file.path_text.clone(),
                size_bytes: file.size_bytes,
                source_generation: file.source_generation.clone(),
                etag: file.etag.clone(),
                object_version: file.version.clone(),
                sha256: file.sha256.clone(),
            }],
        })),
        start_position: None,
        scan_intent: scan_intent.clone(),
        retry_safety: match file.identity_strength {
            GenerationStrength::Weak => cdf_kernel::PartitionRetrySafety::Forbidden,
            GenerationStrength::Strong | GenerationStrength::ContentAddressed => {
                cdf_kernel::PartitionRetrySafety::ImmutableContent
            }
        },
        metadata,
    })
}

pub(super) fn planned_file_path_binding(
    resource_id: &ResourceId,
    glob: &str,
    path: &str,
    match_count: u64,
) -> Result<String> {
    cdf_runtime::artifact_hash(&serde_json::json!({
        "resource_id": resource_id,
        "glob": glob,
        "path": path,
        "match_count": match_count,
    }))
}

/// Revalidates exactly one planned file without listing or resolving the resource glob again.
///
/// Planning owns enumeration. Open and attestation own generation validation for the selected
/// partition only, keeping N-file execution O(N) rather than O(N²).
pub(super) fn resolve_planned_file_match(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    path: &str,
    context: FileResolutionContext<'_>,
) -> Result<ResolvedFileMatch> {
    let components = pattern_components(&plan.glob)?;
    match file_transport_scheme(&plan.root)? {
        Some(FileTransportScheme::Http | FileTransportScheme::Https) => {
            let root_prefix = format!("{}/", plan.root.trim_end_matches('/'));
            let relative_path = path.strip_prefix(&root_prefix).ok_or_else(|| {
                CdfError::contract(format!(
                    "file partition `{path}` is outside HTTP root `{}`",
                    plan.root
                ))
            })?;
            let expected = http_glob_contains(&descriptor.resource_id, &plan.glob, relative_path)?;
            if !expected {
                return Err(CdfError::contract(format!(
                    "file partition `{path}` is outside the compiled HTTP enumeration"
                )));
            }
            let mut logical =
                FileTransportResource::http_url(path).with_egress_allowlist(plan.allowlist.clone());
            if let Some(auth) = &plan.auth {
                logical = logical.with_auth(auth.clone());
            }
            if let Some(credentials) = &plan.credentials {
                logical = logical.with_credentials(credentials.clone());
            }
            let observation =
                context
                    .transport
                    .metadata(context.egress, &logical, context.control)?;
            let compression = resolve_transport_compression(plan, path, context.transforms)?;
            let format = resolve_transport_format(
                &descriptor.resource_id,
                plan,
                path,
                &compression,
                context.formats,
            )?;
            resolved_transport_file_match(
                observation.access_resource(&logical),
                observation.into_identity(),
                compression,
                format,
            )
        }
        Some(FileTransportScheme::Remote(_)) => {
            let relative = remote_relative_path(&plan.root, path)?;
            if !glob_path_matches(&components, &relative) {
                return Err(CdfError::contract(format!(
                    "file partition `{path}` is outside glob `{}`",
                    plan.glob
                )));
            }
            let logical = FileTransportResource::remote_url(path.to_owned())
                .with_egress_allowlist(plan.allowlist.clone());
            let logical = match &plan.credentials {
                Some(credentials) => logical.with_credentials(credentials.clone()),
                None => logical,
            };
            let observation =
                context
                    .transport
                    .metadata(context.egress, &logical, context.control)?;
            let compression = resolve_transport_compression(plan, path, context.transforms)?;
            let format = resolve_transport_format(
                &descriptor.resource_id,
                plan,
                path,
                &compression,
                context.formats,
            )?;
            resolved_transport_file_match(
                observation.access_resource(&logical),
                observation.into_identity(),
                compression,
                format,
            )
        }
        Some(FileTransportScheme::File) => {
            let mut local_plan = plan.clone();
            local_plan.root = file_url_path(&plan.root)?
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| CdfError::data("file URL path is not valid UTF-8"))?;
            resolve_planned_local_file_match(
                descriptor,
                &local_plan,
                path,
                &components,
                context.formats,
                context.transforms,
            )
        }
        None => resolve_planned_local_file_match(
            descriptor,
            plan,
            path,
            &components,
            context.formats,
            context.transforms,
        ),
    }
}

pub(super) fn resolve_planned_local_file_match(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    path: &str,
    components: &[String],
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<ResolvedFileMatch> {
    if !glob_path_matches(components, path) {
        return Err(CdfError::contract(format!(
            "file partition `{path}` is outside glob `{}`",
            plan.glob
        )));
    }
    let root = PathBuf::from(&plan.root);
    if !root.is_absolute() {
        return Err(CdfError::contract(format!(
            "file source root `{}` for resource `{}` must be absolute before runtime open",
            plan.root, descriptor.resource_id
        )));
    }
    let canonical_root = fs::canonicalize(&root).map_err(|error| {
        CdfError::data(format!(
            "canonicalize file source root {}: {error}",
            root.display()
        ))
    })?;
    let candidate = fs::canonicalize(canonical_root.join(path)).map_err(|error| {
        CdfError::data(format!("resolve planned file partition `{path}`: {error}"))
    })?;
    if !candidate.starts_with(&canonical_root) {
        return Err(CdfError::contract(format!(
            "file partition `{path}` escapes its compiled source root"
        )));
    }
    resolved_file_match(
        &descriptor.resource_id,
        &canonical_root,
        candidate,
        plan,
        formats,
        transforms,
    )
}

pub(super) fn file_schema_observation_binding(file: &FileInventoryRecord) -> String {
    let mut hasher = Sha256::new();
    let size = file.size_bytes.to_string();
    for value in [
        file.path_text.as_str(),
        size.as_str(),
        file.source_generation.as_deref().unwrap_or_default(),
        identity_strength_name(file.identity_strength),
        file.etag.as_deref().unwrap_or_default(),
        file.version.as_deref().unwrap_or_default(),
        file.sha256.as_deref().unwrap_or_default(),
        file.modified_ms.as_deref().unwrap_or_default(),
    ] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}
