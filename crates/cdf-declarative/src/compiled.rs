use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_contract::{IdentifierPolicy, normalize_arrow_schema};
use cdf_kernel::{
    CdfError, ContractRef, CursorOrderingClaim, CursorSpec, DeduplicationSpec,
    EffectiveSchemaCatalogEntry, EffectiveSchemaRuntime, FreshnessSpec, PushdownFidelity,
    ResourceCapabilities, ResourceDescriptor, ResourceId, Result, SchemaHash, SchemaSource,
    ScopeKey, TrustLevel, TypePolicyAllowances, WriteDisposition, parse_arrow_field_type,
    with_cdf_metadata,
};
use cdf_runtime::{
    CompiledSourcePlan, SourceCompileContext, SourceCompileRequest, SourceCursorPushdown,
    SourceRegistry,
};
use sha2::{Digest, Sha256};

use crate::declarations::*;

#[derive(Clone, Debug)]
pub struct CompiledResource {
    descriptor: ResourceDescriptor,
    source_name: String,
    resource_name: String,
    project_root: Option<PathBuf>,
    schema: SchemaRef,
    capabilities: ResourceCapabilities,
    source_plan: CompiledSourcePlan,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
    schema_discovery_sample_files: Option<u64>,
    type_policy_allowances: TypePolicyAllowances,
}

impl CompiledResource {
    pub fn from_compiled_source(
        source_name: impl Into<String>,
        resource_name: impl Into<String>,
        project_root: Option<PathBuf>,
        source_plan: CompiledSourcePlan,
    ) -> Result<Self> {
        source_plan.validate()?;
        let descriptor = source_plan.descriptor.clone();
        let schema = Arc::new(source_plan.schema.clone());
        let capabilities = source_plan.resource_capabilities.clone();
        let effective_schema_runtime = source_plan.effective_schema_runtime.clone();
        let baseline_observation_schema_catalog =
            source_plan.baseline_observation_schema_catalog.clone();
        let type_policy_allowances = source_plan.type_policy_allowances;
        Ok(Self {
            descriptor,
            source_name: source_name.into(),
            resource_name: resource_name.into(),
            project_root,
            schema,
            capabilities,
            source_plan,
            effective_schema_runtime,
            baseline_observation_schema_catalog,
            schema_discovery_sample_files: None,
            type_policy_allowances,
        })
    }

    pub fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    pub fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    pub fn project_root(&self) -> Option<&Path> {
        self.project_root.as_deref()
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn source_plan(&self) -> &CompiledSourcePlan {
        &self.source_plan
    }

    pub fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_ref()
    }

    pub fn baseline_observation_schema_catalog(&self) -> &[EffectiveSchemaCatalogEntry] {
        &self.baseline_observation_schema_catalog
    }

    pub fn schema_discovery_sample_files(&self) -> Option<u64> {
        self.schema_discovery_sample_files
    }

    pub fn type_policy_allowances(&self) -> TypePolicyAllowances {
        self.type_policy_allowances
    }

    pub fn with_schema_source_and_schema(
        &self,
        schema_source: SchemaSource,
        schema: SchemaRef,
    ) -> Self {
        let mut resource = self.clone();
        resource.descriptor.schema_source = schema_source.clone();
        resource.schema = Arc::clone(&schema);
        resource.source_plan.descriptor.schema_source = schema_source;
        resource.source_plan.schema = schema.as_ref().clone();
        resource
    }

    pub fn with_effective_schema(
        &self,
        schema: SchemaRef,
        runtime: EffectiveSchemaRuntime,
    ) -> Result<Self> {
        runtime.validate_for_resource(&self.descriptor)?;
        let mut resource = self.clone();
        resource.schema = Arc::clone(&schema);
        resource.source_plan.schema = schema.as_ref().clone();
        resource.source_plan.effective_schema_runtime = Some(runtime.clone());
        resource.effective_schema_runtime = Some(runtime);
        Ok(resource)
    }

    pub fn with_baseline_observation_schema_catalog(
        &self,
        mut catalog: Vec<EffectiveSchemaCatalogEntry>,
    ) -> Self {
        catalog.sort_by(|left, right| left.physical_schema_hash.cmp(&right.physical_schema_hash));
        catalog.dedup_by(|left, right| left.physical_schema_hash == right.physical_schema_hash);
        let mut resource = self.clone();
        resource.baseline_observation_schema_catalog = catalog.clone();
        resource.source_plan.baseline_observation_schema_catalog = catalog;
        resource
    }
}

pub fn compile_document(
    registry: &SourceRegistry,
    document: &DeclarativeDocument,
) -> Result<Vec<CompiledResource>> {
    compile_document_inner(registry, document, None)
}

pub fn compile_document_with_project_root(
    registry: &SourceRegistry,
    document: &DeclarativeDocument,
    project_root: impl AsRef<Path>,
) -> Result<Vec<CompiledResource>> {
    compile_document_inner(registry, document, Some(project_root.as_ref()))
}

fn compile_document_inner(
    registry: &SourceRegistry,
    document: &DeclarativeDocument,
    project_root: Option<&Path>,
) -> Result<Vec<CompiledResource>> {
    if document.source.is_empty() {
        return Err(CdfError::contract(
            "declarative document must contain at least one source",
        ));
    }
    if document.resource.is_empty() {
        return Err(CdfError::contract(
            "declarative document must contain at least one resource",
        ));
    }

    document
        .resource
        .iter()
        .map(|(name, resource)| {
            let source_name = resolve_source_name(document, resource)?;
            let source = document.source.get(&source_name).ok_or_else(|| {
                CdfError::contract(format!(
                    "resource `{name}` references unknown source `{source_name}`"
                ))
            })?;
            compile_resource(registry, name, &source_name, source, resource, project_root)
        })
        .collect()
}

pub fn validate_document(registry: &SourceRegistry, document: &DeclarativeDocument) -> Result<()> {
    compile_document(registry, document).map(drop)
}

pub fn physical_arrow_schema_hash(schema: &Schema) -> Result<SchemaHash> {
    cdf_kernel::canonical_arrow_schema_hash(schema)
}

fn resolve_source_name(
    document: &DeclarativeDocument,
    resource: &ResourceDeclaration,
) -> Result<String> {
    if let Some(source) = &resource.source {
        return Ok(source.clone());
    }
    if document.source.len() == 1 {
        return Ok(document
            .source
            .keys()
            .next()
            .expect("length was checked")
            .clone());
    }
    Err(CdfError::contract(
        "resource source must be declared when a document has multiple sources",
    ))
}

fn compile_resource(
    registry: &SourceRegistry,
    name: &str,
    source_name: &str,
    source: &SourceDeclaration,
    resource: &ResourceDeclaration,
    project_root: Option<&Path>,
) -> Result<CompiledResource> {
    let resource_id = format!("{source_name}.{name}");
    let descriptor_resource_id = ResourceId::new(resource_id.clone())?;
    let schema = compile_schema(resource)?;
    let schema_source = compile_schema_source(&resource_id, resource)?;
    let cursor = compile_cursor(resource.cursor.as_ref())?;
    let write_disposition = compile_write_disposition(resource)?;
    let deduplication = compile_deduplication(&resource_id, resource, &write_disposition)?;
    let merge_key = compile_merge_key(&resource_id, resource, &write_disposition)?;
    validate_fields(name, resource)?;
    let trust_level = compile_trust(resource)?;
    let contract = resource
        .contract
        .as_ref()
        .map(ContractRef::new)
        .transpose()?;
    let descriptor = ResourceDescriptor {
        resource_id: descriptor_resource_id,
        schema_source,
        primary_key: resource.primary_key.clone(),
        merge_key,
        cursor,
        write_disposition,
        deduplication,
        contract,
        state_scope: state_scope(resource)?,
        freshness: match &resource.freshness {
            Some(freshness) => Some(FreshnessSpec {
                max_age_ms: parse_duration_ms(&freshness.max_age)?,
            }),
            None => None,
        },
        trust_level,
    };

    let type_policy_allowances = resource
        .types
        .as_ref()
        .map(|types| TypePolicyAllowances {
            coerce_types: types.coerce_types,
            allow_lossy_mapping: types.allow_lossy_mapping,
        })
        .unwrap_or_default();
    let source_plan = registry.compile(SourceCompileRequest {
        source_kind: source.kind.clone(),
        context: SourceCompileContext {
            source_name: source_name.to_owned(),
            project_root: project_root.map(Path::to_path_buf),
            cursor_pushdown: resource.cursor.as_ref().map(|cursor| SourceCursorPushdown {
                parameter: cursor.param.clone(),
                fidelity: cursor
                    .filter_fidelity
                    .as_ref()
                    .map(to_pushdown_fidelity)
                    .unwrap_or(PushdownFidelity::Inexact),
            }),
        },
        source_options: source.options.clone(),
        resource_options: resource.options.clone(),
        descriptor: descriptor.clone(),
        schema: schema.clone(),
        type_policy_allowances,
        effective_schema_runtime: None,
        baseline_observation_schema_catalog: Vec::new(),
    })?;
    let capabilities = source_plan.resource_capabilities.clone();

    Ok(CompiledResource {
        descriptor,
        source_name: source_name.to_owned(),
        resource_name: name.to_owned(),
        project_root: project_root.map(Path::to_path_buf),
        schema: Arc::new(schema),
        capabilities,
        source_plan,
        effective_schema_runtime: None,
        baseline_observation_schema_catalog: Vec::new(),
        schema_discovery_sample_files: resource.sample_files,
        type_policy_allowances,
    })
}

fn compile_deduplication(
    resource_id: &str,
    resource: &ResourceDeclaration,
    write_disposition: &WriteDisposition,
) -> Result<Option<DeduplicationSpec>> {
    match (resource.deduplicate, write_disposition) {
        (None, _) => Ok(None),
        (Some(DeduplicationDeclaration::ExactRow), WriteDisposition::Append) => {
            Ok(Some(DeduplicationSpec::ExactRow))
        }
        (Some(DeduplicationDeclaration::ExactRow), _) => Err(CdfError::contract(format!(
            "resource `{resource_id}` deduplicate = \"exact_row\" is valid only with append; remove deduplicate or set write_disposition = \"append\""
        ))),
    }
}

fn compile_schema(resource: &ResourceDeclaration) -> Result<Schema> {
    let Some(schema) = &resource.schema else {
        return Ok(Schema::empty());
    };

    let fields = schema
        .fields
        .iter()
        .map(|field| {
            let data_type = field_type(&field.field_type, field.timezone.clone())?;
            let arrow_field = Field::new(&field.name, data_type, field.nullable.unwrap_or(true));
            let source_name = field
                .source_name
                .clone()
                .unwrap_or_else(|| field.name.clone());
            Ok(with_cdf_metadata(
                arrow_field,
                Some(source_name),
                field.semantic.clone(),
                field.null_origin.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    normalize_arrow_schema(&Schema::new(fields), &IdentifierPolicy::default())
}

fn compile_schema_source(
    resource_id: &str,
    resource: &ResourceDeclaration,
) -> Result<SchemaSource> {
    match (resource.schema_mode, &resource.schema) {
        (Some(crate::SchemaModeDeclaration::Hints), Some(schema)) => Ok(SchemaSource::Hints {
            source: format!("declarative:{resource_id}"),
            hints_hash: Some(schema_hash(schema)?),
            snapshot: None,
        }),
        (Some(crate::SchemaModeDeclaration::Hints), None) => Err(CdfError::contract(format!(
            "resource `{resource_id}` schema_mode = \"hints\" requires a schema block"
        ))),
        (Some(crate::SchemaModeDeclaration::Declared), None) => Err(CdfError::contract(format!(
            "resource `{resource_id}` schema_mode = \"declared\" requires a schema block"
        ))),
        (Some(crate::SchemaModeDeclaration::Discover), Some(_)) => {
            Err(CdfError::contract(format!(
                "resource `{resource_id}` schema_mode = \"discover\" cannot carry a schema block; use hints to constrain discovery"
            )))
        }
        (Some(crate::SchemaModeDeclaration::Declared) | None, Some(schema)) => {
            Ok(SchemaSource::Declared {
                schema_hash: schema_hash(schema)?,
                source: format!("declarative:{resource_id}"),
            })
        }
        (Some(crate::SchemaModeDeclaration::Discover) | None, None) => Ok(SchemaSource::Discover),
    }
}

fn compile_cursor(cursor: Option<&CursorDeclaration>) -> Result<Option<CursorSpec>> {
    cursor
        .map(|cursor| {
            Ok(CursorSpec {
                field: cursor.field.clone(),
                ordering: match cursor.ordering {
                    CursorOrderingDeclaration::Exact => CursorOrderingClaim::Exact,
                    CursorOrderingDeclaration::Inexact | CursorOrderingDeclaration::BestEffort => {
                        CursorOrderingClaim::Inexact
                    }
                    CursorOrderingDeclaration::Unordered => CursorOrderingClaim::Unordered,
                },
                lag_tolerance_ms: parse_duration_ms(&cursor.lag)?,
            })
        })
        .transpose()
}

fn compile_trust(resource: &ResourceDeclaration) -> Result<TrustLevel> {
    if let Some(trust) = &resource.trust {
        return Ok(to_trust_level(trust));
    }

    match resource.contract.as_deref() {
        Some("experimental") => Ok(TrustLevel::Experimental),
        Some("governed") => Ok(TrustLevel::Governed),
        Some("financial") => Ok(TrustLevel::Financial),
        Some("serving") => Ok(TrustLevel::Serving),
        Some(contract) => Err(CdfError::contract(format!(
            "resource with custom contract `{contract}` must also declare trust"
        ))),
        None => Err(CdfError::contract(
            "resource must declare trust or use a built-in contract preset",
        )),
    }
}

fn state_scope(resource: &ResourceDeclaration) -> Result<ScopeKey> {
    match &resource.partition {
        Some(PartitionDeclaration {
            by: PartitionByDeclaration::CursorWindow,
            width,
        }) => {
            let width = width
                .as_ref()
                .ok_or_else(|| CdfError::contract("cursor_window partitions must declare width"))?;
            parse_duration_ms(width)?;
            Ok(ScopeKey::Window {
                start: "cursor".to_owned(),
                end: format!("cursor+{width}"),
            })
        }
        Some(PartitionDeclaration {
            by: PartitionByDeclaration::File,
            ..
        }) => Ok(ScopeKey::File {
            path: "*".to_owned(),
        }),
        Some(PartitionDeclaration {
            by: PartitionByDeclaration::Resource,
            ..
        })
        | None => Ok(ScopeKey::Resource),
    }
}

fn validate_fields(name: &str, resource: &ResourceDeclaration) -> Result<()> {
    let mut required = resource.primary_key.clone();
    if let Some(merge_key) = &resource.merge_key {
        required.extend(merge_key.iter().cloned());
    }
    if let Some(cursor) = &resource.cursor {
        required.push(cursor.field.clone());
    }
    required.sort();
    required.dedup();

    if required.is_empty() {
        return Ok(());
    }

    if let Some(schema) = &resource.schema {
        let declared = schema
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<BTreeSet<_>>();
        ensure_fields_exist(name, "declared schema", &declared, &required)?;
    }

    if let Some(sample) = &resource.sample {
        let sample_fields = sample
            .fields
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        ensure_fields_exist(name, "sample", &sample_fields, &required)?;
    }

    Ok(())
}

fn ensure_fields_exist(
    resource_name: &str,
    field_set_name: &str,
    fields: &BTreeSet<&str>,
    required: &[String],
) -> Result<()> {
    let missing = required
        .iter()
        .filter(|field| !fields.contains(field.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }

    Err(CdfError::contract(format!(
        "resource `{resource_name}` is missing required field(s) {} in {field_set_name}",
        missing.join(", ")
    )))
}

fn schema_hash(schema: &SchemaDeclaration) -> Result<SchemaHash> {
    let bytes =
        serde_json::to_vec(schema).map_err(|error| CdfError::contract(error.to_string()))?;
    let digest = Sha256::digest(bytes);
    SchemaHash::new(format!("sha256:{}", hex::encode(digest)))
}

fn field_type(field_type: &FieldTypeDeclaration, timezone: Option<String>) -> Result<DataType> {
    let raw = field_type.as_str();
    let data_type = parse_arrow_field_type(raw).map_err(|error| {
        CdfError::contract(format!("invalid declarative field type `{raw}`: {error}"))
    })?;

    match (data_type, timezone) {
        (DataType::Timestamp(_, Some(type_timezone)), Some(field_timezone))
            if type_timezone.as_ref() != field_timezone.as_str() =>
        {
            Err(CdfError::contract(format!(
                "invalid declarative field type `{raw}`: timezone `{field_timezone}` conflicts with type timezone `{type_timezone}`"
            )))
        }
        (DataType::Timestamp(unit, None), Some(field_timezone)) => {
            Ok(DataType::Timestamp(unit, Some(field_timezone.into())))
        }
        (data_type, _) => Ok(data_type),
    }
}

fn to_write_disposition(disposition: &WriteDispositionDeclaration) -> Result<WriteDisposition> {
    Ok(match disposition {
        WriteDispositionDeclaration::Append => WriteDisposition::Append,
        WriteDispositionDeclaration::Replace => WriteDisposition::Replace,
        WriteDispositionDeclaration::Merge => WriteDisposition::Merge,
        WriteDispositionDeclaration::CdcApply => WriteDisposition::CdcApply,
    })
}

fn compile_write_disposition(resource: &ResourceDeclaration) -> Result<WriteDisposition> {
    resource
        .write_disposition
        .as_ref()
        .map(to_write_disposition)
        .transpose()
        .map(|disposition| disposition.unwrap_or(WriteDisposition::Append))
}

fn compile_merge_key(
    resource_id: &str,
    resource: &ResourceDeclaration,
    write_disposition: &WriteDisposition,
) -> Result<Vec<String>> {
    match write_disposition {
        WriteDisposition::Merge => match &resource.merge_key {
            Some(keys) if !keys.is_empty() => Ok(keys.clone()),
            _ => Err(CdfError::contract(format!(
                "resource `{resource_id}` declares write_disposition = \"merge\" but is missing merge_key; add `merge_key = [...]` or use `write_disposition = \"append\"`"
            ))),
        },
        WriteDisposition::Append | WriteDisposition::Replace | WriteDisposition::CdcApply => {
            Ok(resource.merge_key.clone().unwrap_or_default())
        }
    }
}

fn to_trust_level(trust: &TrustDeclaration) -> TrustLevel {
    match trust {
        TrustDeclaration::Experimental => TrustLevel::Experimental,
        TrustDeclaration::Governed => TrustLevel::Governed,
        TrustDeclaration::Financial => TrustLevel::Financial,
        TrustDeclaration::Serving => TrustLevel::Serving,
    }
}

fn to_pushdown_fidelity(fidelity: &FilterFidelityDeclaration) -> PushdownFidelity {
    match fidelity {
        FilterFidelityDeclaration::Exact => PushdownFidelity::Exact,
        FilterFidelityDeclaration::Inexact => PushdownFidelity::Inexact,
        FilterFidelityDeclaration::Unsupported => PushdownFidelity::Unsupported,
    }
}

fn parse_duration_ms(value: &str) -> Result<u64> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CdfError::contract("duration cannot be empty"));
    }

    let digits = value
        .chars()
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        return Err(CdfError::contract(format!(
            "duration `{value}` must start with a number"
        )));
    }
    let unit = &value[digits.len()..];
    let amount = digits.parse::<u64>().map_err(|error| {
        CdfError::contract(format!("duration `{value}` has invalid number: {error}"))
    })?;

    let multiplier = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "" => {
            return Err(CdfError::contract(format!(
                "duration `{value}` must include a unit"
            )));
        }
        _ => {
            return Err(CdfError::contract(format!(
                "duration `{value}` has unsupported unit `{unit}`"
            )));
        }
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| CdfError::contract(format!("duration `{value}` is too large")))
}
