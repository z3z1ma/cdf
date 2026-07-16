use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_schema::{
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
    DataType, Field, Fields, Schema, SchemaRef, TimeUnit,
};
use cdf_contract::{IdentifierPolicy, normalize_arrow_schema};
use cdf_kernel::{
    CdfError, ContractRef, CursorOrderingClaim, CursorSpec, DeduplicationSpec,
    EffectiveSchemaCatalogEntry, EffectiveSchemaRuntime, FreshnessSpec, PushdownFidelity,
    ResourceCapabilities, ResourceDescriptor, ResourceId, Result, SchemaHash, SchemaSource,
    ScopeKey, TrustLevel, TypePolicyAllowances, WriteDisposition, with_cdf_metadata,
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
    let data_type = parse_field_data_type(raw).map_err(|error| {
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

fn parse_field_data_type(raw: &str) -> std::result::Result<DataType, String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err("type string is empty".to_owned());
    }

    match value.to_ascii_lowercase().as_str() {
        "string" | "utf8" | "json" => return Ok(DataType::Utf8),
        "large_utf8" => return Ok(DataType::LargeUtf8),
        "boolean" => return Ok(DataType::Boolean),
        "int8" => return Ok(DataType::Int8),
        "int16" => return Ok(DataType::Int16),
        "int32" => return Ok(DataType::Int32),
        "int64" => return Ok(DataType::Int64),
        "uint8" => return Ok(DataType::UInt8),
        "uint16" => return Ok(DataType::UInt16),
        "uint32" => return Ok(DataType::UInt32),
        "uint64" | "u_int64" => return Ok(DataType::UInt64),
        "float16" => return Ok(DataType::Float16),
        "float32" => return Ok(DataType::Float32),
        "float64" => return Ok(DataType::Float64),
        "date32" => return Ok(DataType::Date32),
        "date64" => return Ok(DataType::Date64),
        "timestamp_millis" => {
            return Ok(DataType::Timestamp(TimeUnit::Millisecond, None));
        }
        "timestamp_micros" => {
            return Ok(DataType::Timestamp(TimeUnit::Microsecond, None));
        }
        "binary" => return Ok(DataType::Binary),
        "large_binary" => return Ok(DataType::LargeBinary),
        _ => {}
    }

    if let Some(body) = enclosed_body(value, "decimal", '(', ')')? {
        return decimal_type(value, body, DecimalWidth::Decimal128);
    }
    if let Some(body) = enclosed_body(value, "decimal128", '(', ')')? {
        return decimal_type(value, body, DecimalWidth::Decimal128);
    }
    if let Some(body) = enclosed_body(value, "decimal256", '(', ')')? {
        return decimal_type(value, body, DecimalWidth::Decimal256);
    }
    if let Some(body) = enclosed_body(value, "date", '(', ')')? {
        return date_type(body);
    }
    if let Some(body) = enclosed_body(value, "time", '(', ')')? {
        return time_type(body);
    }
    if let Some(body) = enclosed_body(value, "time32", '(', ')')? {
        return Ok(DataType::Time32(time_unit(
            body,
            &[TimeUnit::Second, TimeUnit::Millisecond],
        )?));
    }
    if let Some(body) = enclosed_body(value, "time64", '(', ')')? {
        return Ok(DataType::Time64(time_unit(
            body,
            &[TimeUnit::Microsecond, TimeUnit::Nanosecond],
        )?));
    }
    if let Some(body) = enclosed_body(value, "timestamp", '(', ')')? {
        return timestamp_type(body);
    }
    if let Some(body) = enclosed_body(value, "duration", '(', ')')? {
        return Ok(DataType::Duration(time_unit(body, ALL_TIME_UNITS)?));
    }
    if let Some(body) = enclosed_body(value, "list", '<', '>')? {
        return Ok(DataType::new_list(parse_field_data_type(body)?, true));
    }
    if let Some(body) = enclosed_body(value, "large_list", '<', '>')? {
        return Ok(DataType::new_large_list(parse_field_data_type(body)?, true));
    }
    if let Some(body) = enclosed_body(value, "struct", '<', '>')? {
        return struct_type(body);
    }
    if let Some(body) = enclosed_body(value, "map", '<', '>')? {
        return map_type(body);
    }

    Err("expected an Arrow type string such as `int64`, `timestamp(us, UTC)`, `list<int64>`, or `struct<name: utf8>`".to_owned())
}

pub fn parse_arrow_field_type(raw: &str) -> Result<DataType> {
    parse_field_data_type(raw).map_err(|error| {
        CdfError::contract(format!("invalid Arrow type declaration {raw:?}: {error}"))
    })
}

#[derive(Clone, Copy)]
enum DecimalWidth {
    Decimal128,
    Decimal256,
}

const ALL_TIME_UNITS: &[TimeUnit] = &[
    TimeUnit::Second,
    TimeUnit::Millisecond,
    TimeUnit::Microsecond,
    TimeUnit::Nanosecond,
];

fn decimal_type(
    raw: &str,
    body: &str,
    width: DecimalWidth,
) -> std::result::Result<DataType, String> {
    let args = split_top_level(body, ',')?;
    if args.len() != 2 {
        return Err(format!("{raw} requires precision and scale"));
    }
    let precision = args[0]
        .trim()
        .parse::<u8>()
        .map_err(|_| format!("{raw} precision must be an unsigned integer"))?;
    let scale = args[1]
        .trim()
        .parse::<i8>()
        .map_err(|_| format!("{raw} scale must be an integer"))?;

    let (max_precision, max_scale) = match width {
        DecimalWidth::Decimal128 => (DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE),
        DecimalWidth::Decimal256 => (DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE),
    };
    if precision == 0 || precision > max_precision {
        return Err(format!(
            "{raw} precision must be between 1 and {max_precision}"
        ));
    }
    if i16::from(scale).abs() > i16::from(max_scale) {
        return Err(format!(
            "{raw} scale must be between -{max_scale} and {max_scale}"
        ));
    }

    Ok(match width {
        DecimalWidth::Decimal128 => DataType::Decimal128(precision, scale),
        DecimalWidth::Decimal256 => DataType::Decimal256(precision, scale),
    })
}

fn date_type(body: &str) -> std::result::Result<DataType, String> {
    match body.trim().to_ascii_lowercase().as_str() {
        "day" | "days" | "d" => Ok(DataType::Date32),
        "ms" | "millisecond" | "milliseconds" => Ok(DataType::Date64),
        other => Err(format!("unsupported date unit `{other}`")),
    }
}

fn time_type(body: &str) -> std::result::Result<DataType, String> {
    match time_unit(body, ALL_TIME_UNITS)? {
        TimeUnit::Second => Ok(DataType::Time32(TimeUnit::Second)),
        TimeUnit::Millisecond => Ok(DataType::Time32(TimeUnit::Millisecond)),
        TimeUnit::Microsecond => Ok(DataType::Time64(TimeUnit::Microsecond)),
        TimeUnit::Nanosecond => Ok(DataType::Time64(TimeUnit::Nanosecond)),
    }
}

fn timestamp_type(body: &str) -> std::result::Result<DataType, String> {
    let args = split_top_level(body, ',')?;
    if !(1..=2).contains(&args.len()) {
        return Err("timestamp requires a unit and optional timezone".to_owned());
    }
    let unit = time_unit(args[0], ALL_TIME_UNITS)?;
    let timezone = args
        .get(1)
        .map(|timezone| trim_quotes(timezone.trim()).to_owned().into());
    Ok(DataType::Timestamp(unit, timezone))
}

fn struct_type(body: &str) -> std::result::Result<DataType, String> {
    let fields = split_top_level(body, ',')?
        .into_iter()
        .map(|field| {
            let (name, field_type) = split_once_top_level(field, ':')?
                .ok_or_else(|| format!("struct field `{field}` must use `name: type`"))?;
            let name = name.trim();
            if name.is_empty() {
                return Err(format!("struct field `{field}` has an empty name"));
            }
            Ok(Field::new(
                name,
                parse_field_data_type(field_type.trim())?,
                true,
            ))
        })
        .collect::<std::result::Result<Vec<_>, String>>()?;
    Ok(DataType::Struct(Fields::from(fields)))
}

fn map_type(body: &str) -> std::result::Result<DataType, String> {
    let args = split_top_level(body, ',')?;
    if args.len() != 2 {
        return Err("map requires key and value types".to_owned());
    }
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", parse_field_data_type(args[0].trim())?, false),
            Field::new("value", parse_field_data_type(args[1].trim())?, true),
        ])),
        false,
    );
    Ok(DataType::Map(Arc::new(entries), false))
}

fn time_unit(value: &str, allowed: &[TimeUnit]) -> std::result::Result<TimeUnit, String> {
    let unit = match value.trim().to_ascii_lowercase().as_str() {
        "s" | "sec" | "second" | "seconds" => TimeUnit::Second,
        "ms" | "millisecond" | "milliseconds" => TimeUnit::Millisecond,
        "us" | "microsecond" | "microseconds" => TimeUnit::Microsecond,
        "ns" | "nanosecond" | "nanoseconds" => TimeUnit::Nanosecond,
        other => return Err(format!("unsupported time unit `{other}`")),
    };
    if allowed.contains(&unit) {
        Ok(unit)
    } else {
        Err(format!(
            "time unit `{}` is not valid in this type",
            value.trim()
        ))
    }
}

fn enclosed_body<'a>(
    value: &'a str,
    prefix: &str,
    open: char,
    close: char,
) -> std::result::Result<Option<&'a str>, String> {
    let Some(after_prefix) = value.strip_prefix(prefix) else {
        return Ok(None);
    };
    let rest = after_prefix.trim_start();
    if !rest.starts_with(open) {
        return Ok(None);
    }

    let mut depth = 0_i32;
    for (index, ch) in rest.char_indices() {
        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                let trailing = &rest[index + ch.len_utf8()..];
                if trailing.trim().is_empty() {
                    return Ok(Some(&rest[open.len_utf8()..index]));
                }
                return Err(format!("unexpected trailing content `{}`", trailing.trim()));
            }
        }
    }

    Err(format!("missing closing `{close}`"))
}

fn split_top_level(value: &str, delimiter: char) -> std::result::Result<Vec<&str>, String> {
    let mut parts = Vec::new();
    let mut start = 0;
    for index in top_level_delimiter_indices(value, delimiter)? {
        parts.push(&value[start..index]);
        start = index + delimiter.len_utf8();
    }

    parts.push(&value[start..]);
    Ok(parts)
}

fn split_once_top_level(
    value: &str,
    delimiter: char,
) -> std::result::Result<Option<(&str, &str)>, String> {
    let Some(index) = top_level_delimiter_indices(value, delimiter)?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    Ok(Some((
        &value[..index],
        &value[index + delimiter.len_utf8()..],
    )))
}

fn top_level_delimiter_indices(
    value: &str,
    delimiter: char,
) -> std::result::Result<Vec<usize>, String> {
    let mut indices = Vec::new();
    let mut angle_depth = 0_i32;
    let mut paren_depth = 0_i32;

    for (index, ch) in value.char_indices() {
        match ch {
            '<' => angle_depth += 1,
            '>' => {
                angle_depth -= 1;
                if angle_depth < 0 {
                    return Err("unexpected `>`".to_owned());
                }
            }
            '(' => paren_depth += 1,
            ')' => {
                paren_depth -= 1;
                if paren_depth < 0 {
                    return Err("unexpected `)`".to_owned());
                }
            }
            _ if ch == delimiter && angle_depth == 0 && paren_depth == 0 => {
                indices.push(index);
            }
            _ => {}
        }
    }

    if angle_depth != 0 {
        return Err("unbalanced angle brackets".to_owned());
    }
    if paren_depth != 0 {
        return Err("unbalanced parentheses".to_owned());
    }

    Ok(indices)
}

fn trim_quotes(value: &str) -> &str {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        if (bytes[0] == b'"' && bytes[value.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[value.len() - 1] == b'\'')
        {
            return &value[1..value.len() - 1];
        }
    }
    value
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
