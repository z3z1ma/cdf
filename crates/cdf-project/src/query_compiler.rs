use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use arrow_schema::Schema;
use cdf_contract::RelationalExpressionPlan;
use cdf_declarative::{CompiledResource, ExecutionDeclaration, compile_execution_extent};
use cdf_engine::{
    ParsedProjectQuery, ProjectSqlSpan, analyze_project_query_at, parse_project_query_at,
};
use cdf_kernel::{
    CanonicalArrowSchema, CdfError, CursorOrderingClaim, CursorSpec, DestinationSheet,
    ResourceDescriptor, Result, SchemaSource, ScopeKey, TargetName, TrustLevel,
    TypePolicyAllowances, WriteDisposition,
};
use cdf_runtime::{
    SourceCompileContext, SourceCompileRequest, SourceCursorPushdown, SourceRegistry,
};
use cdf_semantic::{SemanticAuthority, SemanticCatalog};
use serde::{Deserialize, Serialize};

use crate::{
    AuthoredDisposition, AuthoredResourceEnvelope, AuthoredResourceFile, AuthoredResourceForm,
    ProjectConfig, ProjectResourceInput, ProjectResourceInventory,
    ProjectResourceSelectionResolution, ProjectSourceBinding, TrustPreset, WriteDispositionPreset,
    internal::validate_secret_references_in_json,
    inventory_project_resources, parse_resource_file,
    project_inputs::{read_project_resource_path, resolve_project_source_binding},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionOrigin {
    Authored,
    ProjectDefault,
    BuiltInDefault,
    ResourcePathDefault,
    Absent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedResourceValue<T> {
    pub value: T,
    pub origin: ResolutionOrigin,
    pub canonical_identity: String,
    pub authored_span: Option<ProjectSqlSpan>,
}

impl<T> ResolvedResourceValue<T>
where
    T: Clone + Serialize,
{
    fn new(
        value: T,
        origin: ResolutionOrigin,
        authored_span: Option<ProjectSqlSpan>,
    ) -> Result<Self> {
        Ok(Self {
            canonical_identity: cdf_runtime::artifact_hash(&value)?,
            value,
            origin,
            authored_span,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectiveResourceEnvelope {
    pub target: ResolvedResourceValue<TargetName>,
    pub disposition: ResolvedResourceValue<WriteDisposition>,
    pub merge_keys: ResolvedResourceValue<Vec<String>>,
    pub cursor: ResolvedResourceValue<Option<String>>,
    pub trust: ResolvedResourceValue<TrustLevel>,
    pub semantics: ResolvedResourceValue<BTreeMap<String, String>>,
    pub execution: ResolvedResourceValue<cdf_kernel::ExecutionExtent>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfiguredSourceIdentity {
    pub configured_source: String,
    pub source_type: String,
    pub base_configuration_hash: String,
    pub overlay_configuration_hash: String,
    pub effective_configuration_hash: String,
    pub driver_id: String,
    pub driver_version: String,
    pub driver_option_schema_hash: String,
    pub driver_descriptor_hash: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectQueryCompilation {
    pub relative_path: String,
    pub namespace: String,
    pub resource_name: String,
    pub resource_id: String,
    pub default_target: TargetName,
    pub authored_sql: String,
    pub authored_content_hash: String,
    pub authored_form: AuthoredResourceForm,
    pub authored_file: AuthoredResourceFile,
    pub parsed_query: ParsedProjectQuery,
    pub configured_source: ProjectConfiguredSourceIdentity,
    pub source_node_id: String,
    pub effective: EffectiveResourceEnvelope,
    pub relational_plan: Option<RelationalExpressionPlan>,
}

#[derive(Clone, Debug)]
pub struct CompiledProjectResource {
    pub resource: CompiledResource,
    pub query: ProjectQueryCompilation,
}

#[derive(Clone, Debug)]
pub struct ProjectInputSchemaAuthority {
    pub schema_source: SchemaSource,
    pub schema: Schema,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StaticProjectResourceValidation {
    pub configured_source: String,
}

pub(crate) fn validate_static_configured_source(
    registry: &SourceRegistry,
    config: &ProjectConfig,
    environment: &str,
    source_name: &str,
) -> Result<()> {
    let source_name = crate::ProjectSourceName::new(source_name, "cdf.toml configured source")?;
    let source = config.sources.get(source_name.as_str()).ok_or_else(|| {
        CdfError::contract(format!(
            "configured source {:?} is not declared in cdf.toml",
            source_name.as_str()
        ))
    })?;
    if source.source_type.trim().is_empty() {
        return Err(CdfError::contract(format!(
            "[sources.{}] requires one non-empty `type`",
            source_name.as_str()
        )));
    }
    let overlay = config
        .environments
        .get(environment)
        .and_then(|environment| environment.sources.get(source_name.as_str()));
    if overlay
        .and_then(|overlay| overlay.source_type.as_ref())
        .is_some()
    {
        return Err(CdfError::contract(format!(
            "[environments.{environment}.sources.{}] may not override immutable source `type`; remove `type` from the environment overlay",
            source_name.as_str()
        )));
    }
    let mut effective_options = source.options.clone();
    if let Some(overlay) = overlay {
        effective_options.extend(overlay.options.clone());
    }
    registry
        .validate_source_configuration(&source.source_type, &effective_options)
        .map_err(|error| {
            CdfError::new(
                error.kind,
                format!(
                    "[sources.{}] effective configuration for environment `{environment}`: {}",
                    source_name.as_str(),
                    error.message
                ),
            )
        })?;
    validate_secret_references_in_json(&serde_json::to_value(&effective_options).map_err(
        |error| CdfError::internal(format!("serialize source options for validation: {error}")),
    )?)
}

pub(crate) fn validate_static_query_project_resource(
    registry: &SourceRegistry,
    config: &ProjectConfig,
    environment: &str,
    input: &ProjectResourceInput,
    semantic_catalog: &SemanticCatalog,
) -> Result<StaticProjectResourceValidation> {
    let authored = parse_resource_file(&input.sql, &input.relative_path)?;
    let parsed = parse_project_query_at(
        &authored.query_sql,
        &input.relative_path,
        authored.query_span.start_line,
        authored.query_span.start_column,
    )?;
    let source_name =
        crate::ProjectSourceName::new(&parsed.upstream.configured_source, &input.relative_path)?;
    let source = config.sources.get(source_name.as_str()).ok_or_else(|| {
        CdfError::contract(format!(
            "{}:{}:{}: upstream references unknown configured source {:?}; declare [sources.{}] in cdf.toml",
            input.relative_path,
            parsed.upstream.span.start_line,
            parsed.upstream.span.start_column,
            parsed.upstream.configured_source,
            parsed.upstream.configured_source,
        ))
        .with_code("CDF-SOURCE-UNKNOWN")
    })?;
    validate_static_configured_source(registry, config, environment, source_name.as_str())?;
    registry
        .validate_resource_configuration(&source.source_type, &parsed.upstream.resource_options)
        .map_err(|error| {
            CdfError::new(
                error.kind,
                format!(
                    "[CDF-SOURCE-RESOURCE-OPTIONS] {}:{}:{}: {}",
                    input.relative_path,
                    parsed.upstream.span.start_line,
                    parsed.upstream.span.start_column,
                    error.message
                ),
            )
        })?;
    validate_secret_references_in_json(
        &serde_json::to_value(&parsed.upstream.resource_options).map_err(|error| {
            CdfError::internal(format!(
                "serialize resource options for validation: {error}"
            ))
        })?,
    )?;
    let effective = resolve_envelope(config, &authored.envelope, &input.default_target)?;
    for (field, reference) in &effective.semantics.value {
        if field.starts_with("_cdf_") {
            return Err(CdfError::contract(format!(
                "[CDF-SEMANTIC-CONTROL] protected CDF field {field:?} cannot receive an authored semantic annotation"
            )));
        }
        let reference = semantic_catalog.parse_reference(reference, SemanticAuthority::Authored)?;
        semantic_catalog.resolve_reference(&reference, SemanticAuthority::Authored)?;
    }
    Ok(StaticProjectResourceValidation {
        configured_source: source_name.as_str().to_owned(),
    })
}

impl ProjectInputSchemaAuthority {
    pub fn new(schema_source: SchemaSource, schema: Schema) -> Result<Self> {
        if matches!(schema_source, SchemaSource::Discover) && !schema.fields().is_empty() {
            return Err(CdfError::contract(
                "nonempty project input schema requires declared, pinned, hints, or contract authority",
            ));
        }
        Ok(Self {
            schema_source,
            schema,
        })
    }
}

pub fn compile_query_project_resources(
    registry: &SourceRegistry,
    config: &ProjectConfig,
    project_root: &Path,
    environment: &str,
    destination: &DestinationSheet,
    semantic_catalog: &SemanticCatalog,
    input_schemas: &BTreeMap<String, ProjectInputSchemaAuthority>,
) -> Result<Vec<CompiledProjectResource>> {
    let inventory = inventory_project_resources(project_root, config, environment, registry)?;
    compile_inventory(
        registry,
        config,
        project_root,
        destination,
        semantic_catalog,
        input_schemas,
        inventory,
    )
}

pub fn compile_selected_query_project_resources(
    registry: &SourceRegistry,
    config: &ProjectConfig,
    project_root: &Path,
    environment: &str,
    destination: &DestinationSheet,
    semantic_catalog: &SemanticCatalog,
    selection: &ProjectResourceSelectionResolution,
) -> Result<Vec<CompiledProjectResource>> {
    let mut compiled = Vec::with_capacity(selection.resources.len());
    for path in &selection.resources {
        let input = read_project_resource_path(path)?;
        let authored = parse_resource_file(&input.sql, &input.relative_path)?;
        let parsed = parse_project_query_at(
            &authored.query_sql,
            &input.relative_path,
            authored.query_span.start_line,
            authored.query_span.start_column,
        )?;
        let source_name = crate::ProjectSourceName::new(
            &parsed.upstream.configured_source,
            &input.relative_path,
        )?;
        let source = resolve_project_source_binding(config, environment, registry, &source_name)
            .map_err(|error| {
                if config.sources.contains_key(source_name.as_str()) {
                    error
                } else {
                    CdfError::contract(format!(
                        "{}:{}:{}: upstream references unknown configured source {:?}; declare [sources.{}] in cdf.toml",
                        input.relative_path,
                        parsed.upstream.span.start_line,
                        parsed.upstream.span.start_column,
                        parsed.upstream.configured_source,
                        parsed.upstream.configured_source,
                    ))
                    .with_code("CDF-SOURCE-UNKNOWN")
                }
            })?;
        registry
            .validate_resource_configuration(&source.source_type, &parsed.upstream.resource_options)
            .map_err(|error| {
                CdfError::new(
                    error.kind,
                    format!(
                        "[CDF-SOURCE-RESOURCE-OPTIONS] {}:{}:{}: {}",
                        input.relative_path,
                        parsed.upstream.span.start_line,
                        parsed.upstream.span.start_column,
                        error.message
                    ),
                )
            })?;
        compiled.push(compile_input(
            registry,
            config,
            project_root,
            destination,
            semantic_catalog,
            &input,
            &authored,
            parsed,
            &source,
            None,
        )?);
    }
    Ok(compiled)
}

fn compile_inventory(
    registry: &SourceRegistry,
    config: &ProjectConfig,
    project_root: &Path,
    destination: &DestinationSheet,
    semantic_catalog: &SemanticCatalog,
    input_schemas: &BTreeMap<String, ProjectInputSchemaAuthority>,
    inventory: ProjectResourceInventory,
) -> Result<Vec<CompiledProjectResource>> {
    let mut referenced_sources = BTreeSet::new();
    let mut compiled = Vec::with_capacity(inventory.resources.len());
    for input in &inventory.resources {
        let authored = parse_resource_file(&input.sql, &input.relative_path)?;
        let parsed = parse_project_query_at(
            &authored.query_sql,
            &input.relative_path,
            authored.query_span.start_line,
            authored.query_span.start_column,
        )?;
        let source_name = crate::ProjectSourceName::new(
            &parsed.upstream.configured_source,
            &input.relative_path,
        )?;
        let source = inventory.sources.get(&source_name).ok_or_else(|| {
            CdfError::contract(format!(
                "{}:{}:{}: upstream references unknown configured source {:?}; declare [sources.{}] in cdf.toml",
                input.relative_path,
                parsed.upstream.span.start_line,
                parsed.upstream.span.start_column,
                parsed.upstream.configured_source,
                parsed.upstream.configured_source,
            ))
            .with_code("CDF-SOURCE-UNKNOWN")
        })?;
        referenced_sources.insert(source.name.clone());
        registry
            .validate_resource_configuration(&source.source_type, &parsed.upstream.resource_options)
            .map_err(|error| {
                CdfError::new(
                    error.kind,
                    format!(
                        "[CDF-SOURCE-RESOURCE-OPTIONS] {}:{}:{}: {}",
                        input.relative_path,
                        parsed.upstream.span.start_line,
                        parsed.upstream.span.start_column,
                        error.message
                    ),
                )
            })?;
        compiled.push(compile_input(
            registry,
            config,
            project_root,
            destination,
            semantic_catalog,
            input,
            &authored,
            parsed,
            source,
            input_schemas.get(input.resource_id.as_str()),
        )?);
    }
    let unreferenced = inventory
        .sources
        .keys()
        .filter(|source| !referenced_sources.contains(*source))
        .map(|source| source.as_str())
        .collect::<Vec<_>>();
    if !unreferenced.is_empty() {
        return Err(CdfError::contract(format!(
            "[CDF-SOURCE-UNREFERENCED] configured source(s) {} are not referenced by any accepted cdf/<namespace>/<resource>.cdf.sql query",
            unreferenced.join(", ")
        )));
    }
    Ok(compiled)
}

#[allow(clippy::too_many_arguments)]
fn compile_input(
    registry: &SourceRegistry,
    config: &ProjectConfig,
    project_root: &Path,
    destination: &DestinationSheet,
    semantic_catalog: &SemanticCatalog,
    input: &ProjectResourceInput,
    authored: &AuthoredResourceFile,
    parsed: ParsedProjectQuery,
    source: &ProjectSourceBinding,
    input_schema: Option<&ProjectInputSchemaAuthority>,
) -> Result<CompiledProjectResource> {
    let effective = resolve_envelope(config, &authored.envelope, &input.default_target)?;
    let schema_source = input_schema
        .map(|authority| authority.schema_source.clone())
        .unwrap_or(SchemaSource::Discover);
    let schema = input_schema
        .map(|authority| authority.schema.clone())
        .unwrap_or_else(Schema::empty);
    let descriptor = ResourceDescriptor {
        resource_id: input.resource_id.clone(),
        schema_source,
        primary_key: Vec::new(),
        merge_key: effective.merge_keys.value.clone(),
        cursor: effective.cursor.value.as_ref().map(|field| CursorSpec {
            field: field.clone(),
            ordering: CursorOrderingClaim::Exact,
            lag_tolerance_ms: 0,
        }),
        write_disposition: effective.disposition.value.clone(),
        deduplication: None,
        contract: None,
        state_scope: ScopeKey::Resource,
        freshness: None,
        trust_level: effective.trust.value.clone(),
    };
    descriptor.validate()?;
    let source_plan = registry.compile(SourceCompileRequest {
        source_kind: source.source_type.clone(),
        context: SourceCompileContext {
            source_name: source.name.as_str().to_owned(),
            project_root: Some(project_root.to_path_buf()),
            cursor_pushdown: effective
                .cursor
                .value
                .as_ref()
                .map(|_| SourceCursorPushdown {
                    parameter: None,
                    fidelity: cdf_kernel::PushdownFidelity::Exact,
                }),
        },
        source_options: source.effective_options.clone(),
        resource_options: parsed.upstream.resource_options.clone(),
        descriptor,
        schema,
        type_policy_allowances: TypePolicyAllowances::default(),
        effective_schema_runtime: None,
        baseline_observation_schema_catalog: Vec::new(),
    })?;
    validate_effective_applicability(&effective, &source_plan, destination)?;
    let resource = CompiledResource::from_compiled_source_with_execution(
        source.name.as_str(),
        input.resource_name.as_str(),
        Some(project_root.to_path_buf()),
        source_plan,
        effective.execution.value.clone(),
    )?;
    let source_identity = configured_source_identity(source);
    let source_node_id = cdf_runtime::artifact_hash(&(
        input.resource_id.as_str(),
        source.name.as_str(),
        source.effective_hash.as_str(),
        source.driver_descriptor_hash.as_str(),
        parsed.upstream.canonical_arguments_hash.as_str(),
    ))?;
    let query = ProjectQueryCompilation {
        relative_path: input.relative_path.clone(),
        namespace: input.namespace.as_str().to_owned(),
        resource_name: input.resource_name.as_str().to_owned(),
        resource_id: input.resource_id.as_str().to_owned(),
        default_target: input.default_target.clone(),
        authored_sql: input.sql.clone(),
        authored_content_hash: input.content_hash.as_str().to_owned(),
        authored_form: authored.form,
        authored_file: authored.clone(),
        parsed_query: parsed,
        configured_source: source_identity,
        source_node_id,
        effective,
        relational_plan: None,
    };
    let pending = CompiledProjectResource { resource, query };
    if pending.resource.source_plan().schema.fields().is_empty() {
        Ok(pending)
    } else {
        finalize_query_project_resource(pending, semantic_catalog)
    }
}

pub fn finalize_query_project_resource(
    mut compiled: CompiledProjectResource,
    semantic_catalog: &SemanticCatalog,
) -> Result<CompiledProjectResource> {
    let input_schema = compiled.resource.source_plan().schema.clone();
    if input_schema.fields().is_empty() {
        return Err(CdfError::contract(format!(
            "[CDF-SCHEMA-UNRESOLVED] resource {:?} requires a pinned/discovered input schema before SQL analysis",
            compiled.query.resource_id
        )));
    }
    let analyzed = analyze_project_query_at(
        &compiled.query.authored_file.query_sql,
        &compiled.query.relative_path,
        compiled.query.authored_file.query_span.start_line,
        compiled.query.authored_file.query_span.start_column,
        &input_schema,
        Vec::new(),
    )?;
    if analyzed.upstream != compiled.query.parsed_query.upstream
        || analyzed.authored_ast_hash != compiled.query.parsed_query.authored_ast_hash
    {
        return Err(CdfError::internal(
            "query analysis changed the previously parsed upstream or authored AST identity",
        ));
    }
    let relational_plan = apply_semantics(
        analyzed.relational_plan,
        &compiled.query.effective.semantics.value,
        semantic_catalog,
    )?;
    validate_output_bindings(&compiled.query.effective, &relational_plan.output_schema)?;
    compiled.resource = compiled
        .resource
        .with_relational_expression_plan(relational_plan.clone())?;
    compiled.query.relational_plan = Some(relational_plan);
    Ok(compiled)
}

fn resolve_envelope(
    config: &ProjectConfig,
    authored: &AuthoredResourceEnvelope,
    default_target: &TargetName,
) -> Result<EffectiveResourceEnvelope> {
    let target = match &authored.target {
        Some(value) => ResolvedResourceValue::new(
            value.value.clone(),
            ResolutionOrigin::Authored,
            Some(value.span.clone()),
        )?,
        None => ResolvedResourceValue::new(
            default_target.clone(),
            ResolutionOrigin::ResourcePathDefault,
            None,
        )?,
    };
    let (disposition, merge_keys, disposition_span) = match &authored.disposition {
        Some(value) => match &value.value {
            AuthoredDisposition::Append => (
                WriteDisposition::Append,
                Vec::new(),
                Some(value.span.clone()),
            ),
            AuthoredDisposition::Replace => (
                WriteDisposition::Replace,
                Vec::new(),
                Some(value.span.clone()),
            ),
            AuthoredDisposition::Merge { keys } => (
                WriteDisposition::Merge,
                keys.iter().map(|key| key.value.clone()).collect(),
                Some(value.span.clone()),
            ),
        },
        None => match config.defaults.write_disposition {
            Some(WriteDispositionPreset::Append) => (WriteDisposition::Append, Vec::new(), None),
            Some(WriteDispositionPreset::Replace) => (WriteDisposition::Replace, Vec::new(), None),
            None => (WriteDisposition::Replace, Vec::new(), None),
        },
    };
    let disposition_origin = if authored.disposition.is_some() {
        ResolutionOrigin::Authored
    } else if config.defaults.write_disposition.is_some() {
        ResolutionOrigin::ProjectDefault
    } else {
        ResolutionOrigin::BuiltInDefault
    };
    let disposition =
        ResolvedResourceValue::new(disposition, disposition_origin, disposition_span.clone())?;
    let merge_keys = ResolvedResourceValue::new(merge_keys, disposition_origin, disposition_span)?;
    let cursor = match &authored.cursor {
        Some(value) => ResolvedResourceValue::new(
            Some(value.value.clone()),
            ResolutionOrigin::Authored,
            Some(value.span.clone()),
        )?,
        None => ResolvedResourceValue::new(None::<String>, ResolutionOrigin::Absent, None)?,
    };
    let (trust, trust_origin, trust_span) = match &authored.trust {
        Some(value) => (
            trust_level(&value.value),
            ResolutionOrigin::Authored,
            Some(value.span.clone()),
        ),
        None => match config.defaults.trust.as_ref() {
            Some(value) => (trust_level(value), ResolutionOrigin::ProjectDefault, None),
            None => (
                TrustLevel::Experimental,
                ResolutionOrigin::BuiltInDefault,
                None,
            ),
        },
    };
    let trust = ResolvedResourceValue::new(trust, trust_origin, trust_span)?;
    let semantics = authored
        .semantics
        .iter()
        .map(|binding| (binding.field.value.clone(), binding.reference.value.clone()))
        .collect::<BTreeMap<_, _>>();
    let semantics_span = authored
        .semantics
        .first()
        .map(|binding| binding.field.span.clone());
    let semantics_origin = if semantics.is_empty() {
        ResolutionOrigin::Absent
    } else {
        ResolutionOrigin::Authored
    };
    let semantics = ResolvedResourceValue::new(semantics, semantics_origin, semantics_span)?;
    let (execution_declaration, execution_origin, execution_span) = match &authored.execution {
        Some(value) => (
            value.value.clone(),
            ResolutionOrigin::Authored,
            Some(value.span.clone()),
        ),
        None => match &config.defaults.execution {
            Some(value) => (value.clone(), ResolutionOrigin::ProjectDefault, None),
            None => (
                ExecutionDeclaration::Bounded,
                ResolutionOrigin::BuiltInDefault,
                None,
            ),
        },
    };
    let execution = ResolvedResourceValue::new(
        compile_execution_extent(Some(&execution_declaration))?,
        execution_origin,
        execution_span,
    )?;
    Ok(EffectiveResourceEnvelope {
        target,
        disposition,
        merge_keys,
        cursor,
        trust,
        semantics,
        execution,
    })
}

pub(crate) fn current_effective_resource_envelope(
    config: &ProjectConfig,
    authored_sql: &str,
    relative_path: &str,
    default_target: &TargetName,
) -> Result<EffectiveResourceEnvelope> {
    let authored = parse_resource_file(authored_sql, relative_path)?;
    resolve_envelope(config, &authored.envelope, default_target)
}

fn validate_effective_applicability(
    effective: &EffectiveResourceEnvelope,
    source: &cdf_runtime::CompiledSourcePlan,
    destination: &DestinationSheet,
) -> Result<()> {
    if !destination
        .supported_dispositions
        .contains(&effective.disposition.value)
    {
        return Err(CdfError::contract(format!(
            "[CDF-DISPOSITION-DESTINATION] destination {} does not support {:?}",
            destination.destination, effective.disposition.value
        )));
    }
    if effective.disposition.origin == ResolutionOrigin::BuiltInDefault
        && (!source.execution_capabilities.bounded
            || source.resource_capabilities.replay == cdf_kernel::ReplaySupport::None)
    {
        return Err(CdfError::contract(
            "[CDF-DISPOSITION-DEFAULT] built-in REPLACE requires a proven bounded replayable source; author DISPOSITION or an applicable [defaults].write_disposition",
        ));
    }
    if effective.execution.value.is_bounded() && !source.execution_capabilities.bounded {
        return Err(CdfError::contract(
            "[CDF-EXECUTION-BOUNDED] bounded execution requires a source that truthfully advertises bounded completion",
        ));
    }
    Ok(())
}

fn validate_output_bindings(
    effective: &EffectiveResourceEnvelope,
    schema: &CanonicalArrowSchema,
) -> Result<()> {
    let schema = schema.to_arrow()?;
    for (label, fields) in [
        ("merge key", effective.merge_keys.value.as_slice()),
        ("cursor", effective.cursor.value.as_slice()),
    ] {
        for field in fields {
            let matches = schema
                .fields()
                .iter()
                .filter(|candidate| candidate.name() == field)
                .count();
            if matches != 1 {
                return Err(CdfError::contract(format!(
                    "[CDF-OUTPUT-BINDING] {label} {field:?} must resolve exactly once against the final output schema; matched {matches} fields"
                )));
            }
        }
    }
    Ok(())
}

fn apply_semantics(
    plan: RelationalExpressionPlan,
    semantics: &BTreeMap<String, String>,
    catalog: &SemanticCatalog,
) -> Result<RelationalExpressionPlan> {
    if semantics.is_empty() {
        return Ok(plan);
    }
    let output = plan.output_schema.to_arrow()?;
    let mut fields = output
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    for (name, reference) in semantics {
        if name.starts_with("_cdf_") {
            return Err(CdfError::contract(format!(
                "[CDF-SEMANTIC-CONTROL] protected CDF field {name:?} cannot receive an authored semantic annotation"
            )));
        }
        let matches = fields
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() == name)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let [index] = matches.as_slice() else {
            return Err(CdfError::contract(format!(
                "[CDF-SEMANTIC-FIELD] semantic field {name:?} must resolve exactly once against the final output schema; matched {} fields",
                matches.len()
            )));
        };
        fields[*index] = catalog.apply_reference(
            fields[*index].clone(),
            reference,
            SemanticAuthority::Authored,
        )?;
    }
    let output = Schema::new_with_metadata(fields, output.metadata().clone());
    RelationalExpressionPlan::current(
        plan.input_schema,
        plan.filter,
        plan.projection,
        CanonicalArrowSchema::from_arrow(&output)?,
        plan.control_fields,
    )
}

fn configured_source_identity(source: &ProjectSourceBinding) -> ProjectConfiguredSourceIdentity {
    ProjectConfiguredSourceIdentity {
        configured_source: source.name.as_str().to_owned(),
        source_type: source.source_type.clone(),
        base_configuration_hash: source.base_hash.as_str().to_owned(),
        overlay_configuration_hash: source.overlay_hash.as_str().to_owned(),
        effective_configuration_hash: source.effective_hash.as_str().to_owned(),
        driver_id: source.driver.driver_id.as_str().to_owned(),
        driver_version: source.driver.driver_version.clone(),
        driver_option_schema_hash: source.driver.option_schema_hash.clone(),
        driver_descriptor_hash: source.driver_descriptor_hash.clone(),
    }
}

fn trust_level(value: &TrustPreset) -> TrustLevel {
    match value {
        TrustPreset::Experimental => TrustLevel::Experimental,
        TrustPreset::Governed => TrustLevel::Governed,
    }
}
