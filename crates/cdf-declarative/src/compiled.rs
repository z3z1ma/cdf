use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use arrow_schema::{
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
    DataType, Field, Fields, Schema, SchemaRef, TimeUnit,
};
use cdf_contract::{IdentifierPolicy, normalize_arrow_schema};
use cdf_http::{
    AuthScheme, EgressAllowlist, PaginationConfig, QuotaHeaderPolicy, RateLimitPolicy,
    ResetHeaderSemantics, SecretUri,
};
use cdf_kernel::{
    BackpressureSupport, BatchStream, BoxFuture, CapabilitySupport, CdfError, ContractRef,
    CursorOrderingClaim, CursorSpec, DeliveryGuarantee, EstimateSupport, FilterCapabilities,
    FreshnessSpec, IncrementalShape, PartitionId, PartitionPlan, PartitioningCapabilities, PlanId,
    PushdownFidelity, PushedPredicate, QueryableResource, ReplaySupport, ResourceCapabilities,
    ResourceDescriptor, ResourceId, ResourceStream, Result, ScanPlan, ScanRequest, SchemaHash,
    SchemaSource, ScopeKey, ScopeKind, TrustLevel, WriteDisposition, with_cdf_metadata,
};
use sha2::{Digest, Sha256};

use crate::declarations::*;
use crate::file_runtime::{open_file_resource, open_file_resource_preview};
use crate::rest_runtime::{
    CURSOR_QUERY_PARAM_METADATA, CURSOR_QUERY_VALUE_METADATA, cursor_pushdown_value,
};
use crate::sql_runtime::{
    sql_capabilities_for, sql_partition_for_plan, sql_predicate_fidelity_for,
};

#[derive(Clone, Debug)]
pub struct CompiledResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    capabilities: ResourceCapabilities,
    plan: CompiledResourcePlan,
}

impl CompiledResource {
    pub fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    pub fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    pub fn plan(&self) -> &CompiledResourcePlan {
        &self.plan
    }

    pub fn open_preview(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>> {
        match &self.plan {
            CompiledResourcePlan::Files(plan) => open_file_resource_preview(
                &self.descriptor,
                Arc::clone(&self.schema),
                plan,
                partition,
            ),
            CompiledResourcePlan::Rest(_) | CompiledResourcePlan::Sql(_) => Box::pin(async {
                Err(CdfError::internal(
                    "declarative resource preview execution is outside the MVP compiler crate",
                ))
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompiledResourcePlan {
    Rest(Box<RestResourcePlan>),
    Sql(SqlResourcePlan),
    Files(FileResourcePlan),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestResourcePlan {
    pub source: String,
    pub base_url: String,
    pub path: String,
    pub params: BTreeMap<String, String>,
    pub record_selector: String,
    pub pagination: Option<PaginationConfig>,
    pub auth: Option<AuthScheme>,
    pub rate_limit: RateLimitPolicy,
    pub respect_headers: Vec<String>,
    pub allowlist: EgressAllowlist,
    pub cursor_param: Option<String>,
    pub cursor_filter_fidelity: PushdownFidelity,
    pub records_transform: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlResourcePlan {
    pub source: String,
    pub dialect: Option<String>,
    pub connection: SecretUri,
    pub query: Option<String>,
    pub table: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileResourcePlan {
    pub source: String,
    pub root: String,
    pub glob: String,
    pub format: FileFormatDeclaration,
}

pub fn compile_document(document: &DeclarativeDocument) -> Result<Vec<CompiledResource>> {
    compile_document_inner(document, None)
}

pub fn compile_document_with_project_root(
    document: &DeclarativeDocument,
    project_root: impl AsRef<Path>,
) -> Result<Vec<CompiledResource>> {
    compile_document_inner(document, Some(project_root.as_ref()))
}

fn compile_document_inner(
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
            compile_resource(name, &source_name, source, resource, project_root)
        })
        .collect()
}

pub fn validate_document(document: &DeclarativeDocument) -> Result<()> {
    compile_document(document).map(drop)
}

impl ResourceStream for CompiledResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        Ok(vec![partition_for_plan(
            &self.descriptor,
            &self.schema,
            &self.plan,
            Some(request),
        )?])
    }

    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>> {
        match &self.plan {
            CompiledResourcePlan::Files(plan) => {
                open_file_resource(&self.descriptor, Arc::clone(&self.schema), plan, partition)
            }
            CompiledResourcePlan::Rest(_) | CompiledResourcePlan::Sql(_) => Box::pin(async {
                Err(CdfError::internal(
                    "declarative resource execution is outside the MVP compiler crate",
                ))
            }),
        }
    }
}

impl QueryableResource for CompiledResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        if request.resource_id != self.descriptor.resource_id {
            return Err(CdfError::contract(format!(
                "scan request resource `{}` does not match compiled resource `{}`",
                request.resource_id, self.descriptor.resource_id
            )));
        }

        let mut pushed_predicates = Vec::new();
        let mut unsupported_predicates = Vec::new();
        for predicate in &request.filters {
            match self.predicate_fidelity(&predicate.expression) {
                PushdownFidelity::Unsupported => unsupported_predicates.push(predicate.clone()),
                fidelity => pushed_predicates.push(PushedPredicate {
                    predicate: predicate.clone(),
                    fidelity,
                }),
            }
        }

        Ok(ScanPlan {
            plan_id: PlanId::new(format!("plan-{}", self.descriptor.resource_id))?,
            request: request.clone(),
            partitions: vec![partition_for_plan(
                &self.descriptor,
                &self.schema,
                &self.plan,
                Some(request),
            )?],
            pushed_predicates,
            unsupported_predicates,
            estimated_rows: None,
            estimated_bytes: None,
            delivery_guarantee: delivery_guarantee(&self.descriptor),
        })
    }
}

impl CompiledResource {
    fn predicate_fidelity(&self, expression: &str) -> PushdownFidelity {
        match &self.plan {
            CompiledResourcePlan::Rest(plan) => {
                if cursor_pushdown_value(&self.descriptor, plan, expression).is_some() {
                    plan.cursor_filter_fidelity.clone()
                } else {
                    PushdownFidelity::Unsupported
                }
            }
            CompiledResourcePlan::Sql(plan) => {
                sql_predicate_fidelity_for(&self.schema, plan, expression)
            }
            CompiledResourcePlan::Files(_) => PushdownFidelity::Unsupported,
        }
    }
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
    name: &str,
    source_name: &str,
    source: &SourceDeclaration,
    resource: &ResourceDeclaration,
    project_root: Option<&Path>,
) -> Result<CompiledResource> {
    validate_escape_hatch(resource)?;

    let resource_id = resource
        .id
        .clone()
        .unwrap_or_else(|| format!("{source_name}.{name}"));
    let descriptor_resource_id = ResourceId::new(resource_id.clone())?;
    let schema = compile_schema(resource)?;
    let schema_source = compile_schema_source(&resource_id, resource)?;
    let cursor = compile_cursor(resource.cursor.as_ref())?;
    let write_disposition = compile_write_disposition(resource)?;
    let merge_key = compile_merge_key(name, resource, &write_disposition)?;
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

    let plan = match source {
        SourceDeclaration::Rest(rest) => {
            CompiledResourcePlan::Rest(Box::new(compile_rest_plan(source_name, rest, resource)?))
        }
        SourceDeclaration::Sql(sql) => {
            CompiledResourcePlan::Sql(compile_sql_plan(source_name, sql, resource)?)
        }
        SourceDeclaration::Files(files) => CompiledResourcePlan::Files(compile_file_plan(
            source_name,
            files,
            resource,
            project_root,
        )?),
    };
    let capabilities = capabilities_for(&descriptor, &plan);

    Ok(CompiledResource {
        descriptor,
        schema: Arc::new(schema),
        capabilities,
        plan,
    })
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
    match &resource.schema {
        Some(schema) => Ok(SchemaSource::Declared {
            schema_hash: schema_hash(schema)?,
            source: format!("declarative:{resource_id}"),
        }),
        None => Ok(SchemaSource::Discovered { schema_hash: None }),
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

fn compile_rest_plan(
    source_name: &str,
    source: &RestSourceDeclaration,
    resource: &ResourceDeclaration,
) -> Result<RestResourcePlan> {
    let path = resource
        .path
        .clone()
        .ok_or_else(|| CdfError::contract("REST resources must declare path before compilation"))?;
    let record_selector = resource.records.clone().ok_or_else(|| {
        CdfError::contract("REST resources must declare records before compilation")
    })?;
    let auth = source.auth.as_ref().map(compile_auth).transpose()?;
    let (rate_limit, respect_headers) = compile_rate_limit(source.rate_limit.as_ref())?;
    let allowlist = if source.egress_allowlist.is_empty() {
        EgressAllowlist::allow_any()
    } else {
        EgressAllowlist::from_hosts(source.egress_allowlist.clone())
    };
    let cursor_filter_fidelity = resource
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.filter_fidelity.as_ref())
        .map(to_pushdown_fidelity)
        .unwrap_or(PushdownFidelity::Inexact);

    Ok(RestResourcePlan {
        source: source_name.to_owned(),
        base_url: source.base_url.clone(),
        path,
        params: resource
            .params
            .iter()
            .map(|(name, value)| (name.clone(), value.as_query_value()))
            .collect(),
        record_selector,
        pagination: resource
            .paginate
            .as_ref()
            .map(compile_pagination)
            .transpose()?,
        auth,
        rate_limit,
        respect_headers,
        allowlist,
        cursor_param: resource
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.param.clone()),
        cursor_filter_fidelity,
        records_transform: resource.records_transform.clone(),
    })
}

fn compile_sql_plan(
    source_name: &str,
    source: &SqlSourceDeclaration,
    resource: &ResourceDeclaration,
) -> Result<SqlResourcePlan> {
    if resource.query.is_none() && resource.table.is_none() {
        return Err(CdfError::contract(
            "SQL resources must declare query or table before compilation",
        ));
    }
    if resource.query.is_some() && resource.table.is_some() {
        return Err(CdfError::contract(
            "SQL resources must declare only one of query or table",
        ));
    }

    Ok(SqlResourcePlan {
        source: source_name.to_owned(),
        dialect: source.dialect.clone(),
        connection: SecretUri::new(source.connection.clone())?,
        query: resource.query.clone(),
        table: resource.table.clone(),
    })
}

fn compile_file_plan(
    source_name: &str,
    source: &FileSourceDeclaration,
    resource: &ResourceDeclaration,
    project_root: Option<&Path>,
) -> Result<FileResourcePlan> {
    Ok(FileResourcePlan {
        source: source_name.to_owned(),
        root: compile_file_root(&source.root, project_root)?,
        glob: resource.glob.clone().ok_or_else(|| {
            CdfError::contract("file resources must declare glob before compilation")
        })?,
        format: resource.format.clone().ok_or_else(|| {
            CdfError::contract("file resources must declare format before compilation")
        })?,
    })
}

fn compile_file_root(root: &str, project_root: Option<&Path>) -> Result<String> {
    let root_path = PathBuf::from(root);
    if root_path.is_absolute() {
        return path_to_string(&root_path);
    }
    if path_contains_parent_dir(&root_path) {
        return Err(CdfError::contract(
            "relative file source root must stay under the project root and cannot contain `..`",
        ));
    }
    match project_root {
        Some(project_root) => path_to_string(&absolute_project_root(project_root)?.join(root_path)),
        None => Ok(root.to_owned()),
    }
}

fn absolute_project_root(project_root: &Path) -> Result<PathBuf> {
    if project_root.is_absolute() {
        return Ok(project_root.to_path_buf());
    }
    let current_dir = std::env::current_dir().map_err(|error| {
        CdfError::internal(format!(
            "resolve current directory for project root: {error}"
        ))
    })?;
    Ok(current_dir.join(project_root))
}

fn path_contains_parent_dir(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir))
}

fn path_to_string(path: &Path) -> Result<String> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        CdfError::contract(format!(
            "file source root path is not valid UTF-8: {}",
            path.display()
        ))
    })
}

fn compile_auth(auth: &AuthDeclaration) -> Result<AuthScheme> {
    match auth {
        AuthDeclaration::Bearer { token } => Ok(AuthScheme::Bearer {
            token_uri: SecretUri::new(token.clone())?,
        }),
        AuthDeclaration::Header { name, value } => Ok(AuthScheme::Header {
            name: name.clone(),
            value_uri: SecretUri::new(value.clone())?,
        }),
    }
}

fn compile_rate_limit(
    rate_limit: Option<&RateLimitDeclaration>,
) -> Result<(RateLimitPolicy, Vec<String>)> {
    let Some(rate_limit) = rate_limit else {
        return Ok((RateLimitPolicy::unrestricted(), Vec::new()));
    };

    Ok((
        RateLimitPolicy {
            requests_per_minute: rate_limit.requests_per_minute,
            quota_headers: rate_limit
                .quota_headers
                .iter()
                .map(|quota| {
                    QuotaHeaderPolicy::remaining_until_reset(
                        quota.remaining_header.clone(),
                        quota.reset_header.clone(),
                        match quota.reset {
                            ResetSemanticsDeclaration::DelaySeconds => {
                                ResetHeaderSemantics::DelaySeconds
                            }
                            ResetSemanticsDeclaration::EpochSeconds => {
                                ResetHeaderSemantics::EpochSeconds
                            }
                        },
                    )
                })
                .collect(),
        },
        rate_limit.respect_headers.clone(),
    ))
}

fn compile_pagination(pagination: &PaginationDeclaration) -> Result<PaginationConfig> {
    Ok(match pagination {
        PaginationDeclaration::LinkHeader => PaginationConfig::LinkHeader,
        PaginationDeclaration::CursorParam {
            query_param,
            response_field,
            initial,
        } => PaginationConfig::Cursor {
            query_param: query_param.clone(),
            response_field: response_field.clone(),
            initial: initial.clone(),
        },
        PaginationDeclaration::PageNumber {
            query_param,
            start_page,
        } => PaginationConfig::Page {
            query_param: query_param.clone(),
            start_page: start_page.unwrap_or(1),
        },
        PaginationDeclaration::Offset {
            offset_param,
            limit_param,
            start_offset,
            limit,
        } => PaginationConfig::Offset {
            offset_param: offset_param.clone(),
            limit_param: limit_param.clone(),
            start_offset: start_offset.unwrap_or(0),
            limit: *limit,
        },
        PaginationDeclaration::NextToken {
            query_param,
            response_field,
            initial,
        } => PaginationConfig::NextToken {
            query_param: query_param.clone(),
            response_field: response_field.clone(),
            initial: initial.clone(),
        },
    })
}

fn capabilities_for(
    descriptor: &ResourceDescriptor,
    plan: &CompiledResourcePlan,
) -> ResourceCapabilities {
    match plan {
        CompiledResourcePlan::Rest(rest) => ResourceCapabilities {
            projection: CapabilitySupport::Unsupported,
            filters: FilterCapabilities {
                default_fidelity: if descriptor.cursor.is_some() {
                    rest.cursor_filter_fidelity.clone()
                } else {
                    PushdownFidelity::Unsupported
                },
                supported_operators: if descriptor.cursor.is_some() {
                    vec![">".to_owned(), ">=".to_owned(), "=".to_owned()]
                } else {
                    Vec::new()
                },
            },
            limits: CapabilitySupport::Unsupported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: partitioning_capabilities(descriptor),
            incremental: if descriptor.cursor.is_some() {
                IncrementalShape::Cursor
            } else {
                IncrementalShape::Full
            },
            replay: if descriptor.cursor.is_some() {
                ReplaySupport::FromPosition
            } else {
                ReplaySupport::None
            },
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::None,
        },
        CompiledResourcePlan::Sql(sql) => sql_capabilities_for(descriptor, sql),
        CompiledResourcePlan::Files(_) => ResourceCapabilities {
            projection: CapabilitySupport::Unsupported,
            filters: FilterCapabilities::default(),
            limits: CapabilitySupport::Unsupported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![ScopeKind::File],
            },
            incremental: IncrementalShape::File,
            replay: ReplaySupport::ExactRecordedBatches,
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::Bytes,
        },
    }
}

fn partitioning_capabilities(descriptor: &ResourceDescriptor) -> PartitioningCapabilities {
    match descriptor.state_scope.kind() {
        ScopeKind::Resource => PartitioningCapabilities::default(),
        kind => PartitioningCapabilities {
            parallel_partitions: true,
            supported_scopes: vec![kind],
        },
    }
}

fn partition_for_plan(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    plan: &CompiledResourcePlan,
    request: Option<&ScanRequest>,
) -> Result<PartitionPlan> {
    let (partition_id, scope, mut metadata) = match plan {
        CompiledResourcePlan::Rest(rest) => {
            let mut metadata = BTreeMap::new();
            metadata.insert("kind".to_owned(), "rest".to_owned());
            metadata.insert("path".to_owned(), rest.path.clone());
            if let Some(pagination) = &rest.pagination {
                metadata.insert("pagination".to_owned(), pagination.kind().to_string());
            }
            if let Some(cursor) = &descriptor.cursor {
                metadata.insert("cursor_field".to_owned(), cursor.field.clone());
            }
            if let (Some(request), Some(cursor_param)) = (request, rest.cursor_param.as_ref())
                && rest.cursor_filter_fidelity != PushdownFidelity::Unsupported
                && let Some(value) = request.filters.iter().find_map(|predicate| {
                    cursor_pushdown_value(descriptor, rest, &predicate.expression)
                })
            {
                metadata.insert(CURSOR_QUERY_PARAM_METADATA.to_owned(), cursor_param.clone());
                metadata.insert(CURSOR_QUERY_VALUE_METADATA.to_owned(), value);
            }
            ("rest".to_owned(), descriptor.state_scope.clone(), metadata)
        }
        CompiledResourcePlan::Sql(sql) => {
            return sql_partition_for_plan(descriptor, schema, sql, request);
        }
        CompiledResourcePlan::Files(files) => {
            let mut metadata = BTreeMap::new();
            metadata.insert("kind".to_owned(), "files".to_owned());
            metadata.insert("glob".to_owned(), files.glob.clone());
            (
                "files".to_owned(),
                ScopeKey::File {
                    path: files.glob.clone(),
                },
                metadata,
            )
        }
    };
    metadata.insert("resource_id".to_owned(), descriptor.resource_id.to_string());

    Ok(PartitionPlan {
        partition_id: PartitionId::new(partition_id)?,
        scope,
        start_position: None,
        metadata,
    })
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
            path: resource.glob.clone().unwrap_or_else(|| "*".to_owned()),
        }),
        Some(PartitionDeclaration {
            by: PartitionByDeclaration::Resource,
            ..
        })
        | None => Ok(ScopeKey::Resource),
    }
}

fn delivery_guarantee(descriptor: &ResourceDescriptor) -> DeliveryGuarantee {
    match descriptor.write_disposition {
        WriteDisposition::Merge if !descriptor.merge_key.is_empty() => {
            DeliveryGuarantee::EffectivelyOncePerKey
        }
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
        WriteDisposition::Append | WriteDisposition::Merge => {
            DeliveryGuarantee::AtLeastOnceDuplicateRisk
        }
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

fn validate_escape_hatch(resource: &ResourceDeclaration) -> Result<()> {
    let Some(transform) = &resource.records_transform else {
        return Ok(());
    };
    if transform.starts_with("python://") || transform.starts_with("rust://") {
        Ok(())
    } else {
        Err(CdfError::contract(
            "records_transform must use python:// or rust://",
        ))
    }
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
    name: &str,
    resource: &ResourceDeclaration,
    write_disposition: &WriteDisposition,
) -> Result<Vec<String>> {
    match write_disposition {
        WriteDisposition::Merge => match &resource.merge_key {
            Some(keys) if !keys.is_empty() => Ok(keys.clone()),
            _ => Err(CdfError::contract(format!(
                "resource `{name}` declares write_disposition = \"merge\" but is missing merge_key; add `merge_key = [...]` or use `write_disposition = \"append\"`"
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
