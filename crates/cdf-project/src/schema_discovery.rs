use std::{collections::BTreeMap, path::Path, sync::Arc};

use cdf_contract::{IdentifierPolicy, NORMALIZER_NAMECASE_V1, normalize_arrow_schema};
use cdf_declarative::{
    CompiledResource, CompiledResourcePlan, FileFormatDeclaration, FileRuntimeDependencies,
    FileTransportLocation, FileTransportResource, discover_local_parquet_schema,
    discover_rest_sample_schema, discover_transport_parquet_schema,
    postgres_table_target_for_sql_plan,
};
use cdf_dest_postgres::{POSTGRES_CATALOG_DISCOVERY_PROBE, discover_postgres_table_catalog_schema};
use cdf_http::{HttpTransport, SecretProvider};
use cdf_kernel::{
    CdfError, PartitionPlan, ResourceDescriptor, ResourceStream, Result, ScanRequest, SchemaSource,
};

use crate::{
    DiscoveredParquetSchemaSnapshot, SCHEMA_DISCOVERY_FORMAT_PARQUET,
    SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER, SchemaSnapshotArtifact, SchemaSnapshotStore,
};

#[derive(Clone, Debug)]
pub struct PreparedDiscoveredResource {
    pub resource: CompiledResource,
    pub discovery: Option<ResourceSchemaDiscovery>,
}

#[derive(Clone, Debug)]
pub struct ResourceSchemaDiscovery {
    pub normalized_schema: arrow_schema::SchemaRef,
    pub snapshot: DiscoveredSchemaSnapshot,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DiscoveredSchemaSnapshot {
    pub artifact: SchemaSnapshotArtifact,
    pub reference: cdf_kernel::SchemaSnapshotReference,
    pub source_identity: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct LocalParquetSchemaDiscovery {
    pub normalized_schema: arrow_schema::SchemaRef,
    pub snapshot: DiscoveredParquetSchemaSnapshot,
    pub partition: PartitionPlan,
}

pub fn discover_resource_schema(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
) -> Result<ResourceSchemaDiscovery> {
    discover_resource_schema_inner(resource, secret_provider, None, None)
}

pub fn discover_resource_schema_with_rest_transport(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    rest_transport: &mut dyn HttpTransport,
) -> Result<ResourceSchemaDiscovery> {
    discover_resource_schema_inner(resource, secret_provider, Some(rest_transport), None)
}

pub fn discover_resource_schema_with_file_dependencies(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    file_dependencies: FileRuntimeDependencies,
) -> Result<ResourceSchemaDiscovery> {
    discover_resource_schema_inner(resource, secret_provider, None, Some(file_dependencies))
}

fn discover_resource_schema_inner(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    rest_transport: Option<&mut dyn HttpTransport>,
    file_dependencies: Option<FileRuntimeDependencies>,
) -> Result<ResourceSchemaDiscovery> {
    ensure_discover_schema_mode(resource)?;
    match resource.plan() {
        CompiledResourcePlan::Files(_) => {
            let discovery = discover_parquet_resource_schema(resource, file_dependencies)?;
            Ok(ResourceSchemaDiscovery {
                normalized_schema: Arc::clone(&discovery.normalized_schema),
                snapshot: DiscoveredSchemaSnapshot {
                    artifact: discovery.snapshot.artifact,
                    reference: discovery.snapshot.reference,
                    source_identity: discovery.snapshot.source_identity,
                },
            })
        }
        CompiledResourcePlan::Sql(plan) => {
            discover_postgres_resource_schema(resource, plan, secret_provider)
        }
        CompiledResourcePlan::Rest(_) => match rest_transport {
            Some(transport) => discover_rest_resource_schema(resource, secret_provider, transport),
            None => Err(unsupported_discover_slice(
                resource.descriptor(),
                "REST resource discovery requires an explicit HTTP transport",
            )),
        },
    }
}

fn discover_parquet_resource_schema(
    resource: &CompiledResource,
    file_dependencies: Option<FileRuntimeDependencies>,
) -> Result<LocalParquetSchemaDiscovery> {
    match resource.plan() {
        CompiledResourcePlan::Files(plan) if is_http_root(&plan.root) => {
            let dependencies = file_dependencies.ok_or_else(|| {
                unsupported_discover_slice(
                    resource.descriptor(),
                    "HTTP(S) Parquet discovery requires explicit file transport dependencies",
                )
            })?;
            discover_http_parquet_resource_schema(resource, dependencies)
        }
        CompiledResourcePlan::Files(_) => discover_local_parquet_resource_schema(resource),
        CompiledResourcePlan::Rest(_) | CompiledResourcePlan::Sql(_) => Err(
            unsupported_discover_slice(resource.descriptor(), "resource is not a file resource"),
        ),
    }
}

pub fn discover_local_parquet_resource_schema(
    resource: &CompiledResource,
) -> Result<LocalParquetSchemaDiscovery> {
    ensure_discover_schema_mode(resource)?;
    let (plan, partition) = single_local_parquet_partition(resource)?;
    let relative_path = partition.metadata.get("path").cloned().ok_or_else(|| {
        CdfError::contract(format!(
            "local Parquet discovery for resource `{}` expected file partition path metadata",
            resource.descriptor().resource_id
        ))
    })?;
    let path = Path::new(&plan.root).join(&relative_path);
    let mut probe = discover_local_parquet_schema(&path)?;
    probe
        .source_identity
        .insert("path".to_owned(), relative_path);
    let normalized = normalize_arrow_schema(probe.schema.as_ref(), &IdentifierPolicy::default())?;
    let normalized = Arc::new(normalized);
    let metadata = BTreeMap::from([
        (
            "probe".to_owned(),
            SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER.to_owned(),
        ),
        (
            "format".to_owned(),
            SCHEMA_DISCOVERY_FORMAT_PARQUET.to_owned(),
        ),
        (
            "cdf:normalizer".to_owned(),
            NORMALIZER_NAMECASE_V1.to_owned(),
        ),
    ]);
    let artifact = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        normalized.as_ref(),
        metadata,
    )?;
    let snapshot = DiscoveredParquetSchemaSnapshot {
        reference: artifact.reference(),
        artifact,
        source_identity: probe.source_identity,
    };

    Ok(LocalParquetSchemaDiscovery {
        normalized_schema: normalized,
        snapshot,
        partition,
    })
}

fn discover_http_parquet_resource_schema(
    resource: &CompiledResource,
    dependencies: FileRuntimeDependencies,
) -> Result<LocalParquetSchemaDiscovery> {
    ensure_discover_schema_mode(resource)?;
    let (plan, partition) = single_http_parquet_partition(resource, &dependencies)?;
    let url = partition.metadata.get("path").cloned().ok_or_else(|| {
        CdfError::contract(format!(
            "HTTP(S) Parquet discovery for resource `{}` expected file partition URL metadata",
            resource.descriptor().resource_id
        ))
    })?;
    let resource_request = FileTransportResource {
        location: FileTransportLocation::HttpUrl { url },
        egress_allowlist: plan.allowlist.clone(),
        auth: plan.auth.clone(),
    };
    let mut probe = discover_transport_parquet_schema(resource_request, &dependencies)?;
    let normalized = normalize_arrow_schema(probe.schema.as_ref(), &IdentifierPolicy::default())?;
    let normalized = Arc::new(normalized);
    let metadata = BTreeMap::from([
        (
            "probe".to_owned(),
            SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER.to_owned(),
        ),
        (
            "format".to_owned(),
            SCHEMA_DISCOVERY_FORMAT_PARQUET.to_owned(),
        ),
        ("source_kind".to_owned(), "files".to_owned()),
        (
            "cdf:normalizer".to_owned(),
            NORMALIZER_NAMECASE_V1.to_owned(),
        ),
    ]);
    let artifact = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        normalized.as_ref(),
        metadata,
    )?;
    probe
        .source_identity
        .insert("transport".to_owned(), "https".to_owned());
    let snapshot = DiscoveredParquetSchemaSnapshot {
        reference: artifact.reference(),
        artifact,
        source_identity: probe.source_identity,
    };

    Ok(LocalParquetSchemaDiscovery {
        normalized_schema: normalized,
        snapshot,
        partition,
    })
}

fn discover_postgres_resource_schema(
    resource: &CompiledResource,
    plan: &cdf_declarative::SqlResourcePlan,
    secret_provider: &dyn SecretProvider,
) -> Result<ResourceSchemaDiscovery> {
    if let Some(dialect) = &plan.dialect
        && !dialect.eq_ignore_ascii_case("postgres")
    {
        return Err(unsupported_discover_slice(
            resource.descriptor(),
            format!(
                "SQL dialect `{dialect}` discovery is not implemented in this slice; only dialect `postgres` table resources support catalog discovery"
            ),
        ));
    }
    let target = postgres_table_target_for_sql_plan(plan).map_err(|error| {
        unsupported_discover_slice(
            resource.descriptor(),
            format!(
                "Postgres table catalog discovery is unavailable: {}",
                error.message
            ),
        )
    })?;
    let secret = secret_provider.resolve(&plan.connection)?;
    let probe = discover_postgres_table_catalog_schema(
        secret.as_str()?,
        &resource.descriptor().resource_id,
        &target,
    )?;
    let metadata = BTreeMap::from([
        (
            "probe".to_owned(),
            POSTGRES_CATALOG_DISCOVERY_PROBE.to_owned(),
        ),
        ("source_kind".to_owned(), "sql".to_owned()),
        ("dialect".to_owned(), "postgres".to_owned()),
        ("table".to_owned(), target.display_name()),
        (
            "cdf:normalizer".to_owned(),
            NORMALIZER_NAMECASE_V1.to_owned(),
        ),
    ]);
    build_schema_discovery(resource, &probe.schema, metadata, probe.source_identity)
}

fn discover_rest_resource_schema(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    rest_transport: &mut dyn HttpTransport,
) -> Result<ResourceSchemaDiscovery> {
    let probe = discover_rest_sample_schema(resource, rest_transport, secret_provider)?;
    let metadata = BTreeMap::from([
        ("probe".to_owned(), "rest-sample-page".to_owned()),
        ("source_kind".to_owned(), "rest".to_owned()),
        (
            "cdf:normalizer".to_owned(),
            NORMALIZER_NAMECASE_V1.to_owned(),
        ),
    ]);
    build_schema_discovery(
        resource,
        probe.schema.as_ref(),
        metadata,
        probe.source_identity,
    )
}

fn build_schema_discovery(
    resource: &CompiledResource,
    schema: &arrow_schema::Schema,
    metadata: BTreeMap<String, String>,
    source_identity: BTreeMap<String, String>,
) -> Result<ResourceSchemaDiscovery> {
    let normalized = normalize_arrow_schema(schema, &IdentifierPolicy::default())?;
    let normalized = Arc::new(normalized);
    let artifact = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        normalized.as_ref(),
        metadata,
    )?;
    Ok(ResourceSchemaDiscovery {
        normalized_schema: normalized,
        snapshot: DiscoveredSchemaSnapshot {
            reference: artifact.reference(),
            artifact,
            source_identity,
        },
    })
}

pub fn prepare_local_parquet_discover_resource(
    project_root: impl AsRef<Path>,
    resource: &CompiledResource,
) -> Result<PreparedDiscoveredResource> {
    if !matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return Ok(PreparedDiscoveredResource {
            resource: resource.clone(),
            discovery: None,
        });
    }

    let discovery = discover_local_parquet_resource_schema(resource)?;
    let discovery = ResourceSchemaDiscovery {
        normalized_schema: Arc::clone(&discovery.normalized_schema),
        snapshot: DiscoveredSchemaSnapshot {
            artifact: discovery.snapshot.artifact,
            reference: discovery.snapshot.reference,
            source_identity: discovery.snapshot.source_identity,
        },
    };
    prepare_discovered_schema(project_root, resource, discovery)
}

pub fn prepare_discover_resource(
    project_root: impl AsRef<Path>,
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
) -> Result<PreparedDiscoveredResource> {
    if !matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return Ok(PreparedDiscoveredResource {
            resource: resource.clone(),
            discovery: None,
        });
    }

    let discovery = discover_resource_schema(resource, secret_provider)?;
    prepare_discovered_schema(project_root, resource, discovery)
}

pub fn prepare_discover_resource_with_file_dependencies(
    project_root: impl AsRef<Path>,
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    file_dependencies: FileRuntimeDependencies,
) -> Result<PreparedDiscoveredResource> {
    if !matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return Ok(PreparedDiscoveredResource {
            resource: resource.clone(),
            discovery: None,
        });
    }

    let discovery = discover_resource_schema_with_file_dependencies(
        resource,
        secret_provider,
        file_dependencies,
    )?;
    prepare_discovered_schema(project_root, resource, discovery)
}

pub fn prepare_discover_resource_with_rest_transport(
    project_root: impl AsRef<Path>,
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    rest_transport: &mut dyn HttpTransport,
) -> Result<PreparedDiscoveredResource> {
    if !matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return Ok(PreparedDiscoveredResource {
            resource: resource.clone(),
            discovery: None,
        });
    }

    let discovery =
        discover_resource_schema_with_rest_transport(resource, secret_provider, rest_transport)?;
    prepare_discovered_schema(project_root, resource, discovery)
}

fn prepare_discovered_schema(
    project_root: impl AsRef<Path>,
    resource: &CompiledResource,
    discovery: ResourceSchemaDiscovery,
) -> Result<PreparedDiscoveredResource> {
    let store = SchemaSnapshotStore::new(project_root);
    store.write(&discovery.snapshot.artifact)?;
    let pinned = resource.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: discovery.snapshot.reference.clone(),
        },
        Arc::clone(&discovery.normalized_schema),
    );

    Ok(PreparedDiscoveredResource {
        resource: pinned,
        discovery: Some(discovery),
    })
}

fn ensure_discover_schema_mode(resource: &CompiledResource) -> Result<()> {
    if matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return Ok(());
    }
    Err(CdfError::contract(format!(
        "cdf schema discover supports resources in discover schema mode; resource `{}` already has a declared or pinned schema",
        resource.descriptor().resource_id
    )))
}

fn single_local_parquet_partition(
    resource: &CompiledResource,
) -> Result<(&cdf_declarative::FileResourcePlan, PartitionPlan)> {
    let plan = match resource.plan() {
        CompiledResourcePlan::Files(plan) => plan,
        CompiledResourcePlan::Rest(_) => {
            return Err(unsupported_discover_slice(
                resource.descriptor(),
                "REST resource discovery is not implemented in this slice",
            ));
        }
        CompiledResourcePlan::Sql(_) => {
            return Err(unsupported_discover_slice(
                resource.descriptor(),
                "SQL resource discovery is not implemented in this slice",
            ));
        }
    };
    if plan.format != FileFormatDeclaration::Parquet {
        return Err(unsupported_discover_slice(
            resource.descriptor(),
            format!(
                "only local single-file Parquet discovery is implemented in this slice; resource uses format = {:?}",
                plan.format
            ),
        ));
    }

    let partitions = resource.plan_partitions(&discovery_scan_request(resource.descriptor())?)?;
    match partitions.as_slice() {
        [partition] => Ok((plan, partition.clone())),
        [] => Err(CdfError::data(format!(
            "local Parquet discovery for resource `{}` matched no files under `{}` for glob `{}`",
            resource.descriptor().resource_id,
            plan.root,
            plan.glob
        ))),
        _ => Err(CdfError::contract(format!(
            "multi-file Parquet discovery is unsupported for resource `{}`; glob `{}` under `{}` resolved to {} files",
            resource.descriptor().resource_id,
            plan.glob,
            plan.root,
            partitions.len()
        ))),
    }
}

fn single_http_parquet_partition<'a>(
    resource: &'a CompiledResource,
    dependencies: &FileRuntimeDependencies,
) -> Result<(&'a cdf_declarative::FileResourcePlan, PartitionPlan)> {
    let plan = match resource.plan() {
        CompiledResourcePlan::Files(plan) => plan,
        CompiledResourcePlan::Rest(_) | CompiledResourcePlan::Sql(_) => {
            return Err(unsupported_discover_slice(
                resource.descriptor(),
                "HTTP(S) Parquet discovery only supports file resources",
            ));
        }
    };
    if plan.format != FileFormatDeclaration::Parquet {
        return Err(unsupported_discover_slice(
            resource.descriptor(),
            format!(
                "only HTTP(S) single-file Parquet discovery is implemented in this slice; resource uses format = {:?}",
                plan.format
            ),
        ));
    }
    let runtime = resource.to_file_resource(dependencies.clone())?;
    let partitions = runtime.plan_partitions(&discovery_scan_request(resource.descriptor())?)?;
    match partitions.as_slice() {
        [partition] => Ok((plan, partition.clone())),
        [] => Err(CdfError::data(format!(
            "HTTP(S) Parquet discovery for resource `{}` matched no file for `{}` and glob `{}`",
            resource.descriptor().resource_id,
            plan.root,
            plan.glob
        ))),
        _ => Err(CdfError::contract(format!(
            "multi-file HTTP(S) Parquet discovery is unsupported for resource `{}`; glob `{}` under `{}` resolved to {} files",
            resource.descriptor().resource_id,
            plan.glob,
            plan.root,
            partitions.len()
        ))),
    }
}

fn is_http_root(root: &str) -> bool {
    root.starts_with("http://") || root.starts_with("https://")
}

fn discovery_scan_request(descriptor: &ResourceDescriptor) -> Result<ScanRequest> {
    Ok(ScanRequest {
        resource_id: descriptor.resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: descriptor.state_scope.clone(),
    })
}

fn unsupported_discover_slice(
    descriptor: &ResourceDescriptor,
    reason: impl Into<String>,
) -> CdfError {
    CdfError::contract(format!(
        "unsupported schema discovery slice for resource `{}`: {}",
        descriptor.resource_id,
        reason.into()
    ))
}
