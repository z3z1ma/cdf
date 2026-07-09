use std::{collections::BTreeMap, path::Path, sync::Arc};

use cdf_contract::{IdentifierPolicy, NORMALIZER_NAMECASE_V1, normalize_arrow_schema};
use cdf_declarative::{
    CompiledResource, CompiledResourcePlan, FileFormatDeclaration, discover_local_parquet_schema,
    postgres_table_target_for_sql_plan,
};
use cdf_dest_postgres::{POSTGRES_CATALOG_DISCOVERY_PROBE, discover_postgres_table_catalog_schema};
use cdf_http::SecretProvider;
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
    ensure_discover_schema_mode(resource)?;
    match resource.plan() {
        CompiledResourcePlan::Files(_) => {
            let discovery = discover_local_parquet_resource_schema(resource)?;
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
        CompiledResourcePlan::Rest(_) => Err(unsupported_discover_slice(
            resource.descriptor(),
            "REST resource discovery is not implemented in this slice",
        )),
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
    let normalized = normalize_arrow_schema(&probe.schema, &IdentifierPolicy::default())?;
    let normalized = Arc::new(normalized);
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
            source_identity: probe.source_identity,
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
