use std::{collections::BTreeMap, fs, path::Path, sync::Arc};

use cdf_declarative::CompiledResource;
use cdf_kernel::{CdfError, CursorValue, DestinationProtocol, Result, SourcePosition};
use cdf_project::ProjectRunReport;
use cdf_runtime::{SourceRegistry, SourceResolutionContext};
use mongodb::{
    Client,
    bson::{Document, doc},
    options::{ClientOptions, ServerApi, ServerApiVersion},
};

use super::{MatrixDisposition, RunMatrixCell, test_support::StaticSecretProvider};

const RESOURCE_ID: &str = "warehouse.events";
const ENDPOINT_ENV: &str = "CDF_MONGODB_ENDPOINT";
const DATABASE: &str = "cdf_conformance";

pub(crate) fn resource(
    cell: &RunMatrixCell,
    project_root: &Path,
) -> Result<crate::source_fixture::ResolvedSourceFixture> {
    let endpoint = std::env::var(ENDPOINT_ENV).map_err(|_| {
        CdfError::environment(format!(
            "MongoDB run-matrix shard requires {ENDPOINT_ENV}=mongodb://host:port"
        ))
    })?;
    let collection = format!(
        "cdf_source_{}_{}",
        cell.destination.as_str(),
        cell.disposition.as_str()
    );
    seed_collection(&endpoint, &collection)?;

    let mut registry = SourceRegistry::new();
    registry.register(cdf_source_mongodb::MongoDbSourceDriver::new()?)?;
    let project_toml = project_toml(&endpoint);
    write_query_project(project_root, &project_toml, cell.disposition, &collection)?;
    let config = cdf_project::parse_cdf_toml(&project_toml)?;
    let compile_destination =
        cdf_dest_duckdb::DuckDbDestination::new(project_root.join(".cdf/compile-only.duckdb"))?;
    let mut entries = cdf_project::compile_query_project_resources(
        &registry,
        &config,
        project_root,
        "dev",
        compile_destination.sheet(),
        &cdf_semantic::SemanticCatalog::builtins()?,
        &BTreeMap::new(),
    )?;
    if entries.len() != 1 {
        return Err(CdfError::contract(format!(
            "run matrix expected one MongoDB project resource, found {}",
            entries.len()
        )));
    }
    let mut entry = entries.remove(0);
    let provisional = entry.resource.clone();
    let execution = crate::test_execution_services();
    let context = SourceResolutionContext::new(
        Path::new("."),
        Arc::new(StaticSecretProvider::new(Vec::<(String, String)>::new())),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let mut discovery = cdf_project::discover_resource_schema_with_source_registry(
        &provisional,
        &registry,
        provisional.source_plan(),
        &context,
        cdf_project::SchemaDiscoveryExecutionOptions::new(),
    )?;
    entry.resource =
        cdf_project::compile_discovered_schema_artifacts(&provisional, &mut discovery)?;
    entry = cdf_project::finalize_query_project_resource(
        entry,
        &cdf_semantic::SemanticCatalog::builtins()?,
    )?;
    one_resource(&entry.resource)?;
    crate::source_fixture::ResolvedSourceFixture::resolve(&entry.resource, &registry, &context)
}

pub(crate) fn assert_source_position(report: &ProjectRunReport) {
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("run matrix MongoDB source must checkpoint a cursor position");
    };
    assert_eq!(cursor.version, cdf_kernel::SOURCE_POSITION_VERSION);
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}

pub(crate) fn add_runtime_unknown_field(cell: &RunMatrixCell) -> Result<()> {
    let endpoint = std::env::var(ENDPOINT_ENV).map_err(|_| {
        CdfError::environment(format!(
            "MongoDB runtime-drift fixture requires {ENDPOINT_ENV}=mongodb://host:port"
        ))
    })?;
    let collection = format!(
        "cdf_source_{}_{}",
        cell.destination.as_str(),
        cell.disposition.as_str()
    );
    crate::test_execution_services().run_io(async move {
        let mut options = ClientOptions::parse(&endpoint)
            .await
            .map_err(|_| CdfError::environment("parse MongoDB fixture endpoint"))?;
        options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
        let client = Client::with_options(options)
            .map_err(|_| CdfError::environment("construct MongoDB fixture client"))?;
        client
            .database(DATABASE)
            .collection::<Document>(&collection)
            .update_one(
                doc! {"_id": 1_i64},
                doc! {"$set": {"runtime_extra": 42_i64}},
            )
            .await
            .map_err(|_| CdfError::environment("inject MongoDB runtime schema drift"))?;
        Ok(())
    })
}

pub(crate) fn seed_collection(endpoint: &str, collection: &str) -> Result<()> {
    let endpoint = endpoint.to_owned();
    let collection = collection.to_owned();
    crate::test_execution_services().run_io(async move {
        let mut options = ClientOptions::parse(&endpoint)
            .await
            .map_err(|_| CdfError::environment("parse MongoDB fixture endpoint"))?;
        options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
        let client = Client::with_options(options)
            .map_err(|_| CdfError::environment("construct MongoDB fixture client"))?;
        let collection = client
            .database(DATABASE)
            .collection::<Document>(&collection);
        collection
            .drop()
            .await
            .map_err(|_| CdfError::environment("reset MongoDB fixture collection"))?;
        collection
            .insert_many([
                doc! {"_id": 1_i64, "name": "ada", "updated_at": 10_i64},
                doc! {"_id": 2_i64, "updated_at": 20_i64},
            ])
            .await
            .map_err(|_| CdfError::environment("seed MongoDB fixture collection"))?;
        Ok(())
    })
}

pub(crate) fn make_lifecycle_runtime_cursor_physically_narrower(
    endpoint: &str,
    collection: &str,
) -> Result<()> {
    let endpoint = endpoint.to_owned();
    let collection = collection.to_owned();
    crate::test_execution_services().run_io(async move {
        let mut options = ClientOptions::parse(&endpoint)
            .await
            .map_err(|_| CdfError::environment("parse MongoDB fixture endpoint"))?;
        options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
        let client = Client::with_options(options)
            .map_err(|_| CdfError::environment("construct MongoDB fixture client"))?;
        let result = client
            .database(DATABASE)
            .collection::<Document>(&collection)
            .update_one(doc! {"_id": 2_i64}, doc! {"$set": {"updated_at": 20_i32}})
            .await
            .map_err(|_| CdfError::environment("inject MongoDB physical cursor drift"))?;
        if result.matched_count != 1 || result.modified_count != 1 {
            return Err(CdfError::data(
                "MongoDB physical cursor drift fixture did not update exactly one document",
            ));
        }
        Ok(())
    })
}

fn one_resource(resource: &CompiledResource) -> Result<()> {
    if resource.descriptor().resource_id.as_str() != RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "run matrix compiled unexpected MongoDB resource {}",
            resource.descriptor().resource_id
        )));
    }
    Ok(())
}

fn project_toml(endpoint: &str) -> String {
    format!(
        r#"[project]
name = "mongodb_run_matrix"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.sqlite"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[sources.warehouse]
type = "mongodb"
endpoint = "{endpoint}"
database = "{DATABASE}"
batch_rows = 1
max_pool_size = 2
stream_buffer_batches = 1
"#
    )
}

fn write_query_project(
    project_root: &Path,
    project_toml: &str,
    disposition: MatrixDisposition,
    collection: &str,
) -> Result<()> {
    let directory = project_root.join("cdf/warehouse");
    fs::create_dir_all(&directory).map_err(|error| {
        crate::conformance_private_io_error("create MongoDB query resource directory", error)
    })?;
    fs::write(project_root.join("cdf.toml"), project_toml).map_err(|error| {
        crate::conformance_private_io_error("write MongoDB query project", error)
    })?;
    let disposition = match disposition {
        MatrixDisposition::Append => "APPEND",
        MatrixDisposition::Replace => "REPLACE",
        MatrixDisposition::Merge => "MERGE(id)",
    };
    let sql = format!(
        r#"RESOURCE
TARGET events
DISPOSITION {disposition}
CURSOR updated_at
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT id, name, updated_at
FROM upstream(source => 'warehouse', collection => '{collection}');
"#
    );
    fs::write(directory.join("events.cdf.sql"), sql)
        .map_err(|error| crate::conformance_private_io_error("write MongoDB query resource", error))
}
