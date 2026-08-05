use std::{path::Path, sync::Arc};

use cdf_declarative::CompiledResource;
use cdf_kernel::{CdfError, CursorValue, Result, SourcePosition};
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
    let document =
        cdf_declarative::parse_toml(&resource_toml(cell.disposition, &endpoint, &collection))?;
    let provisional = one_resource(cdf_declarative::compile_document(&registry, &document)?)?;
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
    let compiled = cdf_project::compile_discovered_schema_artifacts(&provisional, &mut discovery)?;
    crate::source_fixture::ResolvedSourceFixture::resolve(&compiled, &registry, &context)
}

pub(crate) fn assert_source_position(report: &ProjectRunReport) {
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("run matrix MongoDB source must checkpoint a cursor position");
    };
    assert_eq!(cursor.version, cdf_kernel::SOURCE_POSITION_VERSION);
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}

fn seed_collection(endpoint: &str, collection: &str) -> Result<()> {
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
                doc! {"_id": 2_i64, "name": "grace", "updated_at": 20_i64},
            ])
            .await
            .map_err(|_| CdfError::environment("seed MongoDB fixture collection"))?;
        Ok(())
    })
}

fn one_resource(mut resources: Vec<CompiledResource>) -> Result<CompiledResource> {
    if resources.len() != 1 {
        return Err(CdfError::contract(format!(
            "run matrix expected one MongoDB resource, found {}",
            resources.len()
        )));
    }
    let resource = resources.remove(0);
    if resource.descriptor().resource_id.as_str() != RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "run matrix compiled unexpected MongoDB resource {}",
            resource.descriptor().resource_id
        )));
    }
    Ok(resource)
}

fn resource_toml(disposition: MatrixDisposition, endpoint: &str, collection: &str) -> String {
    let keys = merge_keys(disposition);
    format!(
        r#"
[source.warehouse]
kind = "mongodb"
endpoint = "{endpoint}"
database = "{DATABASE}"
batch_rows = 1024
max_pool_size = 2
stream_buffer_batches = 1

[resource.events]
collection = "{collection}"
{keys}
cursor = {{ field = "updated_at", ordering = "exact", lag = "0ms" }}
write_disposition = "{}"
trust = "governed"
schema_mode = "hints"
schema = {{ fields = [
  {{ name = "id", source_name = "_id", type = "int64", nullable = false }},
  {{ name = "name", type = "string", nullable = true }},
  {{ name = "updated_at", type = "int64", nullable = false }},
] }}
"#,
        disposition.as_str()
    )
}

fn merge_keys(disposition: MatrixDisposition) -> &'static str {
    if disposition == MatrixDisposition::Merge {
        "primary_key = [\"id\"]\nmerge_key = [\"id\"]"
    } else {
        ""
    }
}
