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

#[cfg(test)]
pub(super) fn assert_physical_reconciliation_stays_out_of_residual_variant(
    package: &std::path::Path,
) {
    use arrow_array::Array;

    let evidence_path = package.join("schema/physical-reconciliations.json");
    assert!(
        evidence_path.is_file(),
        "physical reconciliation evidence was not published"
    );
    let evidence: serde_json::Value =
        serde_json::from_slice(&std::fs::read(evidence_path).unwrap()).unwrap();
    let reconciliations = evidence["reconciliations"].as_array().unwrap();
    assert!(!reconciliations.is_empty());
    assert!(reconciliations.iter().any(|reconciliation| {
        reconciliation["observed_field"]["metadata"][cdf_kernel::PHYSICAL_TYPE_METADATA_KEY]
            == "bson:int32"
            && reconciliation["expected_field"]["metadata"][cdf_kernel::PHYSICAL_TYPE_METADATA_KEY]
                == "bson:int64"
    }));

    let reader = cdf_package::PackageReader::open(package).unwrap();
    let memory: std::sync::Arc<dyn cdf_memory::MemoryCoordinator> = std::sync::Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(
            128 * 1024 * 1024,
            std::collections::BTreeMap::new(),
        )
        .unwrap(),
    );
    for segment in reader
        .verified_canonical_segment_stream(memory, 128 * 1024 * 1024)
        .unwrap()
    {
        for batch in segment.unwrap().batches {
            let variants = batch
                .column_by_name(cdf_contract::VARIANT_COLUMN_NAME)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            assert_eq!(variants.null_count(), variants.len());
        }
    }
}

#[test]
#[ignore = "live MongoDB public CLI lifecycle; set CDF_MONGODB_ENDPOINT"]
fn mongodb_public_cli_lifecycle_is_current_redacted_and_jobs_invariant() {
    let endpoint = std::env::var("CDF_MONGODB_ENDPOINT")
        .expect("CDF_MONGODB_ENDPOINT must name the live MongoDB endpoint");
    let collection = "cdf_public_cli_lifecycle";
    seed_collection(&endpoint, collection).unwrap();
    let base = tempfile::tempdir().unwrap();
    let project_root = base.path().join("project");
    let init = cdf_cli::invoke([
        std::ffi::OsString::from("cdf"),
        "--json".into(),
        "init".into(),
        project_root.as_os_str().to_owned(),
    ]);
    assert_eq!(init.exit_code, 0, "{}", init.stderr);
    let mut location = url::Url::parse(&endpoint).unwrap();
    assert!(
        !location.username().is_empty() && location.password().is_some(),
        "CDF_MONGODB_ENDPOINT must include fixture credentials so runtime secret resolution is exercised"
    );
    let encoded_secret = location.password().unwrap().to_owned();
    location.set_path(&format!("/cdf_conformance/{collection}"));
    let location = location.to_string();
    let auth_source =
        std::env::var("CDF_MONGODB_AUTH_SOURCE").unwrap_or_else(|_| "admin".to_owned());

    let dry = super::tests::invoke_public_cli(
        &project_root,
        false,
        &[
            "add",
            "warehouse.events",
            &location,
            "--option",
            "cursor=updated_at",
            "--option",
            "batch_rows=1",
            "--option",
            &format!("auth_source={auth_source}"),
            "--dry-run",
        ],
    );
    assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
    let add = super::tests::invoke_public_cli(
        &project_root,
        true,
        &[
            "add",
            "warehouse.events",
            &location,
            "--option",
            "cursor=updated_at",
            "--option",
            "batch_rows=1",
            "--option",
            &format!("auth_source={auth_source}"),
        ],
    );
    assert_eq!(add.exit_code, 0, "{}", add.stderr);
    let secret = super::tests::walk_files(&project_root.join(".cdf/secrets"))
        .into_iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .find(|value| {
            url::form_urlencoded::byte_serialize(value.as_bytes()).collect::<String>()
                == encoded_secret
        })
        .expect("MongoDB add must publish the decoded credential to a private secret file");
    assert_ne!(
        secret, encoded_secret,
        "authenticated lifecycle fixture must distinguish decoded and URL-encoded secret forms"
    );
    assert!(location.contains(&encoded_secret));
    super::tests::assert_invocation_redacted(&dry, &secret);
    super::tests::assert_invocation_redacted(&add, &secret);

    // Remove only the scaffolded local source; MongoDB credential references stay active so every
    // contact-bearing lifecycle command exercises secret resolution and output redaction.
    let config_path = project_root.join("cdf.toml");
    let config = std::fs::read_to_string(&config_path).unwrap();
    let mut removing_local = false;
    let config = config
        .lines()
        .filter(|line| {
            if line.trim() == "[sources.local]" {
                removing_local = true;
                return false;
            }
            if removing_local && line.starts_with('[') {
                removing_local = false;
            }
            !removing_local
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&config_path, format!("{config}\n")).unwrap();
    std::fs::remove_dir_all(project_root.join("cdf/local")).unwrap();

    for command in [
        vec!["schema", "discover", "warehouse.events"],
        vec!["schema", "pin", "warehouse.events"],
        vec!["compile", "warehouse.events"],
        vec!["validate"],
        vec!["plan", "warehouse.events"],
    ] {
        let result = super::tests::invoke_public_cli(&project_root, true, &command);
        assert_eq!(
            result.exit_code, 0,
            "command {command:?} failed: {}",
            result.stderr
        );
        super::tests::assert_invocation_redacted(&result, &secret);
    }
    make_lifecycle_runtime_cursor_physically_narrower(&endpoint, collection).unwrap();
    for command in [vec!["preview", "warehouse.events"], vec!["doctor"]] {
        let result = super::tests::invoke_public_cli(&project_root, true, &command);
        assert_eq!(
            result.exit_code, 0,
            "command {command:?} failed: {}",
            result.stderr
        );
        super::tests::assert_invocation_redacted(&result, &secret);
    }

    let jobs_one = base.path().join("jobs-one");
    let jobs_four = base.path().join("jobs-four");
    super::tests::copy_tree(&project_root, &jobs_one, &[]);
    super::tests::copy_tree(&project_root, &jobs_four, &[]);
    let first = super::tests::invoke_public_cli(
        &jobs_one,
        true,
        &["run", "warehouse.events", "--jobs", "1"],
    );
    let second = super::tests::invoke_public_cli(
        &jobs_four,
        true,
        &["run", "warehouse.events", "--jobs", "4"],
    );
    assert_eq!(first.exit_code, 0, "{}", first.stderr);
    assert_eq!(second.exit_code, 0, "{}", second.stderr);
    super::tests::assert_invocation_redacted(&first, &secret);
    super::tests::assert_invocation_redacted(&second, &secret);
    let first_json: serde_json::Value = serde_json::from_str(&first.stdout).unwrap();
    let second_json: serde_json::Value = serde_json::from_str(&second.stdout).unwrap();
    assert_eq!(first_json["result"]["row_count"], 2);
    assert_eq!(second_json["result"]["row_count"], 2);
    assert_eq!(
        first_json["result"]["schema_hash"],
        second_json["result"]["schema_hash"]
    );

    let package_id = first_json["result"]["package_id"].as_str().unwrap();
    let second_package_id = second_json["result"]["package_id"].as_str().unwrap();
    let package = jobs_one.join(".cdf/packages").join(package_id);
    let second_package = jobs_four.join(".cdf/packages").join(second_package_id);
    assert_eq!(
        super::tests::package_identity_semantics(&package),
        super::tests::package_identity_semantics(&second_package)
    );
    assert_eq!(
        super::tests::checkpoint_position_semantics(&package),
        super::tests::checkpoint_position_semantics(&second_package)
    );
    assert_eq!(
        super::tests::receipt_semantics(&package),
        super::tests::receipt_semantics(&second_package)
    );
    assert_physical_reconciliation_stays_out_of_residual_variant(&package);
    std::fs::remove_file(jobs_one.join(".cdf/state.db")).unwrap();
    std::fs::remove_file(jobs_one.join(".cdf/dev.duckdb")).unwrap();
    let replay = super::tests::invoke_public_cli(
        &jobs_one,
        true,
        &["run", "--package", package.to_str().unwrap()],
    );
    assert_eq!(replay.exit_code, 0, "{}", replay.stderr);
    super::tests::assert_invocation_redacted(&replay, &secret);
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
id = "test-project"
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
