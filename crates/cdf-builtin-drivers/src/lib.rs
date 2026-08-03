#![doc = "Deterministic construction of the complete first-party cdf driver catalogs."]

use std::sync::{Arc, Mutex, OnceLock};

use cdf_aws::AwsControlClient;
use cdf_dest_duckdb::DuckDbRuntimeDriver;
use cdf_dest_parquet::ParquetRuntimeDriver;
use cdf_dest_postgres::PostgresRuntimeDriver;
use cdf_dest_sqlite::SqliteRuntimeDriver;
use cdf_kernel::{CdfError, Result};
use cdf_object_access::{FileTransportFacade, ObjectStoreClientPool};
use cdf_python::PythonSourceDriver;
use cdf_runtime::{ByteTransformRegistry, DestinationRegistry, FormatRegistry, SourceRegistry};
use cdf_source_clickhouse::ClickHouseSourceDriver;
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver, file_source_blocking_lane};
use cdf_source_glue::{
    AwsGlueCatalogClient as AwsGlueExternalCatalogClient, AwsLakeFormationClient,
    GlueRuntimeDependencies, GlueSourceDriver,
};
use cdf_source_iceberg::{
    AwsIcebergGlueCatalogClient, IcebergRuntimeDependencies, IcebergSourceDriver,
};
use cdf_source_postgres::PostgresSourceDriver;
use cdf_source_rest::RestSourceDriver;
use cdf_source_sqlite::SqliteSourceDriver;
use cdf_transport_http::ReqwestHttpProvider;

struct ProcessCatalogs {
    sources: SourceRegistry,
    formats: Arc<FormatRegistry>,
    transforms: Arc<ByteTransformRegistry>,
}

static PROCESS_CATALOGS: OnceLock<ProcessCatalogs> = OnceLock::new();
static PROCESS_CATALOGS_INIT: Mutex<()> = Mutex::new(());

struct BuiltinDestinationEntry {
    #[cfg(test)]
    destination_id: &'static str,
    #[cfg(test)]
    schemes: &'static [&'static str],
    #[cfg(test)]
    inspection_uri: &'static str,
    install: fn(&mut DestinationRegistry) -> Result<()>,
}

const BUILTIN_DESTINATIONS: &[BuiltinDestinationEntry] = &[
    BuiltinDestinationEntry {
        #[cfg(test)]
        destination_id: "duckdb",
        #[cfg(test)]
        schemes: &["duckdb"],
        #[cfg(test)]
        inspection_uri: "duckdb:///tmp/cdf-builtin-catalog.duckdb",
        install: |registry| registry.register(DuckDbRuntimeDriver),
    },
    BuiltinDestinationEntry {
        #[cfg(test)]
        destination_id: "parquet_object_store",
        #[cfg(test)]
        schemes: &["parquet"],
        #[cfg(test)]
        inspection_uri: "parquet:///tmp/cdf-builtin-catalog-parquet",
        install: |registry| registry.register(ParquetRuntimeDriver),
    },
    BuiltinDestinationEntry {
        #[cfg(test)]
        destination_id: "postgres",
        #[cfg(test)]
        schemes: &["postgres", "postgresql"],
        #[cfg(test)]
        inspection_uri: "postgres://localhost/cdf_builtin_catalog",
        install: |registry| registry.register(PostgresRuntimeDriver),
    },
    BuiltinDestinationEntry {
        #[cfg(test)]
        destination_id: "sqlite",
        #[cfg(test)]
        schemes: &["sqlite"],
        #[cfg(test)]
        inspection_uri: "sqlite:///tmp/cdf-builtin-catalog.sqlite",
        install: |registry| registry.register(SqliteRuntimeDriver),
    },
];

/// Returns the process-scoped source registry shipped by the standard product.
///
/// Source drivers retain shared HTTP, object-store, format, and transform dependencies for the
/// lifetime of the process. No source-specific type escapes this catalog boundary.
pub fn builtin_source_registry() -> Result<&'static SourceRegistry> {
    Ok(&process_catalogs()?.sources)
}

/// Returns the process-scoped format registry used by the shipped source catalog.
pub fn builtin_format_registry() -> Result<Arc<FormatRegistry>> {
    Ok(Arc::clone(&process_catalogs()?.formats))
}

/// Returns the process-scoped byte-transform registry used by the shipped source catalog.
pub fn builtin_transform_registry() -> Result<Arc<ByteTransformRegistry>> {
    Ok(Arc::clone(&process_catalogs()?.transforms))
}

/// Constructs an independent copy of the shipped format registry.
///
/// This is intended for first-party tests and harnesses that need to add a synthetic format
/// without mutating the process-scoped product catalog.
pub fn new_builtin_format_registry() -> Result<FormatRegistry> {
    let mut registry = FormatRegistry::default();
    registry.register(Arc::new(
        cdf_format_arrow_ipc::ArrowIpcFileFormatDriver::new()?,
    ))?;
    registry.register(Arc::new(
        cdf_format_arrow_ipc::ArrowIpcStreamFormatDriver::new()?,
    ))?;
    registry.register(Arc::new(cdf_format_delimited::CsvFormatDriver::new()?))?;
    registry.register(Arc::new(cdf_format_delimited::DelimitedFormatDriver::tsv()?))?;
    registry.register(Arc::new(cdf_format_delimited::DelimitedFormatDriver::psv()?))?;
    registry.register(Arc::new(
        cdf_format_delimited::DelimitedFormatDriver::custom()?,
    ))?;
    registry.register(Arc::new(
        cdf_format_delimited::FixedWidthFormatDriver::new()?
    ))?;
    registry.register(Arc::new(cdf_format_parquet::ParquetFormatDriver::new()?))?;
    registry.register(Arc::new(cdf_format_protobuf::ProtobufFormatDriver::new()?))?;
    registry.register(Arc::new(cdf_format_json::NdjsonFormatDriver::new()?))?;
    registry.register(Arc::new(cdf_format_json::JsonDocumentFormatDriver::new()?))?;
    Ok(registry)
}

/// Constructs an independent copy of the shipped byte-transform registry.
///
/// This is intended for first-party tests and harnesses that need a non-global catalog value.
pub fn new_builtin_transform_registry() -> Result<ByteTransformRegistry> {
    use cdf_transform_character::{CharacterEncoding, CharacterTransformDriver};

    let mut registry = ByteTransformRegistry::default();
    registry.register(Arc::new(cdf_transform_gzip::GzipTransformDriver::new()?))?;
    registry.register(Arc::new(cdf_transform_zstd::ZstdTransformDriver::new()?))?;
    registry.register(Arc::new(
        cdf_transform_snappy::SnappyFramedTransformDriver::new()?,
    ))?;
    registry.register(Arc::new(cdf_transform_lz4::Lz4FrameTransformDriver::new()?))?;
    registry.register(Arc::new(cdf_transform_brotli::BrotliTransformDriver::new()?))?;
    registry.register(Arc::new(cdf_transform_bzip2::Bzip2TransformDriver::new()?))?;
    registry.register(Arc::new(cdf_transform_xz::XzTransformDriver::new()?))?;
    for encoding in [
        CharacterEncoding::Auto,
        CharacterEncoding::Utf8,
        CharacterEncoding::Utf16Le,
        CharacterEncoding::Utf16Be,
        CharacterEncoding::Windows1252,
        CharacterEncoding::Iso8859_1,
    ] {
        registry.register(Arc::new(CharacterTransformDriver::new(encoding)?))?;
    }
    Ok(registry)
}

/// Constructs the complete destination registry shipped by the standard product.
pub fn builtin_destination_registry() -> Result<DestinationRegistry> {
    let mut registry = DestinationRegistry::new();
    for entry in BUILTIN_DESTINATIONS {
        (entry.install)(&mut registry)?;
    }
    Ok(registry)
}

fn process_catalogs() -> Result<&'static ProcessCatalogs> {
    if let Some(catalogs) = PROCESS_CATALOGS.get() {
        return Ok(catalogs);
    }
    let _initialization = PROCESS_CATALOGS_INIT
        .lock()
        .map_err(|_| CdfError::internal("built-in driver catalog initialization lock poisoned"))?;
    if let Some(catalogs) = PROCESS_CATALOGS.get() {
        return Ok(catalogs);
    }
    PROCESS_CATALOGS
        .set(build_process_catalogs()?)
        .map_err(|_| CdfError::internal("built-in driver catalogs initialized concurrently"))?;
    PROCESS_CATALOGS
        .get()
        .ok_or_else(|| CdfError::internal("initialize built-in driver catalogs"))
}

fn build_process_catalogs() -> Result<ProcessCatalogs> {
    let formats = Arc::new(new_builtin_format_registry()?);
    let transforms = Arc::new(new_builtin_transform_registry()?);
    let sources = build_source_registry(Arc::clone(&formats), Arc::clone(&transforms))?;
    Ok(ProcessCatalogs {
        sources,
        formats,
        transforms,
    })
}

fn build_source_registry(
    formats: Arc<FormatRegistry>,
    transforms: Arc<ByteTransformRegistry>,
) -> Result<SourceRegistry> {
    let mut registry = SourceRegistry::new();
    registry.register(ClickHouseSourceDriver::new()?)?;
    registry.register(PythonSourceDriver::new()?)?;
    registry.register(PostgresSourceDriver::new()?)?;
    registry.register(SqliteSourceDriver::new()?)?;
    let http = ReqwestHttpProvider::new()?;
    let rest_http = http.clone();
    registry.register(RestSourceDriver::new(move || {
        Ok(Box::new(rest_http.clone()))
    })?)?;

    let iceberg_http = http.clone();
    let glue_http = http.clone();
    let file_http = http;
    let object_store_clients = ObjectStoreClientPool::default();
    let iceberg_object_store_clients = object_store_clients.clone();
    let glue_object_store_clients = object_store_clients.clone();
    registry.register(IcebergSourceDriver::new(
        move |secrets, execution, egress, local_listing_lane| {
            let rest_http: Arc<dyn cdf_http::HttpTransport> = Arc::new(iceberg_http.clone());
            Ok(IcebergRuntimeDependencies::new(
                Arc::new(
                    FileTransportFacade::new()
                        .with_http_transport(iceberg_http.clone())
                        .with_shared_secret_provider(Arc::clone(&secrets))
                        .with_shared_object_store_clients(iceberg_object_store_clients.clone())
                        .with_execution_services(execution.clone())
                        .with_local_listing_lane(local_listing_lane)?,
                ),
                Arc::clone(&rest_http),
                Arc::new(AwsIcebergGlueCatalogClient::new(
                    rest_http, secrets, execution, egress,
                )),
            ))
        },
    )?)?;
    let glue_formats = Arc::clone(&formats);
    let glue_transforms = Arc::clone(&transforms);
    registry.register(GlueSourceDriver::new(move |secrets, execution, egress| {
        let control_http: Arc<dyn cdf_http::HttpTransport> = Arc::new(glue_http.clone());
        let object_access = FileTransportFacade::new()
            .with_http_transport(glue_http.clone())
            .with_shared_secret_provider(Arc::clone(&secrets))
            .with_shared_object_store_clients(glue_object_store_clients.clone())
            .with_execution_services(execution.clone())
            .with_local_listing_lane(file_source_blocking_lane())?;
        let aws = Arc::new(AwsControlClient::new(
            control_http,
            secrets,
            execution,
            egress,
        ));
        Ok(GlueRuntimeDependencies::new(
            Arc::new(object_access),
            Arc::new(AwsGlueExternalCatalogClient::new(Arc::clone(&aws))),
            Arc::new(AwsLakeFormationClient::new(Arc::clone(&aws))),
            Arc::clone(&glue_formats),
            Arc::clone(&glue_transforms),
        ))
    })?)?;
    let runtime_formats = Arc::clone(&formats);
    let runtime_transforms = Arc::clone(&transforms);
    registry.register(FileSourceDriver::new(
        formats,
        move |secrets, execution, egress| {
            Ok(FileRuntimeDependencies::new(
                FileTransportFacade::new()
                    .with_http_transport(file_http.clone())
                    .with_shared_secret_provider(secrets)
                    .with_shared_object_store_clients(object_store_clients.clone())
                    .with_execution_services(execution.clone())
                    .with_local_listing_lane(file_source_blocking_lane())?,
                execution,
                Arc::clone(&runtime_formats),
                Arc::clone(&runtime_transforms),
                egress,
            ))
        },
    )?)?;
    Ok(registry)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::Path, process::Command};

    use super::*;

    #[test]
    fn catalog_matches_the_data_driven_first_party_fixture() {
        let expected: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/catalog.json")).unwrap();
        let sources = builtin_source_registry().unwrap();
        let source_schemas = sources.option_schemas();
        let destinations = builtin_destination_registry().unwrap();
        let destination_context = cdf_runtime::DestinationResolutionContext::for_project_inspection(
            Path::new(env!("CARGO_MANIFEST_DIR")),
        );
        let source_artifacts = sources
            .descriptors()
            .into_iter()
            .map(|descriptor| {
                let id = descriptor.driver_id.as_str().to_owned();
                serde_json::json!({
                    "id": id,
                    "descriptor_sha256": cdf_runtime::artifact_hash(&descriptor).unwrap(),
                    "option_schema_sha256": cdf_runtime::artifact_hash(
                        source_schemas.get(descriptor.driver_id.as_str()).unwrap()
                    )
                    .unwrap(),
                })
            })
            .collect::<Vec<_>>();
        let format_artifacts = builtin_format_registry()
            .unwrap()
            .descriptors()
            .into_iter()
            .map(|descriptor| {
                serde_json::json!({
                    "id": descriptor.format_id.as_str(),
                    "descriptor_sha256": cdf_runtime::artifact_hash(&descriptor).unwrap(),
                })
            })
            .collect::<Vec<_>>();
        let transform_artifacts = builtin_transform_registry()
            .unwrap()
            .descriptors()
            .into_iter()
            .map(|descriptor| {
                serde_json::json!({
                    "id": descriptor.transform_id.as_str(),
                    "descriptor_sha256": cdf_runtime::artifact_hash(&descriptor).unwrap(),
                })
            })
            .collect::<Vec<_>>();
        let destination_artifacts = BUILTIN_DESTINATIONS
            .iter()
            .map(|entry| {
                let inspection = destinations
                    .inspect(entry.inspection_uri, &destination_context)
                    .unwrap();
                assert_eq!(
                    inspection.description.destination_id.as_str(),
                    entry.destination_id
                );
                assert_eq!(inspection.description.schemes, entry.schemes);
                let artifact = serde_json::json!({
                    "description": {
                        "destination_id": inspection.description.destination_id.as_str(),
                        "schemes": inspection.description.schemes,
                        "label": inspection.description.label,
                        "product_location_field": inspection.description.product_location_field,
                        "product_receipt_source": inspection.description.product_receipt_source,
                    },
                    "sheet_artifact": inspection.sheet_artifact,
                    "sheet_artifact_hash": inspection.sheet_artifact_hash,
                    "runtime": inspection.runtime,
                    "health_probes": inspection.health_probes,
                });
                serde_json::json!({
                    "id": entry.destination_id,
                    "schemes": inspection.description.schemes,
                    "inspection_sha256": cdf_runtime::artifact_hash(&artifact).unwrap(),
                })
            })
            .collect::<Vec<_>>();
        let actual = serde_json::json!({
            "sources": source_artifacts,
            "formats": format_artifacts,
            "transforms": transform_artifacts,
            "destinations": destination_artifacts,
        });
        assert_eq!(actual, expected);
    }

    #[test]
    fn source_catalog_and_dependencies_are_process_scoped() {
        let first_sources = builtin_source_registry().unwrap();
        let second_sources = builtin_source_registry().unwrap();
        let first_formats = builtin_format_registry().unwrap();
        let second_formats = builtin_format_registry().unwrap();
        let first_transforms = builtin_transform_registry().unwrap();
        let second_transforms = builtin_transform_registry().unwrap();

        assert!(std::ptr::eq(first_sources, second_sources));
        assert!(Arc::ptr_eq(&first_formats, &second_formats));
        assert!(Arc::ptr_eq(&first_transforms, &second_transforms));
    }

    #[test]
    fn manifest_graph_confines_the_leaf_to_product_and_harness_roots() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let allowed_normal_consumers = ["cdf-benchmarks", "cdf-cli", "cdf-conformance"];
        let output = Command::new(env!("CARGO"))
            .args([
                "metadata",
                "--format-version",
                "1",
                "--locked",
                "--all-features",
            ])
            .current_dir(&workspace)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let packages = metadata["packages"].as_array().unwrap();
        let package_names = packages
            .iter()
            .map(|package| {
                (
                    package["id"].as_str().unwrap().to_owned(),
                    package["name"].as_str().unwrap().to_owned(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let leaf_id = packages
            .iter()
            .find(|package| package["name"] == "cdf-builtin-drivers")
            .and_then(|package| package["id"].as_str())
            .unwrap();
        let mut unexpected_consumers = Vec::new();
        for node in metadata["resolve"]["nodes"].as_array().unwrap() {
            for dependency in node["deps"].as_array().unwrap() {
                if dependency["pkg"] != leaf_id {
                    continue;
                }
                let reaches_non_dev_graph = dependency["dep_kinds"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|kind| kind["kind"].as_str() != Some("dev"));
                let package = package_names.get(node["id"].as_str().unwrap()).unwrap();
                if reaches_non_dev_graph && !allowed_normal_consumers.contains(&package.as_str()) {
                    unexpected_consumers.push(package.clone());
                }
            }
        }
        unexpected_consumers.sort();
        unexpected_consumers.dedup();
        assert!(
            unexpected_consumers.is_empty(),
            "built-in catalog leaked below product/harness composition: {unexpected_consumers:?}"
        );

        let cli = packages
            .iter()
            .find(|package| package["name"] == "cdf-cli")
            .unwrap();
        assert_eq!(
            cli["features"]["bundled-duckdb"],
            serde_json::json!(["cdf-builtin-drivers/bundled-duckdb"])
        );
    }
}
