use cdf_aws::AwsControlClient;
use cdf_kernel::Result;
use cdf_object_access::{FileTransportFacade, ObjectStoreClientPool};
use cdf_python::PythonSourceDriver;
use cdf_runtime::{ByteTransformRegistry, FormatRegistry, SourceRegistry};
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver, file_source_blocking_lane};
use cdf_source_glue::{
    AwsGlueCatalogClient as AwsGlueExternalCatalogClient, AwsLakeFormationClient,
    GlueRuntimeDependencies, GlueSourceDriver,
};
use cdf_source_iceberg::{AwsGlueCatalogClient, IcebergRuntimeDependencies, IcebergSourceDriver};
use cdf_source_postgres::PostgresSourceDriver;
use cdf_source_rest::RestSourceDriver;
use std::sync::{Arc, OnceLock};

use crate::http_transport::ReqwestHttpProvider;

static BUILTIN_SOURCE_REGISTRY: OnceLock<SourceRegistry> = OnceLock::new();

pub(crate) fn builtin_source_registry() -> Result<&'static SourceRegistry> {
    if let Some(registry) = BUILTIN_SOURCE_REGISTRY.get() {
        return Ok(registry);
    }
    let registry = build_builtin_source_registry()?;
    let _ = BUILTIN_SOURCE_REGISTRY.set(registry);
    BUILTIN_SOURCE_REGISTRY
        .get()
        .ok_or_else(|| cdf_kernel::CdfError::internal("initialize built-in source registry"))
}

fn build_builtin_source_registry() -> Result<SourceRegistry> {
    let mut registry = SourceRegistry::new();
    registry.register(PythonSourceDriver::new()?)?;
    registry.register(PostgresSourceDriver::new()?)?;
    let http = ReqwestHttpProvider::new()?;
    let rest_http = http.clone();
    registry.register(RestSourceDriver::new(move || {
        Ok(Box::new(rest_http.clone()))
    })?)?;
    // Formats and transforms are process-scoped registries shared by every source adapter that
    // consumes physical objects. Adding a format changes composition only; Glue and file sources
    // never manufacture private codec catalogs.
    let formats = builtin_format_registry()?;
    let transforms = builtin_transform_registry()?;
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
                Arc::new(AwsGlueCatalogClient::new(
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
                std::sync::Arc::clone(&runtime_formats),
                Arc::clone(&runtime_transforms),
                egress,
            ))
        },
    )?)?;
    Ok(registry)
}

pub(crate) fn builtin_transform_registry() -> Result<Arc<ByteTransformRegistry>> {
    use cdf_transform_character::{CharacterEncoding, CharacterTransformDriver};

    let mut registry = ByteTransformRegistry::default();
    registry.register(std::sync::Arc::new(
        cdf_transform_gzip::GzipTransformDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_transform_zstd::ZstdTransformDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_transform_snappy::SnappyFramedTransformDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_transform_lz4::Lz4FrameTransformDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_transform_brotli::BrotliTransformDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_transform_bzip2::Bzip2TransformDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_transform_xz::XzTransformDriver::new()?,
    ))?;
    for encoding in [
        CharacterEncoding::Auto,
        CharacterEncoding::Utf8,
        CharacterEncoding::Utf16Le,
        CharacterEncoding::Utf16Be,
        CharacterEncoding::Windows1252,
        CharacterEncoding::Iso8859_1,
    ] {
        registry.register(std::sync::Arc::new(CharacterTransformDriver::new(
            encoding,
        )?))?;
    }
    Ok(Arc::new(registry))
}

pub(crate) fn builtin_format_registry() -> Result<Arc<FormatRegistry>> {
    let mut registry = FormatRegistry::default();
    registry.register(std::sync::Arc::new(
        cdf_format_arrow_ipc::ArrowIpcFileFormatDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_arrow_ipc::ArrowIpcStreamFormatDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_avro::AvroOcfFormatDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_avro::AvroSingleObjectFormatDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_delimited::CsvFormatDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_delimited::DelimitedFormatDriver::tsv()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_delimited::DelimitedFormatDriver::psv()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_delimited::DelimitedFormatDriver::custom()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_delimited::FixedWidthFormatDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_parquet::ParquetFormatDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_json::NdjsonFormatDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_json::JsonDocumentFormatDriver::new()?,
    ))?;
    Ok(Arc::new(registry))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_registry_is_process_scoped() {
        let first = builtin_source_registry().unwrap();
        let second = builtin_source_registry().unwrap();

        assert!(std::ptr::eq(first, second));
        assert!(
            first
                .descriptors()
                .iter()
                .any(|descriptor| descriptor.driver_id.as_str() == "iceberg")
        );
        assert!(
            first
                .descriptors()
                .iter()
                .any(|descriptor| descriptor.driver_id.as_str() == "glue")
        );
    }

    #[test]
    fn builtin_format_catalog_contains_fixed_width_driver() {
        let formats = builtin_format_registry().unwrap();
        let descriptor = formats
            .descriptors()
            .into_iter()
            .find(|descriptor| descriptor.format_id.as_str() == "fixed_width")
            .expect("fixed-width driver in standard product catalog");

        assert_eq!(descriptor.decode_unit_policy, "fixed_width_stream_v1");
        assert_eq!(
            descriptor.source_access,
            cdf_runtime::FormatSourceAccess::Sequential
        );
    }

    #[test]
    fn builtin_format_catalog_contains_both_avro_framings() {
        let formats = builtin_format_registry().unwrap();
        let descriptors = formats.descriptors();

        assert!(
            descriptors
                .iter()
                .any(|descriptor| descriptor.format_id.as_str() == "avro_ocf")
        );
        assert!(
            descriptors
                .iter()
                .any(|descriptor| descriptor.format_id.as_str() == "avro_single_object")
        );
    }
}
