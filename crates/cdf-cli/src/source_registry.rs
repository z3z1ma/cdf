use cdf_kernel::Result;
use cdf_python::PythonSourceDriver;
use cdf_runtime::{ByteTransformRegistry, FormatRegistry, SourceRegistry};
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver, FileTransportFacade};
use cdf_source_postgres::PostgresSourceDriver;
use cdf_source_rest::RestSourceDriver;
use std::sync::{Arc, OnceLock};

use crate::http_transport::{ReqwestHttpFileTransport, ReqwestHttpTransport};

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
    let rest_http = ReqwestHttpTransport::new()?;
    registry.register(RestSourceDriver::new(move || {
        Ok(Box::new(rest_http.clone()))
    })?)?;
    let file_http = ReqwestHttpFileTransport::new()?;
    let formats = builtin_format_registry()?;
    let runtime_formats = std::sync::Arc::clone(&formats);
    registry.register(FileSourceDriver::new(
        formats,
        move |secrets, execution, egress| {
            Ok(FileRuntimeDependencies::new(
                FileTransportFacade::new()
                    .with_http_transport(file_http.clone())
                    .with_shared_secret_provider(secrets)
                    .with_execution_services(execution.clone()),
                execution,
                std::sync::Arc::clone(&runtime_formats),
                builtin_transform_registry()?,
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
        cdf_format_delimited::CsvFormatDriver::new()?,
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
    }
}
