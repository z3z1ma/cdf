use cdf_kernel::Result;
use cdf_runtime::{ByteTransformRegistry, FormatRegistry, SourceRegistry};
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver, FileTransportFacade};
use cdf_source_postgres::PostgresSourceDriver;
use cdf_source_rest::RestSourceDriver;

use crate::http_transport::ReqwestHttpTransport;

pub(crate) fn builtin_source_registry() -> Result<SourceRegistry> {
    let mut registry = SourceRegistry::new();
    registry.register(PostgresSourceDriver::new()?)?;
    registry.register(RestSourceDriver::new(|| {
        Ok(Box::new(ReqwestHttpTransport::new()?))
    })?)?;
    registry.register(FileSourceDriver::new(|secrets, execution| {
        Ok(FileRuntimeDependencies::new(
            FileTransportFacade::new()
                .with_http_transport(ReqwestHttpTransport::new()?)
                .with_shared_secret_provider(secrets)
                .with_execution_services(execution.clone()),
            execution,
            builtin_format_registry()?,
            builtin_transform_registry()?,
        ))
    })?)?;
    Ok(registry)
}

pub(crate) fn builtin_transform_registry() -> Result<std::sync::Arc<ByteTransformRegistry>> {
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
    Ok(std::sync::Arc::new(registry))
}

pub(crate) fn builtin_format_registry() -> Result<std::sync::Arc<FormatRegistry>> {
    let mut registry = FormatRegistry::default();
    registry.register(std::sync::Arc::new(
        cdf_format_arrow_ipc::ArrowIpcFileFormatDriver::new()?,
    ))?;
    registry.register(std::sync::Arc::new(
        cdf_format_parquet::ParquetFormatDriver::new()?,
    ))?;
    Ok(std::sync::Arc::new(registry))
}
