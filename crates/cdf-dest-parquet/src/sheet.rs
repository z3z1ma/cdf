use crate::*;

pub(crate) fn parquet_sheet() -> Result<DestinationSheet> {
    Ok(DestinationSheet {
        destination: DestinationId::new(DESTINATION_ID)?,
        supported_dispositions: vec![WriteDisposition::Append, WriteDisposition::Replace],
        transactions: TransactionSupport::AtomicTarget,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: vec![
            mapping("Boolean", "BOOLEAN", TypeMappingFidelity::Lossless),
            mapping("Int8", "INT32", TypeMappingFidelity::Lossless),
            mapping("Int16", "INT32", TypeMappingFidelity::Lossless),
            mapping("Int32", "INT32", TypeMappingFidelity::Lossless),
            mapping("Int64", "INT64", TypeMappingFidelity::Lossless),
            mapping("UInt8", "INT32", TypeMappingFidelity::Lossless),
            mapping("UInt16", "INT32", TypeMappingFidelity::Lossless),
            mapping("UInt32", "INT64", TypeMappingFidelity::Lossless),
            mapping("UInt64", "INT64", TypeMappingFidelity::Lossless),
            mapping("Float32", "FLOAT", TypeMappingFidelity::Lossless),
            mapping("Float64", "DOUBLE", TypeMappingFidelity::Lossless),
            mapping("Utf8", "BYTE_ARRAY/UTF8", TypeMappingFidelity::Lossless),
            mapping(
                "LargeUtf8",
                "BYTE_ARRAY/UTF8",
                TypeMappingFidelity::Lossless,
            ),
            mapping("Binary", "BYTE_ARRAY", TypeMappingFidelity::Lossless),
            mapping("LargeBinary", "BYTE_ARRAY", TypeMappingFidelity::Lossless),
            mapping("Date32", "DATE", TypeMappingFidelity::Lossless),
            mapping(
                "Timestamp(microsecond, none)",
                "TIMESTAMP_MICROS",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Timestamp(*, timezone)",
                "TIMESTAMP",
                TypeMappingFidelity::Unsupported,
            ),
            mapping(
                "Struct/List/Map",
                "nested parquet",
                TypeMappingFidelity::Unsupported,
            ),
            mapping("Union", "union", TypeMappingFidelity::Unsupported),
        ],
        identifier_rules: IdentifierRules {
            normalizer: "object-key-component-v1".to_owned(),
            max_length: None,
            allowed_pattern: None,
        },
        migration_support: CapabilitySupport::Unsupported,
        quarantine_tables: CapabilitySupport::Unsupported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    })
}

pub(crate) fn parquet_correction_capabilities() -> cdf_kernel::DestinationCorrectionCapabilities {
    cdf_kernel::DestinationCorrectionCapabilities::default().with_strategy(
        CorrectionStrategyCapability::new(
            CorrectionStrategy::CorrectionSidecar,
            TransactionSupport::AtomicTarget,
            IdempotencySupport::PackageToken,
        ),
    )
}

fn mapping(
    arrow_type: impl Into<String>,
    destination_type: impl Into<String>,
    fidelity: TypeMappingFidelity,
) -> TypeMapping {
    TypeMapping {
        arrow_type: arrow_type.into(),
        destination_type: destination_type.into(),
        fidelity,
    }
}
