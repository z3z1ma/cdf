use crate::*;

pub(crate) fn duckdb_sheet() -> Result<DestinationSheet> {
    Ok(DestinationSheet {
        destination: DestinationId::new(DESTINATION_ID)?,
        supported_dispositions: vec![
            WriteDisposition::Append,
            WriteDisposition::Replace,
            WriteDisposition::Merge,
        ],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: vec![
            mapping("Boolean", "BOOLEAN", TypeMappingFidelity::Lossless),
            mapping("Int8", "TINYINT", TypeMappingFidelity::Lossless),
            mapping("Int16", "SMALLINT", TypeMappingFidelity::Lossless),
            mapping("Int32", "INTEGER", TypeMappingFidelity::Lossless),
            mapping("Int64", "BIGINT", TypeMappingFidelity::Lossless),
            mapping("UInt8", "UTINYINT", TypeMappingFidelity::Lossless),
            mapping("UInt16", "USMALLINT", TypeMappingFidelity::Lossless),
            mapping("UInt32", "UINTEGER", TypeMappingFidelity::Lossless),
            mapping("UInt64", "UBIGINT", TypeMappingFidelity::Lossless),
            mapping("Float32", "FLOAT", TypeMappingFidelity::Lossless),
            mapping("Float64", "DOUBLE", TypeMappingFidelity::Lossless),
            mapping("Utf8", "VARCHAR", TypeMappingFidelity::Lossless),
            mapping("LargeUtf8", "VARCHAR", TypeMappingFidelity::Lossless),
            mapping("Binary", "BLOB", TypeMappingFidelity::Lossless),
            mapping("LargeBinary", "BLOB", TypeMappingFidelity::Lossless),
            mapping("Date32", "DATE", TypeMappingFidelity::Lossless),
            mapping(
                "Time32(second|millisecond)",
                "TIME",
                TypeMappingFidelity::Lossless,
            ),
            mapping("Time64(microsecond)", "TIME", TypeMappingFidelity::Lossless),
            mapping(
                "Time64(nanosecond)",
                "TIME",
                TypeMappingFidelity::Unsupported,
            ),
            mapping(
                "Timestamp(second|millisecond|microsecond, none)",
                "TIMESTAMP",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Timestamp(*, timezone)",
                "TIMESTAMPTZ",
                TypeMappingFidelity::Unsupported,
            ),
            mapping("Decimal*", "DECIMAL", TypeMappingFidelity::Unsupported),
            mapping(
                "Struct/List/Map",
                "JSON/STRUCT/LIST",
                TypeMappingFidelity::Unsupported,
            ),
        ],
        identifier_rules: IdentifierRules {
            normalizer: "namecase-v1".to_owned(),
            max_length: None,
            allowed_pattern: Some("^[a-z_][a-z0-9_]*$".to_owned()),
        },
        migration_support: CapabilitySupport::Supported,
        quarantine_tables: CapabilitySupport::Unsupported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    })
}

pub(crate) fn duckdb_correction_capabilities() -> cdf_kernel::DestinationCorrectionCapabilities {
    cdf_kernel::DestinationCorrectionCapabilities::default()
        .with_row_provenance(RowProvenanceCapabilities::new(
            CapabilitySupport::Supported,
            CapabilitySupport::Supported,
        ))
        .with_residual_readback(CapabilitySupport::Supported)
        .with_strategy(CorrectionStrategyCapability::new(
            CorrectionStrategy::InPlaceUpdate,
            TransactionSupport::AtomicPackage,
            IdempotencySupport::PackageToken,
        ))
}

pub(crate) fn mapping(
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
