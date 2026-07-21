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
            mapping("Null", "VARCHAR", TypeMappingFidelity::Lossless),
            mapping("Boolean", "BOOLEAN", TypeMappingFidelity::Lossless),
            mapping("Int8", "TINYINT", TypeMappingFidelity::Lossless),
            mapping("Int16", "SMALLINT", TypeMappingFidelity::Lossless),
            mapping("Int32", "INTEGER", TypeMappingFidelity::Lossless),
            mapping("Int64", "BIGINT", TypeMappingFidelity::Lossless),
            mapping("UInt8", "UTINYINT", TypeMappingFidelity::Lossless),
            mapping("UInt16", "USMALLINT", TypeMappingFidelity::Lossless),
            mapping("UInt32", "UINTEGER", TypeMappingFidelity::Lossless),
            mapping("UInt64", "UBIGINT", TypeMappingFidelity::Lossless),
            mapping("Float16", "unsupported", TypeMappingFidelity::Unsupported),
            mapping("Float32", "FLOAT", TypeMappingFidelity::Lossless),
            mapping("Float64", "DOUBLE", TypeMappingFidelity::Lossless),
            mapping("Utf8", "VARCHAR", TypeMappingFidelity::Lossless),
            mapping("LargeUtf8", "VARCHAR", TypeMappingFidelity::Lossless),
            mapping("Utf8View", "VARCHAR", TypeMappingFidelity::Lossless),
            mapping("Binary", "BLOB", TypeMappingFidelity::Lossless),
            mapping("LargeBinary", "BLOB", TypeMappingFidelity::Lossless),
            mapping("BinaryView", "BLOB", TypeMappingFidelity::Lossless),
            mapping("FixedSizeBinary(*)", "BLOB", TypeMappingFidelity::Lossless),
            mapping("Date32", "DATE", TypeMappingFidelity::Lossless),
            mapping("Date64", "DATE", TypeMappingFidelity::Lossless),
            mapping(
                "Time32(second|millisecond)",
                "TIME",
                TypeMappingFidelity::Lossless,
            ),
            mapping("Time64(microsecond)", "TIME", TypeMappingFidelity::Lossless),
            mapping(
                "Time64(nanosecond)",
                "TIME_NS",
                TypeMappingFidelity::Lossless,
            ),
            mapping("Time32", "unsupported", TypeMappingFidelity::Unsupported),
            mapping("Time64", "unsupported", TypeMappingFidelity::Unsupported),
            mapping(
                "Timestamp(second|millisecond|microsecond, none)",
                "TIMESTAMP",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Timestamp(nanosecond, none)",
                "TIMESTAMP_NS",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Timestamp(nanosecond,*)",
                "TIMESTAMPTZ",
                TypeMappingFidelity::Unsupported,
            ),
            mapping(
                "Timestamp(*, timezone)",
                "TIMESTAMPTZ",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Decimal32(precision<=9, scale>=0)",
                "DECIMAL",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Decimal64(precision<=18, scale>=0)",
                "DECIMAL",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Decimal128(precision<=38, scale>=0)",
                "DECIMAL",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Decimal32(p,s)",
                "unsupported",
                TypeMappingFidelity::Unsupported,
            ),
            mapping(
                "Decimal64(p,s)",
                "unsupported",
                TypeMappingFidelity::Unsupported,
            ),
            mapping(
                "Decimal128(p,s)",
                "DECIMAL",
                TypeMappingFidelity::Unsupported,
            ),
            mapping(
                "Decimal256(p,s)",
                "DECIMAL/DOUBLE",
                TypeMappingFidelity::Unsupported,
            ),
            mapping("Struct", "STRUCT", TypeMappingFidelity::Lossless),
            mapping("List", "LIST", TypeMappingFidelity::Lossless),
            mapping("LargeList", "LIST", TypeMappingFidelity::Lossless),
            mapping("FixedSizeList", "ARRAY", TypeMappingFidelity::Lossless),
            mapping("ListView", "LIST", TypeMappingFidelity::Lossless),
            mapping("LargeListView", "LIST", TypeMappingFidelity::Lossless),
            mapping("Map", "MAP", TypeMappingFidelity::Lossless),
            mapping("Union(Sparse)", "UNION", TypeMappingFidelity::Lossless),
            mapping(
                "Union(Dense)",
                "unsupported",
                TypeMappingFidelity::Unsupported,
            ),
            mapping(
                "Dictionary",
                "dictionary value type",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Duration(second|millisecond|microsecond)",
                "INTERVAL",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Duration(nanosecond)",
                "INTERVAL",
                TypeMappingFidelity::LossyRequiresContractAllowance,
            ),
            mapping(
                "Interval(YearMonth|DayTime)",
                "INTERVAL",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Interval(MonthDayNano)",
                "INTERVAL",
                TypeMappingFidelity::LossyRequiresContractAllowance,
            ),
            mapping(
                "RunEndEncoded",
                "unsupported",
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
