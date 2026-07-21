use crate::*;

pub(crate) fn parquet_sheet() -> Result<DestinationSheet> {
    Ok(DestinationSheet {
        destination: DestinationId::new(DESTINATION_ID)?,
        supported_dispositions: vec![WriteDisposition::Append, WriteDisposition::Replace],
        transactions: TransactionSupport::AtomicTarget,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: vec![
            mapping("Null", "UNKNOWN", TypeMappingFidelity::Lossless),
            mapping("Boolean", "BOOLEAN", TypeMappingFidelity::Lossless),
            mapping("Int8", "INT32", TypeMappingFidelity::Lossless),
            mapping("Int16", "INT32", TypeMappingFidelity::Lossless),
            mapping("Int32", "INT32", TypeMappingFidelity::Lossless),
            mapping("Int64", "INT64", TypeMappingFidelity::Lossless),
            mapping("UInt8", "INT32", TypeMappingFidelity::Lossless),
            mapping("UInt16", "INT32", TypeMappingFidelity::Lossless),
            mapping("UInt32", "INT64", TypeMappingFidelity::Lossless),
            mapping("UInt64", "INT64", TypeMappingFidelity::Lossless),
            mapping("Float16", "FLOAT16", TypeMappingFidelity::Lossless),
            mapping("Float32", "FLOAT", TypeMappingFidelity::Lossless),
            mapping("Float64", "DOUBLE", TypeMappingFidelity::Lossless),
            mapping("Utf8", "BYTE_ARRAY/UTF8", TypeMappingFidelity::Lossless),
            mapping(
                "LargeUtf8",
                "BYTE_ARRAY/UTF8",
                TypeMappingFidelity::Lossless,
            ),
            mapping("Utf8View", "BYTE_ARRAY/UTF8", TypeMappingFidelity::Lossless),
            mapping("Binary", "BYTE_ARRAY", TypeMappingFidelity::Lossless),
            mapping("LargeBinary", "BYTE_ARRAY", TypeMappingFidelity::Lossless),
            mapping("BinaryView", "BYTE_ARRAY", TypeMappingFidelity::Lossless),
            mapping(
                "FixedSizeBinary(*)",
                "FIXED_LEN_BYTE_ARRAY",
                TypeMappingFidelity::Lossless,
            ),
            mapping("Date32", "DATE", TypeMappingFidelity::Lossless),
            mapping(
                "Date64",
                "INT64 + embedded Arrow schema",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Time32(second|millisecond)",
                "TIME_MILLIS",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Time64(microsecond)",
                "TIME_MICROS",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Time64(nanosecond)",
                "TIME_NANOS",
                TypeMappingFidelity::Lossless,
            ),
            mapping("Time32", "unsupported", TypeMappingFidelity::Unsupported),
            mapping("Time64", "unsupported", TypeMappingFidelity::Unsupported),
            mapping(
                "Timestamp(second,*)",
                "TIMESTAMP_MILLIS + embedded Arrow schema",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Timestamp(millisecond,*)",
                "TIMESTAMP_MILLIS + embedded Arrow schema",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Timestamp(microsecond,*)",
                "TIMESTAMP_MICROS",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Timestamp(nanosecond,*)",
                "TIMESTAMP_NANOS",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Decimal32(p,s)",
                "INT32 DECIMAL",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Decimal64(p,s)",
                "INT64 DECIMAL",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Decimal128(p,s)",
                "FIXED_LEN_BYTE_ARRAY/INT32/INT64 DECIMAL",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Decimal256(p,s)",
                "FIXED_LEN_BYTE_ARRAY DECIMAL",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Duration",
                "INT64 + embedded Arrow schema",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Interval(YearMonth|DayTime)",
                "FIXED_LEN_BYTE_ARRAY/INTERVAL",
                TypeMappingFidelity::Lossless,
            ),
            mapping(
                "Interval(MonthDayNano)",
                "unsupported",
                TypeMappingFidelity::Unsupported,
            ),
            mapping("Struct", "GROUP", TypeMappingFidelity::Lossless),
            mapping("List*", "LIST", TypeMappingFidelity::Lossless),
            mapping("Map", "MAP", TypeMappingFidelity::Lossless),
            mapping(
                "Dictionary",
                "logical value + embedded Arrow schema",
                TypeMappingFidelity::Lossless,
            ),
            mapping("Union", "union", TypeMappingFidelity::Unsupported),
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
        migration_support: CapabilitySupport::Unsupported,
        quarantine_tables: CapabilitySupport::Unsupported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    })
}

pub(crate) fn parquet_correction_capabilities() -> cdf_kernel::DestinationCorrectionCapabilities {
    cdf_kernel::DestinationCorrectionCapabilities::default()
        .with_row_provenance(RowProvenanceCapabilities::new(
            CapabilitySupport::Supported,
            CapabilitySupport::Supported,
        ))
        .with_strategy(CorrectionStrategyCapability::new(
            CorrectionStrategy::CorrectionSidecar,
            TransactionSupport::AtomicTarget,
            IdempotencySupport::PackageToken,
        ))
}

pub(crate) fn parquet_protocol_capabilities() -> cdf_kernel::DestinationProtocolCapabilities {
    cdf_kernel::DestinationProtocolCapabilities::default()
        .with_corrections(parquet_correction_capabilities())
        .with_object_key_rules(ObjectKeyRules::component_v1())
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
