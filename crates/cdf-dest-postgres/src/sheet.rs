use cdf_kernel::{
    CapabilitySupport, CdfError, CommitPlan, ConcurrencyLimit, CorrectionCommitSession,
    DestinationCommitRequest, DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest,
    DestinationCorrectionReceiptEvidence, DestinationId, DestinationProtocol,
    DestinationProtocolCapabilities, DestinationResidualReadback, DestinationSheet,
    IdempotencySupport, IdentifierRules, Receipt, ReceiptVerification, Result,
    RowProvenanceAddress, TargetName, TransactionSupport, WriteDisposition,
};

use crate::{
    POSTGRES_DESTINATION_ID,
    api::plan_postgres_load,
    corrections::{postgres_correction_capabilities, validate_postgres_correction_begin},
    ddl::system_table_migrations,
    models::{
        PostgresCorrectionCommitRequest, PostgresDestination, PostgresDestinationSheet,
        PostgresTypeFidelity, PostgresTypeMapping,
    },
    plan::{PostgresLoadPlan, PostgresLoadPlanInput},
    validate::{delivery_guarantee, ensure_supported_disposition, plan_id},
};

impl Default for PostgresDestination {
    fn default() -> Self {
        Self {
            sheet: postgres_destination_sheet(),
            database_url: None,
            pending_correction: None,
            execution: None,
        }
    }
}

impl PostgresDestination {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_execution_services(
        mut self,
        execution: Option<cdf_runtime::ExecutionServices>,
    ) -> Self {
        self.execution = execution;
        self
    }

    pub fn postgres_sheet(&self) -> &PostgresDestinationSheet {
        &self.sheet
    }

    pub fn plan_load(&self, input: PostgresLoadPlanInput) -> Result<PostgresLoadPlan> {
        plan_postgres_load(input, &self.sheet)
    }

    pub(crate) fn with_correction_request(
        mut self,
        request: PostgresCorrectionCommitRequest,
    ) -> Self {
        self.pending_correction = Some(request);
        self
    }
}

impl DestinationProtocol for PostgresDestination {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet.kernel
    }

    fn protocol_capabilities(&self) -> DestinationProtocolCapabilities {
        DestinationProtocolCapabilities::default()
            .with_corrections(postgres_correction_capabilities())
            .with_routed_target_families()
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        ensure_supported_disposition(&request.disposition)?;
        Ok(CommitPlan {
            plan_id: plan_id(
                &request.target,
                &request.disposition,
                request.package_hash.as_str(),
            )?,
            target: request.target.clone(),
            disposition: request.disposition.clone(),
            idempotency: IdempotencySupport::PackageToken,
            migrations: system_table_migrations(),
            delivery_guarantee: delivery_guarantee(&request.disposition),
        })
    }

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        let verification = self.verify_receipt(receipt)?;
        Ok(ReceiptVerification {
            verified: verification.verified,
            receipt_id: verification.receipt_id,
            reason: verification.reason,
        })
    }

    fn plan_correction(
        &self,
        request: &DestinationCorrectionCommitRequest,
    ) -> Result<DestinationCorrectionCommitPlan> {
        let pending = self.pending_correction.as_ref().ok_or_else(|| {
            CdfError::contract(
                "PostgresDestination::plan_correction requires PostgresDestination::with_correction_request",
            )
        })?;
        validate_postgres_correction_begin(request, &pending.plan.kernel, &pending.plan)?;
        Ok(pending.plan.kernel.clone())
    }

    fn begin_correction(
        &self,
        request: DestinationCorrectionCommitRequest,
        plan: DestinationCorrectionCommitPlan,
    ) -> Result<Box<dyn CorrectionCommitSession + '_>> {
        let pending = self.pending_correction.as_ref().ok_or_else(|| {
            CdfError::contract(
                "PostgresDestination::begin_correction requires PostgresDestination::with_correction_request",
            )
        })?;
        validate_postgres_correction_begin(&request, &plan, &pending.plan)?;
        Ok(Box::new(self.begin_correction_session(
            request,
            pending.plan.clone(),
            pending.package.clone(),
        )?))
    }

    fn verify_correction(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        DestinationCorrectionReceiptEvidence::from_receipt(receipt)?;
        let verification = self.verify_receipt(receipt)?;
        Ok(ReceiptVerification {
            verified: verification.verified,
            receipt_id: verification.receipt_id,
            reason: verification.reason,
        })
    }

    fn read_correction_residual(
        &self,
        target: &TargetName,
        original_row: &RowProvenanceAddress,
    ) -> Result<Option<DestinationResidualReadback>> {
        self.read_addressed_residual(target, original_row)
    }
}

pub fn postgres_destination_sheet() -> PostgresDestinationSheet {
    let type_mappings = postgres_type_mappings();
    let kernel = DestinationSheet {
        destination: DestinationId::new(POSTGRES_DESTINATION_ID).expect("static destination id"),
        supported_dispositions: vec![
            WriteDisposition::Append,
            WriteDisposition::Replace,
            WriteDisposition::Merge,
            WriteDisposition::CdcApply,
        ],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: type_mappings
            .iter()
            .map(PostgresTypeMapping::as_kernel_mapping)
            .collect(),
        identifier_rules: IdentifierRules {
            normalizer: "namecase-v1/postgres-quoted-v1".to_owned(),
            max_length: Some(63),
            allowed_pattern: Some(
                "quoted UTF-8 identifier without NUL; cdf reserves _cdf_*".to_owned(),
            ),
        },
        migration_support: CapabilitySupport::Supported,
        quarantine_tables: CapabilitySupport::Supported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    };

    PostgresDestinationSheet {
        kernel,
        type_mappings,
        migration_operations: vec![
            "create_schema_table".to_owned(),
            "add_nullable_column".to_owned(),
            "transactional_truncate_insert_replace".to_owned(),
        ],
    }
}

pub fn postgres_type_mappings() -> Vec<PostgresTypeMapping> {
    use PostgresTypeFidelity::{Exact, LossyRequiresContractAllowance, Unsupported, Widening};

    vec![
        // Arrow Null has no non-null value domain to lose. PostgreSQL stores every row as SQL NULL
        // in a JSONB-typed column, preserving the complete logical value set without an allowance.
        PostgresTypeMapping::new("Null", "JSONB", Exact),
        PostgresTypeMapping::new("Boolean", "BOOLEAN", Exact),
        PostgresTypeMapping::new("Int8", "SMALLINT", Widening),
        PostgresTypeMapping::new("Int16", "SMALLINT", Exact),
        PostgresTypeMapping::new("Int32", "INTEGER", Exact),
        PostgresTypeMapping::new("Int64", "BIGINT", Exact),
        PostgresTypeMapping::new("UInt8", "SMALLINT", Widening),
        PostgresTypeMapping::new("UInt16", "INTEGER", Widening),
        PostgresTypeMapping::new("UInt32", "BIGINT", Widening),
        PostgresTypeMapping::new("UInt64", "NUMERIC(20,0)", Widening),
        PostgresTypeMapping::new("Float16", "REAL", Widening),
        PostgresTypeMapping::new("Float32", "REAL", Exact),
        PostgresTypeMapping::new("Float64", "DOUBLE PRECISION", Exact),
        PostgresTypeMapping::new("Decimal32(p,s)", "NUMERIC(p,s)", Exact),
        PostgresTypeMapping::new("Decimal64(p,s)", "NUMERIC(p,s)", Exact),
        PostgresTypeMapping::new("Decimal128(p,s)", "NUMERIC(p,s)", Exact),
        PostgresTypeMapping::new("Decimal256(p,s)", "NUMERIC(p,s)", Exact),
        PostgresTypeMapping::new("Utf8", "TEXT", Exact),
        PostgresTypeMapping::new("LargeUtf8", "TEXT", Exact),
        PostgresTypeMapping::new("Utf8View", "TEXT", Exact),
        PostgresTypeMapping::new("Binary", "BYTEA", Exact),
        PostgresTypeMapping::new("LargeBinary", "BYTEA", Exact),
        PostgresTypeMapping::new("BinaryView", "BYTEA", Exact),
        PostgresTypeMapping::new("FixedSizeBinary(*)", "BYTEA", Exact),
        PostgresTypeMapping::new("Date32", "DATE", Exact),
        PostgresTypeMapping::new("Date64", "TIMESTAMP", Exact),
        PostgresTypeMapping::new("Time32(second|millisecond)", "TIME", Exact),
        PostgresTypeMapping::new("Time32", "unsupported", Unsupported),
        PostgresTypeMapping::new("Time64(Microsecond)", "TIME", Exact),
        PostgresTypeMapping::new("Time64(Nanosecond)", "TIME", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("Time64", "unsupported", Unsupported),
        PostgresTypeMapping::new(
            "Timestamp(second|millisecond|microsecond,None)",
            "TIMESTAMP",
            Exact,
        ),
        PostgresTypeMapping::new("Timestamp(*,timezone)", "TIMESTAMPTZ", Exact),
        PostgresTypeMapping::new(
            "Timestamp(Nanosecond,*)",
            "TIMESTAMP/TIMESTAMPTZ",
            LossyRequiresContractAllowance,
        ),
        PostgresTypeMapping::new("Struct", "JSONB", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("List*", "JSONB", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("Map", "JSONB", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("Union", "JSONB", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("Dictionary", "JSONB", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("Duration", "JSONB", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("Interval", "JSONB", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("RunEndEncoded", "JSONB", LossyRequiresContractAllowance),
    ]
}
