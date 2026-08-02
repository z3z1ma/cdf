use std::path::{Path, PathBuf};

use cdf_kernel::{
    CapabilitySupport, CommitPlan, ConcurrencyLimit, DestinationCommitRequest, DestinationId,
    DestinationProtocol, DestinationProtocolCapabilities, DestinationSheet, IdempotencySupport,
    Receipt, ReceiptVerification, Result, RowProvenanceCapabilities, TransactionSupport,
    WriteDisposition,
};

use crate::{
    SQLITE_DESTINATION_ID,
    identifier::sqlite_identifier_rules,
    mapping::sqlite_type_mappings,
    models::{SqliteDestination, SqliteLoadPlan, SqliteLoadPlanInput},
    plan::{plan_sqlite_commit, plan_sqlite_load},
    transaction::verify_receipt_with_cancellation,
};

impl SqliteDestination {
    pub fn connect(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if path.as_os_str().is_empty() {
            return Err(cdf_kernel::CdfError::contract(
                "SQLite destination path cannot be empty",
            ));
        }
        Ok(Self {
            sheet: sqlite_destination_sheet()?,
            database_path: Some(path),
            target: None,
            execution: None,
        })
    }

    pub(crate) fn for_runtime(
        path: PathBuf,
        target: crate::identifier::SqliteIdentifier,
    ) -> Result<Self> {
        Ok(Self {
            sheet: sqlite_destination_sheet()?,
            database_path: Some(path),
            target: Some(target),
            execution: None,
        })
    }

    pub(crate) fn with_execution_services(
        mut self,
        execution: Option<cdf_runtime::ExecutionServices>,
    ) -> Self {
        self.execution = execution;
        self
    }

    pub(crate) fn database_path(&self) -> Result<&Path> {
        self.database_path.as_deref().ok_or_else(|| {
            cdf_kernel::CdfError::contract(
                "SQLite destination operation requires a connected database path",
            )
        })
    }

    pub(crate) fn plan_load(&self, input: SqliteLoadPlanInput) -> Result<SqliteLoadPlan> {
        plan_sqlite_load(input)
    }
}

impl DestinationProtocol for SqliteDestination {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet
    }

    fn protocol_capabilities(&self) -> DestinationProtocolCapabilities {
        DestinationProtocolCapabilities::default().with_corrections(
            cdf_kernel::DestinationCorrectionCapabilities::default().with_row_provenance(
                RowProvenanceCapabilities::new(
                    CapabilitySupport::Supported,
                    CapabilitySupport::Supported,
                ),
            ),
        )
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        plan_sqlite_commit(request)
    }

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        let cancellation = self
            .execution
            .as_ref()
            .map(cdf_runtime::ExecutionServices::run_cancellation)
            .unwrap_or_default();
        match verify_receipt_with_cancellation(self.database_path()?, receipt, &cancellation) {
            Ok(()) => Ok(ReceiptVerification {
                verified: true,
                receipt_id: receipt.receipt_id.clone(),
                reason: None,
            }),
            Err(error) => Ok(ReceiptVerification {
                verified: false,
                receipt_id: receipt.receipt_id.clone(),
                reason: Some(error.to_string()),
            }),
        }
    }
}

pub(crate) fn sqlite_destination_sheet() -> Result<DestinationSheet> {
    Ok(DestinationSheet {
        destination: DestinationId::new(SQLITE_DESTINATION_ID)?,
        supported_dispositions: vec![
            WriteDisposition::Append,
            WriteDisposition::Replace,
            WriteDisposition::Merge,
        ],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: sqlite_type_mappings(),
        identifier_rules: sqlite_identifier_rules(),
        migration_support: CapabilitySupport::Supported,
        quarantine_tables: CapabilitySupport::Supported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    })
}
