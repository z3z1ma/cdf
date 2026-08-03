use cdf_kernel::{
    CapabilitySupport, CommitPlan, ConcurrencyLimit, DestinationCommitRequest, DestinationId,
    DestinationProtocol, DestinationProtocolCapabilities, DestinationSheet, IdempotencySupport,
    Receipt, ReceiptVerification, Result, RowProvenanceCapabilities, TransactionSupport,
    WriteDisposition,
};

use crate::{
    CLICKHOUSE_DESTINATION_ID,
    identifier::identifier_rules,
    mapping::type_mappings,
    models::{
        ClickHouseDestination, ClickHouseLoadPlan, ClickHouseLoadPlanInput, ClickHouseMergeMode,
    },
    plan::{plan_clickhouse_commit, plan_clickhouse_load},
    session::verify_receipt,
};

impl ClickHouseDestination {
    pub fn new() -> Result<Self> {
        Ok(Self {
            sheet: clickhouse_destination_sheet()?,
            connection: None,
            target: None,
            execution: None,
            client: Default::default(),
            secret_redaction: None,
            merge_mode: ClickHouseMergeMode::default(),
        })
    }

    pub(crate) fn for_runtime(
        connection: crate::client::ClickHouseConnectionOptions,
        target: crate::identifier::ClickHouseIdentifier,
        secret_redaction: Option<String>,
        merge_mode: ClickHouseMergeMode,
    ) -> Result<Self> {
        Ok(Self {
            sheet: clickhouse_destination_sheet()?,
            connection: Some(connection),
            target: Some(target),
            execution: None,
            client: Default::default(),
            secret_redaction,
            merge_mode,
        })
    }

    pub(crate) fn with_execution_services(
        mut self,
        execution: Option<cdf_runtime::ExecutionServices>,
    ) -> Self {
        self.execution = execution;
        self
    }

    pub(crate) fn plan_load(&self, input: ClickHouseLoadPlanInput) -> Result<ClickHouseLoadPlan> {
        plan_clickhouse_load(input)
    }
}

impl DestinationProtocol for ClickHouseDestination {
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
        plan_clickhouse_commit(request, self.merge_mode)
    }

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        match verify_receipt(self, receipt) {
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

pub(crate) fn clickhouse_destination_sheet() -> Result<DestinationSheet> {
    Ok(DestinationSheet {
        destination: DestinationId::new(CLICKHOUSE_DESTINATION_ID)?,
        supported_dispositions: vec![
            WriteDisposition::Append,
            WriteDisposition::Replace,
            WriteDisposition::Merge,
        ],
        transactions: TransactionSupport::AtomicTarget,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: type_mappings(),
        identifier_rules: identifier_rules(),
        migration_support: CapabilitySupport::Unsupported,
        quarantine_tables: CapabilitySupport::Unsupported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    })
}
