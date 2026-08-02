#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectReceiptSource {
    DestinationCommit {
        duplicate: bool,
        package_receipt_recorded: bool,
    },
    DestinationCommitReceiptOnly {
        package_receipt_recorded: bool,
    },
    SuppliedDurableReceipt,
}
