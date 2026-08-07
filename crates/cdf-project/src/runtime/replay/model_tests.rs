use std::{collections::BTreeMap, path::PathBuf};

use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CheckpointStatus, CheckpointStore, CommitCounts, CursorPosition,
    CursorValue, DestinationId, IdempotencyToken, PackageHash, PipelineId, Receipt, ReceiptId,
    ResourceId, SchemaHash, ScopeKey, SourcePosition, StateDelta, TargetName, VerifyClause,
    WriteDisposition,
};
use cdf_package::{PackageBuilder, PackageBuilderResources, PackageReader};
use cdf_state_sqlite::SqliteCheckpointStore;
use proptest::{
    prelude::*,
    test_runner::{
        Config as ProptestConfig, RngAlgorithm, RngSeed, TestCaseError, TestCaseResult, TestRunner,
    },
};
use tempfile::TempDir;

use super::{
    commit_or_reuse_committed_checkpoint, propose_or_reuse_exact_checkpoint,
    record_package_receipt_once, validate_destination_receipt_before_checkpoint,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReceiptAuthority {
    Primary,
    Alternate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ModelCheckpoint {
    Missing,
    Proposed,
    Committed(ReceiptAuthority),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SettlementModel {
    durable_receipt: Option<ReceiptAuthority>,
    checkpoint: ModelCheckpoint,
}

impl SettlementModel {
    fn validate(self) -> Result<(), &'static str> {
        if let ModelCheckpoint::Committed(receipt) = self.checkpoint
            && self.durable_receipt != Some(receipt)
        {
            return Err("committed checkpoint is not bound to the durable package receipt");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum SettlementAction {
    PersistPrimary,
    PersistAlternate,
    ProposeExact,
    ProposeStale,
    CommitRecorded,
    CommitOther,
    CommitTampered,
    CrashReopen,
}

impl From<u8> for SettlementAction {
    fn from(value: u8) -> Self {
        match value % 8 {
            0 => Self::PersistPrimary,
            1 => Self::PersistAlternate,
            2 => Self::ProposeExact,
            3 => Self::ProposeStale,
            4 => Self::CommitRecorded,
            5 => Self::CommitOther,
            6 => Self::CommitTampered,
            _ => Self::CrashReopen,
        }
    }
}

struct SettlementFixture {
    _temp: TempDir,
    package_dir: PathBuf,
    state_path: PathBuf,
    store: Option<SqliteCheckpointStore>,
    delta: StateDelta,
    primary: Receipt,
    alternate: Receipt,
    tampered: Receipt,
    model: SettlementModel,
}

impl SettlementFixture {
    fn new() -> Result<Self, TestCaseError> {
        let temp = TempDir::new().map_err(test_failure)?;
        let package_dir = temp.path().join("package");
        let builder = PackageBuilder::create(
            &package_dir,
            "settlement-model",
            cdf_kernel::PackageContentAuthority::rows(
                SchemaHash::new(format!("sha256:{}", "11".repeat(32))).map_err(test_failure)?,
            ),
            PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
                .map_err(test_failure)?,
        )
        .map_err(test_failure)?;
        let manifest = builder.finish().map_err(test_failure)?;
        let package_hash = PackageHash::new(manifest.package_hash).map_err(test_failure)?;
        let schema_hash =
            SchemaHash::new(format!("sha256:{}", "11".repeat(32))).map_err(test_failure)?;
        let target = TargetName::new("orders").map_err(test_failure)?;
        let disposition = WriteDisposition::Append;
        let delta = StateDelta {
            checkpoint_id: cdf_kernel::CheckpointId::new("settlement-model-checkpoint")
                .map_err(test_failure)?,
            pipeline_id: PipelineId::new("settlement-model-pipeline").map_err(test_failure)?,
            resource_id: ResourceId::new("settlement-model-resource").map_err(test_failure)?,
            scope: ScopeKey::Resource,
            state_version: CHECKPOINT_STATE_VERSION,
            parent_checkpoint_id: None,
            input_position: None,
            output_position: cursor_position(1),
            output_watermark: None,
            partition_watermarks: Vec::new(),
            late_data_carryover: Vec::new(),
            source_continuation: None,
            package_hash: package_hash.clone(),
            content: cdf_kernel::PackageContentAuthority::rows(schema_hash.clone()),
            schema_hash: schema_hash.clone(),
            segments: Vec::new(),
        };
        let primary = receipt(
            package_hash.clone(),
            schema_hash.clone(),
            target.clone(),
            disposition.clone(),
            Some(0),
        )?;
        let alternate = receipt(package_hash, schema_hash, target, disposition, None)?;
        let mut tampered = primary.clone();
        tampered.package_hash =
            PackageHash::new(format!("sha256:{}", "22".repeat(32))).map_err(test_failure)?;
        tampered.idempotency_token =
            IdempotencyToken::new(tampered.package_hash.as_str()).map_err(test_failure)?;
        let state_path = temp.path().join("state.db");
        let store = SqliteCheckpointStore::open(&state_path).map_err(test_failure)?;
        Ok(Self {
            _temp: temp,
            package_dir,
            state_path,
            store: Some(store),
            delta,
            primary,
            alternate,
            tampered,
            model: SettlementModel {
                durable_receipt: None,
                checkpoint: ModelCheckpoint::Missing,
            },
        })
    }

    fn run(mut self, actions: Vec<u8>) -> TestCaseResult {
        for action in actions.into_iter().map(SettlementAction::from) {
            self.apply(action)?;
            self.assert_matches_model()?;
        }
        self.converge()?;
        self.assert_matches_model()
    }

    fn apply(&mut self, action: SettlementAction) -> TestCaseResult {
        match action {
            SettlementAction::PersistPrimary => self.persist(ReceiptAuthority::Primary),
            SettlementAction::PersistAlternate => self.persist(ReceiptAuthority::Alternate),
            SettlementAction::ProposeExact => self.propose_exact(),
            SettlementAction::ProposeStale => self.propose_stale(),
            SettlementAction::CommitRecorded => self.commit_recorded(),
            SettlementAction::CommitOther => self.reject_unrecorded_receipt(),
            SettlementAction::CommitTampered => self.reject_tampered_receipt(),
            SettlementAction::CrashReopen => self.crash_reopen(),
        }
    }

    fn persist(&mut self, authority: ReceiptAuthority) -> TestCaseResult {
        let receipt = self.receipt(authority).clone();
        validate_destination_receipt_before_checkpoint(
            &self.delta,
            &receipt.target,
            &receipt.disposition,
            &receipt,
        )
        .map_err(test_failure)?;
        let reader = PackageReader::open(&self.package_dir).map_err(test_failure)?;
        match self.model.durable_receipt {
            None => {
                prop_assert!(record_package_receipt_once(&reader, &receipt).map_err(test_failure)?);
                self.model.durable_receipt = Some(authority);
            }
            Some(existing) if existing == authority => {
                prop_assert!(
                    !record_package_receipt_once(&reader, &receipt).map_err(test_failure)?
                );
            }
            Some(_) => {
                let error = record_package_receipt_once(&reader, &receipt)
                    .expect_err("conflicting package receipt must fail");
                prop_assert!(
                    error
                        .to_string()
                        .contains("conflicting logical commit evidence")
                );
            }
        }
        Ok(())
    }

    fn propose_exact(&mut self) -> TestCaseResult {
        let result = propose_or_reuse_exact_checkpoint(self.store(), &self.delta);
        match self.model.checkpoint {
            ModelCheckpoint::Missing | ModelCheckpoint::Proposed => {
                let checkpoint = result.map_err(test_failure)?;
                prop_assert_eq!(checkpoint.status, CheckpointStatus::Proposed);
                self.model.checkpoint = ModelCheckpoint::Proposed;
            }
            ModelCheckpoint::Committed(_) => {
                let checkpoint = result.map_err(test_failure)?;
                prop_assert_eq!(checkpoint.status, CheckpointStatus::Committed);
            }
        }
        Ok(())
    }

    fn propose_stale(&mut self) -> TestCaseResult {
        if self.model.checkpoint == ModelCheckpoint::Missing {
            self.propose_exact()?;
        }
        let mut stale = self.delta.clone();
        stale.output_position = cursor_position(2);
        let error = propose_or_reuse_exact_checkpoint(self.store(), &stale)
            .expect_err("stale proposal must not reuse checkpoint identity");
        prop_assert!(
            error
                .to_string()
                .contains("not the exact reusable proposal")
        );
        Ok(())
    }

    fn commit_recorded(&mut self) -> TestCaseResult {
        let Some(authority) = self.model.durable_receipt else {
            prop_assert!(!matches!(
                self.model.checkpoint,
                ModelCheckpoint::Committed(_)
            ));
            return Ok(());
        };
        let receipt = self.receipt(authority).clone();
        validate_destination_receipt_before_checkpoint(
            &self.delta,
            &receipt.target,
            &receipt.disposition,
            &receipt,
        )
        .map_err(test_failure)?;
        let result = commit_or_reuse_committed_checkpoint(self.store(), &self.delta, receipt);
        match self.model.checkpoint {
            ModelCheckpoint::Missing => prop_assert!(result.is_err()),
            ModelCheckpoint::Proposed | ModelCheckpoint::Committed(_) => {
                let checkpoint = result.map_err(test_failure)?;
                prop_assert_eq!(checkpoint.status, CheckpointStatus::Committed);
                self.model.checkpoint = ModelCheckpoint::Committed(authority);
            }
        }
        Ok(())
    }

    fn reject_unrecorded_receipt(&self) -> TestCaseResult {
        let Some(recorded) = self.model.durable_receipt else {
            return Ok(());
        };
        let unrecorded = match recorded {
            ReceiptAuthority::Primary => ReceiptAuthority::Alternate,
            ReceiptAuthority::Alternate => ReceiptAuthority::Primary,
        };
        let receipt = self.receipt(unrecorded);
        validate_destination_receipt_before_checkpoint(
            &self.delta,
            &receipt.target,
            &receipt.disposition,
            receipt,
        )
        .map_err(test_failure)?;
        let reader = PackageReader::open(&self.package_dir).map_err(test_failure)?;
        let error = record_package_receipt_once(&reader, receipt)
            .expect_err("unrecorded conflicting receipt must not replace durable authority");
        prop_assert!(
            error
                .to_string()
                .contains("conflicting logical commit evidence")
        );
        Ok(())
    }

    fn reject_tampered_receipt(&mut self) -> TestCaseResult {
        if self.model.checkpoint == ModelCheckpoint::Missing {
            self.propose_exact()?;
        }
        prop_assert!(
            validate_destination_receipt_before_checkpoint(
                &self.delta,
                &self.tampered.target,
                &self.tampered.disposition,
                &self.tampered,
            )
            .is_err()
        );
        prop_assert!(
            commit_or_reuse_committed_checkpoint(self.store(), &self.delta, self.tampered.clone(),)
                .is_err()
        );
        Ok(())
    }

    fn crash_reopen(&mut self) -> TestCaseResult {
        drop(self.store.take());
        self.store = Some(SqliteCheckpointStore::open(&self.state_path).map_err(test_failure)?);
        Ok(())
    }

    fn converge(&mut self) -> TestCaseResult {
        if self.model.durable_receipt.is_none() {
            self.persist(ReceiptAuthority::Primary)?;
        }
        if self.model.checkpoint == ModelCheckpoint::Missing {
            self.propose_exact()?;
        }
        self.commit_recorded()?;
        self.commit_recorded()
    }

    fn assert_matches_model(&self) -> TestCaseResult {
        self.model.validate().map_err(TestCaseError::fail)?;
        let reader = PackageReader::open(&self.package_dir).map_err(test_failure)?;
        let mut receipts = Vec::new();
        reader
            .for_each_receipt(&mut |receipt| {
                receipts.push(receipt);
                Ok(())
            })
            .map_err(test_failure)?;
        match self.model.durable_receipt {
            None => prop_assert!(receipts.is_empty()),
            Some(authority) => prop_assert_eq!(receipts, vec![self.receipt(authority).clone()]),
        }

        let history = self
            .store()
            .history(
                &self.delta.pipeline_id,
                &self.delta.resource_id,
                &self.delta.scope,
            )
            .map_err(test_failure)?;
        match self.model.checkpoint {
            ModelCheckpoint::Missing => prop_assert!(history.is_empty()),
            ModelCheckpoint::Proposed => {
                prop_assert_eq!(history.len(), 1);
                prop_assert_eq!(&history[0].status, &CheckpointStatus::Proposed);
                prop_assert!(history[0].receipt.is_none());
            }
            ModelCheckpoint::Committed(authority) => {
                prop_assert_eq!(history.len(), 1);
                prop_assert_eq!(&history[0].status, &CheckpointStatus::Committed);
                prop_assert!(history[0].is_head);
                prop_assert_eq!(history[0].receipt.as_ref(), Some(self.receipt(authority)));
            }
        }
        Ok(())
    }

    fn store(&self) -> &SqliteCheckpointStore {
        self.store.as_ref().expect("settlement store must be open")
    }

    fn receipt(&self, authority: ReceiptAuthority) -> &Receipt {
        match authority {
            ReceiptAuthority::Primary => &self.primary,
            ReceiptAuthority::Alternate => &self.alternate,
        }
    }
}

fn cursor_position(value: u64) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "id".to_owned(),
        value: CursorValue::U64(value),
    })
}

fn receipt(
    package_hash: PackageHash,
    schema_hash: SchemaHash,
    target: TargetName,
    disposition: WriteDisposition,
    rows_inserted: Option<u64>,
) -> Result<Receipt, TestCaseError> {
    Ok(Receipt {
        receipt_id: ReceiptId::new("settlement-model-receipt").map_err(test_failure)?,
        destination: DestinationId::new("settlement-model-destination").map_err(test_failure)?,
        target,
        idempotency_token: IdempotencyToken::new(package_hash.as_str()).map_err(test_failure)?,
        content: cdf_kernel::PackageContentAuthority::rows(schema_hash.clone()),
        package_hash,
        segment_acks: Vec::new(),
        disposition,
        transaction: None,
        counts: CommitCounts::rows(0, rows_inserted, Some(0), Some(0)),
        schema_hash,
        migrations: Vec::new(),
        committed_at_ms: 1_700_000_000_000,
        verify: VerifyClause {
            kind: "model".to_owned(),
            statement: "durable model receipt".to_owned(),
            parameters: BTreeMap::new(),
        },
    })
}

fn test_failure(error: impl std::fmt::Display) -> TestCaseError {
    TestCaseError::fail(error.to_string())
}

#[test]
fn model_based_receipt_gated_settlement_converges_across_recovery_sequences() {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 24,
        max_shrink_iters: 128,
        failure_persistence: None,
        rng_algorithm: RngAlgorithm::ChaCha,
        rng_seed: RngSeed::Fixed(0xcdf2_0260_7310_0002),
        ..ProptestConfig::default()
    });
    let actions = proptest::collection::vec(any::<u8>(), 1..=24);

    runner
        .run(&actions, |actions| SettlementFixture::new()?.run(actions))
        .unwrap();
}

#[test]
fn settlement_model_detects_a_faulty_unrecorded_commit() {
    let faulty = SettlementModel {
        durable_receipt: Some(ReceiptAuthority::Primary),
        checkpoint: ModelCheckpoint::Committed(ReceiptAuthority::Alternate),
    };

    assert!(faulty.validate().is_err());
}
