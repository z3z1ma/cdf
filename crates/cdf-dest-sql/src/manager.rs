use std::collections::BTreeSet;

use cdf_kernel::{CdfError, Receipt, Result};
use cdf_package_contract::QuarantineRecord;

use crate::{
    LoadMirrorKey, LoadMirrorMutation, LoadMirrorRow, MirrorCommit, MirrorInsertOutcome,
    QuarantineMirrorKey, QuarantineMirrorMutation, QuarantineMirrorRow, SegmentMirrorMutation,
    SegmentMirrorPolicy, SegmentMirrorRow, StateMirrorKey, StateMirrorMutation, StateMirrorRow,
};

/// A backend bound to the adapter's already-open payload transaction.
///
/// Implementations own SQL, parameters, native JSON values, row decoding, and transaction
/// commit/rollback. The common manager deliberately has no commit method, so mirror work cannot
/// escape the payload transaction.
pub trait TransactionalMirrorBackend {
    fn read_load(&mut self, key: &LoadMirrorKey) -> Result<Option<LoadMirrorRow>>;
    fn insert_load(
        &mut self,
        mutation: &LoadMirrorMutation,
    ) -> Result<MirrorInsertOutcome<LoadMirrorRow>>;
    fn read_state(&mut self, key: &StateMirrorKey) -> Result<Option<StateMirrorRow>>;
    fn upsert_state(
        &mut self,
        mutation: &StateMirrorMutation,
    ) -> Result<MirrorInsertOutcome<StateMirrorRow>>;
    fn insert_segment(
        &mut self,
        mutation: &SegmentMirrorMutation,
    ) -> Result<MirrorInsertOutcome<SegmentMirrorRow>>;
    fn read_mirror_segment(
        &mut self,
        mutation: &SegmentMirrorMutation,
    ) -> Result<Option<SegmentMirrorRow>>;
    fn insert_quarantine(
        &mut self,
        mutation: &QuarantineMirrorMutation,
    ) -> Result<MirrorInsertOutcome<QuarantineMirrorRow>>;
    fn read_quarantine(&mut self, key: &QuarantineMirrorKey)
    -> Result<Option<QuarantineMirrorRow>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MirrorApplyOutcome {
    Inserted,
    Duplicate,
}

pub struct TransactionalMirrorManager<'a, B> {
    backend: &'a mut B,
}

impl<'a, B: TransactionalMirrorBackend> TransactionalMirrorManager<'a, B> {
    pub fn new(backend: &'a mut B) -> Self {
        Self { backend }
    }

    pub fn find_duplicate<F>(
        &mut self,
        key: &LoadMirrorKey,
        expected_logical_receipt: F,
    ) -> Result<Option<Receipt>>
    where
        F: FnOnce(&Receipt) -> Result<Receipt>,
    {
        let stored = self
            .backend
            .read_load(key)?
            .map(|row| validate_load_row(key, row))
            .transpose()?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        let expected = expected_logical_receipt(&stored)?;
        if !logically_equivalent_receipts(&stored, &expected) {
            return Err(CdfError::destination(format!(
                "SQL mirror idempotency key records a different logical receipt ({})",
                logical_receipt_differences(&stored, &expected).join(", ")
            )));
        }
        Ok(Some(stored))
    }

    pub fn apply(&mut self, commit: MirrorCommit) -> Result<MirrorApplyOutcome> {
        self.apply_with_quarantines(commit, |_| Ok(()))
    }

    /// Applies one mirror commit while visiting quarantine records incrementally.
    ///
    /// The caller supplies the package-owned streaming reader. Records are converted to typed
    /// mutations and inserted immediately, so the common lifecycle never materializes a
    /// package-sized quarantine collection. Backend uniqueness constraints remain authoritative
    /// for duplicate quarantine identities.
    pub fn apply_with_quarantines<F>(
        &mut self,
        mut commit: MirrorCommit,
        visit_quarantines: F,
    ) -> Result<MirrorApplyOutcome>
    where
        F: FnOnce(&mut dyn FnMut(QuarantineRecord) -> Result<()>) -> Result<()>,
    {
        validate_commit(&commit)?;
        let key = commit.load.key();
        if let Some(existing) = self
            .backend
            .read_load(&key)?
            .map(|row| validate_load_row(&key, row))
            .transpose()?
        {
            if logically_equivalent_receipts(&existing, &commit.load.receipt) {
                return Ok(MirrorApplyOutcome::Duplicate);
            }
            return Err(CdfError::destination(
                "SQL mirror idempotency key already records a different receipt",
            ));
        }

        commit
            .segments
            .sort_by(|left, right| left.segment_id.cmp(&right.segment_id));

        if let Some(state) = &commit.state {
            validate_state_transition(self.backend.read_state(&state.key)?, state)?;
        }

        match self.backend.insert_load(&commit.load)? {
            MirrorInsertOutcome::Inserted(stored)
                if stored == LoadMirrorRow::from(&commit.load) => {}
            MirrorInsertOutcome::Inserted(_) => {
                return Err(CdfError::destination(
                    "SQL mirror exact receipt readback differs from the inserted receipt",
                ));
            }
            MirrorInsertOutcome::Conflict => {
                return Err(CdfError::destination(
                    "SQL mirror load insert conflicted after duplicate preflight",
                ));
            }
        }
        if let Some(state) = &commit.state {
            match self.backend.upsert_state(state)? {
                MirrorInsertOutcome::Inserted(stored) if stored == StateMirrorRow::from(state) => {}
                MirrorInsertOutcome::Inserted(_) => {
                    return Err(CdfError::destination(
                        "SQL mirror state readback differs from the requested mutation",
                    ));
                }
                MirrorInsertOutcome::Conflict => {
                    return Err(CdfError::destination(
                        "SQL mirror state rejected a concurrent non-successor",
                    ));
                }
            }
        }
        for segment in &commit.segments {
            match self.backend.insert_segment(segment)? {
                MirrorInsertOutcome::Inserted(stored)
                    if stored == SegmentMirrorRow::from(segment) => {}
                MirrorInsertOutcome::Inserted(_) => {
                    return Err(CdfError::destination(
                        "SQL mirror segment readback differs from the requested mutation",
                    ));
                }
                MirrorInsertOutcome::Conflict => {
                    return Err(CdfError::destination(
                        "SQL mirror segment identity or row range conflicts",
                    ));
                }
            }
        }
        let receipt = &commit.load.receipt;
        visit_quarantines(&mut |record| {
            let mutation = QuarantineMirrorMutation::from_record(receipt, record);
            validate_quarantine_mutation(receipt, &mutation)?;
            match self.backend.insert_quarantine(&mutation)? {
                MirrorInsertOutcome::Inserted(stored)
                    if stored == QuarantineMirrorRow::from(&mutation) => {}
                MirrorInsertOutcome::Inserted(_) => {
                    return Err(CdfError::destination(
                        "SQL mirror quarantine readback differs from the requested mutation",
                    ));
                }
                MirrorInsertOutcome::Conflict => {
                    if self.backend.read_quarantine(&mutation.key)?
                        != Some(QuarantineMirrorRow::from(&mutation))
                    {
                        return Err(CdfError::destination(
                            "SQL mirror quarantine identity records conflicting evidence",
                        ));
                    }
                }
            }
            Ok(())
        })?;

        Ok(MirrorApplyOutcome::Inserted)
    }
}

fn validate_commit(commit: &MirrorCommit) -> Result<()> {
    let receipt = &commit.load.receipt;
    if receipt.committed_at_ms < 0 {
        return Err(CdfError::data(
            "SQL mirror commit timestamp cannot be negative",
        ));
    }
    match commit.segment_policy {
        SegmentMirrorPolicy::Persist { .. } => {
            let receipt_segments = receipt
                .segment_acks
                .iter()
                .map(|ack| (&ack.segment_id, ack.row_count, ack.byte_count))
                .collect::<BTreeSet<_>>();
            let mutation_segments = commit
                .segments
                .iter()
                .map(|segment| (&segment.segment_id, segment.row_count, segment.byte_count))
                .collect::<BTreeSet<_>>();
            if receipt_segments.len() != receipt.segment_acks.len()
                || mutation_segments.len() != commit.segments.len()
                || receipt_segments != mutation_segments
            {
                return Err(CdfError::data(
                    "SQL mirror segment mutations differ from receipt acknowledgements",
                ));
            }
        }
        SegmentMirrorPolicy::Exclude if !commit.segments.is_empty() => {
            return Err(CdfError::internal(
                "SQL mirror excluded segment policy contains segment mutations",
            ));
        }
        SegmentMirrorPolicy::Exclude => {}
    }
    if commit.segments.iter().any(|segment| {
        segment.target != receipt.target
            || segment.package_hash != receipt.package_hash
            || segment.idempotency_token != receipt.idempotency_token
            || segment.committed_at_ms != receipt.committed_at_ms
    }) {
        return Err(CdfError::data(
            "SQL mirror mutation identity differs from its receipt",
        ));
    }
    Ok(())
}

fn validate_load_row(key: &LoadMirrorKey, row: LoadMirrorRow) -> Result<Receipt> {
    let receipt = row.receipt;
    if receipt.target != key.target
        || receipt.package_hash != key.package_hash
        || receipt.idempotency_token != key.idempotency_token
    {
        return Err(CdfError::data(
            "SQL mirror load readback identity differs from the requested key",
        ));
    }
    Ok(receipt)
}

fn logically_equivalent_receipts(left: &Receipt, right: &Receipt) -> bool {
    left.receipt_id == right.receipt_id
        && left.destination == right.destination
        && left.target == right.target
        && left.package_hash == right.package_hash
        && left.segment_acks == right.segment_acks
        && left.disposition == right.disposition
        && left.idempotency_token == right.idempotency_token
        && left.counts == right.counts
        && left.schema_hash == right.schema_hash
        && left.migrations == right.migrations
}

fn logical_receipt_differences(left: &Receipt, right: &Receipt) -> Vec<&'static str> {
    [
        (left.receipt_id != right.receipt_id, "receipt_id"),
        (left.destination != right.destination, "destination"),
        (left.target != right.target, "target"),
        (left.package_hash != right.package_hash, "package_hash"),
        (left.segment_acks != right.segment_acks, "segment_acks"),
        (left.disposition != right.disposition, "disposition"),
        (
            left.idempotency_token != right.idempotency_token,
            "idempotency_token",
        ),
        (left.counts != right.counts, "counts"),
        (left.schema_hash != right.schema_hash, "schema_hash"),
        (left.migrations != right.migrations, "migrations"),
    ]
    .into_iter()
    .filter_map(|(differs, field)| differs.then_some(field))
    .collect()
}

fn validate_state_transition(
    existing: Option<StateMirrorRow>,
    incoming: &StateMirrorMutation,
) -> Result<()> {
    match existing {
        None if incoming.parent_checkpoint_id.is_none() => Ok(()),
        None => Err(CdfError::data(
            "SQL mirror state parent checkpoint is absent",
        )),
        Some(existing) if existing.mutation == *incoming => Ok(()),
        Some(existing)
            if incoming.parent_checkpoint_id.as_ref() == Some(&existing.mutation.checkpoint_id) =>
        {
            Ok(())
        }
        Some(_) => Err(CdfError::data(
            "SQL mirror state update is not a successor of the committed head",
        )),
    }
}

fn validate_quarantine_mutation(
    receipt: &Receipt,
    quarantine: &QuarantineMirrorMutation,
) -> Result<()> {
    if quarantine.key.target != receipt.target
        || quarantine.key.package_hash != receipt.package_hash
        || quarantine.receipt_id != receipt.receipt_id
        || quarantine.committed_at_ms != receipt.committed_at_ms
    {
        return Err(CdfError::data(
            "SQL mirror quarantine mutation identity differs from its receipt",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cdf_kernel::{
        CheckpointId, CommitCounts, CursorPosition, CursorValue, IdempotencyToken, PackageHash,
        PipelineId, ReceiptId, ResourceId, SchemaHash, ScopeKey, SegmentAck, SegmentId, StateDelta,
        StateSegment, TargetName, TransactionMetadata, VerifyClause, WriteDisposition,
    };

    use super::*;

    #[derive(Default)]
    struct MemoryBackend {
        load: Option<Receipt>,
        state: Option<StateMirrorMutation>,
        segments: BTreeMap<SegmentId, SegmentMirrorMutation>,
        quarantines: BTreeMap<QuarantineMirrorKey, QuarantineMirrorMutation>,
        calls: Vec<String>,
        fail_on: Option<&'static str>,
        inserted_load_override: Option<Receipt>,
    }

    impl TransactionalMirrorBackend for MemoryBackend {
        fn read_load(&mut self, _key: &LoadMirrorKey) -> Result<Option<LoadMirrorRow>> {
            self.calls.push("read_load".to_owned());
            Ok(self.load.clone().map(|receipt| LoadMirrorRow { receipt }))
        }

        fn insert_load(
            &mut self,
            mutation: &LoadMirrorMutation,
        ) -> Result<MirrorInsertOutcome<LoadMirrorRow>> {
            self.calls.push("load".to_owned());
            fail_if(self.fail_on, "load")?;
            let receipt = self
                .inserted_load_override
                .clone()
                .unwrap_or_else(|| mutation.receipt.clone());
            self.load = Some(receipt.clone());
            Ok(MirrorInsertOutcome::Inserted(LoadMirrorRow { receipt }))
        }

        fn read_state(&mut self, _key: &StateMirrorKey) -> Result<Option<StateMirrorRow>> {
            self.calls.push("read_state".to_owned());
            Ok(self.state.as_ref().map(StateMirrorRow::from))
        }

        fn upsert_state(
            &mut self,
            mutation: &StateMirrorMutation,
        ) -> Result<MirrorInsertOutcome<StateMirrorRow>> {
            self.calls.push("state".to_owned());
            fail_if(self.fail_on, "state")?;
            self.state = Some(mutation.clone());
            Ok(MirrorInsertOutcome::Inserted(StateMirrorRow::from(
                mutation,
            )))
        }

        fn insert_segment(
            &mut self,
            mutation: &SegmentMirrorMutation,
        ) -> Result<MirrorInsertOutcome<SegmentMirrorRow>> {
            self.calls
                .push(format!("segment:{}", mutation.segment_id.as_str()));
            fail_if(self.fail_on, "segment")?;
            self.segments
                .insert(mutation.segment_id.clone(), mutation.clone());
            Ok(MirrorInsertOutcome::Inserted(SegmentMirrorRow::from(
                mutation,
            )))
        }

        fn read_mirror_segment(
            &mut self,
            mutation: &SegmentMirrorMutation,
        ) -> Result<Option<SegmentMirrorRow>> {
            self.calls
                .push(format!("read_segment:{}", mutation.segment_id.as_str()));
            Ok(self
                .segments
                .get(&mutation.segment_id)
                .map(SegmentMirrorRow::from))
        }

        fn insert_quarantine(
            &mut self,
            mutation: &QuarantineMirrorMutation,
        ) -> Result<MirrorInsertOutcome<QuarantineMirrorRow>> {
            self.calls
                .push(format!("quarantine:{}", mutation.key.source_row_ordinal));
            fail_if(self.fail_on, "quarantine")?;
            if self.quarantines.contains_key(&mutation.key) {
                Ok(MirrorInsertOutcome::Conflict)
            } else {
                self.quarantines
                    .insert(mutation.key.clone(), mutation.clone());
                Ok(MirrorInsertOutcome::Inserted(QuarantineMirrorRow::from(
                    mutation,
                )))
            }
        }

        fn read_quarantine(
            &mut self,
            key: &QuarantineMirrorKey,
        ) -> Result<Option<QuarantineMirrorRow>> {
            self.calls
                .push(format!("read_quarantine:{}", key.source_row_ordinal));
            Ok(self.quarantines.get(key).map(QuarantineMirrorRow::from))
        }
    }

    fn fail_if(configured: Option<&str>, current: &str) -> Result<()> {
        if configured == Some(current) {
            Err(CdfError::destination(format!(
                "injected {current} mirror failure"
            )))
        } else {
            Ok(())
        }
    }

    fn receipt() -> Receipt {
        Receipt {
            receipt_id: ReceiptId::new("receipt-1").unwrap(),
            destination: cdf_kernel::DestinationId::new("test").unwrap(),
            target: TargetName::new("orders").unwrap(),
            package_hash: PackageHash::new("package-1").unwrap(),
            content: cdf_kernel::PackageContentAuthority::rows(
                SchemaHash::new("schema-1").unwrap(),
            ),
            segment_acks: vec![
                SegmentAck {
                    kind: cdf_kernel::PackageSegmentKind::Row,
                    segment_id: SegmentId::new("z-segment").unwrap(),
                    row_count: 1,
                    byte_count: 10,
                },
                SegmentAck {
                    kind: cdf_kernel::PackageSegmentKind::Row,
                    segment_id: SegmentId::new("a-segment").unwrap(),
                    row_count: 1,
                    byte_count: 20,
                },
            ],
            disposition: WriteDisposition::Append,
            idempotency_token: IdempotencyToken::new("token-1").unwrap(),
            transaction: Some(TransactionMetadata {
                system: "test".to_owned(),
                values: BTreeMap::new(),
            }),
            counts: CommitCounts {
                rows_written: 2,
                rows_inserted: Some(2),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: SchemaHash::new("schema-1").unwrap(),
            migrations: Vec::new(),
            committed_at_ms: 100,
            verify: VerifyClause {
                kind: "test".to_owned(),
                statement: "verify".to_owned(),
                parameters: BTreeMap::new(),
            },
        }
    }

    fn delta(receipt: &Receipt) -> StateDelta {
        StateDelta {
            checkpoint_id: CheckpointId::new("checkpoint-1").unwrap(),
            pipeline_id: PipelineId::new("pipeline-1").unwrap(),
            resource_id: ResourceId::new("resource-1").unwrap(),
            scope: ScopeKey::Resource,
            state_version: cdf_kernel::CHECKPOINT_STATE_VERSION,
            parent_checkpoint_id: None,
            input_position: None,
            output_position: cdf_kernel::SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "offset".to_owned(),
                value: CursorValue::U64(2),
            }),
            output_watermark: None,
            partition_watermarks: Vec::new(),
            late_data_carryover: Vec::new(),
            source_continuation: None,
            package_hash: receipt.package_hash.clone(),
            content: receipt.content.clone(),
            schema_hash: receipt.schema_hash.clone(),
            segments: vec![
                StateSegment {
                    kind: cdf_kernel::PackageSegmentKind::Row,
                    segment_id: SegmentId::new("z-segment").unwrap(),
                    scope: ScopeKey::Resource,
                    output_position: cdf_kernel::SourcePosition::Cursor(CursorPosition {
                        version: cdf_kernel::SOURCE_POSITION_VERSION,
                        field: "offset".to_owned(),
                        value: CursorValue::U64(1),
                    }),
                    row_count: 1,
                    byte_count: 10,
                },
                StateSegment {
                    kind: cdf_kernel::PackageSegmentKind::Row,
                    segment_id: SegmentId::new("a-segment").unwrap(),
                    scope: ScopeKey::Resource,
                    output_position: cdf_kernel::SourcePosition::Cursor(CursorPosition {
                        version: cdf_kernel::SOURCE_POSITION_VERSION,
                        field: "offset".to_owned(),
                        value: CursorValue::U64(2),
                    }),
                    row_count: 1,
                    byte_count: 20,
                },
            ],
        }
    }

    fn commit() -> MirrorCommit {
        let receipt = receipt();
        let delta = delta(&receipt);
        MirrorCommit::new(
            receipt,
            Some(delta.resource_id.clone()),
            Some(&delta),
            &delta.segments,
            Vec::new(),
            SegmentMirrorPolicy::Persist {
                require_row_ranges: false,
            },
        )
        .unwrap()
    }

    #[test]
    fn manager_orders_typed_mutations_and_reads_receipt_back() {
        let mut backend = MemoryBackend::default();
        assert_eq!(
            TransactionalMirrorManager::new(&mut backend)
                .apply(commit())
                .unwrap(),
            MirrorApplyOutcome::Inserted
        );
        assert_eq!(
            backend.calls,
            vec![
                "read_load",
                "read_state",
                "load",
                "state",
                "segment:a-segment",
                "segment:z-segment",
            ]
        );
    }

    #[test]
    fn exact_duplicate_is_a_noop_but_conflicting_receipt_fails() {
        let commit = commit();
        let mut physically_distinct = commit.load.receipt.clone();
        physically_distinct.committed_at_ms += 1;
        physically_distinct.verify.statement = "other physical verifier".to_owned();
        let mut backend = MemoryBackend {
            load: Some(physically_distinct),
            ..MemoryBackend::default()
        };
        assert_eq!(
            TransactionalMirrorManager::new(&mut backend)
                .apply(commit.clone())
                .unwrap(),
            MirrorApplyOutcome::Duplicate
        );
        assert_eq!(backend.calls, vec!["read_load"]);

        backend.calls.clear();
        backend.load.as_mut().unwrap().schema_hash = SchemaHash::new("other").unwrap();
        assert!(
            TransactionalMirrorManager::new(&mut backend)
                .apply(commit)
                .is_err()
        );
        assert_eq!(backend.calls, vec!["read_load"]);
    }

    #[test]
    fn duplicate_readback_must_match_the_complete_typed_key() {
        let commit = commit();
        let key = commit.load.key();
        let mut wrong = commit.load.receipt;
        wrong.package_hash = PackageHash::new("other-package").unwrap();
        let mut backend = MemoryBackend {
            load: Some(wrong),
            ..MemoryBackend::default()
        };
        assert!(
            TransactionalMirrorManager::new(&mut backend)
                .find_duplicate(&key, |_| unreachable!("invalid key must fail first"))
                .unwrap_err()
                .to_string()
                .contains("requested key")
        );
    }

    #[test]
    fn duplicate_probe_requires_the_expected_logical_receipt() {
        let commit = commit();
        let key = commit.load.key();
        let mut backend = MemoryBackend {
            load: Some(commit.load.receipt.clone()),
            ..MemoryBackend::default()
        };
        assert!(
            TransactionalMirrorManager::new(&mut backend)
                .find_duplicate(&key, |stored| {
                    let mut expected = stored.clone();
                    expected.counts.rows_written += 1;
                    Ok(expected)
                })
                .unwrap_err()
                .to_string()
                .contains("different logical receipt")
        );
    }

    #[test]
    fn newly_inserted_receipt_requires_exact_physical_readback() {
        let commit = commit();
        let mut mismatched = commit.load.receipt.clone();
        mismatched.committed_at_ms += 1;
        let mut backend = MemoryBackend {
            inserted_load_override: Some(mismatched),
            ..MemoryBackend::default()
        };
        assert!(
            TransactionalMirrorManager::new(&mut backend)
                .apply(commit)
                .unwrap_err()
                .to_string()
                .contains("exact receipt readback")
        );
    }

    #[test]
    fn stale_state_fails_before_backend_mutation() {
        let mut backend = MemoryBackend {
            state: Some({
                let mut state = commit().state.unwrap();
                state.checkpoint_id = CheckpointId::new("other-head").unwrap();
                state
            }),
            ..MemoryBackend::default()
        };
        assert!(
            TransactionalMirrorManager::new(&mut backend)
                .apply(commit())
                .unwrap_err()
                .to_string()
                .contains("successor")
        );
        assert_eq!(backend.calls, vec!["read_load", "read_state"]);
    }

    #[test]
    fn state_successorship_uses_checkpoint_lineage_not_wall_clock_time() {
        let mut previous = commit().state.unwrap();
        previous.committed_at_ms = 1_000;
        let mut successor = previous.clone();
        successor.parent_checkpoint_id = Some(previous.checkpoint_id.clone());
        successor.checkpoint_id = CheckpointId::new("checkpoint-2").unwrap();
        successor.committed_at_ms = 1;
        assert!(
            validate_state_transition(Some(StateMirrorRow { mutation: previous }), &successor)
                .is_ok()
        );

        let mut branch = successor.clone();
        branch.parent_checkpoint_id = Some(CheckpointId::new("other-parent").unwrap());
        branch.committed_at_ms = 2_000;
        assert!(
            validate_state_transition(
                Some(StateMirrorRow {
                    mutation: successor
                }),
                &branch
            )
            .is_err()
        );
    }

    #[test]
    fn quarantine_records_stream_through_the_shared_sequence() {
        let mut backend = MemoryBackend::default();
        let records = [7, 3].map(
            |source_row_ordinal| cdf_package_contract::QuarantineRecord {
                source_row_ordinal,
                rule_id: "rule".to_owned(),
                error_code: "bad".to_owned(),
                source_position: None,
                observed_value_redacted: cdf_package_contract::QuarantineObservedValue::Omitted,
            },
        );
        assert_eq!(
            TransactionalMirrorManager::new(&mut backend)
                .apply_with_quarantines(commit(), |visitor| {
                    records.into_iter().try_for_each(visitor)
                })
                .unwrap(),
            MirrorApplyOutcome::Inserted
        );
        assert_eq!(
            backend.calls,
            vec![
                "read_load",
                "read_state",
                "load",
                "state",
                "segment:a-segment",
                "segment:z-segment",
                "quarantine:7",
                "quarantine:3",
            ]
        );
    }

    #[test]
    fn conflicting_quarantine_evidence_fails_during_streaming_readback() {
        let mut backend = MemoryBackend::default();
        let mut records = [
            cdf_package_contract::QuarantineObservedValue::Omitted,
            cdf_package_contract::QuarantineObservedValue::Masked {
                value: "masked".to_owned(),
            },
        ]
        .into_iter()
        .map(
            |observed_value_redacted| cdf_package_contract::QuarantineRecord {
                source_row_ordinal: 7,
                rule_id: "rule".to_owned(),
                error_code: "bad".to_owned(),
                source_position: None,
                observed_value_redacted,
            },
        );
        assert!(
            TransactionalMirrorManager::new(&mut backend)
                .apply_with_quarantines(commit(), |visitor| records.try_for_each(visitor))
                .unwrap_err()
                .to_string()
                .contains("quarantine identity records conflicting evidence")
        );
    }

    #[test]
    fn injected_failure_stops_the_shared_sequence_before_readback() {
        let mut backend = MemoryBackend {
            fail_on: Some("segment"),
            ..MemoryBackend::default()
        };
        assert!(
            TransactionalMirrorManager::new(&mut backend)
                .apply(commit())
                .is_err()
        );
        assert_eq!(
            backend.calls,
            vec![
                "read_load",
                "read_state",
                "load",
                "state",
                "segment:a-segment",
            ]
        );
    }

    #[test]
    fn typed_segment_state_and_range_drift_fail_before_backend_contact() {
        let base_receipt = receipt();
        let mut delta = delta(&base_receipt);
        delta.segments[0].byte_count += 1;
        assert!(
            MirrorCommit::new(
                base_receipt.clone(),
                Some(delta.resource_id.clone()),
                None,
                &delta.segments,
                Vec::new(),
                SegmentMirrorPolicy::Persist {
                    require_row_ranges: false,
                },
            )
            .unwrap_err()
            .to_string()
            .contains("state counts")
        );

        let range = crate::SegmentRowRange {
            segment_id: SegmentId::new("z-segment").unwrap(),
            row_key_start: 10,
            row_key_end: 11,
        };
        let duplicate_id = crate::SegmentRowRange {
            row_key_start: 20,
            row_key_end: 21,
            ..range.clone()
        };
        assert!(
            MirrorCommit::new(
                base_receipt,
                None,
                None,
                &[],
                vec![range, duplicate_id],
                SegmentMirrorPolicy::Persist {
                    require_row_ranges: false,
                },
            )
            .unwrap_err()
            .to_string()
            .contains("duplicate segment")
        );

        let receipt = receipt();
        let overlapping = vec![
            crate::SegmentRowRange {
                segment_id: SegmentId::new("z-segment").unwrap(),
                row_key_start: 10,
                row_key_end: 11,
            },
            crate::SegmentRowRange {
                segment_id: SegmentId::new("a-segment").unwrap(),
                row_key_start: 10,
                row_key_end: 11,
            },
        ];
        assert!(
            MirrorCommit::new(
                receipt,
                None,
                None,
                &[],
                overlapping,
                SegmentMirrorPolicy::Persist {
                    require_row_ranges: true,
                },
            )
            .unwrap_err()
            .to_string()
            .contains("overlap")
        );
    }
}
