mod fixtures;

use firn_kernel::{CheckpointId, CheckpointStatus, CheckpointStore, PackageHash, RewindRequest};

use self::fixtures::{
    assert_plausible_created_at, commit_delta, cursor_position, delta, delta_for,
    other_partition_scope, other_resource_id, partition_scope, pipeline_id, receipt, resource_id,
};

pub fn assert_checkpoint_store_send_sync<S: CheckpointStore + Send + Sync>() {}

pub fn assert_checkpoint_store_conformance<S, F>(mut fresh_store: F)
where
    S: CheckpointStore,
    F: FnMut() -> S,
{
    assert_checkpoint_store_send_sync::<S>();
    assert_commit_requires_receipt_covering_delta(&fresh_store());
    assert_proposed_and_abandoned_checkpoints_never_become_heads(&fresh_store());
    assert_committed_head_history_ordering_and_tuple_isolation(&fresh_store());
    assert_rewind_rejects_invalid_targets(&fresh_store());
    assert_rewind_appends_marker_moves_head_and_reports_current_branch(&fresh_store());
}

fn assert_commit_requires_receipt_covering_delta<S: CheckpointStore>(store: &S) {
    let delta = delta(
        "checkpoint-bad-receipt",
        None,
        partition_scope(),
        cursor_position(1),
        "package-sha256",
    );
    let checkpoint_id = delta.checkpoint_id.clone();
    let proposed = store.propose(delta.clone()).unwrap();
    assert_eq!(proposed.status, CheckpointStatus::Proposed);
    assert!(!proposed.is_head);
    assert_plausible_created_at(&proposed);

    let mut wrong_package = receipt(&delta);
    wrong_package.package_hash = PackageHash::new("other-package-sha256").unwrap();
    assert!(store.commit(&checkpoint_id, wrong_package).is_err());

    let mut wrong_schema = receipt(&delta);
    wrong_schema.schema_hash = firn_kernel::SchemaHash::new("other-schema-sha256").unwrap();
    assert!(store.commit(&checkpoint_id, wrong_schema).is_err());

    let mut missing_segment = receipt(&delta);
    missing_segment.segment_acks.pop();
    assert!(store.commit(&checkpoint_id, missing_segment).is_err());

    let mut wrong_row_count = receipt(&delta);
    wrong_row_count.segment_acks[0].row_count += 1;
    assert!(store.commit(&checkpoint_id, wrong_row_count).is_err());

    let mut low_row_count = receipt(&delta);
    low_row_count.segment_acks[1].row_count -= 1;
    assert!(store.commit(&checkpoint_id, low_row_count).is_err());

    let mut wrong_byte_count = receipt(&delta);
    wrong_byte_count.segment_acks[1].byte_count += 1;
    assert!(store.commit(&checkpoint_id, wrong_byte_count).is_err());

    let mut low_byte_count = receipt(&delta);
    low_byte_count.segment_acks[0].byte_count -= 1;
    assert!(store.commit(&checkpoint_id, low_byte_count).is_err());

    assert!(
        store
            .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_none()
    );

    let committed = store.commit(&checkpoint_id, receipt(&delta)).unwrap();
    assert_eq!(committed.status, CheckpointStatus::Committed);
    assert!(committed.is_head);
    assert_eq!(
        store
            .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        checkpoint_id
    );
}

fn assert_proposed_and_abandoned_checkpoints_never_become_heads<S: CheckpointStore>(store: &S) {
    let proposed_delta = delta(
        "checkpoint-proposed-only",
        None,
        partition_scope(),
        cursor_position(1),
        "package-proposed-only",
    );
    let proposed = store.propose(proposed_delta.clone()).unwrap();
    assert_eq!(proposed.status, CheckpointStatus::Proposed);
    assert!(!proposed.is_head);
    assert!(
        store
            .head(
                &proposed_delta.pipeline_id,
                &proposed_delta.resource_id,
                &proposed_delta.scope,
            )
            .unwrap()
            .is_none()
    );

    let abandoned_delta = delta(
        "checkpoint-abandon",
        None,
        other_partition_scope(),
        cursor_position(2),
        "package-abandon",
    );
    let abandoned_id = abandoned_delta.checkpoint_id.clone();
    let proposed = store.propose(abandoned_delta.clone()).unwrap();
    assert_eq!(proposed.status, CheckpointStatus::Proposed);
    assert!(!proposed.is_head);

    let abandoned = store.abandon(&abandoned_id).unwrap();
    assert_eq!(abandoned.status, CheckpointStatus::Abandoned);
    assert!(!abandoned.is_head);
    assert_eq!(abandoned.committed_at_ms, None);
    assert!(
        store
            .commit(&abandoned_id, receipt(&abandoned_delta))
            .is_err()
    );
    assert!(
        store
            .head(
                &abandoned_delta.pipeline_id,
                &abandoned_delta.resource_id,
                &abandoned_delta.scope,
            )
            .unwrap()
            .is_none()
    );

    assert_eq!(
        store
            .history(
                &abandoned_delta.pipeline_id,
                &abandoned_delta.resource_id,
                &abandoned_delta.scope,
            )
            .unwrap(),
        vec![abandoned]
    );
}

fn assert_committed_head_history_ordering_and_tuple_isolation<S: CheckpointStore>(store: &S) {
    let scope = partition_scope();
    let other_scope = other_partition_scope();
    let first = commit_delta(
        store,
        delta(
            "checkpoint-history-1",
            None,
            scope.clone(),
            cursor_position(1),
            "package-history-1",
        ),
    );
    let second = commit_delta(
        store,
        delta(
            "checkpoint-history-2",
            Some(&first.delta.checkpoint_id),
            scope.clone(),
            cursor_position(2),
            "package-history-2",
        ),
    );
    let other_resource = commit_delta(
        store,
        delta_for(
            "checkpoint-isolation-resource",
            None,
            pipeline_id(),
            other_resource_id(),
            scope.clone(),
            cursor_position(3),
            "package-isolation-resource",
        ),
    );
    let other_scope_checkpoint = commit_delta(
        store,
        delta_for(
            "checkpoint-isolation-scope",
            None,
            pipeline_id(),
            resource_id(),
            other_scope.clone(),
            cursor_position(4),
            "package-isolation-scope",
        ),
    );

    assert_eq!(
        store
            .head(&pipeline_id(), &resource_id(), &scope)
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        second.delta.checkpoint_id
    );
    assert_eq!(
        store
            .head(&pipeline_id(), &other_resource_id(), &scope)
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        other_resource.delta.checkpoint_id
    );
    assert_eq!(
        store
            .head(&pipeline_id(), &resource_id(), &other_scope)
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        other_scope_checkpoint.delta.checkpoint_id
    );
    assert!(
        store
            .head(&pipeline_id(), &other_resource_id(), &other_scope)
            .unwrap()
            .is_none()
    );

    let main_history = store
        .history(&pipeline_id(), &resource_id(), &scope)
        .unwrap();
    assert_eq!(
        checkpoint_ids(&main_history),
        vec!["checkpoint-history-1", "checkpoint-history-2"]
    );
    assert!(
        !main_history[0].is_head,
        "older committed checkpoint is retained as non-head history"
    );
    assert!(main_history[1].is_head);

    assert_eq!(
        store
            .history(&pipeline_id(), &other_resource_id(), &scope)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .history(&pipeline_id(), &resource_id(), &other_scope)
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .history(&pipeline_id(), &other_resource_id(), &other_scope)
            .unwrap()
            .is_empty()
    );
}

fn assert_rewind_rejects_invalid_targets<S: CheckpointStore>(store: &S) {
    let scope = partition_scope();
    let committed = commit_delta(
        store,
        delta(
            "checkpoint-rewind-validation-head",
            None,
            scope.clone(),
            cursor_position(1),
            "package-rewind-validation-head",
        ),
    );
    let proposed_delta = delta(
        "checkpoint-rewind-validation-proposed",
        Some(&committed.delta.checkpoint_id),
        scope.clone(),
        cursor_position(2),
        "package-rewind-validation-proposed",
    );
    let proposed_id = proposed_delta.checkpoint_id.clone();
    store.propose(proposed_delta).unwrap();
    let other_resource = commit_delta(
        store,
        delta_for(
            "checkpoint-rewind-validation-resource",
            None,
            pipeline_id(),
            other_resource_id(),
            scope.clone(),
            cursor_position(3),
            "package-rewind-validation-resource",
        ),
    );
    let other_scope_checkpoint = commit_delta(
        store,
        delta_for(
            "checkpoint-rewind-validation-scope",
            None,
            pipeline_id(),
            resource_id(),
            other_partition_scope(),
            cursor_position(4),
            "package-rewind-validation-scope",
        ),
    );

    assert!(
        store
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-to-proposed").unwrap(),
                pipeline_id: pipeline_id(),
                resource_id: resource_id(),
                scope: scope.clone(),
                target_checkpoint_id: proposed_id,
            })
            .is_err()
    );
    assert!(
        store
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-to-missing").unwrap(),
                pipeline_id: pipeline_id(),
                resource_id: resource_id(),
                scope: scope.clone(),
                target_checkpoint_id: CheckpointId::new("checkpoint-missing").unwrap(),
            })
            .is_err()
    );
    assert!(
        store
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-wrong-resource").unwrap(),
                pipeline_id: pipeline_id(),
                resource_id: resource_id(),
                scope: scope.clone(),
                target_checkpoint_id: other_resource.delta.checkpoint_id,
            })
            .is_err()
    );
    assert!(
        store
            .rewind(RewindRequest {
                marker_checkpoint_id: CheckpointId::new("rewind-wrong-scope").unwrap(),
                pipeline_id: pipeline_id(),
                resource_id: resource_id(),
                scope: scope.clone(),
                target_checkpoint_id: other_scope_checkpoint.delta.checkpoint_id,
            })
            .is_err()
    );

    assert_eq!(
        store
            .head(&pipeline_id(), &resource_id(), &scope)
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        committed.delta.checkpoint_id
    );
    assert_eq!(
        checkpoint_ids(
            &store
                .history(&pipeline_id(), &resource_id(), &scope)
                .unwrap()
        ),
        vec![
            "checkpoint-rewind-validation-head",
            "checkpoint-rewind-validation-proposed"
        ]
    );
}

fn assert_rewind_appends_marker_moves_head_and_reports_current_branch<S: CheckpointStore>(
    store: &S,
) {
    let scope = partition_scope();
    let base = commit_delta(
        store,
        delta(
            "checkpoint-branch-base",
            None,
            scope.clone(),
            cursor_position(1),
            "package-branch-base",
        ),
    );
    let target_branch = commit_delta(
        store,
        delta(
            "checkpoint-branch-target",
            Some(&base.delta.checkpoint_id),
            scope.clone(),
            cursor_position(2),
            "package-branch-target",
        ),
    );
    let current_branch_parent = commit_delta(
        store,
        delta(
            "checkpoint-branch-current-parent",
            Some(&base.delta.checkpoint_id),
            scope.clone(),
            cursor_position(3),
            "package-branch-current-parent",
        ),
    );
    let current_branch_head = commit_delta(
        store,
        delta(
            "checkpoint-branch-current-head",
            Some(&current_branch_parent.delta.checkpoint_id),
            scope.clone(),
            cursor_position(4),
            "package-branch-current-head",
        ),
    );

    let report = store
        .rewind(RewindRequest {
            marker_checkpoint_id: CheckpointId::new("rewind-branch-marker").unwrap(),
            pipeline_id: pipeline_id(),
            resource_id: resource_id(),
            scope: scope.clone(),
            target_checkpoint_id: target_branch.delta.checkpoint_id.clone(),
        })
        .unwrap();

    assert_eq!(report.marker.status, CheckpointStatus::Rewound);
    assert!(!report.marker.is_head);
    assert_eq!(report.marker.receipt, None);
    assert_eq!(report.marker.committed_at_ms, None);
    assert_eq!(
        report.marker.delta.parent_checkpoint_id,
        Some(current_branch_head.delta.checkpoint_id.clone())
    );
    assert_eq!(
        report.marker.delta.input_position,
        Some(current_branch_head.delta.output_position.clone())
    );
    assert_eq!(
        report.marker.rewind_target_checkpoint_id,
        Some(target_branch.delta.checkpoint_id.clone())
    );
    assert_eq!(
        report.head.delta.checkpoint_id,
        target_branch.delta.checkpoint_id
    );
    assert_eq!(
        report.packages_ahead,
        vec![
            current_branch_head.delta.package_hash.clone(),
            current_branch_parent.delta.package_hash.clone()
        ]
    );
    assert!(!report.packages_ahead.contains(&base.delta.package_hash));
    assert!(
        !report
            .packages_ahead
            .contains(&target_branch.delta.package_hash)
    );

    let history = store
        .history(&pipeline_id(), &resource_id(), &scope)
        .unwrap();
    assert_eq!(
        checkpoint_ids(&history),
        vec![
            "checkpoint-branch-base",
            "checkpoint-branch-target",
            "checkpoint-branch-current-parent",
            "checkpoint-branch-current-head",
            "rewind-branch-marker"
        ]
    );
    assert_eq!(history.len(), 5);
    assert!(
        history
            .iter()
            .any(|checkpoint| checkpoint.delta.checkpoint_id
                == current_branch_head.delta.checkpoint_id
                && checkpoint.status == CheckpointStatus::Committed
                && !checkpoint.is_head),
        "rewind keeps later committed checkpoints as non-head history"
    );
    assert_eq!(
        store
            .head(&pipeline_id(), &resource_id(), &scope)
            .unwrap()
            .unwrap()
            .delta
            .checkpoint_id,
        target_branch.delta.checkpoint_id
    );
}

fn checkpoint_ids(checkpoints: &[firn_kernel::Checkpoint]) -> Vec<&str> {
    checkpoints
        .iter()
        .map(|checkpoint| checkpoint.delta.checkpoint_id.as_str())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{Mutex, MutexGuard},
    };

    use firn_kernel::{
        Checkpoint, CheckpointId, FirnError, PipelineId, Receipt, ResourceId, Result, RewindReport,
        ScopeKey, StateDelta,
    };

    use super::*;

    #[test]
    fn conformance_runner_is_not_a_noop() {
        assert_panics(|| assert_checkpoint_store_conformance(|| PanicStore));
    }

    #[test]
    fn overreported_receipt_row_count_check_is_exercised() {
        assert_panics(|| {
            assert_commit_requires_receipt_covering_delta(&FaultyStore::new(
                Fault::AcceptOverreportedRowCount,
            ));
        });
    }

    #[test]
    fn underreported_receipt_row_count_check_is_exercised() {
        assert_panics(|| {
            assert_commit_requires_receipt_covering_delta(&FaultyStore::new(
                Fault::AcceptUnderreportedRowCount,
            ));
        });
    }

    #[test]
    fn overreported_receipt_byte_count_check_is_exercised() {
        assert_panics(|| {
            assert_commit_requires_receipt_covering_delta(&FaultyStore::new(
                Fault::AcceptOverreportedByteCount,
            ));
        });
    }

    #[test]
    fn underreported_receipt_byte_count_check_is_exercised() {
        assert_panics(|| {
            assert_commit_requires_receipt_covering_delta(&FaultyStore::new(
                Fault::AcceptUnderreportedByteCount,
            ));
        });
    }

    #[test]
    fn proposed_head_check_is_exercised() {
        assert_panics(|| {
            assert_proposed_and_abandoned_checkpoints_never_become_heads(&FaultyStore::new(
                Fault::ProposedIsHead,
            ));
        });
    }

    #[test]
    fn committed_head_and_history_checks_are_exercised() {
        assert_panics(|| {
            assert_committed_head_history_ordering_and_tuple_isolation(&FaultyStore::new(
                Fault::NoHeadLookup,
            ));
        });
    }

    #[test]
    fn invalid_rewind_checks_are_exercised() {
        assert_panics(|| {
            assert_rewind_rejects_invalid_targets(&FaultyStore::new(Fault::InvalidRewindSucceeds));
        });
    }

    #[test]
    fn valid_rewind_report_checks_are_exercised() {
        assert_panics(|| {
            assert_rewind_appends_marker_moves_head_and_reports_current_branch(&FaultyStore::new(
                Fault::ValidRewindReturnsWrongReport,
            ));
        });
    }

    #[test]
    fn timestamp_plausibility_check_is_exercised() {
        let mut checkpoint = checkpoint_for(
            delta(
                "checkpoint-zero-timestamp",
                None,
                partition_scope(),
                cursor_position(1),
                "package-zero-timestamp",
            ),
            CheckpointStatus::Proposed,
        );
        checkpoint.created_at_ms = 0;
        assert_panics(|| assert_plausible_created_at(&checkpoint));
    }

    fn assert_panics(f: impl FnOnce()) {
        assert!(
            catch_unwind(AssertUnwindSafe(f)).is_err(),
            "conformance self-test store should violate the harness"
        );
    }

    struct PanicStore;

    impl CheckpointStore for PanicStore {
        fn propose(&self, _delta: StateDelta) -> Result<Checkpoint> {
            panic!("conformance runner must call store operations")
        }

        fn commit(&self, _checkpoint_id: &CheckpointId, _receipt: Receipt) -> Result<Checkpoint> {
            panic!("conformance runner must call store operations")
        }

        fn abandon(&self, _checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
            panic!("conformance runner must call store operations")
        }

        fn head(
            &self,
            _pipeline_id: &PipelineId,
            _resource_id: &ResourceId,
            _scope: &ScopeKey,
        ) -> Result<Option<Checkpoint>> {
            panic!("conformance runner must call store operations")
        }

        fn history(
            &self,
            _pipeline_id: &PipelineId,
            _resource_id: &ResourceId,
            _scope: &ScopeKey,
        ) -> Result<Vec<Checkpoint>> {
            panic!("conformance runner must call store operations")
        }

        fn rewind(&self, _request: RewindRequest) -> Result<RewindReport> {
            panic!("conformance runner must call store operations")
        }
    }

    #[derive(Clone, Copy)]
    enum Fault {
        AcceptOverreportedRowCount,
        AcceptUnderreportedRowCount,
        AcceptOverreportedByteCount,
        AcceptUnderreportedByteCount,
        ProposedIsHead,
        NoHeadLookup,
        InvalidRewindSucceeds,
        ValidRewindReturnsWrongReport,
    }

    struct FaultyStore {
        fault: Fault,
        state: Mutex<FaultyState>,
    }

    #[derive(Default)]
    struct FaultyState {
        checkpoints: BTreeMap<CheckpointId, Checkpoint>,
        order: Vec<CheckpointId>,
    }

    impl FaultyStore {
        fn new(fault: Fault) -> Self {
            Self {
                fault,
                state: Mutex::new(FaultyState::default()),
            }
        }

        fn lock_state(&self) -> MutexGuard<'_, FaultyState> {
            self.state.lock().unwrap()
        }
    }

    impl CheckpointStore for FaultyStore {
        fn propose(&self, delta: StateDelta) -> Result<Checkpoint> {
            let mut checkpoint = checkpoint_for(delta, CheckpointStatus::Proposed);
            if matches!(self.fault, Fault::ProposedIsHead) {
                checkpoint.is_head = true;
            }
            let mut state = self.lock_state();
            state.order.push(checkpoint.delta.checkpoint_id.clone());
            state
                .checkpoints
                .insert(checkpoint.delta.checkpoint_id.clone(), checkpoint.clone());
            Ok(checkpoint)
        }

        fn commit(&self, checkpoint_id: &CheckpointId, receipt: Receipt) -> Result<Checkpoint> {
            let mut state = self.lock_state();
            let checkpoint = state
                .checkpoints
                .get(checkpoint_id)
                .cloned()
                .ok_or_else(|| FirnError::contract("missing checkpoint"))?;
            if checkpoint.status != CheckpointStatus::Proposed {
                return Err(FirnError::contract("checkpoint is not proposed"));
            }
            let receipt_match = receipt_match(&checkpoint.delta, &receipt);
            if !receipt_match.is_valid() && !self.accepts_faulty_receipt(receipt_match) {
                return Err(FirnError::contract("receipt does not cover checkpoint"));
            }

            for existing in state.checkpoints.values_mut() {
                if same_tuple(&existing.delta, &checkpoint.delta) {
                    existing.is_head = false;
                }
            }
            let committed = state.checkpoints.get_mut(checkpoint_id).unwrap();
            committed.status = CheckpointStatus::Committed;
            committed.receipt = Some(receipt.clone());
            committed.is_head = true;
            committed.committed_at_ms = Some(receipt.committed_at_ms);
            Ok(committed.clone())
        }

        fn abandon(&self, checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
            let mut state = self.lock_state();
            let checkpoint = state
                .checkpoints
                .get_mut(checkpoint_id)
                .ok_or_else(|| FirnError::contract("missing checkpoint"))?;
            checkpoint.status = CheckpointStatus::Abandoned;
            checkpoint.is_head = false;
            Ok(checkpoint.clone())
        }

        fn head(
            &self,
            pipeline_id: &PipelineId,
            resource_id: &ResourceId,
            scope: &ScopeKey,
        ) -> Result<Option<Checkpoint>> {
            if matches!(self.fault, Fault::NoHeadLookup) {
                return Ok(None);
            }
            let state = self.lock_state();
            Ok(state
                .checkpoints
                .values()
                .find(|checkpoint| {
                    checkpoint.status == CheckpointStatus::Committed
                        && checkpoint.is_head
                        && same_tuple_parts(checkpoint, pipeline_id, resource_id, scope)
                })
                .cloned())
        }

        fn history(
            &self,
            pipeline_id: &PipelineId,
            resource_id: &ResourceId,
            scope: &ScopeKey,
        ) -> Result<Vec<Checkpoint>> {
            let state = self.lock_state();
            Ok(state
                .order
                .iter()
                .filter_map(|checkpoint_id| state.checkpoints.get(checkpoint_id))
                .filter(|checkpoint| same_tuple_parts(checkpoint, pipeline_id, resource_id, scope))
                .cloned()
                .collect())
        }

        fn rewind(&self, request: RewindRequest) -> Result<RewindReport> {
            if matches!(self.fault, Fault::InvalidRewindSucceeds) {
                return self.wrong_rewind_report(&request);
            }
            if matches!(self.fault, Fault::ValidRewindReturnsWrongReport) {
                return self.wrong_rewind_report(&request);
            }
            Err(FirnError::contract(
                "rewind is not implemented by faulty store",
            ))
        }
    }

    impl FaultyStore {
        fn accepts_faulty_receipt(&self, receipt_match: ReceiptMatch) -> bool {
            matches!(
                (self.fault, receipt_match),
                (
                    Fault::AcceptOverreportedRowCount,
                    ReceiptMatch::OverreportedRowCount
                ) | (
                    Fault::AcceptUnderreportedRowCount,
                    ReceiptMatch::UnderreportedRowCount
                ) | (
                    Fault::AcceptOverreportedByteCount,
                    ReceiptMatch::OverreportedByteCount
                ) | (
                    Fault::AcceptUnderreportedByteCount,
                    ReceiptMatch::UnderreportedByteCount
                )
            )
        }

        fn wrong_rewind_report(&self, request: &RewindRequest) -> Result<RewindReport> {
            let state = self.lock_state();
            let head = state
                .checkpoints
                .values()
                .find(|checkpoint| checkpoint.status == CheckpointStatus::Committed)
                .cloned()
                .ok_or_else(|| FirnError::contract("missing committed checkpoint"))?;
            let mut marker = head.clone();
            marker.delta.checkpoint_id = request.marker_checkpoint_id.clone();
            marker.status = CheckpointStatus::Rewound;
            marker.receipt = None;
            marker.is_head = false;
            marker.committed_at_ms = None;
            marker.rewind_target_checkpoint_id = Some(request.target_checkpoint_id.clone());
            Ok(RewindReport {
                marker,
                head,
                packages_ahead: Vec::new(),
            })
        }
    }

    #[derive(Clone, Copy)]
    enum ReceiptMatch {
        Valid,
        PackageOrSchemaMismatch,
        MissingSegment,
        OverreportedRowCount,
        UnderreportedRowCount,
        OverreportedByteCount,
        UnderreportedByteCount,
    }

    impl ReceiptMatch {
        fn is_valid(self) -> bool {
            matches!(self, Self::Valid)
        }
    }

    fn receipt_match(delta: &StateDelta, receipt: &Receipt) -> ReceiptMatch {
        if receipt.package_hash != delta.package_hash || receipt.schema_hash != delta.schema_hash {
            return ReceiptMatch::PackageOrSchemaMismatch;
        }
        let acks = receipt
            .segment_acks
            .iter()
            .map(|ack| (&ack.segment_id, ack))
            .collect::<BTreeMap<_, _>>();
        let segment_ids = delta
            .segments
            .iter()
            .map(|segment| &segment.segment_id)
            .collect::<BTreeSet<_>>();
        if acks.keys().copied().collect::<BTreeSet<_>>() != segment_ids {
            return ReceiptMatch::MissingSegment;
        }
        for segment in &delta.segments {
            let ack = acks[&segment.segment_id];
            if ack.row_count > segment.row_count {
                return ReceiptMatch::OverreportedRowCount;
            }
            if ack.row_count < segment.row_count {
                return ReceiptMatch::UnderreportedRowCount;
            }
            if ack.byte_count > segment.byte_count {
                return ReceiptMatch::OverreportedByteCount;
            }
            if ack.byte_count < segment.byte_count {
                return ReceiptMatch::UnderreportedByteCount;
            }
        }
        ReceiptMatch::Valid
    }

    fn checkpoint_for(delta: StateDelta, status: CheckpointStatus) -> Checkpoint {
        Checkpoint {
            delta,
            status,
            receipt: None,
            is_head: false,
            created_at_ms: 1_700_000_000_000,
            committed_at_ms: None,
            rewind_target_checkpoint_id: None,
        }
    }

    fn same_tuple(left: &StateDelta, right: &StateDelta) -> bool {
        left.pipeline_id == right.pipeline_id
            && left.resource_id == right.resource_id
            && left.scope == right.scope
    }

    fn same_tuple_parts(
        checkpoint: &Checkpoint,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> bool {
        checkpoint.delta.pipeline_id == *pipeline_id
            && checkpoint.delta.resource_id == *resource_id
            && checkpoint.delta.scope == *scope
    }
}
