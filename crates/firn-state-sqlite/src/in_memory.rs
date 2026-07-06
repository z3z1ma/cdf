use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard},
};

use firn_kernel::{
    Checkpoint, CheckpointId, CheckpointStatus, CheckpointStore, FirnError, PipelineId, Receipt,
    ResourceId, Result, RewindReport, RewindRequest, ScopeKey, StateDelta,
};

use crate::support::{
    lock_error, missing_checkpoint, now_ms, packages_ahead_of_state, rewind_marker, same_tuple,
    validate_state_version, verify_receipt,
};

#[derive(Default)]
pub struct InMemoryCheckpointStore {
    inner: Mutex<InMemoryCheckpointState>,
}

#[derive(Default)]
struct InMemoryCheckpointState {
    checkpoints: BTreeMap<CheckpointId, Checkpoint>,
    order: Vec<CheckpointId>,
}

impl InMemoryCheckpointStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CheckpointStore for InMemoryCheckpointStore {
    fn propose(&self, delta: StateDelta) -> Result<Checkpoint> {
        validate_state_version(delta.state_version)?;
        let mut state = self.lock_inner()?;
        if state.checkpoints.contains_key(&delta.checkpoint_id) {
            return Err(FirnError::contract(format!(
                "checkpoint {} already exists",
                delta.checkpoint_id
            )));
        }

        let checkpoint = Checkpoint {
            delta,
            status: CheckpointStatus::Proposed,
            receipt: None,
            is_head: false,
            created_at_ms: now_ms()?,
            committed_at_ms: None,
            rewind_target_checkpoint_id: None,
        };
        state.order.push(checkpoint.delta.checkpoint_id.clone());
        state
            .checkpoints
            .insert(checkpoint.delta.checkpoint_id.clone(), checkpoint.clone());
        Ok(checkpoint)
    }

    fn commit(&self, checkpoint_id: &CheckpointId, receipt: Receipt) -> Result<Checkpoint> {
        let mut state = self.lock_inner()?;
        let checkpoint = state
            .checkpoints
            .get(checkpoint_id)
            .ok_or_else(|| missing_checkpoint(checkpoint_id))?
            .clone();
        if checkpoint.status != CheckpointStatus::Proposed {
            return Err(FirnError::contract(format!(
                "checkpoint {checkpoint_id} is not proposed"
            )));
        }
        verify_receipt(&receipt, &checkpoint.delta)?;

        for existing in state.checkpoints.values_mut() {
            if same_tuple(
                &existing.delta,
                &checkpoint.delta.pipeline_id,
                &checkpoint.delta.resource_id,
                &checkpoint.delta.scope,
            ) {
                existing.is_head = false;
            }
        }

        let committed = state
            .checkpoints
            .get_mut(checkpoint_id)
            .ok_or_else(|| missing_checkpoint(checkpoint_id))?;
        committed.status = CheckpointStatus::Committed;
        committed.receipt = Some(receipt.clone());
        committed.is_head = true;
        committed.committed_at_ms = Some(receipt.committed_at_ms);
        Ok(committed.clone())
    }

    fn abandon(&self, checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
        let mut state = self.lock_inner()?;
        let checkpoint = state
            .checkpoints
            .get_mut(checkpoint_id)
            .ok_or_else(|| missing_checkpoint(checkpoint_id))?;
        if checkpoint.status != CheckpointStatus::Proposed {
            return Err(FirnError::contract(format!(
                "checkpoint {checkpoint_id} is not proposed"
            )));
        }
        checkpoint.status = CheckpointStatus::Abandoned;
        Ok(checkpoint.clone())
    }

    fn head(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>> {
        let state = self.lock_inner()?;
        Ok(in_memory_head(&state, pipeline_id, resource_id, scope))
    }

    fn history(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>> {
        let state = self.lock_inner()?;
        Ok(in_memory_history(&state, pipeline_id, resource_id, scope))
    }

    fn rewind(&self, request: RewindRequest) -> Result<RewindReport> {
        let mut state = self.lock_inner()?;
        if state
            .checkpoints
            .contains_key(&request.marker_checkpoint_id)
        {
            return Err(FirnError::contract(format!(
                "checkpoint {} already exists",
                request.marker_checkpoint_id
            )));
        }

        let target = state
            .checkpoints
            .get(&request.target_checkpoint_id)
            .ok_or_else(|| missing_checkpoint(&request.target_checkpoint_id))?
            .clone();
        if target.status != CheckpointStatus::Committed
            || !same_tuple(
                &target.delta,
                &request.pipeline_id,
                &request.resource_id,
                &request.scope,
            )
        {
            return Err(FirnError::contract(
                "rewind target must be a committed checkpoint for the requested scope",
            ));
        }
        let current_head = in_memory_head(
            &state,
            &request.pipeline_id,
            &request.resource_id,
            &request.scope,
        )
        .ok_or_else(|| FirnError::contract("cannot rewind without a committed head"))?;
        let history = in_memory_history(
            &state,
            &request.pipeline_id,
            &request.resource_id,
            &request.scope,
        );
        let packages_ahead = packages_ahead_of_state(
            &history,
            &current_head.delta.checkpoint_id,
            &target.delta.checkpoint_id,
        );

        for checkpoint in state.checkpoints.values_mut() {
            if same_tuple(
                &checkpoint.delta,
                &request.pipeline_id,
                &request.resource_id,
                &request.scope,
            ) {
                checkpoint.is_head = false;
            }
        }
        let head = state
            .checkpoints
            .get_mut(&request.target_checkpoint_id)
            .ok_or_else(|| missing_checkpoint(&request.target_checkpoint_id))?;
        head.is_head = true;
        let head = head.clone();

        let marker = rewind_marker(&request, &current_head, &target, now_ms()?);
        state.order.push(marker.delta.checkpoint_id.clone());
        state
            .checkpoints
            .insert(marker.delta.checkpoint_id.clone(), marker.clone());

        Ok(RewindReport {
            marker,
            head,
            packages_ahead,
        })
    }
}

impl InMemoryCheckpointStore {
    fn lock_inner(&self) -> Result<MutexGuard<'_, InMemoryCheckpointState>> {
        self.inner.lock().map_err(lock_error)
    }
}

#[cfg(test)]
impl InMemoryCheckpointStore {
    pub(crate) fn clear_head_for_test(&self, checkpoint_id: &CheckpointId) {
        let mut state = self.inner.lock().unwrap();
        state.checkpoints.get_mut(checkpoint_id).unwrap().is_head = false;
    }
}

fn in_memory_head(
    state: &InMemoryCheckpointState,
    pipeline_id: &PipelineId,
    resource_id: &ResourceId,
    scope: &ScopeKey,
) -> Option<Checkpoint> {
    state
        .checkpoints
        .values()
        .find(|checkpoint| {
            checkpoint.status == CheckpointStatus::Committed
                && checkpoint.is_head
                && same_tuple(&checkpoint.delta, pipeline_id, resource_id, scope)
        })
        .cloned()
}

fn in_memory_history(
    state: &InMemoryCheckpointState,
    pipeline_id: &PipelineId,
    resource_id: &ResourceId,
    scope: &ScopeKey,
) -> Vec<Checkpoint> {
    state
        .order
        .iter()
        .filter_map(|checkpoint_id| state.checkpoints.get(checkpoint_id))
        .filter(|checkpoint| same_tuple(&checkpoint.delta, pipeline_id, resource_id, scope))
        .cloned()
        .collect()
}
