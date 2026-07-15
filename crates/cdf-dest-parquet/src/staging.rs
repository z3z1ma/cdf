use std::collections::{BTreeMap, BTreeSet, VecDeque};

use cdf_kernel::{CdfError, DestinationProtocol, Result, SchemaHash, SegmentId, StateSegment};
use cdf_runtime::{
    DestinationCommitOutcome, DestinationReceiptReportingPolicy, StagedIngressRequest,
    StagedIngressSession, StagedSegmentIdentity, StagedSegmentRequest, StagedSegmentStream,
    StagingRecoveryMode, StagingSnapshot, VerifiedFinalBinding,
};

use crate::{
    ParquetCommitRequest, ParquetDestination,
    api::{duplicate_parquet_receipt, finalize_parquet_objects},
    manifest::ParquetObjectEntry,
    manifest::canonical_json_bytes,
    package::write_parquet_staged_segment,
    store::{
        ObjectKeyEncoder, StoreClient, StoredObject, now_ms, segment_object_key,
        staged_attempt_metadata_key, staged_segment_object_key,
    },
};

const ENCODE_LANE: &str = "parquet.encode";
const PHYSICAL_PLAN_PATH: &str = "arrow_ipc_to_parquet";
const PHYSICAL_PLAN_VERSION: u16 = 2;
const STAGING_METADATA_VERSION: u16 = 1;
const HEARTBEAT_INTERVAL_MS: i64 = 60_000;

pub(crate) struct ParquetStagedIngressSession {
    destination: ParquetDestination,
    request: StagedIngressRequest,
    accepted: BTreeMap<u32, StagedSegmentIdentity>,
    objects: BTreeMap<u32, StagedParquetObject>,
    active_attempt_key: String,
    physical_plan: ParquetPhysicalWritePlan,
    metadata_key: String,
    started_at_ms: i64,
    last_heartbeat_ms: i64,
}

#[derive(Clone)]
struct ParquetPhysicalWritePlan {
    encoder: ObjectKeyEncoder,
    target: cdf_kernel::TargetName,
    attempt_id: cdf_runtime::LoadAttemptId,
    writers: u16,
    rows_per_batch: u64,
    bytes_per_batch: u64,
}

impl ParquetPhysicalWritePlan {
    fn compile(destination: &ParquetDestination, request: &StagedIngressRequest) -> Result<Self> {
        let descriptor = &request.bulk_path().descriptor;
        if descriptor.path_id != PHYSICAL_PLAN_PATH || descriptor.version != PHYSICAL_PLAN_VERSION {
            return Err(CdfError::contract(format!(
                "Parquet staged ingress requires physical plan {PHYSICAL_PLAN_PATH}@{PHYSICAL_PLAN_VERSION}, got {}@{}",
                descriptor.path_id, descriptor.version
            )));
        }
        if request.bulk_path().writers > request.scheduling().max_in_flight_segments {
            return Err(CdfError::contract(format!(
                "Parquet physical plan requires {} writers but staged ingress permits only {} in-flight segments",
                request.bulk_path().writers,
                request.scheduling().max_in_flight_segments
            )));
        }
        Ok(Self {
            encoder: destination.object_key_encoder(),
            target: request.binding().target.clone(),
            attempt_id: request.attempt_id().clone(),
            writers: request.bulk_path().writers,
            rows_per_batch: request.bulk_path().rows_per_batch,
            bytes_per_batch: request.bulk_path().bytes_per_batch,
        })
    }

    fn staging_key(&self, segment_id: &SegmentId) -> String {
        staged_segment_object_key(self.encoder, &self.target, &self.attempt_id, segment_id)
    }

    fn final_keys(
        &self,
        token: &cdf_kernel::IdempotencyToken,
        segments: &[SegmentId],
    ) -> Vec<String> {
        segments
            .iter()
            .map(|segment| segment_object_key(self.encoder, &self.target, token, segment))
            .collect()
    }
}

#[derive(serde::Serialize)]
struct StagingAttemptMetadata<'a> {
    version: u16,
    target: &'a str,
    attempt_id: &'a str,
    physical_plan_path: &'a str,
    physical_plan_version: u16,
    writers: u16,
    rows_per_batch: u64,
    bytes_per_batch: u64,
    started_at_ms: i64,
    heartbeat_at_ms: i64,
}

impl Drop for ParquetStagedIngressSession {
    fn drop(&mut self) {
        self.destination
            .release_staged_attempt(&self.active_attempt_key);
    }
}

struct StagedParquetObject {
    identity: StagedSegmentIdentity,
    store: StoreClient,
    execution: cdf_runtime::ExecutionServices,
    staging_key: String,
    stored: StoredObject,
    sha256: String,
    cleanup: bool,
}

impl Drop for StagedParquetObject {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = self.store.delete(&self.execution, &self.staging_key);
        }
    }
}

impl ParquetStagedIngressSession {
    pub(crate) fn new(
        destination: ParquetDestination,
        request: StagedIngressRequest,
    ) -> Result<Self> {
        if request.binding().destination_id != destination.sheet().destination {
            return Err(CdfError::contract(
                "Parquet staged ingress request names a different destination",
            ));
        }
        if !request.binding().merge_keys.is_empty() {
            return Err(CdfError::contract(
                "Parquet staged ingress does not support merge keys",
            ));
        }
        cdf_package::validate_parquet_schema(request.output_schema())?;
        let physical_plan = ParquetPhysicalWritePlan::compile(&destination, &request)?;
        let started_at_ms = now_ms()?;
        destination.cleanup_expired_staging(&request.binding().target, started_at_ms)?;
        let active_attempt_key =
            destination.claim_staged_attempt(&request.binding().target, request.attempt_id())?;
        if let Err(error) =
            destination.cleanup_staged_attempt(&request.binding().target, request.attempt_id())
        {
            destination.release_staged_attempt(&active_attempt_key);
            return Err(error);
        }
        let metadata_key = staged_attempt_metadata_key(
            destination.object_key_encoder(),
            &request.binding().target,
            request.attempt_id(),
        );
        let session = Self {
            destination,
            request,
            accepted: BTreeMap::new(),
            objects: BTreeMap::new(),
            active_attempt_key,
            physical_plan,
            metadata_key,
            started_at_ms,
            last_heartbeat_ms: started_at_ms,
        };
        if let Err(error) = session.write_heartbeat(started_at_ms) {
            session
                .destination
                .release_staged_attempt(&session.active_attempt_key);
            return Err(error);
        }
        Ok(session)
    }

    fn write_heartbeat(&self, heartbeat_at_ms: i64) -> Result<()> {
        self.destination.store().put(
            self.destination.execution(),
            &self.metadata_key,
            canonical_json_bytes(&StagingAttemptMetadata {
                version: STAGING_METADATA_VERSION,
                target: self.request.binding().target.as_str(),
                attempt_id: self.request.attempt_id().as_str(),
                physical_plan_path: PHYSICAL_PLAN_PATH,
                physical_plan_version: PHYSICAL_PLAN_VERSION,
                writers: self.physical_plan.writers,
                rows_per_batch: self.physical_plan.rows_per_batch,
                bytes_per_batch: self.physical_plan.bytes_per_batch,
                started_at_ms: self.started_at_ms,
                heartbeat_at_ms,
            })?,
        )?;
        Ok(())
    }

    fn refresh_heartbeat(&mut self) -> Result<()> {
        let now = now_ms()?;
        if now.saturating_sub(self.last_heartbeat_ms) < HEARTBEAT_INTERVAL_MS {
            return Ok(());
        }
        self.write_heartbeat(now)?;
        self.last_heartbeat_ms = now;
        Ok(())
    }

    fn validate_identity(&self, identity: &StagedSegmentIdentity) -> Result<()> {
        if identity.schema_hash != self.request.binding().schema_hash {
            return Err(CdfError::data(
                "Parquet staged segment schema hash differs from its attempt",
            ));
        }
        if identity.row_count == 0 {
            return Err(CdfError::data(
                "Parquet staged data segment must contain at least one row",
            ));
        }
        if self.accepted.contains_key(&identity.ordinal)
            || self
                .accepted
                .values()
                .any(|accepted| accepted.segment_id == identity.segment_id)
        {
            return Err(CdfError::data(
                "Parquet staged segments must have unique ids and ordinals",
            ));
        }
        Ok(())
    }

    fn spawn_encode(
        &self,
        segment: StagedSegmentRequest,
    ) -> Result<cdf_runtime::ScopedBlockingTask<StagedParquetObject>> {
        let identity = segment.identity.clone();
        self.validate_identity(&identity)?;
        let destination = self.destination.clone();
        let output_schema = self.request.output_schema().clone();
        let attempt_id = self.request.attempt_id().clone();
        let staging_key = self.physical_plan.staging_key(&identity.segment_id);
        let run_id = format!("parquet-stage-{}-{}", attempt_id.as_str(), identity.ordinal);
        self.destination.execution().spawn_blocking_value(
            &run_id,
            ENCODE_LANE,
            move |cancellation| {
                cancellation.check()?;
                let file = destination.store().staging_file()?;
                let (identity, encoded) = write_parquet_staged_segment(
                    segment,
                    &output_schema,
                    destination.execution().memory(),
                    destination.execution().spill(),
                    file,
                    &cancellation,
                )?;
                cancellation.check()?;
                let sha256 = encoded.sha256.clone();
                let stored = destination.store().put_encoded_file(
                    destination.execution(),
                    &staging_key,
                    encoded,
                )?;
                Ok(StagedParquetObject {
                    identity,
                    store: destination.store().clone(),
                    execution: destination.execution().clone(),
                    staging_key,
                    stored,
                    sha256,
                    cleanup: true,
                })
            },
        )
    }

    fn complete_oldest(
        &mut self,
        pending: &mut VecDeque<cdf_runtime::ScopedBlockingTask<StagedParquetObject>>,
        stream: &mut dyn StagedSegmentStream,
    ) -> Result<()> {
        let task = pending
            .pop_front()
            .ok_or_else(|| CdfError::internal("Parquet staged encode queue is empty"))?;
        let completed = (|| {
            let object = self.destination.execution().run_io(task)?;
            let identity = object.identity().clone();
            stream.acknowledge(cdf_runtime::StagedSegmentAck {
                attempt_id: self.request.attempt_id().clone(),
                identity: identity.clone(),
                external_durable: object.external_durable(),
            })?;
            if self
                .accepted
                .insert(identity.ordinal, identity.clone())
                .is_some()
                || self.objects.insert(identity.ordinal, object).is_some()
            {
                return Err(CdfError::destination(
                    "Parquet staged encode completed a duplicate ordinal",
                ));
            }
            self.refresh_heartbeat()?;
            Ok(())
        })();
        if let Err(error) = completed {
            self.cancel_and_join(pending);
            return Err(error);
        }
        Ok(())
    }

    fn cancel_and_join(
        &self,
        pending: &mut VecDeque<cdf_runtime::ScopedBlockingTask<StagedParquetObject>>,
    ) {
        for task in pending.iter() {
            task.termination().cancel();
        }
        while let Some(task) = pending.pop_front() {
            // Cancellation is the expected terminal result. Awaiting every task is the
            // structural ownership barrier that prevents a retry from overlapping orphan work.
            let _ = self.destination.execution().run_io(task);
        }
    }

    fn accepted_in_order(&self) -> Vec<StagedSegmentIdentity> {
        self.accepted.values().cloned().collect()
    }

    fn validate_final_binding(&self, binding: &VerifiedFinalBinding) -> Result<()> {
        if binding.attempt_id() != self.request.attempt_id()
            || binding.execution_plan_id() != &self.request.binding().execution_plan_id
            || binding.commit().target != self.request.binding().target
            || binding.commit().disposition != self.request.binding().disposition
            || binding.schema_hash() != &self.request.binding().schema_hash
            || binding.output_arrow_schema_hash()
                != &self.request.binding().output_arrow_schema_hash
            || binding.merge_keys() != self.request.binding().merge_keys
        {
            return Err(CdfError::destination(
                "Parquet staged ingress final binding differs from its attempt authority",
            ));
        }
        binding.validate_staged_identities(&self.accepted_in_order())
    }

    fn cleanup(&mut self) -> Result<()> {
        let mut first_error = None;
        for object in self.objects.values_mut() {
            if let Err(error) = object.cleanup() {
                first_error.get_or_insert(error);
            }
        }
        self.objects.clear();
        let prefix = crate::store::staged_attempt_prefix(
            self.destination.object_key_encoder(),
            &self.request.binding().target,
            self.request.attempt_id(),
        );
        if let Err(error) = self
            .destination
            .store()
            .delete_prefix(self.destination.execution(), &prefix)
        {
            first_error.get_or_insert(error);
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl StagedParquetObject {
    fn identity(&self) -> &StagedSegmentIdentity {
        &self.identity
    }

    fn external_durable(&self) -> bool {
        true
    }

    fn cleanup(&mut self) -> Result<()> {
        if self.cleanup {
            self.store.delete(&self.execution, &self.staging_key)?;
            self.cleanup = false;
        }
        Ok(())
    }

    fn finalize(
        self,
        state: &StateSegment,
        final_key: String,
        schema_hash: &SchemaHash,
    ) -> Result<ParquetObjectEntry> {
        let mut staged = self;
        let identity = staged.identity.clone();
        if identity.segment_id != state.segment_id || identity.row_count != state.row_count {
            return Err(CdfError::data(format!(
                "Parquet staged segment {} differs from final state authority",
                identity.segment_id
            )));
        }
        let stored = staged.store.promote_create_or_verify(
            &staged.execution,
            &staged.staging_key,
            &final_key,
            &staged.sha256,
            staged.stored.byte_count,
            staged.stored.e_tag.clone(),
        )?;
        staged
            .store
            .delete(&staged.execution, &staged.staging_key)?;
        staged.cleanup = false;
        Ok(ParquetObjectEntry {
            segment_id: identity.segment_id.as_str().to_owned(),
            key: final_key,
            row_count: identity.row_count,
            byte_count: state.byte_count,
            package_byte_count: identity.byte_count,
            parquet_byte_count: stored.byte_count,
            sha256: staged.sha256.clone(),
            etag: stored.e_tag,
            schema_hash: schema_hash.as_str().to_owned(),
        })
    }
}

impl StagedIngressSession for ParquetStagedIngressSession {
    fn stage_stream(&mut self, stream: &mut dyn StagedSegmentStream) -> Result<()> {
        let maximum = usize::from(self.physical_plan.writers);
        let mut pending = VecDeque::with_capacity(maximum);
        loop {
            let segment = match stream.next_segment() {
                Ok(Some(segment)) => segment,
                Ok(None) => break,
                Err(error) => {
                    self.cancel_and_join(&mut pending);
                    return Err(error);
                }
            };
            let task = match self.spawn_encode(segment) {
                Ok(task) => task,
                Err(error) => {
                    self.cancel_and_join(&mut pending);
                    return Err(error);
                }
            };
            pending.push_back(task);
            if pending.len() == maximum {
                self.complete_oldest(&mut pending, stream)?;
            }
        }
        while !pending.is_empty() {
            self.complete_oldest(&mut pending, stream)?;
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<StagingSnapshot> {
        Ok(StagingSnapshot {
            attempt_id: self.request.attempt_id().clone(),
            binding: self.request.binding().clone(),
            recovery: StagingRecoveryMode::RollbackRedrive,
            accepted_segments: self.accepted_in_order(),
        })
    }

    fn bind_final(
        mut self: Box<Self>,
        binding: VerifiedFinalBinding,
    ) -> Result<DestinationCommitOutcome> {
        self.validate_final_binding(&binding)?;
        let request = ParquetCommitRequest {
            commit: binding.commit().clone(),
            schema_hash: binding.schema_hash().clone(),
        };
        let segment_ids = binding
            .ordered_segments()
            .iter()
            .map(|identity| identity.segment_id.clone())
            .collect::<Vec<SegmentId>>();
        let rows = binding
            .ordered_segments()
            .iter()
            .map(|identity| identity.row_count)
            .sum();
        let package_bytes = binding
            .ordered_segments()
            .iter()
            .map(|identity| identity.byte_count)
            .sum();
        let plan =
            self.destination
                .plan_package_shape(&request, &segment_ids, rows, package_bytes)?;
        if plan.object_keys
            != self
                .physical_plan
                .final_keys(&request.commit.idempotency_token, &segment_ids)
        {
            return Err(CdfError::destination(
                "Parquet final object keys differ from the compiled physical write plan",
            ));
        }
        if &plan.kernel != binding.plan() {
            return Err(CdfError::destination(
                "Parquet staged final binding commit plan differs from destination planning",
            ));
        }
        if let Some(existing) = self
            .destination
            .existing_verified_manifest(&request, &plan)?
        {
            self.cleanup()?;
            let receipt = duplicate_parquet_receipt(request, plan, existing)?;
            return Ok(DestinationCommitOutcome::new(
                receipt,
                DestinationReceiptReportingPolicy::DestinationCommit { duplicate: true },
            ));
        }

        let mut objects = Vec::with_capacity(segment_ids.len());
        let staged = std::mem::take(&mut self.objects);
        let states = binding
            .commit()
            .segments
            .iter()
            .map(|state| (state.segment_id.clone(), state))
            .collect::<BTreeMap<_, _>>();
        let mut seen = BTreeSet::new();
        for (ordinal, object) in staged {
            let identity = self.accepted.get(&ordinal).ok_or_else(|| {
                CdfError::internal("Parquet staged object has no accepted identity")
            })?;
            if !seen.insert(identity.segment_id.clone()) {
                return Err(CdfError::data(
                    "Parquet staged final binding repeats a segment id",
                ));
            }
            let state = states.get(&identity.segment_id).ok_or_else(|| {
                CdfError::data(format!(
                    "Parquet staged segment {} is absent from final state authority",
                    identity.segment_id
                ))
            })?;
            let key =
                plan.object_keys
                    .get(usize::try_from(ordinal).map_err(|_| {
                        CdfError::data("Parquet staged ordinal exceeds platform usize")
                    })?)
                    .ok_or_else(|| {
                        CdfError::data("Parquet staged ordinal is outside the final object plan")
                    })?
                    .clone();
            objects.push(object.finalize(state, key, binding.schema_hash())?);
        }
        self.destination
            .store()
            .sync_local_object_parents(&plan.object_keys)?;
        let publication = finalize_parquet_objects(&self.destination, request, plan, objects)?;
        let (receipt, verification) = publication.into_parts();
        self.cleanup()?;
        DestinationCommitOutcome::new(
            receipt,
            DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false },
        )
        .with_commit_verification(verification)
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        self.cleanup()
    }
}
