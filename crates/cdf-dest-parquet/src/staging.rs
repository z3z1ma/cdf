use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::mpsc,
};

use cdf_kernel::{
    CdfError, CommittedContentMembership, CommittedContentRoot, CommittedContentRootId,
    ContentDigest, ContentDigestAlgorithm, ContentDigestValue, ContentObjectKey,
    ContentPublicationClaim, ContentPublicationClaimId, ContentPublicationClaimState,
    ContentRootIntent, ContentRootState, DestinationProtocol, Result, SchemaHash, SegmentId,
    StateSegment,
};
use cdf_runtime::{
    DestinationCommitOutcome, DestinationReceiptReportingPolicy, StagedIngressRequest,
    StagedIngressSession, StagedSegmentIdentity, StagedSegmentRequest, StagedSegmentStream,
    StagingRecoveryMode, StagingSnapshot, VerifiedFinalBinding,
};
use sha2::Digest;

use crate::{
    ParquetCommitRequest, ParquetDestination,
    api::{duplicate_parquet_receipt, finalize_parquet_objects},
    layout::{
        PHYSICAL_PLAN_PATH, PHYSICAL_PLAN_VERSION, ParquetObjectLayoutPolicy, ParquetSegmentLayout,
    },
    manifest::canonical_json_bytes,
    manifest::{ParquetObjectEntry, ParquetObjectSegmentEntry},
    package::{
        ParquetGroupCommand, ParquetWriterSettings, StagedParquetEncodeContext,
        write_parquet_staged_group,
    },
    store::{
        ObjectKeyEncoder, StoredObject, data_object_key, now_ms, package_publication_metadata_key,
        staged_attempt_metadata_key,
    },
};

const ENCODE_LANE: &str = "parquet.encode";
const STAGING_METADATA_VERSION: u16 = 1;
const OBJECT_PUBLICATION_MODE: &str = "atomic_content_create_v1";
pub(crate) struct ParquetStagedIngressSession {
    destination: ParquetDestination,
    request: StagedIngressRequest,
    accepted: BTreeMap<u32, StagedSegmentIdentity>,
    objects: BTreeMap<u32, StagedParquetObject>,
    prepared_root: Option<(CommittedContentRootId, u64)>,
    physical_plan: ParquetPhysicalWritePlan,
}

#[derive(Clone)]
struct ParquetPhysicalWritePlan {
    encoder: ObjectKeyEncoder,
    target: cdf_kernel::TargetName,
    writers: u16,
    rows_per_batch: u64,
    bytes_per_batch: u64,
    object_layout: ParquetObjectLayoutPolicy,
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
            writers: request.bulk_path().writers,
            rows_per_batch: request.bulk_path().rows_per_batch,
            bytes_per_batch: request.bulk_path().bytes_per_batch,
            object_layout: ParquetObjectLayoutPolicy::current().validate()?,
        })
    }

    fn content_key(&self, sha256: &str) -> String {
        data_object_key(self.encoder, &self.target, sha256)
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct StagingAttemptMetadata {
    version: u16,
    target: String,
    attempt_id: String,
    physical_plan_path: String,
    physical_plan_version: u16,
    object_publication_mode: String,
    writers: u16,
    rows_per_batch: u64,
    bytes_per_batch: u64,
    object_target_package_bytes: u64,
    max_segments_per_object: u16,
    started_at_ms: i64,
    pub(crate) staging_lease: cdf_runtime::StagingLease,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PublicationAttemptMetadata {
    version: u16,
    pub(crate) staging_lease: cdf_runtime::StagingLease,
    pub(crate) root_id: CommittedContentRootId,
    pub(crate) root_generation: u64,
    pub(crate) manifest_key: String,
}

struct StagedParquetObject {
    object_ordinal: u32,
    identities: Vec<StagedSegmentIdentity>,
    object_key: String,
    stored: StoredObject,
    sha256: String,
    claim: ContentPublicationClaim,
}

struct ActiveParquetGroup {
    package_byte_count: u64,
    segment_count: usize,
    commands: mpsc::SyncSender<ParquetGroupCommand>,
    consumed: mpsc::Receiver<Result<StagedSegmentIdentity>>,
    task: cdf_runtime::ScopedBlockingTask<StagedParquetObject>,
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
        let metadata_key = staged_attempt_metadata_key(
            destination.object_key_encoder(),
            &request.binding().target,
            request.staging_lease().authority_domain_id(),
            request.attempt_id(),
            request.staging_lease().fencing_token(),
        );
        let metadata = StagingAttemptMetadata {
            version: STAGING_METADATA_VERSION,
            target: request.binding().target.as_str().to_owned(),
            attempt_id: request.attempt_id().as_str().to_owned(),
            physical_plan_path: PHYSICAL_PLAN_PATH.to_owned(),
            physical_plan_version: PHYSICAL_PLAN_VERSION,
            object_publication_mode: OBJECT_PUBLICATION_MODE.to_owned(),
            writers: physical_plan.writers,
            rows_per_batch: physical_plan.rows_per_batch,
            bytes_per_batch: physical_plan.bytes_per_batch,
            object_target_package_bytes: physical_plan.object_layout.target_package_bytes,
            max_segments_per_object: physical_plan.object_layout.max_segments,
            started_at_ms,
            staging_lease: request.staging_lease().clone(),
        };
        request.mutation_guard().assert_current()?;
        destination.store().put(
            destination.execution(),
            &metadata_key,
            canonical_json_bytes(&metadata)?,
        )?;
        request.mutation_guard().assert_current()?;
        Ok(Self {
            destination,
            request,
            accepted: BTreeMap::new(),
            objects: BTreeMap::new(),
            prepared_root: None,
            physical_plan,
        })
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

    fn start_group(&self, object_ordinal: u32) -> Result<ActiveParquetGroup> {
        let destination = self.destination.clone();
        let output_schema = self.request.output_schema().clone();
        let attempt_id = self.request.attempt_id().clone();
        let physical_plan = self.physical_plan.clone();
        let writer_settings = ParquetWriterSettings {
            rows_per_batch: self.physical_plan.rows_per_batch,
            bytes_per_batch: self.physical_plan.bytes_per_batch,
        };
        let mutation_guard = self.request.mutation_guard().clone();
        let run_id = format!(
            "parquet-stage-{}-object-{object_ordinal}",
            attempt_id.as_str()
        );
        let (commands, command_receiver) = mpsc::sync_channel(0);
        let (consumed_sender, consumed) = mpsc::sync_channel(0);
        let task = self.destination.execution().spawn_blocking_value(
            &run_id,
            ENCODE_LANE,
            move |cancellation| {
                cancellation.check()?;
                let file = destination.store().staging_file()?;
                let group = write_parquet_staged_group(
                    command_receiver,
                    consumed_sender,
                    StagedParquetEncodeContext {
                        expected_schema: &output_schema,
                        writer_memory: destination.execution().memory(),
                        spill: destination.execution().spill(),
                        file,
                        cancellation: &cancellation,
                        mutation_guard: &mutation_guard,
                        settings: writer_settings,
                    },
                )?;
                cancellation.check()?;
                mutation_guard.assert_current()?;
                let sha256 = group.encoded.sha256.clone();
                let object_key = physical_plan.content_key(&sha256);
                let planned_content = cdf_kernel::ImmutableContentIdentity::new(
                    destination.store().namespace().clone(),
                    ContentObjectKey::new(object_key.clone())?,
                    group.encoded.byte_count,
                    ContentDigest::new(
                        ContentDigestAlgorithm::new("sha256")?,
                        ContentDigestValue::new(sha256.clone())?,
                    )?,
                    None,
                )?;
                let claim_id = publication_claim_id(
                    &attempt_id,
                    mutation_guard.assert_current()?.fencing_token(),
                    object_ordinal,
                    &object_key,
                )?;
                let planned_claim = destination
                    .execution()
                    .content_reachability_store()?
                    .install_claim(mutation_guard.assert_current()?.content_publication_claim(
                        planned_content.clone(),
                        claim_id,
                        1,
                        ContentPublicationClaimState::Planned,
                    )?)?;
                let stored = match destination.store().put_encoded_file(
                    destination.execution(),
                    &object_key,
                    group.encoded,
                    &mutation_guard,
                    &cancellation,
                ) {
                    Ok(stored) => stored,
                    Err(error) => {
                        let release = destination
                            .execution()
                            .content_reachability_store()?
                            .release_claim(&planned_claim.claim_id, planned_claim.claim_generation);
                        return Err(match release {
                            Ok(()) => error,
                            Err(release) => attach_secondary(
                                error,
                                "failed to release unpublished content claim",
                                release,
                            ),
                        });
                    }
                };
                let published_content = match stored.provider_generation.clone() {
                    Some(provider_generation) => {
                        planned_content.with_provider_generation(provider_generation)
                    }
                    None => planned_content,
                };
                let claim = destination
                    .execution()
                    .content_reachability_store()?
                    .publish_claim(
                        &planned_claim.claim_id,
                        planned_claim.claim_generation,
                        published_content,
                    )?;
                mutation_guard.assert_current()?;
                Ok(StagedParquetObject {
                    object_ordinal,
                    identities: group.identities,
                    object_key,
                    stored,
                    sha256,
                    claim,
                })
            },
        )?;
        Ok(ActiveParquetGroup {
            package_byte_count: 0,
            segment_count: 0,
            commands,
            consumed,
            task,
        })
    }

    fn feed_group_segment(
        &mut self,
        group: &mut ActiveParquetGroup,
        segment: StagedSegmentRequest,
        stream: &mut dyn StagedSegmentStream,
    ) -> Result<()> {
        let identity = segment.identity.clone();
        self.validate_identity(&identity)?;
        group
            .commands
            .send(ParquetGroupCommand::Segment(segment))
            .map_err(|_| CdfError::destination("Parquet object group encoder stopped"))?;
        let consumed = group
            .consumed
            .recv()
            .map_err(|_| CdfError::destination("Parquet object group encoder stopped"))??;
        if consumed != identity {
            return Err(CdfError::destination(
                "Parquet object group consumed a different segment identity",
            ));
        }
        self.request.mutation_guard().assert_current()?;
        stream.acknowledge(cdf_runtime::StagedSegmentAck {
            attempt_id: self.request.attempt_id().clone(),
            identity: identity.clone(),
            external_durable: false,
        })?;
        self.request.mutation_guard().assert_current()?;
        if self.accepted.insert(identity.ordinal, identity).is_some() {
            return Err(CdfError::destination(
                "Parquet object group acknowledged a duplicate segment ordinal",
            ));
        }
        group.segment_count += 1;
        group.package_byte_count = group
            .package_byte_count
            .checked_add(consumed.byte_count)
            .ok_or_else(|| CdfError::data("Parquet object package byte count overflow"))?;
        Ok(())
    }

    fn finish_group(
        &self,
        group: ActiveParquetGroup,
    ) -> cdf_runtime::ScopedBlockingTask<StagedParquetObject> {
        let _ = group.commands.send(ParquetGroupCommand::Finish);
        group.task
    }

    fn complete_oldest(
        &mut self,
        pending: &mut VecDeque<cdf_runtime::ScopedBlockingTask<StagedParquetObject>>,
    ) -> Result<()> {
        let task = pending
            .pop_front()
            .ok_or_else(|| CdfError::internal("Parquet staged encode queue is empty"))?;
        let object = match self.destination.execution().run_io(task) {
            Ok(object) => object,
            Err(error) => return Err(self.with_join_failures(error, None, pending)),
        };
        if self.objects.contains_key(&object.object_ordinal) {
            return Err(self.with_join_failures(
                CdfError::destination("Parquet staged encode completed a duplicate object ordinal"),
                None,
                pending,
            ));
        }
        self.objects.insert(object.object_ordinal, object);
        Ok(())
    }

    fn cancel_and_join(
        &self,
        active: Option<ActiveParquetGroup>,
        pending: &mut VecDeque<cdf_runtime::ScopedBlockingTask<StagedParquetObject>>,
    ) -> Result<()> {
        if let Some(active) = active {
            active.task.termination().cancel();
            pending.push_back(active.task);
        }
        for task in pending.iter() {
            task.termination().cancel();
        }
        let mut failure = None;
        while let Some(task) = pending.pop_front() {
            // Cancellation is the expected terminal result. Awaiting every task is the
            // structural ownership barrier that prevents a retry from overlapping orphan work.
            match self.destination.execution().run_io(task) {
                Ok(_) => {}
                Err(error) => append_failure(&mut failure, "staged sibling join", error),
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn with_join_failures(
        &self,
        error: CdfError,
        active: Option<ActiveParquetGroup>,
        pending: &mut VecDeque<cdf_runtime::ScopedBlockingTask<StagedParquetObject>>,
    ) -> CdfError {
        match self.cancel_and_join(active, pending) {
            Ok(()) => error,
            Err(cleanup) => attach_secondary(error, "Parquet staged sibling cleanup", cleanup),
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
        self.request.mutation_guard().assert_current()?;
        let mut failure = None;
        if let Some((root_id, generation)) = self.prepared_root.take() {
            if let Err(error) = self
                .destination
                .execution()
                .content_reachability_store()?
                .abort_root(&root_id, generation)
            {
                append_failure(&mut failure, "prepared content root abort", error);
            }
        } else {
            for object in self.objects.values() {
                if let Err(error) = self
                    .destination
                    .execution()
                    .content_reachability_store()?
                    .release_claim(&object.claim.claim_id, object.claim.claim_generation)
                {
                    append_failure(&mut failure, "content publication claim release", error);
                }
            }
        }
        self.objects.clear();
        self.request.mutation_guard().assert_current()?;
        let prefix = crate::store::staged_attempt_prefix(
            self.destination.object_key_encoder(),
            &self.request.binding().target,
            self.request.staging_lease().authority_domain_id(),
            self.request.attempt_id(),
            self.request.staging_lease().fencing_token(),
        );
        if let Err(error) = self.destination.store().delete_prefix(
            self.destination.execution(),
            &prefix,
            self.request.mutation_guard(),
        ) {
            append_failure(&mut failure, "staged attempt cleanup", error);
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn prepare_content_root(
        &mut self,
        request: &ParquetCommitRequest,
    ) -> Result<ContentRootIntent> {
        let mut identities = self
            .objects
            .values()
            .map(|object| object.claim.content.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        identities.sort();
        let claim_ids = self
            .objects
            .values()
            .map(|object| object.claim.claim_id.clone())
            .collect::<Vec<_>>();
        let root_id = publication_root_id(request, self.destination.store().namespace())?;
        let intent = ContentRootIntent {
            root: CommittedContentRoot {
                destination_id: self.destination.sheet().destination.clone(),
                target: request.commit.target.clone(),
                root_id: root_id.clone(),
                root_generation: 1,
                retained_until_ms: None,
                membership: CommittedContentMembership::Inline { identities },
            },
            claim_ids,
            state: ContentRootState::Prepared,
        };
        let intent = self
            .destination
            .execution()
            .content_reachability_store()?
            .prepare_root(intent)?;
        self.prepared_root = Some((root_id, intent.root.root_generation));
        Ok(intent)
    }

    fn commit_prepared_root(&mut self) -> Result<()> {
        let (root_id, generation) = self.prepared_root.as_ref().ok_or_else(|| {
            CdfError::internal("Parquet publication has no prepared content root")
        })?;
        self.destination
            .execution()
            .content_reachability_store()?
            .commit_root(root_id, *generation)?;
        self.prepared_root = None;
        Ok(())
    }
}

impl StagedParquetObject {
    fn finalize(
        self,
        states: &BTreeMap<SegmentId, &StateSegment>,
        schema_hash: &SchemaHash,
    ) -> Result<ParquetObjectEntry> {
        let staged = self;
        (|| {
            let mut row_offset = 0_u64;
            let mut byte_count = 0_u64;
            let mut package_byte_count = 0_u64;
            let mut segments = Vec::with_capacity(staged.identities.len());
            for identity in &staged.identities {
                let state = states.get(&identity.segment_id).ok_or_else(|| {
                    CdfError::data(format!(
                        "Parquet staged segment {} is absent from final state authority",
                        identity.segment_id
                    ))
                })?;
                if identity.row_count != state.row_count {
                    return Err(CdfError::data(format!(
                        "Parquet staged segment {} differs from final state authority",
                        identity.segment_id
                    )));
                }
                segments.push(ParquetObjectSegmentEntry {
                    segment_id: identity.segment_id.as_str().to_owned(),
                    package_row_ord_start: identity.package_row_ord_start,
                    row_offset,
                    row_count: identity.row_count,
                    byte_count: state.byte_count,
                    package_byte_count: identity.byte_count,
                });
                row_offset = row_offset
                    .checked_add(identity.row_count)
                    .ok_or_else(|| CdfError::data("Parquet object row count overflow"))?;
                byte_count = byte_count
                    .checked_add(state.byte_count)
                    .ok_or_else(|| CdfError::data("Parquet object state byte count overflow"))?;
                package_byte_count = package_byte_count
                    .checked_add(identity.byte_count)
                    .ok_or_else(|| CdfError::data("Parquet object package byte count overflow"))?;
            }
            Ok(ParquetObjectEntry {
                key: staged.object_key,
                row_count: row_offset,
                byte_count,
                package_byte_count,
                parquet_byte_count: staged.stored.byte_count,
                sha256: staged.sha256,
                etag: staged.stored.e_tag,
                schema_hash: schema_hash.as_str().to_owned(),
                segments,
            })
        })()
    }
}

impl StagedIngressSession for ParquetStagedIngressSession {
    fn stage_stream(&mut self, stream: &mut dyn StagedSegmentStream) -> Result<()> {
        let maximum = usize::from(self.physical_plan.writers);
        let mut pending = VecDeque::with_capacity(maximum);
        let mut active = None;
        let mut next_object_ordinal = 0_u32;
        loop {
            let segment = match stream.next_segment() {
                Ok(Some(segment)) => segment,
                Ok(None) => break,
                Err(error) => {
                    return Err(self.with_join_failures(error, active.take(), &mut pending));
                }
            };
            if active.as_ref().is_some_and(|group: &ActiveParquetGroup| {
                self.physical_plan.object_layout.closes_before(
                    group.segment_count,
                    group.package_byte_count,
                    segment.identity.byte_count,
                )
            }) {
                let group = active.take().expect("active group was observed");
                pending.push_back(self.finish_group(group));
                if pending.len() == maximum {
                    self.complete_oldest(&mut pending)?;
                }
            }
            if active.is_none() {
                active = match self.start_group(next_object_ordinal) {
                    Ok(group) => Some(group),
                    Err(error) => {
                        return Err(self.with_join_failures(error, None, &mut pending));
                    }
                };
                next_object_ordinal = next_object_ordinal.checked_add(1).ok_or_else(|| {
                    CdfError::data("Parquet destination object count exceeds u32")
                })?;
            }
            if let Err(error) = self.feed_group_segment(
                active.as_mut().expect("active group was initialized"),
                segment,
                stream,
            ) {
                return Err(self.with_join_failures(error, active.take(), &mut pending));
            }
        }
        if let Some(group) = active.take() {
            pending.push_back(self.finish_group(group));
        }
        while !pending.is_empty() {
            self.complete_oldest(&mut pending)?;
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
        let result = (|| {
            self.validate_final_binding(&binding)?;
            let request = ParquetCommitRequest {
                commit: binding.commit().clone(),
                schema_hash: binding.schema_hash().clone(),
            };
            let segment_layouts = binding
                .ordered_segments()
                .iter()
                .map(|identity| ParquetSegmentLayout {
                    segment_id: identity.segment_id.clone(),
                    package_byte_count: identity.byte_count,
                })
                .collect::<Vec<_>>();
            let object_layouts = self.physical_plan.object_layout.plan(segment_layouts)?;
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
            let plan = self
                .destination
                .plan_package_shape(&request, rows, package_bytes)?;
            if &plan.kernel != binding.plan() {
                return Err(CdfError::destination(
                    "Parquet staged final binding commit plan differs from destination planning",
                ));
            }
            let root_intent = self.prepare_content_root(&request)?;

            let publication_key = package_publication_metadata_key(
                self.destination.object_key_encoder(),
                &request.commit.target,
                self.request.staging_lease().authority_domain_id(),
                self.request.attempt_id(),
                self.request.staging_lease().fencing_token(),
                &request.commit.idempotency_token,
            );
            self.request.mutation_guard().assert_current()?;
            self.destination.store().put(
                self.destination.execution(),
                &publication_key,
                canonical_json_bytes(&PublicationAttemptMetadata {
                    version: STAGING_METADATA_VERSION,
                    staging_lease: self.request.staging_lease().clone(),
                    root_id: root_intent.root.root_id.clone(),
                    root_generation: root_intent.root.root_generation,
                    manifest_key: plan.manifest_key.clone(),
                })?,
            )?;
            self.request.mutation_guard().assert_current()?;

            if let Some(existing) = self.destination.existing_verified_manifest(
                &request,
                &plan,
                self.request.mutation_guard(),
            )? {
                validate_existing_manifest_content(&existing.manifest, &root_intent.root)?;
                self.commit_prepared_root()?;
                self.objects.clear();
                self.destination
                    .store()
                    .delete(self.destination.execution(), &publication_key)?;
                self.cleanup()?;
                let receipt = duplicate_parquet_receipt(request, plan, existing)?;
                return Ok(DestinationCommitOutcome::new(
                    receipt,
                    DestinationReceiptReportingPolicy::DestinationCommit { duplicate: true },
                ));
            }

            let mut objects = Vec::with_capacity(object_layouts.len());
            let staged = std::mem::take(&mut self.objects);
            let states = binding
                .commit()
                .segments
                .iter()
                .map(|state| (state.segment_id.clone(), state))
                .collect::<BTreeMap<_, _>>();
            let mut seen = BTreeSet::new();
            for (ordinal, object) in staged {
                let expected = object_layouts
                    .get(usize::try_from(ordinal).map_err(|_| {
                        CdfError::data("Parquet object ordinal exceeds platform usize")
                    })?)
                    .ok_or_else(|| {
                        CdfError::data("Parquet staged object is outside the final object plan")
                    })?;
                let actual = object
                    .identities
                    .iter()
                    .map(|identity| &identity.segment_id)
                    .collect::<Vec<_>>();
                let expected_ids = expected
                    .segments
                    .iter()
                    .map(|segment| &segment.segment_id)
                    .collect::<Vec<_>>();
                if actual != expected_ids {
                    return Err(CdfError::data(
                        "Parquet staged object segments differ from the final object layout",
                    ));
                }
                for segment_id in actual {
                    if !seen.insert(segment_id.clone()) {
                        return Err(CdfError::data(
                            "Parquet staged final binding repeats a segment id",
                        ));
                    }
                }
                objects.push(object.finalize(&states, binding.schema_hash())?);
            }
            let publication = finalize_parquet_objects(
                &self.destination,
                request,
                plan,
                objects,
                self.request.mutation_guard(),
            )?;
            self.commit_prepared_root()?;
            self.request.mutation_guard().assert_current()?;
            self.destination
                .store()
                .delete(self.destination.execution(), &publication_key)?;
            let (receipt, verification) = publication.into_parts();
            self.cleanup()?;
            DestinationCommitOutcome::new(
                receipt,
                DestinationReceiptReportingPolicy::DestinationCommit { duplicate: false },
            )
            .with_commit_verification(verification)
        })();
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) => match self.cleanup() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(attach_secondary(
                    error,
                    "failed Parquet final-binding cleanup",
                    cleanup,
                )),
            },
        }
    }

    fn abort(mut self: Box<Self>) -> Result<()> {
        self.cleanup()
    }
}

fn publication_claim_id(
    attempt_id: &cdf_runtime::LoadAttemptId,
    fencing_token: u64,
    object_ordinal: u32,
    object_key: &str,
) -> Result<ContentPublicationClaimId> {
    let digest = sha2::Sha256::digest(
        format!(
            "{}\0{fencing_token}\0{object_ordinal}\0{object_key}",
            attempt_id.as_str()
        )
        .as_bytes(),
    );
    ContentPublicationClaimId::new(format!("parquet-{}", hex::encode(digest)))
}

fn publication_root_id(
    request: &ParquetCommitRequest,
    namespace: &cdf_kernel::ContentStoreNamespace,
) -> Result<CommittedContentRootId> {
    let digest = sha2::Sha256::digest(
        format!(
            "{}\0{}\0{}\0{}",
            namespace.as_str(),
            request.commit.target.as_str(),
            request.commit.package_hash.as_str(),
            request.commit.idempotency_token.as_str()
        )
        .as_bytes(),
    );
    CommittedContentRootId::new(format!("parquet-{}", hex::encode(digest)))
}

fn validate_existing_manifest_content(
    manifest: &crate::manifest::ParquetObjectManifest,
    root: &CommittedContentRoot,
) -> Result<()> {
    let expected = match &root.membership {
        CommittedContentMembership::Inline { identities } => identities
            .iter()
            .map(|identity| {
                (
                    identity.object_key.as_str(),
                    identity.byte_count,
                    identity.digest.value.as_str(),
                )
            })
            .collect::<BTreeSet<_>>(),
        CommittedContentMembership::Shard { .. } => {
            return Err(CdfError::internal(
                "Parquet content root unexpectedly uses sharded membership",
            ));
        }
    };
    let observed = manifest
        .objects
        .iter()
        .map(|object| {
            (
                object.key.as_str(),
                object.parquet_byte_count,
                object.sha256.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    if observed == expected {
        Ok(())
    } else {
        Err(CdfError::destination(
            "existing Parquet manifest content differs from its prepared reachability root",
        ))
    }
}

fn attach_secondary(mut primary: CdfError, context: &str, secondary: CdfError) -> CdfError {
    primary
        .message
        .push_str(&format!("; {context} also failed: {secondary}"));
    primary
}

fn append_failure(failure: &mut Option<CdfError>, context: &str, error: CdfError) {
    match failure.take() {
        Some(primary) => *failure = Some(attach_secondary(primary, context, error)),
        None => {
            let mut error = error;
            error.message = format!("{context}: {}", error.message);
            *failure = Some(error);
        }
    }
}
