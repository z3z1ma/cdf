use super::{
    Arc, AtomicBool, BTreeMap, CHECKPOINT_STATE_VERSION, CdfError, Checkpoint, CheckpointId,
    CheckpointStatus, CheckpointStore, CommitCounts, ContractPolicy, DESTINATION_COMMIT_PLAN_FILE,
    DataType, DependencyTuple, DestinationCommitPlanPreimage, DestinationProtocol,
    DuckDbDestination, EnginePlan, Field, FileManifest, FilePosition, IdempotencyToken,
    IdentifierRules, InMemoryScopeLeaseStore, LineageInputObservation, LineageSummary,
    MANIFEST_FILE, Mutex, Ordering, PROCESSED_OBSERVATIONS_FILE, PackageArtifactRecoveryRequest,
    PackageArtifactReplayRequest, PackageBuilder, PackageManifest, PackageReader,
    PackageReplayHooks, PackageReplayStage, PackageStatus, ParquetDestination, PartitionId, Path,
    PipelineId, PostgresDestination, PostgresTarget, ProcessedObservationEvidenceArtifact,
    ProcessedObservationOutcome, ProcessedObservationPosition, ProjectDestinationDescription,
    ProjectDestinationDriver, ProjectDestinationRegistry, ProjectDestinationRuntime,
    ProjectReceiptSource, ProjectResolutionContext, ProjectRunRequest, ProjectRunSource,
    QueryableResource, RECEIPTS_FILE, Receipt, ReceiptId, ResolvedProjectDestination, ResourceId,
    ResourceStream, Result, RewindReport, RewindRequest, RunEventKind, RunEventValue, RunId,
    RunPhase, RunPhaseStatus, RunTelemetryConfig, RuntimeStage, STATE_INPUT_CHECKPOINT_FILE,
    STATE_PROPOSED_DELTA_FILE, Schema, SchemaHash, ScopeKey, SegmentAck, SemanticCatalog,
    SourcePosition, SqliteCheckpointStore, SqliteRunLedger, StateDelta, StateDeltaPreimage,
    TargetName, VerifyClause, WriteDisposition, canonical_json_bytes, fs,
    generate_lockfile_with_destination_artifacts, identifier_policy_from_destination_rules,
    parse_cdf_toml, record_package_receipt_once, recover_package_from_artifacts,
    replay_package_from_artifacts, replay_package_from_artifacts_with_stage_hook,
    replay_package_with_runtime, resolve_project_run_destination,
    support::{
        BoundTestResource, LivePostgres, MockDestination, MockDestinationCounters,
        MockProjectDestinationRuntime, SCHEMA_HASH, SIMPLE_FILE_RESOURCE_APPEND,
        artifact_expression_plan, artifact_fixture_partition_binding, build_package_with_carryover,
        compiled_test_source_plan, delta, destination, live_file_resource, live_plan,
        live_plan_for_queryable_with_exact_policy, live_plan_with_exact_policy, mock_bulk_path,
        multi_file_resource, package_receipts, package_status, parquet_project_run_request,
        position, postgres_project_run_request, postgres_runtime_resource, project_run_request,
        quote_identifier, resolve_postgres_resource, resolved_duckdb_destination, run_project,
        run_project_fixture, sample_batch, scope, simple_file_resource, test_execution_services,
        write_compiled_expression_artifacts, write_state_commit_artifacts,
    },
};

pub(super) struct MockProjectDestinationDriver {
    pub(super) destination: MockDestination,
    pub(super) counters: MockDestinationCounters,
}

impl MockProjectDestinationDriver {
    pub(super) fn new(destination: MockDestination, counters: MockDestinationCounters) -> Self {
        Self {
            destination,
            counters,
        }
    }
}

impl ProjectDestinationDriver for MockProjectDestinationDriver {
    fn schemes(&self) -> &'static [&'static str] {
        &["mock"]
    }

    fn inspect(
        &self,
        _uri: &str,
        _context: &ProjectResolutionContext<'_>,
    ) -> Result<cdf_runtime::DestinationInspection> {
        let sheet_artifact = cdf_kernel::DestinationSheetArtifact::new(
            self.destination.sheet.clone(),
            Default::default(),
        )?;
        Ok(cdf_runtime::DestinationInspection {
            description: ProjectDestinationDescription::new(
                self.destination.sheet.destination.clone(),
                &["mock"],
                "mock quasar destination",
            ),
            sheet_artifact_hash: cdf_runtime::artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: cdf_runtime::DestinationRuntimeCapabilities {
                commit_payload_mode: cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming,
                max_in_flight_segments: Some(1),
                max_in_flight_bytes: Some(64 * 1024 * 1024),
                ..Default::default()
            },
            health_probes: vec![cdf_runtime::DestinationHealthProbe {
                probe_id: "mock_ready".to_owned(),
                description: "mock quasar destination readiness".to_owned(),
                requires_credentials: true,
                mutates_destination: false,
            }],
        })
    }

    fn health(
        &self,
        _uri: &str,
        _context: &ProjectResolutionContext<'_>,
    ) -> Result<Vec<cdf_runtime::DestinationHealthResult>> {
        Ok(vec![cdf_runtime::DestinationHealthResult {
            probe_id: "mock_ready".to_owned(),
            status: cdf_runtime::DestinationHealthStatus::Passed,
            message: "mock quasar destination is ready".to_owned(),
            details: Default::default(),
        }])
    }

    fn resolve(
        &self,
        uri: &str,
        _context: &ProjectResolutionContext<'_>,
    ) -> Result<Box<dyn ProjectDestinationRuntime>> {
        if !uri.starts_with("mock:") {
            return Err(CdfError::contract(format!(
                "mock destination driver cannot resolve `{uri}`"
            )));
        }
        self.counters.resolves.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(MockProjectDestinationRuntime::with_destination(
            self.destination.clone(),
            self.counters.clone(),
        )))
    }
}

pub(super) fn reset_postgres_schema(postgres: &LivePostgres) {
    let schema = quote_identifier(&postgres.schema);
    postgres
        .client()
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
        ))
        .unwrap();
}

pub(super) fn postgres_table_exists(postgres: &LivePostgres, table: &str) -> bool {
    postgres
        .client()
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&postgres.schema, &table],
        )
        .unwrap()
        .get(0)
}

pub(super) struct RejectMockStagingSubmissionHost {
    pub(super) inner: Arc<dyn cdf_runtime::ExecutionHost>,
}

pub(super) struct RejectMockStagingSubmissionScope {
    pub(super) inner: Box<dyn cdf_runtime::ExecutionTaskScope>,
}

impl cdf_runtime::ExecutionTaskScope for RejectMockStagingSubmissionScope {
    fn cancellation(&self) -> cdf_runtime::RunCancellation {
        self.inner.cancellation()
    }

    fn spawn_io(&mut self, task: cdf_runtime::IoTask) -> Result<()> {
        self.inner.spawn_io(task)
    }

    fn spawn_cpu(
        &mut self,
        spec: cdf_runtime::CpuTaskSpec,
        task: cdf_runtime::BlockingTask,
    ) -> Result<()> {
        self.inner.spawn_cpu(spec, task)
    }

    fn spawn_cpu_future(
        &mut self,
        spec: cdf_runtime::CpuTaskSpec,
        task: cdf_runtime::CpuFutureTask,
    ) -> Result<()> {
        self.inner.spawn_cpu_future(spec, task)
    }

    fn spawn_blocking(&mut self, lane: &str, task: cdf_runtime::BlockingTask) -> Result<()> {
        if lane == "mock.staged" {
            drop(task);
            return Err(CdfError::internal(
                "injected mock staging task submission failure",
            ));
        }
        self.inner.spawn_blocking(lane, task)
    }

    fn cancel(&self) {
        self.inner.cancel();
    }

    fn join(
        self: Box<Self>,
    ) -> cdf_kernel::BoxFuture<'static, Result<cdf_runtime::TaskScopeReport>> {
        self.inner.join()
    }
}

impl cdf_runtime::ExecutionHost for RejectMockStagingSubmissionHost {
    fn capabilities(&self) -> cdf_runtime::ExecutionHostCapabilities {
        self.inner.capabilities()
    }

    fn memory(&self) -> Arc<dyn cdf_memory::MemoryCoordinator> {
        self.inner.memory()
    }

    fn spill(&self) -> Arc<dyn cdf_runtime::SpillBudgetCoordinator> {
        self.inner.spill()
    }

    fn open_scope(&self, run_id: &str) -> Result<Box<dyn cdf_runtime::ExecutionTaskScope>> {
        Ok(Box::new(RejectMockStagingSubmissionScope {
            inner: self.inner.open_scope(run_id)?,
        }))
    }

    fn run_io_blocking(&self, task: cdf_runtime::IoValueTask) -> Result<cdf_runtime::IoValue> {
        self.inner.run_io_blocking(task)
    }

    fn delay(
        &self,
        duration: std::time::Duration,
        cancellation: cdf_runtime::RunCancellation,
    ) -> cdf_kernel::BoxFuture<'static, Result<()>> {
        self.inner.delay(duration, cancellation)
    }

    fn monotonic_now(&self) -> std::time::Duration {
        self.inner.monotonic_now()
    }

    fn unix_now(&self) -> std::time::Duration {
        self.inner.unix_now()
    }

    fn entropy_u64(&self) -> u64 {
        self.inner.entropy_u64()
    }

    fn ensure_blocking_lanes(&self, lanes: &[cdf_runtime::BlockingLaneSpec]) -> Result<()> {
        self.inner.ensure_blocking_lanes(lanes)
    }

    fn run_blocking_value(
        &self,
        lane: &str,
        task: cdf_runtime::BlockingValueTask,
    ) -> Result<cdf_runtime::IoValue> {
        self.inner.run_blocking_value(lane, task)
    }
}

pub(super) fn rejecting_mock_staging_submission_services() -> cdf_runtime::ExecutionServices {
    let base = test_execution_services();
    let host: Arc<dyn cdf_runtime::ExecutionHost> = Arc::new(RejectMockStagingSubmissionHost {
        inner: Arc::clone(base.host()),
    });
    let scopes: Arc<dyn cdf_kernel::ScopeLeaseStore> = Arc::new(InMemoryScopeLeaseStore::new());
    cdf_runtime::ExecutionServices::new(host)
        .unwrap()
        .with_staging_lease_authority(Arc::new(cdf_runtime::ScopeStagingLeaseAuthority::new(
            scopes,
        )))
        .unwrap()
        .with_content_reachability_store(Arc::new(
            cdf_state_sqlite::SqliteContentReachabilityStore::open_in_memory().unwrap(),
        ))
}
pub(super) fn build_package(
    package_dir: &Path,
    package_id: &str,
    status: PackageStatus,
) -> PackageManifest {
    build_package_with_expression_tuple(package_dir, package_id, status, false)
}

pub(super) fn build_package_for_checkpoint(
    package_dir: &Path,
    package_id: &str,
    status: PackageStatus,
    checkpoint_id: &str,
) -> PackageManifest {
    build_package_with_options(
        package_dir,
        package_id,
        status,
        false,
        WriteDisposition::Append,
        checkpoint_id,
    )
}

pub(super) fn build_package_with_expression_tuple(
    package_dir: &Path,
    package_id: &str,
    status: PackageStatus,
    stale: bool,
) -> PackageManifest {
    build_package_with_options(
        package_dir,
        package_id,
        status,
        stale,
        WriteDisposition::Append,
        "checkpoint-artifact",
    )
}

pub(super) fn build_package_with_options(
    package_dir: &Path,
    package_id: &str,
    status: PackageStatus,
    stale: bool,
    disposition: WriteDisposition,
    checkpoint_id: &str,
) -> PackageManifest {
    build_package_with_options_and_scan_tamper(
        package_dir,
        package_id,
        status,
        stale,
        disposition,
        checkpoint_id,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_package_with_options_and_scan_tamper(
    package_dir: &Path,
    package_id: &str,
    status: PackageStatus,
    stale: bool,
    disposition: WriteDisposition,
    checkpoint_id: &str,
    duplicate_scan_observation: bool,
) -> PackageManifest {
    let builder = PackageBuilder::create(
        package_dir,
        package_id,
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    builder.update_status(PackageStatus::Extracting).unwrap();
    builder
        .write_runtime_arrow_schema(sample_batch(vec![], vec![]).schema().as_ref())
        .unwrap();
    builder
        .write_json_artifact(
            "schema/output.arrow.json",
            &BTreeMap::from([("schema_hash", SCHEMA_HASH)]),
        )
        .unwrap();
    let batches = cdf_package_contract::append_package_row_ord(
        vec![sample_batch(
            vec![1, 2, 3],
            vec![Some("ada"), Some("grace"), None],
        )],
        0,
    )
    .unwrap();
    let segment = builder
        .write_segment(
            cdf_kernel::SegmentId::new("seg-000001").unwrap(),
            0,
            &batches,
        )
        .unwrap();
    builder
        .write_lineage_artifact(
            "lineage.json",
            &canonical_json_bytes(&LineageSummary {
                input_rows: 3,
                input_observations: vec![LineageInputObservation {
                    observation_id: "artifact-fixture".to_owned(),
                    partition_id: PartitionId::new("artifact-fixture").unwrap(),
                    partition_binding: artifact_fixture_partition_binding(),
                    observed_rows: 3,
                    output_position: Some(position(3)),
                }],
            })
            .unwrap(),
        )
        .unwrap();
    write_state_commit_artifacts(&builder, &segment, disposition, checkpoint_id, Vec::new());
    write_compiled_expression_artifacts(&builder, stale, true, None, duplicate_scan_observation);
    builder.finish_with_status(status).unwrap();
    cdf_package::read_manifest(package_dir).unwrap()
}

pub(super) fn build_zero_segment_processed_package(
    package_dir: &Path,
    package_id: &str,
) -> PackageManifest {
    let builder = PackageBuilder::create(
        package_dir,
        package_id,
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    builder.update_status(PackageStatus::Extracting).unwrap();
    builder
        .write_runtime_arrow_schema(sample_batch(vec![], vec![]).schema().as_ref())
        .unwrap();
    builder
        .write_lineage_artifact(
            "lineage.json",
            &canonical_json_bytes(&LineageSummary::default()).unwrap(),
        )
        .unwrap();
    let output_position = SourcePosition::FileManifest(FileManifest {
        version: CHECKPOINT_STATE_VERSION,
        files: vec![FilePosition {
            path: "month-07.parquet".to_owned(),
            size_bytes: 41,
            source_generation: None,
            etag: Some("etag-07".to_owned()),
            object_version: None,
            sha256: Some(format!("sha256:{}", "07".repeat(32))),
        }],
    });
    let processed = ProcessedObservationPosition::new(
        "month-07.parquet",
        ProcessedObservationOutcome::Quarantined,
        output_position.clone(),
    )
    .unwrap();
    let scope = ScopeKey::Resource;
    let state_delta = StateDeltaPreimage {
        checkpoint_id: CheckpointId::new("checkpoint-zero-artifact").unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: output_position.clone(),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        schema_hash: SchemaHash::new(SCHEMA_HASH).unwrap(),
        segments: Vec::new(),
    };
    builder
        .write_json_artifact(
            PROCESSED_OBSERVATIONS_FILE,
            &ProcessedObservationEvidenceArtifact::new(
                None,
                WriteDisposition::Append,
                vec![processed],
                output_position,
            )
            .unwrap(),
        )
        .unwrap();
    let physical_schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let artifact_plan = artifact_expression_plan();
    let constraint = artifact_plan
        .compiled_schema_admission
        .constraint_schema
        .to_arrow()
        .unwrap();
    let reconciliation = cdf_contract::plan_schema_reconciliation(
        physical_schema.as_ref(),
        constraint.as_ref(),
        &artifact_plan.compiled_schema_admission.type_policy,
    )
    .unwrap();
    assert!(!reconciliation.errors.is_empty());
    let fields = reconciliation
        .errors
        .into_iter()
        .map(|error| {
            let observed = physical_schema
                .fields()
                .iter()
                .find(|field| {
                    cdf_kernel::source_name(field.as_ref()).unwrap_or_else(|| field.name())
                        == error.source_name
                })
                .map(|field| cdf_kernel::CanonicalArrowField::from_arrow(field.as_ref()))
                .transpose()?;
            let effective = constraint
                .fields()
                .iter()
                .find(|field| {
                    cdf_kernel::source_name(field.as_ref()).unwrap_or_else(|| field.name())
                        == error.source_name
                })
                .map(|field| cdf_kernel::CanonicalArrowField::from_arrow(field.as_ref()))
                .transpose()?;
            cdf_kernel::SchemaObservationFieldQuarantine::new_field_path(
                vec![error.source_name],
                observed,
                effective,
                error.message,
            )
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let mut quarantine = cdf_kernel::TerminalSchemaObservationQuarantine::new(
        "month-07.parquet",
        physical_hash,
        "schema-observation:incompatible",
        "schema_observation_quarantined",
        cdf_kernel::SchemaObservationPolicy::Evolve,
        "publish a compatible source type, declare an allowed coercion, or repin the schema after review",
        fields,
    )
    .unwrap();
    quarantine
        .bind_source_position(state_delta.output_position.clone())
        .unwrap();
    builder
        .write_json_artifact(
            "quarantine/schema-observations.json",
            &vec![quarantine.clone()],
        )
        .unwrap();
    builder.write_input_checkpoint_artifact(&None).unwrap();
    builder
        .write_state_delta_preimage_artifact(&state_delta)
        .unwrap();
    builder
        .write_commit_plan_preimage_artifact(&DestinationCommitPlanPreimage::package_hash_token(
            TargetName::new("orders").unwrap(),
            WriteDisposition::Append,
            Vec::new(),
            SchemaHash::new(SCHEMA_HASH).unwrap(),
        ))
        .unwrap();
    write_compiled_expression_artifacts(
        &builder,
        false,
        true,
        Some((
            &quarantine,
            cdf_engine::PhysicalObservationEvidence::arrow_schema(physical_schema.as_ref())
                .unwrap(),
        )),
        false,
    );
    builder.finish().unwrap();
    cdf_package::read_manifest(package_dir).unwrap()
}

pub(super) fn artifact_replay_request<'a, Store: CheckpointStore + ?Sized>(
    package_dir: &Path,
    destination: &'a DuckDbDestination,
    checkpoint_store: &'a Store,
) -> PackageArtifactReplayRequest<'a, Store> {
    PackageArtifactReplayRequest {
        package_dir: package_dir.to_path_buf(),
        destination: resolved_duckdb_destination(destination, TargetName::new("orders").unwrap()),
        checkpoint_store,
        after_receipt_verified: None,
    }
}

pub(super) fn recovery_request<'a, Store: CheckpointStore + ?Sized>(
    package_dir: &Path,
    destination: &'a DuckDbDestination,
    checkpoint_store: &'a Store,
    receipt: Receipt,
) -> PackageArtifactRecoveryRequest<'a, Store> {
    PackageArtifactRecoveryRequest {
        package_dir: package_dir.to_path_buf(),
        destination: resolved_duckdb_destination(destination, TargetName::new("orders").unwrap()),
        checkpoint_store,
        receipt,
        after_receipt_verified: None,
    }
}

pub(super) struct MockStagedProjectRuntime {
    pub(super) destination: MockDestination,
    pub(super) fail_stage_after: Option<usize>,
    pub(super) max_in_flight_bytes: u64,
}

impl ProjectDestinationRuntime for MockStagedProjectRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.destination
    }

    fn ingress(&mut self) -> cdf_runtime::DestinationIngress<'_> {
        cdf_runtime::DestinationIngress::StagedSegments(self)
    }

    fn describe(&self) -> ProjectDestinationDescription {
        ProjectDestinationDescription::new(
            self.destination.sheet.destination.clone(),
            &["mock-staged"],
            "mock staged",
        )
    }

    fn runtime_capabilities(&self) -> cdf_runtime::DestinationRuntimeCapabilities {
        let path = mock_bulk_path(
            "mock-staged",
            cdf_runtime::DestinationIngressMode::StagedDurableSegments,
            cdf_runtime::DestinationWriterModel::SingleWriter,
            Some("mock.staged"),
        );
        cdf_runtime::DestinationRuntimeCapabilities {
            blocking_lanes: vec![cdf_runtime::BlockingLaneSpec {
                lane_id: "mock.staged".to_owned(),
                binding: cdf_runtime::BlockingLaneBinding::Static,
                maximum_concurrency: 1,
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
                affinity: cdf_runtime::LaneAffinity::Shared,
                interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
            }],
            staged_ingress_lane: Some("mock.staged".to_owned()),
            ingress_mode: cdf_runtime::DestinationIngressMode::StagedDurableSegments,
            staged_ingress: Some(cdf_runtime::StagedIngressCapabilities {
                recovery: cdf_runtime::StagingRecoveryMode::RollbackRedrive,
                visibility: cdf_runtime::StagingVisibility::IsolatedUntilFinalBinding,
                abort_idempotent: true,
                lifecycle_cleanup: true,
                final_binding_requires_exclusive_writer: false,
            }),
            writer_model: cdf_runtime::DestinationWriterModel::SingleWriter,
            commit_payload_mode: cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming,
            max_in_flight_segments: Some(1),
            max_in_flight_bytes: Some(self.max_in_flight_bytes),
            bulk_paths: vec![path],
            bulk_path: Some("mock-staged".to_owned()),
            bulk_evidence_version: Some("mock-v1".to_owned()),
            ..Default::default()
        }
    }
}

impl cdf_runtime::StagedSegmentIngress for MockStagedProjectRuntime {
    fn begin_staged_ingress(
        &mut self,
        request: cdf_runtime::StagedIngressRequest,
    ) -> Result<Box<dyn cdf_runtime::StagedIngressSession>> {
        Ok(Box::new(MockProjectStagedSession {
            destination: self.destination.clone(),
            request,
            accepted: Vec::new(),
            fail_stage_after: self.fail_stage_after,
        }))
    }

    fn inspect_staged_ingress(
        &mut self,
        _attempt_id: &cdf_runtime::LoadAttemptId,
    ) -> Result<Option<cdf_runtime::StagingSnapshot>> {
        Ok(None)
    }
}

pub(super) struct MockProjectStagedSession {
    pub(super) destination: MockDestination,
    pub(super) request: cdf_runtime::StagedIngressRequest,
    pub(super) accepted: Vec<cdf_runtime::StagedSegmentIdentity>,
    pub(super) fail_stage_after: Option<usize>,
}

impl cdf_runtime::StagedIngressSession for MockProjectStagedSession {
    fn stage_stream(&mut self, stream: &mut dyn cdf_runtime::StagedSegmentStream) -> Result<()> {
        while let Some(mut segment) = stream.next_segment()? {
            self.destination
                .stage_threads
                .lock()
                .unwrap()
                .push(std::thread::current().id());
            if self
                .fail_stage_after
                .is_some_and(|limit| self.accepted.len() >= limit)
            {
                return Err(CdfError::destination("injected staged write failure"));
            }
            while segment.reader_mut().next_batch()?.is_some() {}
            let identity = segment.identity;
            if identity.ordinal != u32::try_from(self.accepted.len()).unwrap() {
                return Err(CdfError::destination(
                    "mock staged integration received noncanonical segment order",
                ));
            }
            self.destination
                .writes
                .lock()
                .unwrap()
                .push(identity.segment_id.clone());
            self.accepted.push(identity.clone());
            stream.acknowledge(cdf_runtime::StagedSegmentAck {
                attempt_id: self.request.attempt_id().clone(),
                identity,
                external_durable: true,
            })?;
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<cdf_runtime::StagingSnapshot> {
        Ok(cdf_runtime::StagingSnapshot {
            attempt_id: self.request.attempt_id().clone(),
            binding: self.request.binding().clone(),
            recovery: cdf_runtime::StagingRecoveryMode::RollbackRedrive,
            accepted_segments: self.accepted.clone(),
        })
    }

    fn bind_final(
        self: Box<Self>,
        binding: cdf_runtime::VerifiedFinalBinding,
    ) -> Result<cdf_runtime::DestinationCommitOutcome> {
        if binding.execution_plan_id() != &self.request.binding().execution_plan_id {
            return Err(CdfError::destination(
                "mock staged final binding changed plan authority",
            ));
        }
        binding.validate_staged_identities(&self.accepted)?;
        let rows_written = self.accepted.iter().map(|item| item.row_count).sum();
        let receipt = Receipt {
            receipt_id: ReceiptId::new(format!(
                "mock-staged-receipt:{}",
                binding.commit().package_hash
            ))?,
            destination: self.destination.sheet.destination.clone(),
            target: binding.commit().target.clone(),
            package_hash: binding.commit().package_hash.clone(),
            segment_acks: self
                .accepted
                .iter()
                .map(|item| SegmentAck {
                    segment_id: item.segment_id.clone(),
                    row_count: item.row_count,
                    byte_count: item.byte_count,
                })
                .collect(),
            disposition: binding.commit().disposition.clone(),
            idempotency_token: binding.commit().idempotency_token.clone(),
            transaction: None,
            counts: CommitCounts {
                rows_written,
                rows_inserted: Some(rows_written),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: binding.schema_hash().clone(),
            migrations: binding.plan().migrations.clone(),
            committed_at_ms: 1_700_000_000_000,
            verify: VerifyClause {
                kind: "mock".to_owned(),
                statement: "mock staged durable receipt".to_owned(),
                parameters: BTreeMap::new(),
            },
        };
        self.destination
            .receipts
            .lock()
            .unwrap()
            .push(receipt.clone());
        Ok(cdf_runtime::DestinationCommitOutcome::new(
            receipt,
            cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommitReceiptOnly,
        ))
    }

    fn abort(self: Box<Self>) -> Result<()> {
        self.destination.aborts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

pub(super) fn package_replay_stage_name(stage: PackageReplayStage<'_>) -> &'static str {
    match stage {
        PackageReplayStage::PackageReplayVerified => "package_replay_verified",
        PackageReplayStage::CheckpointProposed { .. } => "checkpoint_proposed",
        PackageReplayStage::DestinationWriteReady => "destination_write_ready",
        PackageReplayStage::DestinationCommitStarted { .. } => "destination_commit_started",
        PackageReplayStage::DestinationSegmentAcknowledged { .. } => {
            "destination_segment_acknowledged"
        }
        PackageReplayStage::DestinationReceiptRecorded { .. } => "destination_receipt_recorded",
        PackageReplayStage::CheckpointCommitted { .. } => "checkpoint_committed",
        PackageReplayStage::PackageStatusUpdated { .. } => "package_status_updated",
    }
}

pub(super) fn assert_no_head<Store: CheckpointStore>(store: &Store, delta: &StateDelta) {
    assert!(
        store
            .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_none()
    );
}

pub(super) fn assert_head<Store: CheckpointStore>(store: &Store, delta: &StateDelta) -> Checkpoint {
    store
        .head(&delta.pipeline_id, &delta.resource_id, &delta.scope)
        .unwrap()
        .expect("checkpoint head")
}

pub(super) fn remove_package_receipts(package_dir: &Path) {
    let path = package_dir.join(RECEIPTS_FILE);
    if path.exists() {
        fs::remove_file(path).unwrap();
    }
}

pub(super) fn live_plan_for_identifier_rules(
    resource: &dyn QueryableResource,
    package_id: &str,
    rules: &IdentifierRules,
) -> EnginePlan {
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = identifier_policy_from_destination_rules(rules).unwrap();
    live_plan_for_queryable_with_exact_policy(resource, package_id, &policy)
}

pub(super) fn stage_successful_replay(
    package_dir: &Path,
    db_path: &Path,
    checkpoint_id: &str,
) -> (DuckDbDestination, StateDelta, Receipt) {
    let manifest = build_package_for_checkpoint(
        package_dir,
        "pkg-stage",
        PackageStatus::Packaged,
        checkpoint_id,
    );
    let delta = delta(&manifest, checkpoint_id);
    let destination = destination(db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let report =
        replay_package_from_artifacts(artifact_replay_request(package_dir, &destination, &store))
            .unwrap();
    (destination, delta, report.receipt)
}

pub(super) fn assert_bad_reuse_head_rejected(
    package_id: &str,
    checkpoint_id: &str,
    mutate_head: impl FnOnce(&mut Checkpoint),
) {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join(package_id);
    let db_path = temp.path().join("local.duckdb");
    let (destination, delta, receipt) =
        stage_successful_replay(&package_dir, &db_path, checkpoint_id);
    let mut head = Checkpoint {
        delta: delta.clone(),
        status: CheckpointStatus::Committed,
        receipt: Some(receipt.clone()),
        is_head: true,
        created_at_ms: receipt.committed_at_ms,
        committed_at_ms: Some(receipt.committed_at_ms),
        rewind_target_checkpoint_id: None,
    };
    mutate_head(&mut head);
    let store = HeadOnlyCommitFailingStore { head };

    let error = recover_package_from_artifacts(recovery_request(
        &package_dir,
        &destination,
        &store,
        receipt,
    ))
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("injected checkpoint commit failure")
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
}

pub(super) struct CommitFailingStore {
    pub(super) inner: SqliteCheckpointStore,
    pub(super) fail_commit: AtomicBool,
}

pub(super) struct AbandonFailingStore {
    pub(super) inner: SqliteCheckpointStore,
}

impl AbandonFailingStore {
    pub(super) fn new() -> Self {
        Self {
            inner: SqliteCheckpointStore::open_in_memory().unwrap(),
        }
    }
}

impl CheckpointStore for AbandonFailingStore {
    fn propose(&self, delta: StateDelta) -> Result<Checkpoint> {
        self.inner.propose(delta)
    }

    fn commit(&self, checkpoint_id: &CheckpointId, receipt: Receipt) -> Result<Checkpoint> {
        self.inner.commit(checkpoint_id, receipt)
    }

    fn abandon(&self, _checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
        Err(CdfError::internal(
            "injected checkpoint abandonment failure",
        ))
    }

    fn head(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>> {
        self.inner.head(pipeline_id, resource_id, scope)
    }

    fn history(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>> {
        self.inner.history(pipeline_id, resource_id, scope)
    }

    fn rewind(&self, request: RewindRequest) -> Result<RewindReport> {
        self.inner.rewind(request)
    }
}

impl CommitFailingStore {
    pub(super) fn new() -> Self {
        Self {
            inner: SqliteCheckpointStore::open_in_memory().unwrap(),
            fail_commit: AtomicBool::new(true),
        }
    }

    pub(super) fn allow_commit(&self) {
        self.fail_commit.store(false, Ordering::SeqCst);
    }
}

impl CheckpointStore for CommitFailingStore {
    fn propose(&self, delta: StateDelta) -> Result<Checkpoint> {
        self.inner.propose(delta)
    }

    fn commit(&self, checkpoint_id: &CheckpointId, receipt: Receipt) -> Result<Checkpoint> {
        if self.fail_commit.load(Ordering::SeqCst) {
            return Err(CdfError::internal("injected checkpoint commit failure"));
        }
        self.inner.commit(checkpoint_id, receipt)
    }

    fn abandon(&self, checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
        self.inner.abandon(checkpoint_id)
    }

    fn head(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>> {
        self.inner.head(pipeline_id, resource_id, scope)
    }

    fn history(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>> {
        self.inner.history(pipeline_id, resource_id, scope)
    }

    fn rewind(&self, request: RewindRequest) -> Result<RewindReport> {
        self.inner.rewind(request)
    }
}

pub(super) struct HeadOnlyCommitFailingStore {
    pub(super) head: Checkpoint,
}

impl CheckpointStore for HeadOnlyCommitFailingStore {
    fn propose(&self, _delta: StateDelta) -> Result<Checkpoint> {
        Err(CdfError::internal("unexpected propose"))
    }

    fn commit(&self, _checkpoint_id: &CheckpointId, _receipt: Receipt) -> Result<Checkpoint> {
        Err(CdfError::internal("injected checkpoint commit failure"))
    }

    fn abandon(&self, _checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
        Err(CdfError::internal("unexpected abandon"))
    }

    fn head(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>> {
        if &self.head.delta.pipeline_id == pipeline_id
            && &self.head.delta.resource_id == resource_id
            && &self.head.delta.scope == scope
        {
            Ok(Some(self.head.clone()))
        } else {
            Ok(None)
        }
    }

    fn history(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>> {
        Ok(self
            .head(pipeline_id, resource_id, scope)?
            .into_iter()
            .collect())
    }

    fn rewind(&self, _request: RewindRequest) -> Result<RewindReport> {
        Err(CdfError::internal("unexpected rewind"))
    }
}

#[test]
fn live_file_run_post_receipt_failure_keeps_checkpoint_uncommitted_and_receipt_recoverable() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-live-hook-failure";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let pipeline_id = PipelineId::new("pipeline-live").unwrap();
    let hook = |_receipt: &Receipt| Err(CdfError::internal("injected live checkpoint failure"));
    let source = compiled_test_source_plan(&resource);
    let plan = live_plan(&resource, package_id)
        .bind_compiled_source(&source)
        .unwrap();
    let bound = BoundTestResource {
        inner: &resource,
        compiled_source_plan_hash: source.compiled_source_plan_hash().unwrap(),
        replay_retention: None,
    };

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&bound),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: pipeline_id.clone(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-live-hook-failure").unwrap(),
        destination: crate::test_destinations::duckdb(
            duckdb_path.clone(),
            TargetName::new("events").unwrap(),
        )
        .unwrap(),
        run_id: None,
        event_sink: None,
        after_receipt_verified: Some(&hook),
    }))
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("injected live checkpoint failure"),
        "{error}"
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let destination = destination(&duckdb_path);
    assert!(destination.verify_receipt(&receipts[0]).unwrap().verified);

    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let scope = resource.descriptor().state_scope.clone();
    assert!(
        store
            .head(&pipeline_id, &resource.descriptor().resource_id, &scope)
            .unwrap()
            .is_none()
    );
    let history = store
        .history(&pipeline_id, &resource.descriptor().resource_id, &scope)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Proposed);
    assert!(matches!(
        history[0].delta.output_position,
        SourcePosition::FileManifest(_)
    ));
}

#[test]
fn late_data_carryover_survives_the_receipt_to_checkpoint_crash_window() {
    let temp = tempfile::tempdir().unwrap();
    let package_root = temp.path().join("packages");
    let package_id = "pkg-carryover-crash-window";
    let package_dir = package_root.join(package_id);
    let (_manifest, reference) = build_package_with_carryover(&package_dir, package_id);
    let destination = destination(&temp.path().join("destination.duckdb"));
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let hook = |_receipt: &Receipt| Err(CdfError::internal("injected checkpoint crash window"));
    let mut request = artifact_replay_request(&package_dir, &destination, &store);
    request.after_receipt_verified = Some(&hook);

    let error = replay_package_from_artifacts(request).unwrap_err();
    assert!(
        error.message.contains("injected checkpoint crash window"),
        "{error}"
    );
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    assert!(destination.verify_receipt(&receipts[0]).unwrap().verified);

    let recovery = recover_package_from_artifacts(recovery_request(
        &package_dir,
        &destination,
        &store,
        receipts[0].clone(),
    ))
    .unwrap();
    assert_eq!(recovery.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(
        recovery.checkpoint.delta.late_data_carryover,
        vec![reference]
    );
    assert_eq!(
        crate::runtime::load_late_data_carryover(&package_root, Some(&recovery.checkpoint),)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn parquet_artifact_recovery_after_general_run_failure_does_not_need_source() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-parquet-recovery";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let parquet_root = temp.path().join(".cdf/lake");
    let state_path = temp.path().join(".cdf/state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before parquet checkpoint"));
    let mut request = parquet_project_run_request(
        &resource,
        package_id,
        &package_root,
        &parquet_root,
        &state_path,
        "run-general-parquet-recovery",
    );
    request.after_receipt_verified = Some(&hook);
    let initial_error = futures_executor::block_on(run_project(request)).unwrap_err();
    assert!(
        initial_error
            .to_string()
            .contains("stop before parquet checkpoint"),
        "{initial_error}"
    );

    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let target = receipts[0].target.clone();
    let report = recover_package_from_artifacts(PackageArtifactRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: crate::test_destinations::parquet_filesystem(&parquet_root, target).unwrap(),
        checkpoint_store: &store,
        receipt: receipts[0].clone(),
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
}

#[test]
fn parquet_artifact_replay_after_source_loss_without_receipt_commits_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-parquet-artifact-replay";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let parquet_root = temp.path().join(".cdf/lake");
    let replay_root = temp.path().join(".cdf/replay-lake");
    let state_path = temp.path().join(".cdf/state.db");
    let replay_state_path = temp.path().join(".cdf/replay-state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before parquet checkpoint"));
    let mut request = parquet_project_run_request(
        &resource,
        package_id,
        &package_root,
        &parquet_root,
        &state_path,
        "run-general-parquet-artifact-replay",
    );
    request.after_receipt_verified = Some(&hook);
    let initial_error = futures_executor::block_on(run_project(request)).unwrap_err();
    assert!(
        initial_error
            .to_string()
            .contains("stop before parquet checkpoint"),
        "{initial_error}"
    );
    fs::remove_file(temp.path().join("data/events.ndjson")).unwrap();
    remove_package_receipts(&package_dir);
    assert!(package_receipts(&package_dir).is_empty());

    let store = SqliteCheckpointStore::open(&replay_state_path).unwrap();
    let target = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .destination_commit
        .target;
    let report = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: crate::test_destinations::parquet_filesystem(&replay_root, target).unwrap(),
        checkpoint_store: &store,
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: true
        }
    );
    assert_eq!(package_receipts(&package_dir), vec![report.receipt.clone()]);
    assert!(
        ParquetDestination::new_filesystem(&replay_root, test_execution_services())
            .unwrap()
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified
    );
    assert_eq!(
        assert_head(&store, &report.checkpoint.delta)
            .delta
            .checkpoint_id,
        report.checkpoint.delta.checkpoint_id
    );
}

#[test]
fn postgres_artifact_recovery_after_durable_receipt_commits_without_source_contact() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-postgres-recovery";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let state_path = temp.path().join(".cdf/state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before postgres checkpoint"));
    let target = PostgresTarget::new(Some(&postgres.schema), "events_recovery").unwrap();
    let mut request = postgres_project_run_request(
        &resource,
        package_id,
        &package_root,
        &postgres.url,
        target.clone(),
        &state_path,
        "run-general-postgres-recovery",
    );
    request.after_receipt_verified = Some(&hook);
    futures_executor::block_on(run_project(request)).unwrap_err();
    fs::remove_file(temp.path().join("data/events.ndjson")).unwrap();

    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let report = recover_package_from_artifacts(PackageArtifactRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: crate::test_destinations::postgres(postgres.url.clone(), target, None)
            .unwrap(),
        checkpoint_store: &store,
        receipt: receipts[0].clone(),
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
    let mut client = postgres.client();
    let rows: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                postgres.table("events_recovery")
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 2);
}

#[test]
fn postgres_artifact_replay_after_source_loss_without_receipt_commits_checkpoint() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-postgres-artifact-replay";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let state_path = temp.path().join(".cdf/state.db");
    let replay_state_path = temp.path().join(".cdf/replay-state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before postgres checkpoint"));
    let target = PostgresTarget::new(Some(&postgres.schema), "events_artifact_replay").unwrap();
    let mut request = postgres_project_run_request(
        &resource,
        package_id,
        &package_root,
        &postgres.url,
        target.clone(),
        &state_path,
        "run-general-postgres-artifact-replay",
    );
    request.after_receipt_verified = Some(&hook);
    futures_executor::block_on(run_project(request)).unwrap_err();
    fs::remove_file(temp.path().join("data/events.ndjson")).unwrap();
    remove_package_receipts(&package_dir);
    reset_postgres_schema(&postgres);
    assert!(package_receipts(&package_dir).is_empty());

    let store = SqliteCheckpointStore::open(&replay_state_path).unwrap();
    let report = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: crate::test_destinations::postgres(postgres.url.clone(), target, None)
            .unwrap(),
        checkpoint_store: &store,
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommitReceiptOnly {
            package_receipt_recorded: true
        }
    );
    assert_eq!(package_receipts(&package_dir), vec![report.receipt.clone()]);
    assert!(
        PostgresDestination::connect(postgres.url.clone())
            .unwrap()
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified
    );
    assert_eq!(
        assert_head(&store, &report.checkpoint.delta)
            .delta
            .checkpoint_id,
        report.checkpoint.delta.checkpoint_id
    );
    let mut client = postgres.client();
    let rows: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                postgres.table("events_artifact_replay")
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 2);
}

#[test]
fn postgres_artifact_replay_rejects_mismatched_explicit_target_before_mutation() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-postgres-target-mismatch";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let state_path = temp.path().join(".cdf/state.db");
    let replay_state_path = temp.path().join(".cdf/replay-state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before postgres checkpoint"));
    let target = PostgresTarget::new(Some(&postgres.schema), "events_target_match").unwrap();
    let mut request = postgres_project_run_request(
        &resource,
        package_id,
        &package_root,
        &postgres.url,
        target,
        &state_path,
        "run-general-postgres-target-mismatch",
    );
    request.after_receipt_verified = Some(&hook);
    futures_executor::block_on(run_project(request)).unwrap_err();
    fs::remove_file(temp.path().join("data/events.ndjson")).unwrap();
    remove_package_receipts(&package_dir);
    reset_postgres_schema(&postgres);
    let delta = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta;

    let store = SqliteCheckpointStore::open(&replay_state_path).unwrap();
    let wrong_target = PostgresTarget::new(Some(&postgres.schema), "events_target_wrong").unwrap();
    let error = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: crate::test_destinations::postgres(postgres.url.clone(), wrong_target, None)
            .unwrap(),
        checkpoint_store: &store,
        after_receipt_verified: None,
    })
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not match package destination commit target"),
        "{error}"
    );
    assert!(package_receipts(&package_dir).is_empty());
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    assert!(
        store
            .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_empty()
    );
    assert!(!postgres_table_exists(&postgres, "events_target_match"));
    assert!(!postgres_table_exists(&postgres, "events_target_wrong"));
}

#[test]
fn general_project_run_records_failure_after_durable_receipt_without_advancing_state() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-general-run-failed";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("injected general failure"));
    let mut request = project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-general-failed",
    );
    request.after_receipt_verified = Some(&hook);

    let error = futures_executor::block_on(run_project(request)).unwrap_err();

    assert!(error.to_string().contains("injected general failure"));
    let ledger = SqliteRunLedger::open(&state_path).unwrap();
    let snapshot = ledger
        .snapshot(&RunId::new("run-general-failed").unwrap())
        .unwrap()
        .unwrap();
    let kinds = snapshot
        .events
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            RunEventKind::RunStarted,
            RunEventKind::PlanRecorded,
            RunEventKind::PackageStarted,
            RunEventKind::PackageSegmentRecorded,
            RunEventKind::PackageFinalized,
            RunEventKind::ValidationDepthTransitionRecorded,
            RunEventKind::CheckpointProposed,
            RunEventKind::DestinationCommitStarted,
            RunEventKind::DestinationSegmentAcknowledged,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::RunFailed,
        ]
    );
    assert!(
        snapshot
            .events
            .last()
            .unwrap()
            .details
            .attributes
            .contains_key("elapsed_ms")
    );
    assert_eq!(
        snapshot
            .events
            .last()
            .unwrap()
            .details
            .attributes
            .get("error_kind"),
        Some(&RunEventValue::String("internal".to_owned()))
    );

    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let scope = resource.descriptor().state_scope.clone();
    assert!(
        store
            .head(
                &PipelineId::new("pipeline-live").unwrap(),
                &resource.descriptor().resource_id,
                &scope
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let destination = destination(&duckdb_path);
    assert!(destination.verify_receipt(&receipts[0]).unwrap().verified);
}

#[test]
fn package_artifact_recovery_after_general_run_failure_does_not_need_source() {
    let temp = tempfile::tempdir().unwrap();
    let resource = live_file_resource(temp.path());
    let package_id = "pkg-general-recovery";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before checkpoint"));
    let mut request = project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-general-recovery",
    );
    request.after_receipt_verified = Some(&hook);
    futures_executor::block_on(run_project(request)).unwrap_err();

    let destination = destination(&duckdb_path);
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let receipts = package_receipts(&package_dir);
    let report = recover_package_from_artifacts(PackageArtifactRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: resolved_duckdb_destination(&destination, receipts[0].target.clone()),
        checkpoint_store: &store,
        receipt: receipts[0].clone(),
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
}

#[test]
fn generic_lock_plan_replay_and_recovery_drive_mock_runtime_without_destination_branch() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-generic-mock");
    build_package(&package_dir, "pkg-generic-mock", PackageStatus::Packaged);
    let package = PackageReader::open(&package_dir)
        .unwrap()
        .into_verified()
        .unwrap();
    let inputs = package.replay_inputs().unwrap();
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let destination = MockDestination::new();
    let counters = MockDestinationCounters::new();
    let mut registry = ProjectDestinationRegistry::new();
    registry
        .register(MockProjectDestinationDriver::new(
            destination.clone(),
            counters.clone(),
        ))
        .unwrap();
    let target = TargetName::new("orders").unwrap();
    let context = ProjectResolutionContext::for_project_run(temp.path(), &target);
    let inspection = registry
        .inspect(
            "mock://user:quasar-secret@example.invalid/database",
            &context,
        )
        .unwrap();
    assert_eq!(inspection.description.destination_id.as_str(), "mock");
    assert_eq!(inspection.sheet_artifact.sheet.destination.as_str(), "mock");
    assert!(
        inspection
            .health_probes
            .iter()
            .all(|probe| !probe.mutates_destination)
    );
    assert_eq!(destination.write_count(), 0, "inspection must not mutate");
    let lock_config = parse_cdf_toml(
        r#"
[project]
name = "quasar-driver-lock"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = ".cdf/state.db"
packages = ".cdf/packages"
destination = "mock://user:quasar-secret@example.invalid/database"

[resources."mock.*"]
source = "resources/mock.toml"
"#,
    )
    .unwrap();
    let lock = generate_lockfile_with_destination_artifacts(
        &lock_config,
        &[],
        DependencyTuple {
            cdf: "test".to_owned(),
            arrow_rs: "test".to_owned(),
            datafusion: None,
            object_store: None,
            duckdb_rs: None,
            rust: None,
        },
        std::slice::from_ref(&inspection.sheet_artifact),
        BTreeMap::new(),
        &SemanticCatalog::builtins().unwrap(),
    )
    .unwrap();
    assert_eq!(
        lock.destinations["mock"].sheet_artifact().unwrap(),
        inspection.sheet_artifact
    );
    assert_eq!(
        destination.write_count(),
        0,
        "lock generation must not mutate"
    );
    let health = registry
        .health(
            "mock://user:quasar-secret@example.invalid/database",
            &context,
        )
        .unwrap();
    assert_eq!(
        health[0].status,
        cdf_runtime::DestinationHealthStatus::Passed
    );
    assert_eq!(destination.write_count(), 0, "health must not mutate");
    let compiled = postgres_runtime_resource("public.events");
    let execution = test_execution_services();
    let resource = resolve_postgres_resource(
        &compiled,
        "postgres://user:password@example.invalid/database",
        &execution,
    );
    let mut planned_destination =
        resolve_project_run_destination(&registry, "mock://registered", &context).unwrap();
    let mut plan_policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    plan_policy.normalization.identifier = planned_destination
        .column_identifier_policy()
        .unwrap()
        .unwrap();
    let engine_plan = live_plan_with_exact_policy(&resource, "pkg-quasar-plan", &plan_policy);
    let planned = planned_destination
        .plan_resource_commit(&resource, &engine_plan)
        .unwrap();
    assert_eq!(planned.description.destination_id.as_str(), "mock");
    assert_eq!(planned.target, target);
    assert_eq!(destination.write_count(), 0, "planning must not mutate");

    let mut replay_runtime = registry.resolve("mock://registered", &context).unwrap();
    assert_eq!(replay_runtime.secret_redaction(), Some("quasar-secret"));
    let replay_stages = Arc::new(Mutex::new(Vec::new()));
    let replay_stages_hook = Arc::clone(&replay_stages);
    let stage_hook = move |stage: PackageReplayStage<'_>| {
        replay_stages_hook
            .lock()
            .unwrap()
            .push(package_replay_stage_name(stage));
        Ok(())
    };

    let report = replay_package_with_runtime(
        package,
        replay_runtime.as_mut(),
        &store,
        test_execution_services().memory(),
        PackageReplayHooks {
            stage: Some(&stage_hook),
            ..Default::default()
        },
        None,
    )
    .unwrap();

    assert_eq!(counters.resolve_count(), 2);
    assert_eq!(counters.prepare_count(), 1);
    assert_eq!(counters.bind_count(), 1);
    assert_eq!(destination.write_count(), 1);
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: true
        }
    );
    assert!(report.receipt.covers_state_delta(&inputs.state_delta));
    let mut conflicting = report.receipt.clone();
    conflicting.counts.rows_written += 1;
    let conflict =
        record_package_receipt_once(&PackageReader::open(&package_dir).unwrap(), &conflicting)
            .unwrap_err();
    assert!(
        conflict
            .to_string()
            .contains("conflicting logical commit evidence"),
        "{conflict}"
    );
    assert_eq!(
        *replay_stages.lock().unwrap(),
        vec![
            "package_replay_verified",
            "checkpoint_proposed",
            "destination_write_ready",
            "destination_commit_started",
            "destination_segment_acknowledged",
            "destination_receipt_recorded",
            "checkpoint_committed",
            "package_status_updated",
        ]
    );

    let writes_before_recovery = destination.write_count();
    let recovery_destination =
        resolve_project_run_destination(&registry, "mock://registered", &context).unwrap();
    let recovery = recover_package_from_artifacts(PackageArtifactRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: recovery_destination,
        checkpoint_store: &store,
        receipt: report.receipt.clone(),
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(counters.resolve_count(), 3);
    assert_eq!(counters.prepare_count(), 1);
    assert_eq!(counters.bind_count(), 1);
    assert_eq!(destination.write_count(), writes_before_recovery);
    assert_eq!(
        recovery.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
    assert_eq!(recovery.checkpoint, report.checkpoint);
}

#[test]
fn generic_replay_streams_verified_segments_through_staged_final_binding() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-generic-staged");
    build_package(&package_dir, "pkg-generic-staged", PackageStatus::Packaged);
    let package = PackageReader::open(&package_dir)
        .unwrap()
        .into_verified()
        .unwrap();
    let inputs = package.replay_inputs().unwrap();
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let destination = MockDestination::new();
    let mut runtime = MockStagedProjectRuntime {
        destination: destination.clone(),
        fail_stage_after: None,
        max_in_flight_bytes: 64 * 1024 * 1024,
    };
    let stages = Arc::new(Mutex::new(Vec::new()));
    let stage_capture = Arc::clone(&stages);
    let stage_hook = move |stage: PackageReplayStage<'_>| {
        stage_capture
            .lock()
            .unwrap()
            .push(package_replay_stage_name(stage));
        Ok(())
    };

    let execution = test_execution_services();
    let report = replay_package_with_runtime(
        package,
        &mut runtime,
        &store,
        execution.memory(),
        PackageReplayHooks {
            stage: Some(&stage_hook),
            ..Default::default()
        },
        Some(&execution),
    )
    .unwrap();

    assert_eq!(destination.write_count(), inputs.state_delta.segments.len());
    assert!(report.receipt.covers_state_delta(&inputs.state_delta));
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommitReceiptOnly {
            package_receipt_recorded: true
        }
    );
    assert_eq!(
        *stages.lock().unwrap(),
        vec![
            "package_replay_verified",
            "checkpoint_proposed",
            "destination_write_ready",
            "destination_commit_started",
            "destination_segment_acknowledged",
            "destination_receipt_recorded",
            "checkpoint_committed",
            "package_status_updated",
        ]
    );
}

#[test]
fn ordinary_run_stages_each_segment_at_durable_publish_before_final_binding() {
    let temp = tempfile::tempdir().unwrap();
    let resource = multi_file_resource(temp.path());
    let package_id = "pkg-live-staged-overlap";
    let destination = MockDestination::new();
    let run_thread = std::thread::current().id();
    let plan =
        live_plan_for_identifier_rules(&resource, package_id, &destination.sheet.identifier_rules);
    let request = ProjectRunRequest {
        resource: ProjectRunSource::new(&resource),
        plan,
        package_root: temp.path().join(".cdf/packages"),
        state_store_path: temp.path().join(".cdf/state.db"),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-live-staged").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-live-staged").unwrap(),
        destination: ResolvedProjectDestination::new(
            Box::new(MockStagedProjectRuntime {
                destination: destination.clone(),
                fail_stage_after: None,
                max_in_flight_bytes: 64 * 1024 * 1024,
            }),
            TargetName::new("events").unwrap(),
        ),
        run_id: Some(RunId::new("run-live-staged").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    };

    let report = futures_executor::block_on(run_project(request)).unwrap();

    assert_eq!(report.segment_count, 2);
    assert_eq!(
        u64::try_from(destination.write_count()).unwrap(),
        report.segment_count
    );
    assert_eq!(
        u64::try_from(destination.stage_threads().len()).unwrap(),
        report.segment_count
    );
    assert!(
        destination
            .stage_threads()
            .iter()
            .all(|thread| *thread != run_thread)
    );
    assert!(report.receipt.covers_state_delta(&report.checkpoint.delta));
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommitReceiptOnly {
            package_receipt_recorded: true
        }
    );
}

#[test]
fn rejected_background_submission_aborts_staged_session_before_lease_release() {
    let temp = tempfile::tempdir().unwrap();
    let resource = multi_file_resource(temp.path());
    let package_id = "pkg-live-staged-rejected-submission";
    let package_root = temp.path().join(".cdf/packages");
    let destination = MockDestination::new();
    let plan =
        live_plan_for_identifier_rules(&resource, package_id, &destination.sheet.identifier_rules);
    let request = ProjectRunRequest {
        resource: ProjectRunSource::new(&resource),
        plan,
        package_root: package_root.clone(),
        state_store_path: temp.path().join(".cdf/state.db"),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-live-staged-rejected-submission").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-live-staged-rejected-submission").unwrap(),
        destination: ResolvedProjectDestination::new(
            Box::new(MockStagedProjectRuntime {
                destination: destination.clone(),
                fail_stage_after: None,
                max_in_flight_bytes: 64 * 1024 * 1024,
            }),
            TargetName::new("events").unwrap(),
        ),
        run_id: Some(RunId::new("run-live-staged-rejected-submission").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    };
    let services = rejecting_mock_staging_submission_services();

    let error = futures_executor::block_on(run_project_fixture(
        request,
        &services,
        RunTelemetryConfig::disabled(),
    ))
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("injected mock staging task submission failure")
    );
    assert_eq!(destination.write_count(), 0);
    assert_eq!(destination.abort_count(), 1);
    assert!(!package_root.join(package_id).exists());
}

#[test]
fn staged_publish_failure_aborts_attempt_and_never_proposes_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let resource = multi_file_resource(temp.path());
    let package_id = "pkg-live-staged-failure";
    let package_root = temp.path().join(".cdf/packages");
    let state_path = temp.path().join(".cdf/state.db");
    let destination = MockDestination::new();
    let plan =
        live_plan_for_identifier_rules(&resource, package_id, &destination.sheet.identifier_rules);
    let request = ProjectRunRequest {
        resource: ProjectRunSource::new(&resource),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-live-staged-failure").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-live-staged-failure").unwrap(),
        destination: ResolvedProjectDestination::new(
            Box::new(MockStagedProjectRuntime {
                destination: destination.clone(),
                fail_stage_after: Some(1),
                max_in_flight_bytes: 64 * 1024 * 1024,
            }),
            TargetName::new("events").unwrap(),
        ),
        run_id: Some(RunId::new("run-live-staged-failure").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    };

    let error = futures_executor::block_on(run_project(request)).unwrap_err();

    assert!(error.to_string().contains("injected staged write failure"));
    assert_eq!(destination.write_count(), 1);
    assert_eq!(destination.abort_count(), 1);
    assert_eq!(
        package_status(&package_root.join(package_id)),
        PackageStatus::Extracting
    );
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    assert!(
        store
            .head(
                &PipelineId::new("pipeline-live-staged-failure").unwrap(),
                &resource.descriptor().resource_id,
                &resource.descriptor().state_scope,
            )
            .unwrap()
            .is_none()
    );
}

#[test]
fn generic_stage_hook_stops_mock_replay_before_destination_write() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-generic-mock-failpoint");
    build_package(
        &package_dir,
        "pkg-generic-mock-failpoint",
        PackageStatus::Packaged,
    );
    let package = PackageReader::open(&package_dir)
        .unwrap()
        .into_verified()
        .unwrap();
    let inputs = package.replay_inputs().unwrap();
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let destination = MockDestination::new();
    let counters = MockDestinationCounters::new();
    let mut registry = ProjectDestinationRegistry::new();
    registry
        .register(MockProjectDestinationDriver::new(
            destination.clone(),
            counters.clone(),
        ))
        .unwrap();
    let context = ProjectResolutionContext::new();
    let mut runtime = registry
        .resolve("mock://registered-failpoint", &context)
        .unwrap();
    let stage_hook = |stage: PackageReplayStage<'_>| {
        if matches!(stage, PackageReplayStage::DestinationWriteReady) {
            return Err(CdfError::internal("stop at generic destination write hook"));
        }
        Ok(())
    };

    let error = replay_package_with_runtime(
        package,
        runtime.as_mut(),
        &store,
        test_execution_services().memory(),
        PackageReplayHooks {
            stage: Some(&stage_hook),
            ..Default::default()
        },
        None,
    )
    .unwrap_err();

    assert!(error.to_string().contains("generic destination write"));
    assert_eq!(counters.resolve_count(), 1);
    assert_eq!(counters.prepare_count(), 0);
    assert_eq!(counters.bind_count(), 0);
    assert_eq!(destination.write_count(), 0);
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let history = store
        .history(
            &inputs.state_delta.pipeline_id,
            &inputs.state_delta.resource_id,
            &inputs.state_delta.scope,
        )
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Proposed);
}

#[test]
fn replay_commits_duckdb_receipt_then_checkpoint_and_marks_package_checkpointed() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-success");
    let manifest = build_package_for_checkpoint(
        &package_dir,
        "pkg-success",
        PackageStatus::Packaged,
        "checkpoint-success",
    );
    let delta = delta(&manifest, "checkpoint-success");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let report =
        replay_package_from_artifacts(artifact_replay_request(&package_dir, &destination, &store))
            .unwrap();

    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(
        assert_head(&store, &delta).delta.checkpoint_id,
        delta.checkpoint_id
    );
    assert_eq!(report.receipt.package_hash, delta.package_hash);
    assert_eq!(
        report.receipt.idempotency_token.as_str(),
        delta.package_hash.as_str()
    );
    assert_eq!(
        report.receipt.segment_acks[0].byte_count,
        delta.segments[0].byte_count
    );
    assert!(
        destination
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified
    );
    assert_eq!(package_receipts(&package_dir), vec![report.receipt.clone()]);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: true
        }
    );
    assert_eq!(
        report
            .phase_metrics
            .iter()
            .map(|metric| metric.phase)
            .collect::<Vec<_>>(),
        vec![
            RunPhase::DestinationWriteReceipt,
            RunPhase::CheckpointGate,
            RunPhase::PackageExecution,
        ]
    );
    assert!(
        report
            .phase_metrics
            .iter()
            .all(|metric| metric.status == RunPhaseStatus::Completed)
    );
    assert_eq!(
        report.phase_metrics[0].input_bytes,
        delta.segments[0].byte_count
    );
    assert_eq!(report.phase_metrics[0].operations, 1);
    assert_eq!(report.phase_metrics[1].operations, 1);
    assert_eq!(report.phase_metrics[2].operations, 1);
}

#[test]
fn artifact_replay_reconstructs_delta_and_commit_request_from_package_files() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-artifact-success");
    let manifest = build_package(
        &package_dir,
        "pkg-artifact-success",
        PackageStatus::Packaged,
    );
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let report =
        replay_package_from_artifacts(artifact_replay_request(&package_dir, &destination, &store))
            .unwrap();

    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(
        report.checkpoint.delta.checkpoint_id.as_str(),
        "checkpoint-artifact"
    );
    assert_eq!(
        report.checkpoint.delta.package_hash.as_str(),
        manifest.package_hash
    );
    assert_eq!(
        report.receipt.idempotency_token.as_str(),
        manifest.package_hash
    );
    assert_head(&store, &report.checkpoint.delta);
    assert_eq!(package_receipts(&package_dir), vec![report.receipt.clone()]);
}

#[test]
fn artifact_replay_rejects_duplicate_scan_observation_before_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-artifact-duplicate-scan-observation");
    build_package_with_options_and_scan_tamper(
        &package_dir,
        "pkg-artifact-duplicate-scan-observation",
        PackageStatus::Packaged,
        false,
        WriteDisposition::Append,
        "checkpoint-artifact",
        true,
    );
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let error =
        replay_package_from_artifacts(artifact_replay_request(&package_dir, &destination, &store))
            .unwrap_err();

    assert!(
        error.to_string().contains("assigned to planned partitions"),
        "{error}"
    );
    assert!(!db_path.exists());
    assert!(
        store
            .history(
                &PipelineId::new("pipeline-1").unwrap(),
                &ResourceId::new("orders").unwrap(),
                &scope(),
            )
            .unwrap()
            .is_empty()
    );
}

#[test]
fn artifact_replay_rejects_corrupted_or_missing_preimages_before_mutation() {
    for path in [
        STATE_INPUT_CHECKPOINT_FILE,
        STATE_PROPOSED_DELTA_FILE,
        DESTINATION_COMMIT_PLAN_FILE,
        "plan/schema-admission.json",
        "schema/stream-admission-evidence.json",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let package_dir = temp
            .path()
            .join(format!("pkg-artifact-tampered-{}", path.replace('/', "-")));
        build_package(
            &package_dir,
            "pkg-artifact-tampered",
            PackageStatus::Packaged,
        );
        fs::write(package_dir.join(path), b"{\"tampered\":true}").unwrap();
        let db_path = temp.path().join("local.duckdb");
        let duckdb = destination(&db_path);
        let store = SqliteCheckpointStore::open_in_memory().unwrap();

        let error =
            replay_package_from_artifacts(artifact_replay_request(&package_dir, &duckdb, &store))
                .unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&format!("tampered identity file {path}")),
            "{path}: {error}"
        );
        assert!(
            store
                .history(
                    &PipelineId::new("pipeline-1").unwrap(),
                    &ResourceId::new("orders").unwrap(),
                    &scope()
                )
                .unwrap()
                .is_empty()
        );
        assert!(!db_path.exists());

        let temp = tempfile::tempdir().unwrap();
        let package_dir = temp
            .path()
            .join(format!("pkg-artifact-missing-{}", path.replace('/', "-")));
        build_package(
            &package_dir,
            "pkg-artifact-missing",
            PackageStatus::Packaged,
        );
        fs::remove_file(package_dir.join(path)).unwrap();
        let db_path = temp.path().join("local.duckdb");
        let duckdb = destination(&db_path);
        let store = SqliteCheckpointStore::open_in_memory().unwrap();

        let error =
            replay_package_from_artifacts(artifact_replay_request(&package_dir, &duckdb, &store))
                .unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&format!("missing identity file {path}")),
            "{path}: {error}"
        );
        assert!(
            store
                .history(
                    &PipelineId::new("pipeline-1").unwrap(),
                    &ResourceId::new("orders").unwrap(),
                    &scope()
                )
                .unwrap()
                .is_empty()
        );
        assert!(!db_path.exists());
    }
}

#[test]
fn artifact_replay_rejects_manifest_package_hash_mismatch_before_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-artifact-hash-mismatch");
    build_package(
        &package_dir,
        "pkg-artifact-hash-mismatch",
        PackageStatus::Packaged,
    );
    let mut manifest = cdf_package::read_manifest(&package_dir).unwrap();
    manifest.package_hash = "sha256:wrong-package".to_owned();
    manifest.signature.signing_input = manifest.package_hash.clone();
    fs::write(
        package_dir.join(MANIFEST_FILE),
        canonical_json_bytes(&manifest).unwrap(),
    )
    .unwrap();
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let error =
        replay_package_from_artifacts(artifact_replay_request(&package_dir, &destination, &store))
            .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("manifest identity hash mismatch")
    );
    assert!(
        store
            .history(
                &PipelineId::new("pipeline-1").unwrap(),
                &ResourceId::new("orders").unwrap(),
                &scope()
            )
            .unwrap()
            .is_empty()
    );
    assert!(!db_path.exists());
}

#[test]
fn artifact_replay_rejects_stale_compiled_expression_plan_before_destination_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-stale-compiled-expression-plan");
    build_package_with_expression_tuple(
        &package_dir,
        "pkg-stale-compiled-expression-plan",
        PackageStatus::Packaged,
        true,
    );
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let error =
        replay_package_from_artifacts(artifact_replay_request(&package_dir, &destination, &store))
            .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("expression compatibility tuple is not supported"),
        "{error}"
    );
    assert!(
        store
            .history(
                &PipelineId::new("pipeline-1").unwrap(),
                &ResourceId::new("orders").unwrap(),
                &scope()
            )
            .unwrap()
            .is_empty()
    );
    assert!(!db_path.exists());
}

#[test]
fn duplicate_destination_replay_returns_duplicate_receipt_and_commits_pinned_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-duplicate");
    let db_path = temp.path().join("local.duckdb");
    let (destination, first_delta, first_receipt) =
        stage_successful_replay(&package_dir, &db_path, "checkpoint-first");
    let second_store = SqliteCheckpointStore::open_in_memory().unwrap();

    let report = replay_package_from_artifacts(artifact_replay_request(
        &package_dir,
        &destination,
        &second_store,
    ))
    .unwrap();

    assert_eq!(report.receipt.receipt_id, first_receipt.receipt_id);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: true,
            package_receipt_recorded: false
        }
    );
    assert_eq!(
        assert_head(&second_store, &first_delta).delta.checkpoint_id,
        first_delta.checkpoint_id
    );
    let snapshot = destination.read_mirror_snapshot_read_only().unwrap();
    assert_eq!(snapshot.loads.len(), 1);
    assert_eq!(snapshot.state.len(), 1);
}

#[test]
fn logical_receipt_replay_to_second_physical_destination_keeps_one_package_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-second-physical-destination");
    build_package(
        &package_dir,
        "pkg-second-physical-destination",
        PackageStatus::Packaged,
    );
    let first_destination = destination(&temp.path().join("first.duckdb"));
    let first_store = SqliteCheckpointStore::open_in_memory().unwrap();
    let first = replay_package_from_artifacts(artifact_replay_request(
        &package_dir,
        &first_destination,
        &first_store,
    ))
    .unwrap();

    let second_destination = destination(&temp.path().join("second.duckdb"));
    let second_store = SqliteCheckpointStore::open_in_memory().unwrap();
    let second = replay_package_from_artifacts(artifact_replay_request(
        &package_dir,
        &second_destination,
        &second_store,
    ))
    .unwrap();

    assert_eq!(second.receipt.receipt_id, first.receipt.receipt_id);
    assert_ne!(second.receipt.transaction, first.receipt.transaction);
    assert_eq!(second.checkpoint.receipt, Some(second.receipt.clone()));
    assert_eq!(
        second.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: false
        }
    );
    assert_eq!(package_receipts(&package_dir), vec![first.receipt]);
}

#[test]
fn verified_destination_receipt_before_package_record_replays_idempotently() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-unsettled-receipt");
    let manifest = build_package_for_checkpoint(
        &package_dir,
        "pkg-unsettled-receipt",
        PackageStatus::Packaged,
        "checkpoint-unsettled-receipt",
    );
    let delta = delta(&manifest, "checkpoint-unsettled-receipt");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let stop_before_record = |_receipt: &Receipt| {
        Err(CdfError::internal(
            "injected failure before package receipt persistence",
        ))
    };
    let package = PackageReader::open(&package_dir)
        .unwrap()
        .into_verified()
        .unwrap();
    let execution = test_execution_services();
    let mut resolved =
        resolved_duckdb_destination(&destination, TargetName::new("orders").unwrap());

    let error = replay_package_with_runtime(
        package,
        resolved.runtime_mut(),
        &store,
        execution.memory(),
        PackageReplayHooks {
            before_package_receipt_recorded: Some(&stop_before_record),
            ..Default::default()
        },
        Some(&execution),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected failure before package receipt persistence"),
        "{error}"
    );
    assert!(package_receipts(&package_dir).is_empty());
    assert_no_head(&store, &delta);
    let history = store
        .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Proposed);
    let loads_after_interruption = destination
        .read_mirror_snapshot_read_only()
        .unwrap()
        .loads
        .len();
    assert_eq!(loads_after_interruption, 1);

    let report =
        replay_package_from_artifacts(artifact_replay_request(&package_dir, &destination, &store))
            .unwrap();
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(package_receipts(&package_dir), vec![report.receipt.clone()]);
    assert_eq!(
        destination
            .read_mirror_snapshot_read_only()
            .unwrap()
            .loads
            .len(),
        loads_after_interruption,
        "idempotent redrive must not create a second destination load"
    );
}

#[test]
fn checkpoint_abandonment_failure_is_attached_to_primary_replay_failure() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-abandonment-failure");
    build_package_for_checkpoint(
        &package_dir,
        "pkg-abandonment-failure",
        PackageStatus::Packaged,
        "checkpoint-abandonment-failure",
    );
    let package = PackageReader::open(&package_dir)
        .unwrap()
        .into_verified()
        .unwrap();
    let destination = MockDestination::new().with_begin_failure();
    let counters = MockDestinationCounters::new();
    let store = AbandonFailingStore::new();
    let execution = test_execution_services();
    let mut runtime =
        MockProjectDestinationRuntime::with_destination(destination.clone(), counters);

    let error = replay_package_with_runtime(
        package,
        &mut runtime,
        &store,
        execution.memory(),
        PackageReplayHooks::default(),
        Some(&execution),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("injected primary replay failure"),
        "{message}"
    );
    assert!(
        message.contains("checkpoint abandonment also failed"),
        "{message}"
    );
    assert!(
        message.contains("injected checkpoint abandonment failure"),
        "{message}"
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    assert!(package_receipts(&package_dir).is_empty());
    assert_eq!(destination.write_count(), 0);
}

#[test]
fn recovery_verifies_durable_receipt_and_commits_without_new_destination_write() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-recovery");
    let manifest = build_package_for_checkpoint(
        &package_dir,
        "pkg-recovery",
        PackageStatus::Packaged,
        "checkpoint-recovery",
    );
    let delta = delta(&manifest, "checkpoint-recovery");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before checkpoint commit"));
    let mut request = artifact_replay_request(&package_dir, &destination, &store);
    request.after_receipt_verified = Some(&hook);

    let error = replay_package_from_artifacts(request).unwrap_err();
    assert!(error.to_string().contains("stop before checkpoint commit"));
    assert_no_head(&store, &delta);
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    let loads_before = destination
        .read_mirror_snapshot_read_only()
        .unwrap()
        .loads
        .len();

    let report = recover_package_from_artifacts(recovery_request(
        &package_dir,
        &destination,
        &store,
        receipts[0].clone(),
    ))
    .unwrap();

    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        destination
            .read_mirror_snapshot_read_only()
            .unwrap()
            .loads
            .len(),
        loads_before
    );
}

#[test]
fn zero_segment_processed_package_recovers_after_receipt_without_source_or_data_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-zero-recovery");
    build_zero_segment_processed_package(&package_dir, "pkg-zero-recovery");
    let db_path = temp.path().join("local.duckdb");
    let state_path = temp.path().join("state.db");
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    let hook = |_receipt: &Receipt| Err(CdfError::internal("stop before zero checkpoint"));

    let error = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: crate::test_destinations::duckdb(&db_path, TargetName::new("orders").unwrap())
            .unwrap(),
        checkpoint_store: &store,
        after_receipt_verified: Some(&hook),
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("stop before zero checkpoint"),
        "{error}"
    );
    let reader = PackageReader::open(&package_dir).unwrap();
    let inputs = reader.replay_inputs().unwrap();
    assert!(inputs.state_delta.segments.is_empty());
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    assert!(receipts[0].segment_acks.is_empty());
    assert!(
        store
            .head(
                &inputs.state_delta.pipeline_id,
                &inputs.state_delta.resource_id,
                &inputs.state_delta.scope,
            )
            .unwrap()
            .is_none()
    );

    let recovered = recover_package_from_artifacts(PackageArtifactRecoveryRequest {
        package_dir: package_dir.clone(),
        destination: crate::test_destinations::duckdb(&db_path, TargetName::new("orders").unwrap())
            .unwrap(),
        checkpoint_store: &store,
        receipt: receipts[0].clone(),
        after_receipt_verified: None,
    })
    .unwrap();
    assert_eq!(recovered.checkpoint.status, CheckpointStatus::Committed);
    assert!(recovered.checkpoint.delta.segments.is_empty());
    assert_eq!(
        recovered.checkpoint.delta.output_position,
        inputs.state_delta.output_position
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);

    let mirrors = DuckDbDestination::new(&db_path)
        .unwrap()
        .read_mirror_snapshot_read_only()
        .unwrap();
    assert_eq!(mirrors.loads.len(), 1);
    assert!(mirrors.state.is_empty());
}

#[test]
fn named_failpoint_after_checkpoint_proposal_stops_before_destination_write() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-after-proposal");
    let manifest = build_package_for_checkpoint(
        &package_dir,
        "pkg-after-proposal",
        PackageStatus::Packaged,
        "checkpoint-after-proposal",
    );
    let delta = delta(&manifest, "checkpoint-after-proposal");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let hook = |stage: RuntimeStage<'_>| {
        if matches!(stage, RuntimeStage::DestinationWriteReady) {
            return Err(CdfError::internal("stop after checkpoint proposal"));
        }
        Ok(())
    };

    let error = replay_package_from_artifacts_with_stage_hook(
        artifact_replay_request(&package_dir, &destination, &store),
        Some(&hook),
    )
    .unwrap_err();

    assert!(error.to_string().contains("stop after checkpoint proposal"));
    assert!(!db_path.exists());
    assert!(package_receipts(&package_dir).is_empty());
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    assert_no_head(&store, &delta);
    let history = store
        .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Proposed);
}

#[test]
fn named_failpoint_after_checkpoint_commit_allows_status_only_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-after-checkpoint");
    let manifest = build_package_for_checkpoint(
        &package_dir,
        "pkg-after-checkpoint",
        PackageStatus::Packaged,
        "checkpoint-after-checkpoint",
    );
    let delta = delta(&manifest, "checkpoint-after-checkpoint");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let hook = |stage: RuntimeStage<'_>| {
        if let RuntimeStage::CheckpointCommitted { checkpoint } = stage {
            assert!(checkpoint.receipt.is_some());
            return Err(CdfError::internal("stop after checkpoint commit"));
        }
        Ok(())
    };

    let error = replay_package_from_artifacts_with_stage_hook(
        artifact_replay_request(&package_dir, &destination, &store),
        Some(&hook),
    )
    .unwrap_err();

    assert!(error.to_string().contains("stop after checkpoint commit"));
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let head = assert_head(&store, &delta);
    assert_eq!(head.status, CheckpointStatus::Committed);
    assert_eq!(head.delta, delta);
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    assert!(destination.verify_receipt(&receipts[0]).unwrap().verified);
    let snapshot_before = destination.read_mirror_snapshot_read_only().unwrap();

    let report = recover_package_from_artifacts(recovery_request(
        &package_dir,
        &destination,
        &store,
        receipts[0].clone(),
    ))
    .unwrap();

    assert_eq!(report.checkpoint, head);
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::SuppliedDurableReceipt
    );
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(
        destination.read_mirror_snapshot_read_only().unwrap(),
        snapshot_before
    );
}

#[test]
fn recovery_reuses_only_exact_committed_checkpoint_head() {
    assert_bad_reuse_head_rejected(
        "pkg-reuse-proposed-head",
        "checkpoint-reuse-proposed-head",
        |head| {
            head.status = CheckpointStatus::Proposed;
        },
    );
    assert_bad_reuse_head_rejected("pkg-reuse-non-head", "checkpoint-reuse-non-head", |head| {
        head.is_head = false;
    });
    assert_bad_reuse_head_rejected(
        "pkg-reuse-wrong-delta",
        "checkpoint-reuse-wrong-delta",
        |head| {
            head.delta.checkpoint_id = CheckpointId::new("checkpoint-other-head").unwrap();
        },
    );
    assert_bad_reuse_head_rejected(
        "pkg-reuse-missing-receipt",
        "checkpoint-reuse-missing-receipt",
        |head| {
            head.receipt = None;
        },
    );
}

#[test]
fn recovery_rejects_receipt_verification_failure_without_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-verification-failure");
    let db_path = temp.path().join("local.duckdb");
    let (destination, staged_delta, mut receipt) =
        stage_successful_replay(&package_dir, &db_path, "checkpoint-staged");
    receipt.committed_at_ms += 1;
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let error = recover_package_from_artifacts(recovery_request(
        &package_dir,
        &destination,
        &store,
        receipt,
    ))
    .unwrap_err();

    assert!(error.to_string().contains("did not verify"));
    assert_no_head(&store, &staged_delta);
}

#[test]
fn recovery_rejects_bad_receipt_identity_without_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-bad-identity");
    let db_path = temp.path().join("local.duckdb");
    let (destination, staged_delta, mut receipt) =
        stage_successful_replay(&package_dir, &db_path, "checkpoint-staged");
    receipt.idempotency_token = IdempotencyToken::new("different-token").unwrap();
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let error = recover_package_from_artifacts(recovery_request(
        &package_dir,
        &destination,
        &store,
        receipt,
    ))
    .unwrap_err();

    assert!(error.to_string().contains("idempotency token"));
    assert_no_head(&store, &staged_delta);
}

#[test]
fn recovery_rejects_missing_segment_ack_without_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-missing-ack");
    let db_path = temp.path().join("local.duckdb");
    let (destination, staged_delta, mut receipt) =
        stage_successful_replay(&package_dir, &db_path, "checkpoint-staged");
    receipt.segment_acks.clear();
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let error = recover_package_from_artifacts(recovery_request(
        &package_dir,
        &destination,
        &store,
        receipt,
    ))
    .unwrap_err();

    assert!(error.to_string().contains("acknowledges 0 segment"));
    assert_no_head(&store, &staged_delta);
}

#[test]
fn replay_rejects_non_replayable_package_before_checkpoint_or_destination_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-not-replayable");
    let manifest = build_package_for_checkpoint(
        &package_dir,
        "pkg-not-replayable",
        PackageStatus::Validated,
        "checkpoint-not-replayable",
    );
    let delta = delta(&manifest, "checkpoint-not-replayable");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let error =
        replay_package_from_artifacts(artifact_replay_request(&package_dir, &destination, &store))
            .unwrap_err();

    assert!(error.to_string().contains("not replayable"));
    assert_eq!(package_status(&package_dir), PackageStatus::Validated);
    assert!(
        store
            .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()
            .is_empty()
    );
    assert!(!db_path.exists());
}

#[test]
fn destination_failure_before_receipt_abandons_proposed_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-destination-failure");
    build_package_with_options(
        &package_dir,
        "pkg-destination-failure",
        PackageStatus::Packaged,
        false,
        WriteDisposition::CdcApply,
        "checkpoint-destination-failure",
    );
    let delta = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta;
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = SqliteCheckpointStore::open_in_memory().unwrap();
    let error =
        replay_package_from_artifacts(artifact_replay_request(&package_dir, &destination, &store))
            .unwrap_err();

    assert!(
        error.to_string().contains("does not support cdc_apply"),
        "{error}"
    );
    let history = store
        .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, CheckpointStatus::Abandoned);
    assert_no_head(&store, &delta);
    assert!(package_receipts(&package_dir).is_empty());
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
}

#[test]
fn checkpoint_failure_after_receipt_keeps_receipt_recoverable_and_state_unadvanced() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-checkpoint-failure");
    let manifest = build_package_for_checkpoint(
        &package_dir,
        "pkg-checkpoint-failure",
        PackageStatus::Packaged,
        "checkpoint-fails-once",
    );
    let delta = delta(&manifest, "checkpoint-fails-once");
    let db_path = temp.path().join("local.duckdb");
    let destination = destination(&db_path);
    let store = CommitFailingStore::new();

    let error =
        replay_package_from_artifacts(artifact_replay_request(&package_dir, &destination, &store))
            .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("injected checkpoint commit failure")
    );
    assert_no_head(&store, &delta);
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
    let receipts = package_receipts(&package_dir);
    assert_eq!(receipts.len(), 1);
    assert!(destination.verify_receipt(&receipts[0]).unwrap().verified);
    assert!(matches!(
        store
            .history(&delta.pipeline_id, &delta.resource_id, &delta.scope)
            .unwrap()[0]
            .status,
        CheckpointStatus::Proposed
    ));

    store.allow_commit();
    let report = recover_package_from_artifacts(recovery_request(
        &package_dir,
        &destination,
        &store,
        receipts[0].clone(),
    ))
    .unwrap();

    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(
        assert_head(&store, &delta).delta.checkpoint_id,
        delta.checkpoint_id
    );
}

#[test]
fn recovery_refuses_receipts_not_covering_state_delta_counts() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("pkg-wrong-counts");
    let db_path = temp.path().join("local.duckdb");
    let (destination, staged_delta, mut receipt) =
        stage_successful_replay(&package_dir, &db_path, "checkpoint-staged");
    receipt.segment_acks[0].row_count += 1;
    let store = SqliteCheckpointStore::open_in_memory().unwrap();

    let error = recover_package_from_artifacts(recovery_request(
        &package_dir,
        &destination,
        &store,
        receipt,
    ))
    .unwrap_err();

    assert!(error.to_string().contains("StateDelta has"));
    assert_no_head(&store, &staged_delta);
}
