pub(super) use super::*;

pub(super) fn collect_quarantine_records(
    reader: &cdf_package::PackageReader,
) -> Vec<cdf_package_contract::QuarantineRecord> {
    let mut records = Vec::new();
    reader
        .for_each_quarantine_record(&mut |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
    records
}

pub(super) fn read_package_segment(
    reader: &cdf_package::PackageReader,
    segment_id: &cdf_kernel::SegmentId,
) -> Vec<RecordBatch> {
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(128 * 1024 * 1024, BTreeMap::new())
            .unwrap(),
    );
    reader
        .verified_canonical_segment_stream(memory, 128 * 1024 * 1024)
        .unwrap()
        .find_map(|segment| {
            let segment = segment.unwrap();
            (segment.entry.segment_id == *segment_id).then_some(segment.batches)
        })
        .unwrap_or_else(|| panic!("segment {segment_id} is not in the verified package"))
}

fn executable_mock_plan(plan: &EnginePlan, resource: &MockResource) -> Result<EnginePlan> {
    if plan.compiled_source_execution.is_some() {
        return Ok(plan.clone());
    }
    let source = match resource.compiled_source_plan.get() {
        Some(source) => source.clone(),
        None => {
            let source = mock_compiled_source_plan(resource, None);
            resource.bind_compiled_source(&source);
            source
        }
    };
    plan.clone().bind_compiled_source(&source)
}

pub(super) fn executable_mock_options(
    config: EngineExecutionConfig,
) -> Result<EngineExecutionInvocation> {
    let config = if config.services.is_some() {
        config
    } else {
        let (_, services) =
            StandaloneExecutionHost::default_services(cdf_memory::DEFAULT_PROCESS_BUDGET_BYTES)?;
        config.with_execution_services(services)
    };
    Ok(config.new_invocation())
}

pub(super) async fn execute_to_package(
    plan: &EnginePlan,
    resource: &MockResource,
    package_dir: impl AsRef<std::path::Path>,
) -> Result<EngineRunOutput> {
    let plan = executable_mock_plan(plan, resource)?;
    super::execute_to_package(&plan, resource, package_dir).await
}

pub(super) async fn preview_resource(
    plan: &EnginePlan,
    resource: &MockResource,
    limits: EnginePreviewLimits,
) -> Result<EnginePreviewOutput> {
    let plan = executable_mock_plan(plan, resource)?;
    super::preview_resource(&plan, resource, limits).await
}

pub(super) async fn execute_to_package_with_run_id(
    run_id: &RunId,
    plan: &EnginePlan,
    resource: &MockResource,
    package_dir: impl AsRef<std::path::Path>,
) -> Result<EngineRunOutput> {
    let plan = executable_mock_plan(plan, resource)?;
    super::execute_to_package_with_run_id(run_id, &plan, resource, package_dir).await
}

pub(super) async fn execute_to_package_with_segment_positions(
    plan: &EnginePlan,
    resource: &MockResource,
    package_dir: impl AsRef<std::path::Path>,
) -> Result<EngineRunOutputWithSegmentPositions> {
    let plan = executable_mock_plan(plan, resource)?;
    super::execute_to_package_with_segment_positions(&plan, resource, package_dir).await
}

pub(super) async fn execute_to_package_with_segment_positions_and_pre_finalize(
    plan: &EnginePlan,
    resource: &MockResource,
    package_dir: impl AsRef<std::path::Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
    options: EngineExecutionConfig,
) -> Result<EngineRunOutputWithSegmentPositions> {
    let plan = executable_mock_plan(plan, resource)?;
    let options = executable_mock_options(options)?;
    super::execute_to_package_with_segment_positions_and_pre_finalize(
        &plan,
        resource,
        package_dir,
        pre_finalize,
        options,
    )
    .await
}

pub(super) async fn execute_to_package_with_streaming_hooks<'a>(
    plan: &EnginePlan,
    resource: &MockResource,
    package_dir: impl AsRef<std::path::Path>,
    pre_finalize: &PackagePreFinalizeHook<'_>,
    durable_segment: &'a mut DurableSegmentHook<'a>,
    stream_finalize: &'a mut StreamingFinalizeHook<'a>,
    options: EngineExecutionConfig,
) -> Result<EngineRunOutputWithSegmentPositions> {
    let plan = executable_mock_plan(plan, resource)?;
    let options = executable_mock_options(options)?;
    super::execute_to_package_with_streaming_hooks(
        &plan,
        resource,
        package_dir,
        pre_finalize,
        durable_segment,
        stream_finalize,
        options,
    )
    .await
}

pub(super) fn terminal_effective_schema_runtime(
    physical_schema: SchemaRef,
    physical_hash: SchemaHash,
) -> EffectiveSchemaRuntime {
    let authority_plan = Planner::new()
        .plan_tier_b(
            &MockResource::tier_b(Vec::new()),
            plan_input(Vec::new(), None, None, ExecutionExtent::bounded()),
        )
        .unwrap();
    let CompiledSchemaAdmissionOutcome::Quarantined(terminal_0) = authority_plan
        .compiled_schema_admission
        .instantiate_or_quarantine("input-0", physical_schema.as_ref(), &physical_hash)
        .unwrap()
    else {
        panic!("incompatible fixture must compile to terminal quarantine");
    };
    let CompiledSchemaAdmissionOutcome::Quarantined(terminal_1) = authority_plan
        .compiled_schema_admission
        .instantiate_or_quarantine("input-1", physical_schema.as_ref(), &physical_hash)
        .unwrap()
    else {
        panic!("incompatible fixture must compile to terminal quarantine");
    };
    let evidence = bound_effective_schema_evidence(
        SchemaHash::new("effective-snapshot-v1").unwrap(),
        "manifest-v1",
        ".cdf/schemas/orders@manifest-v1.discovery.json",
        vec![
            EffectiveSchemaObservationEvidence::new(
                "input-0",
                physical_hash.clone(),
                schema_observation_binding("input-0"),
            ),
            EffectiveSchemaObservationEvidence::new(
                "input-1",
                physical_hash.clone(),
                schema_observation_binding("input-1"),
            ),
        ],
    );
    EffectiveSchemaRuntime::new(
        evidence,
        vec![EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap()
    .with_terminal_quarantines(vec![*terminal_0, *terminal_1])
    .unwrap()
    .with_discovery_executor_budget(
        DiscoveryExecutorBudgetEvidence::new(64, 1_000, 128, 2).unwrap(),
    )
    .unwrap()
}

pub(super) fn terminal_file_position() -> SourcePosition {
    SourcePosition::FileManifest(FileManifest {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        files: vec![FilePosition {
            path: "input-0".to_owned(),
            size_bytes: 10,
            source_generation: None,
            etag: Some("etag-0".to_owned()),
            object_version: None,
            sha256: Some(format!("sha256:{}", "ab".repeat(32))),
        }],
    })
}

pub(super) fn mock_compiled_source_plan(
    resource: &MockResource,
    retry_policy: Option<cdf_runtime::SourceRetryPolicy>,
) -> cdf_runtime::CompiledSourcePlan {
    mock_compiled_source_plan_with_speculation(resource, retry_policy, true)
}

pub(super) fn fast_test_retry_policy() -> cdf_runtime::SourceRetryPolicy {
    cdf_runtime::SourceRetryPolicy {
        max_total_attempts: 3,
        max_elapsed_ms: 30_000,
        base_delay_ms: 1,
        max_delay_ms: 1,
    }
}

#[derive(Clone)]
pub(super) struct MockResource {
    pub(super) descriptor: ResourceDescriptor,
    pub(super) schema: SchemaRef,
    pub(super) batches: Vec<Batch>,
    pub(super) partition_count: usize,
    pub(super) negotiate_count: Arc<AtomicUsize>,
    pub(super) negotiated_frontier: Arc<Mutex<Option<SourcePosition>>>,
    pub(super) open_count: Arc<AtomicUsize>,
    pub(super) batch_poll_count: Arc<AtomicUsize>,
    pub(super) attest_count: Arc<AtomicUsize>,
    pub(super) attestation: Option<PartitionAttestation>,
    pub(super) completion_attestation: Option<PartitionAttestation>,
    pub(super) attestation_error: Option<String>,
    pub(super) dynamic_attestation: bool,
    pub(super) transient_open_failures: Arc<AtomicUsize>,
    pub(super) transient_stream_failures: Arc<AtomicUsize>,
    pub(super) effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    pub(super) baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
    pub(super) type_policy_allowances: cdf_kernel::TypePolicyAllowances,
    pub(super) duplicate_observation_identity: bool,
    pub(super) misroute_batches: bool,
    pub(super) retry_safety: cdf_kernel::PartitionRetrySafety,
    pub(super) stall_after_batches: bool,
    pub(super) tier_a_intent: cdf_kernel::CompiledScanIntent,
    pub(super) compiled_source_plan: Arc<OnceLock<cdf_runtime::CompiledSourcePlan>>,
    pub(super) compiled_source_plan_hash: Arc<OnceLock<cdf_kernel::CompiledSourcePlanHash>>,
}

impl MockResource {
    pub(super) fn tier_a(batches: Vec<Batch>) -> Self {
        Self::new(batches, false)
    }

    pub(super) fn tier_b(batches: Vec<Batch>) -> Self {
        Self::new(batches, true)
    }

    pub(super) fn new(batches: Vec<Batch>, tier_b: bool) -> Self {
        let schema = batches
            .first()
            .and_then(Batch::record_batch)
            .map(RecordBatch::schema)
            .unwrap_or_else(sample_schema);
        Self {
            descriptor: descriptor(),
            schema,
            batches,
            partition_count: if tier_b { 2 } else { 1 },
            negotiate_count: Arc::new(AtomicUsize::new(0)),
            negotiated_frontier: Arc::new(Mutex::new(None)),
            open_count: Arc::new(AtomicUsize::new(0)),
            batch_poll_count: Arc::new(AtomicUsize::new(0)),
            attest_count: Arc::new(AtomicUsize::new(0)),
            attestation: None,
            completion_attestation: None,
            attestation_error: None,
            dynamic_attestation: false,
            transient_open_failures: Arc::new(AtomicUsize::new(0)),
            transient_stream_failures: Arc::new(AtomicUsize::new(0)),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
            type_policy_allowances: cdf_kernel::TypePolicyAllowances::default(),
            duplicate_observation_identity: false,
            misroute_batches: false,
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            stall_after_batches: false,
            tier_a_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            compiled_source_plan: Arc::new(OnceLock::new()),
            compiled_source_plan_hash: Arc::new(OnceLock::new()),
        }
    }

    pub(super) fn with_write_disposition(mut self, write_disposition: WriteDisposition) -> Self {
        self.descriptor.write_disposition = write_disposition;
        self
    }

    pub(super) fn without_control_keys(mut self) -> Self {
        self.descriptor.primary_key.clear();
        self.descriptor.merge_key.clear();
        self.descriptor.cursor = None;
        self.descriptor.write_disposition = WriteDisposition::Append;
        self
    }

    pub(super) fn with_partition_count(mut self, partition_count: usize) -> Self {
        self.partition_count = partition_count;
        self
    }

    pub(super) fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
        self
    }

    pub(super) fn with_effective_schema_runtime(
        mut self,
        schema: SchemaRef,
        runtime: EffectiveSchemaRuntime,
    ) -> Self {
        let SchemaBaselineReference::Pinned { snapshot } = &runtime.evidence.baseline else {
            panic!("engine effective-schema fixtures require a pinned discovery baseline");
        };
        self.schema = schema;
        self.descriptor.schema_source = SchemaSource::Discovered {
            snapshot: snapshot.clone(),
        };
        self.baseline_observation_schema_catalog = runtime.schema_catalog.clone();
        self.effective_schema_runtime = Some(runtime);
        self
    }

    pub(super) fn with_baseline_observation_schema_catalog(
        mut self,
        mut catalog: Vec<EffectiveSchemaCatalogEntry>,
    ) -> Self {
        catalog.sort_by(|left, right| left.physical_schema_hash.cmp(&right.physical_schema_hash));
        self.baseline_observation_schema_catalog = catalog;
        self
    }

    pub(super) fn with_attestation(mut self, attestation: PartitionAttestation) -> Self {
        self.attestation = Some(attestation);
        self
    }

    pub(super) fn with_dynamic_attestation(mut self) -> Self {
        self.dynamic_attestation = true;
        self
    }

    pub(super) fn with_completion_attestation(mut self, attestation: PartitionAttestation) -> Self {
        self.completion_attestation = Some(attestation);
        self
    }

    pub(super) fn with_attestation_error(mut self, error: impl Into<String>) -> Self {
        self.attestation_error = Some(error.into());
        self
    }

    pub(super) fn with_transient_open_failures(mut self, failures: usize) -> Self {
        self.transient_open_failures
            .store(failures, Ordering::SeqCst);
        self.retry_safety = cdf_kernel::PartitionRetrySafety::ImmutableContent;
        self
    }

    pub(super) fn with_transient_stream_failures(mut self, failures: usize) -> Self {
        self.transient_stream_failures
            .store(failures, Ordering::SeqCst);
        self.retry_safety = cdf_kernel::PartitionRetrySafety::ImmutableContent;
        self
    }

    pub(super) fn with_type_policy_allowances(
        mut self,
        allowances: cdf_kernel::TypePolicyAllowances,
    ) -> Self {
        self.type_policy_allowances = allowances;
        self
    }

    pub(super) fn with_duplicate_observation_identity(mut self) -> Self {
        self.duplicate_observation_identity = true;
        self
    }

    pub(super) fn with_misrouted_batches(mut self) -> Self {
        self.misroute_batches = true;
        self
    }

    pub(super) fn with_tier_a_intent(mut self, intent: cdf_kernel::CompiledScanIntent) -> Self {
        self.tier_a_intent = intent;
        self
    }

    pub(super) fn with_stall_after_batches(mut self) -> Self {
        self.stall_after_batches = true;
        self
    }

    pub(super) fn bind_compiled_source(&self, source: &cdf_runtime::CompiledSourcePlan) {
        let hash = source.compiled_source_plan_hash().unwrap();
        match self.compiled_source_plan.set(source.clone()) {
            Ok(()) => {}
            Err(source) => assert_eq!(
                self.compiled_source_plan.get(),
                Some(&source),
                "mock source compiler binding is single-assignment"
            ),
        }
        match self.compiled_source_plan_hash.set(hash) {
            Ok(()) => {}
            Err(hash) => assert_eq!(
                self.compiled_source_plan_hash.get(),
                Some(&hash),
                "mock source hash binding is single-assignment"
            ),
        }
    }
}

impl ResourceStream for MockResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
        self.compiled_source_plan_hash.get()
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        (0..self.partition_count)
            .map(|index| {
                let mut metadata = BTreeMap::from([("ordinal".to_owned(), index.to_string())]);
                if self.duplicate_observation_identity {
                    metadata.insert(
                        PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
                        "duplicate-observation".to_owned(),
                    );
                } else if let Some(runtime) = &self.effective_schema_runtime {
                    let observation_id = runtime
                        .evidence
                        .observations
                        .get(index)
                        .map(|observation| observation.observation_id.clone())
                        .unwrap_or_else(|| format!("unobserved-part-{index}"));
                    metadata.insert(PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(), observation_id);
                }
                if self.effective_schema_runtime.is_some() {
                    metadata.insert(
                        PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
                        schema_observation_binding(&format!("input-{index}")).to_string(),
                    );
                }
                Ok(PartitionPlan {
                    partition_id: PartitionId::new(format!("part-{index}"))?,
                    scope: ScopeKey::Partition {
                        partition_id: PartitionId::new(format!("part-{index}"))?,
                    },
                    planned_position: None,
                    start_position: None,
                    scan_intent: self.tier_a_intent.clone(),
                    retry_safety: self.retry_safety,
                    metadata,
                })
            })
            .collect()
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.open_count.fetch_add(1, Ordering::SeqCst);
        if self
            .transient_open_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
                Err(cdf_kernel::CdfError::transient("mock open unavailable"))
            }));
        }
        let start_position = partition.start_position.clone();
        let batches = self
            .batches
            .iter()
            .filter(|batch| {
                self.misroute_batches || batch.header.partition_id == partition.partition_id
            })
            .filter(|batch| {
                start_position.as_ref().is_none_or(|start| {
                    batch
                        .header
                        .source_position
                        .as_ref()
                        .is_some_and(|position| cursor_position_is_after(position, start))
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let transient_stream_failure = self
            .transient_stream_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        let completion_attestation = self.completion_attestation.clone();
        let batch_poll_count = Arc::clone(&self.batch_poll_count);
        let stall_after_batches = self.stall_after_batches;
        cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
            let stream = if transient_stream_failure {
                Box::pin(stream::iter([Err(cdf_kernel::CdfError::transient(
                    "mock lazy stream unavailable",
                ))])) as BatchStream
            } else {
                let batches = stream::iter(batches.into_iter().map(Ok));
                if stall_after_batches {
                    Box::pin(batches.chain(stream::pending())) as BatchStream
                } else {
                    Box::pin(batches) as BatchStream
                }
            };
            let stream = Box::pin(stream.inspect(move |_| {
                batch_poll_count.fetch_add(1, Ordering::SeqCst);
            })) as BatchStream;
            match completion_attestation {
                Some(attestation) => Ok(cdf_kernel::PartitionStreamPayload::new(
                    stream,
                    Box::pin(async move {
                        Ok(cdf_kernel::PartitionCompletion::new(
                            Some(attestation),
                            None,
                        ))
                    }),
                )),
                None => Ok(cdf_kernel::PartitionStreamPayload::batches(stream)),
            }
        }))
    }

    fn attest_partition(
        &self,
        partition: PartitionPlan,
    ) -> cdf_kernel::PartitionAttestationAttempt<'_> {
        self.attest_count.fetch_add(1, Ordering::SeqCst);
        let attestation = if self.dynamic_attestation {
            self.batches
                .iter()
                .filter(|batch| batch.header.partition_id == partition.partition_id)
                .filter_map(|batch| batch.header.source_position.clone())
                .next_back()
                .map(|position| PartitionAttestation::new(position, None))
        } else {
            self.attestation.clone()
        };
        let error = self.attestation_error.clone();
        cdf_kernel::PartitionAttestationAttempt::materialized(Box::pin(async move {
            if let Some(error) = error {
                return Err(cdf_kernel::CdfError::data(error));
            }
            Ok(attestation)
        }))
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_ref()
    }

    fn baseline_observation_schema_catalog(&self) -> &[EffectiveSchemaCatalogEntry] {
        &self.baseline_observation_schema_catalog
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.type_policy_allowances
    }
}

fn cursor_position_is_after(position: &SourcePosition, start: &SourcePosition) -> bool {
    match (position, start) {
        (SourcePosition::Cursor(position), SourcePosition::Cursor(start))
            if position.field == start.field =>
        {
            match (&position.value, &start.value) {
                (CursorValue::I64(position), CursorValue::I64(start)) => position > start,
                (CursorValue::U64(position), CursorValue::U64(start)) => position > start,
                _ => false,
            }
        }
        _ => false,
    }
}

impl QueryableResource for MockResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        static CAPABILITIES: std::sync::OnceLock<ResourceCapabilities> = std::sync::OnceLock::new();
        CAPABILITIES.get_or_init(|| ResourceCapabilities {
            projection: CapabilitySupport::Supported,
            filters: FilterCapabilities {
                default_fidelity: PushdownFidelity::Inexact,
                supported_operators: vec![">".to_owned(), ">=".to_owned(), "=".to_owned()],
            },
            limits: CapabilitySupport::Supported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![cdf_kernel::ScopeKind::Partition],
            },
            incremental: IncrementalShape::Cursor,
            replay: cdf_kernel::ReplaySupport::ExactRecordedBatches,
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::RowsAndBytes,
        })
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.negotiate_count.fetch_add(1, Ordering::SeqCst);
        let mut plan = negotiate_scan_plan(
            request.resource_id.clone(),
            request.clone(),
            self.capabilities(),
            self.plan_partitions(request)?,
            Some(3),
            Some(256),
            DeliveryGuarantee::EffectivelyOncePerKey,
        )?;
        for pushed in &mut plan.pushed_predicates {
            if pushed.predicate.expression == "id > 1"
                || pushed.predicate.expression == "updated_at >= '2026-07-12T00:00:00Z'"
            {
                pushed.fidelity = PushdownFidelity::Exact;
            }
        }
        let pushed_predicates = plan.pushed_predicates.clone();
        for partition in plan.inline_partitions_mut().unwrap() {
            partition.scan_intent.predicates = pushed_predicates.clone();
        }
        Ok(plan)
    }

    fn negotiate_with_committed_frontier(
        &self,
        request: &ScanRequest,
        committed_frontier: Option<&SourcePosition>,
    ) -> Result<ScanPlan> {
        *self.negotiated_frontier.lock().unwrap() = committed_frontier.cloned();
        let scan = self.negotiate(request)?;
        match committed_frontier {
            Some(frontier) => self.rebind_scan_for_resume(scan, frontier),
            None => Ok(scan),
        }
    }
}

pub(super) fn assert_honest_cdf_native_operator_metadata(plan: &EnginePlan) {
    let plan_json = serde_json::to_value(plan).unwrap();
    let plan_text = serde_json::to_string(&plan_json).unwrap();
    assert!(!plan_text.contains("data_fusion_table_provider"));
    assert!(!plan_text.contains("data_fusion_scan_exec"));
    assert!(!plan_text.contains("datafusion_table_provider"));

    assert_cdf_native_operator_kinds(&plan_json["operator_chain"]);
    assert_cdf_native_operator_kinds(&plan_json["explain"]["operator_chain"]);
    assert_eq!(
        plan_json["operator_chain"][0]["adapter_kind"],
        "cdf_native_resource_adapter"
    );
    assert_eq!(
        plan_json["explain"]["operator_chain"][0]["adapter_kind"],
        "cdf_native_resource_adapter"
    );
}

fn assert_cdf_native_operator_kinds(operator_chain: &serde_json::Value) {
    let actual = operator_chain
        .as_array()
        .unwrap()
        .iter()
        .map(|operator| operator["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            "cdf_resource_adapter",
            "cdf_native_scan",
            "schema_fingerprint_exec",
            "contract_exec",
            "normalize_exec",
            "profile_exec",
            "lineage_exec",
            "package_sink",
        ]
    );
}

pub(super) fn assert_explain_carries_required_fields(explain_json: &serde_json::Value) {
    for field in [
        "pushed_predicates",
        "inexact_predicates",
        "unsupported_predicates",
        "partitions",
        "estimates",
        "delivery_guarantee",
        "execution_extent",
    ] {
        assert!(explain_json.get(field).is_some(), "missing {field}");
    }
}

pub(super) fn batch_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let index = batch.schema().index_of(column).unwrap();
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row).to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

pub(super) fn plan_input(
    filters: Vec<&str>,
    projection: Option<Vec<String>>,
    limit: Option<u64>,
    execution_extent: ExecutionExtent,
) -> EnginePlanInput {
    plan_input_for_schema(
        sample_schema(),
        filters,
        projection,
        limit,
        execution_extent,
    )
}

pub(super) fn sample_stream_epoch_policy() -> StreamEpochPolicy {
    StreamEpochPolicy {
        version: STREAM_EPOCH_POLICY_VERSION,
        checkpoint_cadence: EpochClosureTrigger::Rows { count: 5 },
        package_rotation: EpochClosureTrigger::Bytes { count: 1 << 20 },
        watermark: WatermarkPolicy::Disabled,
        late_data: LateDataAction::Quarantine,
        safe_frontier: SafeFrontierPolicy::CanonicalAdmittedSourcePosition,
    }
}

pub(super) fn plan_input_for_schema(
    schema: SchemaRef,
    filters: Vec<&str>,
    projection: Option<Vec<String>>,
    limit: Option<u64>,
    execution_extent: ExecutionExtent,
) -> EnginePlanInput {
    let observed = ObservedSchema::from_arrow(schema.as_ref());
    let validation_program =
        compile_validation_program(&ContractPolicy::for_trust(TrustLevel::Governed), &observed)
            .unwrap();
    EnginePlanInput {
        request: ScanRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            projection,
            filters: filters
                .into_iter()
                .enumerate()
                .map(|(index, expression)| {
                    ScanPredicate::new(PredicateId::new(format!("p{index}")).unwrap(), expression)
                        .unwrap()
                })
                .collect(),
            limit,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        },
        validation_program,
        execution_extent,
        segmentation: CanonicalSegmentationPolicy::performance_default(),
        package_id: "pkg-engine-test".to_owned(),
        committed_frontier: None,
    }
}

pub(super) fn rename_column_program_output(
    program: &mut cdf_contract::ValidationProgram,
    source_name: &str,
    output_name: &str,
) {
    let column = program
        .column_programs
        .iter_mut()
        .find(|column| column.source_name == source_name)
        .unwrap();
    column.output_name = output_name.to_owned();
}

pub(super) fn coercion_decision<'a>(
    plan: &'a cdf_contract::SchemaCoercionPlan,
    source_name: &str,
) -> &'a cdf_contract::FieldCoercion {
    plan.fields
        .iter()
        .find(|field| field.source_name == source_name)
        .unwrap()
}

pub(super) fn stream_admission_coercion(
    package_dir: &std::path::Path,
) -> cdf_contract::SchemaCoercionPlan {
    let evidence: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &std::fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(evidence.observations.len(), 1);
    evidence
        .observations
        .into_iter()
        .next()
        .unwrap()
        .coercion_plan
}

pub(super) fn descriptor() -> ResourceDescriptor {
    let schema_hash = SchemaHash::new("schema-v1").unwrap();
    ResourceDescriptor {
        resource_id: ResourceId::new("orders").unwrap(),
        schema_source: SchemaSource::Discovered {
            snapshot: SchemaSnapshotReference {
                schema_hash,
                path: ".cdf/schemas/orders@schema-v1.json".to_owned(),
                metadata: BTreeMap::from([("probe".to_owned(), "engine-test".to_owned())]),
            },
        },
        primary_key: vec!["id".to_owned()],
        merge_key: vec!["id".to_owned()],
        cursor: None,
        write_disposition: WriteDisposition::Merge,
        deduplication: None,
        contract: Some(ContractRef::new("contract-orders").unwrap()),
        state_scope: ScopeKey::Resource,
        freshness: Some(FreshnessSpec { max_age_ms: 60_000 }),
        trust_level: TrustLevel::Governed,
    }
}

pub(super) fn bound_effective_schema_evidence(
    effective_schema_hash: SchemaHash,
    manifest_hash: &str,
    manifest_path: &str,
    observations: Vec<EffectiveSchemaObservationEvidence>,
) -> EffectiveSchemaEvidence {
    let discovery_manifest = DiscoveryManifestReference {
        manifest_hash: DiscoveryManifestHash::new(manifest_hash).unwrap(),
        path: manifest_path.to_owned(),
    };
    let snapshot = descriptor()
        .schema_source
        .pinned_snapshot()
        .unwrap()
        .clone()
        .with_discovery_manifest(&discovery_manifest)
        .unwrap();
    EffectiveSchemaEvidence::new(
        SchemaBaselineReference::Pinned { snapshot },
        effective_schema_hash,
        discovery_manifest,
        observations,
    )
    .unwrap()
}

pub(super) fn schema_observation_binding(
    observation_id: &str,
) -> cdf_kernel::SchemaObservationBinding {
    cdf_kernel::SchemaObservationBinding::new(
        cdf_runtime::artifact_hash(&("engine-test-schema-observation", observation_id)).unwrap(),
    )
    .unwrap()
}

pub(super) fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

pub(super) fn incompatible_sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

pub(super) fn sample_batches() -> Vec<Batch> {
    vec![
        batch_for_partition(
            "batch-0",
            "part-0",
            vec![1, 2, 3],
            vec!["one", "two", "three"],
            vec![false, true, true],
        ),
        batch_for_partition(
            "batch-1",
            "part-1",
            vec![1, 2, 3],
            vec!["one", "two", "three"],
            vec![false, true, true],
        ),
    ]
}

pub(super) fn batch_for_partition(
    batch_id: &str,
    partition_id: &str,
    ids: Vec<i32>,
    names: Vec<&str>,
    active: Vec<bool>,
) -> Batch {
    batch_for_partition_with_schema(batch_id, partition_id, sample_schema(), ids, names, active)
}

pub(super) fn batch_for_partition_with_schema(
    batch_id: &str,
    partition_id: &str,
    schema: SchemaRef,
    ids: Vec<i32>,
    names: Vec<&str>,
    active: Vec<bool>,
) -> Batch {
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(BooleanArray::from(active)) as ArrayRef,
        ],
    )
    .unwrap();

    Batch {
        header: BatchHeader::new(
            BatchId::new(batch_id).unwrap(),
            ResourceId::new("orders").unwrap(),
            PartitionId::new(partition_id).unwrap(),
            cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
            record_batch.num_rows() as u64,
            record_batch.get_array_memory_size() as u64,
        ),
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    }
}

pub(super) fn mock_compiled_source_plan_with_speculation(
    resource: &MockResource,
    retry_policy: Option<cdf_runtime::SourceRetryPolicy>,
    speculative_safe: bool,
) -> cdf_runtime::CompiledSourcePlan {
    let retry_enabled = retry_policy.is_some();
    cdf_runtime::CompiledSourcePlan::new(
        cdf_runtime::SourceDriverDescriptor {
            driver_id: cdf_runtime::SourceDriverId::new("external_mock").unwrap(),
            driver_version: "mock-v1".to_owned(),
            option_schema_hash:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            kinds: vec!["external_mock".to_owned()],
            schemes: vec!["mock".to_owned()],
        },
        resource.capabilities().clone(),
        cdf_runtime::SourceExecutionCapabilities {
            minimum_poll_bytes: 1024,
            maximum_poll_bytes: 1024 * 1024,
            minimum_decode_bytes: 1024,
            maximum_decode_bytes: 8 * 1024 * 1024,
            maximum_emitted_batch_bytes: 8 * 1024 * 1024,
            maximum_concurrency: 8,
            useful_concurrency: 4,
            executor_class: cdf_runtime::SourceExecutorClass::Io,
            blocking_lane: None,
            pausable: true,
            spillable: false,
            idempotent_reads: true,
            reopenable: true,
            resumable: true,
            speculative_safe,
            retry_granularity: if retry_enabled {
                cdf_runtime::SourceRetryGranularity::Partition
            } else {
                cdf_runtime::SourceRetryGranularity::None
            },
            retryable_errors: retry_enabled
                .then_some(cdf_kernel::ErrorKind::Transient)
                .into_iter()
                .collect(),
            retry_policy,
            attestation: if retry_enabled || speculative_safe {
                cdf_runtime::SourceAttestationStrength::ImmutableContent
            } else {
                cdf_runtime::SourceAttestationStrength::None
            },
            rate_limit: None,
            quota_authority: None,
            canonical_order: true,
            bounded: true,
            batch_memory: if retry_enabled {
                cdf_runtime::SourceBatchMemoryContract::Preaccounted
            } else {
                cdf_runtime::SourceBatchMemoryContract::FrontierReserved
            },
            telemetry_version: "mock-v1".to_owned(),
        },
        cdf_runtime::CompiledSourcePlanInput {
            descriptor: resource.descriptor().clone(),
            schema: resource.schema().as_ref().clone(),
            type_policy_allowances: resource.type_policy_allowances,
            effective_schema_runtime: resource.effective_schema_runtime.clone(),
            baseline_observation_schema_catalog: resource
                .baseline_observation_schema_catalog
                .clone(),
            redacted_options: serde_json::json!({"endpoint": "redacted"}),
            physical_plan: serde_json::json!({"partitioning": "mock"}),
        },
    )
    .unwrap()
}

#[derive(Debug)]
pub(super) struct RetainedEngineRun {
    pub(super) run: EngineRunOutputWithSegmentPositions,
    pub(super) _package: TempDir,
}

impl std::ops::Deref for RetainedEngineRun {
    type Target = EngineRunOutputWithSegmentPositions;

    fn deref(&self) -> &Self::Target {
        &self.run
    }
}

pub(super) fn mock_unbounded_source_plan(
    resource: &MockResource,
) -> cdf_runtime::CompiledSourcePlan {
    let mut source = mock_compiled_source_plan(resource, None);
    source.execution_capabilities.bounded = false;
    source.execution_capabilities.speculative_safe = false;
    source.stream_capabilities = Some(cdf_runtime::SourceStreamCapabilities {
        quiescence: true,
        watermark_behavior: cdf_kernel::OperatorWatermarkBehavior::Drop,
        watermark: None,
        safe_frontiers: vec![SafeFrontierPolicy::CanonicalAdmittedSourcePosition],
        source_frontiers: vec![cdf_runtime::SourceFrontierCapability::FileManifest],
        idleness_capabilities: Vec::new(),
    });
    source.validate().unwrap();
    source
}

pub(super) fn missing_control_field_batch(
    batch_id: &str,
    partition_id: &str,
    names: Vec<&str>,
) -> Batch {
    let row_count = names.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true; row_count])) as ArrayRef,
        ],
    )
    .unwrap();
    Batch {
        header: BatchHeader::new(
            BatchId::new(batch_id).unwrap(),
            ResourceId::new("orders").unwrap(),
            PartitionId::new(partition_id).unwrap(),
            cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref()).unwrap(),
            record_batch.num_rows() as u64,
            record_batch.get_array_memory_size() as u64,
        ),
        payload: cdf_kernel::BatchPayload::in_memory(record_batch),
    }
}
