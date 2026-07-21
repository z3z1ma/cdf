use std::{
    collections::BTreeMap,
    ffi::CString,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, Int64Array, TimestampMicrosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_kernel::{
    BackpressureSupport, CapabilitySupport, CompiledSourcePlanHash, CursorOrderingClaim,
    CursorPosition, CursorSpec, CursorValue, DeliveryGuarantee, EffectiveSchemaCatalogEntry,
    EffectiveSchemaRuntime, ErrorKind, EstimateSupport, FilterCapabilities, ForeignState,
    IncrementalShape, PartitionAuthority, PartitionId, PartitionPlan, PartitioningCapabilities,
    PlanId, QueryableResource, ReplaySupport, ResourceCapabilities, ResourceDescriptor, ResourceId,
    ResourceStream, Result, ScanPlan, ScanRequest, SchemaSource, ScopeKey, SourcePosition,
    TrustLevel, TypePolicyAllowances, WriteDisposition, parse_arrow_field_type,
};
use cdf_runtime::CompiledSourcePlan;
use pyo3::{
    Python,
    types::{PyAnyMethods, PyModule},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{PythonBridgeOptions, PythonResourceBridge, internal::py_error};
use cdf_foreign_stream::{
    ForeignBackpressure, ForeignCancellation, ForeignCancellationContract, ForeignExecutionLane,
    ForeignLaneCapabilities, ForeignMemoryContract, ForeignProducer, ForeignProducerDescriptor,
    ForeignProducerId, ForeignProtocolVersion, ForeignSecurityContract, ForeignStartupModel,
    ForeignStateContract, ForeignStreamEvent, ForeignStreamOpen, ForeignStreamOpenRequest,
    ForeignTerminalStatus, ForeignTransferMode,
};

const PARTITION_ID: &str = "python-000001";

#[derive(Clone, Debug)]
pub struct PythonResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    capabilities: ResourceCapabilities,
    module_path: PathBuf,
    module_relative: String,
    callable: String,
    content_hash: String,
    bounded: bool,
    dict_batch_rows: usize,
    max_boundary_bytes: u64,
    execution: Option<cdf_runtime::ExecutionServices>,
    blocking_lane: Option<String>,
    compiled_source_plan_hash: Option<CompiledSourcePlanHash>,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
    type_policy_allowances: TypePolicyAllowances,
    foreign_descriptor: ForeignProducerDescriptor,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PythonPhysicalPlan {
    pub(crate) module_relative: String,
    pub(crate) callable: String,
    pub(crate) content_hash: String,
    pub(crate) bounded: bool,
    pub(crate) dict_batch_rows: usize,
    pub(crate) max_boundary_bytes: u64,
}

impl PythonResource {
    pub fn load(
        project_root: &Path,
        uri: &str,
        resource_id: ResourceId,
        trust_level: TrustLevel,
        dict_batch_rows: usize,
        max_boundary_bytes: u64,
    ) -> Result<Self> {
        if dict_batch_rows == 0 || max_boundary_bytes < 2 {
            return Err(cdf_kernel::CdfError::contract(
                "Python source requires positive dict_batch_rows and max_boundary_bytes of at least 2",
            ));
        }
        let (module_relative, callable) = parse_python_uri(uri)?;
        let module_path = resolve_module_path(project_root, &module_relative)?;
        let source = fs::read_to_string(&module_path).map_err(|error| {
            cdf_kernel::CdfError::contract(format!(
                "read Python resource module {}: {error}",
                module_path.display()
            ))
        })?;
        let metadata = inspect_metadata(&source, &module_relative, &callable)?;
        let schema = Arc::new(Schema::new(
            metadata
                .schema
                .into_iter()
                .map(|(name, field_type, nullable)| {
                    Ok(Field::new(
                        name,
                        parse_arrow_field_type(&field_type)?,
                        nullable,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        ));
        let schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?;
        let write_disposition = match metadata.write_disposition.as_str() {
            "append" => WriteDisposition::Append,
            "replace" => WriteDisposition::Replace,
            "merge" => WriteDisposition::Merge,
            other => {
                return Err(cdf_kernel::CdfError::contract(format!(
                    "Python resource `{resource_id}` declares unsupported write disposition `{other}`"
                )));
            }
        };
        if write_disposition == WriteDisposition::Merge && metadata.merge_key.is_empty() {
            return Err(cdf_kernel::CdfError::contract(format!(
                "Python resource `{resource_id}` uses merge without a merge key"
            )));
        }
        let has_cursor = metadata.cursor.is_some();
        let cursor = metadata.cursor.map(|field| CursorSpec {
            field,
            ordering: CursorOrderingClaim::Exact,
            lag_tolerance_ms: 0,
        });
        let content_hash = format!("sha256:{}", hex::encode(Sha256::digest(source.as_bytes())));
        let foreign_descriptor = python_foreign_descriptor(max_boundary_bytes)?;
        Ok(Self {
            descriptor: ResourceDescriptor {
                resource_id,
                schema_source: SchemaSource::Declared {
                    schema_hash,
                    source: uri.to_owned(),
                },
                primary_key: metadata.primary_key,
                merge_key: metadata.merge_key,
                cursor,
                write_disposition,
                deduplication: None,
                contract: None,
                state_scope: ScopeKey::Resource,
                freshness: None,
                trust_level,
            },
            schema,
            capabilities: ResourceCapabilities {
                projection: CapabilitySupport::Unsupported,
                filters: FilterCapabilities::default(),
                limits: CapabilitySupport::Unsupported,
                ordering: CapabilitySupport::Unsupported,
                partitioning: PartitioningCapabilities {
                    parallel_partitions: false,
                    supported_scopes: vec![cdf_kernel::ScopeKind::Resource],
                },
                incremental: if has_cursor {
                    IncrementalShape::Cursor
                } else {
                    IncrementalShape::Full
                },
                replay: ReplaySupport::None,
                idempotent_reads: false,
                backpressure: BackpressureSupport::Pausable,
                estimates: EstimateSupport::None,
            },
            module_path,
            module_relative,
            callable,
            content_hash,
            bounded: metadata.bounded,
            dict_batch_rows,
            max_boundary_bytes,
            execution: None,
            blocking_lane: None,
            compiled_source_plan_hash: None,
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
            type_policy_allowances: TypePolicyAllowances::default(),
            foreign_descriptor,
        })
    }

    pub(crate) fn physical_plan(&self) -> PythonPhysicalPlan {
        PythonPhysicalPlan {
            module_relative: self.module_relative.clone(),
            callable: self.callable.clone(),
            content_hash: self.content_hash.clone(),
            bounded: self.bounded,
            dict_batch_rows: self.dict_batch_rows,
            max_boundary_bytes: self.max_boundary_bytes,
        }
    }

    pub(crate) fn from_compiled(
        project_root: &Path,
        plan: &CompiledSourcePlan,
        physical: PythonPhysicalPlan,
    ) -> Result<Self> {
        if physical.dict_batch_rows == 0 || physical.max_boundary_bytes < 2 {
            return Err(cdf_kernel::CdfError::contract(
                "compiled Python source requires positive dict_batch_rows and max_boundary_bytes of at least 2",
            ));
        }
        let module_path = resolve_module_path(project_root, &physical.module_relative)?;
        let foreign_descriptor = python_foreign_descriptor(physical.max_boundary_bytes)?;
        Ok(Self {
            descriptor: plan.descriptor.clone(),
            schema: Arc::new(plan.schema.clone()),
            capabilities: plan.resource_capabilities.clone(),
            module_path,
            module_relative: physical.module_relative,
            callable: physical.callable,
            content_hash: physical.content_hash,
            bounded: physical.bounded,
            dict_batch_rows: physical.dict_batch_rows,
            max_boundary_bytes: physical.max_boundary_bytes,
            execution: None,
            blocking_lane: None,
            compiled_source_plan_hash: Some(plan.compiled_source_plan_hash()?),
            effective_schema_runtime: plan.effective_schema_runtime.clone(),
            baseline_observation_schema_catalog: plan.baseline_observation_schema_catalog.clone(),
            type_policy_allowances: plan.type_policy_allowances,
            foreign_descriptor,
        })
    }

    pub(crate) fn with_execution_services_and_lane(
        mut self,
        execution: cdf_runtime::ExecutionServices,
        lane_id: String,
    ) -> Result<Self> {
        if lane_id.is_empty() {
            return Err(cdf_kernel::CdfError::contract(
                "compiled Python source requires a nonempty blocking lane",
            ));
        }
        self.execution = Some(execution);
        self.blocking_lane = Some(lane_id);
        Ok(self)
    }

    fn partition(&self) -> Result<PartitionPlan> {
        let mut partition = PartitionPlan {
            partition_id: PartitionId::new(PARTITION_ID)?,
            scope: self.descriptor.state_scope.clone(),
            planned_position: None,
            start_position: None,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::from([
                ("source_kind".to_owned(), "python".to_owned()),
                ("module".to_owned(), self.module_relative.clone()),
                ("callable".to_owned(), self.callable.clone()),
                ("content_identity".to_owned(), self.content_hash.clone()),
            ]),
        };
        if let Some(runtime) = &self.effective_schema_runtime {
            cdf_kernel::bind_partition_schema_observation(
                &mut partition,
                runtime,
                self.descriptor.resource_id.as_str(),
            )?;
        }
        Ok(partition)
    }

    fn produce_foreign_stream(
        &self,
        partition: PartitionPlan,
        sender: &mut cdf_runtime::BlockingTaskStreamSender<ForeignStreamEvent>,
        cancellation: &cdf_runtime::RunCancellation,
        foreign_cancellation: &ForeignCancellation,
        memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    ) -> Result<Option<SourcePosition>> {
        if partition.partition_id.as_str() != PARTITION_ID {
            return Err(cdf_kernel::CdfError::contract(format!(
                "Python resource planned partition `{PARTITION_ID}` but received `{}`",
                partition.partition_id
            )));
        }
        let source = fs::read_to_string(&self.module_path).map_err(|error| {
            cdf_kernel::CdfError::data(format!(
                "read Python resource module {}: {error}",
                self.module_path.display()
            ))
        })?;
        let observed_content_hash =
            format!("sha256:{}", hex::encode(Sha256::digest(source.as_bytes())));
        if observed_content_hash != self.content_hash {
            return Err(cdf_kernel::CdfError::data(format!(
                "Python resource module `{}` changed after planning; replan before execution",
                self.module_relative
            )));
        }
        let opaque_blob = format!("{}#{}", self.content_hash, self.callable).into_bytes();
        let blob_sha256 = format!("sha256:{}", hex::encode(Sha256::digest(&opaque_blob)));
        let cursor = self.descriptor.cursor.clone();
        let execution = self.execution.as_ref().ok_or_else(|| {
            cdf_kernel::CdfError::contract(
                "Python foreign producer requires injected execution services",
            )
        })?;
        let mut reservation = Some(reserve_python_batch(
            execution,
            cancellation,
            foreign_cancellation,
            Arc::clone(&memory),
            self.max_boundary_bytes,
        )?);
        let mut final_position = None;
        let produced = Python::attach(|py| -> Result<_> {
            let module = load_module(py, &source, &self.module_relative)?;
            let callable = module.getattr(self.callable.as_str()).map_err(|_| {
                cdf_kernel::CdfError::contract(format!(
                    "Python resource callable `{}` is missing; run `cdf doctor` after repairing the resource target",
                    self.callable
                ))
            })?;
            let iterable = callable.call0().map_err(|_| {
                cdf_kernel::CdfError::data(format!(
                    "Python resource callable `{}` failed without emitting a batch",
                    self.callable
                ))
            })?;
            PythonResourceBridge::new(PythonBridgeOptions::new(
                self.descriptor.resource_id.clone(),
                partition.partition_id.clone(),
            )
            .with_dict_batch_rows(self.dict_batch_rows)?
            .with_max_boundary_bytes(self.max_boundary_bytes)?)
            .visit_python_foreign_iterable(&iterable, |outcome, _kind| {
                foreign_cancellation.check()?;
                let cdf_foreign_stream::ForeignBatchOutcome {
                    sequence,
                    mut batch,
                    transfer_mode,
                    copy,
                } = outcome;
                cancellation.check()?;
                batch.header.source_position = match &cursor {
                    Some(cursor) => batch
                        .record_batch()
                        .map(|record_batch| cursor_position(record_batch, cursor))
                        .transpose()?,
                    None => Some(SourcePosition::ForeignState(ForeignState {
                        version: 1,
                        protocol: "python-resource-v1".to_owned(),
                        opaque_blob: opaque_blob.clone(),
                        blob_sha256: blob_sha256.clone(),
                    })),
                };
                final_position.clone_from(&batch.header.source_position);
                let retained_bytes = batch
                    .record_batch()
                    .map(cdf_memory::record_batch_retained_bytes)
                    .transpose()?
                    .unwrap_or(0)
                    .checked_add(batch.header.pre_contract_evidence_retained_bytes()?)
                    .ok_or_else(|| {
                        cdf_kernel::CdfError::data("Python batch retained memory exceeds u64")
                    })?;
                if retained_bytes == 0 || retained_bytes > self.max_boundary_bytes {
                    return Err(cdf_kernel::CdfError::data(format!(
                        "Python source batch retains {retained_bytes} bytes outside its compiled 1..={}-byte limit; emit smaller Arrow batches, lower dict_batch_rows, or raise max_boundary_bytes",
                        self.max_boundary_bytes
                    )));
                }
                let lease = reservation.take().ok_or_else(|| {
                    cdf_kernel::CdfError::internal(
                        "Python source batch omitted its memory reservation",
                    )
                })?;
                lease.reconcile(retained_bytes)?;
                let batch = batch.with_retention(cdf_kernel::PayloadRetention::new(
                    Arc::new(lease),
                    retained_bytes,
                )?)?;
                sender.send(ForeignStreamEvent::Outcome(
                    cdf_foreign_stream::ForeignBatchOutcome {
                        sequence,
                        batch,
                        transfer_mode,
                        copy,
                    },
                ))?;
                cancellation.check()?;
                foreign_cancellation.check()?;
                reservation = Some(reserve_python_batch(
                    execution,
                    cancellation,
                    foreign_cancellation,
                    Arc::clone(&memory),
                    self.max_boundary_bytes,
                )?);
                Ok(())
            })?;
            Ok(())
        });
        produced?;
        Ok(final_position)
    }
}

fn reserve_python_batch(
    execution: &cdf_runtime::ExecutionServices,
    cancellation: &cdf_runtime::RunCancellation,
    foreign_cancellation: &ForeignCancellation,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    maximum_boundary_bytes: u64,
) -> Result<cdf_memory::MemoryLease> {
    let request = cdf_memory::ReservationRequest::new(
        cdf_memory::ConsumerKey::new("python-source-batch", cdf_memory::MemoryClass::Source)?,
        maximum_boundary_bytes,
    )?;
    let cancellation = cancellation.clone();
    let foreign_cancellation = foreign_cancellation.clone();
    execution.run_io(async move {
        let reserve = foreign_cancellation.await_or_cancel(cdf_memory::reserve(memory, request));
        cancellation.await_or_cancel(reserve).await
    })
}

fn cursor_position(
    batch: &arrow_array::RecordBatch,
    cursor: &CursorSpec,
) -> Result<SourcePosition> {
    let index = batch.schema().index_of(&cursor.field).map_err(|_| {
        cdf_kernel::CdfError::data(format!(
            "Python resource batch is missing cursor field `{}`",
            cursor.field
        ))
    })?;
    let array = batch.column(index);
    let value = match array.data_type() {
        DataType::Int64 => CursorValue::I64(max_int64(
            array.as_any().downcast_ref::<Int64Array>().unwrap(),
            &cursor.field,
        )?),
        DataType::UInt64 => CursorValue::U64(max_u64(
            array.as_any().downcast_ref::<UInt64Array>().unwrap(),
            &cursor.field,
        )?),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => CursorValue::TimestampMicros {
            micros: max_timestamp_micros(
                array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap(),
                &cursor.field,
            )?,
            timezone: timezone.as_ref().map(ToString::to_string),
        },
        other => {
            return Err(cdf_kernel::CdfError::data(format!(
                "Python cursor field `{}` has unsupported Arrow type {other}; use int64, uint64, or timestamp(us)",
                cursor.field
            )));
        }
    };
    Ok(SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: cursor.field.clone(),
        value,
    }))
}

fn max_int64(array: &Int64Array, field: &str) -> Result<i64> {
    (0..array.len())
        .filter(|index| !array.is_null(*index))
        .map(|index| array.value(index))
        .max()
        .ok_or_else(|| {
            cdf_kernel::CdfError::data(format!(
                "Python cursor field `{field}` contains no non-null values"
            ))
        })
}

fn max_timestamp_micros(array: &TimestampMicrosecondArray, field: &str) -> Result<i64> {
    (0..array.len())
        .filter(|index| !array.is_null(*index))
        .map(|index| array.value(index))
        .max()
        .ok_or_else(|| {
            cdf_kernel::CdfError::data(format!(
                "Python cursor field `{field}` contains no non-null values"
            ))
        })
}

fn max_u64(array: &UInt64Array, field: &str) -> Result<u64> {
    (0..array.len())
        .filter(|index| !array.is_null(*index))
        .map(|index| array.value(index))
        .max()
        .ok_or_else(|| {
            cdf_kernel::CdfError::data(format!(
                "Python cursor field `{field}` contains no non-null values"
            ))
        })
}

fn python_foreign_descriptor(max_boundary_bytes: u64) -> Result<ForeignProducerDescriptor> {
    let descriptor = ForeignProducerDescriptor {
        producer_id: ForeignProducerId::new("cdf.python")?,
        protocol_version: ForeignProtocolVersion::new("1")?,
        transfer_modes: vec![
            ForeignTransferMode::ArrowCData,
            ForeignTransferMode::RowCompat,
        ],
        startup: ForeignStartupModel::InProcessAttached,
        lanes: ForeignLaneCapabilities {
            execution_lane: ForeignExecutionLane::Blocking,
            maximum_internal_parallelism: 1,
            backpressure: ForeignBackpressure::HostWindow,
        },
        memory: ForeignMemoryContract {
            payload_window_bytes: Some(max_boundary_bytes),
            control_queue_bytes: None,
            diagnostic_queue_bytes: None,
            native_scratch_bytes: None,
            child_process_bytes: None,
        },
        cancellation: ForeignCancellationContract {
            cooperative_stop: true,
            interrupt_safe: false,
            force_termination_authorized: false,
            drains_on_cancel: false,
        },
        state: ForeignStateContract {
            emits_positions: true,
            emits_watermarks: false,
            emits_foreign_state: true,
            terminal_state_required: true,
        },
        security: ForeignSecurityContract {
            ambient_network: true,
            ambient_filesystem: true,
            secret_names: Vec::new(),
        },
    };
    descriptor.validate()?;
    Ok(descriptor)
}

impl ForeignProducer for PythonResource {
    fn descriptor(&self) -> &ForeignProducerDescriptor {
        &self.foreign_descriptor
    }

    fn open(
        &self,
        request: ForeignStreamOpenRequest,
    ) -> cdf_kernel::BoxFuture<'_, Result<ForeignStreamOpen>> {
        let resource = Arc::new(self.clone());
        Box::pin(async move {
            if request.resource_id != resource.descriptor.resource_id
                || request.partition_id.as_str() != PARTITION_ID
            {
                return Err(cdf_kernel::CdfError::contract(
                    "Python foreign stream request does not match the resolved resource partition",
                ));
            }
            request.cancellation.check()?;
            let execution = resource.execution.clone().ok_or_else(|| {
                cdf_kernel::CdfError::contract(
                    "Python foreign producer requires injected execution services",
                )
            })?;
            let lane = resource.blocking_lane.clone().ok_or_else(|| {
                cdf_kernel::CdfError::contract(
                    "Python foreign producer requires a resolved blocking lane",
                )
            })?;
            let descriptor = resource.foreign_descriptor.clone();
            let memory = execution.memory();
            let partition = resource.partition()?;
            let events = execution.spawn_blocking_stream(
                "python-foreign-producer",
                &lane,
                1,
                move |mut sender, cancellation| {
                    let foreign_cancellation = request.cancellation;
                    let produced = resource.produce_foreign_stream(
                        partition,
                        &mut sender,
                        &cancellation,
                        &foreign_cancellation,
                        memory,
                    );
                    let terminal = match produced {
                        Ok(final_position) => ForeignTerminalStatus::Succeeded { final_position },
                        Err(_)
                            if cancellation.is_cancelled()
                                || foreign_cancellation.is_cancelled() =>
                        {
                            ForeignTerminalStatus::Cancelled
                        }
                        Err(error) => ForeignTerminalStatus::Failed {
                            retryable: matches!(
                                error.kind,
                                ErrorKind::Transient | ErrorKind::RateLimited
                            ),
                            message: error.message,
                        },
                    };
                    match sender.send(ForeignStreamEvent::Terminal(terminal)) {
                        Err(_) if cancellation.is_cancelled() => Ok(()),
                        result => result,
                    }
                },
            )?;
            let termination = events.termination();
            Ok(ForeignStreamOpen {
                descriptor,
                events: Box::pin(events),
                termination,
            })
        })
    }
}

impl ResourceStream for PythonResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn compiled_source_plan_hash(&self) -> Option<&CompiledSourcePlanHash> {
        self.compiled_source_plan_hash.as_ref()
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_ref()
    }

    fn baseline_observation_schema_catalog(&self) -> &[EffectiveSchemaCatalogEntry] {
        &self.baseline_observation_schema_catalog
    }

    fn type_policy_allowances(&self) -> TypePolicyAllowances {
        self.type_policy_allowances
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        if request.resource_id != self.descriptor.resource_id {
            return Err(cdf_kernel::CdfError::contract(
                "Python scan request resource does not match the resolved resource",
            ));
        }
        Ok(vec![self.partition()?])
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        let Some(execution) = &self.execution else {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
                Err(cdf_kernel::CdfError::contract(
                    "Python source execution requires injected execution services",
                ))
            }));
        };
        let resource = Arc::new(self.clone());
        let request = ForeignStreamOpenRequest {
            resource_id: self.descriptor.resource_id.clone(),
            partition_id: partition.partition_id.clone(),
            cancellation: ForeignCancellation::default(),
        };
        let opened = match execution
            .run_io(async move { ForeignProducer::open(resource.as_ref(), request).await })
        {
            Ok(opened) => opened,
            Err(error) => {
                return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
                    Err(error)
                }));
            }
        };
        if opened.descriptor != self.foreign_descriptor {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
                Err(cdf_kernel::CdfError::contract(
                    "Python foreign producer changed its compiled descriptor while opening",
                ))
            }));
        }
        let termination = opened.termination.clone();
        let opening = Box::pin(async move {
            let stream = cdf_foreign_stream::batch_stream_from_foreign_events(opened.events);
            Ok(cdf_kernel::PartitionStreamPayload::new(
                stream,
                Box::pin(async { Ok(cdf_kernel::PartitionCompletion::default()) }),
            ))
        });
        cdf_kernel::PartitionOpenAttempt::with_termination(opening, termination)
    }
}

impl QueryableResource for PythonResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        let partitions = self.plan_partitions(request)?;
        Ok(ScanPlan::from_partition_authority(
            PlanId::new(format!("python-plan-{}", self.descriptor.resource_id))?,
            request.clone(),
            PartitionAuthority::Inline(partitions),
            Vec::new(),
            request.filters.clone(),
            None,
            None,
            DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        ))
    }
}

struct PythonMetadata {
    schema: Vec<(String, String, bool)>,
    primary_key: Vec<String>,
    merge_key: Vec<String>,
    cursor: Option<String>,
    bounded: bool,
    write_disposition: String,
}

fn inspect_metadata(source: &str, file_name: &str, callable_name: &str) -> Result<PythonMetadata> {
    Python::attach(|py| {
        let module = load_module(py, source, file_name)?;
        let callable = module.getattr(callable_name).map_err(|_| {
            cdf_kernel::CdfError::contract(format!(
                "Python resource target `{file_name}#{callable_name}` is missing"
            ))
        })?;
        if !callable.is_callable() {
            return Err(cdf_kernel::CdfError::contract(format!(
                "Python resource target `{file_name}#{callable_name}` is not callable"
            )));
        }
        if !callable
            .getattr("__cdf_resource__")
            .and_then(|value| value.extract::<bool>())
            .unwrap_or(false)
        {
            return Err(cdf_kernel::CdfError::contract(format!(
                "Python resource target `{file_name}#{callable_name}` must use `@cdf_sdk.resource`"
            )));
        }
        let schema = callable
            .getattr("__cdf_schema__")
            .and_then(|value| value.extract::<Vec<(String, String, bool)>>())
            .map_err(|_| {
                cdf_kernel::CdfError::contract(format!(
                    "Python resource target `{file_name}#{callable_name}` requires explicit `schema={{...}}` metadata for plan-time discovery"
                ))
            })?;
        if schema.is_empty() {
            return Err(cdf_kernel::CdfError::contract(
                "Python resource schema metadata cannot be empty",
            ));
        }
        Ok(PythonMetadata {
            schema,
            primary_key: callable
                .getattr("__cdf_primary_key__")
                .and_then(|value| value.extract())
                .map_err(py_error)?,
            merge_key: callable
                .getattr("__cdf_merge_key__")
                .and_then(|value| value.extract())
                .map_err(py_error)?,
            cursor: callable
                .getattr("__cdf_cursor__")
                .and_then(|value| value.extract())
                .map_err(py_error)?,
            bounded: callable
                .getattr("__cdf_bounded__")
                .and_then(|value| value.extract())
                .map_err(py_error)?,
            write_disposition: callable
                .getattr("__cdf_write_disposition__")
                .and_then(|value| value.extract())
                .map_err(py_error)?,
        })
    })
}

fn load_module<'py>(
    py: Python<'py>,
    source: &str,
    file_name: &str,
) -> Result<pyo3::Bound<'py, PyModule>> {
    let code = CString::new(source)
        .map_err(|_| cdf_kernel::CdfError::contract("Python module contains a NUL byte"))?;
    let file_name = CString::new(file_name)
        .map_err(|_| cdf_kernel::CdfError::contract("Python module path contains a NUL byte"))?;
    let module_name = CString::new("cdf_project_resource").unwrap();
    PyModule::from_code(py, &code, &file_name, &module_name).map_err(|_| {
        cdf_kernel::CdfError::contract(
            "Python resource module could not be imported; run `cdf doctor` and inspect the module syntax",
        )
    })
}

fn parse_python_uri(uri: &str) -> Result<(String, String)> {
    let target = uri.strip_prefix("python://").ok_or_else(|| {
        cdf_kernel::CdfError::contract("Python resource URI must start with `python://`")
    })?;
    let (module, callable) = target.split_once('#').ok_or_else(|| {
        cdf_kernel::CdfError::contract(
            "Python resource URI must use `python://project/path.py#callable`",
        )
    })?;
    if module.is_empty()
        || callable.is_empty()
        || callable.contains('#')
        || callable.contains('/')
        || callable.contains('\\')
    {
        return Err(cdf_kernel::CdfError::contract(
            "Python resource URI has an ambiguous or empty module/callable target",
        ));
    }
    Ok((module.to_owned(), callable.to_owned()))
}

fn resolve_module_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let root = root.canonicalize().map_err(|error| {
        cdf_kernel::CdfError::contract(format!("resolve project root: {error}"))
    })?;
    let candidate = root.join(relative);
    let canonical = candidate.canonicalize().map_err(|error| {
        cdf_kernel::CdfError::contract(format!(
            "Python resource module `{relative}` is missing or inaccessible: {error}"
        ))
    })?;
    if !canonical.starts_with(&root)
        || canonical.extension().and_then(|value| value.to_str()) != Some("py")
    {
        return Err(cdf_kernel::CdfError::contract(
            "Python resource module must be a project-relative `.py` file and cannot escape the project root",
        ));
    }
    Ok(canonical)
}
