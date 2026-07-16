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
    BackpressureSupport, Batch, BatchStream, CapabilitySupport, CursorOrderingClaim,
    CursorPosition, CursorSpec, CursorValue, DeliveryGuarantee, EstimateSupport,
    FilterCapabilities, ForeignState, IncrementalShape, PartitionId, PartitionPlan,
    PartitioningCapabilities, PlanId, QueryableResource, ReplaySupport, ResourceCapabilities,
    ResourceDescriptor, ResourceId, ResourceStream, Result, ScanPlan, ScanRequest, SchemaSource,
    ScopeKey, SourcePosition, TrustLevel, WriteDisposition, parse_arrow_field_type,
};
use pyo3::{
    Python,
    types::{PyAnyMethods, PyModule},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    DEFAULT_BOUNDARY_CHANNEL_BYTES, PythonBridgeOptions, PythonResourceBridge, internal::py_error,
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
    execution: Option<cdf_runtime::ExecutionServices>,
    blocking_lane: Option<String>,
    compiled_source_plan_hash: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PythonPhysicalPlan {
    pub(crate) module_relative: String,
    pub(crate) callable: String,
    pub(crate) content_hash: String,
    pub(crate) bounded: bool,
}

impl PythonResource {
    pub fn load(
        project_root: &Path,
        uri: &str,
        resource_id: ResourceId,
        trust_level: TrustLevel,
    ) -> Result<Self> {
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
                    parallel_partitions: metadata.parallel,
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
            execution: None,
            blocking_lane: None,
            compiled_source_plan_hash: None,
        })
    }

    pub(crate) fn physical_plan(&self) -> PythonPhysicalPlan {
        PythonPhysicalPlan {
            module_relative: self.module_relative.clone(),
            callable: self.callable.clone(),
            content_hash: self.content_hash.clone(),
            bounded: self.bounded,
        }
    }

    pub(crate) fn from_compiled(
        project_root: &Path,
        descriptor: ResourceDescriptor,
        schema: SchemaRef,
        capabilities: ResourceCapabilities,
        physical: PythonPhysicalPlan,
        compiled_source_plan_hash: String,
    ) -> Result<Self> {
        let module_path = resolve_module_path(project_root, &physical.module_relative)?;
        Ok(Self {
            descriptor,
            schema,
            capabilities,
            module_path,
            module_relative: physical.module_relative,
            callable: physical.callable,
            content_hash: physical.content_hash,
            bounded: physical.bounded,
            execution: None,
            blocking_lane: None,
            compiled_source_plan_hash: Some(compiled_source_plan_hash),
        })
    }

    pub fn with_execution_services(
        mut self,
        execution: cdf_runtime::ExecutionServices,
    ) -> Result<Self> {
        let host = execution.capabilities();
        let interpreter = crate::attached_interpreter_report()?;
        let semantics = crate::execution_semantics(
            &interpreter,
            self.capabilities.partitioning.parallel_partitions,
            usize::from(host.logical_cpu_slots),
        );
        let lane = crate::python_execution_lane_spec(&semantics);
        execution.ensure_blocking_lanes(std::slice::from_ref(&lane))?;
        self.execution = Some(execution);
        self.blocking_lane = Some(lane.lane_id);
        Ok(self)
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
        Ok(PartitionPlan {
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
        })
    }

    fn execute_stream(
        &self,
        partition: PartitionPlan,
        mut sender: cdf_runtime::BlockingTaskStreamSender<Batch>,
        cancellation: cdf_runtime::RunCancellation,
        memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    ) -> Result<()> {
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
        let mut reservation = Some(reserve_python_batch(Arc::clone(&memory))?);
        Python::attach(|py| -> Result<_> {
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
            ))
            .visit_python_iterable(&iterable, |mut batch, _kind| {
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
                let retained_bytes = batch
                    .record_batch()
                    .map(cdf_memory::record_batch_retained_bytes)
                    .transpose()?
                    .unwrap_or(0)
                    .checked_add(batch.header.pre_contract_evidence_retained_bytes()?)
                    .ok_or_else(|| {
                        cdf_kernel::CdfError::data("Python batch retained memory exceeds u64")
                    })?;
                if retained_bytes == 0 || retained_bytes > DEFAULT_BOUNDARY_CHANNEL_BYTES {
                    return Err(cdf_kernel::CdfError::data(format!(
                        "Python source batch retains {retained_bytes} bytes outside its compiled 1..={DEFAULT_BOUNDARY_CHANNEL_BYTES}-byte limit; emit smaller Arrow batches or lower dict_batch_rows"
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
                sender.send(batch)?;
                cancellation.check()?;
                reservation = Some(reserve_python_batch(Arc::clone(&memory))?);
                Ok(())
            })?;
            Ok(())
        })?;
        Ok(())
    }
}

fn reserve_python_batch(
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
) -> Result<cdf_memory::MemoryLease> {
    cdf_memory::reserve_blocking(
        memory,
        &cdf_memory::ReservationRequest::new(
            cdf_memory::ConsumerKey::new("python-source-batch", cdf_memory::MemoryClass::Source)?,
            DEFAULT_BOUNDARY_CHANNEL_BYTES,
        )?,
    )
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

impl ResourceStream for PythonResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn compiled_source_plan_hash(&self) -> Option<&str> {
        self.compiled_source_plan_hash.as_deref()
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
        let (Some(execution), Some(lane)) = (&self.execution, &self.blocking_lane) else {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
                Err(cdf_kernel::CdfError::contract(
                    "Python source execution requires injected execution services",
                ))
            }));
        };
        let resource = self.clone();
        let memory = execution.memory();
        let task = match execution.spawn_blocking_stream(
            "python-source-open",
            lane,
            1,
            move |sender, cancellation| {
                cancellation.check()?;
                resource.execute_stream(partition, sender, cancellation.clone(), memory)?;
                cancellation.check()?;
                Ok(())
            },
        ) {
            Ok(task) => task,
            Err(error) => {
                return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
                    Err(error)
                }));
            }
        };
        let termination = task.termination();
        let opening = Box::pin(async move {
            let stream = Box::pin(task) as BatchStream;
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
        Ok(ScanPlan {
            plan_id: PlanId::new(format!("python-plan-{}", self.descriptor.resource_id))?,
            request: request.clone(),
            partitions,
            pushed_predicates: Vec::new(),
            unsupported_predicates: request.filters.clone(),
            estimated_rows: None,
            estimated_bytes: None,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        })
    }
}

struct PythonMetadata {
    schema: Vec<(String, String, bool)>,
    primary_key: Vec<String>,
    merge_key: Vec<String>,
    cursor: Option<String>,
    parallel: bool,
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
            parallel: callable
                .getattr("__cdf_parallel__")
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
