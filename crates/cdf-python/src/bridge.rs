use crate::internal::*;
use crate::*;
use std::{io::Cursor, sync::Arc};

use arrow_array::RecordBatch;
use arrow_json::reader::{ReaderBuilder as JsonReaderBuilder, infer_json_schema};
use cdf_foreign_stream::{ForeignBatchOutcome, ForeignCopyClassification, ForeignTransferMode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PythonBridgeOptions {
    pub resource_id: ResourceId,
    pub partition_id: PartitionId,
    pub batch_id_prefix: String,
    pub dict_batch_rows: usize,
    pub max_boundary_bytes: u64,
}

impl PythonBridgeOptions {
    pub fn new(resource_id: ResourceId, partition_id: PartitionId) -> Self {
        let batch_id_prefix = format!(
            "{}-{}",
            sanitize_id_part(resource_id.as_str()),
            sanitize_id_part(partition_id.as_str())
        );
        Self {
            resource_id,
            partition_id,
            batch_id_prefix,
            dict_batch_rows: DEFAULT_DICT_BATCH_ROWS,
            max_boundary_bytes: DEFAULT_MAX_BOUNDARY_BYTES,
        }
    }

    pub fn with_dict_batch_rows(mut self, dict_batch_rows: usize) -> Result<Self> {
        if dict_batch_rows == 0 {
            return Err(CdfError::contract(
                "dict batch rows must be greater than zero",
            ));
        }
        self.dict_batch_rows = dict_batch_rows;
        Ok(self)
    }

    pub fn with_max_boundary_bytes(mut self, max_boundary_bytes: u64) -> Result<Self> {
        if max_boundary_bytes < 2 {
            return Err(CdfError::contract(
                "Python boundary byte limit must be at least 2 bytes",
            ));
        }
        self.max_boundary_bytes = max_boundary_bytes;
        Ok(self)
    }

    pub fn with_resource_id(mut self, resource_id: ResourceId) -> Self {
        self.resource_id = resource_id;
        self.batch_id_prefix = format!(
            "{}-{}",
            sanitize_id_part(self.resource_id.as_str()),
            sanitize_id_part(self.partition_id.as_str())
        );
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PythonYieldKind {
    DictRows,
    ArrowCArray,
    ArrowCStream,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrowCapsuleBoundary {
    pub kind: PythonYieldKind,
    pub method: String,
    pub capsule_names: Vec<String>,
    pub zero_copy_intent: bool,
}

impl ArrowCapsuleBoundary {
    pub fn for_c_array() -> Self {
        Self {
            kind: PythonYieldKind::ArrowCArray,
            method: ARROW_C_ARRAY_METHOD.to_owned(),
            capsule_names: vec!["arrow_schema".to_owned(), "arrow_array".to_owned()],
            zero_copy_intent: true,
        }
    }

    pub fn for_c_stream() -> Self {
        Self {
            kind: PythonYieldKind::ArrowCStream,
            method: ARROW_C_STREAM_METHOD.to_owned(),
            capsule_names: vec!["arrow_array_stream".to_owned()],
            zero_copy_intent: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PythonFirstObservation {
    pub descriptor: ResourceDescriptor,
    pub schema_hash: SchemaHash,
}

#[derive(Clone, Debug, Default)]
pub struct PythonStreamSummary {
    pub first_observation: Option<PythonFirstObservation>,
    pub outcome_count: u64,
    pub row_count: u64,
    pub byte_count: u64,
    pub peak_boundary_bytes: u64,
    pub dict_row_outcomes: u64,
    pub arrow_c_array_outcomes: u64,
    pub arrow_c_stream_outcomes: u64,
}

impl PythonStreamSummary {
    fn observe(
        &mut self,
        schema_hash: SchemaHash,
        options: &PythonBridgeOptions,
        kind: PythonYieldKind,
        rows: u64,
        bytes: u64,
        boundary_bytes: u64,
    ) -> Result<()> {
        if self.first_observation.is_none() {
            self.first_observation = Some(PythonFirstObservation {
                descriptor: descriptor_for(
                    options.resource_id.clone(),
                    ScopeKey::Stream {
                        name: "python_arrow_capsule".to_owned(),
                    },
                    schema_hash.clone(),
                ),
                schema_hash,
            });
        }
        self.outcome_count = self
            .outcome_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Python outcome count exceeds u64"))?;
        self.row_count = self
            .row_count
            .checked_add(rows)
            .ok_or_else(|| CdfError::data("Python row count exceeds u64"))?;
        self.byte_count = self
            .byte_count
            .checked_add(bytes)
            .ok_or_else(|| CdfError::data("Python byte count exceeds u64"))?;
        self.peak_boundary_bytes = self.peak_boundary_bytes.max(boundary_bytes);
        let counter = match kind {
            PythonYieldKind::DictRows => &mut self.dict_row_outcomes,
            PythonYieldKind::ArrowCArray => &mut self.arrow_c_array_outcomes,
            PythonYieldKind::ArrowCStream => &mut self.arrow_c_stream_outcomes,
        };
        *counter = counter
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Python yield-kind count exceeds u64"))?;
        Ok(())
    }
}

#[derive(Default)]
struct PythonBridgeState {
    summary: PythonStreamSummary,
    next_batch_index: usize,
    next_outcome_sequence: u64,
}

impl PythonBridgeState {
    fn emit_record_batch<F>(
        &mut self,
        record_batch: RecordBatch,
        kind: PythonYieldKind,
        boundary_peak_bytes: Option<u64>,
        options: &PythonBridgeOptions,
        emit: &mut F,
    ) -> Result<()>
    where
        F: FnMut(ForeignBatchOutcome, PythonYieldKind) -> Result<()>,
    {
        let observed_schema_hash =
            cdf_kernel::canonical_arrow_schema_hash(record_batch.schema().as_ref())?;
        let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
        if retained_bytes == 0 || retained_bytes > options.max_boundary_bytes {
            return Err(CdfError::data(format!(
                "Python Arrow batch retains {retained_bytes} bytes outside its compiled 1..={}-byte boundary; emit smaller Arrow batches or raise max_boundary_bytes",
                options.max_boundary_bytes
            )));
        }
        self.next_batch_index = self
            .next_batch_index
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Python batch index exceeds usize"))?;
        self.next_outcome_sequence = self
            .next_outcome_sequence
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Python outcome sequence exceeds u64"))?;
        let batch = Batch::from_record_batch(
            batch_id(options, self.next_batch_index)?,
            options.resource_id.clone(),
            options.partition_id.clone(),
            observed_schema_hash.clone(),
            record_batch,
        )?;
        let rows = batch.header.row_count;
        let bytes = batch.header.byte_count;
        let outcome = python_foreign_outcome(self.next_outcome_sequence, batch, kind)?;
        emit(outcome, kind)?;
        self.summary.observe(
            observed_schema_hash,
            options,
            kind,
            rows,
            bytes,
            boundary_peak_bytes.unwrap_or(retained_bytes),
        )
    }

    fn finish(self) -> PythonStreamSummary {
        self.summary
    }
}

#[derive(Default)]
struct DictRowWindow {
    rows: usize,
    bytes: Vec<u8>,
}

impl DictRowWindow {
    fn try_push(&mut self, row: &str, maximum_bytes: u64) -> Result<bool> {
        let row_bytes = u64::try_from(row.len())
            .map_err(|_| CdfError::data("Python dict row length exceeds u64"))?;
        let current = u64::try_from(self.bytes.len())
            .map_err(|_| CdfError::data("Python dict conversion window exceeds u64"))?;
        let required = current
            .checked_add(row_bytes)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| CdfError::data("Python dict conversion window size overflowed"))?;
        let current_capacity = u64::try_from(self.bytes.capacity())
            .map_err(|_| CdfError::data("Python dict conversion capacity exceeds u64"))?;
        let maximum_buffer_capacity = maximum_bytes.saturating_sub(row_bytes);
        if required > maximum_buffer_capacity {
            return Ok(false);
        }
        let minimum_peak = current_capacity
            .max(required)
            .checked_add(row_bytes)
            .ok_or_else(|| CdfError::data("Python dict conversion peak size overflowed"))?;
        if minimum_peak > maximum_bytes {
            return Ok(false);
        }
        if required > current_capacity {
            let geometric = current_capacity
                .checked_mul(2)
                .unwrap_or(maximum_buffer_capacity)
                .max(1);
            let target_capacity = required.max(geometric).min(maximum_buffer_capacity);
            self.bytes
                .try_reserve_exact(target_capacity.saturating_sub(current).try_into().map_err(
                    |_| CdfError::data("Python dict conversion reservation exceeds usize"),
                )?)
                .map_err(|error| {
                    CdfError::data(format!("reserve Python dict conversion window: {error}"))
                })?;
        }
        let admitted_peak = u64::try_from(self.bytes.capacity())
            .map_err(|_| CdfError::data("Python dict conversion capacity exceeds u64"))?
            .checked_add(row_bytes)
            .ok_or_else(|| CdfError::data("Python dict conversion peak size overflowed"))?;
        if admitted_peak > maximum_bytes {
            return Ok(false);
        }
        self.bytes.extend_from_slice(row.as_bytes());
        self.bytes.push(b'\n');
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Python dict row count exceeds usize"))?;
        Ok(true)
    }
}

impl PythonFirstObservation {
    fn apply_descriptor_metadata(&mut self, metadata: &DltBridgeMetadata) -> Result<()> {
        metadata.apply_to_descriptor(&mut self.descriptor)
    }
}

impl PythonStreamSummary {
    fn apply_descriptor_metadata(&mut self, metadata: &DltBridgeMetadata) -> Result<()> {
        if let Some(observation) = self.first_observation.as_mut() {
            observation.apply_descriptor_metadata(metadata)?;
        }
        Ok(())
    }

    pub fn descriptor(&self) -> Option<&ResourceDescriptor> {
        self.first_observation
            .as_ref()
            .map(|observation| &observation.descriptor)
    }

    pub fn first_schema_hash(&self) -> Option<&SchemaHash> {
        self.first_observation
            .as_ref()
            .map(|observation| &observation.schema_hash)
    }

    pub fn empty() -> Self {
        Self { ..Self::default() }
    }
}

#[derive(Clone, Debug)]
pub struct PythonResourceBridge {
    options: PythonBridgeOptions,
}

impl PythonResourceBridge {
    pub fn new(options: PythonBridgeOptions) -> Self {
        Self { options }
    }

    pub fn options(&self) -> &PythonBridgeOptions {
        &self.options
    }

    pub fn visit_json_dict_rows<I, F>(&self, rows: I, mut emit: F) -> Result<PythonStreamSummary>
    where
        I: IntoIterator<Item = serde_json::Value>,
        F: FnMut(ForeignBatchOutcome, PythonYieldKind) -> Result<()> + Send,
    {
        let mut state = PythonBridgeState::default();
        let mut window = DictRowWindow::default();
        for row in rows {
            if !row.is_object() {
                return Err(CdfError::data(
                    "Python dict batching accepts JSON objects only",
                ));
            }
            let row = serde_json::to_string(&row).map_err(json_error)?;
            self.push_json_row_with(
                &mut window,
                &mut state,
                &row,
                &mut emit,
                &mut |window, state, transient_bytes, emit| {
                    self.flush_json_rows(window, state, transient_bytes, emit)
                },
            )?;
        }

        self.flush_json_rows(&mut window, &mut state, 0, &mut emit)?;
        Ok(state.finish())
    }

    /// Incrementally imports one Python iterator as neutral foreign-stream outcomes.
    /// Callbacks run without the GIL so host backpressure and memory admission do not stall
    /// unrelated Python partitions.
    pub fn visit_python_foreign_iterable<F>(
        &self,
        iterable: &Bound<'_, PyAny>,
        mut emit: F,
    ) -> Result<PythonStreamSummary>
    where
        F: FnMut(ForeignBatchOutcome, PythonYieldKind) -> Result<()> + Send,
    {
        let py = iterable.py();
        let mut state = PythonBridgeState::default();
        let mut window = DictRowWindow::default();
        let iterator = iterable.try_iter().map_err(py_error)?;

        for item in iterator {
            let item = item.map_err(py_error)?;
            match arrow_boundary_for(&item)? {
                Some(boundary) if boundary.kind == PythonYieldKind::ArrowCStream => {
                    py.detach(|| self.flush_json_rows(&mut window, &mut state, 0, &mut emit))?;
                    let reader = import_arrow_stream(&item)?;
                    for batch in reader {
                        let batch = batch.map_err(|_| {
                            CdfError::data(
                                "Python Arrow C stream failed while producing a batch; inspect the Python resource locally for exception details",
                            )
                        })?;
                        py.detach(|| {
                            state.emit_record_batch(
                                batch,
                                PythonYieldKind::ArrowCStream,
                                None,
                                &self.options,
                                &mut emit,
                            )
                        })?;
                    }
                }
                Some(boundary) if boundary.kind == PythonYieldKind::ArrowCArray => {
                    py.detach(|| self.flush_json_rows(&mut window, &mut state, 0, &mut emit))?;
                    let batch = arrow_capsule::import_record_batch(&item).map_err(py_error)?;
                    py.detach(|| {
                        state.emit_record_batch(
                            batch,
                            PythonYieldKind::ArrowCArray,
                            None,
                            &self.options,
                            &mut emit,
                        )
                    })?;
                }
                Some(_) => unreachable!("arrow boundary kinds are exhausted"),
                None if item.cast::<PyDict>().is_ok() => {
                    let row = python_dict_to_json(py, &item)?;
                    self.push_json_row_with(
                        &mut window,
                        &mut state,
                        &row,
                        &mut emit,
                        &mut |window, state, transient_bytes, emit| {
                            py.detach(|| self.flush_json_rows(window, state, transient_bytes, emit))
                        },
                    )?;
                }
                None => {
                    return Err(CdfError::data(
                        "Python resource yielded unsupported value; expected dict or Arrow PyCapsule-speaking object",
                    ));
                }
            }
        }

        py.detach(|| self.flush_json_rows(&mut window, &mut state, 0, &mut emit))?;
        Ok(state.finish())
    }

    pub fn visit_dlt_resource<F>(
        &self,
        resource: &Bound<'_, PyAny>,
        mut emit: F,
    ) -> Result<DltBridgeSummary>
    where
        F: FnMut(ForeignBatchOutcome, PythonYieldKind) -> Result<()> + Send,
    {
        let metadata = extract_dlt_metadata(resource)?.ok_or_else(|| {
            CdfError::contract(
                "dlt preview requires cdf dlt bridge metadata on the resource object",
            )
        })?;
        if metadata.kind != DltBridgeObjectKind::Resource {
            return Err(CdfError::contract(
                "dlt preview expected resource metadata; use visit_dlt_source for sources",
            ));
        }
        let bridge = self.bridge_for_dlt_metadata(&metadata)?;
        let iterable = materialize_dlt_resource(resource)?;
        let mut stream = bridge.visit_python_foreign_iterable(&iterable, &mut emit)?;
        stream.apply_descriptor_metadata(&metadata)?;
        Ok(DltBridgeSummary {
            mapping_table: metadata.mapping_table(),
            metadata,
            stream,
        })
    }

    pub fn visit_dlt_source<F>(
        &self,
        source: &Bound<'_, PyAny>,
        mut emit: F,
    ) -> Result<Vec<DltBridgeSummary>>
    where
        F: FnMut(&DltBridgeMetadata, ForeignBatchOutcome, PythonYieldKind) -> Result<()> + Send,
    {
        let source_metadata = extract_dlt_metadata(source)?;
        if let Some(metadata) = &source_metadata
            && metadata.kind == DltBridgeObjectKind::Resource
        {
            return self
                .visit_dlt_resource(source, |outcome, kind| emit(metadata, outcome, kind))
                .map(|summary| vec![summary]);
        }
        let source_name = source_metadata
            .as_ref()
            .and_then(|metadata| metadata.resource_id_hint())
            .map(ToOwned::to_owned);

        let source_output = if source.hasattr("__call__").map_err(py_error)? {
            source.call0().map_err(py_error)?
        } else {
            source.clone()
        };
        if let Some(mut metadata) = extract_dlt_metadata(&source_output)?
            && metadata.kind == DltBridgeObjectKind::Resource
        {
            if metadata.source_name.is_none() {
                metadata.source_name.clone_from(&source_name);
            }
            let mut summary = self.visit_dlt_resource(&source_output, |outcome, kind| {
                emit(&metadata, outcome, kind)
            })?;
            summary
                .metadata
                .source_name
                .clone_from(&metadata.source_name);
            return Ok(vec![summary]);
        }

        let mut summaries = Vec::new();
        let iterator = source_output.try_iter().map_err(py_error)?;
        for item in iterator {
            let item = item.map_err(py_error)?;
            let Some(mut metadata) = extract_dlt_metadata(&item)? else {
                return Err(CdfError::contract(
                    "dlt source yielded an object without CDF resource metadata",
                ));
            };
            if metadata.kind != DltBridgeObjectKind::Resource {
                return Err(CdfError::contract(
                    "dlt source yielded nested source metadata where resource metadata was required",
                ));
            }
            if !metadata.selected_for_source_expansion() {
                continue;
            }
            if metadata.source_name.is_none() {
                metadata.source_name.clone_from(&source_name);
            }
            let mut summary =
                self.visit_dlt_resource(&item, |outcome, kind| emit(&metadata, outcome, kind))?;
            summary
                .metadata
                .source_name
                .clone_from(&metadata.source_name);
            summaries.push(summary);
        }
        Ok(summaries)
    }

    fn bridge_for_dlt_metadata(&self, metadata: &DltBridgeMetadata) -> Result<Self> {
        let Some(resource_id) = metadata.resource_id_hint() else {
            return Ok(self.clone());
        };
        Ok(Self::new(
            self.options
                .clone()
                .with_resource_id(ResourceId::new(resource_id)?),
        ))
    }

    fn flush_json_rows<F>(
        &self,
        window: &mut DictRowWindow,
        state: &mut PythonBridgeState,
        transient_bytes: u64,
        emit: &mut F,
    ) -> Result<()>
    where
        F: FnMut(ForeignBatchOutcome, PythonYieldKind) -> Result<()>,
    {
        if window.rows == 0 {
            return Ok(());
        }
        let rows = std::mem::take(&mut window.rows);
        let bytes = std::mem::take(&mut window.bytes);
        let input_capacity = u64::try_from(bytes.capacity())
            .map_err(|_| CdfError::data("Python dict input capacity exceeds u64"))?;
        let (schema, _) = infer_json_schema(Cursor::new(bytes.as_slice()), Some(rows)).map_err(
            |_| {
                CdfError::data(
                    "infer Python dict-row schema failed; inspect the Python resource locally for the offending value",
                )
            },
        )?;
        let mut reader = JsonReaderBuilder::new(Arc::new(schema))
            .with_batch_size(rows)
            .build(Cursor::new(bytes.as_slice()))
            .map_err(|_| {
                CdfError::data(
                    "initialize Python dict-row decoder failed; inspect the Python resource locally",
                )
            })?;
        let record_batch = reader
            .next()
            .transpose()
            .map_err(|_| {
                CdfError::data(
                    "decode Python dict rows failed; inspect the Python resource locally for the offending value",
                )
            })?
            .ok_or_else(|| CdfError::data("Python dict-row decoder emitted no batch"))?;
        if reader
            .next()
            .transpose()
            .map_err(|_| {
                CdfError::data(
                    "decode Python dict rows failed; inspect the Python resource locally for the offending value",
                )
            })?
            .is_some()
        {
            return Err(CdfError::internal(
                "one Python dict conversion window emitted more than one Arrow batch",
            ));
        }
        drop(reader);
        let output_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
        let peak_bytes = input_capacity
            .checked_add(output_bytes)
            .and_then(|bytes| bytes.checked_add(transient_bytes))
            .ok_or_else(|| CdfError::data("Python dict boundary peak exceeds u64"))?;
        if peak_bytes > self.options.max_boundary_bytes {
            return Err(CdfError::data(format!(
                "Python dict conversion input plus Arrow output requires {peak_bytes} bytes but the boundary limit is {} bytes; lower dict_batch_rows or raise max_boundary_bytes",
                self.options.max_boundary_bytes
            )));
        }
        drop(bytes);
        state.emit_record_batch(
            record_batch,
            PythonYieldKind::DictRows,
            Some(peak_bytes),
            &self.options,
            emit,
        )
    }

    fn push_json_row_with<F, G>(
        &self,
        window: &mut DictRowWindow,
        state: &mut PythonBridgeState,
        row: &str,
        emit: &mut F,
        flush: &mut G,
    ) -> Result<()>
    where
        F: FnMut(ForeignBatchOutcome, PythonYieldKind) -> Result<()>,
        G: FnMut(&mut DictRowWindow, &mut PythonBridgeState, u64, &mut F) -> Result<()>,
    {
        if !window.try_push(row, self.options.max_boundary_bytes)? {
            if window.rows == 0 {
                let row_bytes = u64::try_from(row.len())
                    .map_err(|_| CdfError::data("Python dict row length exceeds u64"))?;
                return Err(CdfError::data(format!(
                    "one Python dict row and its serialized conversion require more than the {}-byte boundary limit (serialized row: {row_bytes} bytes); raise max_boundary_bytes",
                    self.options.max_boundary_bytes
                )));
            }
            let row_bytes = u64::try_from(row.len())
                .map_err(|_| CdfError::data("Python dict row length exceeds u64"))?;
            flush(window, state, row_bytes, emit)?;
            if !window.try_push(row, self.options.max_boundary_bytes)? {
                return Err(CdfError::internal(
                    "Python dict row did not fit an empty admitted conversion window",
                ));
            }
        }
        if window.rows == self.options.dict_batch_rows {
            let row_bytes = u64::try_from(row.len())
                .map_err(|_| CdfError::data("Python dict row length exceeds u64"))?;
            flush(window, state, row_bytes, emit)?;
        }
        Ok(())
    }
}

fn python_foreign_outcome(
    sequence: u64,
    batch: Batch,
    kind: PythonYieldKind,
) -> Result<ForeignBatchOutcome> {
    let transfer_mode = match kind {
        PythonYieldKind::DictRows => ForeignTransferMode::RowCompat,
        PythonYieldKind::ArrowCArray | PythonYieldKind::ArrowCStream => {
            ForeignTransferMode::ArrowCData
        }
    };
    let copy = match kind {
        PythonYieldKind::DictRows => {
            if batch.header.byte_count == 0 {
                ForeignCopyClassification::CopyUnknown
            } else {
                ForeignCopyClassification::payload_copy_known(batch.header.byte_count)?
            }
        }
        PythonYieldKind::ArrowCArray | PythonYieldKind::ArrowCStream => {
            ForeignCopyClassification::CopyUnknown
        }
    };
    ForeignBatchOutcome::new(sequence, batch, transfer_mode, copy)
}

fn materialize_dlt_resource<'py>(resource: &Bound<'py, PyAny>) -> Result<Bound<'py, PyAny>> {
    if resource.hasattr("__call__").map_err(py_error)? {
        resource.call0().map_err(py_error)
    } else {
        Ok(resource.clone())
    }
}

pub fn arrow_boundary_for(object: &Bound<'_, PyAny>) -> Result<Option<ArrowCapsuleBoundary>> {
    if object.hasattr(ARROW_C_ARRAY_METHOD).map_err(py_error)? {
        Ok(Some(ArrowCapsuleBoundary::for_c_array()))
    } else if object.hasattr(ARROW_C_STREAM_METHOD).map_err(py_error)? {
        Ok(Some(ArrowCapsuleBoundary::for_c_stream()))
    } else {
        Ok(None)
    }
}
