use crate::internal::*;
use crate::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PythonBridgeOptions {
    pub resource_id: ResourceId,
    pub partition_id: PartitionId,
    pub batch_id_prefix: String,
    pub dict_batch_rows: usize,
    pub max_boundary_bytes: u64,
    pub watchdog_ms: u64,
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
            max_boundary_bytes: DEFAULT_BOUNDARY_CHANNEL_BYTES,
            watchdog_ms: DEFAULT_WATCHDOG_MS,
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
        if max_boundary_bytes == 0 {
            return Err(CdfError::contract(
                "boundary channel byte limit must be greater than zero",
            ));
        }
        self.max_boundary_bytes = max_boundary_bytes;
        Ok(self)
    }

    pub fn with_watchdog_ms(mut self, watchdog_ms: u64) -> Result<Self> {
        if watchdog_ms == 0 {
            return Err(CdfError::contract(
                "python watchdog must be greater than zero milliseconds",
            ));
        }
        self.watchdog_ms = watchdog_ms;
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

    fn read_options(&self) -> Result<ReadOptions> {
        ReadOptions::new(self.resource_id.clone(), self.partition_id.clone())
            .with_batch_id_prefix(self.batch_id_prefix.clone())?
            .with_batch_size(self.dict_batch_rows)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug)]
pub struct PythonBatchRead {
    pub descriptor: Option<ResourceDescriptor>,
    pub schema_hash: Option<SchemaHash>,
    pub batches: Vec<Batch>,
    pub yield_kinds: Vec<PythonYieldKind>,
}

impl PythonBatchRead {
    pub fn empty() -> Self {
        Self {
            descriptor: None,
            schema_hash: None,
            batches: Vec::new(),
            yield_kinds: Vec::new(),
        }
    }

    pub fn row_count(&self) -> u64 {
        self.batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum()
    }

    pub fn byte_count(&self) -> u64 {
        self.batches
            .iter()
            .map(|batch| batch.header.byte_count)
            .sum()
    }

    fn push_read(
        &mut self,
        read: FormatRead,
        kind: PythonYieldKind,
        options: &PythonBridgeOptions,
        next_batch_index: &mut usize,
    ) -> Result<()> {
        self.remember_descriptor(read.descriptor, read.schema_hash.clone())?;
        for mut batch in read.batches {
            *next_batch_index += 1;
            batch.header.batch_id = batch_id(options, *next_batch_index)?;
            self.batches.push(batch);
            self.yield_kinds.push(kind.clone());
        }
        Ok(())
    }

    fn push_record_batches(
        &mut self,
        record_batches: Vec<RecordBatch>,
        kind: PythonYieldKind,
        options: &PythonBridgeOptions,
        next_batch_index: &mut usize,
    ) -> Result<()> {
        let Some(first_batch) = record_batches.first() else {
            return Ok(());
        };
        let schema = first_batch.schema();
        let observed_schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?;
        let descriptor = descriptor_for(
            options.resource_id.clone(),
            ScopeKey::Stream {
                name: "python_arrow_capsule".to_owned(),
            },
            observed_schema_hash.clone(),
        );
        self.remember_descriptor(descriptor, observed_schema_hash.clone())?;

        for record_batch in record_batches {
            if record_batch.schema().as_ref() != schema.as_ref() {
                return Err(CdfError::data(
                    "Python Arrow capsule yielded record batches with different schemas",
                ));
            }
            *next_batch_index += 1;
            let batch = Batch::from_record_batch(
                batch_id(options, *next_batch_index)?,
                options.resource_id.clone(),
                options.partition_id.clone(),
                observed_schema_hash.clone(),
                record_batch,
            )?;
            self.batches.push(batch);
            self.yield_kinds.push(kind.clone());
        }
        Ok(())
    }

    fn remember_descriptor(
        &mut self,
        descriptor: ResourceDescriptor,
        schema_hash: SchemaHash,
    ) -> Result<()> {
        match &self.schema_hash {
            Some(existing) if existing != &schema_hash => Err(CdfError::data(
                "Python resource yielded multiple observed schemas in one boundary read",
            )),
            Some(_) => Ok(()),
            None => {
                self.descriptor = Some(descriptor);
                self.schema_hash = Some(schema_hash);
                Ok(())
            }
        }
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

    pub fn batches_from_json_dict_rows<I>(&self, rows: I) -> Result<PythonBatchRead>
    where
        I: IntoIterator<Item = serde_json::Value>,
    {
        let mut json_rows = Vec::new();
        for row in rows {
            if !row.is_object() {
                return Err(CdfError::data(
                    "Python dict batching accepts JSON objects only",
                ));
            }
            json_rows.push(serde_json::to_string(&row).map_err(json_error)?);
        }

        let mut read = PythonBatchRead::empty();
        let mut next_batch_index = 0;
        self.flush_json_rows(&mut json_rows, &mut read, &mut next_batch_index)?;
        Ok(read)
    }

    pub fn batches_from_python_iterable(
        &self,
        iterable: &Bound<'_, PyAny>,
    ) -> Result<PythonBatchRead> {
        let py = iterable.py();
        let mut read = PythonBatchRead::empty();
        let mut json_rows = Vec::new();
        let mut next_batch_index = 0;
        let iterator = iterable.try_iter().map_err(py_error)?;

        for item in iterator {
            let item = item.map_err(py_error)?;
            match arrow_boundary_for(&item)? {
                Some(boundary) if boundary.kind == PythonYieldKind::ArrowCStream => {
                    self.flush_json_rows(&mut json_rows, &mut read, &mut next_batch_index)?;
                    let batches = import_arrow_stream(&item)?;
                    read.push_record_batches(
                        batches,
                        PythonYieldKind::ArrowCStream,
                        &self.options,
                        &mut next_batch_index,
                    )?;
                }
                Some(boundary) if boundary.kind == PythonYieldKind::ArrowCArray => {
                    self.flush_json_rows(&mut json_rows, &mut read, &mut next_batch_index)?;
                    let batch = item
                        .extract::<PyRecordBatch>()
                        .map(PyRecordBatch::into_inner)
                        .map_err(py_error)?;
                    read.push_record_batches(
                        vec![batch],
                        PythonYieldKind::ArrowCArray,
                        &self.options,
                        &mut next_batch_index,
                    )?;
                }
                Some(_) => unreachable!("arrow boundary kinds are exhausted"),
                None if item.cast::<PyDict>().is_ok() => {
                    json_rows.push(python_dict_to_json(py, &item)?);
                }
                None => {
                    return Err(CdfError::data(
                        "Python resource yielded unsupported value; expected dict or Arrow PyCapsule-speaking object",
                    ));
                }
            }
        }

        self.flush_json_rows(&mut json_rows, &mut read, &mut next_batch_index)?;
        Ok(read)
    }

    pub fn batches_from_dlt_resource(&self, resource: &Bound<'_, PyAny>) -> Result<DltShimRead> {
        let metadata = extract_dlt_metadata(resource)?.ok_or_else(|| {
            CdfError::contract("dlt preview requires cdf dlt shim metadata on the resource object")
        })?;
        if metadata.kind != DltShimObjectKind::Resource {
            return Err(CdfError::contract(
                "dlt preview expected resource metadata; use batches_from_dlt_source for sources",
            ));
        }
        let bridge = self.bridge_for_dlt_metadata(&metadata)?;
        let iterable = materialize_dlt_resource(resource)?;
        let mut read = bridge.batches_from_python_iterable(&iterable)?;
        if let Some(descriptor) = read.descriptor.as_mut() {
            metadata.apply_to_descriptor(descriptor)?;
        }
        Ok(DltShimRead {
            migration_table: metadata.migration_table(),
            metadata,
            read,
        })
    }

    pub fn batches_from_dlt_source(&self, source: &Bound<'_, PyAny>) -> Result<Vec<DltShimRead>> {
        let source_metadata = extract_dlt_metadata(source)?;
        if let Some(metadata) = &source_metadata
            && metadata.kind == DltShimObjectKind::Resource
        {
            return self
                .batches_from_dlt_resource(source)
                .map(|read| vec![read]);
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
        if let Some(metadata) = extract_dlt_metadata(&source_output)?
            && metadata.kind == DltShimObjectKind::Resource
        {
            let mut read = self.batches_from_dlt_resource(&source_output)?;
            if read.metadata.source_name.is_none() {
                read.metadata.source_name.clone_from(&source_name);
            }
            return Ok(vec![read]);
        }

        let mut reads = Vec::new();
        let iterator = source_output.try_iter().map_err(py_error)?;
        for item in iterator {
            let item = item.map_err(py_error)?;
            if let Some(metadata) = extract_dlt_metadata(&item)?
                && metadata.kind == DltShimObjectKind::Resource
                && !metadata.selected_for_source_expansion()
            {
                continue;
            }
            let mut read = self.batches_from_dlt_resource(&item)?;
            if read.metadata.source_name.is_none() {
                read.metadata.source_name.clone_from(&source_name);
            }
            reads.push(read);
        }
        Ok(reads)
    }

    fn bridge_for_dlt_metadata(&self, metadata: &DltShimMetadata) -> Result<Self> {
        let Some(resource_id) = metadata.resource_id_hint() else {
            return Ok(self.clone());
        };
        Ok(Self::new(
            self.options
                .clone()
                .with_resource_id(ResourceId::new(resource_id)?),
        ))
    }

    fn flush_json_rows(
        &self,
        json_rows: &mut Vec<String>,
        read: &mut PythonBatchRead,
        next_batch_index: &mut usize,
    ) -> Result<()> {
        if json_rows.is_empty() {
            return Ok(());
        }

        let mut bytes = Vec::new();
        for row in json_rows.drain(..) {
            bytes.extend_from_slice(row.as_bytes());
            bytes.push(b'\n');
        }
        let format_read = read_ndjson_bytes(
            &bytes,
            &self.options.read_options()?,
            &JsonOptions::default(),
        )?;
        read.push_read(
            format_read,
            PythonYieldKind::DictRows,
            &self.options,
            next_batch_index,
        )
    }
}

fn materialize_dlt_resource<'py>(resource: &Bound<'py, PyAny>) -> Result<Bound<'py, PyAny>> {
    if resource.hasattr("__call__").map_err(py_error)? {
        resource.call0().map_err(py_error)
    } else {
        Ok(resource.clone())
    }
}

pub fn arrow_boundary_for(object: &Bound<'_, PyAny>) -> Result<Option<ArrowCapsuleBoundary>> {
    if object.hasattr(ARROW_C_STREAM_METHOD).map_err(py_error)? {
        Ok(Some(ArrowCapsuleBoundary::for_c_stream()))
    } else if object.hasattr(ARROW_C_ARRAY_METHOD).map_err(py_error)? {
        Ok(Some(ArrowCapsuleBoundary::for_c_array()))
    } else {
        Ok(None)
    }
}
