use crate::*;

pub(crate) fn import_arrow_stream(object: &Bound<'_, PyAny>) -> Result<Vec<RecordBatch>> {
    let table = object.extract::<PyTable>().map_err(py_error)?;
    let (batches, _) = table.into_inner();
    Ok(batches)
}

pub(crate) fn python_dict_to_json(py: Python<'_>, object: &Bound<'_, PyAny>) -> Result<String> {
    let json = PyModule::import(py, "json").map_err(py_error)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("sort_keys", true).map_err(py_error)?;
    let json_text = json
        .getattr("dumps")
        .and_then(|dumps| dumps.call((object,), Some(&kwargs)))
        .and_then(|value| value.extract::<String>())
        .map_err(py_error)?;
    let value: serde_json::Value = serde_json::from_str(&json_text).map_err(json_error)?;
    if !value.is_object() {
        return Err(CdfError::data(
            "Python dict batching accepts JSON objects only",
        ));
    }
    Ok(json_text)
}

pub(crate) fn write_ipc_hash(batch: &RecordBatch, hasher: &mut Sha256) -> Result<()> {
    let mut bytes = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut bytes, batch.schema().as_ref()).map_err(CdfError::from)?;
        writer.write(batch).map_err(CdfError::from)?;
        writer.finish().map_err(CdfError::from)?;
    }
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
    Ok(())
}

pub(crate) fn descriptor_for(
    resource_id: ResourceId,
    state_scope: ScopeKey,
    observed_schema_hash: SchemaHash,
) -> ResourceDescriptor {
    ResourceDescriptor {
        resource_id,
        schema_source: SchemaSource::Discovered {
            schema_hash: Some(observed_schema_hash),
        },
        primary_key: Vec::new(),
        merge_key: Vec::new(),
        cursor: None,
        write_disposition: WriteDisposition::Append,
        contract: None,
        state_scope,
        freshness: None,
        trust_level: TrustLevel::Experimental,
    }
}

pub(crate) fn batch_id(options: &PythonBridgeOptions, index: usize) -> Result<BatchId> {
    BatchId::new(format!("{}-{index:06}", options.batch_id_prefix))
}

pub(crate) fn same_path(expected: &Path, actual: &Path) -> Result<bool> {
    let expected = expected.canonicalize().map_err(|error| {
        CdfError::contract(format!("configured Python interpreter path error: {error}"))
    })?;
    let actual = actual.canonicalize().map_err(|error| {
        CdfError::contract(format!("attached Python interpreter path error: {error}"))
    })?;
    Ok(expected == actual)
}

pub(crate) fn sanitize_id_part(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

pub(crate) fn py_error(error: PyErr) -> CdfError {
    CdfError::data(error.to_string())
}

pub(crate) fn json_error(error: serde_json::Error) -> CdfError {
    CdfError::data(error.to_string())
}
