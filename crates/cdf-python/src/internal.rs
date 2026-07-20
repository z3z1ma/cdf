use std::collections::BTreeMap;

use crate::*;

pub(crate) fn import_arrow_stream(
    object: &Bound<'_, PyAny>,
) -> Result<Box<dyn arrow_array::RecordBatchReader + Send>> {
    let reader = arrow_capsule::import_record_batch_stream(object).map_err(py_error)?;
    Ok(Box::new(reader))
}

pub(crate) fn python_dict_to_json(py: Python<'_>, object: &Bound<'_, PyAny>) -> Result<String> {
    let json = PyModule::import(py, "json").map_err(py_error)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("sort_keys", true).map_err(py_error)?;
    let json_text = json
        .getattr("dumps")
        .and_then(|dumps| dumps.call((object,), Some(&kwargs)))
        .and_then(|value| value.extract::<String>())
        .map_err(|_| {
            CdfError::data(
                "Python dict row contains a value that cannot be encoded as JSON; emit Arrow for non-JSON-native values",
            )
        })?;
    Ok(json_text)
}

pub(crate) fn descriptor_for(
    resource_id: ResourceId,
    state_scope: ScopeKey,
    observed_schema_hash: SchemaHash,
) -> ResourceDescriptor {
    let snapshot_path = format!(".cdf/schemas/{resource_id}@{observed_schema_hash}.json");
    ResourceDescriptor {
        resource_id,
        schema_source: SchemaSource::Discovered {
            snapshot: SchemaSnapshotReference {
                schema_hash: observed_schema_hash,
                path: snapshot_path,
                metadata: BTreeMap::from([("probe".to_owned(), "python-arrow".to_owned())]),
            },
        },
        primary_key: Vec::new(),
        merge_key: Vec::new(),
        cursor: None,
        write_disposition: WriteDisposition::Append,
        deduplication: None,
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
    let exception = Python::attach(|py| {
        error
            .get_type(py)
            .name()
            .and_then(|name| name.to_str().map(str::to_owned))
            .ok()
            .filter(|name| safe_exception_name(name))
            .unwrap_or_else(|| "Exception".to_owned())
    });
    CdfError::data(format!(
        "Python execution failed at the foreign boundary ({exception}); inspect the Python resource locally for exception details"
    ))
}

fn safe_exception_name(name: &str) -> bool {
    let mut characters = name.chars();
    name.len() <= 128
        && characters
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

pub(crate) fn json_error(error: serde_json::Error) -> CdfError {
    CdfError::data(error.to_string())
}
