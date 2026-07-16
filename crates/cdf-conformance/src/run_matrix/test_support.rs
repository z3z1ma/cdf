use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

use cdf_http::{HttpRequest, HttpResponse, HttpTransport, SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{CdfError, Result};

#[derive(Clone, Default)]
pub(crate) struct RecordingTransport {
    state: Arc<Mutex<RecordingTransportState>>,
}

#[derive(Default)]
struct RecordingTransportState {
    requests: Vec<HttpRequest>,
    responses: VecDeque<RecordingResponse>,
}

pub(crate) struct RecordingResponse {
    response: HttpResponse,
    body: Vec<u8>,
}

impl RecordingTransport {
    pub(crate) fn new<I>(responses: I) -> Self
    where
        I: IntoIterator<Item = RecordingResponse>,
    {
        Self {
            state: Arc::new(Mutex::new(RecordingTransportState {
                requests: Vec::new(),
                responses: responses.into_iter().collect(),
            })),
        }
    }

    pub(crate) fn requests(&self) -> Vec<HttpRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

impl HttpTransport for RecordingTransport {
    fn send(
        &self,
        request: HttpRequest,
        budget: cdf_http::HttpResponseBudget,
    ) -> Result<HttpResponse> {
        let mut state = self.state.lock().unwrap();
        state.requests.push(request);
        let template = state
            .responses
            .pop_front()
            .ok_or_else(|| CdfError::internal("run matrix REST transport exhausted responses"))?;
        Ok(template
            .response
            .with_body(budget.account_body(template.body)?))
    }
}

pub(crate) struct StaticSecretProvider {
    values: BTreeMap<String, String>,
}

impl StaticSecretProvider {
    pub(crate) fn new<I, K, V>(values: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            values: values
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }
}

impl SecretProvider for StaticSecretProvider {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        self.values
            .get(uri.as_str())
            .map(|value| SecretValue::new(value.clone()))
            .ok_or_else(|| CdfError::auth(format!("missing run matrix secret `{uri}`")))
    }
}

pub(crate) fn json_response(body: &str) -> RecordingResponse {
    RecordingResponse {
        response: HttpResponse::new(200).with_header("content-type", "application/json"),
        body: body.as_bytes().to_vec(),
    }
}

pub(crate) fn copy_dir_all(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)
        .map_err(|error| CdfError::data(format!("create {}: {error}", destination.display())))?;
    for entry in fs::read_dir(source)
        .map_err(|error| CdfError::data(format!("read {}: {error}", source.display())))?
    {
        let entry = entry.map_err(|error| {
            CdfError::data(format!("read entry in {}: {error}", source.display()))
        })?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type().map_err(|error| {
            CdfError::data(format!(
                "read file type for {}: {error}",
                source_path.display()
            ))
        })?;
        if file_type.is_dir() {
            copy_dir_all(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            fs::copy(&source_path, &destination_path).map_err(|error| {
                CdfError::data(format!(
                    "copy {} to {}: {error}",
                    source_path.display(),
                    destination_path.display()
                ))
            })?;
        }
    }
    Ok(())
}
