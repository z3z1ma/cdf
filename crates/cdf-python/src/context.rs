use crate::internal::*;
use crate::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PythonContext {
    redactor: Redactor,
    cursor: Option<SourcePosition>,
    logs: Vec<ContextLogEvent>,
}

impl PythonContext {
    pub fn new(cursor: Option<SourcePosition>) -> Self {
        Self {
            redactor: Redactor::default(),
            cursor,
            logs: Vec::new(),
        }
    }

    pub fn cursor(&self) -> Option<&SourcePosition> {
        self.cursor.as_ref()
    }

    pub fn redactor(&self) -> &Redactor {
        &self.redactor
    }

    pub fn resolve_bearer_request(
        &mut self,
        request: HttpRequest,
        uri: &SecretUri,
        provider: &dyn SecretProvider,
    ) -> Result<HttpRequest> {
        let secret = provider.resolve(uri)?;
        self.redactor.register_secret_value(&secret)?;
        Ok(request.with_header("authorization", format!("Bearer {}", secret.as_str()?)))
    }

    pub fn trace_request(&self, request: &HttpRequest) -> TraceEvent {
        TraceEvent::from_request(request, &self.redactor)
    }

    pub fn log(&mut self, level: impl Into<String>, message: impl AsRef<str>) {
        self.logs.push(ContextLogEvent {
            level: level.into(),
            message: self.redactor.redact_text(message.as_ref()),
        });
    }

    pub fn logs(&self) -> &[ContextLogEvent] {
        &self.logs
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextLogEvent {
    pub level: String,
    pub message: String,
}

pub fn deterministic_fixture_hash(read: &PythonBatchRead) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"cdf-python-fixture-v1");
    if let Some(schema_hash) = &read.schema_hash {
        hasher.update(schema_hash.as_str().as_bytes());
    }
    for (batch, kind) in read.batches.iter().zip(&read.yield_kinds) {
        hasher.update(format!("{kind:?}\n").as_bytes());
        hasher.update(batch.header.batch_id.as_str().as_bytes());
        hasher.update(b"\n");
        hasher.update(batch.header.row_count.to_le_bytes());
        hasher.update(batch.header.byte_count.to_le_bytes());
        let record_batch = batch.record_batch().ok_or_else(|| {
            CdfError::data("deterministic Python fixture hash requires in-memory Arrow batches")
        })?;
        write_ipc_hash(record_batch, &mut hasher)?;
    }
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}
