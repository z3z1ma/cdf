use std::{path::Path, sync::Arc};

use cdf_declarative::CompiledResource;
use cdf_http::HttpMethod;
use cdf_kernel::{CdfError, CursorValue, Result, SourcePosition};
use cdf_project::ProjectRunReport;
use cdf_runtime::SourceResolutionContext;

use super::{
    MatrixDisposition,
    test_support::{RecordingTransport, StaticSecretProvider, json_response},
};

const RESOURCE_ID: &str = "api.events";
const SECRET_REF: &str = "secret://env/API_TOKEN";

pub(crate) fn resource(
    disposition: MatrixDisposition,
) -> Result<(
    crate::source_fixture::ResolvedSourceFixture,
    RecordingTransport,
)> {
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "id": 1, "name": "ada", "updated_at": 10 },
            { "id": 2, "name": "grace", "updated_at": 20 }
        ] }"#,
    )]);
    let registry = crate::test_rest_source_registry(transport.clone())?;
    let document = cdf_declarative::parse_toml(&resource_toml(disposition))?;
    let compiled = one_resource(cdf_declarative::compile_document(&registry, &document)?)?;
    let execution = crate::test_execution_services();
    let context = SourceResolutionContext::new(
        Path::new("."),
        Arc::new(StaticSecretProvider::new([(
            SECRET_REF,
            "run-matrix-token",
        )])),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let resource =
        crate::source_fixture::ResolvedSourceFixture::resolve(&compiled, &registry, &context)?;
    Ok((resource, transport))
}

pub(crate) fn assert_runtime_observed(transport: &RecordingTransport) {
    let requests = transport.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.method, HttpMethod::Get);
    assert_eq!(request.url, "https://api.example.test/events");
    assert!(
        request
            .headers
            .get("authorization")
            .is_some_and(|value| value.starts_with("Bearer "))
    );
}

pub(crate) fn assert_source_position(report: &ProjectRunReport) {
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("run matrix REST source must checkpoint a cursor position");
    };
    assert_eq!(cursor.version, 1);
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}

fn one_resource(mut resources: Vec<CompiledResource>) -> Result<CompiledResource> {
    if resources.len() != 1 {
        return Err(CdfError::contract(format!(
            "run matrix expected one REST resource, found {}",
            resources.len()
        )));
    }
    let resource = resources.remove(0);
    if resource.descriptor().resource_id.as_str() != RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "run matrix compiled unexpected REST resource {}",
            resource.descriptor().resource_id
        )));
    }
    Ok(resource)
}

fn resource_toml(disposition: MatrixDisposition) -> String {
    let keys = merge_keys(disposition);
    format!(
        r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"
auth = {{ kind = "bearer", token = "{SECRET_REF}" }}
egress_allowlist = ["api.example.test"]

[resource.events]
path = "/events"
records = "$.items"
{keys}
cursor = {{ field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }}
write_disposition = "{}"
trust = "governed"
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
  {{ name = "name", type = "string", nullable = true }},
  {{ name = "updated_at", type = "int64", nullable = false }},
] }}
"#,
        disposition.as_str()
    )
}

fn merge_keys(disposition: MatrixDisposition) -> &'static str {
    if disposition == MatrixDisposition::Merge {
        "primary_key = [\"id\"]\nmerge_key = [\"id\"]"
    } else {
        ""
    }
}
