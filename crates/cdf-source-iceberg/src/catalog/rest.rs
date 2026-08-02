use std::{collections::BTreeMap, sync::Arc};

use cdf_http::{EgressAllowlist, HttpMethod, HttpRequest, HttpResponseBudget, SecretUri};
use cdf_kernel::{CdfError, Result};
use cdf_memory::AccountedBytes;
use cdf_runtime::RunCancellation;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::{
    CatalogObservation, IcebergCatalogBinding, IcebergCatalogContext, IcebergCatalogLoadRequest,
    LoadedIcebergTable, allowlist, build_loaded_table, reserve_parse_memory,
};
use crate::{IcebergCatalogOptions, IcebergResourceOptions};

pub(super) struct RestCatalogBinding;

impl IcebergCatalogBinding for RestCatalogBinding {
    fn kind(&self) -> &'static str {
        "rest"
    }

    fn load_table(
        &self,
        request: &IcebergCatalogLoadRequest,
        context: &IcebergCatalogContext,
    ) -> Result<LoadedIcebergTable> {
        let IcebergCatalogOptions::Rest {
            uri,
            warehouse,
            credentials,
        } = &request.source.catalog
        else {
            return Err(CdfError::internal(
                "REST binding received another catalog kind",
            ));
        };
        let authorization = credentials
            .as_ref()
            .map(|reference| {
                context
                    .secrets
                    .resolve(&SecretUri::new(reference.clone())?)?
                    .as_str()
                    .map(|token| format!("Bearer {token}"))
            })
            .transpose()?;
        let allowlist = allowlist(&request.source);
        let config_endpoint = rest_config_endpoint(uri, warehouse.as_deref())?;
        context.egress.authorize(&config_endpoint)?;
        let config_payload = send_rest_request(
            context,
            &allowlist,
            config_endpoint,
            authorization.as_deref(),
            request.source.maximum_metadata_bytes,
            request.cancellation.clone(),
        )?;
        let config_bytes = u64::try_from(config_payload.payload().len())
            .map_err(|_| CdfError::data("Iceberg REST config length exceeds u64"))?;
        let config_parse_lease = reserve_parse_memory(
            context.execution.memory(),
            config_bytes,
            request.source.metadata_parse_amplification_bps,
            "iceberg-rest-config-parse",
        )?;
        let catalog_config: RestCatalogConfigResponse =
            serde_json::from_slice(config_payload.payload()).map_err(|error| {
                CdfError::data(format!("decode Iceberg REST catalog config: {error}"))
            })?;
        let routing = RestCatalogRouting::negotiate(uri, catalog_config)?;
        drop(config_parse_lease);
        drop(config_payload);
        let endpoint =
            rest_table_endpoint(&routing.uri, routing.prefix.as_deref(), &request.resource)?;
        context.egress.authorize(&endpoint)?;
        let payload = send_rest_request(
            context,
            &allowlist,
            endpoint,
            authorization.as_deref(),
            request.source.maximum_metadata_bytes,
            request.cancellation.clone(),
        )?;
        let response_bytes = u64::try_from(payload.payload().len())
            .map_err(|_| CdfError::data("Iceberg REST response length exceeds u64"))?;
        let envelope: RestLoadTableResponse =
            serde_json::from_slice(payload.payload()).map_err(|error| {
                CdfError::data(format!("decode Iceberg REST table response: {error}"))
            })?;
        let metadata_location = envelope.metadata_location.ok_or_else(|| {
            CdfError::data("Iceberg REST table response omitted metadata-location")
        })?;
        build_loaded_table(
            request,
            context,
            CatalogObservation {
                metadata_location,
                catalog_generation: None,
                metadata_payload: payload,
                embedded_metadata: Some(envelope.metadata),
                bytes_read: config_bytes.saturating_add(response_bytes),
                objects_read: 2,
            },
        )
    }
}

fn send_rest_request(
    context: &IcebergCatalogContext,
    allowlist: &EgressAllowlist,
    endpoint: String,
    authorization: Option<&str>,
    maximum_bytes: u64,
    cancellation: RunCancellation,
) -> Result<AccountedBytes> {
    let mut request = HttpRequest::new(HttpMethod::Get, endpoint);
    if let Some(authorization) = authorization {
        request = request.with_header("authorization", authorization);
    }
    let budget = HttpResponseBudget::new(
        maximum_bytes,
        context.execution.memory(),
        Arc::new(move || cancellation.check()),
    )?;
    let rest_http = Arc::clone(&context.rest_http);
    let allowlist = allowlist.clone();
    let response = context.execution.run_io(async move {
        cdf_http::send_with_policy(rest_http.as_ref(), &allowlist, request, budget).await
    })?;
    if response.status != 200 {
        return Err(http_catalog_error(response.status));
    }
    response
        .accounted_body()
        .cloned()
        .ok_or_else(|| CdfError::data("Iceberg REST response omitted its JSON body"))
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RestLoadTableResponse {
    metadata_location: Option<String>,
    metadata: Box<RawValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestCatalogConfigResponse {
    #[serde(default)]
    defaults: BTreeMap<String, String>,
    #[serde(default)]
    overrides: BTreeMap<String, String>,
    #[serde(default)]
    endpoints: Option<Vec<String>>,
}

struct RestCatalogRouting {
    uri: String,
    prefix: Option<String>,
}

impl RestCatalogRouting {
    fn negotiate(configured_uri: &str, response: RestCatalogConfigResponse) -> Result<Self> {
        let _advertised_endpoints = response.endpoints;
        let mut properties = response.defaults;
        properties.extend(response.overrides);
        let uri = properties
            .remove("uri")
            .unwrap_or_else(|| configured_uri.to_owned());
        validate_rest_uri("negotiated Iceberg REST catalog URI", &uri)?;
        let prefix = properties.remove("prefix");
        if let Some(prefix) = &prefix {
            validate_rest_prefix(prefix)?;
        }
        Ok(Self { uri, prefix })
    }
}

fn rest_config_endpoint(root: &str, warehouse: Option<&str>) -> Result<String> {
    let mut url = url::Url::parse(root)
        .map_err(|error| CdfError::contract(format!("invalid Iceberg REST URI: {error}")))?;
    {
        let mut path = url.path_segments_mut().map_err(|_| {
            CdfError::contract("Iceberg REST URI cannot be used as a hierarchical URL")
        })?;
        path.pop_if_empty().push("v1").push("config");
    }
    if let Some(warehouse) = warehouse {
        url.query_pairs_mut().append_pair("warehouse", warehouse);
    }
    Ok(url.to_string())
}

fn rest_table_endpoint(
    root: &str,
    prefix: Option<&str>,
    resource: &IcebergResourceOptions,
) -> Result<String> {
    let mut url = url::Url::parse(root)
        .map_err(|error| CdfError::contract(format!("invalid Iceberg REST URI: {error}")))?;
    {
        let mut path = url.path_segments_mut().map_err(|_| {
            CdfError::contract("Iceberg REST URI cannot be used as a hierarchical URL")
        })?;
        path.pop_if_empty().push("v1");
        if let Some(prefix) = prefix {
            for component in prefix.split('/') {
                path.push(component);
            }
        }
        path.push("namespaces")
            .push(&resource.namespace.join("\u{001f}"))
            .push("tables")
            .push(&resource.table);
    }
    Ok(url.to_string())
}

fn validate_rest_uri(label: &str, value: &str) -> Result<()> {
    let parsed = url::Url::parse(value)
        .map_err(|error| CdfError::data(format!("{label} is invalid: {error}")))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(CdfError::data(format!(
            "{label} requires an HTTP(S) URL without userinfo, query, or fragment"
        )));
    }
    Ok(())
}

fn validate_rest_prefix(prefix: &str) -> Result<()> {
    if prefix.is_empty()
        || prefix.starts_with('/')
        || prefix.ends_with('/')
        || prefix
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
        || prefix.chars().any(char::is_control)
    {
        return Err(CdfError::data(
            "Iceberg REST catalog returned an invalid routing prefix",
        ));
    }
    Ok(())
}

fn http_catalog_error(status: u16) -> CdfError {
    match status {
        401 | 403 => CdfError::auth(format!(
            "Iceberg REST catalog rejected table access with HTTP {status}"
        )),
        404 => CdfError::data("Iceberg REST catalog table was not found"),
        408 | 425 | 429 | 500..=599 => CdfError::transient(format!(
            "Iceberg REST catalog returned retryable HTTP {status}"
        )),
        _ => CdfError::data(format!(
            "Iceberg REST catalog returned unsupported HTTP {status}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IcebergScanMode, IcebergSnapshotSelector};

    #[test]
    fn rest_endpoint_uses_iceberg_namespace_encoding() {
        let endpoint = rest_table_endpoint(
            "https://catalog.example.test",
            None,
            &IcebergResourceOptions {
                namespace: vec!["org".to_owned(), "analytics".to_owned()],
                table: "events".to_owned(),
                selector: IcebergSnapshotSelector::Current,
                mode: IcebergScanMode::Snapshot,
            },
        )
        .unwrap();
        assert_eq!(
            endpoint,
            "https://catalog.example.test/v1/namespaces/org%1Fanalytics/tables/events"
        );
    }

    #[test]
    fn rest_negotiation_keeps_warehouse_in_config_query_and_prefixes_table_route() {
        let config =
            rest_config_endpoint("https://catalog.example.test/api", Some("prod/main")).unwrap();
        assert_eq!(
            config,
            "https://catalog.example.test/api/v1/config?warehouse=prod%2Fmain"
        );
        let routing = RestCatalogRouting::negotiate(
            "https://catalog.example.test/api",
            RestCatalogConfigResponse {
                defaults: BTreeMap::from([("prefix".to_owned(), "ice/prod".to_owned())]),
                overrides: BTreeMap::from([(
                    "uri".to_owned(),
                    "https://routed.example.test/catalog".to_owned(),
                )]),
                endpoints: None,
            },
        )
        .unwrap();
        let endpoint = rest_table_endpoint(
            &routing.uri,
            routing.prefix.as_deref(),
            &IcebergResourceOptions {
                namespace: vec!["analytics".to_owned()],
                table: "events".to_owned(),
                selector: IcebergSnapshotSelector::Current,
                mode: IcebergScanMode::Snapshot,
            },
        )
        .unwrap();
        assert_eq!(
            endpoint,
            "https://routed.example.test/catalog/v1/ice/prod/namespaces/analytics/tables/events"
        );
    }
}
