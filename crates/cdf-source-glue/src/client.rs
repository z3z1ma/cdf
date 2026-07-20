use std::sync::Arc;

use bytes::Bytes;
use cdf_aws::{AwsControlClient, AwsControlRequest, AwsControlTarget};
use cdf_http::SecretUri;
use cdf_kernel::{BoxFuture, CdfError, Result};
use cdf_runtime::RunCancellation;
use serde::{Deserialize, Serialize};

use crate::model::{GluePartition, GlueTable};

const GLUE_SERVICE: &str = "glue";
const GET_TABLE_TARGET: &str = "AWSGlue.GetTable";
const GET_PARTITIONS_TARGET: &str = "AWSGlue.GetPartitions";
const GET_UNFILTERED_TABLE_TARGET: &str = "AWSGlue.GetUnfilteredTableMetadata";
const GET_UNFILTERED_PARTITIONS_TARGET: &str = "AWSGlue.GetUnfilteredPartitionsMetadata";

#[derive(Clone, Debug)]
pub struct GlueGetTableRequest {
    pub region: String,
    pub catalog_id: Option<String>,
    pub database: String,
    pub table: String,
    pub endpoint: Option<String>,
    pub credentials: Option<SecretUri>,
    pub maximum_response_bytes: u64,
    pub cancellation: RunCancellation,
}

impl GlueGetTableRequest {
    fn validate(&self) -> Result<()> {
        validate_catalog_request(
            &self.region,
            self.catalog_id.as_deref(),
            &self.database,
            &self.table,
            self.maximum_response_bytes,
        )
    }
}

#[derive(Clone, Debug)]
pub struct GlueGetPartitionsRequest {
    pub region: String,
    pub catalog_id: Option<String>,
    pub database: String,
    pub table: String,
    pub expression: Option<String>,
    pub next_token: Option<String>,
    pub page_size: u16,
    pub endpoint: Option<String>,
    pub credentials: Option<SecretUri>,
    pub maximum_response_bytes: u64,
    pub cancellation: RunCancellation,
}

#[derive(Clone, Debug)]
pub struct GlueGetUnfilteredTableRequest {
    pub region: String,
    pub catalog_id: String,
    pub database: String,
    pub table: String,
    pub requested_columns: Vec<String>,
    pub all_columns_requested: bool,
    pub query_id: String,
    pub query_start_unix_seconds: u64,
    pub endpoint: Option<String>,
    pub credentials: Option<SecretUri>,
    pub maximum_response_bytes: u64,
    pub cancellation: RunCancellation,
}

#[derive(Clone, Debug)]
pub struct GlueGetUnfilteredPartitionsRequest {
    pub region: String,
    pub catalog_id: String,
    pub database: String,
    pub table: String,
    pub expression: Option<String>,
    pub next_token: Option<String>,
    pub page_size: u16,
    pub requested_columns: Vec<String>,
    pub all_columns_requested: bool,
    pub query_id: String,
    pub query_start_unix_seconds: u64,
    pub query_authorization_id: String,
    pub endpoint: Option<String>,
    pub credentials: Option<SecretUri>,
    pub maximum_response_bytes: u64,
    pub cancellation: RunCancellation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlueLakeFormationAuthorization {
    pub query_id: String,
    pub query_start_unix_seconds: u64,
    pub query_authorization_id: String,
    pub resource_arn: String,
    pub authorized_columns: Vec<String>,
}

impl GlueGetPartitionsRequest {
    fn validate(&self) -> Result<()> {
        validate_catalog_request(
            &self.region,
            self.catalog_id.as_deref(),
            &self.database,
            &self.table,
            self.maximum_response_bytes,
        )?;
        if !(1..=1000).contains(&self.page_size) {
            return Err(CdfError::contract(
                "Glue GetPartitions page_size must be between 1 and 1000",
            ));
        }
        if self
            .expression
            .as_ref()
            .is_some_and(|expression| expression.len() > 2048)
        {
            return Err(CdfError::contract(
                "Glue GetPartitions expression exceeds the 2048-byte service limit",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlueTableResponse {
    pub table: GlueTable,
    pub lake_formation: Option<GlueLakeFormationAuthorization>,
    pub bytes_read: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GluePartitionPage {
    pub partitions: Vec<GluePartition>,
    pub next_token: Option<String>,
    pub bytes_read: u64,
}

pub trait GlueCatalogClient: Send + Sync {
    fn get_table(&self, request: GlueGetTableRequest) -> BoxFuture<'_, Result<GlueTableResponse>>;
    fn get_partitions(
        &self,
        request: GlueGetPartitionsRequest,
    ) -> BoxFuture<'_, Result<GluePartitionPage>>;
    fn get_unfiltered_table(
        &self,
        _request: GlueGetUnfilteredTableRequest,
    ) -> BoxFuture<'_, Result<GlueTableResponse>> {
        Box::pin(async {
            Err(CdfError::contract(
                "configured Glue catalog client does not support Lake Formation governed metadata",
            ))
        })
    }
    fn get_unfiltered_partitions(
        &self,
        _request: GlueGetUnfilteredPartitionsRequest,
    ) -> BoxFuture<'_, Result<GluePartitionPage>> {
        Box::pin(async {
            Err(CdfError::contract(
                "configured Glue catalog client does not support Lake Formation governed partition metadata",
            ))
        })
    }
}

#[derive(Clone, Debug)]
pub struct AwsGlueCatalogClient {
    aws: Arc<AwsControlClient>,
}

impl AwsGlueCatalogClient {
    pub fn new(aws: Arc<AwsControlClient>) -> Self {
        Self { aws }
    }

    async fn request_table(&self, request: GlueGetTableRequest) -> Result<GlueTableResponse> {
        request.validate()?;
        request.cancellation.check()?;
        let body = serde_json::to_vec(&GetTableBody {
            catalog_id: request.catalog_id.as_deref(),
            database_name: &request.database,
            name: &request.table,
        })
        .map_err(|error| CdfError::internal(format!("encode Glue GetTable: {error}")))?;
        let response = self
            .aws
            .send(AwsControlRequest {
                service: GLUE_SERVICE.to_owned(),
                target: AwsControlTarget::JsonTarget {
                    target: GET_TABLE_TARGET.to_owned(),
                },
                region: request.region,
                endpoint: request.endpoint,
                credentials: request.credentials,
                body: Bytes::from(body),
                maximum_response_bytes: request.maximum_response_bytes,
                cancellation: request.cancellation,
            })
            .await?;
        if response.status() != 200 {
            return Err(glue_error("GetTable", response.status(), response.body()));
        }
        let payload = response
            .body()
            .ok_or_else(|| CdfError::data("Glue GetTable response omitted its JSON body"))?;
        let decoded: GetTableResponse = serde_json::from_slice(payload)
            .map_err(|error| CdfError::data(format!("decode Glue GetTable: {error}")))?;
        Ok(GlueTableResponse {
            table: decoded
                .table
                .ok_or_else(|| CdfError::data("Glue GetTable response omitted Table"))?,
            lake_formation: None,
            bytes_read: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        })
    }

    async fn request_partitions(
        &self,
        request: GlueGetPartitionsRequest,
    ) -> Result<GluePartitionPage> {
        request.validate()?;
        request.cancellation.check()?;
        let body = serde_json::to_vec(&GetPartitionsBody {
            catalog_id: request.catalog_id.as_deref(),
            database_name: &request.database,
            table_name: &request.table,
            expression: request.expression.as_deref(),
            next_token: request.next_token.as_deref(),
            max_results: request.page_size,
        })
        .map_err(|error| CdfError::internal(format!("encode Glue GetPartitions: {error}")))?;
        let response = self
            .aws
            .send(AwsControlRequest {
                service: GLUE_SERVICE.to_owned(),
                target: AwsControlTarget::JsonTarget {
                    target: GET_PARTITIONS_TARGET.to_owned(),
                },
                region: request.region,
                endpoint: request.endpoint,
                credentials: request.credentials,
                body: Bytes::from(body),
                maximum_response_bytes: request.maximum_response_bytes,
                cancellation: request.cancellation,
            })
            .await?;
        if response.status() != 200 {
            return Err(glue_error(
                "GetPartitions",
                response.status(),
                response.body(),
            ));
        }
        let payload = response
            .body()
            .ok_or_else(|| CdfError::data("Glue GetPartitions response omitted its JSON body"))?;
        let decoded: GetPartitionsResponse = serde_json::from_slice(payload)
            .map_err(|error| CdfError::data(format!("decode Glue GetPartitions: {error}")))?;
        if decoded.next_token.as_ref().is_some_and(String::is_empty) {
            return Err(CdfError::data(
                "Glue GetPartitions returned an empty continuation token",
            ));
        }
        Ok(GluePartitionPage {
            partitions: decoded.partitions,
            next_token: decoded.next_token,
            bytes_read: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        })
    }

    async fn request_unfiltered_table(
        &self,
        request: GlueGetUnfilteredTableRequest,
    ) -> Result<GlueTableResponse> {
        validate_catalog_request(
            &request.region,
            Some(&request.catalog_id),
            &request.database,
            &request.table,
            request.maximum_response_bytes,
        )?;
        validate_authorization_request(
            &request.requested_columns,
            request.all_columns_requested,
            &request.query_id,
        )?;
        if request.query_start_unix_seconds == 0 {
            return Err(CdfError::contract(
                "Glue query start time must be a positive Unix timestamp",
            ));
        }
        request.cancellation.check()?;
        let body = serde_json::to_vec(&GetUnfilteredTableBody {
            catalog_id: &request.catalog_id,
            database_name: &request.database,
            name: &request.table,
            audit_context: GlueAuditContext {
                requested_columns: (!request.all_columns_requested)
                    .then_some(request.requested_columns.as_slice()),
                all_columns_requested: request.all_columns_requested,
            },
            supported_permission_types: ["COLUMN_PERMISSION"],
            permissions: ["SELECT"],
            query_session_context: GlueQuerySessionContext {
                query_id: &request.query_id,
                query_start_time: request.query_start_unix_seconds,
                query_authorization_id: None,
            },
        })
        .map_err(|error| {
            CdfError::internal(format!("encode Glue GetUnfilteredTableMetadata: {error}"))
        })?;
        let response = self
            .aws
            .send(AwsControlRequest {
                service: GLUE_SERVICE.to_owned(),
                target: AwsControlTarget::JsonTarget {
                    target: GET_UNFILTERED_TABLE_TARGET.to_owned(),
                },
                region: request.region,
                endpoint: request.endpoint,
                credentials: request.credentials,
                body: Bytes::from(body),
                maximum_response_bytes: request.maximum_response_bytes,
                cancellation: request.cancellation,
            })
            .await?;
        if response.status() != 200 {
            return Err(glue_error(
                "GetUnfilteredTableMetadata",
                response.status(),
                response.body(),
            ));
        }
        let payload = response.body().ok_or_else(|| {
            CdfError::data("Glue GetUnfilteredTableMetadata response omitted its JSON body")
        })?;
        let decoded: GetUnfilteredTableResponse =
            serde_json::from_slice(payload).map_err(|error| {
                CdfError::data(format!("decode Glue GetUnfilteredTableMetadata: {error}"))
            })?;
        validate_unfiltered_filters(decoded.row_filter.as_deref(), &decoded.cell_filters)?;
        if !decoded.is_registered_with_lake_formation {
            return Err(CdfError::auth(
                "Glue governed metadata response is not registered with Lake Formation",
            ));
        }
        let mut table = decoded.table.ok_or_else(|| {
            CdfError::data("Glue GetUnfilteredTableMetadata response omitted Table")
        })?;
        table.is_registered_with_lake_formation = true;
        let descriptor = table
            .storage_descriptor
            .as_mut()
            .ok_or_else(|| CdfError::data("Glue governed table omitted its StorageDescriptor"))?;
        let available = descriptor
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let authorized = decoded
            .authorized_columns
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        if authorized.is_empty() && !descriptor.columns.is_empty() {
            return Err(CdfError::auth(
                "Glue governed table response omitted AuthorizedColumns",
            ));
        }
        if let Some(column) = authorized
            .iter()
            .find(|column| !available.contains(**column))
        {
            return Err(CdfError::data(format!(
                "Glue governed table authorized unknown column `{column}`"
            )));
        }
        if let Some(column) = request
            .requested_columns
            .iter()
            .find(|column| !authorized.contains(column.as_str()))
        {
            return Err(CdfError::auth(format!(
                "Lake Formation did not authorize requested column `{column}`"
            )));
        }
        let effective_columns = if request.all_columns_requested {
            decoded.authorized_columns.clone()
        } else {
            request.requested_columns.clone()
        };
        let effective = effective_columns
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        descriptor
            .columns
            .retain(|column| effective.contains(column.name.as_str()));
        let query_authorization_id =
            required_control_text("Glue QueryAuthorizationId", decoded.query_authorization_id)?;
        let resource_arn = required_control_text("Glue ResourceArn", decoded.resource_arn)?;
        Ok(GlueTableResponse {
            table,
            lake_formation: Some(GlueLakeFormationAuthorization {
                query_id: request.query_id,
                query_start_unix_seconds: request.query_start_unix_seconds,
                query_authorization_id,
                resource_arn,
                authorized_columns: effective_columns,
            }),
            bytes_read: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        })
    }

    async fn request_unfiltered_partitions(
        &self,
        request: GlueGetUnfilteredPartitionsRequest,
    ) -> Result<GluePartitionPage> {
        validate_catalog_request(
            &request.region,
            Some(&request.catalog_id),
            &request.database,
            &request.table,
            request.maximum_response_bytes,
        )?;
        validate_authorization_request(
            &request.requested_columns,
            request.all_columns_requested,
            &request.query_id,
        )?;
        if request.query_start_unix_seconds == 0 {
            return Err(CdfError::contract(
                "Glue query start time must be a positive Unix timestamp",
            ));
        }
        if request.query_authorization_id.is_empty()
            || request.query_authorization_id.chars().any(char::is_control)
        {
            return Err(CdfError::contract(
                "Glue query authorization id must be nonempty and control-free",
            ));
        }
        if !(1..=1000).contains(&request.page_size) {
            return Err(CdfError::contract(
                "Glue GetUnfilteredPartitionsMetadata page_size must be between 1 and 1000",
            ));
        }
        request.cancellation.check()?;
        let body = serde_json::to_vec(&GetUnfilteredPartitionsBody {
            catalog_id: &request.catalog_id,
            database_name: &request.database,
            table_name: &request.table,
            expression: request.expression.as_deref(),
            audit_context: GlueAuditContext {
                requested_columns: (!request.all_columns_requested)
                    .then_some(request.requested_columns.as_slice()),
                all_columns_requested: request.all_columns_requested,
            },
            supported_permission_types: ["COLUMN_PERMISSION"],
            next_token: request.next_token.as_deref(),
            max_results: request.page_size,
            query_session_context: GlueQuerySessionContext {
                query_id: &request.query_id,
                query_start_time: request.query_start_unix_seconds,
                query_authorization_id: Some(&request.query_authorization_id),
            },
        })
        .map_err(|error| {
            CdfError::internal(format!(
                "encode Glue GetUnfilteredPartitionsMetadata: {error}"
            ))
        })?;
        let response = self
            .aws
            .send(AwsControlRequest {
                service: GLUE_SERVICE.to_owned(),
                target: AwsControlTarget::JsonTarget {
                    target: GET_UNFILTERED_PARTITIONS_TARGET.to_owned(),
                },
                region: request.region,
                endpoint: request.endpoint,
                credentials: request.credentials,
                body: Bytes::from(body),
                maximum_response_bytes: request.maximum_response_bytes,
                cancellation: request.cancellation,
            })
            .await?;
        if response.status() != 200 {
            return Err(glue_error(
                "GetUnfilteredPartitionsMetadata",
                response.status(),
                response.body(),
            ));
        }
        let payload = response.body().ok_or_else(|| {
            CdfError::data("Glue GetUnfilteredPartitionsMetadata response omitted its JSON body")
        })?;
        let decoded: GetUnfilteredPartitionsResponse =
            serde_json::from_slice(payload).map_err(|error| {
                CdfError::data(format!(
                    "decode Glue GetUnfilteredPartitionsMetadata: {error}"
                ))
            })?;
        let requested = request
            .requested_columns
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        let mut partitions = Vec::with_capacity(decoded.unfiltered_partitions.len());
        for unfiltered in decoded.unfiltered_partitions {
            if !unfiltered.is_registered_with_lake_formation {
                return Err(CdfError::auth(
                    "Glue governed partition response is not registered with Lake Formation",
                ));
            }
            let authorized = unfiltered
                .authorized_columns
                .iter()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>();
            if let Some(column) = requested
                .iter()
                .find(|column| !authorized.contains(**column))
            {
                return Err(CdfError::auth(format!(
                    "Lake Formation did not authorize requested partition column `{column}`"
                )));
            }
            partitions.push(unfiltered.partition.ok_or_else(|| {
                CdfError::data("Glue governed partition response omitted Partition")
            })?);
        }
        if decoded.next_token.as_ref().is_some_and(String::is_empty) {
            return Err(CdfError::data(
                "Glue GetUnfilteredPartitionsMetadata returned an empty continuation token",
            ));
        }
        Ok(GluePartitionPage {
            partitions,
            next_token: decoded.next_token,
            bytes_read: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        })
    }
}

fn validate_catalog_request(
    region: &str,
    catalog_id: Option<&str>,
    database: &str,
    table: &str,
    maximum_response_bytes: u64,
) -> Result<()> {
    for (label, value) in [("region", region), ("database", database), ("table", table)] {
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(CdfError::contract(format!(
                "Glue {label} must be nonempty and control-free"
            )));
        }
    }
    if catalog_id.is_some_and(|value| value.is_empty() || value.chars().any(char::is_control)) {
        return Err(CdfError::contract(
            "Glue catalog id must be nonempty and control-free",
        ));
    }
    if maximum_response_bytes == 0 {
        return Err(CdfError::contract(
            "Glue response byte budget must be nonzero",
        ));
    }
    Ok(())
}

impl GlueCatalogClient for AwsGlueCatalogClient {
    fn get_table(&self, request: GlueGetTableRequest) -> BoxFuture<'_, Result<GlueTableResponse>> {
        Box::pin(async move { self.request_table(request).await })
    }

    fn get_partitions(
        &self,
        request: GlueGetPartitionsRequest,
    ) -> BoxFuture<'_, Result<GluePartitionPage>> {
        Box::pin(async move { self.request_partitions(request).await })
    }

    fn get_unfiltered_table(
        &self,
        request: GlueGetUnfilteredTableRequest,
    ) -> BoxFuture<'_, Result<GlueTableResponse>> {
        Box::pin(async move { self.request_unfiltered_table(request).await })
    }

    fn get_unfiltered_partitions(
        &self,
        request: GlueGetUnfilteredPartitionsRequest,
    ) -> BoxFuture<'_, Result<GluePartitionPage>> {
        Box::pin(async move { self.request_unfiltered_partitions(request).await })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct GetTableBody<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_id: Option<&'a str>,
    database_name: &'a str,
    name: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetTableResponse {
    table: Option<GlueTable>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct GetPartitionsBody<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_id: Option<&'a str>,
    database_name: &'a str,
    table_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    expression: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_token: Option<&'a str>,
    max_results: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetPartitionsResponse {
    #[serde(default)]
    partitions: Vec<GluePartition>,
    #[serde(default)]
    next_token: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct GlueAuditContext<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_columns: Option<&'a [String]>,
    all_columns_requested: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct GlueQuerySessionContext<'a> {
    query_id: &'a str,
    query_start_time: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_authorization_id: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct GetUnfilteredTableBody<'a> {
    catalog_id: &'a str,
    database_name: &'a str,
    name: &'a str,
    audit_context: GlueAuditContext<'a>,
    supported_permission_types: [&'static str; 1],
    permissions: [&'static str; 1],
    query_session_context: GlueQuerySessionContext<'a>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetUnfilteredTableResponse {
    table: Option<GlueTable>,
    #[serde(default)]
    authorized_columns: Vec<String>,
    #[serde(default)]
    is_registered_with_lake_formation: bool,
    #[serde(default)]
    cell_filters: Vec<serde_json::Value>,
    query_authorization_id: Option<String>,
    resource_arn: Option<String>,
    row_filter: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct GetUnfilteredPartitionsBody<'a> {
    catalog_id: &'a str,
    database_name: &'a str,
    table_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    expression: Option<&'a str>,
    audit_context: GlueAuditContext<'a>,
    supported_permission_types: [&'static str; 1],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_token: Option<&'a str>,
    max_results: u16,
    query_session_context: GlueQuerySessionContext<'a>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetUnfilteredPartitionsResponse {
    #[serde(default)]
    unfiltered_partitions: Vec<UnfilteredPartition>,
    #[serde(default)]
    next_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UnfilteredPartition {
    partition: Option<GluePartition>,
    #[serde(default)]
    authorized_columns: Vec<String>,
    #[serde(default)]
    is_registered_with_lake_formation: bool,
}

fn validate_authorization_request(
    requested_columns: &[String],
    all_columns_requested: bool,
    query_id: &str,
) -> Result<()> {
    if all_columns_requested != requested_columns.is_empty() {
        return Err(CdfError::contract(
            "Glue governed request must choose either all columns or a nonempty requested-column list",
        ));
    }
    if query_id.is_empty() || query_id.chars().any(char::is_control) {
        return Err(CdfError::contract(
            "Glue query id must be nonempty and control-free",
        ));
    }
    let mut unique = std::collections::BTreeSet::new();
    for column in requested_columns {
        if column.is_empty()
            || column.chars().any(char::is_control)
            || !unique.insert(column.as_str())
        {
            return Err(CdfError::contract(
                "Glue requested columns must be unique, nonempty, and control-free",
            ));
        }
    }
    Ok(())
}

fn validate_unfiltered_filters(
    row_filter: Option<&str>,
    cell_filters: &[serde_json::Value],
) -> Result<()> {
    if row_filter.is_some_and(|filter| !filter.trim().is_empty()) || !cell_filters.is_empty() {
        return Err(CdfError::contract(
            "Lake Formation requires cell/nested filtering that CDF cannot enforce exactly; query this table through Athena or Trino",
        ));
    }
    Ok(())
}

fn required_control_text(label: &str, value: Option<String>) -> Result<String> {
    let value = value.ok_or_else(|| CdfError::auth(format!("{label} was omitted")))?;
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(CdfError::data(format!(
            "{label} must be nonempty and control-free"
        )));
    }
    Ok(value)
}

fn glue_error(operation: &str, status: u16, payload: Option<&[u8]>) -> CdfError {
    let kind = payload
        .and_then(|payload| serde_json::from_slice::<GlueErrorEnvelope>(payload).ok())
        .and_then(|value| value.kind)
        .unwrap_or_else(|| "unknown".to_owned());
    let kind = kind.rsplit(['#', ':']).next().unwrap_or(&kind);
    let message = format!("AWS Glue {operation} failed ({kind}, HTTP {status})");
    match (status, kind) {
        (401 | 403, _) | (_, "AccessDeniedException" | "UnrecognizedClientException") => {
            CdfError::auth(message)
        }
        (_, "PermissionTypeMismatchException") => CdfError::contract(format!(
            "AWS Glue {operation} requires cell/nested filtering that CDF cannot enforce exactly; query this table through Athena or Trino"
        )),
        (_, "EntityNotFoundException") | (404, _) => CdfError::data(message),
        (429, _) | (_, "ThrottlingException") => CdfError::rate_limited(message, None),
        (408 | 425 | 500..=599, _)
        | (_, "InternalServiceException" | "OperationTimeoutException") => {
            CdfError::transient(message)
        }
        _ => CdfError::data(message),
    }
}

#[derive(Deserialize)]
struct GlueErrorEnvelope {
    #[serde(rename = "__type")]
    kind: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glue_errors_preserve_retry_and_auth_classes_without_payload_details() {
        assert_eq!(
            glue_error(
                "GetTable",
                429,
                Some(br#"{"__type":"ThrottlingException"}"#),
            )
            .kind,
            cdf_kernel::ErrorKind::RateLimited
        );
        assert_eq!(
            glue_error(
                "GetTable",
                403,
                Some(br#"{"__type":"AccessDeniedException"}"#),
            )
            .kind,
            cdf_kernel::ErrorKind::Auth
        );
        assert_eq!(
            glue_error(
                "GetTable",
                500,
                Some(br#"{"__type":"InternalServiceException"}"#),
            )
            .kind,
            cdf_kernel::ErrorKind::Transient
        );
    }

    #[test]
    fn partition_request_enforces_service_bounds_before_transport() {
        let request = GlueGetPartitionsRequest {
            region: "us-west-2".to_owned(),
            catalog_id: None,
            database: "analytics".to_owned(),
            table: "events".to_owned(),
            expression: Some("x".repeat(2049)),
            next_token: None,
            page_size: 1001,
            endpoint: None,
            credentials: None,
            maximum_response_bytes: 1024,
            cancellation: RunCancellation::default(),
        };
        assert!(
            request
                .validate()
                .unwrap_err()
                .message
                .contains("page_size")
        );

        let mut request = request;
        request.page_size = 1000;
        assert!(
            request
                .validate()
                .unwrap_err()
                .message
                .contains("2048-byte")
        );
    }

    #[test]
    fn governed_metadata_admits_only_explicit_column_or_all_column_audit() {
        validate_authorization_request(&[], true, "query-1").unwrap();
        validate_authorization_request(&["id".to_owned()], false, "query-1").unwrap();
        assert!(validate_authorization_request(&[], false, "query-1").is_err());
        assert!(validate_authorization_request(&["id".to_owned()], true, "query-1").is_err());
        assert!(
            validate_authorization_request(&["id".to_owned(), "id".to_owned()], false, "query-1")
                .is_err()
        );
    }

    #[test]
    fn governed_row_or_cell_filters_fail_before_object_access() {
        validate_unfiltered_filters(None, &[]).unwrap();
        for error in [
            validate_unfiltered_filters(Some("tenant_id = 7"), &[]).unwrap_err(),
            validate_unfiltered_filters(None, &[serde_json::json!({"ColumnName": "ssn"})])
                .unwrap_err(),
        ] {
            assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
            assert!(error.message.contains("Athena or Trino"));
        }
    }
}
