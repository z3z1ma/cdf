use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};

use arrow_array::RecordBatch;
use cdf_kernel::{CdfError, Result};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest, reserve,
};
use clickhouse::{Client, Compression, ResponseLimits};
use clickhouse_ext_arrow::ArrowClientExt;

use crate::{error::classify_clickhouse_error, identifier::ClickHouseIdentifier};

pub(crate) const HTTP_TRANSPORT_BYTES: u64 = clickhouse::DEFAULT_HTTP1_MAX_BUFFER_BYTES as u64;
pub(crate) const ARROW_WRITER_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAXIMUM_INPUT_BATCH_BYTES: u64 = 32 * 1024 * 1024;
const RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const RESPONSE_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct ClickHouseConnectionOptions {
    pub(crate) endpoint: String,
    pub(crate) database: ClickHouseIdentifier,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
}

impl std::fmt::Debug for ClickHouseConnectionOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClickHouseConnectionOptions")
            .field("endpoint", &self.endpoint)
            .field("database", &self.database)
            .field("username", &self.username.as_ref().map(|_| "<redacted>"))
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct AuthorizedClickHouseClient {
    client: Client,
    _transport: Arc<MemoryLease>,
}

impl AuthorizedClickHouseClient {
    pub(crate) fn new(
        options: &ClickHouseConnectionOptions,
        transport: MemoryLease,
    ) -> Result<Self> {
        if transport.bytes() < HTTP_TRANSPORT_BYTES {
            return Err(CdfError::internal(format!(
                "ClickHouse destination transport needs {HTTP_TRANSPORT_BYTES} bytes, admitted {}",
                transport.bytes()
            )));
        }
        let mut client = Client::default()
            .with_url(&options.endpoint)
            .with_database(options.database.as_str())
            .with_compression(Compression::Lz4)
            .with_setting("async_insert", "0")
            .with_setting("wait_for_async_insert", "1");
        if let Some(username) = &options.username {
            client = client.with_user(username);
        }
        if let Some(password) = &options.password {
            client = client.with_password(password);
        }
        Ok(Self {
            client,
            _transport: Arc::new(transport),
        })
    }

    pub(crate) fn query(&self, sql: &str) -> clickhouse::query::Query {
        self.client
            .query(sql)
            .with_response_limits(response_limits())
    }

    pub(crate) async fn execute(&self, sql: &str, action: &str) -> Result<()> {
        self.query(sql)
            .execute()
            .await
            .map_err(|error| classify_clickhouse_error(action, error))
    }

    pub(crate) async fn insert_arrow_batches<I>(
        &self,
        sql: &str,
        token: &str,
        batches: I,
        action: &str,
    ) -> Result<u64>
    where
        I: IntoIterator<Item = Result<RecordBatch>>,
    {
        let client = self
            .client
            .clone()
            .with_setting("async_insert", "0")
            .with_setting("wait_for_async_insert", "1")
            .with_setting("insert_deduplication_token", token);
        let mut insert = client.insert_arrow_with(sql);
        let mut rows = 0_u64;
        for batch in batches {
            let batch = batch?;
            let batch_bytes = u64::try_from(batch.get_array_memory_size())
                .map_err(|_| CdfError::data("ClickHouse Arrow batch memory size exceeds u64"))?;
            if batch_bytes > MAXIMUM_INPUT_BATCH_BYTES {
                return Err(CdfError::data(format!(
                    "ClickHouse Arrow batch retains {batch_bytes} bytes beyond the {MAXIMUM_INPUT_BATCH_BYTES}-byte admitted input ceiling"
                )));
            }
            rows = rows
                .checked_add(
                    u64::try_from(batch.num_rows()).map_err(|_| {
                        CdfError::data("ClickHouse Arrow batch row count exceeds u64")
                    })?,
                )
                .ok_or_else(|| CdfError::data("ClickHouse inserted row count overflowed"))?;
            insert
                .write(&batch)
                .await
                .map_err(|error| classify_clickhouse_error(action, error))?;
        }
        insert
            .flush()
            .await
            .map_err(|error| classify_clickhouse_error(action, error))?;
        insert
            .end()
            .await
            .map_err(|error| classify_clickhouse_error(action, error))?;
        Ok(rows)
    }
}

pub(crate) async fn shared_authorized_client(
    cache: Arc<OnceLock<AuthorizedClickHouseClient>>,
    options: ClickHouseConnectionOptions,
    memory: Arc<dyn MemoryCoordinator>,
) -> Result<AuthorizedClickHouseClient> {
    if let Some(client) = cache.get() {
        return Ok(client.clone());
    }
    let request = ReservationRequest::new(
        ConsumerKey::new("clickhouse-destination-http1", MemoryClass::Destination)?,
        HTTP_TRANSPORT_BYTES,
    )?
    .as_minimum_working_set();
    let transport = reserve(memory, request).await?;
    let candidate = AuthorizedClickHouseClient::new(&options, transport)?;
    if cache.set(candidate.clone()).is_ok() {
        return Ok(candidate);
    }
    cache
        .get()
        .cloned()
        .ok_or_else(|| CdfError::internal("ClickHouse client cache lost concurrent installation"))
}

fn response_limits() -> ResponseLimits {
    let response = NonZeroUsize::new(RESPONSE_BYTES).unwrap_or(NonZeroUsize::MIN);
    let chunk = NonZeroUsize::new(RESPONSE_CHUNK_BYTES).unwrap_or(NonZeroUsize::MIN);
    ResponseLimits::new(response, response, chunk, response)
}
