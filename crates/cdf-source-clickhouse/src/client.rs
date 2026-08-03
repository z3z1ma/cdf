use std::{
    fmt,
    sync::{Arc, OnceLock},
};

use clickhouse::{Client, Compression};
use clickhouse_ext_arrow::{ArrowCursor, ArrowQueryExt, ArrowStreamLimits};

use cdf_kernel::{CdfError, Result};
use cdf_memory::MemoryLease;

use crate::{
    error::classify_clickhouse_error, identifier::ClickHouseIdentifier, query::QueryParameter,
};

struct TransportAuthorizedClient {
    client: Client,
    _transport_lease: MemoryLease,
}

pub(crate) struct ClickHouseArrowCursor {
    cursor: ArrowCursor,
    _transport_authority: Arc<TransportAuthorizedClient>,
    _cursor_state_lease: MemoryLease,
}

impl ClickHouseArrowCursor {
    pub(crate) fn schema(&self) -> Option<arrow_schema::SchemaRef> {
        self.cursor.schema()
    }

    pub(crate) async fn next(
        &mut self,
    ) -> clickhouse::error::Result<Option<arrow_array::RecordBatch>> {
        self.cursor.next().await
    }
}

#[derive(Clone)]
pub(crate) struct ClickHouseConnection {
    pub(crate) endpoint: String,
    pub(crate) database: ClickHouseIdentifier,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) max_threads: u64,
    pub(crate) max_block_rows: u64,
    client: Arc<OnceLock<Arc<TransportAuthorizedClient>>>,
}

impl fmt::Debug for ClickHouseConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClickHouseConnection")
            .field("endpoint", &self.endpoint)
            .field("database", &self.database)
            .field("username", &self.username.as_ref().map(|_| "<redacted>"))
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("max_threads", &self.max_threads)
            .field("max_block_rows", &self.max_block_rows)
            .finish()
    }
}

impl ClickHouseConnection {
    pub(crate) fn new(
        endpoint: String,
        database: ClickHouseIdentifier,
        username: Option<String>,
        password: Option<String>,
        max_threads: u64,
        max_block_rows: u64,
    ) -> Self {
        Self {
            endpoint,
            database,
            username,
            password,
            max_threads,
            max_block_rows,
            client: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if !matches!(self.endpoint.split_once("://"), Some(("http" | "https", _))) {
            return Err(CdfError::contract(
                "ClickHouse operational endpoint must use HTTP or HTTPS",
            ));
        }
        if self.max_threads == 0 || self.max_threads > 256 {
            return Err(CdfError::contract(
                "ClickHouse max_threads must be between 1 and 256",
            ));
        }
        if self.max_block_rows == 0 || self.max_block_rows > 1_000_000 {
            return Err(CdfError::contract(
                "ClickHouse max_block_rows must be between 1 and 1,000,000",
            ));
        }
        if self.username.as_ref().is_some_and(String::is_empty)
            || self.password.as_ref().is_some_and(String::is_empty)
        {
            return Err(CdfError::auth(
                "ClickHouse username and password secrets must not resolve empty",
            ));
        }
        Ok(())
    }

    pub(crate) fn install_transport_authority(&self, transport_lease: MemoryLease) -> Result<()> {
        self.validate()?;
        if transport_lease.bytes() < crate::memory::CLICKHOUSE_HTTP1_TRANSPORT_BYTES {
            return Err(CdfError::internal(format!(
                "ClickHouse transport requires a {}-byte lease, admitted {} bytes",
                crate::memory::CLICKHOUSE_HTTP1_TRANSPORT_BYTES,
                transport_lease.bytes()
            )));
        }
        if self.client.get().is_some() {
            return Ok(());
        }
        let mut client = Client::default()
            .with_url(&self.endpoint)
            .with_database(self.database.as_str())
            // A compressed response frame declares its decoded allocation before the adapter can
            // admit it to CDF memory accounting. Plain HTTP keeps the official Arrow decoder
            // inside the schema-derived, server-enforced block envelope.
            .with_compression(Compression::None);
        if let Some(username) = &self.username {
            client = client.with_user(username);
        }
        if let Some(password) = &self.password {
            client = client.with_password(password);
        }
        // The lease is stored in the same Arc as the client. The connection retains that Arc for
        // idle-pool lifetime and every returned cursor clones it, so no pooled transport clone can
        // outlive its memory authority.
        let _ = self.client.set(Arc::new(TransportAuthorizedClient {
            client,
            _transport_lease: transport_lease,
        }));
        Ok(())
    }

    fn client(&self) -> Result<Arc<TransportAuthorizedClient>> {
        self.validate()?;
        self.client.get().cloned().ok_or_else(|| {
            CdfError::internal(
                "ClickHouse client was used before transport memory authority was installed",
            )
        })
    }

    pub(crate) fn arrow_query_with_max_block_rows(
        &self,
        sql: &str,
        parameters: Vec<QueryParameter>,
        action: &str,
        maximum_block_rows: u64,
        cursor_state_lease: MemoryLease,
        limits: ArrowStreamLimits,
    ) -> Result<ClickHouseArrowCursor> {
        if cursor_state_lease.bytes() < crate::memory::CLICKHOUSE_CURSOR_STATE_BYTES {
            return Err(CdfError::internal(format!(
                "ClickHouse Arrow cursor requires a {}-byte state lease, admitted {} bytes",
                crate::memory::CLICKHOUSE_CURSOR_STATE_BYTES,
                cursor_state_lease.bytes()
            )));
        }
        let transport_authority = self.client()?;
        let mut query = transport_authority
            .client
            .clone()
            .query(sql)
            .with_setting("readonly", "1")
            .with_setting("max_threads", self.max_threads.to_string())
            .with_setting("max_block_size", maximum_block_rows.to_string())
            // ClickHouse String permits arbitrary bytes, so Binary is the truthful default.
            .with_setting("output_format_arrow_string_as_string", "0");
        for parameter in parameters {
            query = match parameter {
                QueryParameter::Boolean(value) => query.bind(value),
                QueryParameter::Signed(value) => query.bind(value),
                QueryParameter::Unsigned(value) => query.bind(value),
                QueryParameter::Float(value) => query.bind(value),
                QueryParameter::String(value) => query.bind(value),
            };
        }
        let cursor = query
            .fetch_arrow_with_limits(limits)
            .map_err(|error| classify_clickhouse_error(action, error))?;
        Ok(ClickHouseArrowCursor {
            cursor,
            _transport_authority: transport_authority,
            _cursor_state_lease: cursor_state_lease,
        })
    }
}
