use std::{
    env,
    net::TcpListener,
    path::PathBuf,
    process::Command,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use cdf_kernel::{CdfError, Result};
use postgres::{Client, NoTls};
use tempfile::TempDir;

static LIVE_POSTGRES_SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);
static LOCAL_POSTGRES_START: Mutex<()> = Mutex::new(());

pub(crate) struct LivePostgres {
    url: String,
    schema: String,
    _server: Option<LocalPostgres>,
}

struct LocalPostgres {
    data_dir: TempDir,
    _socket_dir: TempDir,
    pg_ctl: PathBuf,
    port: u16,
}

impl LivePostgres {
    pub(crate) fn start() -> Result<Self> {
        let (url, server) = match env::var("TEST_DATABASE_URL") {
            Ok(url) if !url.trim().is_empty() => (url, None),
            _ => {
                let server = LocalPostgres::start()?;
                (server.url(), Some(server))
            }
        };
        let schema = format!(
            "cdf_conformance_run_matrix_{}_{}",
            std::process::id(),
            LIVE_POSTGRES_SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        create_postgres_schema(&url, &schema)?;
        Ok(Self {
            url,
            schema,
            _server: server,
        })
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn schema(&self) -> &str {
        &self.schema
    }

    pub(crate) fn create_source_events_table(&self, table: &str) -> Result<String> {
        let qualified = qualified_name(&self.schema, table);
        Client::connect(&self.url, NoTls)
            .map_err(|error| CdfError::destination(format!("connect to Postgres: {error}")))?
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {qualified};
                 CREATE TABLE {qualified} (
                    \"id\" BIGINT NOT NULL,
                    \"name\" TEXT,
                    \"updated_at\" BIGINT NOT NULL
                 );
                 INSERT INTO {qualified} (\"id\", \"name\", \"updated_at\")
                 VALUES (1, 'ada', 10), (2, 'grace', 20)"
            ))
            .map_err(|error| {
                CdfError::destination(format!("create run matrix SQL source table: {error}"))
            })?;
        Ok(format!("{}.{}", self.schema, table))
    }
}

impl Drop for LivePostgres {
    fn drop(&mut self) {
        if let Ok(mut client) = Client::connect(&self.url, NoTls) {
            let _ = client.batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_identifier(&self.schema)
            ));
        }
    }
}

impl LocalPostgres {
    fn start() -> Result<Self> {
        let _guard = LOCAL_POSTGRES_START.lock().unwrap();
        let initdb = find_binary("initdb").ok_or_else(|| {
            CdfError::data("C2 run matrix requires initdb on PATH or TEST_DATABASE_URL")
        })?;
        let pg_ctl = find_binary("pg_ctl").ok_or_else(|| {
            CdfError::data("C2 run matrix requires pg_ctl on PATH or TEST_DATABASE_URL")
        })?;
        let data_dir = tempfile::tempdir()
            .map_err(|error| CdfError::data(format!("create Postgres data dir: {error}")))?;
        let socket_dir = tempfile::tempdir()
            .map_err(|error| CdfError::data(format!("create Postgres socket dir: {error}")))?;
        let port = free_port().ok_or_else(|| CdfError::data("allocate local Postgres port"))?;
        let data_dir_str = data_dir.path().to_str().ok_or_else(|| {
            CdfError::data(format!(
                "local Postgres data dir is not UTF-8: {}",
                data_dir.path().display()
            ))
        })?;

        let init_status = Command::new(&initdb)
            .args(["-D", data_dir_str])
            .args(["-A", "trust"])
            .args(["-U", "cdf"])
            .arg("--no-sync")
            .status()
            .map_err(|error| CdfError::destination(format!("run initdb: {error}")))?;
        if !init_status.success() {
            return Err(CdfError::destination(format!(
                "initdb failed with status {init_status}"
            )));
        }

        let options = format!("-h 127.0.0.1 -p {port} -k {}", socket_dir.path().display());
        let log_path = data_dir.path().join("postgres.log");
        let log_path_str = log_path.to_str().ok_or_else(|| {
            CdfError::data(format!(
                "local Postgres log path is not UTF-8: {}",
                log_path.display()
            ))
        })?;
        let start_status = Command::new(&pg_ctl)
            .args(["-D", data_dir_str])
            .args(["-l", log_path_str])
            .args(["-o", &options])
            .args(["-w", "start"])
            .status()
            .map_err(|error| CdfError::destination(format!("run pg_ctl start: {error}")))?;
        if !start_status.success() {
            return Err(CdfError::destination(format!(
                "pg_ctl start failed with status {start_status}; log: {}",
                log_path.display()
            )));
        }

        Ok(Self {
            data_dir,
            _socket_dir: socket_dir,
            pg_ctl,
            port,
        })
    }

    fn url(&self) -> String {
        format!("postgresql://cdf@127.0.0.1:{}/postgres", self.port)
    }
}

impl Drop for LocalPostgres {
    fn drop(&mut self) {
        let _ = Command::new(&self.pg_ctl)
            .args(["-D", self.data_dir.path().to_str().unwrap()])
            .args(["-m", "fast"])
            .args(["-w", "stop"])
            .status();
    }
}

pub(crate) fn reset_postgres_schema(database_url: &str, schema: &str) -> Result<()> {
    let schema = quote_identifier(schema);
    Client::connect(database_url, NoTls)
        .map_err(|error| CdfError::destination(format!("connect to Postgres: {error}")))?
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
        ))
        .map_err(|error| CdfError::destination(format!("reset Postgres schema: {error}")))
}

pub(crate) fn qualified_name(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}

fn create_postgres_schema(database_url: &str, schema: &str) -> Result<()> {
    Client::connect(database_url, NoTls)
        .map_err(|error| CdfError::destination(format!("connect to Postgres: {error}")))?
        .batch_execute(&format!("CREATE SCHEMA {}", quote_identifier(schema)))
        .map_err(|error| CdfError::destination(format!("create Postgres schema: {error}")))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn find_binary(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|path| path.join(name))
            .find(|candidate| candidate.is_file())
    })
}

fn free_port() -> Option<u16> {
    TcpListener::bind("127.0.0.1:0")
        .ok()?
        .local_addr()
        .ok()
        .map(|addr| addr.port())
}
