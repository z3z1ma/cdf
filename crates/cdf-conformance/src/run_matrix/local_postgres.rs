use std::{
    env::{self, VarError},
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
            Ok(_) | Err(VarError::NotPresent) => {
                let server = LocalPostgres::start()?;
                (server.url(), Some(server))
            }
            Err(VarError::NotUnicode(_)) => {
                return Err(CdfError::contract("TEST_DATABASE_URL is not valid Unicode"));
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
            .map_err(|error| conformance_postgres_error("connect to Postgres", error))?
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
                conformance_postgres_error("create run matrix SQL source table", error)
            })?;
        Ok(format!("{}.{}", self.schema, table))
    }

    pub(crate) fn alter_source_events_id_to_integer(&self, table: &str) -> Result<()> {
        let qualified = qualified_name(&self.schema, table);
        Client::connect(&self.url, NoTls)
            .map_err(|error| conformance_postgres_error("connect to Postgres", error))?
            .batch_execute(&format!(
                "ALTER TABLE {qualified} ALTER COLUMN \"id\" TYPE INTEGER"
            ))
            .map_err(|error| conformance_postgres_error("alter run matrix SQL source table", error))
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
        let _guard = LOCAL_POSTGRES_START
            .lock()
            .map_err(|_| CdfError::internal("local Postgres startup lock was poisoned"))?;
        let initdb = find_binary("initdb")?;
        let pg_ctl = find_binary("pg_ctl")?;
        let data_dir = tempfile::tempdir()
            .map_err(|error| crate::conformance_host_error("create Postgres data dir", error))?;
        let socket_dir = tempfile::tempdir()
            .map_err(|error| crate::conformance_host_error("create Postgres socket dir", error))?;
        let port = free_port()?;

        let init_status = Command::new(&initdb)
            .arg("-D")
            .arg(data_dir.path())
            .args(["-A", "trust"])
            .args(["-U", "cdf"])
            .arg("--no-sync")
            .status()
            .map_err(|error| crate::conformance_host_error("run initdb", error))?;
        if !init_status.success() {
            return Err(CdfError::environment(format!(
                "initdb failed with status {init_status}"
            )));
        }

        let options = format!("-h 127.0.0.1 -p {port} -k {}", socket_dir.path().display());
        let log_path = data_dir.path().join("postgres.log");
        let start_status = Command::new(&pg_ctl)
            .arg("-D")
            .arg(data_dir.path())
            .arg("-l")
            .arg(&log_path)
            .args(["-o", &options])
            .args(["-w", "start"])
            .status()
            .map_err(|error| crate::conformance_host_error("run pg_ctl start", error))?;
        if !start_status.success() {
            return Err(CdfError::environment(format!(
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
            .arg("-D")
            .arg(self.data_dir.path())
            .args(["-m", "fast"])
            .args(["-w", "stop"])
            .status();
    }
}

pub(crate) fn reset_postgres_schema(database_url: &str, schema: &str) -> Result<()> {
    let schema = quote_identifier(schema);
    connect_postgres("connect to Postgres", database_url)?
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
        ))
        .map_err(|error| conformance_postgres_error("reset Postgres schema", error))
}

pub(crate) fn qualified_name(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}

fn create_postgres_schema(database_url: &str, schema: &str) -> Result<()> {
    connect_postgres("connect to Postgres", database_url)?
        .batch_execute(&format!("CREATE SCHEMA {}", quote_identifier(schema)))
        .map_err(|error| conformance_postgres_error("create Postgres schema", error))
}

fn connect_postgres(action: &str, database_url: &str) -> Result<Client> {
    let config = database_url.parse::<postgres::Config>().map_err(|error| {
        CdfError::contract(format!(
            "{action}: TEST_DATABASE_URL is not a valid Postgres connection string: {error}"
        ))
    })?;
    config
        .connect(NoTls)
        .map_err(|error| conformance_postgres_error(action, error))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn conformance_postgres_error(action: &str, error: postgres::Error) -> CdfError {
    if error
        .as_db_error()
        .is_some_and(|database_error| database_error.code().code().starts_with("28"))
    {
        CdfError::auth(format!("{action}: {error}"))
    } else {
        CdfError::destination(format!("{action}: {error}"))
    }
}

#[test]
fn malformed_test_database_url_is_contract_owned_before_connection() {
    let error = connect_postgres("connect to Postgres", "postgresql://[")
        .err()
        .expect("malformed Postgres URL must fail before connection");

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(error.message.contains("TEST_DATABASE_URL"));
}

fn find_binary(name: &str) -> Result<PathBuf> {
    let paths = env::var_os("PATH").ok_or_else(|| {
        CdfError::environment(format!(
            "C2 run matrix requires {name} on PATH or TEST_DATABASE_URL, but PATH is unavailable"
        ))
    })?;
    for path in env::split_paths(&paths) {
        let candidate = path.join(name);
        match std::fs::metadata(&candidate) {
            Ok(metadata) if metadata.is_file() => return Ok(candidate),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(crate::conformance_host_error(
                    &format!("inspect executable candidate {}", candidate.display()),
                    error,
                ));
            }
        }
    }
    Err(CdfError::environment(format!(
        "C2 run matrix requires {name} on PATH or TEST_DATABASE_URL"
    )))
}

fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|error| crate::conformance_host_error("allocate local Postgres port", error))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|error| crate::conformance_host_error("inspect local Postgres port", error))
}
