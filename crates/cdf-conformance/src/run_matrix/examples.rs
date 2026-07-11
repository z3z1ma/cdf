use std::{
    ffi::OsString,
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use tempfile::TempDir;

use super::{local_postgres::LivePostgres, test_support::copy_dir_all};

#[test]
fn rest_fixture_example_executes_as_a_project() {
    let fixture = fs::read_to_string(example_root("rest-fixture").join("fixtures/events")).unwrap();
    let server = JsonServer::start(fixture);
    let project = copy_example("rest-fixture");
    let resource_path = project.path().join("resources/api.toml");
    let resource = fs::read_to_string(&resource_path)
        .unwrap()
        .replace("http://127.0.0.1:8765", &server.base_url());
    fs::write(resource_path, resource).unwrap();

    for args in [
        vec!["validate", "--deep"],
        vec!["plan", "api.events"],
        vec!["run", "api.events"],
    ] {
        invoke_success(project.path(), &args, None);
    }
    assert!(project.path().join(".cdf/example.duckdb").is_file());
}

#[test]
fn postgres_example_executes_as_a_project() {
    let postgres = LivePostgres::start().unwrap();
    let table = postgres
        .create_source_events_table("cdf_example_orders")
        .unwrap();
    let project = copy_example("postgres");
    let resource_path = project.path().join("resources/warehouse.toml");
    let resource = fs::read_to_string(&resource_path)
        .unwrap()
        .replace("public.cdf_example_orders", &table);
    fs::write(resource_path, resource).unwrap();
    fs::write(project.path().join("postgres-dsn"), postgres.url()).unwrap();

    for args in [
        vec!["validate", "--deep"],
        vec!["plan", "warehouse.orders"],
        vec!["run", "warehouse.orders"],
    ] {
        invoke_success(project.path(), &args, Some(postgres.url()));
    }
    assert!(project.path().join(".cdf/example.duckdb").is_file());
}

fn example_root(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join(name)
}

fn copy_example(name: &str) -> TempDir {
    let root = tempfile::tempdir().unwrap();
    copy_dir_all(&example_root(name), root.path()).unwrap();
    root
}

fn invoke_success(root: &Path, args: &[&str], secret: Option<&str>) {
    let mut argv = vec![
        OsString::from("cdf"),
        OsString::from("--json"),
        OsString::from("--project"),
        root.as_os_str().to_os_string(),
    ];
    argv.extend(args.iter().map(OsString::from));
    let result = cdf_cli::invoke(argv);
    assert_eq!(
        result.exit_code, 0,
        "stdout:\n{}\nstderr:\n{}",
        result.stdout, result.stderr
    );
    if let Some(secret) = secret {
        assert!(!result.stdout.contains(secret));
        assert!(!result.stderr.contains(secret));
    }
}

struct JsonServer {
    address: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl JsonServer {
    fn start(body: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => respond(&mut stream, &body),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("example fixture accept failed: {error}"),
                }
            }
        });
        Self {
            address,
            stop,
            thread: Some(thread),
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }
}

impl Drop for JsonServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn respond(stream: &mut TcpStream, body: &str) {
    let mut request = [0_u8; 4096];
    let _ = stream.read(&mut request);
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}
