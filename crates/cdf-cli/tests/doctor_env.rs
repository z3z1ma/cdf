use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

#[test]
fn doctor_resolves_child_process_env_secrets_without_leaking_values() {
    let project = TestProject::new();
    fs::write(project.root.join("sql-dsn"), "resolved-file-sql-value\n").unwrap();
    write_project(&project);

    let output = Command::new(env!("CARGO_BIN_EXE_cdf"))
        .args(["--json", "--project", project.root_str(), "doctor"])
        .env(
            "CDF_CLI_ENV_DESTINATION_DSN",
            "resolved-env-destination-value",
        )
        .env("CDF_CLI_ENV_AUTH_TOKEN", "resolved-env-auth-token-value")
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(output.status.success(), "stderr: {stderr}");
    for secret in [
        "resolved-env-destination-value",
        "resolved-env-auth-token-value",
        "resolved-file-sql-value",
    ] {
        assert!(!stdout.contains(secret), "stdout leaked {secret}");
        assert!(!stderr.contains(secret), "stderr leaked {secret}");
    }

    let json: Value = serde_json::from_str(&stdout).unwrap();
    let secrets = named_check(&json, "secrets");
    assert_eq!(secrets["status"], "passed");
    assert_eq!(secrets["details"]["count"], 3);
    let references = secrets["details"]["references"].as_array().unwrap();
    for reference in [
        "secret://env/CDF_CLI_ENV_DESTINATION_DSN",
        "secret://env/CDF_CLI_ENV_AUTH_TOKEN",
        "secret://file/sql-dsn",
    ] {
        assert!(
            references.iter().any(|value| value == reference),
            "missing secret reference {reference}"
        );
    }
}

fn write_project(project: &TestProject) {
    fs::create_dir_all(project.root.join("resources")).unwrap();
    fs::write(
        project.root.join("cdf.toml"),
        r#"
[project]
name = "cli_env_test"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "postgres://secret://env/CDF_CLI_ENV_DESTINATION_DSN"

[resources."api.*"]
source = "resources/api.toml"

[resources."warehouse.*"]
source = "resources/sql.toml"
"#,
    )
    .unwrap();
    fs::write(
        project.root.join("resources/api.toml"),
        r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"
auth = { kind = "bearer", token = "secret://env/CDF_CLI_ENV_AUTH_TOKEN" }

[resource.items]
path = "/items"
records = "$"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
] }
"#,
    )
    .unwrap();
    fs::write(
        project.root.join("resources/sql.toml"),
        r#"
[source.warehouse]
kind = "sql"
connection = "secret://file/sql-dsn"

[resource.orders]
table = "orders"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
] }
"#,
    )
    .unwrap();
}

fn named_check<'a>(json: &'a Value, name: &str) -> &'a Value {
    json["result"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == name)
        .unwrap()
}

struct TestProject {
    root: PathBuf,
}

impl TestProject {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::current_dir()
            .unwrap()
            .join("target/quality/test-projects");
        fs::create_dir_all(&base).unwrap();
        let root = base.join(format!(
            "cdf-cli-doctor-env-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        Self { root }
    }

    fn root_str(&self) -> &str {
        self.root.to_str().unwrap()
    }
}

impl Drop for TestProject {
    fn drop(&mut self) {
        let _ = remove_dir_all_if_exists(&self.root);
    }
}

fn remove_dir_all_if_exists(path: &Path) -> std::io::Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
