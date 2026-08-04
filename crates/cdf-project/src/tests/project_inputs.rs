use std::fs;

use cdf_kernel::ErrorKind;

use super::support::test_source_registry;
use crate::{
    ProjectConfig, ProjectResourceName, ProjectSourceName, inventory_project_source_resources,
};

fn project_config(body: &str) -> ProjectConfig {
    toml::from_str(&format!(
        r#"
[project]
name = "input_authority"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]

{body}
"#
    ))
    .unwrap()
}

fn write_resource(root: &std::path::Path, source: &str, resource: &str, sql: &str) {
    let directory = root.join("sources").join(source);
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join(format!("{resource}.cdf.sql")), sql).unwrap();
}

#[test]
fn project_input_inventory_binds_paths_overlays_drivers_and_hashes_canonically() {
    let root = tempfile::tempdir().unwrap();
    write_resource(root.path(), "warehouse", "orders", "SELECT 2\n");
    write_resource(root.path(), "warehouse", "customers", "SELECT 1\n");
    fs::write(root.path().join("sources/warehouse/README.md"), "notes").unwrap();
    let config = project_config(
        r#"
[sources.warehouse]
type = "postgres"
connection = "secret://env/WAREHOUSE_DSN"

[environments.prod]

[environments.prod.sources.warehouse]
connection = "secret://vault/prod/warehouse"
"#,
    );

    let inventory =
        inventory_project_source_resources(root.path(), &config, "prod", &test_source_registry())
            .unwrap();

    assert_eq!(inventory.environment, "prod");
    assert_eq!(inventory.sources.len(), 1);
    assert_eq!(
        inventory
            .resources
            .iter()
            .map(|resource| resource.resource_id.as_str())
            .collect::<Vec<_>>(),
        vec!["warehouse.customers", "warehouse.orders"]
    );
    assert_eq!(
        inventory
            .resources
            .iter()
            .map(|resource| resource.relative_path.as_str())
            .collect::<Vec<_>>(),
        vec![
            "sources/warehouse/customers.cdf.sql",
            "sources/warehouse/orders.cdf.sql"
        ]
    );
    let source = inventory
        .sources
        .get(&ProjectSourceName::new("warehouse", "test").unwrap())
        .unwrap();
    assert_eq!(source.name.as_str(), "warehouse");
    assert_eq!(source.source_type, "postgres");
    assert_eq!(source.driver.driver_id.as_str(), "postgres");
    assert_eq!(
        source.base_options["connection"],
        "secret://env/WAREHOUSE_DSN"
    );
    assert_eq!(
        source.effective_options["connection"],
        "secret://vault/prod/warehouse"
    );
    assert_ne!(source.base_hash, source.effective_hash);
    assert!(source.base_hash.as_str().starts_with("sha256:"));
    assert!(source.overlay_hash.as_str().starts_with("sha256:"));
    assert!(source.driver_descriptor_hash.starts_with("sha256:"));
    assert!(
        inventory
            .resources
            .iter()
            .all(|resource| resource.content_hash.as_str().starts_with("sha256:"))
    );
}

#[test]
fn project_input_inventory_validates_only_source_options_before_relation_parsing() {
    let root = tempfile::tempdir().unwrap();
    write_resource(root.path(), "warehouse", "orders", "not parsed in D1.5a");
    let config = project_config(
        r#"
[sources.warehouse]
type = "postgres"
connection = "secret://env/WAREHOUSE_DSN"
"#,
    );

    let inventory =
        inventory_project_source_resources(root.path(), &config, "dev", &test_source_registry())
            .unwrap();
    assert_eq!(inventory.resources[0].sql, "not parsed in D1.5a");

    let invalid = project_config(
        r#"
[sources.warehouse]
type = "postgres"
connection = "secret://env/WAREHOUSE_DSN"
invented = true
"#,
    );
    let error =
        inventory_project_source_resources(root.path(), &invalid, "dev", &test_source_registry())
            .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.message.contains("$.source"));
    assert!(error.message.contains("invented"));
}

#[test]
fn project_path_tokens_enforce_the_exact_dedicated_grammar() {
    let longest = format!("a{}", "9_".repeat(63));
    assert_eq!(longest.len(), 127);
    let longest = format!("{longest}z");
    assert_eq!(longest.len(), 128);
    assert_eq!(
        ProjectSourceName::new(&longest, "sources/<name>")
            .unwrap()
            .as_str(),
        longest
    );
    assert!(ProjectResourceName::new("a", "resource").is_ok());
    for invalid in [
        "",
        "Warehouse",
        "order-items",
        "1orders",
        "_orders",
        "café",
        &format!("a{}", "b".repeat(128)),
    ] {
        let error = ProjectSourceName::new(invalid, "sources/exact-path").unwrap_err();
        assert_eq!(error.kind, ErrorKind::Contract);
        assert!(error.message.contains("sources/exact-path"));
        assert!(error.message.contains("[a-z][a-z0-9_]{0,127}"));
    }
}

#[test]
fn project_input_inventory_rejects_orphan_missing_and_empty_sources() {
    let orphan_root = tempfile::tempdir().unwrap();
    write_resource(orphan_root.path(), "warehouse", "orders", "SELECT 1");
    let error = inventory_project_source_resources(
        orphan_root.path(),
        &project_config(""),
        "dev",
        &test_source_registry(),
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.message.contains("[sources.warehouse]"));

    let configured = project_config(
        r#"
[sources.warehouse]
type = "postgres"
connection = "secret://env/WAREHOUSE_DSN"
"#,
    );
    let missing_root = tempfile::tempdir().unwrap();
    let error = inventory_project_source_resources(
        missing_root.path(),
        &configured,
        "dev",
        &test_source_registry(),
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.message.contains("sources"));

    let empty_root = tempfile::tempdir().unwrap();
    fs::create_dir_all(empty_root.path().join("sources/warehouse")).unwrap();
    let error = inventory_project_source_resources(
        empty_root.path(),
        &configured,
        "dev",
        &test_source_registry(),
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.message.contains("no valid regular"));
}

#[test]
fn project_input_inventory_rejects_environment_source_shape_changes() {
    let root = tempfile::tempdir().unwrap();
    write_resource(root.path(), "warehouse", "orders", "SELECT 1");
    let type_override = project_config(
        r#"
[sources.warehouse]
type = "postgres"
connection = "secret://env/WAREHOUSE_DSN"

[environments.dev.sources.warehouse]
type = "postgres"
"#,
    );
    let error = inventory_project_source_resources(
        root.path(),
        &type_override,
        "dev",
        &test_source_registry(),
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(
        error
            .message
            .contains("may not override immutable source `type`")
    );

    let added_source = project_config(
        r#"
[sources.warehouse]
type = "postgres"
connection = "secret://env/WAREHOUSE_DSN"

[environments.dev.sources.archive]
connection = "secret://env/ARCHIVE_DSN"
"#,
    );
    let error = inventory_project_source_resources(
        root.path(),
        &added_source,
        "dev",
        &test_source_registry(),
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.message.contains("may not add a source"));
}

#[test]
fn project_input_inventory_rejects_nested_and_malformed_resource_inputs() {
    let config = project_config(
        r#"
[sources.warehouse]
type = "postgres"
connection = "secret://env/WAREHOUSE_DSN"
"#,
    );
    let nested_root = tempfile::tempdir().unwrap();
    fs::create_dir_all(nested_root.path().join("sources/warehouse/nested")).unwrap();
    let error = inventory_project_source_resources(
        nested_root.path(),
        &config,
        "dev",
        &test_source_registry(),
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.message.contains("unsupported filesystem shape"));

    let malformed_root = tempfile::tempdir().unwrap();
    fs::create_dir_all(malformed_root.path().join("sources/warehouse")).unwrap();
    fs::write(
        malformed_root
            .path()
            .join("sources/warehouse/orders.CDF.SQL"),
        "SELECT 1",
    )
    .unwrap();
    let error = inventory_project_source_resources(
        malformed_root.path(),
        &config,
        "dev",
        &test_source_registry(),
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.message.contains("malformed resource input"));

    let invalid_driver = project_config(
        r#"
[sources.warehouse]
type = "not_registered"
"#,
    );
    let error = inventory_project_source_resources(
        malformed_root.path(),
        &invalid_driver,
        "dev",
        &test_source_registry(),
    )
    .unwrap_err();
    assert!(error.message.contains("malformed resource input"));
    assert!(!error.message.contains("no source driver registered"));
}

#[test]
fn project_input_inventory_rejects_files_directly_under_sources() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir_all(root.path().join("sources")).unwrap();
    fs::write(root.path().join("sources/orders"), "SELECT 1").unwrap();

    let error = inventory_project_source_resources(
        root.path(),
        &project_config(""),
        "dev",
        &test_source_registry(),
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.message.contains("must be a real directory"));
}

#[cfg(unix)]
#[test]
fn project_input_inventory_rejects_symlinked_authority() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::create_dir_all(root.path().join("sources")).unwrap();
    symlink(outside.path(), root.path().join("sources/warehouse")).unwrap();
    let config = project_config(
        r#"
[sources.warehouse]
type = "postgres"
connection = "secret://env/WAREHOUSE_DSN"
"#,
    );

    let error =
        inventory_project_source_resources(root.path(), &config, "dev", &test_source_registry())
            .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.message.contains("may not be a symlink"));
}

#[test]
fn project_input_inventory_allows_a_completely_empty_project() {
    let root = tempfile::tempdir().unwrap();
    let inventory = inventory_project_source_resources(
        root.path(),
        &project_config(""),
        "dev",
        &test_source_registry(),
    )
    .unwrap();
    assert!(inventory.sources.is_empty());
    assert!(inventory.resources.is_empty());
    assert_eq!(inventory.total_authored_bytes, 0);
}
