use std::{collections::BTreeSet, path::Path, process::Command};

fn cargo_tree(package: &str, edges: &str) -> String {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("contract crate is below the workspace root");
    let output = Command::new(env!("CARGO"))
        .current_dir(workspace)
        .args([
            "tree", "--locked", "-p", package, "-e", edges, "--prefix", "none",
        ])
        .output()
        .expect("run cargo tree");
    assert!(
        output.status.success(),
        "cargo tree failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("cargo tree output is UTF-8")
}

fn package_names(tree: &str) -> BTreeSet<&str> {
    tree.lines()
        .filter_map(|line| line.split_whitespace().next())
        .collect()
}

#[test]
fn package_contract_graph_contains_only_admitted_workspace_layers() {
    let tree = cargo_tree("cdf-package-contract", "normal,dev");
    let packages = package_names(&tree);
    let unexpected = packages
        .iter()
        .copied()
        .filter(|package| package.starts_with("cdf-"))
        .filter(|package| !matches!(*package, "cdf-package-contract" | "cdf-kernel"))
        .collect::<Vec<_>>();
    assert!(
        unexpected.is_empty(),
        "cdf-package-contract reaches forbidden workspace packages {unexpected:?}:\n{tree}"
    );

    for forbidden in [
        "datafusion",
        "parquet",
        "arrow-ipc",
        "tempfile",
        "object_store",
        "postgres",
        "duckdb",
        "reqwest",
    ] {
        assert!(
            !packages.contains(forbidden),
            "cdf-package-contract reaches forbidden package {forbidden}:\n{tree}"
        );
    }
}
