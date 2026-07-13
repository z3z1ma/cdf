use std::{collections::BTreeSet, path::Path, process::Command};

fn cargo_tree(edges: &str) -> String {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("runtime crate is below the workspace root");
    let output = Command::new(env!("CARGO"))
        .current_dir(workspace)
        .args([
            "tree",
            "--locked",
            "-p",
            "cdf-runtime",
            "-e",
            edges,
            "--prefix",
            "none",
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
fn runtime_graph_excludes_package_implementation_and_codecs() {
    for edges in ["normal", "normal,dev"] {
        let tree = cargo_tree(edges);
        let packages = package_names(&tree);
        for forbidden in ["cdf-package", "parquet", "arrow-ipc"] {
            assert!(
                !packages.contains(forbidden),
                "cdf-runtime {edges} graph reaches forbidden package {forbidden}:\n{tree}"
            );
        }
        if edges == "normal" {
            assert!(
                !packages.contains("tempfile"),
                "cdf-runtime normal graph reaches forbidden package tempfile:\n{tree}"
            );
        }
    }
}
