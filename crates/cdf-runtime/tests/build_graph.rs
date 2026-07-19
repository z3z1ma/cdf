use std::{collections::BTreeSet, path::Path, process::Command};

fn cargo_tree(package: &str, edges: &str) -> String {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("runtime crate is below the workspace root");
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
fn runtime_graph_excludes_package_implementation_and_codecs() {
    for edges in ["normal", "normal,dev"] {
        let tree = cargo_tree("cdf-runtime", edges);
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
            assert!(
                packages.len() <= 67,
                "cdf-runtime normal graph contains {} unique packages, above the 67-package ceiling:\n{tree}",
                packages.len()
            );
        }
    }
}

#[test]
fn first_party_codec_graphs_are_parser_local_and_mutually_isolated() {
    let codecs = [
        "cdf-format-arrow-ipc",
        "cdf-format-delimited",
        "cdf-format-json",
        "cdf-format-parquet",
    ];
    let forbidden_layers = [
        "cdf-cli",
        "cdf-project",
        "cdf-declarative",
        "cdf-source-files",
        "cdf-source-rest",
        "cdf-source-postgres",
        "cdf-dest-duckdb",
        "cdf-dest-parquet",
        "cdf-dest-postgres",
    ];
    for codec in codecs {
        let tree = cargo_tree(codec, "normal");
        let packages = package_names(&tree);
        assert!(
            packages.contains(codec),
            "missing codec root {codec}:\n{tree}"
        );
        assert!(
            !packages.contains("cdf-formats"),
            "codec {codec} reaches deleted aggregation crate:\n{tree}"
        );
        for sibling in codecs.into_iter().filter(|candidate| *candidate != codec) {
            assert!(
                !packages.contains(sibling),
                "codec {codec} reaches sibling codec {sibling}:\n{tree}"
            );
        }
        for forbidden in forbidden_layers {
            assert!(
                !packages.contains(forbidden),
                "codec {codec} reaches upper-layer package {forbidden}:\n{tree}"
            );
        }
    }
}

#[test]
fn generic_source_compiler_graphs_exclude_concrete_drivers() {
    let concrete_drivers = [
        "cdf-source-files",
        "cdf-source-rest",
        "cdf-source-postgres",
        "cdf-python",
    ];
    for root in ["cdf-runtime", "cdf-declarative", "cdf-project"] {
        let tree = cargo_tree(root, "normal");
        let packages = package_names(&tree);
        for driver in concrete_drivers {
            assert!(
                !packages.contains(driver),
                "generic source compiler package {root} reaches concrete driver {driver}:\n{tree}"
            );
        }
    }
}

#[test]
fn first_party_source_driver_graphs_are_sibling_isolated() {
    let drivers = [
        "cdf-source-files",
        "cdf-source-rest",
        "cdf-source-postgres",
        "cdf-python",
    ];
    let forbidden_upper_layers = [
        "cdf-cli",
        "cdf-conformance",
        "cdf-declarative",
        "cdf-engine",
        "cdf-project",
        "cdf-package",
        "cdf-dest-duckdb",
        "cdf-dest-parquet",
        "cdf-dest-postgres",
    ];
    for driver in drivers {
        let tree = cargo_tree(driver, "normal");
        let packages = package_names(&tree);
        assert!(
            packages.contains(driver),
            "missing source-driver root {driver}:\n{tree}"
        );
        for sibling in drivers.into_iter().filter(|candidate| *candidate != driver) {
            assert!(
                !packages.contains(sibling),
                "source driver {driver} reaches sibling driver {sibling}:\n{tree}"
            );
        }
        for forbidden in forbidden_upper_layers {
            assert!(
                !packages.contains(forbidden),
                "source driver {driver} reaches upper-layer package {forbidden}:\n{tree}"
            );
        }
    }
}

#[test]
fn neutral_object_access_graph_excludes_sources_and_upper_layers() {
    let tree = cargo_tree("cdf-object-access", "normal");
    let packages = package_names(&tree);
    for forbidden in [
        "cdf-source-files",
        "cdf-source-rest",
        "cdf-source-postgres",
        "cdf-python",
        "cdf-cli",
        "cdf-conformance",
        "cdf-declarative",
        "cdf-engine",
        "cdf-project",
        "cdf-package",
        "cdf-dest-duckdb",
        "cdf-dest-parquet",
        "cdf-dest-postgres",
    ] {
        assert!(
            !packages.contains(forbidden),
            "neutral object access reaches forbidden package {forbidden}:\n{tree}"
        );
    }
}

#[test]
fn generic_compiler_and_runtime_graphs_exclude_object_access_implementation() {
    for root in ["cdf-runtime", "cdf-declarative", "cdf-project"] {
        let tree = cargo_tree(root, "normal");
        let packages = package_names(&tree);
        assert!(
            !packages.contains("cdf-object-access"),
            "generic package {root} reaches concrete object access implementation:\n{tree}"
        );
    }
}
