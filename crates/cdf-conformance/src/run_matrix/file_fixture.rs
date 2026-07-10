use std::{fs, path::Path};

use cdf_declarative::CompiledResource;
use cdf_kernel::{CdfError, Result, SourcePosition};
use cdf_project::{
    InMemoryResourceSourceResolver, ProjectRunReport,
    compile_project_declarative_resources_with_root, parse_cdf_toml,
};

use super::MatrixDisposition;

const CDF_PROJECT_TOML: &str = r#"
[project]
name = "run_matrix_file_conformance"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.sqlite"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."local.events"]
source = "resources/live.toml"
"#;

pub(crate) const RESOURCE_ID: &str = "local.events";
pub(crate) const SOURCE_PATH: &str = "data/events.ndjson";
pub(crate) const SOURCE_POSITION_PATH: &str = "events.ndjson";
pub(crate) const SOURCE_CONTENTS: &str =
    "{\"id\":1,\"name\":\"ada\"}\n{\"id\":2,\"name\":\"grace\"}\n";
pub(crate) const SOURCE_SHA256: &str =
    "b8ecb46f86694505cef18e88722db9f4bc3a7c07cfb62230bf7ad123e61c9cb6";
pub(crate) const SOURCE_SIZE_BYTES: u64 = 46;

pub(crate) fn resource(
    project_root: &Path,
    disposition: MatrixDisposition,
) -> Result<CompiledResource> {
    let data_dir = project_root.join("data");
    fs::create_dir_all(&data_dir)
        .map_err(|error| CdfError::data(format!("create run matrix data dir: {error}")))?;
    fs::write(project_root.join(SOURCE_PATH), SOURCE_CONTENTS)
        .map_err(|error| CdfError::data(format!("write run matrix source file: {error}")))?;

    compile_resource(project_root, disposition, "events.ndjson")
}

pub(crate) fn multi_resource(
    project_root: &Path,
    disposition: MatrixDisposition,
) -> Result<CompiledResource> {
    let data_dir = project_root.join("data");
    fs::create_dir_all(&data_dir)
        .map_err(|error| CdfError::data(format!("create run matrix data dir: {error}")))?;
    fs::write(
        data_dir.join("part-01.ndjson"),
        "{\"id\":1,\"name\":\"ada\"}\n",
    )
    .map_err(|error| CdfError::data(format!("write first run matrix source file: {error}")))?;
    fs::write(
        data_dir.join("part-02.ndjson"),
        "{\"id\":2,\"name\":\"grace\"}\n",
    )
    .map_err(|error| CdfError::data(format!("write second run matrix source file: {error}")))?;
    compile_resource(project_root, disposition, "part-*.ndjson")
}

fn compile_resource(
    project_root: &Path,
    disposition: MatrixDisposition,
    glob: &str,
) -> Result<CompiledResource> {
    let config = parse_cdf_toml(CDF_PROJECT_TOML)?;
    let resource_toml = resource_toml(disposition, glob);
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/live.toml", resource_toml);
    let mut resources =
        compile_project_declarative_resources_with_root(&config, &resolver, project_root)?;
    if resources.len() != 1 {
        return Err(CdfError::contract(format!(
            "run matrix expected one file resource, found {}",
            resources.len()
        )));
    }
    let resource = resources.remove(0);
    if resource.descriptor().resource_id.as_str() != RESOURCE_ID {
        return Err(CdfError::contract(format!(
            "run matrix compiled unexpected resource {}",
            resource.descriptor().resource_id
        )));
    }
    Ok(resource)
}

pub(crate) fn assert_source_position(report: &ProjectRunReport) {
    let SourcePosition::FileManifest(manifest) = &report.checkpoint.delta.output_position else {
        panic!("run matrix file source must checkpoint a FileManifest");
    };
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.files.len(), 1);
    let file = &manifest.files[0];
    assert!(file.path.ends_with(SOURCE_POSITION_PATH));
    assert_eq!(file.size_bytes, SOURCE_SIZE_BYTES);
    assert_eq!(file.sha256.as_deref(), Some(SOURCE_SHA256));
}

fn resource_toml(disposition: MatrixDisposition, glob: &str) -> String {
    format!(
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
id = "{RESOURCE_ID}"
glob = "{glob}"
format = "ndjson"
primary_key = ["id"]
merge_key = ["id"]
write_disposition = "{}"
trust = "governed"
partition = {{ by = "file" }}
schema = {{ fields = [
  {{ name = "id", type = "int64", nullable = false }},
  {{ name = "name", type = "string", nullable = true }},
] }}
"#,
        disposition.as_str()
    )
}
