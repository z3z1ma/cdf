use std::{fs, path::Path};

use arrow_schema::{DataType, Field, Schema};
use cdf_declarative::CompiledResource;
use cdf_kernel::{Result, SourcePosition};
use cdf_project::ProjectRunReport;

use super::MatrixDisposition;

pub(crate) const SOURCE_PATH: &str = "data/events.ndjson";
pub(crate) const SOURCE_POSITION_PATH: &str = "events.ndjson";
pub(crate) const SOURCE_CONTENTS: &str =
    "{\"id\":1,\"name\":\"ada\"}\n{\"id\":2,\"name\":\"grace\"}\n";
pub(crate) const SOURCE_SHA256: &str =
    "sha256:b8ecb46f86694505cef18e88722db9f4bc3a7c07cfb62230bf7ad123e61c9cb6";
pub(crate) const SOURCE_SIZE_BYTES: u64 = 46;

pub(crate) fn resource(
    project_root: &Path,
    disposition: MatrixDisposition,
) -> Result<CompiledResource> {
    let data_dir = project_root.join("data");
    fs::create_dir_all(&data_dir).map_err(|error| {
        crate::conformance_private_io_error("create run matrix data dir", error)
    })?;
    fs::write(project_root.join(SOURCE_PATH), SOURCE_CONTENTS).map_err(|error| {
        crate::conformance_private_io_error("write run matrix source file", error)
    })?;

    compile_resource(project_root, disposition, "events.ndjson")
}

pub(crate) fn multi_resource(
    project_root: &Path,
    disposition: MatrixDisposition,
) -> Result<CompiledResource> {
    let data_dir = project_root.join("data");
    fs::create_dir_all(&data_dir).map_err(|error| {
        crate::conformance_private_io_error("create run matrix data dir", error)
    })?;
    fs::write(
        data_dir.join("part-01.ndjson"),
        "{\"id\":1,\"name\":\"ada\"}\n",
    )
    .map_err(|error| {
        crate::conformance_private_io_error("write first run matrix source file", error)
    })?;
    fs::write(
        data_dir.join("part-02.ndjson"),
        "{\"id\":2,\"name\":\"grace\"}\n",
    )
    .map_err(|error| {
        crate::conformance_private_io_error("write second run matrix source file", error)
    })?;
    compile_resource(project_root, disposition, "part-*.ndjson")
}

fn compile_resource(
    project_root: &Path,
    disposition: MatrixDisposition,
    glob: &str,
) -> Result<CompiledResource> {
    crate::source_fixture::compile_local_file_project_resource(
        project_root,
        "run_matrix_file_conformance",
        glob,
        disposition.to_write_disposition(),
        &Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]),
    )
}

pub(crate) fn assert_source_position(report: &ProjectRunReport) {
    let SourcePosition::FileManifest(manifest) = &report.checkpoint.delta.output_position else {
        panic!("run matrix file source must checkpoint a FileManifest");
    };
    assert_eq!(manifest.version, cdf_kernel::SOURCE_POSITION_VERSION);
    assert_eq!(manifest.files.len(), 1);
    let file = &manifest.files[0];
    assert!(
        file.path.ends_with(SOURCE_POSITION_PATH),
        "file checkpoint path `{}` does not end with `{SOURCE_POSITION_PATH}`",
        file.path
    );
    assert_eq!(file.size_bytes, SOURCE_SIZE_BYTES);
    assert_eq!(file.sha256.as_deref(), Some(SOURCE_SHA256));
}
