use std::{fs, path::Path};

use cdf_kernel::{CursorValue, ResourceId, Result, SourcePosition, TrustLevel};
use cdf_project::ProjectRunReport;
use cdf_python::PythonResource;

use super::MatrixDisposition;

const RESOURCE_ID: &str = "python.events";

pub(crate) fn resource(
    project_root: &Path,
    disposition: MatrixDisposition,
) -> Result<PythonResource> {
    let source = project_root.join("python_events.py");
    fs::write(
        &source,
        format!(
            r#"
def events():
    yield {{"id": 1, "name": "ada", "updated_at": 10}}
    yield {{"id": 2, "name": "grace", "updated_at": 20}}

events.__cdf_resource__ = True
events.__cdf_primary_key__ = ("id",)
events.__cdf_merge_key__ = ("id",)
events.__cdf_cursor__ = "updated_at"
events.__cdf_parallel__ = False
events.__cdf_schema__ = (("id", "int64", False), ("name", "utf8", False), ("updated_at", "int64", False))
events.__cdf_write_disposition__ = "{}"
"#,
            disposition.as_str()
        ),
    )
    .map_err(|error| cdf_kernel::CdfError::data(format!("write Python fixture: {error}")))?;
    PythonResource::load(
        project_root,
        "python://python_events.py#events",
        ResourceId::new(RESOURCE_ID)?,
        TrustLevel::Governed,
    )?
    .with_execution_services(crate::test_execution_services())
}

pub(crate) fn assert_source_position(report: &ProjectRunReport) {
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("run matrix Python source must checkpoint a cursor position");
    };
    assert_eq!(cursor.version, 1);
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}
