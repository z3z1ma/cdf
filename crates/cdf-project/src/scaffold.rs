use crate::*;
use std::{fs, io::ErrorKind, path::Path};

const RESOURCES_DIR: &str = "resources";
const RESOURCE_FILE: &str = "resources/files.toml";
const DATA_DIR: &str = "data";

const RESOURCE_SCAFFOLD: &str = r#"[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "*.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "int64", nullable = false },
] }
"#;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectScaffoldOptions {
    pub root: PathBuf,
    pub project_name: Option<String>,
    pub force: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectScaffoldReport {
    pub root: String,
    pub project_name: String,
    pub created: Vec<String>,
    pub skipped: Vec<String>,
    pub replaced: Vec<String>,
    pub force: bool,
}

pub fn write_local_project_scaffold(
    options: ProjectScaffoldOptions,
) -> Result<ProjectScaffoldReport> {
    let project_name = match options.project_name {
        Some(name) if name.trim().is_empty() => {
            return Err(CdfError::contract("project scaffold name cannot be empty"));
        }
        Some(name) => name,
        None => default_project_name(&options.root),
    };
    ensure_no_unforced_overwrites(&options.root, options.force)?;

    create_root_directory(&options.root)?;
    let mut report = ProjectScaffoldReport {
        root: options.root.display().to_string(),
        project_name: project_name.clone(),
        created: Vec::new(),
        skipped: Vec::new(),
        replaced: Vec::new(),
        force: options.force,
    };
    write_scaffold_file(
        &options.root,
        PROJECT_FILE_NAME,
        &project_scaffold(&project_name)?,
        options.force,
        &mut report,
    )?;
    ensure_scaffold_directory(&options.root, RESOURCES_DIR, options.force, &mut report)?;
    write_scaffold_file(
        &options.root,
        RESOURCE_FILE,
        RESOURCE_SCAFFOLD,
        options.force,
        &mut report,
    )?;
    ensure_scaffold_directory(&options.root, DATA_DIR, options.force, &mut report)?;
    Ok(report)
}

fn default_project_name(root: &Path) -> String {
    root.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty() && *name != "." && *name != "..")
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "cdf_project".to_owned())
}

fn project_scaffold(name: &str) -> Result<String> {
    let name =
        serde_json::to_string(name).map_err(|error| CdfError::internal(error.to_string()))?;
    Ok(format!(
        r#"[project]
name = {name}
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."local.*"]
source = "resources/files.toml"
"#
    ))
}

fn ensure_no_unforced_overwrites(root: &Path, force: bool) -> Result<()> {
    if force {
        return Ok(());
    }

    let mut conflicts = Vec::new();
    for relative in [PROJECT_FILE_NAME, RESOURCE_FILE, DATA_DIR] {
        if symlink_metadata(root.join(relative))?.is_some() {
            conflicts.push(relative);
        }
    }
    if let Some(metadata) = symlink_metadata(root.join(RESOURCES_DIR))?
        && !metadata.is_dir()
    {
        conflicts.push(RESOURCES_DIR);
    }
    if conflicts.is_empty() {
        Ok(())
    } else {
        Err(CdfError::contract(format!(
            "init would overwrite existing scaffold path(s): {}; rerun with --force to replace scaffold-owned paths",
            conflicts.join(", ")
        )))
    }
}

fn create_root_directory(root: &Path) -> Result<()> {
    match symlink_metadata(root)? {
        Some(metadata) if metadata.is_dir() => Ok(()),
        Some(_) => Err(CdfError::contract(format!(
            "init target {} exists and is not a directory",
            root.display()
        ))),
        None => fs::create_dir_all(root).map_err(|error| fs_error("create", root, error)),
    }
}

fn ensure_scaffold_directory(
    root: &Path,
    relative: &str,
    force: bool,
    report: &mut ProjectScaffoldReport,
) -> Result<()> {
    let path = root.join(relative);
    match symlink_metadata(&path)? {
        Some(metadata) if metadata.is_dir() => {
            report.skipped.push(relative.to_owned());
            Ok(())
        }
        Some(_) if force => {
            fs::remove_file(&path).map_err(|error| fs_error("remove", &path, error))?;
            fs::create_dir(&path).map_err(|error| fs_error("create", &path, error))?;
            report.replaced.push(relative.to_owned());
            Ok(())
        }
        Some(_) => Err(CdfError::contract(format!(
            "init target {} exists and is not a directory; rerun with --force to replace it",
            path.display()
        ))),
        None => {
            fs::create_dir(&path).map_err(|error| fs_error("create", &path, error))?;
            report.created.push(relative.to_owned());
            Ok(())
        }
    }
}

fn write_scaffold_file(
    root: &Path,
    relative: &str,
    contents: &str,
    force: bool,
    report: &mut ProjectScaffoldReport,
) -> Result<()> {
    let path = root.join(relative);
    let Some(metadata) = symlink_metadata(&path)? else {
        fs::write(&path, contents).map_err(|error| fs_error("write", &path, error))?;
        report.created.push(relative.to_owned());
        return Ok(());
    };

    if metadata.is_dir() {
        return Err(CdfError::contract(format!(
            "init target {} exists and is a directory; remove it before writing {}",
            path.display(),
            relative
        )));
    }
    if !force {
        return Err(CdfError::contract(format!(
            "init would overwrite {relative}; rerun with --force to replace it"
        )));
    }

    fs::remove_file(&path).map_err(|error| fs_error("remove", &path, error))?;
    fs::write(&path, contents).map_err(|error| fs_error("write", &path, error))?;
    report.replaced.push(relative.to_owned());
    Ok(())
}

fn symlink_metadata(path: impl AsRef<Path>) -> Result<Option<fs::Metadata>> {
    let path = path.as_ref();
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(fs_error("inspect", path, error)),
    }
}

fn fs_error(action: &str, path: &Path, error: std::io::Error) -> CdfError {
    CdfError::data(format!("{action} {}: {error}", path.display()))
}
