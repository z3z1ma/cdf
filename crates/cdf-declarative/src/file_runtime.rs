use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Component, Path, PathBuf},
    time::UNIX_EPOCH,
};

use arrow_schema::SchemaRef;
use cdf_formats::{
    CsvOptions, FileFormat, FileSource, JsonOptions, ReadOptions, read_arrow_ipc_file,
    read_file_source, read_file_source_with_declared_schema,
};
use cdf_kernel::{
    BatchStream, BoxFuture, CdfError, PartitionId, PartitionPlan, ResourceDescriptor, ResourceId,
    Result, ScopeKey,
};
use futures_util::stream;
use sha2::{Digest, Sha256};

use crate::{FileFormatDeclaration, FileResourcePlan};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedFileMatch {
    path: PathBuf,
    path_text: String,
    size_bytes: u64,
    sha256: String,
    modified_ms: Option<String>,
}

pub(crate) fn file_partitions_for_plan(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
) -> Result<Vec<PartitionPlan>> {
    let matches = resolve_file_matches(&descriptor.resource_id, plan)?;
    if matches.is_empty() {
        return Err(no_file_matches_error(&descriptor.resource_id, plan));
    }

    let total_matches = matches.len();
    matches
        .iter()
        .map(|file| partition_for_file_match(descriptor, plan, file, total_matches))
        .collect()
}

pub(crate) fn open_file_resource(
    descriptor: &ResourceDescriptor,
    declared_schema: SchemaRef,
    plan: &FileResourcePlan,
    partition: PartitionPlan,
) -> BoxFuture<'static, Result<BatchStream>> {
    open_file_resource_inner(descriptor, declared_schema, plan, partition, false)
}

pub(crate) fn open_file_resource_preview(
    descriptor: &ResourceDescriptor,
    declared_schema: SchemaRef,
    plan: &FileResourcePlan,
    partition: PartitionPlan,
) -> BoxFuture<'static, Result<BatchStream>> {
    open_file_resource_inner(descriptor, declared_schema, plan, partition, true)
}

fn open_file_resource_inner(
    descriptor: &ResourceDescriptor,
    declared_schema: SchemaRef,
    plan: &FileResourcePlan,
    partition: PartitionPlan,
    preview_arrow_ipc: bool,
) -> BoxFuture<'static, Result<BatchStream>> {
    let descriptor = descriptor.clone();
    let declared_schema = declared_schema.clone();
    let plan = plan.clone();
    Box::pin(async move {
        let path = validate_partition(&descriptor, &plan, &partition)?;
        let options = ReadOptions::new(descriptor.resource_id.clone(), partition.partition_id);
        let read = read_file_path(
            &path,
            &plan.format,
            options,
            declared_schema,
            preview_arrow_ipc,
        )?;
        Ok(Box::pin(stream::iter(read.batches.into_iter().map(Ok))) as BatchStream)
    })
}

fn uses_declared_file_schema(format: &FileFormat, declared_schema: &SchemaRef) -> bool {
    !declared_schema.fields().is_empty()
        && matches!(
            format,
            FileFormat::Json(_) | FileFormat::Ndjson(_) | FileFormat::Parquet
        )
}

fn read_file_path(
    path: &Path,
    declaration: &FileFormatDeclaration,
    options: ReadOptions,
    declared_schema: SchemaRef,
    preview_arrow_ipc: bool,
) -> Result<cdf_formats::FormatRead> {
    let read = match declaration {
        FileFormatDeclaration::ArrowIpc if preview_arrow_ipc => {
            let file = File::open(path).map_err(|error| {
                CdfError::data(format!("read Arrow IPC file {}: {error}", path.display()))
            })?;
            read_arrow_ipc_file(file, &options)
        }
        _ => {
            let format = compile_format(declaration)?;
            read_non_ipc_file_path(path, format, options, declared_schema)
        }
    }?;
    Ok(read)
}

fn read_non_ipc_file_path(
    path: &Path,
    format: FileFormat,
    options: ReadOptions,
    declared_schema: SchemaRef,
) -> Result<cdf_formats::FormatRead> {
    let source = FileSource::new(path, format, options);
    if uses_declared_file_schema(&source.format, &declared_schema) {
        read_file_source_with_declared_schema(&source, declared_schema)
    } else {
        read_file_source(&source)
    }
}

fn compile_format(format: &FileFormatDeclaration) -> Result<FileFormat> {
    match format {
        FileFormatDeclaration::Csv => Ok(FileFormat::Csv(CsvOptions::default())),
        FileFormatDeclaration::Json => Ok(FileFormat::Json(JsonOptions::default())),
        FileFormatDeclaration::Ndjson => Ok(FileFormat::Ndjson(JsonOptions::default())),
        FileFormatDeclaration::Parquet => Ok(FileFormat::Parquet),
        FileFormatDeclaration::ArrowIpc => Err(CdfError::internal(
            "declarative file format `arrow_ipc` is not supported by FileResource",
        )),
    }
}

fn validate_partition(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    partition: &PartitionPlan,
) -> Result<PathBuf> {
    if partition.metadata.get("kind").map(String::as_str) != Some("files") {
        return Err(CdfError::contract(format!(
            "declarative file resource `{}` expected a file partition plan",
            descriptor.resource_id
        )));
    }
    if partition.metadata.get("resource_id").map(String::as_str)
        != Some(descriptor.resource_id.as_str())
    {
        return Err(CdfError::contract(format!(
            "declarative file partition resource id does not match `{}`",
            descriptor.resource_id
        )));
    }
    if partition.metadata.get("glob").map(String::as_str) != Some(plan.glob.as_str()) {
        return Err(CdfError::contract(format!(
            "declarative file partition glob does not match `{}`",
            plan.glob
        )));
    }
    let path = partition.metadata.get("path").ok_or_else(|| {
        CdfError::contract(format!(
            "declarative file resource `{}` expected file partition path metadata",
            descriptor.resource_id
        ))
    })?;
    let expected_scope = ScopeKey::File { path: path.clone() };
    if partition.scope != expected_scope {
        return Err(CdfError::contract(format!(
            "declarative file partition scope does not match file path `{path}`",
        )));
    }
    let matches = resolve_file_matches(&descriptor.resource_id, plan)?;
    let Some(resolved) = matches.iter().find(|file| file.path_text == *path) else {
        return Err(CdfError::contract(format!(
            "declarative file partition path `{path}` was not produced by glob `{}` under `{}`",
            plan.glob, plan.root
        )));
    };
    let expected_partition_id = if matches.len() == 1 {
        "files".to_owned()
    } else {
        file_partition_id(&resolved.path_text)
    };
    if partition.partition_id.as_str() != expected_partition_id.as_str() {
        return Err(CdfError::contract(format!(
            "declarative file partition id `{}` does not match file path `{path}`",
            partition.partition_id
        )));
    }
    let expected_size = resolved.size_bytes.to_string();
    if partition.metadata.get("bytes").map(String::as_str) != Some(expected_size.as_str()) {
        return Err(CdfError::data(format!(
            "declarative file partition `{path}` changed size after planning"
        )));
    }
    if partition.metadata.get("sha256").map(String::as_str) != Some(resolved.sha256.as_str()) {
        return Err(CdfError::data(format!(
            "declarative file partition `{path}` changed checksum after planning"
        )));
    }
    Ok(resolved.path.clone())
}

fn partition_for_file_match(
    descriptor: &ResourceDescriptor,
    plan: &FileResourcePlan,
    file: &ResolvedFileMatch,
    total_matches: usize,
) -> Result<PartitionPlan> {
    let mut metadata = BTreeMap::new();
    metadata.insert("kind".to_owned(), "files".to_owned());
    metadata.insert("glob".to_owned(), plan.glob.clone());
    metadata.insert("resource_id".to_owned(), descriptor.resource_id.to_string());
    metadata.insert("path".to_owned(), file.path_text.clone());
    metadata.insert("bytes".to_owned(), file.size_bytes.to_string());
    metadata.insert("sha256".to_owned(), file.sha256.clone());
    if let Some(modified_ms) = &file.modified_ms {
        metadata.insert("modified_ms".to_owned(), modified_ms.clone());
    }

    let partition_id = if total_matches == 1 {
        "files".to_owned()
    } else {
        file_partition_id(&file.path_text)
    };

    Ok(PartitionPlan {
        partition_id: PartitionId::new(partition_id)?,
        scope: ScopeKey::File {
            path: file.path_text.clone(),
        },
        start_position: None,
        metadata,
    })
}

fn resolve_file_matches(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
) -> Result<Vec<ResolvedFileMatch>> {
    let root = PathBuf::from(&plan.root);
    if !root.is_absolute() {
        return Err(CdfError::contract(format!(
            "file source root `{}` for resource `{resource_id}` must be absolute before runtime open; compile with an explicit project root or declare an absolute file source root",
            plan.root
        )));
    }

    let components = pattern_components(&plan.glob)?;
    let mut matches = Vec::new();
    collect_matches(&root, &components, &mut matches)?;
    matches.sort();
    matches.dedup();

    let matches = contained_matches(&root, matches)?;
    matches
        .into_iter()
        .map(|path| resolved_file_match(&root, path))
        .collect()
}

fn no_file_matches_error(resource_id: &ResourceId, plan: &FileResourcePlan) -> CdfError {
    CdfError::data(format!(
        "declarative file resource `{resource_id}` matched no files under `{}` for glob `{}`",
        plan.root, plan.glob
    ))
}

fn pattern_components(pattern: &str) -> Result<Vec<String>> {
    let path = Path::new(pattern);
    if path.is_absolute() {
        return Err(CdfError::contract(
            "file resource glob must be relative to its file source root",
        ));
    }

    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => {
                components.push(value.to_str().ok_or_else(|| {
                    CdfError::contract(format!("file resource glob is not valid UTF-8: {pattern}"))
                })?);
            }
            Component::ParentDir => {
                return Err(CdfError::contract(
                    "file resource glob must stay under its file source root and cannot contain `..`",
                ));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(CdfError::contract(
                    "file resource glob must be relative to its file source root",
                ));
            }
        }
    }

    if components.is_empty() {
        return Err(CdfError::contract("file resource glob cannot be empty"));
    }

    Ok(components.into_iter().map(str::to_owned).collect())
}

fn collect_matches(
    current: &Path,
    components: &[String],
    matches: &mut Vec<PathBuf>,
) -> Result<()> {
    let Some((component, rest)) = components.split_first() else {
        return collect_leaf_match(current, matches);
    };

    if component == "**" {
        return collect_recursive_matches(current, components, rest, matches);
    }

    if has_wildcards(component) {
        return collect_wildcard_matches(current, component, rest, matches);
    }

    collect_literal_matches(current, component, rest, matches)
}

fn collect_leaf_match(current: &Path, matches: &mut Vec<PathBuf>) -> Result<()> {
    if current.is_file() {
        matches.push(current.to_path_buf());
    }
    Ok(())
}

fn collect_recursive_matches(
    current: &Path,
    components: &[String],
    rest: &[String],
    matches: &mut Vec<PathBuf>,
) -> Result<()> {
    collect_matches(current, rest, matches)?;
    for path in read_dir_paths(current)? {
        if is_physical_dir(&path)? {
            collect_matches(&path, components, matches)?;
        }
    }
    Ok(())
}

fn collect_wildcard_matches(
    current: &Path,
    component: &str,
    rest: &[String],
    matches: &mut Vec<PathBuf>,
) -> Result<()> {
    for path in read_dir_paths(current)? {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if glob_component_matches(component, name) && can_descend_for_rest(&path, rest)? {
            collect_matches(&path, rest, matches)?;
        }
    }
    Ok(())
}

fn collect_literal_matches(
    current: &Path,
    component: &str,
    rest: &[String],
    matches: &mut Vec<PathBuf>,
) -> Result<()> {
    let next = current.join(component);
    if can_descend_for_rest(&next, rest)? {
        collect_matches(&next, rest, matches)
    } else {
        Ok(())
    }
}

fn can_descend_for_rest(path: &Path, rest: &[String]) -> Result<bool> {
    Ok(rest.is_empty() || is_physical_dir(path)?)
}

fn read_dir_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(CdfError::data(format!(
                "read file source directory {}: {error}",
                path.display()
            )));
        }
    };

    let mut paths = entries
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|error| {
            CdfError::data(format!(
                "read file source directory {}: {error}",
                path.display()
            ))
        })?;
    paths.sort();
    Ok(paths)
}

fn is_physical_dir(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.file_type().is_dir()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(CdfError::data(format!(
            "inspect file source path {}: {error}",
            path.display()
        ))),
    }
}

fn contained_matches(root: &Path, matches: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    if matches.is_empty() {
        return Ok(matches);
    }

    let canonical_root = fs::canonicalize(root).map_err(|error| {
        CdfError::data(format!(
            "canonicalize file source root {}: {error}",
            root.display()
        ))
    })?;

    matches
        .into_iter()
        .map(|path| {
            let canonical_path = fs::canonicalize(&path).map_err(|error| {
                CdfError::data(format!(
                    "canonicalize matched file {}: {error}",
                    path.display()
                ))
            })?;
            if !canonical_path.starts_with(&canonical_root) {
                return Err(CdfError::contract(format!(
                    "matched file {} escapes declared file source root {}",
                    path.display(),
                    root.display()
                )));
            }
            Ok(canonical_path)
        })
        .collect()
}

fn resolved_file_match(root: &Path, path: PathBuf) -> Result<ResolvedFileMatch> {
    let metadata = fs::metadata(&path).map_err(|error| {
        CdfError::data(format!("stat matched file {}: {error}", path.display()))
    })?;
    let modified_ms = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis().to_string());
    let canonical_root = fs::canonicalize(root).map_err(|error| {
        CdfError::data(format!(
            "canonicalize file source root {}: {error}",
            root.display()
        ))
    })?;
    let relative_path = path.strip_prefix(&canonical_root).map_err(|error| {
        CdfError::internal(format!(
            "matched file {} did not remain relative to canonical root {}: {error}",
            path.display(),
            canonical_root.display()
        ))
    })?;
    let path_text = relative_path.to_str().map(str::to_owned).ok_or_else(|| {
        CdfError::data(format!(
            "matched file path is not valid UTF-8: {}",
            relative_path.display()
        ))
    })?;
    let path_text = path_text.replace(std::path::MAIN_SEPARATOR, "/");
    let sha256 = file_sha256(&path)?;
    Ok(ResolvedFileMatch {
        path,
        path_text,
        size_bytes: metadata.len(),
        sha256,
        modified_ms,
    })
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path).map_err(|error| {
        CdfError::data(format!("open matched file {}: {error}", path.display()))
    })?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher).map_err(|error| {
        CdfError::data(format!("hash matched file {}: {error}", path.display()))
    })?;
    Ok(hex::encode(hasher.finalize()))
}

fn file_partition_id(path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("file-{}", &digest[..16])
}

fn has_wildcards(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?')
}

fn glob_component_matches(pattern: &str, candidate: &str) -> bool {
    let pattern = pattern.as_bytes();
    let candidate = candidate.as_bytes();
    let mut table = vec![vec![false; candidate.len() + 1]; pattern.len() + 1];
    table[0][0] = true;

    for i in 1..=pattern.len() {
        if pattern[i - 1] == b'*' {
            table[i][0] = table[i - 1][0];
        }
    }

    for i in 1..=pattern.len() {
        for j in 1..=candidate.len() {
            table[i][j] = match pattern[i - 1] {
                b'*' => table[i - 1][j] || table[i][j - 1],
                b'?' => table[i - 1][j - 1],
                byte => byte == candidate[j - 1] && table[i - 1][j - 1],
            };
        }
    }

    table[pattern.len()][candidate.len()]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn declared_parquet_schema_routes_through_declared_file_reader() {
        let declared_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let empty_schema = Arc::new(Schema::empty());

        assert!(uses_declared_file_schema(
            &FileFormat::Parquet,
            &declared_schema
        ));
        assert!(uses_declared_file_schema(
            &FileFormat::Json(JsonOptions::default()),
            &declared_schema
        ));
        assert!(uses_declared_file_schema(
            &FileFormat::Ndjson(JsonOptions::default()),
            &declared_schema
        ));
        assert!(!uses_declared_file_schema(
            &FileFormat::Csv(CsvOptions::default()),
            &declared_schema
        ));
        assert!(!uses_declared_file_schema(
            &FileFormat::Parquet,
            &empty_schema
        ));
    }
}
