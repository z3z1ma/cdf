use std::{
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    time::UNIX_EPOCH,
};

use cdf_kernel::{CdfError, ResourceId, Result};
use cdf_object_access::{
    FileIdentityMetadata, FileTransport, FileTransportControl, FileTransportLocation,
    FileTransportResource, file_url_path, local_source_generation,
};
use cdf_runtime::{
    ByteTransformId, ByteTransformRegistry, ExecutionServices, FormatDetection,
    FormatDetectionConfidence, FormatDriver, FormatRegistry, GenerationStrength, SourceEgressScope,
    SourceEvidenceLocation,
};
use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};

use crate::{
    FileCompressionDeclaration, FileResourcePlan,
    driver::{FileTransportScheme, file_transport_scheme},
};

use super::model::{
    CompressionEvidence, CompressionSignal, FormatEvidence, ResolvedFileMatch, ResolvedFileOpen,
};

#[derive(Clone)]
pub(super) struct FilePlanningContext<'a> {
    pub(super) transport: &'a dyn FileTransport,
    pub(super) egress: &'a SourceEgressScope,
    pub(super) formats: &'a FormatRegistry,
    pub(super) transforms: &'a ByteTransformRegistry,
    pub(super) maximum_matches: usize,
    pub(super) control: &'a FileTransportControl,
    pub(super) execution: ExecutionServices,
}

#[derive(Clone, Copy)]
pub(super) struct FileResolutionContext<'a> {
    pub(super) transport: &'a dyn FileTransport,
    pub(super) egress: &'a SourceEgressScope,
    pub(super) formats: &'a FormatRegistry,
    pub(super) transforms: &'a ByteTransformRegistry,
    pub(super) control: &'a FileTransportControl,
}

pub(super) trait FileMatchSink: Send {
    fn admit(&mut self, file: ResolvedFileMatch) -> Result<()>;
    fn admitted_count(&self) -> u64;
}

#[cfg(test)]
impl FileMatchSink for Vec<ResolvedFileMatch> {
    fn admit(&mut self, file: ResolvedFileMatch) -> Result<()> {
        self.push(file);
        Ok(())
    }

    fn admitted_count(&self) -> u64 {
        u64::try_from(self.len()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
pub(super) fn resolve_file_matches(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
    egress: &SourceEgressScope,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<Vec<ResolvedFileMatch>> {
    resolve_file_matches_bounded(
        resource_id,
        plan,
        FilePlanningContext {
            transport,
            egress,
            formats,
            transforms,
            maximum_matches: usize::MAX,
            control: &FileTransportControl::default(),
            execution: crate::test_execution_services(),
        },
        Vec::new(),
    )
}

pub(super) fn resolve_file_matches_bounded<S>(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    context: FilePlanningContext<'_>,
    mut sink: S,
) -> Result<S>
where
    S: FileMatchSink + 'static,
{
    match file_transport_scheme(&plan.root)? {
        Some(FileTransportScheme::Http | FileTransportScheme::Https) => {
            if context.maximum_matches == 0 {
                return Err(CdfError::data(
                    "file inventory exceeds the 0-entry boundary",
                ));
            }
            return resolve_http_file_matches_into(resource_id, plan, context, sink);
        }
        Some(FileTransportScheme::Remote(_)) => {
            return resolve_remote_matches_bounded(resource_id, plan, context, sink);
        }
        Some(FileTransportScheme::File) => {
            let mut local_plan = plan.clone();
            local_plan.root = file_url_path(&plan.root)?
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| CdfError::data("file URL path is not valid UTF-8"))?;
            return resolve_file_matches_bounded(resource_id, &local_plan, context, sink);
        }
        None => {}
    }

    let root = PathBuf::from(&plan.root);
    if !root.is_absolute() {
        return Err(CdfError::contract(format!(
            "file source root `{}` for resource `{resource_id}` must be absolute before runtime open; compile with an explicit project root or declare an absolute file source root",
            plan.root
        )));
    }

    let components = pattern_components(&plan.glob)?;
    let mut budget = LocalInventoryBudget::new(
        context.maximum_matches,
        context.control.clone(),
        context.execution.clone(),
    );
    let canonical_root = match fs::canonicalize(&root) {
        Ok(root) => root,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(sink);
        }
        Err(error) => {
            return Err(CdfError::data(format!(
                "canonicalize file source root {}: {error}",
                root.display()
            )));
        }
    };
    let local = LocalMatchContext {
        canonical_root: &canonical_root,
        resource_id,
        plan,
        formats: context.formats,
        transforms: context.transforms,
    };
    collect_matches(&root, &components, &local, &mut sink, &mut budget)?;
    Ok(sink)
}

#[cfg(test)]
pub(super) fn resolve_remote_matches(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    transport: &dyn FileTransport,
    egress: &SourceEgressScope,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<Vec<ResolvedFileMatch>> {
    resolve_remote_matches_bounded(
        resource_id,
        plan,
        FilePlanningContext {
            transport,
            egress,
            formats,
            transforms,
            maximum_matches: usize::MAX,
            control: &FileTransportControl::default(),
            execution: crate::test_execution_services(),
        },
        Vec::new(),
    )
}

pub(super) fn resolve_remote_matches_bounded<S>(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    context: FilePlanningContext<'_>,
    sink: S,
) -> Result<S>
where
    S: FileMatchSink + 'static,
{
    let root_resource = FileTransportResource::remote_url(plan.root.clone())
        .with_egress_allowlist(plan.allowlist.clone());
    let root_resource = match &plan.credentials {
        Some(credentials) => root_resource.with_credentials(credentials.clone()),
        None => root_resource,
    };
    let components = pattern_components(&plan.glob)?;
    let listing = context.transport.list(
        context.egress,
        &root_resource,
        context.maximum_matches,
        context.control,
    )?;
    let termination = listing.termination();
    let no_matches = no_file_matches_error(resource_id, plan);
    let root = plan.root.clone();
    let components_for_listing = components.clone();
    let plan = plan.clone();
    let resource_id = resource_id.clone();
    let formats = context.formats.clone();
    let transforms = context.transforms.clone();
    let maximum_matches = context.maximum_matches;
    let sink = context.execution.run_io(async move {
        let mut listing = listing;
        let mut sink = sink;
        let result: Result<()> = async {
            while let Some(identity) = listing.try_next().await? {
                let metadata = identity.into_identity();
                let relative = remote_relative_path(&root, &metadata.location)?;
                if glob_path_matches(&components_for_listing, &relative) {
                    if sink.admitted_count() >= u64::try_from(maximum_matches).unwrap_or(u64::MAX) {
                        return Err(CdfError::data(format!(
                            "file inventory exceeds the {}-entry boundary",
                            maximum_matches
                        )));
                    }
                    let resource = FileTransportResource::remote_url(metadata.location.clone())
                        .with_egress_allowlist(plan.allowlist.clone());
                    let resource = match &plan.credentials {
                        Some(credentials) => resource.with_credentials(credentials.clone()),
                        None => resource,
                    };
                    let compression =
                        resolve_transport_compression(&plan, &metadata.location, &transforms)?;
                    let format = resolve_transport_format(
                        &resource_id,
                        &plan,
                        &metadata.location,
                        &compression,
                        &formats,
                    )?;
                    sink.admit(resolved_transport_file_match(
                        resource,
                        metadata,
                        compression,
                        format,
                    )?)?;
                }
            }
            termination.join().await?;
            Ok(())
        }
        .await;
        match result {
            Ok(()) => Ok(sink),
            Err(mut error) => {
                if let Err(cleanup) = termination.terminate_and_join().await {
                    error.message = format!(
                        "{}; file listing termination also failed: {}",
                        error.message, cleanup.message
                    );
                }
                Err(error)
            }
        }
    })?;
    if sink.admitted_count() == 0 {
        return Err(no_matches);
    }
    Ok(sink)
}

pub(super) fn remote_relative_path(root: &str, location: &str) -> Result<String> {
    let prefix = format!("{}/", root.trim_end_matches('/'));
    location
        .strip_prefix(&prefix)
        .map(str::to_owned)
        .ok_or_else(|| CdfError::data("object-store listing escaped its configured root prefix"))
}

pub(super) fn resolve_http_file_matches_into<S>(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    context: FilePlanningContext<'_>,
    mut sink: S,
) -> Result<S>
where
    S: FileMatchSink,
{
    let mut globs = HttpGlobExpansion::new(resource_id, &plan.glob)?;
    while let Some(glob) = globs.next_glob() {
        let url = join_http_root_and_glob(&plan.root, &glob);
        let mut resource =
            FileTransportResource::http_url(url).with_egress_allowlist(plan.allowlist.clone());
        if let Some(auth) = &plan.auth {
            resource = resource.with_auth(auth.clone());
        }
        if let Some(credentials) = &plan.credentials {
            resource = resource.with_credentials(credentials.clone());
        }
        let Some(observation) =
            context
                .transport
                .metadata_if_exists(context.egress, &resource, context.control)?
        else {
            continue;
        };
        let logical_location = match &resource.location {
            FileTransportLocation::HttpUrl { url } => url.as_str(),
            _ => unreachable!("HTTP resolver constructed an HTTP transport resource"),
        };
        let compression =
            resolve_transport_compression(plan, logical_location, context.transforms)?;
        let format = resolve_transport_format(
            resource_id,
            plan,
            logical_location,
            &compression,
            context.formats,
        )?;
        let access_resource = observation.access_resource(&resource);
        if sink.admitted_count() >= u64::try_from(context.maximum_matches).unwrap_or(u64::MAX) {
            return Err(CdfError::data(format!(
                "file inventory exceeds the {}-entry boundary",
                context.maximum_matches
            )));
        }
        sink.admit(resolved_transport_file_match(
            access_resource,
            observation.into_identity(),
            compression,
            format,
        )?)?;
    }
    if sink.admitted_count() == 0 {
        Err(no_file_matches_error(resource_id, plan))
    } else {
        Ok(sink)
    }
}

pub(super) fn no_file_matches_error(resource_id: &ResourceId, plan: &FileResourcePlan) -> CdfError {
    CdfError::data(format!(
        "declarative file resource `{resource_id}` matched no files under `{}` for glob `{}`",
        plan.root, plan.glob
    ))
}

pub(super) fn pattern_components(pattern: &str) -> Result<Vec<String>> {
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

pub(super) struct LocalInventoryBudget {
    maximum_entries: usize,
    observed_entries: usize,
    control: FileTransportControl,
    execution: ExecutionServices,
}

pub(super) struct LocalMatchContext<'a> {
    canonical_root: &'a Path,
    resource_id: &'a ResourceId,
    plan: &'a FileResourcePlan,
    formats: &'a FormatRegistry,
    transforms: &'a ByteTransformRegistry,
}

impl LocalInventoryBudget {
    fn new(
        maximum_entries: usize,
        control: FileTransportControl,
        execution: ExecutionServices,
    ) -> Self {
        Self {
            maximum_entries,
            observed_entries: 0,
            control,
            execution,
        }
    }

    fn check(&self) -> Result<()> {
        self.control.check(Some(&self.execution))
    }

    fn observe_entry(&mut self) -> Result<()> {
        self.check()?;
        if self.observed_entries >= self.maximum_entries {
            return Err(CdfError::data(format!(
                "file inventory exceeds the {}-entry boundary",
                self.maximum_entries
            )));
        }
        self.observed_entries = self.observed_entries.saturating_add(1);
        Ok(())
    }

    fn admit_match(&self, admitted_count: u64) -> Result<()> {
        self.check()?;
        if admitted_count >= u64::try_from(self.maximum_entries).unwrap_or(u64::MAX) {
            return Err(CdfError::data(format!(
                "file inventory exceeds the {}-entry boundary",
                self.maximum_entries
            )));
        }
        Ok(())
    }
}

pub(super) fn collect_matches<S>(
    current: &Path,
    components: &[String],
    context: &LocalMatchContext<'_>,
    sink: &mut S,
    budget: &mut LocalInventoryBudget,
) -> Result<()>
where
    S: FileMatchSink,
{
    budget.check()?;
    let Some((component, rest)) = components.split_first() else {
        return collect_leaf_match(current, context, sink, budget);
    };

    if component == "**" {
        return collect_recursive_matches(current, components, rest, context, sink, budget);
    }

    if has_wildcards(component) {
        return collect_wildcard_matches(current, component, rest, context, sink, budget);
    }

    collect_literal_matches(current, component, rest, context, sink, budget)
}

pub(super) fn collect_leaf_match<S>(
    current: &Path,
    context: &LocalMatchContext<'_>,
    sink: &mut S,
    budget: &LocalInventoryBudget,
) -> Result<()>
where
    S: FileMatchSink,
{
    if current.is_file() {
        budget.admit_match(sink.admitted_count())?;
        let canonical_path = fs::canonicalize(current).map_err(|error| {
            CdfError::data(format!(
                "canonicalize matched file {}: {error}",
                current.display()
            ))
        })?;
        if !canonical_path.starts_with(context.canonical_root) {
            return Err(CdfError::contract(format!(
                "matched file {} escapes declared file source root {}",
                current.display(),
                context.canonical_root.display()
            )));
        }
        sink.admit(resolved_canonical_file_match(
            context.resource_id,
            context.canonical_root,
            canonical_path,
            context.plan,
            context.formats,
            context.transforms,
        )?)?;
    }
    Ok(())
}

pub(super) fn collect_recursive_matches<S>(
    current: &Path,
    components: &[String],
    rest: &[String],
    context: &LocalMatchContext<'_>,
    sink: &mut S,
    budget: &mut LocalInventoryBudget,
) -> Result<()>
where
    S: FileMatchSink,
{
    collect_matches(current, rest, context, sink, budget)?;
    let Some(entries) = read_dir_entries(current)? else {
        return Ok(());
    };
    for entry in entries {
        budget.observe_entry()?;
        let path = entry
            .map_err(|error| {
                CdfError::data(format!(
                    "read file source directory {}: {error}",
                    current.display()
                ))
            })?
            .path();
        if is_physical_dir(&path)? {
            collect_matches(&path, components, context, sink, budget)?;
        }
    }
    Ok(())
}

pub(super) fn collect_wildcard_matches<S>(
    current: &Path,
    component: &str,
    rest: &[String],
    context: &LocalMatchContext<'_>,
    sink: &mut S,
    budget: &mut LocalInventoryBudget,
) -> Result<()>
where
    S: FileMatchSink,
{
    let Some(entries) = read_dir_entries(current)? else {
        return Ok(());
    };
    for entry in entries {
        budget.observe_entry()?;
        let path = entry
            .map_err(|error| {
                CdfError::data(format!(
                    "read file source directory {}: {error}",
                    current.display()
                ))
            })?
            .path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if glob_component_matches(component, name) && can_descend_for_rest(&path, rest)? {
            collect_matches(&path, rest, context, sink, budget)?;
        }
    }
    Ok(())
}

pub(super) fn collect_literal_matches<S>(
    current: &Path,
    component: &str,
    rest: &[String],
    context: &LocalMatchContext<'_>,
    sink: &mut S,
    budget: &mut LocalInventoryBudget,
) -> Result<()>
where
    S: FileMatchSink,
{
    let next = current.join(component);
    if can_descend_for_rest(&next, rest)? {
        collect_matches(&next, rest, context, sink, budget)
    } else {
        Ok(())
    }
}

pub(super) fn can_descend_for_rest(path: &Path, rest: &[String]) -> Result<bool> {
    Ok(rest.is_empty() || is_physical_dir(path)?)
}

pub(super) fn read_dir_entries(path: &Path) -> Result<Option<fs::ReadDir>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(None);
        }
        Err(error) => {
            return Err(CdfError::data(format!(
                "read file source directory {}: {error}",
                path.display()
            )));
        }
    };

    Ok(Some(entries))
}

pub(super) fn is_physical_dir(path: &Path) -> Result<bool> {
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

pub(super) fn resolved_file_match(
    resource_id: &ResourceId,
    root: &Path,
    path: PathBuf,
    plan: &FileResourcePlan,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<ResolvedFileMatch> {
    let canonical_root = fs::canonicalize(root).map_err(|error| {
        CdfError::data(format!(
            "canonicalize file source root {}: {error}",
            root.display()
        ))
    })?;
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
    resolved_canonical_file_match(
        resource_id,
        &canonical_root,
        canonical_path,
        plan,
        formats,
        transforms,
    )
}

pub(super) fn resolved_canonical_file_match(
    resource_id: &ResourceId,
    canonical_root: &Path,
    path: PathBuf,
    plan: &FileResourcePlan,
    formats: &FormatRegistry,
    transforms: &ByteTransformRegistry,
) -> Result<ResolvedFileMatch> {
    let metadata = fs::metadata(&path).map_err(|error| {
        CdfError::data(format!("stat matched file {}: {error}", path.display()))
    })?;
    let modified_ms = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis().to_string());
    let relative_path = path.strip_prefix(canonical_root).map_err(|error| {
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
    let magic_signal = local_compression_magic_signal(&path, metadata.len(), transforms)?;
    let compression =
        resolve_local_compression(&path_text, &plan.compression, magic_signal, transforms)?;
    let (format, _) = resolve_local_format(resource_id, plan, &path_text, &compression, formats)?;
    let source_generation = local_source_generation(&path)?;
    Ok(ResolvedFileMatch {
        open: ResolvedFileOpen::LocalPath(path),
        path_text,
        size_bytes: metadata.len(),
        source_generation: Some(source_generation),
        identity_strength: GenerationStrength::Weak,
        sha256: None,
        etag: None,
        version: None,
        modified_ms,
        exact_ranges: true,
        bytes_loaded: None,
        compression,
        format,
    })
}

pub(super) fn resolved_transport_file_match(
    resource: FileTransportResource,
    metadata: FileIdentityMetadata,
    compression: CompressionEvidence,
    format: FormatEvidence,
) -> Result<ResolvedFileMatch> {
    let size_bytes = metadata.size_bytes.ok_or_else(|| {
        CdfError::data(format!(
            "HTTP(S) file metadata for `{}` did not include Content-Length",
            metadata.location
        ))
    })?;
    let sha256 = metadata.sha256().map(str::to_owned);
    let identity_strength = metadata.generation_strength();
    let source_generation = (identity_strength == GenerationStrength::Weak)
        .then(|| metadata.modified.clone())
        .flatten();
    let exact_ranges = metadata.exact_ranges;
    Ok(ResolvedFileMatch {
        open: ResolvedFileOpen::Transport(resource),
        path_text: metadata.location,
        size_bytes,
        source_generation,
        identity_strength,
        sha256,
        etag: metadata.etag,
        version: metadata.version,
        modified_ms: metadata
            .modified
            .as_deref()
            .and_then(|modified| modified.strip_prefix("unix_ms:"))
            .map(str::to_owned),
        exact_ranges,
        bytes_loaded: None,
        compression,
        format,
    })
}

pub(super) fn identity_strength_name(strength: GenerationStrength) -> &'static str {
    match strength {
        GenerationStrength::Weak => "weak",
        GenerationStrength::Strong => "strong",
        GenerationStrength::ContentAddressed => "content_addressed",
    }
}

pub(super) fn resolve_local_format(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    path_text: &str,
    compression: &CompressionEvidence,
    formats: &FormatRegistry,
) -> Result<(FormatEvidence, u64)> {
    let driver = formats.resolve(plan.resolved_format()?.as_str())?;
    let extension = format_extension(path_text, compression);
    validate_format_extension(resource_id, plan, path_text, extension.as_deref(), formats)?;
    Ok((deferred_format_evidence(driver.as_ref(), extension), 0))
}

pub(super) fn resolve_transport_format(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    location: &str,
    compression: &CompressionEvidence,
    formats: &FormatRegistry,
) -> Result<FormatEvidence> {
    let driver = formats.resolve(plan.resolved_format()?.as_str())?;
    let extension = format_extension(location, compression);
    let diagnostic = diagnostic_location(location)?;
    validate_format_extension(
        resource_id,
        plan,
        &diagnostic,
        extension.as_deref(),
        formats,
    )?;
    Ok(deferred_format_evidence(driver.as_ref(), extension))
}

pub(super) fn validate_format_extension(
    resource_id: &ResourceId,
    plan: &FileResourcePlan,
    path_text: &str,
    extension: Option<&str>,
    formats: &FormatRegistry,
) -> Result<()> {
    if plan.format_declared {
        return Ok(());
    }
    let Some(extension) = extension else {
        return Err(CdfError::data(format!(
            "file `{path_text}` for resource `{resource_id}` has no extension that can attest inferred format `{}`; declare `format` explicitly",
            plan.resolved_format()?.as_str()
        )));
    };
    let Some(extension_driver) = formats.by_extension(extension) else {
        return Err(CdfError::data(format!(
            "file `{path_text}` for resource `{resource_id}` has unregistered extension `.{extension}` for inferred format `{}`; declare `format` explicitly or register the extension",
            plan.resolved_format()?.as_str()
        )));
    };
    if extension_driver.descriptor().format_id.as_str() == plan.resolved_format()?.as_str() {
        return Ok(());
    }
    Err(CdfError::data(format!(
        "file format mismatch for resource `{resource_id}`, file `{path_text}`: extension `.{extension}` selects `{}` but the compiled resource selects `{}`; change `format` or the file extension",
        extension_driver.descriptor().format_id,
        plan.resolved_format()?.as_str(),
    )))
}

pub(super) fn deferred_format_evidence(
    driver: &dyn FormatDriver,
    extension: Option<String>,
) -> FormatEvidence {
    FormatEvidence {
        format_id: driver.descriptor().format_id.to_string(),
        driver_version: driver.descriptor().semantic_version.clone(),
        extension,
        detection: FormatDetection {
            confidence: FormatDetectionConfidence::None,
            reason: "content detection is deferred to the admitted decode stream".to_owned(),
        },
    }
}

pub(super) fn format_detection_confidence_name(
    confidence: FormatDetectionConfidence,
) -> &'static str {
    match confidence {
        FormatDetectionConfidence::None => "none",
        FormatDetectionConfidence::Weak => "weak",
        FormatDetectionConfidence::Strong => "strong",
    }
}

pub(super) fn format_extension(
    path_text: &str,
    compression: &CompressionEvidence,
) -> Option<String> {
    let path_without_fragment = path_text.split('#').next().unwrap_or(path_text);
    let mut path_without_query = path_without_fragment
        .split('?')
        .next()
        .unwrap_or(path_without_fragment)
        .to_ascii_lowercase();
    if compression.transform_id.is_some()
        && let Some((inner, _)) = path_without_query.rsplit_once('.')
    {
        path_without_query.truncate(inner.len());
    }
    path_without_query
        .rsplit_once('.')
        .map(|(_, extension)| extension.to_owned())
}

pub(super) fn diagnostic_location(location: &str) -> Result<String> {
    Ok(SourceEvidenceLocation::from_operational(location)?
        .as_str()
        .to_owned())
}

pub(super) fn resolve_local_compression(
    path_text: &str,
    declared: &FileCompressionDeclaration,
    magic_signal: CompressionSignal,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionEvidence> {
    let extension_signal = compression_extension_signal(path_text, transforms);
    resolve_compression_signals(
        path_text,
        declared,
        extension_signal,
        magic_signal,
        transforms,
    )
}

pub(super) fn resolve_transport_compression(
    plan: &FileResourcePlan,
    location: &str,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionEvidence> {
    let extension_signal = compression_extension_signal(location, transforms);
    let diagnostic = diagnostic_location(location)?;
    resolve_compression_signals(
        &diagnostic,
        &plan.compression,
        extension_signal,
        CompressionSignal::default(),
        transforms,
    )
}

pub(super) fn resolve_compression_signals(
    path_text: &str,
    declared: &FileCompressionDeclaration,
    extension_signal: CompressionSignal,
    magic_signal: CompressionSignal,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionEvidence> {
    let transform_id = if declared.is_auto() {
        match (extension_signal.transform_id(), magic_signal.transform_id()) {
            (Some(extension), Some(magic)) if extension != magic => {
                return Err(compression_signal_error(
                    path_text,
                    declared,
                    &extension_signal,
                    &magic_signal,
                ));
            }
            (_, Some(magic)) => Some(magic.clone()),
            (Some(extension), None) => Some(extension.clone()),
            (None, None) => None,
        }
    } else if declared.is_none() {
        if magic_signal.transform_id().is_some() {
            return Err(compression_signal_error(
                path_text,
                declared,
                &extension_signal,
                &magic_signal,
            ));
        }
        None
    } else {
        let declared_id = ByteTransformId::new(declared.as_str().to_owned())?;
        transforms.resolve(&declared_id)?;
        if magic_signal
            .transform_id()
            .is_some_and(|magic| magic != &declared_id)
        {
            return Err(compression_signal_error(
                path_text,
                declared,
                &extension_signal,
                &magic_signal,
            ));
        }
        Some(declared_id)
    };

    Ok(CompressionEvidence {
        transform_id,
        extension_signal,
        magic_signal,
    })
}

pub(super) fn compression_extension_signal(
    path_text: &str,
    transforms: &ByteTransformRegistry,
) -> CompressionSignal {
    let path_without_fragment = path_text.split('#').next().unwrap_or(path_text);
    let lower = path_without_fragment
        .split('?')
        .next()
        .unwrap_or(path_without_fragment)
        .to_ascii_lowercase();
    let extension = lower.rsplit_once('.').map(|(_, extension)| extension);
    CompressionSignal(extension.and_then(|extension| {
        transforms
            .by_extension(extension)
            .map(|driver| driver.descriptor().transform_id.clone())
    }))
}

pub(super) fn local_compression_magic_signal(
    path: &Path,
    size_bytes: u64,
    transforms: &ByteTransformRegistry,
) -> Result<CompressionSignal> {
    let probe_bytes = transforms.maximum_strong_magic_probe_bytes()?;
    let probe_bytes = probe_bytes.min(size_bytes);
    if probe_bytes == 0 {
        return Ok(CompressionSignal::default());
    }
    let probe_bytes = usize::try_from(probe_bytes)
        .map_err(|_| CdfError::contract("byte-transform magic probe length exceeds usize"))?;
    let mut prefix = vec![0_u8; probe_bytes];
    let mut file = fs::File::open(path).map_err(|error| {
        CdfError::data(format!("open matched file {}: {error}", path.display()))
    })?;
    file.read_exact(&mut prefix).map_err(|error| {
        CdfError::data(format!(
            "read compression magic prefix for {}: {error}",
            path.display()
        ))
    })?;
    let Some(driver) = transforms.detect_strong_magic(&prefix)? else {
        return Ok(CompressionSignal::default());
    };
    Ok(CompressionSignal(Some(
        driver.descriptor().transform_id.clone(),
    )))
}

pub(super) fn compression_signal_error(
    path_text: &str,
    declared: &FileCompressionDeclaration,
    extension_signal: &CompressionSignal,
    magic_signal: &CompressionSignal,
) -> CdfError {
    CdfError::data(format!(
        "file `{path_text}` compression mismatch: declared `{}`, extension signal `{}`, magic bytes signal `{}`",
        declared.as_str(),
        extension_signal.as_str(),
        magic_signal.as_str()
    ))
}

pub(super) fn records_compression_evidence(
    compression: &CompressionEvidence,
    declared: &FileCompressionDeclaration,
) -> bool {
    compression.transform_id.is_some()
        || !declared.is_auto()
        || compression.extension_signal.transform_id().is_some()
        || compression.magic_signal.transform_id().is_some()
}

pub(super) enum HttpGlobExpansion<'a> {
    Bounded(std::vec::IntoIter<String>),
    Numeric {
        template: HttpNumericTemplate<'a>,
        next: Option<u64>,
    },
    Single(Option<String>),
}

impl<'a> HttpGlobExpansion<'a> {
    fn new(resource_id: &ResourceId, glob: &'a str) -> Result<Self> {
        if let Some(months) = expand_http_year_month_glob(glob) {
            return Ok(Self::Bounded(months.into_iter()));
        }
        let Some(template) = parse_http_numeric_template(resource_id, glob)? else {
            return Ok(Self::Single(Some(glob.to_owned())));
        };
        let next = Some(template.start);
        Ok(Self::Numeric { template, next })
    }

    fn next_glob(&mut self) -> Option<String> {
        match self {
            Self::Bounded(values) => values.next(),
            Self::Numeric { template, next } => {
                let value = next.take()?;
                if value < template.end {
                    *next = Some(value + 1);
                }
                Some(template.render(value))
            }
            Self::Single(value) => value.take(),
        }
    }
}

#[cfg(test)]
pub(super) fn expand_http_glob(resource_id: &ResourceId, glob: &str) -> Result<Vec<String>> {
    let mut expansion = HttpGlobExpansion::new(resource_id, glob)?;
    let mut values = Vec::new();
    while let Some(value) = expansion.next_glob() {
        values.push(value);
    }
    Ok(values)
}

pub(super) fn http_glob_contains(
    resource_id: &ResourceId,
    glob: &str,
    candidate: &str,
) -> Result<bool> {
    if let Some(months) = expand_http_year_month_glob(glob) {
        return Ok(months.iter().any(|month| month == candidate));
    }
    let Some(template) = parse_http_numeric_template(resource_id, glob)? else {
        return Ok(glob == candidate);
    };
    let Some(value_text) = candidate
        .strip_prefix(template.prefix)
        .and_then(|candidate| candidate.strip_suffix(template.suffix))
    else {
        return Ok(false);
    };
    if value_text.is_empty() || !value_text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(false);
    }
    let Ok(value) = value_text.parse::<u64>() else {
        return Ok(false);
    };
    Ok(value >= template.start
        && value <= template.end
        && template.render_value(value) == value_text)
}

pub(super) struct HttpNumericTemplate<'a> {
    prefix: &'a str,
    suffix: &'a str,
    start: u64,
    end: u64,
    width: usize,
}

impl HttpNumericTemplate<'_> {
    fn render_value(&self, value: u64) -> String {
        if self.width == 0 {
            value.to_string()
        } else {
            format!("{value:0width$}", width = self.width)
        }
    }

    fn render(&self, value: u64) -> String {
        format!("{}{}{}", self.prefix, self.render_value(value), self.suffix)
    }
}

pub(super) fn parse_http_numeric_template<'a>(
    resource_id: &ResourceId,
    glob: &'a str,
) -> Result<Option<HttpNumericTemplate<'a>>> {
    let components = pattern_components(glob)?;
    if components
        .iter()
        .any(|component| component == "**" || has_wildcards(component))
    {
        return Err(CdfError::contract(format!(
            "HTTP(S) file resource `{resource_id}` cannot enumerate unbounded glob `{glob}` because HTTP has no LIST operation; use an explicit file or a finite numeric template such as `{{01..12}}`"
        )));
    }
    let Some(open) = glob.find('{') else {
        if glob.contains('}') {
            return Err(CdfError::contract(format!(
                "HTTP(S) file resource `{resource_id}` has an unmatched `}}` in glob `{glob}`"
            )));
        }
        return Ok(None);
    };
    let close = glob[open + 1..]
        .find('}')
        .map(|offset| open + 1 + offset)
        .ok_or_else(|| {
            CdfError::contract(format!(
                "HTTP(S) file resource `{resource_id}` has an unmatched `{{` in glob `{glob}`"
            ))
        })?;
    if glob[close + 1..].contains('{')
        || glob[close + 1..].contains('}')
        || glob[..open].contains('}')
    {
        return Err(CdfError::contract(format!(
            "HTTP(S) file resource `{resource_id}` supports one numeric range template per glob; got `{glob}`"
        )));
    }
    let range = &glob[open + 1..close];
    let (start_text, end_text) = range.split_once("..").ok_or_else(|| {
        CdfError::contract(format!(
            "HTTP(S) file resource `{resource_id}` template `{{{range}}}` must be an inclusive numeric range such as `{{01..12}}`"
        ))
    })?;
    if start_text.is_empty()
        || end_text.is_empty()
        || !start_text.bytes().all(|byte| byte.is_ascii_digit())
        || !end_text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(CdfError::contract(format!(
            "HTTP(S) file resource `{resource_id}` template `{{{range}}}` must contain decimal integers"
        )));
    }
    let start = start_text.parse::<u64>().map_err(|error| {
        CdfError::contract(format!(
            "invalid HTTP template start `{start_text}`: {error}"
        ))
    })?;
    let end = end_text.parse::<u64>().map_err(|error| {
        CdfError::contract(format!("invalid HTTP template end `{end_text}`: {error}"))
    })?;
    if start > end {
        return Err(CdfError::contract(format!(
            "HTTP(S) file resource `{resource_id}` template start {start} exceeds end {end}"
        )));
    }
    let width = if start_text.starts_with('0') || end_text.starts_with('0') {
        start_text.len().max(end_text.len())
    } else {
        0
    };
    Ok(Some(HttpNumericTemplate {
        prefix: &glob[..open],
        suffix: &glob[close + 1..],
        start,
        end,
        width,
    }))
}

pub(super) fn expand_http_year_month_glob(glob: &str) -> Option<Vec<String>> {
    if glob.matches('*').count() != 1
        || glob.contains("**")
        || glob.contains('?')
        || glob.contains('[')
        || glob.contains(']')
    {
        return None;
    }
    let star = glob.find('*')?;
    let prefix = &glob[..star];
    let year = prefix.strip_suffix('-')?.rsplit(['/', '_', '-']).next()?;
    if year.len() != 4 || !year.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(
        (1..=12)
            .map(|month| format!("{}{:02}{}", prefix, month, &glob[star + 1..]))
            .collect(),
    )
}

pub(super) fn join_http_root_and_glob(root: &str, glob: &str) -> String {
    format!(
        "{}/{}",
        root.trim_end_matches('/'),
        glob.trim_start_matches('/')
    )
}

pub(super) fn file_partition_id(path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("file-{}", &digest[..16])
}

pub(super) fn has_wildcards(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?')
}

pub(super) fn glob_component_matches(pattern: &str, candidate: &str) -> bool {
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

pub(super) fn glob_path_matches(pattern: &[String], candidate: &str) -> bool {
    let candidate = candidate
        .split('/')
        .filter(|component| !component.is_empty())
        .collect::<Vec<_>>();
    let mut table = vec![vec![false; candidate.len() + 1]; pattern.len() + 1];
    table[0][0] = true;
    for pattern_index in 1..=pattern.len() {
        if pattern[pattern_index - 1] == "**" {
            table[pattern_index][0] = table[pattern_index - 1][0];
        }
        for candidate_index in 1..=candidate.len() {
            table[pattern_index][candidate_index] = if pattern[pattern_index - 1] == "**" {
                table[pattern_index - 1][candidate_index]
                    || table[pattern_index][candidate_index - 1]
            } else {
                table[pattern_index - 1][candidate_index - 1]
                    && glob_component_matches(
                        &pattern[pattern_index - 1],
                        candidate[candidate_index - 1],
                    )
            };
        }
    }
    table[pattern.len()][candidate.len()]
}
