use std::{collections::BTreeMap, fs, path::Path, sync::Arc};

use arrow_schema::SchemaRef;
use cdf_kernel::{CdfError, ResourceId, Result};
use cdf_object_access::{FileTransportControl, FileTransportResource, LocalByteSource};
use cdf_runtime::{
    ByteExtent, ByteSource, FormatDetectionConfidence, FormatDiscoveryRequest, FormatDriver,
    FormatProbe, FormatRegistry, GenerationStrength, SourceContentDigest,
};

use crate::FileFormatDeclaration;

use super::{
    FileRuntimeDependencies,
    input::{
        HashingByteSource, PreparedFilePayloadKeyInput, SequentialPayloadCapture,
        prepared_file_payload, prepared_file_payload_key, retain_spool,
        retains_sequential_discovery_payload, spool_byte_source_async, transformed_byte_source,
    },
    resolution::{diagnostic_location, format_detection_confidence_name},
};

#[derive(Clone, Debug)]
pub struct BinarySchemaProbe {
    pub schema: SchemaRef,
    pub source_identity: BTreeMap<String, String>,
    pub probe_bytes_read: u64,
    pub probe_records_read: u64,
}

#[derive(Clone, Debug)]
pub struct SchemaDiscoveryRequest<'a> {
    pub resource_id: &'a ResourceId,
    pub format: &'a FileFormatDeclaration,
    pub format_declared: bool,
    pub format_options: &'a serde_json::Value,
    pub discovery_kind: cdf_runtime::FormatDiscoveryKind,
    pub transform_name: &'a str,
    pub maximum_bytes: u64,
    pub maximum_records: u64,
    pub cancellation: cdf_runtime::RunCancellation,
}

pub fn discover_local_binary_schema(
    path: impl AsRef<Path>,
    location: &str,
    dependencies: &FileRuntimeDependencies,
    initial_bytes_read: u64,
    request: SchemaDiscoveryRequest<'_>,
) -> Result<BinarySchemaProbe> {
    request.cancellation.check()?;
    let path = path.as_ref().to_path_buf();
    let source_size = fs::metadata(&path)
        .map_err(|error| CdfError::data(format!("stat {} for discovery: {error}", path.display())))?
        .len();
    let driver = dependencies.formats().resolve(request.format.as_str())?;
    let upstream: Arc<dyn ByteSource> = Arc::new(LocalByteSource::open(
        &path,
        dependencies.execution().memory(),
    )?);
    let upstream_identity = upstream.identity().clone();
    let transform_id = (request.transform_name != "none")
        .then(|| {
            dependencies
                .transforms()
                .resolve_name(request.transform_name)
        })
        .transpose()?
        .map(|driver| driver.descriptor().transform_id.clone());
    let needs_spool = transform_id.is_some()
        && driver.descriptor().source_access != cdf_runtime::FormatSourceAccess::Sequential;
    let retain_sequential =
        retains_sequential_discovery_payload(driver.descriptor(), request.discovery_kind);
    let extraction_content_hash =
        (needs_spool || retain_sequential).then(SourceContentDigest::default);
    let upstream = match extraction_content_hash.as_ref() {
        Some(observation) => {
            Arc::new(HashingByteSource::new(upstream, observation.clone())) as Arc<dyn ByteSource>
        }
        None => upstream,
    };
    let source = match transform_id.as_ref() {
        Some(transform_id) => transformed_byte_source(upstream, transform_id, dependencies)?,
        None => upstream,
    };
    let logical_source_identity = source.identity().clone();
    let options = driver.canonical_options(request.format_options.clone())?;
    let prepared_payload_key = prepared_file_payload_key(
        PreparedFilePayloadKeyInput {
            resource_id: request.resource_id,
            location,
            size_bytes: source_size,
            source_generation: upstream_identity.generation.as_deref(),
            etag: None,
            object_version: None,
            sha256: upstream_identity.checksum.as_deref(),
            driver: driver.as_ref(),
            canonical_format_options: &options,
            transform_name: request.transform_name,
        },
        dependencies,
    )?;
    let discovery_memory = dependencies.execution().memory();
    let confirmation = FormatConfirmationContext {
        resource_id: request.resource_id.clone(),
        location: location.to_owned(),
        format_declared: request.format_declared,
        transform_name: request.transform_name.to_owned(),
    };
    let maximum_bytes = request.maximum_bytes;
    let maximum_records = request.maximum_records;
    let discovery_kind = request.discovery_kind;
    let cancellation = request.cancellation.clone();
    let observation = dependencies.execution().run_io({
        let dependencies = dependencies.clone();
        let driver = Arc::clone(&driver);
        let source = Arc::clone(&source);
        let extraction_content_hash = extraction_content_hash.clone();
        async move {
            let mut spool = None;
            let mut sequential_capture = None;
            let source = if needs_spool {
                let accounted = Arc::new(
                    spool_byte_source_async(
                        source,
                        None,
                        None,
                        &dependencies,
                        cancellation.clone(),
                    )
                    .await?,
                );
                let local: Arc<dyn ByteSource> = Arc::new(LocalByteSource::open(
                    accounted.path(),
                    dependencies.execution().memory(),
                )?);
                spool = Some(accounted);
                local
            } else if retain_sequential {
                let capture = SequentialPayloadCapture::new(source, &dependencies).await?;
                let source = capture.discovery_source();
                sequential_capture = Some(capture);
                source
            } else {
                source
            };
            let logical_size = match spool.as_ref() {
                Some(spool) => fs::metadata(spool.path())
                    .map_err(|error| {
                        CdfError::data(format!(
                            "stat transformed discovery spool for {}: {error}",
                            confirmation.location
                        ))
                    })?
                    .len(),
                None => source_size,
            };
            let confirmation_bytes = confirm_registered_format(
                source.as_ref(),
                logical_size,
                &driver,
                dependencies.formats(),
                &confirmation,
                cancellation.clone(),
            )
            .await?;
            let discovery_bytes = schema_observation_byte_limit(
                maximum_bytes,
                confirmation_bytes,
                &confirmation,
                discovery_kind,
            )?;
            let observation = driver
                .discover(
                    Arc::clone(&source),
                    FormatDiscoveryRequest {
                        options,
                        discovery_kind,
                        maximum_bytes: discovery_bytes,
                        maximum_records,
                        memory: discovery_memory,
                        cancellation: cancellation.clone(),
                    },
                )
                .await?;
            let probe_bytes_read = if spool.is_some() {
                source_size
            } else {
                observation.sampled_bytes.saturating_add(confirmation_bytes)
            };
            if let Some(capture) = sequential_capture {
                let payload = capture.finish(extraction_content_hash.clone()).await?;
                dependencies
                    .prepared_payloads()
                    .install(prepared_payload_key, payload)?;
            } else if let Some(spool) = spool {
                let retention = retain_spool(&spool, logical_size)?;
                dependencies.prepared_payloads().install(
                    prepared_payload_key,
                    prepared_file_payload(source, retention, extraction_content_hash.clone())?,
                )?;
            }
            Ok::<_, CdfError>((observation, probe_bytes_read))
        }
    })?;
    let (observation, probe_bytes_read) = observation;
    let probe_records_read = observation.sampled_records;
    let schema = observation.arrow_schema;
    let schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?;
    let mut source_identity = BTreeMap::from([
        (
            "stable_id".to_owned(),
            diagnostic_location(&logical_source_identity.stable_id)?,
        ),
        ("format".to_owned(), request.format.as_str().to_owned()),
        (
            "format_driver_version".to_owned(),
            driver.descriptor().semantic_version.clone(),
        ),
        ("schema_hash".to_owned(), schema_hash.to_string()),
        ("size_bytes".to_owned(), source_size.to_string()),
    ]);
    merge_discovery_evidence(&mut source_identity, observation.evidence)?;
    if let Some(generation) = observation.identity.generation {
        source_identity.insert("generation".to_owned(), generation);
    }
    if let Some(checksum) = observation.identity.checksum {
        source_identity.insert("checksum".to_owned(), checksum);
    }
    source_identity.insert("path".to_owned(), path.to_string_lossy().into_owned());
    source_identity.insert("compression".to_owned(), request.transform_name.to_owned());
    source_identity.insert("source_size_bytes".to_owned(), source_size.to_string());
    Ok(BinarySchemaProbe {
        schema,
        source_identity,
        probe_bytes_read: initial_bytes_read.saturating_add(probe_bytes_read),
        probe_records_read,
    })
}

pub fn discover_transport_binary_schema(
    resource: FileTransportResource,
    dependencies: &FileRuntimeDependencies,
    request: SchemaDiscoveryRequest<'_>,
) -> Result<BinarySchemaProbe> {
    let control = FileTransportControl::new(request.cancellation.clone(), None);
    let observation = dependencies
        .with_transport(|transport, egress| transport.metadata(egress, &resource, &control))?;
    let access_resource = observation.access_resource(&resource);
    let metadata = observation.into_identity();
    let evidence_location = diagnostic_location(&metadata.location)?;
    let size_bytes = metadata.size_bytes.ok_or_else(|| {
        CdfError::data(format!(
            "remote binary discovery for `{}` did not receive byte-size metadata",
            evidence_location
        ))
    })?;
    let driver = dependencies.formats().resolve(request.format.as_str())?;
    let upstream = dependencies.with_transport(|transport, egress| {
        transport.open_byte_source(
            egress,
            &access_resource,
            &metadata,
            dependencies.execution().memory(),
        )
    })?;
    let transform_id = (request.transform_name != "none")
        .then(|| {
            dependencies
                .transforms()
                .resolve_name(request.transform_name)
        })
        .transpose()?
        .map(|driver| driver.descriptor().transform_id.clone());
    let needs_spool = driver.descriptor().source_access
        != cdf_runtime::FormatSourceAccess::Sequential
        && (!upstream.capabilities().seekable || transform_id.is_some());
    let retain_sequential =
        retains_sequential_discovery_payload(driver.descriptor(), request.discovery_kind);
    let extraction_content_hash = ((needs_spool || retain_sequential)
        && metadata.generation_strength() == GenerationStrength::Weak)
        .then(SourceContentDigest::default);
    let upstream = match extraction_content_hash.as_ref() {
        Some(observation) => {
            Arc::new(HashingByteSource::new(upstream, observation.clone())) as Arc<dyn ByteSource>
        }
        None => upstream,
    };
    let source = match transform_id.as_ref() {
        Some(transform_id) => transformed_byte_source(upstream, transform_id, dependencies)?,
        None => upstream,
    };
    let logical_source_identity = source.identity().clone();
    let execution = dependencies.execution().clone();
    let memory = execution.memory();
    let options = driver.canonical_options(request.format_options.clone())?;
    let source_generation = (metadata.generation_strength() == GenerationStrength::Weak)
        .then_some(metadata.modified.as_deref())
        .flatten();
    let prepared_payload_key = prepared_file_payload_key(
        PreparedFilePayloadKeyInput {
            resource_id: request.resource_id,
            location: &metadata.location,
            size_bytes,
            source_generation,
            etag: metadata.etag.as_deref(),
            object_version: metadata.version.as_deref(),
            sha256: metadata.sha256(),
            driver: driver.as_ref(),
            canonical_format_options: &options,
            transform_name: request.transform_name,
        },
        dependencies,
    )?;
    let confirmation = FormatConfirmationContext {
        resource_id: request.resource_id.clone(),
        location: evidence_location.clone(),
        format_declared: request.format_declared,
        transform_name: request.transform_name.to_owned(),
    };
    let maximum_bytes = request.maximum_bytes;
    let maximum_records = request.maximum_records;
    let discovery_kind = request.discovery_kind;
    let cancellation = request.cancellation.clone();
    let observation = execution.run_io({
        let dependencies = dependencies.clone();
        let driver = Arc::clone(&driver);
        let extraction_content_hash = extraction_content_hash.clone();
        async move {
            let mut spool = None;
            let mut sequential_capture = None;
            let source = if needs_spool {
                let accounted = Arc::new(
                    spool_byte_source_async(
                        source,
                        None,
                        None,
                        &dependencies,
                        cancellation.clone(),
                    )
                    .await?,
                );
                let local: Arc<dyn ByteSource> = Arc::new(LocalByteSource::open(
                    accounted.path(),
                    dependencies.execution().memory(),
                )?);
                spool = Some(accounted);
                local
            } else if retain_sequential {
                let capture = SequentialPayloadCapture::new(source, &dependencies).await?;
                let source = capture.discovery_source();
                sequential_capture = Some(capture);
                source
            } else {
                source
            };
            let logical_size = match spool.as_ref() {
                Some(spool) => fs::metadata(spool.path())
                    .map_err(|error| {
                        CdfError::data(format!(
                            "stat transformed discovery spool for {}: {error}",
                            confirmation.location
                        ))
                    })?
                    .len(),
                None => size_bytes,
            };
            let confirmation_bytes = confirm_registered_format(
                source.as_ref(),
                logical_size,
                &driver,
                dependencies.formats(),
                &confirmation,
                cancellation.clone(),
            )
            .await?;
            let discovery_bytes = schema_observation_byte_limit(
                maximum_bytes,
                confirmation_bytes,
                &confirmation,
                discovery_kind,
            )?;
            let observation = driver
                .discover(
                    Arc::clone(&source),
                    FormatDiscoveryRequest {
                        options,
                        discovery_kind,
                        maximum_bytes: discovery_bytes,
                        maximum_records,
                        memory,
                        cancellation: cancellation.clone(),
                    },
                )
                .await?;
            let probe_bytes_read = if spool.is_some() {
                size_bytes
            } else {
                observation.sampled_bytes.saturating_add(confirmation_bytes)
            };
            if let Some(capture) = sequential_capture {
                let payload = capture.finish(extraction_content_hash.clone()).await?;
                dependencies
                    .prepared_payloads()
                    .install(prepared_payload_key, payload)?;
            } else if let Some(spool) = spool {
                let retention = retain_spool(&spool, logical_size)?;
                dependencies.prepared_payloads().install(
                    prepared_payload_key,
                    prepared_file_payload(source, retention, extraction_content_hash.clone())?,
                )?;
            }
            Ok::<_, CdfError>((observation, probe_bytes_read))
        }
    })?;
    let (observation, probe_bytes_read) = observation;
    let probe_records_read = observation.sampled_records;
    let schema_hash = cdf_kernel::canonical_arrow_schema_hash(observation.arrow_schema.as_ref())?;
    let mut source_identity = BTreeMap::from([
        (
            "stable_id".to_owned(),
            diagnostic_location(&logical_source_identity.stable_id)?,
        ),
        ("format".to_owned(), request.format.as_str().to_owned()),
        (
            "format_driver_version".to_owned(),
            driver.descriptor().semantic_version.clone(),
        ),
        ("schema_hash".to_owned(), schema_hash.to_string()),
        ("compression".to_owned(), request.transform_name.to_owned()),
        ("source_size_bytes".to_owned(), size_bytes.to_string()),
        ("size_bytes".to_owned(), size_bytes.to_string()),
    ]);
    merge_discovery_evidence(&mut source_identity, observation.evidence)?;
    let mut probe = BinarySchemaProbe {
        schema: observation.arrow_schema,
        source_identity,
        probe_bytes_read,
        probe_records_read,
    };
    probe
        .source_identity
        .insert("url".to_owned(), evidence_location);
    if let Some(etag) = &metadata.etag {
        probe
            .source_identity
            .insert("etag".to_owned(), etag.clone());
    }
    if let Some(version) = &metadata.version {
        probe
            .source_identity
            .insert("version".to_owned(), version.clone());
    }
    if let Some(sha256) = metadata.sha256() {
        probe
            .source_identity
            .insert("sha256".to_owned(), sha256.to_owned());
    }
    Ok(probe)
}

pub(super) struct FormatConfirmationContext {
    pub(super) resource_id: ResourceId,
    pub(super) location: String,
    pub(super) format_declared: bool,
    pub(super) transform_name: String,
}

pub(super) fn discovery_budget_after_confirmation(
    maximum_bytes: u64,
    confirmation_bytes: u64,
    context: &FormatConfirmationContext,
) -> Result<u64> {
    let remaining = maximum_bytes.checked_sub(confirmation_bytes).ok_or_else(|| {
        CdfError::data(format!(
            "format confirmation for resource `{}`, file `{}` requires {confirmation_bytes} bytes, exceeding the configured {maximum_bytes}-byte discovery budget",
            context.resource_id, context.location
        ))
    })?;
    if remaining == 0 {
        return Err(CdfError::data(format!(
            "format confirmation for resource `{}`, file `{}` consumes the configured {maximum_bytes}-byte discovery budget; increase the discovery byte budget to leave room for schema observation",
            context.resource_id, context.location
        )));
    }
    Ok(remaining)
}

pub(super) fn schema_observation_byte_limit(
    maximum_bytes: u64,
    confirmation_bytes: u64,
    context: &FormatConfirmationContext,
    discovery_kind: cdf_runtime::FormatDiscoveryKind,
) -> Result<u64> {
    if discovery_kind == cdf_runtime::FormatDiscoveryKind::FullContent {
        return Ok(maximum_bytes);
    }
    discovery_budget_after_confirmation(maximum_bytes, confirmation_bytes, context)
}

pub(super) async fn confirm_registered_format(
    source: &dyn ByteSource,
    source_size: u64,
    driver: &Arc<dyn FormatDriver>,
    formats: &FormatRegistry,
    context: &FormatConfirmationContext,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<u64> {
    cancellation.check()?;
    if driver.descriptor().magic.is_empty() || source_size == 0 {
        return Ok(0);
    }
    if !source.capabilities().exact_ranges {
        return Err(CdfError::contract(format!(
            "format `{}` requires bounded magic confirmation but byte source `{}` does not support exact ranges; spool the admitted stream before format discovery",
            driver.descriptor().format_id,
            source.identity().stable_id
        )));
    }
    let descriptors = formats.descriptors();
    let prefix_length = descriptors
        .iter()
        .map(|descriptor| u64::from(descriptor.detection_probe.prefix_bytes))
        .max()
        .unwrap_or(0)
        .min(source_size);
    let suffix_length = descriptors
        .iter()
        .map(|descriptor| u64::from(descriptor.detection_probe.suffix_bytes))
        .max()
        .unwrap_or(0)
        .min(source_size);
    let prefix = if prefix_length == 0 {
        None
    } else {
        Some(
            source
                .read_exact_range(ByteExtent::new(0, prefix_length)?, cancellation.clone())
                .await?,
        )
    };
    let suffix = if suffix_length == 0 {
        None
    } else {
        Some(
            source
                .read_exact_range(
                    ByteExtent::new(source_size - suffix_length, suffix_length)?,
                    cancellation,
                )
                .await?,
        )
    };
    let extension = discovery_format_extension(&context.location, &context.transform_name);
    let probe = FormatProbe {
        extension: extension.clone(),
        mime_type: None,
        prefix: prefix
            .as_ref()
            .map(|bytes| bytes.payload().to_vec())
            .unwrap_or_default(),
        suffix: suffix
            .as_ref()
            .map(|bytes| bytes.payload().to_vec())
            .unwrap_or_default(),
    };
    let selected_detection = driver.detect(&probe)?;
    let strong_magic = formats.detect_strong_magic(&probe.prefix)?;
    let strong_magic_id = strong_magic
        .as_ref()
        .map(|detected| detected.descriptor().format_id.as_str());
    let selected_id = driver.descriptor().format_id.as_str();
    if strong_magic_id.is_some_and(|detected| detected != selected_id)
        || selected_detection.confidence == FormatDetectionConfidence::None
    {
        let declared = if context.format_declared {
            selected_id
        } else {
            "<omitted>"
        };
        let magic = strong_magic_id.unwrap_or("none");
        let selected_format_id = driver.descriptor().format_id.clone();
        let alternate = formats
            .detect_best_alternate(&probe, &selected_format_id)?
            .map(|(id, detection)| {
                format!(
                    "; alternate format `{id}` detected with {} confidence: {}",
                    format_detection_confidence_name(detection.confidence),
                    detection.reason
                )
            })
            .unwrap_or_default();
        return Err(CdfError::data(format!(
            "file format confirmation failed for resource `{}`, file `{}`: declared format `{declared}`, inferred format `{selected_id}`, extension signal `{}`, magic bytes signal `{magic}`{alternate}; use `format = \"{selected_id}\"` only when the bytes match, or correct the file/extension",
            context.resource_id,
            context.location,
            extension.as_deref().unwrap_or("none")
        )));
    }
    Ok(prefix_length.saturating_add(suffix_length))
}

pub(super) fn discovery_format_extension(location: &str, transform_name: &str) -> Option<String> {
    let location = location
        .split('#')
        .next()
        .unwrap_or(location)
        .split('?')
        .next()
        .unwrap_or(location);
    let mut pieces = location.rsplit('.');
    let outer = pieces.next()?;
    if transform_name == "none" {
        return Some(outer.to_ascii_lowercase());
    }
    pieces.next().map(str::to_ascii_lowercase)
}

pub(super) fn merge_discovery_evidence(
    source_identity: &mut BTreeMap<String, String>,
    evidence: BTreeMap<String, String>,
) -> Result<()> {
    for (key, value) in evidence {
        if source_identity.contains_key(&key) {
            return Err(CdfError::contract(format!(
                "format discovery evidence key `{key}` conflicts with source identity authority"
            )));
        }
        source_identity.insert(key, value);
    }
    Ok(())
}
