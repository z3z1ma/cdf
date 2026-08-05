use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Component, Path},
};

use cdf_kernel::{CdfError, Result};
use cdf_semantic::SemanticCatalog;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CdfLock, CompiledArtifactInput, CompiledProjectResource, EffectiveEnvironment, LOCK_FILE_NAME,
    LockedResource, ManifestDiagnostic, ManifestLineageEdge, ManifestSemanticDefinition,
    ManifestSemanticSource, PROJECT_FILE_NAME, ProjectConfig, lock_to_toml,
    manifest::{
        ProjectCompilationMode, ProjectManifestCompileRequest, compile_project_manifest,
        validate_compiled_artifact_sections, validate_compiled_artifact_security,
    },
    parse_cdf_toml, parse_lock, project_file_transaction_generation,
    project_inputs::current_project_source_configuration,
};

pub const COMPILATION_INDEX_RELATIVE_PATH: &str = ".cdf/manifest.json";
pub const COMPILED_RESOURCE_DIRECTORY: &str = ".cdf/compiled";
pub const COMPILED_RESOURCE_ARTIFACT_VERSION: u16 = 1;
pub const COMPILATION_INDEX_VERSION: u16 = 1;
const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAX_INDEX_RESOURCES: usize = 100_000;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledResourceArtifact {
    pub version: u16,
    pub artifact_hash: String,
    pub project_name: String,
    pub environment: String,
    pub environment_binding_hash: String,
    pub compiler_version: String,
    pub lock_binding: LockedResource,
    pub inputs: Vec<CompiledArtifactInput>,
    pub resource: crate::ManifestResource,
    pub semantics: Vec<ManifestSemanticDefinition>,
    pub lineage: Vec<ManifestLineageEdge>,
    pub diagnostics: Vec<ManifestDiagnostic>,
}

#[derive(Serialize)]
struct ArtifactIdentity<'a> {
    version: u16,
    project_name: &'a str,
    environment: &'a str,
    environment_binding_hash: &'a str,
    compiler_version: &'a str,
    lock_binding: &'a LockedResource,
    inputs: &'a [CompiledArtifactInput],
    resource: &'a crate::ManifestResource,
    semantics: &'a [ManifestSemanticDefinition],
    lineage: &'a [ManifestLineageEdge],
    diagnostics: &'a [ManifestDiagnostic],
}

pub struct CompiledResourceArtifactRequest<'a> {
    pub config: &'a ProjectConfig,
    pub environment: &'a EffectiveEnvironment,
    pub lock: &'a CdfLock,
    pub resource: &'a CompiledProjectResource,
    pub authored_inputs: Vec<CompiledArtifactInput>,
    pub semantic_catalog: &'a SemanticCatalog,
    pub semantic_sources: BTreeMap<String, ManifestSemanticSource>,
    pub selected_destination_id: &'a str,
    pub diagnostics: Vec<ManifestDiagnostic>,
}

pub fn compile_resource_artifact(
    request: CompiledResourceArtifactRequest<'_>,
) -> Result<CompiledResourceArtifact> {
    let resource_id = request.resource.resource.descriptor().resource_id.as_str();
    let mut scoped_lock = request.lock.clone();
    let locked = scoped_lock
        .resources
        .get(resource_id)
        .cloned()
        .ok_or_else(|| {
            CdfError::contract(format!("cdf.lock is missing resource `{resource_id}`"))
        })?;
    if locked.compiled_artifact_hash.is_some() {
        return Err(CdfError::internal(format!(
            "compiled artifact for `{resource_id}` must be built before its hash is bound"
        )));
    }
    scoped_lock.resources.retain(|id, _| id == resource_id);
    let lock_bytes = lock_to_toml(&scoped_lock)?.into_bytes();
    let manifest = compile_project_manifest(ProjectManifestCompileRequest {
        config: request.config,
        environment: request.environment,
        lock: &scoped_lock,
        lock_bytes: &lock_bytes,
        resources: std::slice::from_ref(request.resource),
        authored_inputs: request.authored_inputs,
        semantic_catalog: request.semantic_catalog,
        semantic_sources: request.semantic_sources,
        selected_destination_id: request.selected_destination_id,
        compilation_mode: ProjectCompilationMode::ResourceArtifact,
        generated_at_unix_ms: None,
        diagnostics: request.diagnostics,
    })?;
    let [resource] = manifest.resources.as_slice() else {
        return Err(CdfError::internal(
            "resource artifact compiler did not produce exactly one resource",
        ));
    };
    let mut artifact = CompiledResourceArtifact {
        version: COMPILED_RESOURCE_ARTIFACT_VERSION,
        artifact_hash: sha256(&[]),
        project_name: manifest.header.project_name,
        environment: manifest.header.environment,
        environment_binding_hash: manifest.header.environment_binding_hash.as_str().to_owned(),
        compiler_version: manifest.header.compiler_version,
        lock_binding: locked,
        inputs: manifest.inputs,
        resource: resource.clone(),
        semantics: manifest.semantics,
        lineage: manifest.lineage,
        diagnostics: manifest.diagnostics,
    };
    artifact.artifact_hash = artifact.identity_hash()?;
    artifact.validate()?;
    Ok(artifact)
}

impl CompiledResourceArtifact {
    pub fn validate(&self) -> Result<()> {
        if self.version != COMPILED_RESOURCE_ARTIFACT_VERSION {
            return Err(CdfError::data(format!(
                "unsupported compiled resource artifact version {}; expected {COMPILED_RESOURCE_ARTIFACT_VERSION}",
                self.version
            )));
        }
        validate_sha256("compiled resource artifact", &self.artifact_hash)?;
        validate_sha256(
            "compiled resource environment binding",
            &self.environment_binding_hash,
        )?;
        if self.lock_binding.compiled_artifact_hash.is_some() {
            return Err(CdfError::data(
                "compiled resource artifact lock binding contains its own artifact hash",
            ));
        }
        if self.resource.resource_id != self.lock_binding.descriptor.resource_id.as_str()
            || self.resource.descriptor != self.lock_binding.descriptor
            || self.resource.capabilities != self.lock_binding.capabilities
            || self.resource.output_schema != self.lock_binding.schema
            || self.resource.output_schema_hash.as_str()
                != self.lock_binding.schema_hash.as_deref().unwrap_or_default()
            || self.resource.source_binding != self.lock_binding.compiler.source
        {
            return Err(CdfError::data(format!(
                "compiled resource artifact `{}` differs from its independent lock authority",
                self.resource.resource_id
            )));
        }
        let inputs = self
            .inputs
            .iter()
            .map(|input| input.input_id.as_str())
            .collect::<BTreeSet<_>>();
        if !self
            .resource
            .origin
            .authored_input_ids
            .iter()
            .all(|input| inputs.contains(input.as_str()))
        {
            return Err(CdfError::data(format!(
                "compiled resource artifact `{}` is missing an authored input",
                self.resource.resource_id
            )));
        }
        validate_compiled_artifact_sections(
            &self.inputs,
            &self.resource,
            &self.semantics,
            &self.lineage,
            &self.diagnostics,
        )?;
        validate_compiled_artifact_security(self)?;
        if self.artifact_hash != self.identity_hash()? {
            return Err(CdfError::data(
                "compiled resource artifact hash does not match its canonical content",
            ));
        }
        Ok(())
    }

    pub fn canonical_json_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        canonical_pretty_json(self, MAX_ARTIFACT_BYTES, "compiled resource artifact")
    }

    fn identity_hash(&self) -> Result<String> {
        canonical_hash(&ArtifactIdentity {
            version: self.version,
            project_name: &self.project_name,
            environment: &self.environment,
            environment_binding_hash: &self.environment_binding_hash,
            compiler_version: &self.compiler_version,
            lock_binding: &self.lock_binding,
            inputs: &self.inputs,
            resource: &self.resource,
            semantics: &self.semantics,
            lineage: &self.lineage,
            diagnostics: &self.diagnostics,
        })
    }
}

pub fn parse_compiled_resource_artifact(bytes: &[u8]) -> Result<CompiledResourceArtifact> {
    if bytes.len() > MAX_ARTIFACT_BYTES {
        return Err(CdfError::data(format!(
            "compiled resource artifact exceeds the {MAX_ARTIFACT_BYTES}-byte read bound"
        )));
    }
    let artifact: CompiledResourceArtifact = serde_json::from_slice(bytes)
        .map_err(|error| CdfError::data(format!("parse compiled resource artifact: {error}")))?;
    artifact.validate()?;
    if artifact.canonical_json_bytes()? != bytes {
        return Err(CdfError::data(
            "compiled resource artifact bytes are not canonical",
        ));
    }
    Ok(artifact)
}

pub fn compiled_resource_artifact_path(resource_id: &str, artifact_hash: &str) -> Result<String> {
    validate_resource_id(resource_id)?;
    validate_sha256("compiled resource artifact", artifact_hash)?;
    Ok(format!(
        "{COMPILED_RESOURCE_DIRECTORY}/{resource_id}@{}.json",
        artifact_hash.trim_start_matches("sha256:")
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompilationStatus {
    Current,
    Stale,
    Failed,
    Absent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilationArtifactReference {
    pub path: String,
    pub artifact_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilationDiagnostic {
    pub code: String,
    pub kind: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilationIndexEntry {
    pub resource_id: String,
    pub path: String,
    pub authored_content_hash: Option<String>,
    pub status: CompilationStatus,
    pub artifact: Option<CompilationArtifactReference>,
    pub diagnostic: Option<CompilationDiagnostic>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilationIndex {
    pub version: u16,
    pub index_hash: String,
    pub project_name: String,
    pub environment: String,
    pub environment_binding_hash: String,
    pub resources: BTreeMap<String, CompilationIndexEntry>,
}

#[derive(Serialize)]
struct IndexIdentity<'a> {
    version: u16,
    project_name: &'a str,
    environment: &'a str,
    environment_binding_hash: &'a str,
    resources: &'a BTreeMap<String, CompilationIndexEntry>,
}

impl CompilationIndex {
    pub fn empty(config: &ProjectConfig, environment: &EffectiveEnvironment) -> Result<Self> {
        let mut index = Self {
            version: COMPILATION_INDEX_VERSION,
            index_hash: sha256(&[]),
            project_name: config.project.name.clone(),
            environment: environment.name.clone(),
            environment_binding_hash: canonical_hash(environment)?,
            resources: BTreeMap::new(),
        };
        index.rehash()?;
        Ok(index)
    }

    pub fn record_current(&mut self, artifact: &CompiledResourceArtifact) -> Result<()> {
        let resource_id = artifact.resource.resource_id.clone();
        let path = compiled_resource_artifact_path(&resource_id, &artifact.artifact_hash)?;
        self.resources.insert(
            resource_id.clone(),
            CompilationIndexEntry {
                resource_id,
                path: artifact.resource.origin.relative_path.clone(),
                authored_content_hash: Some(artifact.resource.origin.authored_content_hash.clone()),
                status: CompilationStatus::Current,
                artifact: Some(CompilationArtifactReference {
                    path,
                    artifact_hash: artifact.artifact_hash.clone(),
                }),
                diagnostic: None,
            },
        );
        self.rehash()
    }

    pub fn record_failure(
        &mut self,
        resource_id: &str,
        path: &str,
        authored_content_hash: Option<String>,
        diagnostic: CompilationDiagnostic,
    ) -> Result<()> {
        validate_resource_id(resource_id)?;
        validate_relative_path(path)?;
        self.resources.insert(
            resource_id.to_owned(),
            CompilationIndexEntry {
                resource_id: resource_id.to_owned(),
                path: path.to_owned(),
                authored_content_hash,
                status: CompilationStatus::Failed,
                artifact: None,
                diagnostic: Some(diagnostic),
            },
        );
        self.rehash()
    }

    pub fn record_absent(&mut self, resource_id: &str, path: &str) -> Result<()> {
        validate_resource_id(resource_id)?;
        validate_relative_path(path)?;
        self.resources.insert(
            resource_id.to_owned(),
            CompilationIndexEntry {
                resource_id: resource_id.to_owned(),
                path: path.to_owned(),
                authored_content_hash: None,
                status: CompilationStatus::Absent,
                artifact: None,
                diagnostic: None,
            },
        );
        self.rehash()
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != COMPILATION_INDEX_VERSION {
            return Err(CdfError::data(format!(
                "unsupported compilation index version {}; expected {COMPILATION_INDEX_VERSION}",
                self.version
            )));
        }
        validate_sha256("compilation index", &self.index_hash)?;
        validate_sha256(
            "compilation index environment binding",
            &self.environment_binding_hash,
        )?;
        validate_safe_text("compilation index project name", &self.project_name, 1024)?;
        validate_safe_text("compilation index environment", &self.environment, 1024)?;
        if self.resources.len() > MAX_INDEX_RESOURCES {
            return Err(CdfError::data(format!(
                "compilation index exceeds the {MAX_INDEX_RESOURCES}-resource bound"
            )));
        }
        for (resource_id, entry) in &self.resources {
            validate_resource_id(resource_id)?;
            if entry.resource_id != *resource_id {
                return Err(CdfError::data(format!(
                    "compilation index key `{resource_id}` differs from its entry identity"
                )));
            }
            validate_relative_path(&entry.path)?;
            if let Some(hash) = &entry.authored_content_hash {
                validate_sha256("authored resource content", hash)?;
            }
            if let Some(diagnostic) = &entry.diagnostic {
                validate_safe_text("compilation diagnostic code", &diagnostic.code, 256)?;
                validate_safe_text("compilation diagnostic kind", &diagnostic.kind, 64)?;
                validate_safe_text("compilation diagnostic message", &diagnostic.message, 4096)?;
            }
            match entry.status {
                CompilationStatus::Current => {
                    let artifact = entry.artifact.as_ref().ok_or_else(|| {
                        CdfError::data(format!(
                            "current compilation index entry `{resource_id}` has no artifact"
                        ))
                    })?;
                    if entry.diagnostic.is_some() || entry.authored_content_hash.is_none() {
                        return Err(CdfError::data(format!(
                            "current compilation index entry `{resource_id}` has invalid status details"
                        )));
                    }
                    validate_sha256("compiled resource artifact", &artifact.artifact_hash)?;
                    if artifact.path
                        != compiled_resource_artifact_path(resource_id, &artifact.artifact_hash)?
                    {
                        return Err(CdfError::data(format!(
                            "compilation index entry `{resource_id}` has a non-canonical artifact path"
                        )));
                    }
                }
                CompilationStatus::Failed => {
                    if entry.artifact.is_some() || entry.diagnostic.is_none() {
                        return Err(CdfError::data(format!(
                            "failed compilation index entry `{resource_id}` has invalid status details"
                        )));
                    }
                }
                CompilationStatus::Stale | CompilationStatus::Absent => {
                    if entry.artifact.is_some() || entry.diagnostic.is_some() {
                        return Err(CdfError::data(format!(
                            "non-current compilation index entry `{resource_id}` retains current or failed authority"
                        )));
                    }
                }
            }
        }
        if self.index_hash != self.identity_hash()? {
            return Err(CdfError::data(
                "compilation index hash does not match its canonical content",
            ));
        }
        Ok(())
    }

    pub fn canonical_json_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        canonical_pretty_json(self, MAX_INDEX_BYTES, "compilation index")
    }

    fn rehash(&mut self) -> Result<()> {
        self.index_hash = self.identity_hash()?;
        Ok(())
    }

    fn identity_hash(&self) -> Result<String> {
        canonical_hash(&IndexIdentity {
            version: self.version,
            project_name: &self.project_name,
            environment: &self.environment,
            environment_binding_hash: &self.environment_binding_hash,
            resources: &self.resources,
        })
    }
}

pub fn parse_compilation_index(bytes: &[u8]) -> Result<CompilationIndex> {
    if bytes.len() > MAX_INDEX_BYTES {
        return Err(CdfError::data(format!(
            "compilation index exceeds the {MAX_INDEX_BYTES}-byte read bound"
        )));
    }
    let index: CompilationIndex = serde_json::from_slice(bytes)
        .map_err(|error| CdfError::data(format!("parse compilation index: {error}")))?;
    index.validate()?;
    if index.canonical_json_bytes()? != bytes {
        return Err(CdfError::data("compilation index bytes are not canonical"));
    }
    Ok(index)
}

pub fn validate_compilation_index_authority(
    index: &CompilationIndex,
    config: &ProjectConfig,
    environment: &EffectiveEnvironment,
) -> Result<()> {
    index.validate()?;
    if index.project_name != config.project.name
        || index.environment != environment.name
        || index.environment_binding_hash != canonical_hash(environment)?
    {
        return Err(CdfError::data(
            "compilation index is stale for the selected project environment",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompilationSnapshot {
    pub config: ProjectConfig,
    pub environment: EffectiveEnvironment,
    pub lock: Option<CdfLock>,
    pub index: CompilationIndex,
    pub artifacts: BTreeMap<String, CompiledResourceArtifact>,
    pub generation: u64,
    pub authority_diagnostic: Option<CompilationDiagnostic>,
}

pub fn load_compilation_snapshot(
    project_root: impl AsRef<Path>,
    environment_name: Option<&str>,
) -> Result<CompilationSnapshot> {
    let root = project_root.as_ref();
    for attempt in 0..3 {
        let generation_before = project_file_transaction_generation(root)?;
        let project_bytes =
            read_required_file(&root.join(PROJECT_FILE_NAME), "project configuration")?;
        let config = parse_cdf_toml(std::str::from_utf8(&project_bytes).map_err(|error| {
            CdfError::data(format!("project configuration is not UTF-8: {error}"))
        })?)?;
        let selected = environment_name.unwrap_or(&config.project.default_environment);
        let environment = config.effective_environment(selected)?;
        let lock_bytes = read_optional_file(&root.join(LOCK_FILE_NAME), "project lockfile")?;
        let index_observation = read_optional_file_bounded(
            &root.join(COMPILATION_INDEX_RELATIVE_PATH),
            "compilation index",
            MAX_INDEX_BYTES,
        );
        let (index_bytes, index_read_invalid) = match &index_observation {
            Ok(bytes) => (bytes.clone(), false),
            Err(error) if error.kind == cdf_kernel::ErrorKind::Data => (None, true),
            Err(error) => return Err(error.clone()),
        };
        let (lock, mut authority_diagnostic) = match lock_bytes.as_deref() {
            Some(bytes) => match std::str::from_utf8(bytes)
                .map_err(|error| CdfError::data(format!("cdf.lock is not UTF-8: {error}")))
                .and_then(parse_lock)
            {
                Ok(lock) => (Some(lock), None),
                Err(_) => (
                    None,
                    Some(CompilationDiagnostic {
                        code: "CDF-COMPILE-LOCK".to_owned(),
                        kind: "data".to_owned(),
                        message: "cdf.lock is not valid local compilation authority".to_owned(),
                    }),
                ),
            },
            None => (None, None),
        };
        let mut index = if index_read_invalid {
            authority_diagnostic = Some(CompilationDiagnostic {
                code: "CDF-COMPILE-INDEX".to_owned(),
                kind: "data".to_owned(),
                message: "the local compilation index is invalid or stale".to_owned(),
            });
            CompilationIndex::empty(&config, &environment)?
        } else {
            match index_bytes.as_deref() {
                Some(bytes) => match parse_compilation_index(bytes).and_then(|index| {
                    validate_compilation_index_authority(&index, &config, &environment)?;
                    Ok(index)
                }) {
                    Ok(index) => index,
                    Err(_) => {
                        authority_diagnostic = Some(CompilationDiagnostic {
                            code: "CDF-COMPILE-INDEX".to_owned(),
                            kind: "data".to_owned(),
                            message: "the local compilation index is invalid or stale".to_owned(),
                        });
                        CompilationIndex::empty(&config, &environment)?
                    }
                },
                None => CompilationIndex::empty(&config, &environment)?,
            }
        };
        let mut artifacts = BTreeMap::new();
        let current_ids = index
            .resources
            .iter()
            .filter(|(_, entry)| entry.status == CompilationStatus::Current)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for resource_id in current_ids {
            let entry = index.resources[&resource_id].clone();
            match load_current_artifact(root, &config, &environment, lock.as_ref(), &entry) {
                Ok(artifact) => {
                    artifacts.insert(resource_id, artifact);
                }
                Err(_) => {
                    let entry = index.resources.get_mut(&resource_id).expect("known entry");
                    entry.status = CompilationStatus::Stale;
                    entry.artifact = None;
                    entry.diagnostic = None;
                }
            }
        }
        index.rehash()?;
        let generation_after = project_file_transaction_generation(root)?;
        let stable = generation_before == generation_after
            && read_required_file(&root.join(PROJECT_FILE_NAME), "project configuration")?
                == project_bytes
            && read_optional_file(&root.join(LOCK_FILE_NAME), "project lockfile")? == lock_bytes
            && read_optional_file_bounded(
                &root.join(COMPILATION_INDEX_RELATIVE_PATH),
                "compilation index",
                MAX_INDEX_BYTES,
            ) == index_observation;
        if stable {
            return Ok(CompilationSnapshot {
                config,
                environment,
                lock,
                index,
                artifacts,
                generation: generation_after,
                authority_diagnostic,
            });
        }
        if attempt == 2 {
            return Err(CdfError::contract(
                "project compilation authority changed repeatedly while reading; retry after the writer completes",
            ));
        }
    }
    Err(CdfError::internal(
        "compilation snapshot stable-read loop exited without a result",
    ))
}

fn load_current_artifact(
    root: &Path,
    config: &ProjectConfig,
    environment: &EffectiveEnvironment,
    lock: Option<&CdfLock>,
    entry: &CompilationIndexEntry,
) -> Result<CompiledResourceArtifact> {
    let reference = entry.artifact.as_ref().ok_or_else(|| {
        CdfError::data("current compilation index entry has no artifact reference")
    })?;
    let bytes = read_required_file_bounded(
        &root.join(&reference.path),
        "compiled resource artifact",
        MAX_ARTIFACT_BYTES,
    )?;
    let artifact = parse_compiled_resource_artifact(&bytes)?;
    if artifact.artifact_hash != reference.artifact_hash
        || artifact.project_name != config.project.name
        || artifact.environment != environment.name
        || artifact.environment_binding_hash != canonical_hash(environment)?
        || artifact.resource.resource_id != entry.resource_id
        || artifact.resource.origin.relative_path != entry.path
        || Some(artifact.resource.origin.authored_content_hash.as_str())
            != entry.authored_content_hash.as_deref()
    {
        return Err(CdfError::data(format!(
            "compiled artifact for `{}` does not match its index authority",
            entry.resource_id
        )));
    }
    let configured = &artifact.resource.configured_source;
    let current = current_project_source_configuration(
        config,
        &environment.name,
        &configured.configured_source,
    )?;
    if artifact.lock_binding.compiler.normalizer != config.project.normalizer
        || configured.source_type != current.source_type
        || configured.base_configuration_hash != current.base_hash
        || configured.overlay_configuration_hash != current.overlay_hash
        || configured.effective_configuration_hash != current.effective_hash
    {
        return Err(CdfError::data(format!(
            "compiled artifact for `{}` is stale for its selected project configuration",
            entry.resource_id
        )));
    }
    let locked = lock
        .and_then(|lock| lock.resources.get(&entry.resource_id))
        .ok_or_else(|| CdfError::data("compiled artifact has no matching lock authority"))?;
    let mut expected_binding = locked.clone();
    if expected_binding.compiled_artifact_hash.as_deref() != Some(artifact.artifact_hash.as_str()) {
        return Err(CdfError::data(
            "compiled artifact hash differs from cdf.lock authority",
        ));
    }
    expected_binding.compiled_artifact_hash = None;
    if artifact.lock_binding != expected_binding {
        return Err(CdfError::data(
            "compiled artifact binding differs from cdf.lock authority",
        ));
    }
    let authored = read_required_file(
        &root.join(&artifact.resource.origin.relative_path),
        "authored resource",
    )?;
    if sha256(&authored) != artifact.resource.origin.authored_content_hash {
        return Err(CdfError::data(format!(
            "authored resource `{}` changed after compilation",
            entry.resource_id
        )));
    }
    Ok(artifact)
}

fn canonical_pretty_json<T: Serialize>(value: &T, max: usize, label: &str) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| CdfError::internal(format!("serialize {label}: {error}")))?;
    bytes.push(b'\n');
    if bytes.len() > max {
        return Err(CdfError::contract(format!(
            "{label} exceeds the {max}-byte bound"
        )));
    }
    Ok(bytes)
}

fn canonical_hash<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_vec(value)
        .map(|bytes| sha256(&bytes))
        .map_err(|error| CdfError::internal(format!("serialize canonical identity: {error}")))
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn validate_sha256(label: &str, value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(CdfError::data(format!(
            "{label} hash must use sha256:<hex>"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CdfError::data(format!(
            "{label} hash must contain exactly 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn validate_resource_id(resource_id: &str) -> Result<()> {
    if resource_id.is_empty()
        || resource_id.starts_with('.')
        || resource_id.ends_with('.')
        || resource_id
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')))
    {
        return Err(CdfError::data(format!(
            "compiled resource id `{resource_id}` is not a safe canonical identifier"
        )));
    }
    Ok(())
}

fn validate_safe_text(label: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(CdfError::data(format!(
            "{label} must be non-empty, at most {max_bytes} bytes, and contain no control characters"
        )));
    }
    Ok(())
}

fn validate_relative_path(path: &str) -> Result<()> {
    validate_safe_text("compilation path", path, 4096)?;
    let path = Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(CdfError::data(format!(
            "compilation path {:?} must be a safe project-relative path",
            path.display()
        )));
    }
    Ok(())
}

fn read_required_file(path: &Path, label: &str) -> Result<Vec<u8>> {
    read_optional_file(path, label)?
        .ok_or_else(|| CdfError::data(format!("{label} {} is missing", path.display())))
}

fn read_optional_file(path: &Path, label: &str) -> Result<Option<Vec<u8>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(CdfError::environment(format!(
                "inspect {label} {}: {error}",
                path.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CdfError::data(format!(
            "{label} {} must be a regular non-symlink file",
            path.display()
        )));
    }
    fs::read(path)
        .map(Some)
        .map_err(|error| CdfError::environment(format!("read {label} {}: {error}", path.display())))
}

fn read_required_file_bounded(path: &Path, label: &str, max_bytes: usize) -> Result<Vec<u8>> {
    read_optional_file_bounded(path, label, max_bytes)?
        .ok_or_else(|| CdfError::data(format!("{label} {} is missing", path.display())))
}

fn read_optional_file_bounded(
    path: &Path,
    label: &str,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(CdfError::environment(format!(
                "inspect {label} {}: {error}",
                path.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CdfError::data(format!(
            "{label} {} must be a regular non-symlink file",
            path.display()
        )));
    }
    if metadata.len() > max_bytes as u64 {
        return Err(CdfError::data(format!(
            "{label} {} exceeds the {max_bytes}-byte read bound",
            path.display()
        )));
    }
    let file = fs::File::open(path).map_err(|error| {
        CdfError::environment(format!("read {label} {}: {error}", path.display()))
    })?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            CdfError::environment(format!("read {label} {}: {error}", path.display()))
        })?;
    if bytes.len() > max_bytes {
        return Err(CdfError::data(format!(
            "{label} {} exceeds the {max_bytes}-byte read bound",
            path.display()
        )));
    }
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority() -> (ProjectConfig, EffectiveEnvironment) {
        let config = parse_cdf_toml(
            r#"
[project]
name = "index_test"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[sources.local]
type = "files"
root = "data"
"#,
        )
        .unwrap();
        let environment = config.effective_environment("dev").unwrap();
        (config, environment)
    }

    #[test]
    fn compilation_index_is_canonical_closed_and_hash_bound() {
        let (config, environment) = authority();
        let mut index = CompilationIndex::empty(&config, &environment).unwrap();
        index
            .record_failure(
                "local.events",
                "cdf/local/events.cdf.sql",
                Some(sha256(b"SELECT 1")),
                CompilationDiagnostic {
                    code: "CDF-COMPILE-RESOURCE".to_owned(),
                    kind: "contract".to_owned(),
                    message: "selected resource did not compile".to_owned(),
                },
            )
            .unwrap();
        let bytes = index.canonical_json_bytes().unwrap();
        assert_eq!(parse_compilation_index(&bytes).unwrap(), index);

        let mut unknown: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_owned(), serde_json::json!(true));
        assert!(parse_compilation_index(&serde_json::to_vec(&unknown).unwrap()).is_err());

        let mut tampered: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        tampered["resources"]["local.events"]["status"] = serde_json::json!("absent");
        assert!(parse_compilation_index(&serde_json::to_vec_pretty(&tampered).unwrap()).is_err());
    }

    #[test]
    fn artifact_paths_are_resource_and_hash_addressed() {
        let hash = sha256(b"artifact");
        assert_eq!(
            compiled_resource_artifact_path("local.events", &hash).unwrap(),
            format!(
                ".cdf/compiled/local.events@{}.json",
                hash.trim_start_matches("sha256:")
            )
        );
        assert!(compiled_resource_artifact_path("../events", &hash).is_err());
    }
}
