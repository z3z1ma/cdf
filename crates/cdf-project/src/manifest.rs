use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path},
};

use cdf_contract::RelationalExpressionPlan;
use cdf_kernel::{
    CanonicalArrowField, CanonicalArrowSchema, CdfError, DestinationSheetArtifact, ExecutionExtent,
    ResourceCapabilities, ResourceDescriptor, Result, SchemaHash, SemanticParameterValue,
};
use cdf_runtime::{CompiledSourceCompilerBinding, CompiledSourcePlan, CompiledStreamPolicy};
use cdf_semantic::{SemanticCatalog, SemanticDefinition};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest, Sha256};

use crate::{
    AuthoredResourceForm, CdfLock, CompiledProjectResource, ContractSnapshot, DependencyTuple,
    EffectiveEnvironment, EffectiveResourceEnvelope, LockedDestination, ProjectConfig,
    ProjectConfiguredSourceIdentity, parse_lock,
    semantic_uses::{compiled_fields, semantic_pins_for_resources},
};

pub const PROJECT_MANIFEST_VERSION: u16 = 1;
pub const PROJECT_MANIFEST_MAX_BYTES: usize = 64 * 1024 * 1024;
pub const PROJECT_MANIFEST_MAX_INPUTS: usize = 100_000;
pub const PROJECT_MANIFEST_MAX_RESOURCES: usize = 100_000;
pub const PROJECT_MANIFEST_MAX_FIELDS: usize = 1_000_000;
pub const PROJECT_MANIFEST_MAX_SEMANTICS: usize = 100_000;
pub const PROJECT_MANIFEST_MAX_LINEAGE_EDGES: usize = 2_000_000;
pub const PROJECT_MANIFEST_MAX_DIAGNOSTICS: usize = 100_000;
const MAX_MANIFEST_STRING_BYTES: usize = 1024 * 1024;

macro_rules! manifest_hash_type {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                validate_sha256(stringify!($name), &value, ManifestErrorAuthority::Compiler)?;
                Ok(Self(value))
            }

            #[allow(
                dead_code,
                reason = "typed artifact hashes share one closed serialization implementation"
            )]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                validate_sha256(stringify!($name), &value, ManifestErrorAuthority::Artifact)
                    .map_err(de::Error::custom)?;
                Ok(Self(value))
            }
        }
    };
}

manifest_hash_type!(ProjectManifestHash);
manifest_hash_type!(ManifestInputContentHash);
manifest_hash_type!(AuthoredInputSetHash);
manifest_hash_type!(ProjectLockContentHash);
manifest_hash_type!(ProjectLockSemanticHash);
manifest_hash_type!(ProjectLockBindingHash);
manifest_hash_type!(EnvironmentBindingHash);
manifest_hash_type!(DependencyTupleHash);
manifest_hash_type!(ResourceCompilationHash);
manifest_hash_type!(SemanticSnapshotHash);
manifest_hash_type!(SemanticProfileHash);
manifest_hash_type!(LineageHash);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectCompilationMode {
    ResourceArtifact,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectManifestHeader {
    pub project_name: String,
    pub environment: String,
    pub environment_binding_hash: EnvironmentBindingHash,
    pub compiler_version: String,
    pub dependency_tuple: DependencyTuple,
    pub dependency_tuple_hash: DependencyTupleHash,
    pub normalizer: String,
    pub lock_content_hash: ProjectLockContentHash,
    pub lock_semantic_hash: ProjectLockSemanticHash,
    pub compilation_mode: ProjectCompilationMode,
    pub compiler_policies: BTreeMap<String, String>,
    pub features: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectManifestHashes {
    pub authored_inputs: AuthoredInputSetHash,
    pub lock_binding: ProjectLockBindingHash,
    pub semantics: SemanticSnapshotHash,
    pub lineage: LineageHash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestInputKind {
    Project,
    ResourceSql,
    SemanticDefinition,
    GeneratedExpansion,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManifestInputLocation {
    ProjectRelativePath { path: String },
    TypedOrigin { origin: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManifestInputGeneration {
    Explicit,
    Generated {
        generator: String,
        generator_hash: ManifestInputContentHash,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledArtifactInput {
    pub input_id: String,
    pub input_kind: ManifestInputKind,
    pub location: ManifestInputLocation,
    pub content_hash: ManifestInputContentHash,
    pub parser: String,
    pub parser_version: u32,
    pub generation: ManifestInputGeneration,
}

impl CompiledArtifactInput {
    pub fn explicit_file(
        path: impl Into<String>,
        input_kind: ManifestInputKind,
        bytes: &[u8],
        parser: impl Into<String>,
        parser_version: u32,
    ) -> Result<Self> {
        let path = path.into();
        let parser = parser.into();
        validate_relative_manifest_path(&path, ManifestErrorAuthority::Compiler)?;
        validate_token(
            "manifest input parser",
            &parser,
            ManifestErrorAuthority::Compiler,
        )?;
        Ok(Self {
            input_id: path.clone(),
            input_kind,
            location: ManifestInputLocation::ProjectRelativePath { path },
            content_hash: ManifestInputContentHash::new(bytes_hash(bytes))?,
            parser,
            parser_version,
            generation: ManifestInputGeneration::Explicit,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestResourceOrigin {
    pub relative_path: String,
    pub namespace: String,
    pub resource_name: String,
    pub default_target: String,
    pub authored_form: AuthoredResourceForm,
    pub authored_sql: String,
    pub authored_content_hash: String,
    pub authored_ast_hash: String,
    pub authored_input_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestField {
    pub ordinal: u32,
    pub path: String,
    pub field: CanonicalArrowField,
    pub semantic_reference: Option<String>,
    pub semantic_definition_hash: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestDestinationBinding {
    pub destination_id: String,
    pub sheet_hash: String,
    pub sheet: DestinationSheetArtifact,
    pub target: Option<String>,
    pub compiled_plan_hash: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestResource {
    pub resource_id: String,
    pub compilation_hash: ResourceCompilationHash,
    pub origin: ManifestResourceOrigin,
    pub configured_source: ProjectConfiguredSourceIdentity,
    pub canonical_arguments_hash: String,
    pub source_node_id: String,
    pub effective: EffectiveResourceEnvelope,
    pub relational_plan: RelationalExpressionPlan,
    pub descriptor: ResourceDescriptor,
    pub capabilities: ResourceCapabilities,
    pub execution_extent: ExecutionExtent,
    pub compiled_stream_policy: Option<CompiledStreamPolicy>,
    pub source_plan: CompiledSourcePlan,
    pub source_binding: CompiledSourceCompilerBinding,
    pub output_schema: CanonicalArrowSchema,
    pub output_schema_hash: SchemaHash,
    pub contract: Option<ContractSnapshot>,
    pub destination: ManifestDestinationBinding,
    pub fields: Vec<ManifestField>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManifestSemanticSource {
    BuiltIn,
    Adapter { adapter_id: String },
    Project,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestSemanticFieldUsage {
    pub resource_id: String,
    pub field_ordinal: u32,
    pub field_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestSemanticReferenceUsage {
    pub reference: String,
    pub normalized_parameters: BTreeMap<String, SemanticParameterValue>,
    pub fields: Vec<ManifestSemanticFieldUsage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestSemanticDefinition {
    pub definition_id: String,
    pub definition_hash: String,
    pub source: ManifestSemanticSource,
    pub definition: SemanticDefinition,
    pub compatibility_profile_hash: SemanticProfileHash,
    pub privacy_profile_hash: SemanticProfileHash,
    pub destination_mapping_profile_hash: SemanticProfileHash,
    pub references: Vec<ManifestSemanticReferenceUsage>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManifestLineageNode {
    Input { input_id: String },
    Source { source_name: String },
    Resource { resource_id: String },
    Field { resource_id: String, ordinal: u32 },
    Semantic { definition_id: String },
    Contract { contract_ref: String },
    Destination { destination_id: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestLineageKind {
    AuthoredBy,
    ReadsFrom,
    DirectField,
    SemanticBinding,
    ContractApplies,
    WritesTo,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestLineageEdge {
    pub edge_id: String,
    pub from: ManifestLineageNode,
    pub to: ManifestLineageNode,
    pub relation: ManifestLineageKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestDiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestDiagnostic {
    pub severity: ManifestDiagnosticSeverity,
    pub code: String,
    pub resource_id: Option<String>,
    pub input_id: Option<String>,
    pub message: String,
    pub remediation: Option<String>,
    pub authority: String,
    pub blocks_execution: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectManifest {
    pub version: u16,
    pub manifest_hash: ProjectManifestHash,
    pub generated_at_unix_ms: Option<i64>,
    pub header: ProjectManifestHeader,
    pub hashes: ProjectManifestHashes,
    pub inputs: Vec<CompiledArtifactInput>,
    pub resources: Vec<ManifestResource>,
    pub semantics: Vec<ManifestSemanticDefinition>,
    pub lineage: Vec<ManifestLineageEdge>,
    pub diagnostics: Vec<ManifestDiagnostic>,
}

pub struct ProjectManifestCompileRequest<'a> {
    pub config: &'a ProjectConfig,
    pub environment: &'a EffectiveEnvironment,
    pub lock: &'a CdfLock,
    pub lock_bytes: &'a [u8],
    pub resources: &'a [CompiledProjectResource],
    pub authored_inputs: Vec<CompiledArtifactInput>,
    pub semantic_catalog: &'a SemanticCatalog,
    pub semantic_sources: BTreeMap<String, ManifestSemanticSource>,
    pub selected_destination_id: &'a str,
    pub compilation_mode: ProjectCompilationMode,
    pub generated_at_unix_ms: Option<i64>,
    pub diagnostics: Vec<ManifestDiagnostic>,
}

#[derive(Serialize)]
struct ManifestIdentity<'a> {
    version: u16,
    header: &'a ProjectManifestHeader,
    hashes: &'a ProjectManifestHashes,
    inputs: &'a [CompiledArtifactInput],
    resources: &'a [ManifestResource],
    semantics: &'a [ManifestSemanticDefinition],
    lineage: &'a [ManifestLineageEdge],
    diagnostics: &'a [ManifestDiagnostic],
}

#[derive(Serialize)]
struct ResourceExecutionIdentity<'a> {
    descriptor: &'a ResourceDescriptor,
    capabilities: &'a ResourceCapabilities,
    execution_extent: &'a ExecutionExtent,
    compiled_stream_policy: &'a Option<CompiledStreamPolicy>,
    source_binding: &'a CompiledSourceCompilerBinding,
    redacted_options_hash: &'a str,
    output_schema_hash: &'a SchemaHash,
    contract: &'a Option<ContractSnapshot>,
    destination_sheet_hash: &'a str,
    configured_source: &'a ProjectConfiguredSourceIdentity,
    canonical_arguments_hash: &'a str,
    source_node_id: &'a str,
    effective_target: &'a cdf_kernel::TargetName,
    effective_disposition: &'a cdf_kernel::WriteDisposition,
    effective_merge_keys: &'a [String],
    effective_cursor: &'a Option<String>,
    effective_trust: &'a cdf_kernel::TrustLevel,
    effective_semantics: &'a BTreeMap<String, String>,
    effective_execution: &'a ExecutionExtent,
    relational_plan: &'a RelationalExpressionPlan,
}

#[derive(Clone, Copy)]
enum ManifestErrorAuthority {
    Compiler,
    Artifact,
}

pub fn compile_project_manifest(
    request: ProjectManifestCompileRequest<'_>,
) -> Result<ProjectManifest> {
    validate_compile_authority(&request)?;
    let mut inputs = request.authored_inputs;
    inputs.sort_by(|left, right| left.input_id.cmp(&right.input_id));
    validate_inputs(&inputs, ManifestErrorAuthority::Compiler)?;
    let bare_resources = request
        .resources
        .iter()
        .map(|entry| entry.resource.clone())
        .collect::<Vec<_>>();
    let fields = compiled_fields(&bare_resources, request.semantic_catalog)?;
    let semantics = compile_semantic_snapshot(&fields, &request.semantic_sources)?;

    let mut resources = Vec::with_capacity(request.resources.len());
    for entry in request.resources {
        let resource_id = entry.resource.descriptor().resource_id.as_str();
        let selected_destination = selected_destination(
            request.environment,
            request.lock,
            resource_id,
            request.selected_destination_id,
        )?;
        let resource_fields = fields
            .iter()
            .filter(|field| field.resource_id == entry.resource.descriptor().resource_id.as_str())
            .map(|field| ManifestField {
                ordinal: field.field_ordinal,
                path: field.field_path.clone(),
                field: field.field.clone(),
                semantic_reference: field
                    .semantic
                    .as_ref()
                    .map(|semantic| semantic.reference().to_string()),
                semantic_definition_hash: field
                    .semantic
                    .as_ref()
                    .map(|semantic| semantic.definition_hash().to_owned()),
            })
            .collect::<Vec<_>>();
        resources.push(compile_manifest_resource(
            entry,
            resource_fields,
            &inputs,
            selected_destination,
            request.lock,
        )?);
    }
    resources.sort_by(|left, right| left.resource_id.cmp(&right.resource_id));
    let lineage = compile_lineage(&resources)?;
    let mut diagnostics = request.diagnostics;
    sort_diagnostics(&mut diagnostics);

    let compiler = common_locked_compiler(request.lock)?;
    let dependency_tuple_hash =
        DependencyTupleHash::new(canonical_hash(&compiler.dependency_tuple)?)?;
    let lock_content_hash = ProjectLockContentHash::new(bytes_hash(request.lock_bytes))?;
    let lock_semantic_hash = ProjectLockSemanticHash::new(canonical_hash(request.lock)?)?;
    let environment_binding_hash =
        EnvironmentBindingHash::new(canonical_hash(request.environment)?)?;
    let header = ProjectManifestHeader {
        project_name: request.config.project.name.clone(),
        environment: request.environment.name.clone(),
        environment_binding_hash,
        compiler_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_tuple: compiler.dependency_tuple.clone(),
        dependency_tuple_hash,
        normalizer: request.config.project.normalizer.clone(),
        lock_content_hash,
        lock_semantic_hash,
        compilation_mode: request.compilation_mode,
        compiler_policies: BTreeMap::from([
            (
                "manifest".to_owned(),
                "compiled-resource-artifact-v1".to_owned(),
            ),
            (
                "source_plan".to_owned(),
                "compiled-source-plan-v1".to_owned(),
            ),
        ]),
        features: BTreeSet::from(["semantic_registry_snapshot".to_owned()]),
    };
    let hashes = ProjectManifestHashes {
        authored_inputs: AuthoredInputSetHash::new(canonical_hash(&inputs)?)?,
        lock_binding: ProjectLockBindingHash::new(canonical_hash(&(
            &header.environment,
            &header.environment_binding_hash,
            &header.dependency_tuple_hash,
            &header.lock_content_hash,
            &header.lock_semantic_hash,
            &header.normalizer,
        ))?)?,
        semantics: SemanticSnapshotHash::new(canonical_hash(&semantics)?)?,
        lineage: LineageHash::new(canonical_hash(&lineage)?)?,
    };
    let mut manifest = ProjectManifest {
        version: PROJECT_MANIFEST_VERSION,
        manifest_hash: ProjectManifestHash::new(bytes_hash(&[]))?,
        generated_at_unix_ms: request.generated_at_unix_ms,
        header,
        hashes,
        inputs,
        resources,
        semantics,
        lineage,
        diagnostics,
    };
    manifest.manifest_hash = ProjectManifestHash::new(manifest.identity_hash()?)?;
    manifest.validate_with(ManifestErrorAuthority::Compiler)?;
    Ok(manifest)
}

impl ProjectManifest {
    fn identity_hash(&self) -> Result<String> {
        canonical_hash(&ManifestIdentity {
            version: self.version,
            header: &self.header,
            hashes: &self.hashes,
            inputs: &self.inputs,
            resources: &self.resources,
            semantics: &self.semantics,
            lineage: &self.lineage,
            diagnostics: &self.diagnostics,
        })
    }

    fn validate_with(&self, authority: ManifestErrorAuthority) -> Result<()> {
        if self.version != PROJECT_MANIFEST_VERSION {
            return manifest_error(
                authority,
                format!(
                    "unsupported project manifest version {}; expected {PROJECT_MANIFEST_VERSION}",
                    self.version
                ),
            );
        }
        check_bound(
            "inputs",
            self.inputs.len(),
            PROJECT_MANIFEST_MAX_INPUTS,
            authority,
        )?;
        check_bound(
            "resources",
            self.resources.len(),
            PROJECT_MANIFEST_MAX_RESOURCES,
            authority,
        )?;
        check_bound(
            "semantics",
            self.semantics.len(),
            PROJECT_MANIFEST_MAX_SEMANTICS,
            authority,
        )?;
        check_bound(
            "lineage edges",
            self.lineage.len(),
            PROJECT_MANIFEST_MAX_LINEAGE_EDGES,
            authority,
        )?;
        check_bound(
            "diagnostics",
            self.diagnostics.len(),
            PROJECT_MANIFEST_MAX_DIAGNOSTICS,
            authority,
        )?;
        validate_inputs(&self.inputs, authority)?;
        validate_sorted_unique(
            &self.resources,
            |resource| resource.resource_id.as_str(),
            "resource id",
            authority,
        )?;
        let field_count = self
            .resources
            .iter()
            .map(|resource| resource.fields.len())
            .sum();
        check_bound(
            "fields",
            field_count,
            PROJECT_MANIFEST_MAX_FIELDS,
            authority,
        )?;
        for resource in &self.resources {
            validate_resource(resource, &self.inputs, authority)?;
        }
        validate_semantics(&self.semantics, &self.resources, authority)?;
        validate_lineage(
            &self.lineage,
            &self.inputs,
            &self.resources,
            &self.semantics,
            authority,
        )?;
        validate_diagnostics(&self.diagnostics, authority)?;
        validate_security(self, authority)?;
        let expected_hashes = ProjectManifestHashes {
            authored_inputs: AuthoredInputSetHash::new(canonical_hash(&self.inputs)?)?,
            lock_binding: ProjectLockBindingHash::new(canonical_hash(&(
                &self.header.environment,
                &self.header.environment_binding_hash,
                &self.header.dependency_tuple_hash,
                &self.header.lock_content_hash,
                &self.header.lock_semantic_hash,
                &self.header.normalizer,
            ))?)?,
            semantics: SemanticSnapshotHash::new(canonical_hash(&self.semantics)?)?,
            lineage: LineageHash::new(canonical_hash(&self.lineage)?)?,
        };
        if self.hashes != expected_hashes {
            return manifest_error(
                authority,
                "project manifest layered hashes do not match their canonical sections",
            );
        }
        if self.header.dependency_tuple_hash.as_str()
            != canonical_hash(&self.header.dependency_tuple)?
        {
            return manifest_error(
                authority,
                "project manifest dependency tuple hash is inconsistent",
            );
        }
        if self.manifest_hash.as_str() != self.identity_hash()? {
            return manifest_error(
                authority,
                "project manifest hash does not match its canonical semantic content",
            );
        }
        Ok(())
    }
}

fn validate_compile_authority(request: &ProjectManifestCompileRequest<'_>) -> Result<()> {
    if request.config.project.name != request.lock.project.name
        || request.config.project.default_environment != request.lock.project.default_environment
        || request
            .lock
            .resources
            .values()
            .any(|resource| resource.compiler.normalizer != request.config.project.normalizer)
    {
        return Err(CdfError::contract(
            "cdf.lock project authority is stale for the project configuration",
        ));
    }
    if request.environment
        != &request
            .config
            .effective_environment(&request.environment.name)?
    {
        return Err(CdfError::contract(
            "manifest compile environment is stale for the project configuration",
        ));
    }
    let parsed_lock = std::str::from_utf8(request.lock_bytes)
        .map_err(|error| CdfError::contract(format!("cdf.lock is not UTF-8: {error}")))
        .and_then(parse_lock)?;
    if &parsed_lock != request.lock {
        return Err(CdfError::contract(
            "manifest compile lock bytes do not encode the supplied typed cdf.lock",
        ));
    }
    let bare_resources = request
        .resources
        .iter()
        .map(|entry| entry.resource.clone())
        .collect::<Vec<_>>();
    let compiled_ids = bare_resources
        .iter()
        .map(|resource| resource.descriptor().resource_id.to_string())
        .collect::<BTreeSet<_>>();
    let locked_ids = request
        .lock
        .resources
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if compiled_ids != locked_ids {
        return Err(CdfError::contract(
            "cdf.lock resource set is stale for the compiled project",
        ));
    }
    for resource in &bare_resources {
        let resource_id = resource.descriptor().resource_id.as_str();
        let locked = &request.lock.resources[resource_id];
        let pins =
            semantic_pins_for_resources(std::slice::from_ref(resource), request.semantic_catalog)?;
        let stream = (!resource.execution_extent().is_bounded())
            .then(|| {
                cdf_runtime::CompiledStreamPolicy::compile(
                    resource.execution_extent(),
                    resource.source_plan(),
                )
            })
            .transpose()?;
        if locked.descriptor != *resource.descriptor()
            || locked.capabilities != *resource.capabilities()
            || locked.execution_extent != *resource.execution_extent()
            || locked.compiled_stream_policy != stream
            || locked.semantic_pins != pins
        {
            return Err(CdfError::contract(format!(
                "cdf.lock resource `{resource_id}` is stale for the compiled plan"
            )));
        }
    }
    for resource in &bare_resources {
        selected_destination(
            request.environment,
            request.lock,
            resource.descriptor().resource_id.as_str(),
            request.selected_destination_id,
        )?;
    }
    Ok(())
}

fn common_locked_compiler(lock: &CdfLock) -> Result<&crate::LockedResourceCompilerBinding> {
    let mut resources = lock.resources.values();
    let compiler = resources
        .next()
        .map(|resource| &resource.compiler)
        .ok_or_else(|| CdfError::contract("compiled authority contains no resource bindings"))?;
    if resources.any(|resource| {
        resource.compiler.dependency_tuple != compiler.dependency_tuple
            || resource.compiler.normalizer != compiler.normalizer
    }) {
        return Err(CdfError::contract(
            "compiled resources do not share one compiler dependency authority",
        ));
    }
    Ok(compiler)
}

fn selected_destination<'a>(
    environment: &EffectiveEnvironment,
    lock: &'a CdfLock,
    resource_id: &str,
    destination_id: &str,
) -> Result<(&'a str, &'a LockedDestination)> {
    lock.resources
        .get(resource_id)
        .and_then(|resource| resource.destinations.get_key_value(destination_id))
        .filter(|(id, destination)| destination.sheet.destination.as_str() == id.as_str())
        .map(|(id, destination)| (id.as_str(), destination))
        .ok_or_else(|| {
            CdfError::contract(format!(
                "cdf.lock has no canonical destination sheet `{destination_id}` for selected URI `{}`",
                environment.destination
            ))
        })
}

fn compile_manifest_resource(
    entry: &CompiledProjectResource,
    fields: Vec<ManifestField>,
    inputs: &[CompiledArtifactInput],
    selected_destination: (&str, &LockedDestination),
    lock: &CdfLock,
) -> Result<ManifestResource> {
    let resource = &entry.resource;
    let resource_id = resource.descriptor().resource_id.to_string();
    let locked = lock.resources.get(&resource_id).ok_or_else(|| {
        CdfError::contract(format!("cdf.lock is missing resource `{resource_id}`"))
    })?;
    let source_binding = CompiledSourceCompilerBinding::compile(resource.source_plan())?;
    let output_schema = CanonicalArrowSchema::from_arrow(resource.schema().as_ref())?;
    let output_schema_hash = cdf_kernel::canonical_arrow_schema_hash(resource.schema().as_ref())?;
    let destination = ManifestDestinationBinding {
        destination_id: selected_destination.0.to_owned(),
        sheet_hash: selected_destination.1.sheet_hash.clone(),
        sheet: selected_destination.1.sheet_artifact()?,
        target: Some(entry.query.effective.target.value.to_string()),
        compiled_plan_hash: None,
    };
    let origin = compile_origin(entry, inputs)?;
    let relational_plan = entry.query.relational_plan.clone().ok_or_else(|| {
        CdfError::contract(format!(
            "[CDF-SCHEMA-UNRESOLVED] resource {resource_id:?} has no finalized relational plan; run `cdf compile {resource_id}`"
        ))
    })?;
    let compiled_stream_policy = (!resource.execution_extent().is_bounded())
        .then(|| {
            cdf_runtime::CompiledStreamPolicy::compile(
                resource.execution_extent(),
                resource.source_plan(),
            )
        })
        .transpose()?;
    let mut manifest = ManifestResource {
        resource_id,
        compilation_hash: ResourceCompilationHash::new(bytes_hash(&[]))?,
        origin,
        configured_source: entry.query.configured_source.clone(),
        canonical_arguments_hash: entry
            .query
            .parsed_query
            .upstream
            .canonical_arguments_hash
            .clone(),
        source_node_id: entry.query.source_node_id.clone(),
        effective: entry.query.effective.clone(),
        relational_plan,
        descriptor: resource.descriptor().clone(),
        capabilities: resource.capabilities().clone(),
        execution_extent: resource.execution_extent().clone(),
        compiled_stream_policy,
        source_plan: resource.source_plan().clone(),
        source_binding,
        output_schema,
        output_schema_hash,
        contract: locked.contract.clone(),
        destination,
        fields,
    };
    manifest.compilation_hash = ResourceCompilationHash::new(resource_hash(&manifest)?)?;
    Ok(manifest)
}

fn compile_origin(
    entry: &CompiledProjectResource,
    inputs: &[CompiledArtifactInput],
) -> Result<ManifestResourceOrigin> {
    let query = &entry.query;
    validate_relative_manifest_path(&query.relative_path, ManifestErrorAuthority::Compiler)?;
    let input = inputs
        .iter()
        .find(|input| {
            matches!(
                &input.location,
                ManifestInputLocation::ProjectRelativePath { path } if path == &query.relative_path
            )
        })
        .ok_or_else(|| {
            CdfError::contract(format!(
                "compiled resource origin {:?} has no authored manifest input",
                query.relative_path
            ))
        })?;
    Ok(ManifestResourceOrigin {
        relative_path: query.relative_path.clone(),
        namespace: query.namespace.clone(),
        resource_name: query.resource_name.clone(),
        default_target: query.default_target.to_string(),
        authored_form: query.authored_form,
        authored_sql: query.authored_sql.clone(),
        authored_content_hash: query.authored_content_hash.clone(),
        authored_ast_hash: query.parsed_query.authored_ast_hash.clone(),
        authored_input_ids: vec![input.input_id.clone()],
    })
}

fn compile_semantic_snapshot(
    fields: &[crate::semantic_uses::CompiledField],
    semantic_sources: &BTreeMap<String, ManifestSemanticSource>,
) -> Result<Vec<ManifestSemanticDefinition>> {
    let builtin_ids = SemanticCatalog::builtins()?
        .definitions()
        .map(|registered| definition_id(&registered.definition))
        .collect::<BTreeSet<_>>();
    let mut by_definition = BTreeMap::<String, ManifestSemanticDefinition>::new();
    for field in fields {
        let Some(resolved) = &field.semantic else {
            continue;
        };
        let id = definition_id(resolved.definition());
        if !by_definition.contains_key(&id) {
            let source = if builtin_ids.contains(&id) {
                if semantic_sources.contains_key(&id) {
                    return Err(CdfError::contract(format!(
                        "built-in semantic definition `{id}` cannot override its source provenance"
                    )));
                }
                ManifestSemanticSource::BuiltIn
            } else {
                semantic_sources.get(&id).cloned().ok_or_else(|| {
                    CdfError::contract(format!(
                        "semantic definition `{id}` requires explicit project or adapter provenance"
                    ))
                })?
            };
            by_definition.insert(
                id.clone(),
                ManifestSemanticDefinition {
                    definition_id: id.clone(),
                    definition_hash: resolved.definition_hash().to_owned(),
                    source,
                    definition: resolved.definition().clone(),
                    compatibility_profile_hash: SemanticProfileHash::new(
                        semantic_compatibility_hash(resolved.definition())?,
                    )?,
                    privacy_profile_hash: SemanticProfileHash::new(canonical_hash(
                        &resolved.definition().privacy,
                    )?)?,
                    destination_mapping_profile_hash: SemanticProfileHash::new(canonical_hash(
                        &resolved.definition().destination_mappings,
                    )?)?,
                    references: Vec::new(),
                },
            );
        }
        let entry = by_definition
            .get_mut(&id)
            .ok_or_else(|| CdfError::internal("semantic definition insertion failed"))?;
        let reference = resolved.reference().to_string();
        let usage = match entry
            .references
            .iter_mut()
            .find(|usage| usage.reference == reference)
        {
            Some(usage) => usage,
            None => {
                entry.references.push(ManifestSemanticReferenceUsage {
                    reference: reference.clone(),
                    normalized_parameters: resolved.reference().parameters().clone(),
                    fields: Vec::new(),
                });
                entry
                    .references
                    .last_mut()
                    .ok_or_else(|| CdfError::internal("semantic reference insertion failed"))?
            }
        };
        usage.fields.push(ManifestSemanticFieldUsage {
            resource_id: field.resource_id.clone(),
            field_ordinal: field.field_ordinal,
            field_path: field.field_path.clone(),
        });
    }
    let mut semantics = by_definition.into_values().collect::<Vec<_>>();
    for semantic in &mut semantics {
        semantic
            .references
            .sort_by(|left, right| left.reference.cmp(&right.reference));
        for usage in &mut semantic.references {
            usage.fields.sort();
        }
    }
    Ok(semantics)
}

fn semantic_compatibility_hash(definition: &SemanticDefinition) -> Result<String> {
    canonical_hash(&(
        &definition.arrow_patterns,
        &definition.nullability,
        &definition.required_metadata,
        &definition.validation,
        definition.base_arrow_fallback,
    ))
}

fn compile_lineage(resources: &[ManifestResource]) -> Result<Vec<ManifestLineageEdge>> {
    let mut edges = Vec::new();
    for resource in resources {
        for input_id in &resource.origin.authored_input_ids {
            push_lineage(
                &mut edges,
                ManifestLineageNode::Input {
                    input_id: input_id.clone(),
                },
                ManifestLineageNode::Resource {
                    resource_id: resource.resource_id.clone(),
                },
                ManifestLineageKind::AuthoredBy,
            )?;
        }
        push_lineage(
            &mut edges,
            ManifestLineageNode::Source {
                source_name: resource.configured_source.configured_source.clone(),
            },
            ManifestLineageNode::Resource {
                resource_id: resource.resource_id.clone(),
            },
            ManifestLineageKind::ReadsFrom,
        )?;
        for field in &resource.fields {
            let field_node = ManifestLineageNode::Field {
                resource_id: resource.resource_id.clone(),
                ordinal: field.ordinal,
            };
            push_lineage(
                &mut edges,
                ManifestLineageNode::Resource {
                    resource_id: resource.resource_id.clone(),
                },
                field_node.clone(),
                ManifestLineageKind::DirectField,
            )?;
            if let Some(reference) = &field.semantic_reference {
                let reference = reference
                    .parse::<cdf_kernel::SemanticReference>()
                    .map_err(|error| {
                        CdfError::internal(format!(
                            "compiled semantic reference is invalid during lineage assembly: {error}"
                        ))
                    })?;
                push_lineage(
                    &mut edges,
                    ManifestLineageNode::Semantic {
                        definition_id: format!(
                            "{}.{}@{}",
                            reference.namespace(),
                            reference.name(),
                            reference.version()
                        ),
                    },
                    field_node.clone(),
                    ManifestLineageKind::SemanticBinding,
                )?;
            }
            if let Some(contract) = &resource.descriptor.contract {
                push_lineage(
                    &mut edges,
                    ManifestLineageNode::Contract {
                        contract_ref: contract.to_string(),
                    },
                    field_node,
                    ManifestLineageKind::ContractApplies,
                )?;
            }
        }
        push_lineage(
            &mut edges,
            ManifestLineageNode::Resource {
                resource_id: resource.resource_id.clone(),
            },
            ManifestLineageNode::Destination {
                destination_id: resource.destination.destination_id.clone(),
            },
            ManifestLineageKind::WritesTo,
        )?;
    }
    edges.sort_by(|left, right| left.edge_id.cmp(&right.edge_id));
    Ok(edges)
}

fn push_lineage(
    edges: &mut Vec<ManifestLineageEdge>,
    from: ManifestLineageNode,
    to: ManifestLineageNode,
    relation: ManifestLineageKind,
) -> Result<()> {
    let edge_id = canonical_hash(&(&from, &to, relation))?;
    edges.push(ManifestLineageEdge {
        edge_id,
        from,
        to,
        relation,
    });
    Ok(())
}

fn validate_resource(
    resource: &ManifestResource,
    inputs: &[CompiledArtifactInput],
    authority: ManifestErrorAuthority,
) -> Result<()> {
    let expected_path = format!(
        "cdf/{}/{}.cdf.sql",
        resource.origin.namespace, resource.origin.resource_name
    );
    let expected_resource_id = format!(
        "{}.{}",
        resource.origin.namespace, resource.origin.resource_name
    );
    if resource.origin.relative_path != expected_path
        || resource.resource_id != expected_resource_id
        || resource.origin.default_target != expected_resource_id
        || resource.origin.authored_content_hash
            != bytes_hash(resource.origin.authored_sql.as_bytes())
    {
        return manifest_error(
            authority,
            format!(
                "manifest resource `{}` contains inconsistent path-derived or authored identity",
                resource.resource_id
            ),
        );
    }
    let authored_input = inputs
        .iter()
        .find(|input| input.input_id == resource.origin.relative_path)
        .ok_or_else(|| {
            remap(
                CdfError::contract(format!(
                    "manifest resource `{}` has no authored SQL input",
                    resource.resource_id
                )),
                authority,
            )
        })?;
    if authored_input.input_kind != ManifestInputKind::ResourceSql
        || authored_input.content_hash.as_str() != resource.origin.authored_content_hash
        || !resource
            .origin
            .authored_input_ids
            .contains(&authored_input.input_id)
    {
        return manifest_error(
            authority,
            format!(
                "manifest resource `{}` origin does not match its authored SQL input",
                resource.resource_id
            ),
        );
    }
    validate_sha256(
        "canonical resource arguments",
        &resource.canonical_arguments_hash,
        authority,
    )?;
    validate_sha256("source node", &resource.source_node_id, authority)?;
    resource
        .descriptor
        .validate()
        .map_err(|error| remap(error, authority))?;
    resource
        .capabilities
        .validate()
        .map_err(|error| remap(error, authority))?;
    resource
        .source_plan
        .validate()
        .map_err(|error| remap(error, authority))?;
    if resource.resource_id != resource.descriptor.resource_id.as_str()
        || resource.source_plan.descriptor != resource.descriptor
        || resource.source_plan.resource_capabilities != resource.capabilities
    {
        return manifest_error(
            authority,
            format!(
                "manifest resource `{}` contains inconsistent descriptor or capabilities",
                resource.resource_id
            ),
        );
    }
    let configured = &resource.configured_source;
    let source_driver = &resource.source_plan.driver;
    let expected_driver_hash =
        cdf_runtime::artifact_hash(source_driver).map_err(|error| remap(error, authority))?;
    let expected_source_node = cdf_runtime::artifact_hash(&(
        resource.resource_id.as_str(),
        configured.configured_source.as_str(),
        configured.effective_configuration_hash.as_str(),
        configured.driver_descriptor_hash.as_str(),
        resource.canonical_arguments_hash.as_str(),
    ))
    .map_err(|error| remap(error, authority))?;
    if configured.driver_id != source_driver.driver_id.as_str()
        || configured.driver_version != source_driver.driver_version
        || configured.driver_option_schema_hash != source_driver.option_schema_hash
        || configured.driver_descriptor_hash != expected_driver_hash
        || resource.source_node_id != expected_source_node
    {
        return manifest_error(
            authority,
            format!(
                "manifest resource `{}` contains inconsistent configured-source identity",
                resource.resource_id
            ),
        );
    }
    let descriptor_cursor = resource
        .descriptor
        .cursor
        .as_ref()
        .map(|cursor| cursor.field.as_str());
    if resource.effective.disposition.value != resource.descriptor.write_disposition
        || resource.effective.merge_keys.value != resource.descriptor.merge_key
        || resource.effective.cursor.value.as_deref() != descriptor_cursor
        || resource.effective.trust.value != resource.descriptor.trust_level
        || resource.effective.execution.value != resource.execution_extent
        || resource.destination.target.as_deref() != Some(resource.effective.target.value.as_str())
    {
        return manifest_error(
            authority,
            format!(
                "manifest resource `{}` contains inconsistent effective metadata",
                resource.resource_id
            ),
        );
    }
    let binding = CompiledSourceCompilerBinding::compile(&resource.source_plan)
        .map_err(|error| remap(error, authority))?;
    if binding != resource.source_binding {
        return manifest_error(
            authority,
            format!(
                "manifest resource `{}` source binding is inconsistent",
                resource.resource_id
            ),
        );
    }
    resource
        .relational_plan
        .validate_recorded()
        .map_err(|error| remap(error, authority))?;
    if CanonicalArrowSchema::from_arrow(&resource.source_plan.schema)
        .map_err(|error| remap(error, authority))?
        != resource.relational_plan.input_schema
        || resource.relational_plan.output_schema != resource.output_schema
        || resource
            .output_schema
            .to_arrow()
            .and_then(|schema| cdf_kernel::canonical_arrow_schema_hash(&schema))
            .map_err(|error| remap(error, authority))?
            != resource.output_schema_hash
    {
        return manifest_error(
            authority,
            format!(
                "manifest resource `{}` output schema is inconsistent",
                resource.resource_id
            ),
        );
    }
    for (expected, field) in resource.fields.iter().enumerate() {
        if usize::try_from(field.ordinal).ok() != Some(expected) {
            return manifest_error(
                authority,
                format!(
                    "manifest resource `{}` field ordinals must be contiguous and unique",
                    resource.resource_id
                ),
            );
        }
        if field.semantic_reference.is_some() != field.semantic_definition_hash.is_some() {
            return manifest_error(
                authority,
                format!(
                    "manifest resource `{}` field {} has an incomplete semantic binding",
                    resource.resource_id, field.ordinal
                ),
            );
        }
    }
    if resource.compilation_hash.as_str() != resource_hash(resource)? {
        return manifest_error(
            authority,
            format!(
                "manifest resource `{}` compilation hash is inconsistent",
                resource.resource_id
            ),
        );
    }
    Ok(())
}

fn resource_hash(resource: &ManifestResource) -> Result<String> {
    canonical_hash(&ResourceExecutionIdentity {
        descriptor: &resource.descriptor,
        capabilities: &resource.capabilities,
        execution_extent: &resource.execution_extent,
        compiled_stream_policy: &resource.compiled_stream_policy,
        source_binding: &resource.source_binding,
        redacted_options_hash: &resource.source_plan.redacted_options_hash,
        output_schema_hash: &resource.output_schema_hash,
        contract: &resource.contract,
        destination_sheet_hash: &resource.destination.sheet_hash,
        configured_source: &resource.configured_source,
        canonical_arguments_hash: &resource.canonical_arguments_hash,
        source_node_id: &resource.source_node_id,
        effective_target: &resource.effective.target.value,
        effective_disposition: &resource.effective.disposition.value,
        effective_merge_keys: &resource.effective.merge_keys.value,
        effective_cursor: &resource.effective.cursor.value,
        effective_trust: &resource.effective.trust.value,
        effective_semantics: &resource.effective.semantics.value,
        effective_execution: &resource.effective.execution.value,
        relational_plan: &resource.relational_plan,
    })
}

fn validate_semantics(
    semantics: &[ManifestSemanticDefinition],
    resources: &[ManifestResource],
    authority: ManifestErrorAuthority,
) -> Result<()> {
    validate_sorted_unique(
        semantics,
        |semantic| semantic.definition_id.as_str(),
        "semantic definition id",
        authority,
    )?;
    let catalog = SemanticCatalog::new(
        semantics
            .iter()
            .map(|semantic| semantic.definition.clone())
            .collect(),
    )
    .map_err(|error| remap(error, authority))?;
    let mut field_bindings = BTreeMap::new();
    for resource in resources {
        for field in &resource.fields {
            if let (Some(reference), Some(hash)) = (
                field.semantic_reference.as_ref(),
                field.semantic_definition_hash.as_ref(),
            ) {
                field_bindings.insert(
                    (
                        resource.resource_id.clone(),
                        field.ordinal,
                        field.path.clone(),
                    ),
                    (reference.clone(), hash.clone()),
                );
            }
        }
    }
    let mut observed = BTreeMap::new();
    for semantic in semantics {
        if definition_id(&semantic.definition) != semantic.definition_id {
            return manifest_error(authority, "semantic snapshot definition id is inconsistent");
        }
        let registered = catalog
            .definitions()
            .find(|registered| definition_id(&registered.definition) == semantic.definition_id)
            .ok_or_else(|| {
                manifest_owned_error(authority, "semantic snapshot definition is missing")
            })?;
        if registered.definition_hash != semantic.definition_hash {
            return manifest_error(
                authority,
                "semantic snapshot definition hash is inconsistent",
            );
        }
        if semantic.compatibility_profile_hash.as_str()
            != semantic_compatibility_hash(&semantic.definition)?
            || semantic.privacy_profile_hash.as_str()
                != canonical_hash(&semantic.definition.privacy)?
            || semantic.destination_mapping_profile_hash.as_str()
                != canonical_hash(&semantic.definition.destination_mappings)?
        {
            return manifest_error(authority, "semantic snapshot profile hash is inconsistent");
        }
        validate_sorted_unique(
            &semantic.references,
            |usage| usage.reference.as_str(),
            "semantic reference",
            authority,
        )?;
        for usage in &semantic.references {
            let reference = catalog
                .parse_reference(&usage.reference, cdf_semantic::SemanticAuthority::Compiled)
                .map_err(|error| remap(error, authority))?;
            let resolved = catalog
                .resolve_reference(&reference, cdf_semantic::SemanticAuthority::Compiled)
                .map_err(|error| remap(error, authority))?;
            if reference.parameters() != &usage.normalized_parameters
                || resolved.definition_hash() != semantic.definition_hash
            {
                return manifest_error(authority, "semantic reference snapshot is inconsistent");
            }
            if usage.fields.windows(2).any(|pair| pair[0] >= pair[1]) {
                return manifest_error(
                    authority,
                    "semantic field usages must be sorted and unique",
                );
            }
            for field in &usage.fields {
                let key = (
                    field.resource_id.clone(),
                    field.field_ordinal,
                    field.field_path.clone(),
                );
                let binding = field_bindings.get(&key).ok_or_else(|| {
                    manifest_owned_error(authority, "semantic usage references a missing field")
                })?;
                if binding != &(usage.reference.clone(), semantic.definition_hash.clone()) {
                    return manifest_error(
                        authority,
                        "semantic usage does not match its field binding",
                    );
                }
                if observed.insert(key, binding.clone()).is_some() {
                    return manifest_error(authority, "semantic field usage is duplicated");
                }
            }
        }
    }
    if observed != field_bindings {
        return manifest_error(
            authority,
            "semantic field bindings have dangling or missing snapshot usages",
        );
    }
    Ok(())
}

fn validate_lineage(
    lineage: &[ManifestLineageEdge],
    inputs: &[CompiledArtifactInput],
    resources: &[ManifestResource],
    semantics: &[ManifestSemanticDefinition],
    authority: ManifestErrorAuthority,
) -> Result<()> {
    validate_sorted_unique(
        lineage,
        |edge| edge.edge_id.as_str(),
        "lineage edge id",
        authority,
    )?;
    let input_ids = inputs
        .iter()
        .map(|input| input.input_id.as_str())
        .collect::<BTreeSet<_>>();
    let resource_ids = resources
        .iter()
        .map(|resource| resource.resource_id.as_str())
        .collect::<BTreeSet<_>>();
    let semantic_ids = semantics
        .iter()
        .map(|semantic| semantic.definition_id.as_str())
        .collect::<BTreeSet<_>>();
    for edge in lineage {
        if edge.edge_id != canonical_hash(&(&edge.from, &edge.to, edge.relation))? {
            return manifest_error(authority, "lineage edge id is inconsistent");
        }
        for node in [&edge.from, &edge.to] {
            let valid = match node {
                ManifestLineageNode::Input { input_id } => input_ids.contains(input_id.as_str()),
                ManifestLineageNode::Source { source_name } => resources
                    .iter()
                    .any(|resource| resource.configured_source.configured_source == *source_name),
                ManifestLineageNode::Resource { resource_id } => {
                    resource_ids.contains(resource_id.as_str())
                }
                ManifestLineageNode::Field {
                    resource_id,
                    ordinal,
                } => resources.iter().any(|resource| {
                    resource.resource_id == *resource_id
                        && resource
                            .fields
                            .iter()
                            .any(|field| field.ordinal == *ordinal)
                }),
                ManifestLineageNode::Semantic { definition_id } => {
                    semantic_ids.contains(definition_id.as_str())
                }
                ManifestLineageNode::Contract { contract_ref } => {
                    resources.iter().any(|resource| {
                        resource
                            .descriptor
                            .contract
                            .as_ref()
                            .is_some_and(|contract| contract.as_str() == contract_ref)
                    })
                }
                ManifestLineageNode::Destination { destination_id } => resources
                    .iter()
                    .any(|resource| resource.destination.destination_id == *destination_id),
            };
            if !valid {
                return manifest_error(authority, "lineage edge contains a dangling node");
            }
        }
    }
    Ok(())
}

fn validate_inputs(
    inputs: &[CompiledArtifactInput],
    authority: ManifestErrorAuthority,
) -> Result<()> {
    validate_sorted_unique(
        inputs,
        |input| input.input_id.as_str(),
        "authored input id",
        authority,
    )?;
    let mut paths = BTreeSet::new();
    for input in inputs {
        validate_token("manifest input id", &input.input_id, authority)?;
        validate_token("manifest parser", &input.parser, authority)?;
        if input.parser_version == 0 {
            return manifest_error(authority, "manifest input parser version must be positive");
        }
        if let ManifestInputLocation::ProjectRelativePath { path } = &input.location {
            validate_relative_manifest_path(path, authority)?;
            if input.input_id != *path || !paths.insert(path) {
                return manifest_error(
                    authority,
                    "manifest authored paths must be unique and equal their input ids",
                );
            }
        }
    }
    if inputs
        .iter()
        .any(|input| input.input_kind == ManifestInputKind::Project)
    {
        return manifest_error(
            authority,
            "compiled resource artifacts must bind selected inputs, not the whole project file",
        );
    }
    Ok(())
}

fn validate_diagnostics(
    diagnostics: &[ManifestDiagnostic],
    authority: ManifestErrorAuthority,
) -> Result<()> {
    let mut expected = diagnostics.to_vec();
    sort_diagnostics(&mut expected);
    if expected != diagnostics {
        return manifest_error(authority, "manifest diagnostics must be canonically sorted");
    }
    for diagnostic in diagnostics {
        validate_token("manifest diagnostic code", &diagnostic.code, authority)?;
        validate_token(
            "manifest diagnostic authority",
            &diagnostic.authority,
            authority,
        )?;
        validate_token(
            "manifest diagnostic message",
            &diagnostic.message,
            authority,
        )?;
    }
    Ok(())
}

fn validate_security(manifest: &ProjectManifest, authority: ManifestErrorAuthority) -> Result<()> {
    let value = serde_json::to_value(manifest)
        .map_err(|error| CdfError::internal(format!("inspect manifest security: {error}")))?;
    inspect_json_security(&value, authority)
}

pub(crate) fn validate_compiled_artifact_sections(
    inputs: &[CompiledArtifactInput],
    resource: &ManifestResource,
    semantics: &[ManifestSemanticDefinition],
    lineage: &[ManifestLineageEdge],
    diagnostics: &[ManifestDiagnostic],
) -> Result<()> {
    let authority = ManifestErrorAuthority::Artifact;
    check_bound(
        "inputs",
        inputs.len(),
        PROJECT_MANIFEST_MAX_INPUTS,
        authority,
    )?;
    check_bound(
        "fields",
        resource.fields.len(),
        PROJECT_MANIFEST_MAX_FIELDS,
        authority,
    )?;
    check_bound(
        "semantics",
        semantics.len(),
        PROJECT_MANIFEST_MAX_SEMANTICS,
        authority,
    )?;
    check_bound(
        "lineage edges",
        lineage.len(),
        PROJECT_MANIFEST_MAX_LINEAGE_EDGES,
        authority,
    )?;
    check_bound(
        "diagnostics",
        diagnostics.len(),
        PROJECT_MANIFEST_MAX_DIAGNOSTICS,
        authority,
    )?;
    validate_inputs(inputs, authority)?;
    validate_resource(resource, inputs, authority)?;
    validate_semantics(semantics, std::slice::from_ref(resource), authority)?;
    validate_lineage(
        lineage,
        inputs,
        std::slice::from_ref(resource),
        semantics,
        authority,
    )?;
    validate_diagnostics(diagnostics, authority)
}

pub(crate) fn validate_compiled_artifact_security(value: &impl Serialize) -> Result<()> {
    let value = serde_json::to_value(value)
        .map_err(|error| CdfError::internal(format!("inspect artifact security: {error}")))?;
    inspect_json_security(&value, ManifestErrorAuthority::Artifact)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ManifestSecurityLocation {
    Root,
    Resources,
    Resource,
    ResourceOrigin,
    AuthoredSql,
    Other,
}

fn inspect_json_security(
    value: &serde_json::Value,
    authority: ManifestErrorAuthority,
) -> Result<()> {
    inspect_json_security_at(None, value, authority, ManifestSecurityLocation::Root)
}

fn inspect_json_security_at(
    key: Option<&str>,
    value: &serde_json::Value,
    authority: ManifestErrorAuthority,
    location: ManifestSecurityLocation,
) -> Result<()> {
    match value {
        serde_json::Value::String(value) => {
            let authored_sql = location == ManifestSecurityLocation::AuthoredSql;
            let forbidden_control = value.chars().any(|character| {
                character.is_control() && !(authored_sql && matches!(character, '\t' | '\n' | '\r'))
            });
            if value.len() > MAX_MANIFEST_STRING_BYTES || forbidden_control {
                return manifest_error(
                    authority,
                    "manifest string exceeds bounds or contains a forbidden control character",
                );
            }
            if let Some(key) = key {
                let sensitive = ["password", "token", "api_key", "secret", "credentials"]
                    .iter()
                    .any(|candidate| key.eq_ignore_ascii_case(candidate));
                if sensitive && value != "[REDACTED]" && !value.starts_with("secret://") {
                    return manifest_error(
                        authority,
                        format!(
                            "manifest field `{key}` contains a value where only redaction or a secret reference is allowed"
                        ),
                    );
                }
                let path_shaped = ["root", "source_file", "project_root", "local_path"]
                    .iter()
                    .any(|candidate| key.eq_ignore_ascii_case(candidate));
                if path_shaped && absolute_host_path(value) {
                    return manifest_error(
                        authority,
                        format!("manifest field `{key}` contains an absolute host path"),
                    );
                }
            }
            crate::internal::reject_plaintext_uri_credentials("manifest", value)
                .map_err(|error| remap(error, authority))?;
        }
        serde_json::Value::Array(values) => {
            let element_location = if location == ManifestSecurityLocation::Resources {
                ManifestSecurityLocation::Resource
            } else {
                ManifestSecurityLocation::Other
            };
            for value in values {
                inspect_json_security_at(key, value, authority, element_location)?;
            }
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                let child_location = match (location, key.as_str()) {
                    (ManifestSecurityLocation::Root, "resource") => {
                        ManifestSecurityLocation::Resource
                    }
                    (ManifestSecurityLocation::Root, "resources") => {
                        ManifestSecurityLocation::Resources
                    }
                    (ManifestSecurityLocation::Resource, "origin") => {
                        ManifestSecurityLocation::ResourceOrigin
                    }
                    (ManifestSecurityLocation::ResourceOrigin, "authored_sql") => {
                        ManifestSecurityLocation::AuthoredSql
                    }
                    _ => ManifestSecurityLocation::Other,
                };
                inspect_json_security_at(Some(key), value, authority, child_location)?;
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
    Ok(())
}

fn validate_relative_manifest_path(path: &str, authority: ManifestErrorAuthority) -> Result<()> {
    validate_token("project-relative manifest path", path, authority)?;
    if path.contains('\\') || Path::new(path).is_absolute() {
        return manifest_error(
            authority,
            format!("manifest path `{path}` is not project-relative canonical text"),
        );
    }
    if Path::new(path)
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return manifest_error(
            authority,
            format!("manifest path `{path}` contains a non-normal component"),
        );
    }
    Ok(())
}

fn absolute_host_path(value: &str) -> bool {
    Path::new(value).is_absolute()
        || value.as_bytes().get(1) == Some(&b':')
            && value
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphabetic)
}

fn sort_diagnostics(diagnostics: &mut [ManifestDiagnostic]) {
    diagnostics.sort_by(|left, right| {
        (
            left.severity,
            &left.code,
            &left.resource_id,
            &left.input_id,
            &left.message,
        )
            .cmp(&(
                right.severity,
                &right.code,
                &right.resource_id,
                &right.input_id,
                &right.message,
            ))
    });
}

fn validate_sorted_unique<T, F>(
    values: &[T],
    key: F,
    label: &str,
    authority: ManifestErrorAuthority,
) -> Result<()>
where
    F: Fn(&T) -> &str,
{
    if values.windows(2).any(|pair| key(&pair[0]) >= key(&pair[1])) {
        return manifest_error(
            authority,
            format!("manifest {label}s must be sorted and unique"),
        );
    }
    Ok(())
}

fn check_bound(
    label: &str,
    actual: usize,
    maximum: usize,
    authority: ManifestErrorAuthority,
) -> Result<()> {
    if actual > maximum {
        return manifest_error(
            authority,
            format!("manifest {label} count {actual} exceeds {maximum}"),
        );
    }
    Ok(())
}

fn validate_token(label: &str, value: &str, authority: ManifestErrorAuthority) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_MANIFEST_STRING_BYTES
        || value.chars().any(char::is_control)
    {
        return manifest_error(
            authority,
            format!("{label} must be non-empty, bounded, and control-free"),
        );
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str, authority: ManifestErrorAuthority) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return manifest_error(authority, format!("{label} must use sha256:<hex>"));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return manifest_error(
            authority,
            format!("{label} must contain 64 hexadecimal characters"),
        );
    }
    Ok(())
}

fn definition_id(definition: &SemanticDefinition) -> String {
    format!(
        "{}.{}@{}",
        definition.namespace, definition.name, definition.version
    )
}

fn canonical_hash<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        CdfError::internal(format!("serialize canonical manifest identity: {error}"))
    })?;
    Ok(bytes_hash(&bytes))
}

fn bytes_hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn manifest_error(authority: ManifestErrorAuthority, message: impl Into<String>) -> Result<()> {
    Err(manifest_owned_error(authority, message))
}

fn manifest_owned_error(authority: ManifestErrorAuthority, message: impl Into<String>) -> CdfError {
    match authority {
        ManifestErrorAuthority::Compiler => CdfError::internal(message),
        ManifestErrorAuthority::Artifact => CdfError::data(message),
    }
}

fn remap(error: CdfError, authority: ManifestErrorAuthority) -> CdfError {
    manifest_owned_error(authority, error.message)
}

#[cfg(test)]
mod security_tests {
    use cdf_kernel::ErrorKind;

    use super::{MAX_MANIFEST_STRING_BYTES, ManifestErrorAuthority, inspect_json_security};

    #[test]
    fn authored_sql_admits_only_safe_authored_whitespace_controls() {
        let safe = serde_json::json!({
            "resources": [{
                "origin": {
                    "authored_sql": "SELECT\t*\r\nFROM upstream(source => 'local');\n"
                }
            }]
        });
        inspect_json_security(&safe, ManifestErrorAuthority::Compiler).unwrap();
        inspect_json_security(&safe, ManifestErrorAuthority::Artifact).unwrap();

        for codepoint in (0_u32..=0x1f).chain(0x7f..=0x9f) {
            if matches!(codepoint, 0x09 | 0x0a | 0x0d) {
                continue;
            }
            let character = char::from_u32(codepoint).unwrap();
            let value = serde_json::json!({
                "resources": [{
                    "origin": { "authored_sql": format!("SELECT{character}1") }
                }]
            });
            let compiler_error =
                inspect_json_security(&value, ManifestErrorAuthority::Compiler).unwrap_err();
            assert_eq!(
                compiler_error.kind,
                ErrorKind::Internal,
                "U+{codepoint:04X}"
            );
            let artifact_error =
                inspect_json_security(&value, ManifestErrorAuthority::Artifact).unwrap_err();
            assert_eq!(artifact_error.kind, ErrorKind::Data, "U+{codepoint:04X}");
        }
    }

    #[test]
    fn adapter_owned_authored_sql_key_does_not_receive_the_typed_sql_exception() {
        let collision = serde_json::json!({
            "resources": [{
                "source_plan": {
                    "physical_plan": {
                        "authored_sql": "adapter-owned\nnot authored SQL"
                    }
                }
            }]
        });
        let error =
            inspect_json_security(&collision, ManifestErrorAuthority::Compiler).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Internal);
    }

    #[test]
    fn non_sql_strings_and_existing_security_fences_remain_strict() {
        for character in ['\t', '\n', '\r'] {
            let value = serde_json::json!({ "message": format!("unsafe{character}text") });
            let error =
                inspect_json_security(&value, ManifestErrorAuthority::Compiler).unwrap_err();
            assert_eq!(error.kind, ErrorKind::Internal);
        }

        for value in [
            serde_json::json!({ "password": "plaintext" }),
            serde_json::json!({ "local_path": "/private/host/path" }),
            serde_json::json!("x".repeat(MAX_MANIFEST_STRING_BYTES + 1)),
        ] {
            let error =
                inspect_json_security(&value, ManifestErrorAuthority::Compiler).unwrap_err();
            assert_eq!(error.kind, ErrorKind::Internal);
        }
    }
}
