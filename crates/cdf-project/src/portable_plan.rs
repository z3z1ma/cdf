use std::collections::BTreeSet;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use cdf_engine::EnginePlan;
use cdf_kernel::{CdfError, CheckpointId, PipelineId, Result, SourcePosition, TargetName};
use cdf_runtime::DestinationRuntimeCapabilities;
use serde::{Deserialize, Serialize};

use crate::{
    CdfLock, CompiledResourceArtifact, LockedResource, ProjectResourceSelection, lock_to_toml,
    parse_lock,
};

pub const PORTABLE_PLAN_VERSION: u16 = 1;
pub const PORTABLE_PLAN_MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortablePlanArtifact {
    pub version: u16,
    pub plan_hash: String,
    pub cdf_version: String,
    pub project: String,
    pub environment: String,
    pub environment_binding_hash: String,
    pub selection: ProjectResourceSelection,
    pub lock_precondition: PortableLockPrecondition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_lock: Option<CdfLock>,
    pub failure_policy: PortablePlanFailurePolicy,
    pub required_host: PortableHostRequirements,
    pub resources: Vec<PortablePlanResource>,
}

#[derive(Serialize)]
struct PortablePlanIdentity<'a> {
    version: u16,
    cdf_version: &'a str,
    project: &'a str,
    environment: &'a str,
    environment_binding_hash: &'a str,
    selection: &'a ProjectResourceSelection,
    lock_precondition: &'a PortableLockPrecondition,
    proposed_lock: &'a Option<CdfLock>,
    failure_policy: PortablePlanFailurePolicy,
    required_host: &'a PortableHostRequirements,
    resources: &'a [PortablePlanResource],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum PortableLockPrecondition {
    Absent,
    Present { content_sha256: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortablePlanFailurePolicy {
    ContinueIndependent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableHostRequirements {
    pub minimum_logical_cpu_slots: u16,
    pub minimum_io_workers: u16,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortablePlanResource {
    pub resource_id: String,
    pub schema_authority: PortableSchemaAuthority,
    pub compiled_resource: CompiledResourceArtifact,
    pub compiled_source_plan_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_task_set: Option<PortableTaskSetArtifact>,
    pub engine_plan: EnginePlan,
    pub destination: PortableDestinationBinding,
    pub pipeline_id: PipelineId,
    pub checkpoint_id: CheckpointId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_checkpoint_head: Option<SourcePosition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableTaskSetArtifact {
    pub reference: cdf_kernel::PlannedTaskSetReference,
    pub content_base64: String,
}

impl PortableTaskSetArtifact {
    pub fn new(reference: cdf_kernel::PlannedTaskSetReference, content: &[u8]) -> Result<Self> {
        let artifact = Self {
            reference,
            content_base64: BASE64.encode(content),
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn content(&self) -> Result<Vec<u8>> {
        BASE64.decode(&self.content_base64).map_err(|error| {
            CdfError::data(format!(
                "portable task-set content is not canonical base64: {error}"
            ))
        })
    }

    pub fn validate(&self) -> Result<()> {
        self.reference.validate()?;
        let content = self.content()?;
        if BASE64.encode(&content) != self.content_base64
            || u64::try_from(content.len()).unwrap_or(u64::MAX) != self.reference.byte_count
            || sha256(&content) != self.reference.content_sha256
        {
            return Err(CdfError::data(
                "portable task-set content differs from its planned reference",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum PortableSchemaAuthority {
    Locked {
        lock_binding: LockedResource,
    },
    ProposedFirstUse {
        lock_binding: LockedResource,
        artifacts: Vec<PortableInlineArtifact>,
    },
}

impl PortableSchemaAuthority {
    pub fn lock_binding(&self) -> &LockedResource {
        match self {
            Self::Locked { lock_binding } | Self::ProposedFirstUse { lock_binding, .. } => {
                lock_binding
            }
        }
    }

    pub fn is_proposed_first_use(&self) -> bool {
        matches!(self, Self::ProposedFirstUse { .. })
    }

    pub fn inline_artifacts(&self) -> &[PortableInlineArtifact] {
        match self {
            Self::Locked { .. } => &[],
            Self::ProposedFirstUse { artifacts, .. } => artifacts,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableInlineArtifact {
    pub path: String,
    pub byte_count: u64,
    pub content_sha256: String,
    pub content: String,
}

impl PortableInlineArtifact {
    pub fn new(path: impl Into<String>, bytes: Vec<u8>) -> Result<Self> {
        let path = path.into();
        let content = String::from_utf8(bytes).map_err(|error| {
            CdfError::data(format!(
                "portable plan inline artifact `{path}` is not UTF-8: {error}"
            ))
        })?;
        let byte_count = u64::try_from(content.len())
            .map_err(|_| CdfError::contract("portable inline artifact byte count overflow"))?;
        let content_sha256 = sha256(content.as_bytes());
        let artifact = Self {
            path,
            byte_count,
            content_sha256,
            content,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> Result<()> {
        validate_relative_artifact_path(&self.path)?;
        if self.byte_count != u64::try_from(self.content.len()).unwrap_or(u64::MAX)
            || self.content_sha256 != sha256(self.content.as_bytes())
        {
            return Err(CdfError::data(format!(
                "portable inline artifact `{}` does not match its byte/hash authority",
                self.path
            )));
        }
        validate_sha256("portable inline artifact", &self.content_sha256)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableDestinationBinding {
    pub uri: String,
    pub configuration_hash: String,
    pub destination_id: String,
    pub sheet_hash: String,
    pub sheet: cdf_kernel::DestinationSheetArtifact,
    pub runtime_capabilities: DestinationRuntimeCapabilities,
    pub target: TargetName,
}

impl PortableDestinationBinding {
    pub fn validate(&self) -> Result<()> {
        validate_sha256(
            "portable destination configuration",
            &self.configuration_hash,
        )?;
        validate_sha256("portable destination sheet", &self.sheet_hash)?;
        self.runtime_capabilities.validate()?;
        if cdf_runtime::artifact_hash(&self.sheet)? != self.sheet_hash {
            return Err(CdfError::data(
                "portable destination sheet hash does not match its typed authority",
            ));
        }
        if self.uri.is_empty() || self.uri.contains('@') {
            return Err(CdfError::data(
                "portable destination URI is empty or contains inline credentials",
            ));
        }
        Ok(())
    }
}

impl PortablePlanArtifact {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cdf_version: impl Into<String>,
        project: impl Into<String>,
        environment: impl Into<String>,
        environment_binding_hash: impl Into<String>,
        selection: ProjectResourceSelection,
        lock_precondition: PortableLockPrecondition,
        proposed_lock: Option<CdfLock>,
        resources: Vec<PortablePlanResource>,
    ) -> Result<Self> {
        let mut artifact = Self {
            version: PORTABLE_PLAN_VERSION,
            plan_hash: sha256(&[]),
            cdf_version: cdf_version.into(),
            project: project.into(),
            environment: environment.into(),
            environment_binding_hash: environment_binding_hash.into(),
            selection,
            lock_precondition,
            proposed_lock,
            failure_policy: PortablePlanFailurePolicy::ContinueIndependent,
            required_host: PortableHostRequirements {
                minimum_logical_cpu_slots: 1,
                minimum_io_workers: 1,
            },
            resources,
        };
        artifact.plan_hash = artifact.identity_hash()?;
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != PORTABLE_PLAN_VERSION {
            return Err(CdfError::data(format!(
                "unsupported portable plan version {}; expected {PORTABLE_PLAN_VERSION}",
                self.version
            )));
        }
        validate_sha256("portable plan", &self.plan_hash)?;
        validate_sha256(
            "portable plan environment binding",
            &self.environment_binding_hash,
        )?;
        if self.cdf_version.is_empty() || self.project.is_empty() || self.environment.is_empty() {
            return Err(CdfError::data(
                "portable plan project, environment, and CDF version must be non-empty",
            ));
        }
        match &self.lock_precondition {
            PortableLockPrecondition::Absent => {}
            PortableLockPrecondition::Present { content_sha256 } => {
                validate_sha256("portable plan lock precondition", content_sha256)?;
            }
        }
        if self.required_host.minimum_logical_cpu_slots == 0
            || self.required_host.minimum_io_workers == 0
        {
            return Err(CdfError::data(
                "portable plan host requirements must be nonzero",
            ));
        }
        if self.resources.is_empty() {
            return Err(CdfError::data(
                "portable plan must contain at least one resource",
            ));
        }
        let resource_ids = self
            .resources
            .iter()
            .map(|resource| resource.resource_id.as_str())
            .collect::<Vec<_>>();
        if resource_ids.windows(2).any(|pair| pair[0] >= pair[1])
            || self.selection.resolved
                != resource_ids
                    .iter()
                    .map(|id| (*id).to_owned())
                    .collect::<Vec<_>>()
        {
            return Err(CdfError::data(
                "portable plan resources must exactly match the canonical ordered selection",
            ));
        }
        let mut inline_paths = BTreeSet::new();
        for resource in &self.resources {
            resource.validate()?;
            for artifact in resource.schema_authority.inline_artifacts() {
                artifact.validate()?;
                if !inline_paths.insert(artifact.path.as_str()) {
                    return Err(CdfError::data(format!(
                        "portable plan repeats inline artifact path `{}`",
                        artifact.path
                    )));
                }
            }
        }
        let has_proposals = self
            .resources
            .iter()
            .any(|resource| resource.schema_authority.is_proposed_first_use());
        if has_proposals != self.proposed_lock.is_some() {
            return Err(CdfError::data(
                "portable plan proposed schema authority and proposed lock disagree",
            ));
        }
        if let Some(lock) = &self.proposed_lock {
            parse_lock(&lock_to_toml(lock)?)?;
            for resource in self
                .resources
                .iter()
                .filter(|resource| resource.schema_authority.is_proposed_first_use())
            {
                if lock.resources.get(&resource.resource_id)
                    != Some(resource.schema_authority.lock_binding())
                {
                    return Err(CdfError::data(format!(
                        "portable plan proposed lock differs from resource `{}` authority",
                        resource.resource_id
                    )));
                }
            }
        }
        if self.plan_hash != self.identity_hash()? {
            return Err(CdfError::data(
                "portable plan hash does not match its canonical content",
            ));
        }
        Ok(())
    }

    pub fn canonical_json_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| CdfError::internal(format!("serialize portable plan: {error}")))?;
        bytes.push(b'\n');
        if bytes.len() > PORTABLE_PLAN_MAX_BYTES {
            return Err(CdfError::contract(format!(
                "portable plan exceeds the {PORTABLE_PLAN_MAX_BYTES}-byte bound"
            )));
        }
        Ok(bytes)
    }

    fn identity_hash(&self) -> Result<String> {
        cdf_runtime::artifact_hash(&PortablePlanIdentity {
            version: self.version,
            cdf_version: &self.cdf_version,
            project: &self.project,
            environment: &self.environment,
            environment_binding_hash: &self.environment_binding_hash,
            selection: &self.selection,
            lock_precondition: &self.lock_precondition,
            proposed_lock: &self.proposed_lock,
            failure_policy: self.failure_policy,
            required_host: &self.required_host,
            resources: &self.resources,
        })
    }
}

impl PortablePlanResource {
    pub fn validate(&self) -> Result<()> {
        self.compiled_resource.validate()?;
        self.destination.validate()?;
        PipelineId::new(self.pipeline_id.as_str())?;
        CheckpointId::new(self.checkpoint_id.as_str())?;
        if self.resource_id != self.compiled_resource.resource.resource_id
            || self.resource_id != self.engine_plan.scan.request.resource_id.as_str()
            || self.compiled_resource.resource.descriptor
                != self.schema_authority.lock_binding().descriptor
            || self.compiled_source_plan_hash
                != cdf_runtime::artifact_hash(&self.compiled_resource.resource.source_plan)?
            || self.engine_plan.initial_committed_frontier != self.input_checkpoint_head
        {
            return Err(CdfError::data(format!(
                "portable plan resource `{}` has inconsistent compiled, source, checkpoint, or lock authority",
                self.resource_id
            )));
        }
        validate_sha256(
            "portable compiled source plan",
            &self.compiled_source_plan_hash,
        )?;
        match (
            self.engine_plan.scan.external_task_set(),
            &self.source_task_set,
        ) {
            (Some(reference), Some(artifact)) if reference == &artifact.reference => {
                artifact.validate()?;
            }
            (None, None) => {}
            _ => {
                return Err(CdfError::data(format!(
                    "portable plan resource `{}` task-set authority is incomplete or inconsistent",
                    self.resource_id
                )));
            }
        }
        if self
            .schema_authority
            .lock_binding()
            .compiled_artifact_hash
            .as_deref()
            != Some(self.compiled_resource.artifact_hash.as_str())
        {
            return Err(CdfError::data(format!(
                "portable plan resource `{}` lock does not bind its compiled artifact",
                self.resource_id
            )));
        }
        if self.engine_plan.package_id.is_empty() {
            return Err(CdfError::data(format!(
                "portable plan resource `{}` has an empty package identity",
                self.resource_id
            )));
        }
        self.engine_plan.validate_execution_extent_for_execution()?;
        self.engine_plan.validate_partition_schedule()?;
        self.engine_plan.validate_compiled_expression_plan()?;
        Ok(())
    }
}

pub fn parse_portable_plan(bytes: &[u8]) -> Result<PortablePlanArtifact> {
    if bytes.len() > PORTABLE_PLAN_MAX_BYTES {
        return Err(CdfError::data(format!(
            "portable plan exceeds the {PORTABLE_PLAN_MAX_BYTES}-byte read bound"
        )));
    }
    let artifact: PortablePlanArtifact = serde_json::from_slice(bytes)
        .map_err(|error| CdfError::data(format!("parse portable plan: {error}")))?;
    artifact.validate()?;
    if artifact.canonical_json_bytes()? != bytes {
        return Err(CdfError::data("portable plan bytes are not canonical"));
    }
    Ok(artifact)
}

pub fn lock_precondition(bytes: Option<&[u8]>) -> PortableLockPrecondition {
    bytes.map_or(PortableLockPrecondition::Absent, |bytes| {
        PortableLockPrecondition::Present {
            content_sha256: sha256(bytes),
        }
    })
}

fn validate_relative_artifact_path(path: &str) -> Result<()> {
    let path = std::path::Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(CdfError::data(
            "portable inline artifact path must be a nonempty project-relative normal path",
        ));
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> Result<()> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(CdfError::data(format!(
            "{label} has an invalid SHA-256 hash"
        )));
    }
    Ok(())
}

pub fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("sha256:{:x}", Sha256::digest(bytes))
}
