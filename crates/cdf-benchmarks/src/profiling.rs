use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    BenchResult, Capability, ChildCommand, HostCapabilityProvider, ToolIdentity, bench_error,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileTool {
    Flamegraph,
    PerfStat,
}

impl ProfileTool {
    fn executable(self) -> &'static str {
        match self {
            Self::Flamegraph => "flamegraph",
            Self::PerfStat => "perf",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfilePlan {
    pub tool: ProfileTool,
    pub tool_identity: ToolIdentity,
    pub command: Vec<String>,
    pub artifact: PathBuf,
}

pub fn plan_profile(
    provider: &dyn HostCapabilityProvider,
    tool: ProfileTool,
    command: &ChildCommand,
    output_root: &Path,
    artifact_name: &str,
) -> BenchResult<Capability<ProfilePlan>> {
    if artifact_name.is_empty()
        || artifact_name.contains('/')
        || artifact_name.contains('\\')
        || artifact_name == "."
        || artifact_name == ".."
    {
        return Err(bench_error(
            "profile artifact name must be one safe path component",
        ));
    }
    let tool_capability = provider.discover_tool(tool.executable());
    let identity = match tool_capability {
        Capability::Supported { value, .. } => value,
        Capability::Unavailable {
            reason,
            method,
            provider_version,
        } => {
            return Ok(Capability::Unavailable {
                reason,
                method,
                provider_version,
            });
        }
        Capability::Failed {
            error,
            method,
            provider_version,
        } => {
            return Ok(Capability::Failed {
                error,
                method,
                provider_version,
            });
        }
    };
    let profile_root = output_root.join("target/cdf-benchmarks/profiles");
    let extension = match tool {
        ProfileTool::Flamegraph => "svg",
        ProfileTool::PerfStat => "txt",
    };
    let artifact = profile_root.join(format!("{artifact_name}.{extension}"));
    let mut exact = match tool {
        ProfileTool::Flamegraph => vec![
            identity.executable.clone(),
            "--output".to_owned(),
            artifact.display().to_string(),
            "--".to_owned(),
        ],
        ProfileTool::PerfStat => vec![
            identity.executable.clone(),
            "stat".to_owned(),
            "-o".to_owned(),
            artifact.display().to_string(),
            "--".to_owned(),
        ],
    };
    exact.push(command.program.display().to_string());
    exact.extend(command.args.clone());
    Ok(Capability::Supported {
        value: ProfilePlan {
            tool,
            tool_identity: identity,
            command: exact,
            artifact,
        },
        method: "profile-dry-run-plan".to_owned(),
        provider_version: "profile-plan-v1".to_owned(),
    })
}
