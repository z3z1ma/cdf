use crate::ExecutionServices;
use crate::prelude::*;

pub trait DestinationPolicyProvider: std::panic::RefUnwindSafe {
    fn value(&self, destination: &str, key: &str) -> Option<&str>;
}

#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct DestinationResolutionContext<'a> {
    project_root: Option<&'a Path>,
    target: Option<&'a TargetName>,
    environment_name: Option<&'a str>,
    destination_policy: Option<&'a dyn DestinationPolicyProvider>,
    secret_provider: Option<&'a RuntimeSecretProvider>,
    execution_services: Option<&'a ExecutionServices>,
}

impl<'a> DestinationResolutionContext<'a> {
    pub fn new() -> Self {
        Self {
            project_root: None,
            target: None,
            environment_name: None,
            destination_policy: None,
            secret_provider: None,
            execution_services: None,
        }
    }

    pub fn for_project_run(project_root: &'a Path, target: &'a TargetName) -> Self {
        Self {
            project_root: Some(project_root),
            target: Some(target),
            environment_name: None,
            destination_policy: None,
            secret_provider: None,
            execution_services: None,
        }
    }

    pub fn for_project_inspection(project_root: &'a Path) -> Self {
        Self {
            project_root: Some(project_root),
            target: None,
            environment_name: None,
            destination_policy: None,
            secret_provider: None,
            execution_services: None,
        }
    }

    pub fn with_environment_name(mut self, environment_name: &'a str) -> Self {
        self.environment_name = Some(environment_name);
        self
    }

    pub fn with_destination_policy(mut self, policy: &'a dyn DestinationPolicyProvider) -> Self {
        self.destination_policy = Some(policy);
        self
    }

    pub fn with_secret_provider(mut self, provider: &'a RuntimeSecretProvider) -> Self {
        self.secret_provider = Some(provider);
        self
    }

    pub fn with_execution_services(mut self, services: &'a ExecutionServices) -> Self {
        self.execution_services = Some(services);
        self
    }

    pub fn project_root(&self) -> Result<&'a Path> {
        self.project_root.ok_or_else(|| {
            CdfError::contract("project destination resolution requires a project root")
        })
    }

    pub fn target(&self) -> Result<&'a TargetName> {
        self.target.ok_or_else(|| {
            CdfError::contract("project destination resolution requires a run target")
        })
    }

    pub fn policy_value(&self, destination: &str, key: &str) -> Result<&'a str> {
        self.destination_policy
            .and_then(|policy| policy.value(destination, key))
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "project destination resolution requires `{destination}.{key}` policy"
                ))
            })
    }

    pub fn secret_provider(&self) -> Result<&'a RuntimeSecretProvider> {
        self.secret_provider.ok_or_else(|| {
            CdfError::auth("secret-backed destination URI requires a SecretProvider")
        })
    }

    pub fn environment_name(&self) -> &str {
        self.environment_name.unwrap_or("<selected>")
    }

    pub fn execution_services(&self) -> Option<&'a ExecutionServices> {
        self.execution_services
    }
}

impl std::fmt::Debug for DestinationResolutionContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DestinationResolutionContext")
            .field("project_root", &self.project_root)
            .field("target", &self.target)
            .field("environment_name", &self.environment_name)
            .field("destination_policy", &self.destination_policy.is_some())
            .field("secret_provider", &self.secret_provider.is_some())
            .field("execution_services", &self.execution_services.is_some())
            .finish_non_exhaustive()
    }
}

impl Default for DestinationResolutionContext<'_> {
    fn default() -> Self {
        Self::new()
    }
}
