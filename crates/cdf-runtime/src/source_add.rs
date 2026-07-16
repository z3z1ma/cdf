use std::{
    collections::BTreeMap,
    fmt,
    path::{Component, Path, PathBuf},
};

use cdf_http::{SecretUri, SecretValue};
use cdf_kernel::{CdfError, Result};

use crate::{SourceDriverDescriptor, SourceEvidenceLocation};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceAddCursorOrdering {
    Exact,
    Inexact,
    BestEffort,
    Unordered,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceAddCursor {
    pub field: String,
    pub parameter: Option<String>,
    pub ordering: SourceAddCursorOrdering,
    pub lag_tolerance_ms: u64,
}

impl SourceAddCursor {
    pub fn validate(&self) -> Result<()> {
        if self.field.is_empty()
            || self.field.chars().any(char::is_control)
            || self.parameter.as_ref().is_some_and(|parameter| {
                parameter.is_empty() || parameter.chars().any(char::is_control)
            })
        {
            return Err(CdfError::contract(
                "source add cursor field and optional parameter must be nonempty and control-free",
            ));
        }
        Ok(())
    }
}

/// Source-neutral input to the registered `cdf add` compiler hook.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceAddRequest {
    pub source_name: String,
    pub resource_name: String,
    pub location: String,
    pub project_root: PathBuf,
    pub current_dir: PathBuf,
    pub options: BTreeMap<String, String>,
    /// Only the options belonging to the driver receiving this request.
    pub project_options: Option<serde_json::Value>,
}

impl SourceAddRequest {
    pub fn validate(&self) -> Result<()> {
        validate_name("source", &self.source_name)?;
        validate_name("resource", &self.resource_name)?;
        if self.location.is_empty() || self.location.chars().any(char::is_control) {
            return Err(CdfError::contract(
                "source add location must be nonempty and control-free",
            ));
        }
        if self.options.iter().any(|(key, value)| {
            key.is_empty()
                || key.chars().any(char::is_control)
                || value.is_empty()
                || value.chars().any(char::is_control)
        }) {
            return Err(CdfError::contract(
                "source add option names and values must be nonempty and control-free",
            ));
        }
        if self
            .project_options
            .as_ref()
            .is_some_and(|value| !value.is_object())
        {
            return Err(CdfError::contract(
                "source add project options must be a JSON object",
            ));
        }
        Ok(())
    }
}

fn validate_name(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(CdfError::contract(format!(
            "source add {label} name must contain only ASCII letters, digits, `_`, or `-`"
        )));
    }
    Ok(())
}

/// A private file a source driver needs during discovery and future execution.
#[derive(Clone, PartialEq, Eq)]
pub struct SourceAddPrivateFile {
    pub reference: SecretUri,
    pub relative_path: PathBuf,
    pub value: SecretValue,
}

impl fmt::Debug for SourceAddPrivateFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceAddPrivateFile")
            .field("reference", &self.reference)
            .field("relative_path", &self.relative_path)
            .field("value", &"[REDACTED]")
            .finish()
    }
}

impl SourceAddPrivateFile {
    pub fn validate(&self) -> Result<()> {
        validate_private_path(&self.relative_path)?;
        let expected = format!("secret://file/{}", portable_path(&self.relative_path)?);
        if self.reference.as_str() != expected {
            return Err(CdfError::contract(format!(
                "source add private file reference must be `{expected}`"
            )));
        }
        Ok(())
    }
}

fn validate_private_path(path: &Path) -> Result<()> {
    let components = path.components().collect::<Vec<_>>();
    if components.is_empty()
        || components
            .iter()
            .any(|component| !matches!(component, Component::Normal(_)))
        || !path.starts_with(".cdf/secrets/sources")
    {
        return Err(CdfError::contract(
            "source add private files must use a relative path below `.cdf/secrets/sources`",
        ));
    }
    Ok(())
}

fn portable_path(path: &Path) -> Result<String> {
    path.to_str()
        .map(|value| value.replace(std::path::MAIN_SEPARATOR, "/"))
        .ok_or_else(|| CdfError::contract("source add private file path must be valid UTF-8"))
}

/// Declarative input proposed by one registered source driver.
#[derive(Clone, Debug, PartialEq)]
pub struct SourceAddProposal {
    pub source_kind: String,
    pub source_options: BTreeMap<String, serde_json::Value>,
    pub resource_options: BTreeMap<String, serde_json::Value>,
    pub cursor: Option<SourceAddCursor>,
    pub display_location: SourceEvidenceLocation,
    pub display_selection: String,
    pub private_files: Vec<SourceAddPrivateFile>,
}

impl SourceAddProposal {
    pub fn validate(&self) -> Result<()> {
        if self.source_kind.is_empty()
            || self.source_kind.chars().any(char::is_control)
            || self.display_selection.is_empty()
            || self.display_selection.chars().any(char::is_control)
        {
            return Err(CdfError::contract(
                "source add proposal requires a nonempty control-free kind and selection",
            ));
        }
        if self
            .source_options
            .keys()
            .chain(self.resource_options.keys())
            .any(|key| key.is_empty() || key.chars().any(char::is_control))
        {
            return Err(CdfError::contract(
                "source add proposal option names must be nonempty and control-free",
            ));
        }
        for private_file in &self.private_files {
            private_file.validate()?;
        }
        if let Some(cursor) = &self.cursor {
            cursor.validate()?;
        }
        let mut paths = self
            .private_files
            .iter()
            .map(|file| &file.relative_path)
            .collect::<Vec<_>>();
        paths.sort();
        if paths.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CdfError::contract(
                "source add proposal contains duplicate private file paths",
            ));
        }
        Ok(())
    }
}

pub trait SourceAddPlanner: Send + Sync {
    /// Returns `None` when this driver does not own the request.
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlannedSourceAdd {
    pub driver: SourceDriverDescriptor,
    pub proposal: SourceAddProposal,
}
