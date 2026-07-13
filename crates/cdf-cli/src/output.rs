use cdf_kernel::{CdfError, ErrorKind};
use serde::Serialize;

use crate::error_catalog;
use crate::progress::ProgressSnapshot;
use crate::render::{RenderConfig, RenderDocument};
use crate::terminal::{OutputChannel, TerminalPolicy};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvocationResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ErrorBody {
    pub kind: ErrorKind,
    pub message: String,
    pub exit_code: i32,
    pub not_supported: bool,
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remediation: Option<ErrorRemediation>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub suggestions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ErrorRemediation {
    pub summary: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<String>,
}

impl ErrorRemediation {
    fn from_template(template: error_catalog::RemediationTemplate) -> Self {
        Self {
            summary: template.summary.to_owned(),
            steps: template
                .steps
                .iter()
                .map(|step| (*step).to_owned())
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CliError {
    pub kind: ErrorKind,
    pub message: String,
    pub exit_code: i32,
    pub not_supported: bool,
    pub code: String,
    pub details: Option<Box<serde_json::Value>>,
    pub remediation: Option<Box<ErrorRemediation>>,
    pub suggestions: Box<[String]>,
    pub(crate) progress: Option<Box<ProgressSnapshot>>,
}

impl CliError {
    fn from_mapping(
        kind: ErrorKind,
        message: impl Into<String>,
        not_supported: bool,
        mapping: error_catalog::ErrorMapping,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            exit_code: mapping.exit_code,
            not_supported,
            code: mapping.code.to_owned(),
            details: None,
            remediation: mapping
                .remediation
                .map(ErrorRemediation::from_template)
                .map(Box::new),
            suggestions: Box::new([]),
            progress: None,
        }
    }

    /// Generic parser/grammar mapping for direct `CliError::usage` sites.
    /// Command modules should use `usage_with` when a narrower product code is
    /// useful; pure CLI grammar errors intentionally share `CDF-CLI-USAGE`.
    pub fn usage(message: impl Into<String>) -> Self {
        Self::usage_with(message, error_catalog::USAGE)
    }

    pub(crate) fn usage_with(
        message: impl Into<String>,
        mapping: error_catalog::ErrorMapping,
    ) -> Self {
        Self::from_mapping(ErrorKind::Contract, message, false, mapping)
    }

    /// Generic not-supported mapping for direct `CliError::not_supported`
    /// sites. Callers must name the required lower layer; command modules may
    /// use `not_supported_with` for narrower product codes.
    pub fn not_supported(
        command: impl AsRef<str>,
        reason: impl AsRef<str>,
        required_lower_layer: impl AsRef<str>,
    ) -> Self {
        Self::not_supported_with(
            command,
            reason,
            required_lower_layer,
            error_catalog::NOT_SUPPORTED,
        )
    }

    pub(crate) fn not_supported_with(
        command: impl AsRef<str>,
        reason: impl AsRef<str>,
        required_lower_layer: impl AsRef<str>,
        mapping: error_catalog::ErrorMapping,
    ) -> Self {
        Self::from_mapping(
            ErrorKind::Internal,
            format!(
                "{} is not yet supported: {}; required lower layer: {}",
                command.as_ref(),
                reason.as_ref(),
                required_lower_layer.as_ref()
            ),
            true,
            mapping,
        )
    }

    pub(crate) fn mapped(error: CdfError, mapping: error_catalog::ErrorMapping) -> Self {
        Self::from_mapping(error.kind, error.message, false, mapping)
    }

    pub(crate) fn with_suggestions(mut self, suggestions: Vec<String>) -> Self {
        self.suggestions = suggestions.into_boxed_slice();
        self
    }

    pub(crate) fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(Box::new(details));
        self
    }

    pub(crate) fn with_progress(mut self, progress: ProgressSnapshot) -> Self {
        self.progress = Some(Box::new(progress));
        self
    }

    fn body(&self) -> ErrorBody {
        ErrorBody {
            kind: self.kind.clone(),
            message: self.message.clone(),
            exit_code: self.exit_code,
            not_supported: self.not_supported,
            code: self.code.clone(),
            details: self.details.as_deref().cloned(),
            remediation: self.remediation.as_deref().cloned(),
            suggestions: self.suggestions.to_vec(),
        }
    }
}

impl From<CdfError> for CliError {
    fn from(error: CdfError) -> Self {
        let mapping = error_catalog::generic_lower_layer_mapping(&error.kind);
        Self::from_mapping(error.kind, error.message, false, mapping)
    }
}

#[derive(Clone, Debug)]
pub struct CommandOutput {
    pub command: &'static str,
    pub exit_code: i32,
    pub human: HumanOutput,
    pub json: serde_json::Value,
}

#[derive(Clone, Debug)]
pub(crate) enum HumanOutput {
    Rendered(RenderDocument),
    RenderedWithProgress {
        progress: ProgressSnapshot,
        document: RenderDocument,
    },
}

impl HumanOutput {
    fn render_channels(
        self,
        stdout_config: &RenderConfig,
        stderr_config: &RenderConfig,
    ) -> (String, String) {
        match self {
            Self::Rendered(document) => (document.render(stdout_config), String::new()),
            Self::RenderedWithProgress { progress, document } => {
                let mut stderr = progress.render_for_config(stderr_config);
                if !stderr.is_empty() && !stderr.ends_with('\n') {
                    stderr.push('\n');
                }
                (document.render(stdout_config), stderr)
            }
        }
    }
}

impl CommandOutput {
    pub(crate) fn rendered<T: Serialize>(
        command: &'static str,
        document: RenderDocument,
        value: T,
    ) -> Result<Self, CliError> {
        Self::rendered_with_exit_code(command, document, value, 0)
    }

    pub(crate) fn rendered_with_progress<T: Serialize>(
        command: &'static str,
        document: RenderDocument,
        value: T,
        progress: ProgressSnapshot,
    ) -> Result<Self, CliError> {
        Self::rendered_human_with_exit_code(
            command,
            HumanOutput::RenderedWithProgress { progress, document },
            value,
            0,
        )
    }

    pub(crate) fn rendered_with_exit_code<T: Serialize>(
        command: &'static str,
        document: RenderDocument,
        value: T,
        exit_code: i32,
    ) -> Result<Self, CliError> {
        Self::rendered_human_with_exit_code(
            command,
            HumanOutput::Rendered(document),
            value,
            exit_code,
        )
    }

    pub(crate) fn rendered_with_progress_and_exit_code<T: Serialize>(
        command: &'static str,
        document: RenderDocument,
        value: T,
        progress: ProgressSnapshot,
        exit_code: i32,
    ) -> Result<Self, CliError> {
        Self::rendered_human_with_exit_code(
            command,
            HumanOutput::RenderedWithProgress { progress, document },
            value,
            exit_code,
        )
    }

    fn rendered_human_with_exit_code<T: Serialize>(
        command: &'static str,
        human: HumanOutput,
        value: T,
        exit_code: i32,
    ) -> Result<Self, CliError> {
        Ok(Self {
            command,
            exit_code,
            human,
            json: serde_json::to_value(value).map_err(|error| {
                CliError::mapped(
                    CdfError::internal(error.to_string()),
                    error_catalog::CLI_JSON,
                )
            })?,
        })
    }
}

#[derive(Serialize)]
struct SuccessEnvelope<'a> {
    ok: bool,
    command: &'a str,
    result: &'a serde_json::Value,
}

#[derive(Serialize)]
struct ErrorEnvelope {
    ok: bool,
    error: ErrorBody,
}

impl InvocationResult {
    #[cfg(test)]
    pub(crate) fn from_output(
        json_mode: bool,
        render_config: &RenderConfig,
        output: CommandOutput,
    ) -> Self {
        Self::from_output_with_configs(json_mode, render_config, render_config, output)
    }

    pub(crate) fn from_output_with_configs(
        json_mode: bool,
        stdout_config: &RenderConfig,
        stderr_config: &RenderConfig,
        output: CommandOutput,
    ) -> Self {
        let (stdout, stderr) = if json_mode {
            let envelope = SuccessEnvelope {
                ok: true,
                command: output.command,
                result: &output.json,
            };
            (
                format!(
                    "{}\n",
                    serde_json::to_string_pretty(&envelope)
                        .expect("CLI success envelope must serialize")
                ),
                String::new(),
            )
        } else {
            let (human, progress) = output.human.render_channels(stdout_config, stderr_config);
            let stdout = if human.ends_with('\n') {
                human
            } else {
                format!("{human}\n")
            };
            (stdout, progress)
        };
        Self {
            exit_code: output.exit_code,
            stdout,
            stderr,
        }
    }

    pub(crate) fn from_error_with_config(
        json_mode: bool,
        render_config: &RenderConfig,
        error: CliError,
    ) -> Self {
        if json_mode {
            let envelope = ErrorEnvelope {
                ok: false,
                error: error.body(),
            };
            Self {
                exit_code: error.exit_code,
                stdout: String::new(),
                stderr: format!(
                    "{}\n",
                    serde_json::to_string_pretty(&envelope)
                        .expect("CLI error envelope must serialize")
                ),
            }
        } else {
            let progress = error.progress;
            let exit_code = error.exit_code;
            let message = error.message;
            let details = error
                .details
                .map(|details| format_error_details(&details))
                .unwrap_or_default();
            let remediation = error
                .remediation
                .map(|remediation| format_remediation(*remediation))
                .unwrap_or_default();
            let suggestions = format_suggestions(&error.suggestions);
            let mut stderr = String::new();
            if let Some(progress) = progress {
                let rendered = progress.render_for_config(render_config);
                if !rendered.is_empty() {
                    stderr.push_str(&rendered);
                    if !stderr.ends_with('\n') {
                        stderr.push('\n');
                    }
                    stderr.push('\n');
                }
            }
            stderr.push_str(&format!(
                "error: {message}{details}{remediation}{suggestions}\n"
            ));
            Self {
                exit_code,
                stdout: String::new(),
                stderr,
            }
        }
    }

    pub fn from_error(json_mode: bool, error: CliError) -> Self {
        Self::from_error_with_config(
            json_mode,
            &RenderConfig::detect(&TerminalPolicy::default(), OutputChannel::Stderr),
            error,
        )
    }
}

fn format_error_details(details: &serde_json::Value) -> String {
    let Some(object) = details.as_object() else {
        return format!("\ndetails: {details}");
    };
    let mut rendered = String::from("\ndetails:");
    for (key, value) in object {
        rendered.push_str("\n  ");
        rendered.push_str(key);
        rendered.push_str(": ");
        match value {
            serde_json::Value::String(value) => rendered.push_str(value),
            _ => rendered.push_str(&value.to_string()),
        }
    }
    rendered
}

fn format_remediation(remediation: ErrorRemediation) -> String {
    let mut text = format!("\nremediation: {}", remediation.summary);
    for step in &remediation.steps {
        text.push_str("\n  - ");
        text.push_str(step);
    }
    text
}

fn format_suggestions(suggestions: &[String]) -> String {
    if suggestions.is_empty() {
        return String::new();
    }
    let mut text = String::from("\nsuggestions:");
    for suggestion in suggestions {
        text.push_str("\n  - ");
        text.push_str(suggestion);
    }
    text
}
