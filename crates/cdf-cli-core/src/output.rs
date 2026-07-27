use cdf_kernel::{CdfError, ErrorKind};
use serde::Serialize;

use crate::error_catalog;
use crate::progress::ProgressSnapshot;
use crate::render::{
    RenderConfig, RenderDocument,
    primitives::{ErrorBlock, RenderPrimitive},
    redaction::{is_sensitive_key, redact_uri_userinfo, redacted},
};
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
    pub progress: Option<Box<ProgressSnapshot>>,
}

impl CliError {
    fn from_mapping(
        kind: ErrorKind,
        message: impl Into<String>,
        not_supported: bool,
        mapping: error_catalog::ErrorMapping,
    ) -> Self {
        let message = redact_uri_userinfo(message.into());
        Self {
            kind,
            message,
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

    pub fn usage_with(message: impl Into<String>, mapping: error_catalog::ErrorMapping) -> Self {
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

    pub fn not_supported_with(
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

    pub fn mapped(error: CdfError, mapping: error_catalog::ErrorMapping) -> Self {
        Self::from_mapping(error.kind, error.message, false, mapping)
    }

    pub fn with_suggestions(mut self, suggestions: Vec<String>) -> Self {
        self.suggestions = suggestions
            .into_iter()
            .map(redact_uri_userinfo)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        self
    }

    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(Box::new(redact_json_value(details)));
        self
    }

    pub fn with_progress(mut self, progress: ProgressSnapshot) -> Self {
        self.progress = Some(Box::new(progress));
        self
    }

    fn body(&self) -> ErrorBody {
        ErrorBody {
            kind: self.kind.clone(),
            message: redact_uri_userinfo(&self.message),
            exit_code: self.exit_code,
            not_supported: self.not_supported,
            code: self.code.clone(),
            details: self.details.as_deref().cloned().map(redact_json_value),
            remediation: self
                .remediation
                .as_deref()
                .cloned()
                .map(|remediation| ErrorRemediation {
                    summary: redact_uri_userinfo(remediation.summary),
                    steps: remediation
                        .steps
                        .into_iter()
                        .map(redact_uri_userinfo)
                        .collect(),
                }),
            suggestions: self.suggestions.iter().map(redact_uri_userinfo).collect(),
        }
    }
}

impl From<CdfError> for CliError {
    fn from(error: CdfError) -> Self {
        let mapping = error_catalog::generic_lower_layer_mapping(&error.kind);
        Self::from_mapping(error.kind, error.message, false, mapping)
    }
}

impl From<CliError> for CdfError {
    fn from(error: CliError) -> Self {
        CdfError::new(error.kind, error.message)
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
pub enum HumanOutput {
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
    pub fn rendered<T: Serialize>(
        command: &'static str,
        document: RenderDocument,
        value: T,
    ) -> Result<Self, CliError> {
        Self::rendered_with_exit_code(command, document, value, 0)
    }

    pub fn rendered_with_progress<T: Serialize>(
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

    pub fn rendered_with_exit_code<T: Serialize>(
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

    pub fn rendered_with_progress_and_exit_code<T: Serialize>(
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
    pub fn from_output(
        json_mode: bool,
        render_config: &RenderConfig,
        output: CommandOutput,
    ) -> Self {
        Self::from_output_with_configs(json_mode, render_config, render_config, output)
    }

    pub fn from_output_with_configs(
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

    pub fn from_error_with_config(
        json_mode: bool,
        render_config: &RenderConfig,
        error: CliError,
    ) -> Self {
        let body = error.body();
        if json_mode {
            let envelope = ErrorEnvelope {
                ok: false,
                error: body,
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
            let mut error_block = ErrorBlock::new(body.code, body.message);
            if let Some(details) = body.details {
                if let Some(object) = details.as_object() {
                    for (key, value) in object {
                        error_block = error_block.detail(key, display_json_value(value));
                    }
                } else {
                    error_block = error_block.detail("details", details.to_string());
                }
            }
            if let Some(remediation) = body.remediation {
                error_block = error_block.help(remediation.summary);
                for step in remediation.steps {
                    error_block = error_block.help(step);
                }
            }
            for suggestion in body.suggestions {
                error_block = error_block.suggestion(suggestion);
            }
            stderr.push_str(&error_block.render(render_config));
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

fn display_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
}

fn redact_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(value) => serde_json::Value::String(redact_uri_userinfo(value)),
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(redact_json_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| {
                    let value = if is_sensitive_key(&key) {
                        serde_json::Value::String(redacted())
                    } else {
                        redact_json_value(value)
                    };
                    (redact_uri_userinfo(key), value)
                })
                .collect(),
        ),
        value => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::config::{DisplayMode, RenderEnv};

    fn tty_config() -> RenderConfig {
        RenderConfig::new(
            DisplayMode::Tty,
            80,
            RenderEnv {
                no_color: true,
                clicolor_force: false,
                unicode_supported: true,
            },
            TerminalPolicy {
                color: crate::terminal::PolicyMode::Never,
                ..TerminalPolicy::default()
            },
        )
    }

    #[test]
    fn environment_error_is_stable_in_json_headless_and_tty_output() {
        let lower = CdfError::environment(
            "required executable `cdf-helper` for postgres://user:secret@example/db and mysql://other:second@elsewhere/db was not found; install it or configure an absolute executable path",
        );
        let decorate = |error: CdfError| {
            CliError::from(error)
                .with_details(serde_json::json!({
                    "dsn": "postgres://detail:third@example/db",
                    "private_key": "-----BEGIN PRIVATE KEY-----sentinel",
                    "nested": ["mysql://nested:fourth@example/db"]
                }))
                .with_suggestions(vec![
                    "retry sqlite://suggestion:fifth@example/db".to_owned(),
                ])
        };
        let mapped = decorate(lower.clone());
        assert_eq!(mapped.kind, ErrorKind::Environment);
        assert_eq!(mapped.code, "CDF-ENV-HOST");
        assert_eq!(mapped.exit_code, 70);

        let json = InvocationResult::from_error_with_config(
            true,
            &RenderConfig::headless_for_width(80),
            decorate(lower.clone()),
        );
        let envelope: serde_json::Value = serde_json::from_str(&json.stderr).unwrap();
        assert_eq!(envelope["error"]["kind"], "environment");
        assert_eq!(envelope["error"]["code"], "CDF-ENV-HOST");
        assert_eq!(envelope["error"]["exit_code"], 70);
        assert!(!json.stderr.contains("user:secret"));
        for secret in [
            "other:second",
            "detail:third",
            "PRIVATE KEY",
            "nested:fourth",
            "suggestion:fifth",
        ] {
            assert!(!json.stderr.contains(secret));
        }
        assert!(json.stderr.contains("postgres://[redacted]@example/db"));
        assert!(json.stderr.contains("mysql://[redacted]@elsewhere/db"));
        assert!(
            envelope["error"]["remediation"]["summary"]
                .as_str()
                .unwrap()
                .contains("host or process")
        );

        let headless = InvocationResult::from_error_with_config(
            false,
            &RenderConfig::headless_for_width(80),
            decorate(lower.clone()),
        );
        let tty = InvocationResult::from_error_with_config(false, &tty_config(), decorate(lower));
        for rendered in [&headless.stderr, &tty.stderr] {
            assert!(rendered.contains("CDF-ENV-HOST"));
            assert!(rendered.contains("cdf-helper"));
            assert!(!rendered.contains("user:secret"));
            for secret in [
                "other:second",
                "detail:third",
                "PRIVATE KEY",
                "nested:fourth",
                "suggestion:fifth",
            ] {
                assert!(!rendered.contains(secret));
            }
            assert!(rendered.contains("postgres://[redacted]@example/db"));
            assert!(rendered.contains("mysql://[redacted]@elsewhere/db"));
            assert!(rendered.contains("Restore the required host or process facility"));
            assert!(!rendered.contains("\u{1b}["));
        }
    }

    #[test]
    fn invariant_error_retains_internal_mapping() {
        let mapped = CliError::from(CdfError::internal("poisoned ownership invariant"));
        assert_eq!(mapped.kind, ErrorKind::Internal);
        assert_eq!(mapped.code, "CDF-INTERNAL-UNEXPECTED");
        assert_eq!(mapped.exit_code, 70);
    }
}
