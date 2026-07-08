use cdf_kernel::{CdfError, ErrorKind};
use serde::Serialize;

use crate::error_catalog;
use crate::render::{RenderConfig, RenderDocument};

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
    pub remediation: Option<ErrorRemediation>,
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
    pub remediation: Option<ErrorRemediation>,
}

impl CliError {
    pub fn usage(message: impl Into<String>) -> Self {
        let mapping = error_catalog::USAGE;
        Self {
            kind: ErrorKind::Contract,
            message: message.into(),
            exit_code: mapping.exit_code,
            not_supported: false,
            code: mapping.code.to_owned(),
            remediation: mapping.remediation.map(ErrorRemediation::from_template),
        }
    }

    pub fn not_supported(
        command: impl AsRef<str>,
        reason: impl AsRef<str>,
        required_lower_layer: impl AsRef<str>,
    ) -> Self {
        let mapping = error_catalog::NOT_SUPPORTED;
        Self {
            kind: ErrorKind::Internal,
            message: format!(
                "{} is not yet supported: {}; required lower layer: {}",
                command.as_ref(),
                reason.as_ref(),
                required_lower_layer.as_ref()
            ),
            exit_code: mapping.exit_code,
            not_supported: true,
            code: mapping.code.to_owned(),
            remediation: mapping.remediation.map(ErrorRemediation::from_template),
        }
    }

    fn body(&self) -> ErrorBody {
        ErrorBody {
            kind: self.kind.clone(),
            message: self.message.clone(),
            exit_code: self.exit_code,
            not_supported: self.not_supported,
            code: self.code.clone(),
            remediation: self.remediation.clone(),
        }
    }
}

impl From<CdfError> for CliError {
    fn from(error: CdfError) -> Self {
        let mapping = error_catalog::generic_lower_layer_mapping(&error.kind);
        Self {
            kind: error.kind,
            message: error.message,
            exit_code: mapping.exit_code,
            not_supported: false,
            code: mapping.code.to_owned(),
            remediation: mapping.remediation.map(ErrorRemediation::from_template),
        }
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
}

impl HumanOutput {
    fn render(self, config: &RenderConfig) -> String {
        match self {
            Self::Rendered(document) => document.render(config),
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

    pub(crate) fn rendered_with_exit_code<T: Serialize>(
        command: &'static str,
        document: RenderDocument,
        value: T,
        exit_code: i32,
    ) -> Result<Self, CliError> {
        Ok(Self {
            command,
            exit_code,
            human: HumanOutput::Rendered(document),
            json: serde_json::to_value(value)
                .map_err(|error| CliError::from(CdfError::internal(error.to_string())))?,
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
    pub(crate) fn from_output(
        json_mode: bool,
        render_config: &RenderConfig,
        output: CommandOutput,
    ) -> Self {
        let stdout = if json_mode {
            let envelope = SuccessEnvelope {
                ok: true,
                command: output.command,
                result: &output.json,
            };
            format!(
                "{}\n",
                serde_json::to_string_pretty(&envelope)
                    .expect("CLI success envelope must serialize")
            )
        } else {
            let human = output.human.render(render_config);
            if human.ends_with('\n') {
                human
            } else {
                format!("{human}\n")
            }
        };
        Self {
            exit_code: output.exit_code,
            stdout,
            stderr: String::new(),
        }
    }

    pub fn from_error(json_mode: bool, error: CliError) -> Self {
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
            let remediation = error
                .remediation
                .map(format_remediation)
                .unwrap_or_default();
            Self {
                exit_code: error.exit_code,
                stdout: String::new(),
                stderr: format!("error: {}{remediation}\n", error.message),
            }
        }
    }
}

fn format_remediation(remediation: ErrorRemediation) -> String {
    let mut text = format!("\nremediation: {}", remediation.summary);
    for step in &remediation.steps {
        text.push_str("\n  - ");
        text.push_str(step);
    }
    text
}
