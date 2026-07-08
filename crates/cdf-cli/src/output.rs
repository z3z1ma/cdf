use cdf_kernel::{CdfError, ErrorKind};
use serde::Serialize;

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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CliError {
    pub kind: ErrorKind,
    pub message: String,
    pub exit_code: i32,
    pub not_supported: bool,
}

impl CliError {
    pub fn usage(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Contract,
            message: message.into(),
            exit_code: 2,
            not_supported: false,
        }
    }

    pub fn not_supported(
        command: impl AsRef<str>,
        reason: impl AsRef<str>,
        required_lower_layer: impl AsRef<str>,
    ) -> Self {
        Self {
            kind: ErrorKind::Internal,
            message: format!(
                "{} is not yet supported: {}; required lower layer: {}",
                command.as_ref(),
                reason.as_ref(),
                required_lower_layer.as_ref()
            ),
            exit_code: 78,
            not_supported: true,
        }
    }

    fn body(&self) -> ErrorBody {
        ErrorBody {
            kind: self.kind.clone(),
            message: self.message.clone(),
            exit_code: self.exit_code,
            not_supported: self.not_supported,
        }
    }
}

impl From<CdfError> for CliError {
    fn from(error: CdfError) -> Self {
        let exit_code = match error.kind {
            ErrorKind::Transient | ErrorKind::RateLimited => 75,
            ErrorKind::Auth => 4,
            ErrorKind::Contract => 3,
            ErrorKind::Data => 5,
            ErrorKind::Destination => 6,
            ErrorKind::Internal => 70,
        };
        Self {
            kind: error.kind,
            message: error.message,
            exit_code,
            not_supported: false,
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
    Plain(String),
    #[allow(dead_code)]
    // 10x: renderer hook is exercised by WS3B tests before command-family migration tickets use it.
    Rendered(RenderDocument),
}

impl HumanOutput {
    fn render(self, config: &RenderConfig) -> String {
        match self {
            Self::Plain(text) => text,
            Self::Rendered(document) => document.render(config),
        }
    }
}

impl CommandOutput {
    #[allow(dead_code)] // 10x: WS3B installs the typed renderer output hook; WS3C/WS3D adopt it by command family.
    pub(crate) fn rendered<T: Serialize>(
        command: &'static str,
        document: RenderDocument,
        value: T,
    ) -> Result<Self, CliError> {
        Ok(Self {
            command,
            exit_code: 0,
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
            Self {
                exit_code: error.exit_code,
                stdout: String::new(),
                stderr: format!("error: {}\n", error.message),
            }
        }
    }
}
