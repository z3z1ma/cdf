use std::io::IsTerminal;

use crate::output::CliError;

pub const DEFAULT_TERMINAL_WIDTH: usize = 80;
pub const MIN_TERMINAL_WIDTH: usize = 20;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PolicyMode {
    #[default]
    Auto,
    Always,
    Never,
}

impl PolicyMode {
    pub fn parse(flag: &str, value: &str) -> Result<Self, CliError> {
        match value {
            "auto" => Ok(Self::Auto),
            "always" => Ok(Self::Always),
            "never" => Ok(Self::Never),
            _ => Err(CliError::usage(format!(
                "{flag} must be one of auto, always, or never; try `{flag} auto`"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Verbosity {
    Quiet,
    #[default]
    Normal,
    Verbose(u8),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TerminalPolicy {
    pub color: PolicyMode,
    pub progress: PolicyMode,
    pub unicode: PolicyMode,
    pub verbosity: Verbosity,
}

impl TerminalPolicy {
    pub fn progress_enabled(self, json: bool) -> bool {
        !json && self.progress != PolicyMode::Never && self.verbosity != Verbosity::Quiet
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cx1_terminal_width_precedence_is_terminal_then_columns_then_fallback() {
        let base = TerminalEnvironment::default();
        assert_eq!(base.width(), DEFAULT_TERMINAL_WIDTH);
        assert_eq!(
            TerminalEnvironment {
                columns: Some(41),
                ..base
            }
            .width(),
            41
        );
        assert_eq!(
            TerminalEnvironment {
                terminal_width: Some(133),
                columns: Some(41),
                ..base
            }
            .width(),
            133
        );
        assert_eq!(
            TerminalEnvironment {
                terminal_width: Some(8),
                ..base
            }
            .width(),
            MIN_TERMINAL_WIDTH
        );
        for width in [40, 80, 160] {
            assert_eq!(
                TerminalEnvironment {
                    terminal_width: Some(width),
                    ..base
                }
                .width(),
                width
            );
        }
    }

    #[test]
    fn cx1_quiet_and_never_disable_progress() {
        assert!(
            !TerminalPolicy {
                verbosity: Verbosity::Quiet,
                ..TerminalPolicy::default()
            }
            .progress_enabled(false)
        );
        assert!(
            !TerminalPolicy {
                progress: PolicyMode::Never,
                ..TerminalPolicy::default()
            }
            .progress_enabled(false)
        );
        assert!(!TerminalPolicy::default().progress_enabled(true));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputChannel {
    Stdout,
    Stderr,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TerminalEnvironment {
    pub stdout_is_terminal: bool,
    pub stderr_is_terminal: bool,
    pub terminal_width: Option<usize>,
    pub terminal_rows: Option<usize>,
    pub columns: Option<usize>,
    pub no_color: bool,
    pub clicolor_force: bool,
    pub unicode_supported: bool,
}

impl TerminalEnvironment {
    pub fn detect() -> Self {
        let stdout_is_terminal = std::io::stdout().is_terminal();
        let stderr_is_terminal = std::io::stderr().is_terminal();
        let (terminal_width, terminal_rows) = if stdout_is_terminal || stderr_is_terminal {
            crossterm::terminal::size()
                .ok()
                .map(|(columns, rows)| (Some(usize::from(columns)), Some(usize::from(rows))))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };
        Self {
            stdout_is_terminal,
            stderr_is_terminal,
            terminal_width,
            terminal_rows,
            columns: std::env::var("COLUMNS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0),
            no_color: std::env::var_os("NO_COLOR").is_some(),
            clicolor_force: std::env::var("CLICOLOR_FORCE")
                .map(|value| !value.is_empty() && value != "0")
                .unwrap_or(false),
            unicode_supported: unicode_environment_supported(),
        }
    }

    pub fn is_terminal(self, channel: OutputChannel) -> bool {
        match channel {
            OutputChannel::Stdout => self.stdout_is_terminal,
            OutputChannel::Stderr => self.stderr_is_terminal,
        }
    }

    pub fn width(self) -> usize {
        self.terminal_width
            .or(self.columns)
            .unwrap_or(DEFAULT_TERMINAL_WIDTH)
            .max(MIN_TERMINAL_WIDTH)
    }
}

fn unicode_environment_supported() -> bool {
    if std::env::var("TERM").is_ok_and(|term| term.eq_ignore_ascii_case("dumb")) {
        return false;
    }
    ["LC_ALL", "LC_CTYPE", "LANG"]
        .into_iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
        .is_some_and(|locale| {
            let normalized = locale.to_ascii_lowercase().replace('-', "");
            normalized.contains("utf8")
        })
}
