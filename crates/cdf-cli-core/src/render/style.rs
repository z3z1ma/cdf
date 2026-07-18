use crate::render::{RenderConfig, config::DisplayMode};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Color {
    Success,
    Warning,
    Error,
    Accent,
    Dim,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Glyphs {
    pub success: &'static str,
    pub warning: &'static str,
    pub error: &'static str,
    pub flow: &'static str,
    pub active: &'static str,
    pub section: &'static str,
    pub horizontal: char,
    pub top_left: char,
    pub top_right: char,
    pub bottom_left: char,
    pub bottom_right: char,
    pub tee_down: char,
    pub tee_up: char,
    pub tee_left: char,
    pub tee_right: char,
    pub cross: char,
    pub vertical: char,
}

impl Glyphs {
    pub(super) fn for_config(config: &RenderConfig) -> Self {
        if config.rich_glyphs() {
            Self {
                success: "✓",
                warning: "!",
                error: "✗",
                flow: "→",
                active: "●",
                section: "◆",
                horizontal: '─',
                top_left: '┌',
                top_right: '┐',
                bottom_left: '└',
                bottom_right: '┘',
                tee_down: '┬',
                tee_up: '┴',
                tee_left: '┤',
                tee_right: '├',
                cross: '┼',
                vertical: '│',
            }
        } else {
            Self {
                success: "OK",
                warning: "WARN",
                error: "ERR",
                flow: "->",
                active: "*",
                section: "#",
                horizontal: '-',
                top_left: '+',
                top_right: '+',
                bottom_left: '+',
                bottom_right: '+',
                tee_down: '+',
                tee_up: '+',
                tee_left: '+',
                tee_right: '+',
                cross: '+',
                vertical: '|',
            }
        }
    }
}

pub(super) fn paint(config: &RenderConfig, color: Color, text: impl AsRef<str>) -> String {
    let text = text.as_ref();
    if !config.color_enabled() {
        return text.to_owned();
    }
    let code = match color {
        Color::Success => "32",
        Color::Warning => "33",
        Color::Error => "31",
        Color::Accent => "36",
        Color::Dim => "2",
    };
    format!("\u{1b}[{code}m{text}\u{1b}[0m")
}

pub(super) fn plain_rule_char(config: &RenderConfig) -> char {
    match config.display_mode() {
        DisplayMode::Tty => '━',
        DisplayMode::Headless => '-',
    }
}
