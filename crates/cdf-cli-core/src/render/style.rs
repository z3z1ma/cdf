use crate::render::RenderConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Color {
    Success,
    Warning,
    Error,
    Accent,
    Dim,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Emphasis {
    Strong,
    Dim,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Glyphs {
    pub success: &'static str,
    pub warning: &'static str,
    pub error: &'static str,
}

impl Glyphs {
    pub(super) fn for_config(config: &RenderConfig) -> Self {
        if config.rich_glyphs() {
            Self {
                success: "✓",
                warning: "!",
                error: "✗",
            }
        } else {
            Self {
                success: "OK",
                warning: "WARN",
                error: "ERR",
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

pub(super) fn emphasize(
    config: &RenderConfig,
    emphasis: Emphasis,
    text: impl AsRef<str>,
) -> String {
    let text = text.as_ref();
    if !config.color_enabled() {
        return text.to_owned();
    }
    let code = match emphasis {
        Emphasis::Strong => "1",
        Emphasis::Dim => "2",
    };
    format!("\u{1b}[{code}m{text}\u{1b}[0m")
}
