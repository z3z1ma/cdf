use crate::render::{
    RenderConfig,
    style::{Color, Glyphs, paint, plain_rule_char},
};

const MIN_COLUMN_WIDTH: usize = 8;

pub(crate) trait RenderPrimitive {
    fn render(&self, config: &RenderConfig) -> String;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RenderDocument {
    blocks: Vec<Block>,
}

impl RenderDocument {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push(mut self, block: impl Into<Block>) -> Self {
        self.blocks.push(block.into());
        self
    }

    pub(crate) fn blank_line(mut self) -> Self {
        self.blocks.push(Block::BlankLine);
        self
    }

    pub(crate) fn render(&self, config: &RenderConfig) -> String {
        let mut output = String::new();
        for block in &self.blocks {
            output.push_str(&block.render(config));
        }
        output
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Block {
    StatusLine(StatusLine),
    KeyValuePanel(KeyValuePanel),
    Table(Table),
    SectionRule(SectionRule),
    NextCommand(NextCommand),
    BlankLine,
}

impl Block {
    fn render(&self, config: &RenderConfig) -> String {
        match self {
            Self::StatusLine(line) => line.render(config),
            Self::KeyValuePanel(panel) => panel.render(config),
            Self::Table(table) => table.render(config),
            Self::SectionRule(rule) => rule.render(config),
            Self::NextCommand(command) => command.render(config),
            Self::BlankLine => "\n".to_owned(),
        }
    }
}

impl From<StatusLine> for Block {
    fn from(value: StatusLine) -> Self {
        Self::StatusLine(value)
    }
}

impl From<KeyValuePanel> for Block {
    fn from(value: KeyValuePanel) -> Self {
        Self::KeyValuePanel(value)
    }
}

impl From<Table> for Block {
    fn from(value: Table) -> Self {
        Self::Table(value)
    }
}

impl From<SectionRule> for Block {
    fn from(value: SectionRule) -> Self {
        Self::SectionRule(value)
    }
}

impl From<NextCommand> for Block {
    fn from(value: NextCommand) -> Self {
        Self::NextCommand(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StatusKind {
    Success,
    Warning,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StatusLine {
    kind: StatusKind,
    message: String,
}

impl StatusLine {
    pub(crate) fn new(kind: StatusKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl RenderPrimitive for StatusLine {
    fn render(&self, config: &RenderConfig) -> String {
        let glyphs = Glyphs::for_config(config);
        let (glyph, color) = match self.kind {
            StatusKind::Success => (glyphs.success, Color::Success),
            StatusKind::Warning => (glyphs.warning, Color::Warning),
            StatusKind::Error => (glyphs.error, Color::Error),
        };
        format!("{} {}\n", paint(config, color, glyph), self.message)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeyValuePanel {
    title: String,
    rows: Vec<(String, String)>,
}

impl KeyValuePanel {
    pub(crate) fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            rows: Vec::new(),
        }
    }

    pub(crate) fn row(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.rows.push((key.into(), value.into()));
        self
    }
}

impl RenderPrimitive for KeyValuePanel {
    fn render(&self, config: &RenderConfig) -> String {
        let mut output = String::new();
        output.push_str(&paint(config, Color::Accent, &self.title));
        output.push('\n');
        let key_width = self
            .rows
            .iter()
            .map(|(key, _)| key.len())
            .max()
            .unwrap_or(0);
        for (key, value) in &self.rows {
            output.push_str("  ");
            output.push_str(&pad_right(key, key_width));
            output.push_str("  ");
            output.push_str(value);
            output.push('\n');
        }
        output
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SectionRule;

impl SectionRule {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl RenderPrimitive for SectionRule {
    fn render(&self, config: &RenderConfig) -> String {
        let rule = std::iter::repeat_n(plain_rule_char(config), config.width()).collect::<String>();
        format!("{}\n", paint(config, Color::Accent, rule))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NextCommand {
    command: String,
}

impl NextCommand {
    pub(crate) fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
        }
    }
}

impl RenderPrimitive for NextCommand {
    fn render(&self, config: &RenderConfig) -> String {
        let glyphs = Glyphs::for_config(config);
        format!(
            "{} {}\n",
            paint(config, Color::Accent, glyphs.flow),
            self.command
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub(crate) fn new<const N: usize>(headers: [impl Into<String>; N]) -> Self {
        Self {
            headers: headers.into_iter().map(Into::into).collect(),
            rows: Vec::new(),
        }
    }

    pub(crate) fn row<const N: usize>(mut self, values: [impl Into<String>; N]) -> Self {
        let values = values.into_iter().map(Into::into).collect::<Vec<_>>();
        assert_eq!(
            values.len(),
            self.headers.len(),
            "renderer table row width must match header width"
        );
        self.rows.push(values);
        self
    }
}

impl RenderPrimitive for Table {
    fn render(&self, config: &RenderConfig) -> String {
        if self.headers.is_empty() {
            return String::new();
        }
        let glyphs = Glyphs::for_config(config);
        let widths = table_widths(&self.headers, &self.rows, config.width());
        let mut output = String::new();
        output.push_str(&border_line(
            &widths,
            glyphs.top_left,
            glyphs.tee_down,
            glyphs.top_right,
            glyphs.horizontal,
        ));
        output.push_str(&row_line(&self.headers, &widths, glyphs.vertical));
        output.push_str(&border_line(
            &widths,
            glyphs.tee_right,
            glyphs.cross,
            glyphs.tee_left,
            glyphs.horizontal,
        ));
        for row in &self.rows {
            output.push_str(&row_line(row, &widths, glyphs.vertical));
        }
        output.push_str(&border_line(
            &widths,
            glyphs.bottom_left,
            glyphs.tee_up,
            glyphs.bottom_right,
            glyphs.horizontal,
        ));
        output
    }
}

fn table_widths(headers: &[String], rows: &[Vec<String>], max_width: usize) -> Vec<usize> {
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.len());
        }
    }
    let overhead = widths.len() * 3 + 1;
    let available = max_width.saturating_sub(overhead).max(widths.len());
    while widths.iter().sum::<usize>() > available {
        let Some((index, width)) = widths
            .iter()
            .enumerate()
            .filter(|(_, width)| **width > MIN_COLUMN_WIDTH)
            .max_by_key(|(_, width)| **width)
        else {
            break;
        };
        widths[index] = width - 1;
    }
    widths
}

fn border_line(
    widths: &[usize],
    left: char,
    separator: char,
    right: char,
    horizontal: char,
) -> String {
    let mut line = String::new();
    line.push(left);
    for (index, width) in widths.iter().enumerate() {
        line.extend(std::iter::repeat_n(horizontal, width + 2));
        if index == widths.len() - 1 {
            line.push(right);
        } else {
            line.push(separator);
        }
    }
    line.push('\n');
    line
}

fn row_line(values: &[String], widths: &[usize], vertical: char) -> String {
    let mut line = String::new();
    line.push(vertical);
    for (value, width) in values.iter().zip(widths) {
        line.push(' ');
        let value = truncate(value, *width);
        line.push_str(&pad_right(&value, *width));
        line.push(' ');
        line.push(vertical);
    }
    line.push('\n');
    line
}

fn pad_right(value: &str, width: usize) -> String {
    let current = value.chars().count();
    if current >= width {
        return value.to_owned();
    }
    format!("{value}{}", " ".repeat(width - current))
}

fn truncate(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_owned();
    }
    if width <= 1 {
        return "…".to_owned();
    }
    let mut output = value.chars().take(width - 1).collect::<String>();
    output.push('…');
    output
}
