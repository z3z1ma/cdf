use crate::render::{
    RenderConfig,
    style::{Color, Glyphs, paint, plain_rule_char},
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

const MIN_COLUMN_WIDTH: usize = 8;

pub trait RenderPrimitive {
    fn render(&self, config: &RenderConfig) -> String;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RenderDocument {
    blocks: Vec<Block>,
}

impl RenderDocument {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(mut self, block: impl Into<Block>) -> Self {
        self.blocks.push(block.into());
        self
    }

    pub fn blank_line(mut self) -> Self {
        self.blocks.push(Block::BlankLine);
        self
    }

    pub fn text(text: impl Into<String>) -> Self {
        Self::new().push(TextBlock::new(text))
    }

    pub fn render(&self, config: &RenderConfig) -> String {
        let mut output = String::new();
        for block in &self.blocks {
            output.push_str(&block.render(config));
        }
        output
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Block {
    StatusLine(StatusLine),
    KeyValuePanel(KeyValuePanel),
    Table(Table),
    SectionRule(SectionRule),
    NextCommand(NextCommand),
    TextBlock(TextBlock),
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
            Self::TextBlock(text) => text.render(config),
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

impl From<TextBlock> for Block {
    fn from(value: TextBlock) -> Self {
        Self::TextBlock(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextBlock {
    text: String,
}

impl TextBlock {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

impl RenderPrimitive for TextBlock {
    fn render(&self, _config: &RenderConfig) -> String {
        self.text.clone()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusKind {
    Success,
    Warning,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusLine {
    kind: StatusKind,
    message: String,
}

impl StatusLine {
    pub fn new(kind: StatusKind, message: impl Into<String>) -> Self {
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
pub struct KeyValuePanel {
    title: String,
    rows: Vec<(String, String)>,
}

impl KeyValuePanel {
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            rows: Vec::new(),
        }
    }

    pub fn row(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
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
            .map(|(key, _)| display_width(key))
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
pub struct SectionRule;

impl SectionRule {
    pub fn new() -> Self {
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
pub struct NextCommand {
    command: String,
}

impl NextCommand {
    pub fn new(command: impl Into<String>) -> Self {
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
pub struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new<const N: usize>(headers: [impl Into<String>; N]) -> Self {
        Self {
            headers: headers.into_iter().map(Into::into).collect(),
            rows: Vec::new(),
        }
    }

    pub fn from_headers(headers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            headers: headers.into_iter().map(Into::into).collect(),
            rows: Vec::new(),
        }
    }

    pub fn row<const N: usize>(mut self, values: [impl Into<String>; N]) -> Self {
        let values = values.into_iter().map(Into::into).collect::<Vec<_>>();
        assert_eq!(
            values.len(),
            self.headers.len(),
            "renderer table row width must match header width"
        );
        self.rows.push(values);
        self
    }

    pub fn row_values(mut self, values: impl IntoIterator<Item = impl Into<String>>) -> Self {
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
        if grid_would_truncate(&self.headers, &self.rows, config.width()) {
            return stacked_records(&self.headers, &self.rows, config.width());
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
        output.push_str(&row_line(
            &self.headers,
            &widths,
            glyphs.vertical,
            config.rich_glyphs(),
        ));
        output.push_str(&border_line(
            &widths,
            glyphs.tee_right,
            glyphs.cross,
            glyphs.tee_left,
            glyphs.horizontal,
        ));
        for row in &self.rows {
            output.push_str(&row_line(
                row,
                &widths,
                glyphs.vertical,
                config.rich_glyphs(),
            ));
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

fn grid_would_truncate(headers: &[String], rows: &[Vec<String>], width: usize) -> bool {
    let mut natural_widths = headers
        .iter()
        .map(|header| display_width(header))
        .collect::<Vec<_>>();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            natural_widths[index] = natural_widths[index].max(display_width(value));
        }
    }
    let framing = natural_widths.len().saturating_mul(3).saturating_add(1);
    natural_widths.iter().sum::<usize>().saturating_add(framing) > width
}

fn stacked_records(headers: &[String], rows: &[Vec<String>], width: usize) -> String {
    let mut output = String::new();
    for (row_index, row) in rows.iter().enumerate() {
        if row_index > 0 {
            output.push('\n');
        }
        for (header, value) in headers.iter().zip(row) {
            let mut label_chunks = display_chunks(header, width.saturating_sub(1).max(1));
            let last_label = label_chunks.pop().unwrap_or_default();
            for chunk in label_chunks {
                output.push_str(&chunk);
                output.push('\n');
            }
            output.push_str(&last_label);
            output.push_str(":\n");
            for chunk in display_chunks(value, width.saturating_sub(2).max(1)) {
                output.push_str("  ");
                output.push_str(&chunk);
                output.push('\n');
            }
        }
    }
    output
}

fn display_chunks(value: &str, width: usize) -> Vec<String> {
    if value.is_empty() {
        return vec![String::new()];
    }
    let mut chunks = Vec::new();
    let mut chunk = String::new();
    let mut used = 0;
    for character in value.chars() {
        let character_width = character.width().unwrap_or(0);
        if used > 0 && used + character_width > width {
            chunks.push(chunk);
            chunk = String::new();
            used = 0;
        }
        chunk.push(character);
        used += character_width;
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

fn table_widths(headers: &[String], rows: &[Vec<String>], max_width: usize) -> Vec<usize> {
    let mut widths = headers
        .iter()
        .map(|header| display_width(header))
        .collect::<Vec<_>>();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(display_width(value));
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

fn row_line(values: &[String], widths: &[usize], vertical: char, unicode: bool) -> String {
    let mut line = String::new();
    line.push(vertical);
    for (value, width) in values.iter().zip(widths) {
        line.push(' ');
        let value = truncate(value, *width, unicode);
        line.push_str(&pad_right(&value, *width));
        line.push(' ');
        line.push(vertical);
    }
    line.push('\n');
    line
}

fn pad_right(value: &str, width: usize) -> String {
    let current = display_width(value);
    if current >= width {
        return value.to_owned();
    }
    format!("{value}{}", " ".repeat(width - current))
}

fn truncate(value: &str, width: usize, unicode: bool) -> String {
    if display_width(value) <= width {
        return value.to_owned();
    }
    if width <= 1 {
        return if unicode { "…" } else { "~" }.to_owned();
    }
    let marker = if unicode { '…' } else { '~' };
    let marker_width = marker.width().unwrap_or(1);
    let content_width = width.saturating_sub(marker_width);
    let mut output = String::new();
    let mut used = 0;
    for character in value.chars() {
        let character_width = character.width().unwrap_or(0);
        if used + character_width > content_width {
            break;
        }
        output.push(character);
        used += character_width;
    }
    output.push(marker);
    output
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}
