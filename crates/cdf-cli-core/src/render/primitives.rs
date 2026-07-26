use crate::{
    render::{
        RenderConfig,
        style::{Color, Emphasis, Glyphs, emphasize, paint},
    },
    terminal::Verbosity,
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

const MIN_COLUMN_WIDTH: usize = 8;

pub trait RenderPrimitive {
    fn render(&self, config: &RenderConfig) -> String;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Visibility {
    Always,
    Normal,
    Verbose,
    Diagnostic,
}

impl Visibility {
    fn visible(self, verbosity: Verbosity) -> bool {
        match (self, verbosity) {
            (Self::Always, _) => true,
            (Self::Normal, Verbosity::Normal | Verbosity::Verbose(_)) => true,
            (Self::Verbose, Verbosity::Verbose(_)) => true,
            (Self::Diagnostic, Verbosity::Verbose(level)) => level >= 2,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DocumentBlock {
    block: Block,
    visibility: Visibility,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RenderDocument {
    blocks: Vec<DocumentBlock>,
}

impl RenderDocument {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(mut self, block: impl Into<Block>) -> Self {
        let block = block.into();
        let visibility = block.default_visibility();
        self.blocks.push(DocumentBlock { block, visibility });
        self
    }

    pub fn push_verbose(mut self, block: impl Into<Block>) -> Self {
        self.blocks.push(DocumentBlock {
            block: block.into(),
            visibility: Visibility::Verbose,
        });
        self
    }

    pub fn push_diagnostic(mut self, block: impl Into<Block>) -> Self {
        self.blocks.push(DocumentBlock {
            block: block.into(),
            visibility: Visibility::Diagnostic,
        });
        self
    }

    pub fn blank_line(mut self) -> Self {
        self.blocks.push(DocumentBlock {
            block: Block::BlankLine,
            visibility: Visibility::Normal,
        });
        self
    }

    pub fn text(text: impl Into<String>) -> Self {
        Self::new().push(TextBlock::new(text))
    }

    pub fn render(&self, config: &RenderConfig) -> String {
        let mut output = String::new();
        let mut blank_pending = false;
        for document_block in &self.blocks {
            if !document_block.visibility.visible(config.verbosity()) {
                continue;
            }
            if matches!(document_block.block, Block::BlankLine) {
                blank_pending = !output.is_empty();
                continue;
            }
            let rendered = document_block.block.render(config);
            if rendered.is_empty() {
                continue;
            }
            if blank_pending && !output.ends_with("\n\n") {
                if !output.ends_with('\n') {
                    output.push('\n');
                }
                output.push('\n');
            }
            blank_pending = false;
            output.push_str(&rendered);
        }
        output
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Block {
    StatusLine(StatusLine),
    ActivityLine(ActivityLine),
    KeyValuePanel(KeyValuePanel),
    Table(Table),
    ErrorBlock(ErrorBlock),
    NextCommand(NextCommand),
    TextBlock(TextBlock),
    BlankLine,
}

impl Block {
    fn default_visibility(&self) -> Visibility {
        match self {
            Self::StatusLine(_) | Self::Table(_) | Self::ErrorBlock(_) | Self::TextBlock(_) => {
                Visibility::Always
            }
            Self::ActivityLine(_)
            | Self::KeyValuePanel(_)
            | Self::NextCommand(_)
            | Self::BlankLine => Visibility::Normal,
        }
    }

    fn render(&self, config: &RenderConfig) -> String {
        match self {
            Self::StatusLine(line) => line.render(config),
            Self::ActivityLine(line) => line.render(config),
            Self::KeyValuePanel(panel) => panel.render(config),
            Self::Table(table) => table.render(config),
            Self::ErrorBlock(error) => error.render(config),
            Self::NextCommand(command) => command.render(config),
            Self::TextBlock(text) => text.render(config),
            Self::BlankLine => String::new(),
        }
    }
}

macro_rules! block_from {
    ($type:ty, $variant:ident) => {
        impl From<$type> for Block {
            fn from(value: $type) -> Self {
                Self::$variant(value)
            }
        }
    };
}

block_from!(StatusLine, StatusLine);
block_from!(ActivityLine, ActivityLine);
block_from!(KeyValuePanel, KeyValuePanel);
block_from!(Table, Table);
block_from!(ErrorBlock, ErrorBlock);
block_from!(NextCommand, NextCommand);
block_from!(TextBlock, TextBlock);

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
        render_wrapped_prefixed(
            config,
            &paint(config, color, glyph),
            display_width(glyph),
            &self.message,
            1,
            WrappedBodyStyle::Emphasis(Emphasis::Strong),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityState {
    Active,
    Complete,
    Warning,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivityLine {
    state: ActivityState,
    verb: String,
    detail: String,
    metrics: Vec<String>,
}

impl ActivityLine {
    pub fn new(state: ActivityState, verb: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            state,
            verb: verb.into(),
            detail: detail.into(),
            metrics: Vec::new(),
        }
    }

    pub fn metric(mut self, metric: impl Into<String>) -> Self {
        self.metrics.push(metric.into());
        self
    }
}

impl RenderPrimitive for ActivityLine {
    fn render(&self, config: &RenderConfig) -> String {
        let color = match self.state {
            ActivityState::Active => Color::Accent,
            ActivityState::Complete => Color::Success,
            ActivityState::Warning => Color::Warning,
            ActivityState::Failed => Color::Error,
        };
        let separator = if config.rich_glyphs() { " · " } else { " | " };
        let mut body = self.detail.clone();
        if !self.metrics.is_empty() {
            if !body.is_empty() {
                body.push_str(separator);
            }
            body.push_str(&self.metrics.join(separator));
        }
        let verb_width = 10.min(config.width().saturating_sub(2));
        let verb = pad_left(&self.verb, verb_width);
        render_wrapped_prefixed(
            config,
            &paint(config, color, verb),
            verb_width,
            &body,
            2,
            WrappedBodyStyle::Plain,
        )
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
        if !self.title.is_empty() {
            for chunk in display_chunks(&self.title, config.width()) {
                output.push_str(&emphasize(config, Emphasis::Strong, chunk));
                output.push('\n');
            }
        }
        let natural_key_width = self
            .rows
            .iter()
            .map(|(key, _)| display_width(key))
            .max()
            .unwrap_or(0);
        let key_width = natural_key_width.min((config.width() / 3).max(MIN_COLUMN_WIDTH));
        for (key, value) in &self.rows {
            if display_width(key) > key_width {
                for chunk in display_chunks(key, config.width().saturating_sub(2).max(1)) {
                    output.push_str("  ");
                    output.push_str(&paint(config, Color::Dim, chunk));
                    output.push('\n');
                }
                render_indented_value(&mut output, value, config.width(), 4);
                continue;
            }
            let prefix = format!("  {}  ", pad_right(key, key_width));
            let prefix_width = display_width(&prefix);
            if prefix_width + display_width(value) <= config.width() {
                output.push_str(&paint(config, Color::Dim, prefix));
                output.push_str(value);
                output.push('\n');
                continue;
            }
            output.push_str(&paint(config, Color::Dim, format!("  {key}")));
            output.push('\n');
            render_indented_value(&mut output, value, config.width(), 4);
        }
        output
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
        format!(
            "{} {}\n",
            paint(config, Color::Dim, "Next:"),
            paint(config, Color::Accent, &self.command)
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorBlock {
    code: String,
    message: String,
    details: Vec<(String, String)>,
    help: Vec<String>,
    suggestions: Vec<String>,
}

impl ErrorBlock {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: Vec::new(),
            help: Vec::new(),
            suggestions: Vec::new(),
        }
    }

    pub fn detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.push((key.into(), value.into()));
        self
    }

    pub fn help(mut self, help: impl Into<String>) -> Self {
        self.help.push(help.into());
        self
    }

    pub fn suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.suggestions.push(suggestion.into());
        self
    }
}

impl RenderPrimitive for ErrorBlock {
    fn render(&self, config: &RenderConfig) -> String {
        let label = format!("error[{}]", self.code);
        let mut output = format!("{}: {}\n", paint(config, Color::Error, label), self.message);
        for (key, value) in &self.details {
            output.push_str(&paint(config, Color::Dim, format!("{key}:")));
            output.push(' ');
            output.push_str(value);
            output.push('\n');
        }
        for help in &self.help {
            output.push_str(&paint(config, Color::Accent, "help:"));
            output.push(' ');
            output.push_str(help);
            output.push('\n');
        }
        for suggestion in &self.suggestions {
            output.push_str(&paint(config, Color::Accent, "try:"));
            output.push(' ');
            output.push_str(suggestion);
            output.push('\n');
        }
        output
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
        if table_would_overflow(&self.headers, &self.rows, config.width()) {
            return stacked_records(&self.headers, &self.rows, config);
        }
        let widths = natural_table_widths(&self.headers, &self.rows);
        let mut output = String::new();
        output.push_str(&aligned_row(
            &self.headers,
            &widths,
            Some((config, Emphasis::Dim)),
        ));
        for row in &self.rows {
            output.push_str(&aligned_row(row, &widths, None));
        }
        output
    }
}

fn table_would_overflow(headers: &[String], rows: &[Vec<String>], width: usize) -> bool {
    let widths = natural_table_widths(headers, rows);
    let spacing = widths.len().saturating_sub(1).saturating_mul(2);
    widths.iter().sum::<usize>().saturating_add(spacing) > width
}

fn natural_table_widths(headers: &[String], rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths = headers
        .iter()
        .map(|header| display_width(header).max(MIN_COLUMN_WIDTH.min(display_width(header))))
        .collect::<Vec<_>>();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(display_width(value));
        }
    }
    widths
}

fn aligned_row(
    values: &[String],
    widths: &[usize],
    emphasis: Option<(&RenderConfig, Emphasis)>,
) -> String {
    let mut line = String::new();
    for (index, (value, width)) in values.iter().zip(widths).enumerate() {
        if index > 0 {
            line.push_str("  ");
        }
        line.push_str(&pad_right(value, *width));
    }
    if let Some((config, emphasis)) = emphasis {
        line = emphasize(config, emphasis, line);
    }
    line.push('\n');
    line
}

fn stacked_records(headers: &[String], rows: &[Vec<String>], config: &RenderConfig) -> String {
    let mut output = String::new();
    for (row_index, row) in rows.iter().enumerate() {
        if row_index > 0 {
            output.push('\n');
        }
        for (header, value) in headers.iter().zip(row) {
            for chunk in display_chunks(header, config.width().saturating_sub(1).max(1)) {
                output.push_str(&emphasize(config, Emphasis::Dim, format!("{chunk}:")));
                output.push('\n');
            }
            for chunk in display_chunks(value, config.width().saturating_sub(2).max(1)) {
                output.push_str("  ");
                output.push_str(&chunk);
                output.push('\n');
            }
        }
    }
    output
}

#[derive(Clone, Copy)]
enum WrappedBodyStyle {
    Plain,
    Emphasis(Emphasis),
}

fn render_wrapped_prefixed(
    config: &RenderConfig,
    painted_prefix: &str,
    plain_prefix_width: usize,
    body: &str,
    gap: usize,
    style: WrappedBodyStyle,
) -> String {
    let indent = plain_prefix_width
        .saturating_add(gap)
        .min(config.width().saturating_sub(1));
    let body_width = config.width().saturating_sub(indent).max(1);
    let mut chunks = display_chunks(body, body_width).into_iter();
    let mut output = painted_prefix.to_owned();
    if let Some(first) = chunks.next() {
        output.push_str(&" ".repeat(gap));
        output.push_str(&style_wrapped_body(config, style, first));
    }
    output.push('\n');
    for chunk in chunks {
        output.push_str(&" ".repeat(indent));
        output.push_str(&style_wrapped_body(config, style, chunk));
        output.push('\n');
    }
    output
}

fn style_wrapped_body(config: &RenderConfig, style: WrappedBodyStyle, chunk: String) -> String {
    match style {
        WrappedBodyStyle::Plain => chunk,
        WrappedBodyStyle::Emphasis(emphasis) => emphasize(config, emphasis, chunk),
    }
}

fn render_indented_value(output: &mut String, value: &str, width: usize, indent: usize) {
    let indent = indent.min(width.saturating_sub(1));
    for chunk in display_chunks(value, width.saturating_sub(indent).max(1)) {
        output.push_str(&" ".repeat(indent));
        output.push_str(&chunk);
        output.push('\n');
    }
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

fn pad_left(value: &str, width: usize) -> String {
    let current = display_width(value);
    if current >= width {
        return value.to_owned();
    }
    format!("{}{value}", " ".repeat(width - current))
}

fn pad_right(value: &str, width: usize) -> String {
    let current = display_width(value);
    if current >= width {
        return value.to_owned();
    }
    format!("{value}{}", " ".repeat(width - current))
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}
