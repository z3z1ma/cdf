use std::collections::BTreeSet;

use cdf_declarative::{
    DrainTerminationDeclaration, EpochClosureDeclaration, ExecutionDeclaration,
    LateDataDeclaration, SafeFrontierDeclaration, WatermarkDeclaration,
};
use cdf_engine::ProjectSqlSpan;
use cdf_kernel::{CdfError, Result, TargetName};
use serde::{Deserialize, Serialize};

use crate::TrustPreset;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthoredResourceForm {
    BareSelect,
    ResourceEnvelope,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpannedResourceValue<T> {
    pub value: T,
    pub span: ProjectSqlSpan,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthoredDisposition {
    Append,
    Replace,
    Merge {
        keys: Vec<SpannedResourceValue<String>>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoredSemanticBinding {
    pub field: SpannedResourceValue<String>,
    pub reference: SpannedResourceValue<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoredResourceEnvelope {
    pub target: Option<SpannedResourceValue<TargetName>>,
    pub disposition: Option<SpannedResourceValue<AuthoredDisposition>>,
    pub cursor: Option<SpannedResourceValue<String>>,
    pub trust: Option<SpannedResourceValue<TrustPreset>>,
    pub semantics: Vec<AuthoredSemanticBinding>,
    pub execution: Option<SpannedResourceValue<ExecutionDeclaration>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoredResourceFile {
    pub form: AuthoredResourceForm,
    pub envelope: AuthoredResourceEnvelope,
    pub query_sql: String,
    pub query_span: ProjectSqlSpan,
}

pub fn parse_resource_file(sql: &str, file: &str) -> Result<AuthoredResourceFile> {
    let tokens = lex_envelope_prefix(sql, file)?;
    let first = tokens.first().ok_or_else(|| {
        resource_sql_error(
            "CDF-RESOURCE-EMPTY",
            file,
            None,
            "resource file must contain one SELECT query",
        )
    })?;
    if first.is_word("SELECT") {
        return Ok(AuthoredResourceFile {
            form: AuthoredResourceForm::BareSelect,
            envelope: AuthoredResourceEnvelope::default(),
            query_sql: sql[first.start..].to_owned(),
            query_span: first.span.clone(),
        });
    }
    if !first.is_word("RESOURCE") {
        return Err(resource_sql_error(
            "CDF-RESOURCE-FORM",
            file,
            Some(&first.span),
            "resource file must begin with SELECT or the no-identifier RESOURCE envelope",
        ));
    }

    let mut parser = EnvelopeParser::new(file, tokens, 1);
    let mut envelope = AuthoredResourceEnvelope::default();
    let mut previous_rank = 0_u8;
    loop {
        let token = parser.peek()?.clone();
        if token.is_word("AS") {
            parser.advance();
            break;
        }
        let (rank, clause) = clause_rank(&token).ok_or_else(|| {
            resource_sql_error(
                "CDF-RESOURCE-CLAUSE",
                file,
                Some(&token.span),
                "RESOURCE takes no identifier; expected TARGET, DISPOSITION, CURSOR, TRUST, SEMANTICS, EXECUTION, or AS",
            )
        })?;
        if rank <= previous_rank {
            return Err(resource_sql_error(
                "CDF-RESOURCE-CLAUSE-ORDER",
                file,
                Some(&token.span),
                format!(
                    "{clause} is repeated or out of order; canonical order is TARGET, DISPOSITION, CURSOR, TRUST, SEMANTICS, EXECUTION"
                ),
            ));
        }
        previous_rank = rank;
        parser.advance();
        match clause {
            "TARGET" => envelope.target = Some(parser.parse_target(&token.span)?),
            "DISPOSITION" => envelope.disposition = Some(parser.parse_disposition(&token.span)?),
            "CURSOR" => envelope.cursor = Some(parser.parse_name("cursor column")?),
            "TRUST" => envelope.trust = Some(parser.parse_trust()?),
            "SEMANTICS" => envelope.semantics = parser.parse_semantics()?,
            "EXECUTION" => envelope.execution = Some(parser.parse_execution(&token.span)?),
            _ => {
                return Err(CdfError::internal(
                    "recognized envelope clause lost its parser",
                ));
            }
        }
    }
    let query = parser.peek()?.clone();
    if !query.is_word("SELECT") {
        return Err(resource_sql_error(
            "CDF-RESOURCE-AS",
            file,
            Some(&query.span),
            "RESOURCE AS must be followed by one SELECT query",
        ));
    }
    Ok(AuthoredResourceFile {
        form: AuthoredResourceForm::ResourceEnvelope,
        envelope,
        query_sql: sql[query.start..].to_owned(),
        query_span: query.span,
    })
}

fn clause_rank(token: &Token) -> Option<(u8, &'static str)> {
    [
        (1, "TARGET"),
        (2, "DISPOSITION"),
        (3, "CURSOR"),
        (4, "TRUST"),
        (5, "SEMANTICS"),
        (6, "EXECUTION"),
    ]
    .into_iter()
    .find(|(_, clause)| token.is_word(clause))
}

struct EnvelopeParser<'a> {
    file: &'a str,
    tokens: Vec<Token>,
    cursor: usize,
}

impl<'a> EnvelopeParser<'a> {
    fn new(file: &'a str, tokens: Vec<Token>, cursor: usize) -> Self {
        Self {
            file,
            tokens,
            cursor,
        }
    }

    fn peek(&self) -> Result<&Token> {
        self.tokens.get(self.cursor).ok_or_else(|| {
            resource_sql_error(
                "CDF-RESOURCE-INCOMPLETE",
                self.file,
                None,
                "resource envelope ended before AS SELECT",
            )
        })
    }

    fn advance(&mut self) {
        self.cursor += 1;
    }

    fn take(&mut self) -> Result<Token> {
        let token = self.peek()?.clone();
        self.advance();
        Ok(token)
    }

    fn expect_word(&mut self, expected: &str, code: &str) -> Result<Token> {
        let token = self.take()?;
        if !token.is_word(expected) {
            return Err(resource_sql_error(
                code,
                self.file,
                Some(&token.span),
                format!("expected {expected}"),
            ));
        }
        Ok(token)
    }

    fn expect_punctuation(&mut self, expected: char, code: &str) -> Result<Token> {
        let token = self.take()?;
        if token.kind != TokenKind::Punctuation(expected) {
            return Err(resource_sql_error(
                code,
                self.file,
                Some(&token.span),
                format!("expected {expected}"),
            ));
        }
        Ok(token)
    }

    fn parse_name(&mut self, label: &str) -> Result<SpannedResourceValue<String>> {
        let token = self.take()?;
        let TokenKind::Word(value) = token.kind else {
            return Err(resource_sql_error(
                "CDF-RESOURCE-NAME",
                self.file,
                Some(&token.span),
                format!("{label} must be an unquoted identifier"),
            ));
        };
        Ok(SpannedResourceValue {
            value,
            span: token.span,
        })
    }

    fn parse_target(
        &mut self,
        clause_span: &ProjectSqlSpan,
    ) -> Result<SpannedResourceValue<TargetName>> {
        let first = self.parse_name("logical target")?;
        let mut value = first.value;
        let mut span = first.span;
        while self
            .tokens
            .get(self.cursor)
            .is_some_and(|token| token.kind == TokenKind::Punctuation('.'))
        {
            self.advance();
            let component = self.parse_name("logical target component")?;
            value.push('.');
            value.push_str(&component.value);
            span.end_line = component.span.end_line;
            span.end_column = component.span.end_column;
        }
        let value = TargetName::new(value).map_err(|error| {
            resource_sql_error(
                "CDF-RESOURCE-TARGET",
                self.file,
                Some(clause_span),
                error.message,
            )
        })?;
        Ok(SpannedResourceValue { value, span })
    }

    fn parse_disposition(
        &mut self,
        clause_span: &ProjectSqlSpan,
    ) -> Result<SpannedResourceValue<AuthoredDisposition>> {
        let kind = self.take()?;
        let value = if kind.is_word("APPEND") {
            AuthoredDisposition::Append
        } else if kind.is_word("REPLACE") {
            AuthoredDisposition::Replace
        } else if kind.is_word("MERGE") {
            self.expect_punctuation('(', "CDF-RESOURCE-MERGE")?;
            let mut keys = Vec::new();
            let mut seen = BTreeSet::new();
            loop {
                if self
                    .tokens
                    .get(self.cursor)
                    .is_some_and(|token| token.kind == TokenKind::Punctuation(')'))
                {
                    if keys.is_empty() {
                        return Err(resource_sql_error(
                            "CDF-RESOURCE-MERGE-EMPTY",
                            self.file,
                            Some(&kind.span),
                            "DISPOSITION MERGE requires at least one output key",
                        ));
                    }
                    self.advance();
                    break;
                }
                let key = self.parse_name("merge key")?;
                if !seen.insert(key.value.clone()) {
                    return Err(resource_sql_error(
                        "CDF-RESOURCE-MERGE-DUPLICATE",
                        self.file,
                        Some(&key.span),
                        format!("merge key {:?} appears more than once", key.value),
                    ));
                }
                keys.push(key);
                let separator = self.take()?;
                match separator.kind {
                    TokenKind::Punctuation(',') => {}
                    TokenKind::Punctuation(')') => break,
                    _ => {
                        return Err(resource_sql_error(
                            "CDF-RESOURCE-MERGE",
                            self.file,
                            Some(&separator.span),
                            "expected comma or closing parenthesis after merge key",
                        ));
                    }
                }
            }
            AuthoredDisposition::Merge { keys }
        } else {
            return Err(resource_sql_error(
                "CDF-RESOURCE-DISPOSITION",
                self.file,
                Some(&kind.span),
                "DISPOSITION must be APPEND, REPLACE, or MERGE(key, ...)",
            ));
        };
        Ok(SpannedResourceValue {
            value,
            span: union_span(clause_span, &kind.span),
        })
    }

    fn parse_trust(&mut self) -> Result<SpannedResourceValue<TrustPreset>> {
        let token = self.take()?;
        let value = if token.is_word("EXPERIMENTAL") {
            TrustPreset::Experimental
        } else if token.is_word("GOVERNED") {
            TrustPreset::Governed
        } else if token.is_word("FINANCIAL") {
            TrustPreset::Financial
        } else if token.is_word("SERVING") {
            TrustPreset::Serving
        } else {
            return Err(resource_sql_error(
                "CDF-RESOURCE-TRUST",
                self.file,
                Some(&token.span),
                "TRUST must be EXPERIMENTAL, GOVERNED, FINANCIAL, or SERVING",
            ));
        };
        Ok(SpannedResourceValue {
            value,
            span: token.span,
        })
    }

    fn parse_semantics(&mut self) -> Result<Vec<AuthoredSemanticBinding>> {
        self.expect_punctuation('(', "CDF-RESOURCE-SEMANTICS")?;
        let mut bindings = Vec::new();
        let mut seen = BTreeSet::new();
        loop {
            if self
                .tokens
                .get(self.cursor)
                .is_some_and(|token| token.kind == TokenKind::Punctuation(')'))
            {
                if bindings.is_empty() {
                    return Err(resource_sql_error(
                        "CDF-RESOURCE-SEMANTICS-EMPTY",
                        self.file,
                        Some(&self.peek()?.span),
                        "SEMANTICS requires at least one field binding",
                    ));
                }
                self.advance();
                break;
            }
            let field = self.parse_name("semantic output field")?;
            if !seen.insert(field.value.clone()) {
                return Err(resource_sql_error(
                    "CDF-RESOURCE-SEMANTICS-DUPLICATE",
                    self.file,
                    Some(&field.span),
                    format!("semantic field {:?} appears more than once", field.value),
                ));
            }
            let arrow = self.take()?;
            if arrow.kind != TokenKind::RightArrow {
                return Err(resource_sql_error(
                    "CDF-RESOURCE-SEMANTICS",
                    self.file,
                    Some(&arrow.span),
                    "semantic bindings use field => 'canonical.reference'",
                ));
            }
            let reference = self.take()?;
            let TokenKind::String(reference_value) = reference.kind else {
                return Err(resource_sql_error(
                    "CDF-RESOURCE-SEMANTICS-REFERENCE",
                    self.file,
                    Some(&reference.span),
                    "semantic reference must be a single-quoted string literal",
                ));
            };
            bindings.push(AuthoredSemanticBinding {
                field,
                reference: SpannedResourceValue {
                    value: reference_value,
                    span: reference.span,
                },
            });
            let separator = self.take()?;
            match separator.kind {
                TokenKind::Punctuation(',') => {}
                TokenKind::Punctuation(')') => break,
                _ => {
                    return Err(resource_sql_error(
                        "CDF-RESOURCE-SEMANTICS",
                        self.file,
                        Some(&separator.span),
                        "expected comma or closing parenthesis after semantic binding",
                    ));
                }
            }
        }
        Ok(bindings)
    }

    fn parse_execution(
        &mut self,
        clause_span: &ProjectSqlSpan,
    ) -> Result<SpannedResourceValue<ExecutionDeclaration>> {
        let mode = self.take()?;
        let value = if mode.is_word("BOUNDED") {
            ExecutionDeclaration::Bounded
        } else if mode.is_word("DRAIN") {
            self.parse_drain()?
        } else {
            return Err(resource_sql_error(
                "CDF-RESOURCE-EXECUTION",
                self.file,
                Some(&mode.span),
                "EXECUTION must be BOUNDED or a complete DRAIN policy",
            ));
        };
        Ok(SpannedResourceValue {
            value,
            span: union_span(clause_span, &mode.span),
        })
    }

    fn parse_drain(&mut self) -> Result<ExecutionDeclaration> {
        self.expect_punctuation('(', "CDF-RESOURCE-DRAIN")?;
        self.expect_word("CHECKPOINT", "CDF-RESOURCE-DRAIN-CHECKPOINT")?;
        let checkpoint_cadence = self.parse_epoch_trigger()?;
        self.expect_punctuation(',', "CDF-RESOURCE-DRAIN")?;
        self.expect_word("PACKAGE", "CDF-RESOURCE-DRAIN-PACKAGE")?;
        let package_rotation = self.parse_epoch_trigger()?;
        self.expect_punctuation(',', "CDF-RESOURCE-DRAIN")?;
        self.expect_word("UNTIL", "CDF-RESOURCE-DRAIN-UNTIL")?;
        let termination = Box::new(self.parse_termination()?);
        self.expect_punctuation(',', "CDF-RESOURCE-DRAIN")?;
        self.expect_word("WATERMARK", "CDF-RESOURCE-DRAIN-WATERMARK")?;
        self.expect_word("DISABLED", "CDF-RESOURCE-DRAIN-WATERMARK")?;
        self.expect_punctuation(',', "CDF-RESOURCE-DRAIN")?;
        self.expect_word("LATE", "CDF-RESOURCE-DRAIN-LATE-DATA")?;
        self.expect_word("DATA", "CDF-RESOURCE-DRAIN-LATE-DATA")?;
        let late_data = self.parse_late_data()?;
        self.expect_punctuation(',', "CDF-RESOURCE-DRAIN")?;
        self.expect_word("SAFE", "CDF-RESOURCE-DRAIN-SAFE-FRONTIER")?;
        self.expect_word("FRONTIER", "CDF-RESOURCE-DRAIN-SAFE-FRONTIER")?;
        self.expect_word("CANONICAL", "CDF-RESOURCE-DRAIN-SAFE-FRONTIER")?;
        self.expect_word("ADMITTED", "CDF-RESOURCE-DRAIN-SAFE-FRONTIER")?;
        self.expect_word("SOURCE", "CDF-RESOURCE-DRAIN-SAFE-FRONTIER")?;
        self.expect_word("POSITION", "CDF-RESOURCE-DRAIN-SAFE-FRONTIER")?;
        // Optional trailing CDC member. Only change-data-capture resources bound a settlement unit,
        // so requiring it everywhere would force every finite drain to declare an irrelevant value.
        let transaction_limit_bytes = if self
            .peek()
            .is_ok_and(|token| token.kind == TokenKind::Punctuation(','))
        {
            self.expect_punctuation(',', "CDF-RESOURCE-DRAIN")?;
            self.expect_word("TRANSACTION", "CDF-RESOURCE-DRAIN-TRANSACTION-LIMIT")?;
            self.expect_word("LIMIT", "CDF-RESOURCE-DRAIN-TRANSACTION-LIMIT")?;
            self.expect_word("BYTES", "CDF-RESOURCE-DRAIN-TRANSACTION-LIMIT")?;
            Some(self.parse_positive_u64("TRANSACTION LIMIT BYTES")?)
        } else {
            None
        };
        self.expect_punctuation(')', "CDF-RESOURCE-DRAIN")?;
        Ok(ExecutionDeclaration::Drain {
            checkpoint_cadence,
            package_rotation,
            termination,
            watermark: Box::new(WatermarkDeclaration::Disabled),
            late_data,
            safe_frontier: SafeFrontierDeclaration::CanonicalAdmittedSourcePosition,
            transaction_limit_bytes,
        })
    }

    fn parse_epoch_trigger(&mut self) -> Result<EpochClosureDeclaration> {
        let kind = self.take()?;
        if kind.is_word("ELAPSED") {
            self.expect_word("MILLISECONDS", "CDF-RESOURCE-DRAIN-TRIGGER")?;
            return Ok(EpochClosureDeclaration::Elapsed {
                milliseconds: self.parse_positive_u64("elapsed milliseconds")?,
            });
        }
        if kind.is_word("WATERMARK") {
            self.expect_word("ADVANCE", "CDF-RESOURCE-DRAIN-TRIGGER")?;
            return Ok(EpochClosureDeclaration::WatermarkAdvance {
                units: self.parse_positive_u64("watermark advance units")?,
            });
        }
        let count = self.parse_positive_u64("epoch trigger count")?;
        if kind.is_word("BATCHES") {
            Ok(EpochClosureDeclaration::Batches { count })
        } else if kind.is_word("ROWS") {
            Ok(EpochClosureDeclaration::Rows { count })
        } else if kind.is_word("BYTES") {
            Ok(EpochClosureDeclaration::Bytes { count })
        } else {
            Err(resource_sql_error(
                "CDF-RESOURCE-DRAIN-TRIGGER",
                self.file,
                Some(&kind.span),
                "epoch trigger must be BATCHES, ROWS, BYTES, ELAPSED MILLISECONDS, or WATERMARK ADVANCE",
            ))
        }
    }

    fn parse_termination(&mut self) -> Result<DrainTerminationDeclaration> {
        let kind = self.take()?;
        if kind.is_word("QUIESCENT") {
            return Ok(DrainTerminationDeclaration::Quiescent);
        }
        if kind.is_word("DURATION") {
            self.expect_word("MILLISECONDS", "CDF-RESOURCE-DRAIN-UNTIL")?;
            return Ok(DrainTerminationDeclaration::Duration {
                milliseconds: self.parse_positive_u64("termination milliseconds")?,
            });
        }
        if kind.is_word("RECORDS") {
            return Ok(DrainTerminationDeclaration::Records {
                count: self.parse_positive_u64("termination record count")?,
            });
        }
        if kind.is_word("BYTES") {
            return Ok(DrainTerminationDeclaration::Bytes {
                count: self.parse_positive_u64("termination byte count")?,
            });
        }
        Err(resource_sql_error(
            "CDF-RESOURCE-DRAIN-UNTIL",
            self.file,
            Some(&kind.span),
            "UNTIL must be QUIESCENT, DURATION MILLISECONDS, RECORDS, or BYTES",
        ))
    }

    fn parse_late_data(&mut self) -> Result<LateDataDeclaration> {
        let kind = self.take()?;
        if kind.is_word("QUARANTINE") {
            return Ok(LateDataDeclaration::Quarantine);
        }
        if kind.is_word("RECAPTURE") {
            self.expect_word("NEXT", "CDF-RESOURCE-DRAIN-LATE-DATA")?;
            self.expect_word("EPOCH", "CDF-RESOURCE-DRAIN-LATE-DATA")?;
            return Ok(LateDataDeclaration::RecaptureNextEpoch);
        }
        if kind.is_word("ADMIT") {
            self.expect_word("WITH", "CDF-RESOURCE-DRAIN-LATE-DATA")?;
            self.expect_word("ANNOTATION", "CDF-RESOURCE-DRAIN-LATE-DATA")?;
            return Ok(LateDataDeclaration::AdmitWithAnnotation);
        }
        Err(resource_sql_error(
            "CDF-RESOURCE-DRAIN-LATE-DATA",
            self.file,
            Some(&kind.span),
            "LATE DATA must be QUARANTINE, RECAPTURE NEXT EPOCH, or ADMIT WITH ANNOTATION",
        ))
    }

    fn parse_positive_u64(&mut self, label: &str) -> Result<u64> {
        let token = self.take()?;
        let TokenKind::Number(value) = &token.kind else {
            return Err(resource_sql_error(
                "CDF-RESOURCE-DRAIN-NUMBER",
                self.file,
                Some(&token.span),
                format!("{label} must be a positive integer"),
            ));
        };
        let value = value.parse::<u64>().map_err(|error| {
            resource_sql_error(
                "CDF-RESOURCE-DRAIN-NUMBER",
                self.file,
                Some(&token.span),
                format!("{label} is invalid: {error}"),
            )
        })?;
        if value == 0 {
            return Err(resource_sql_error(
                "CDF-RESOURCE-DRAIN-NUMBER",
                self.file,
                Some(&token.span),
                format!("{label} must be greater than zero"),
            ));
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Token {
    kind: TokenKind,
    span: ProjectSqlSpan,
    start: usize,
}

impl Token {
    fn is_word(&self, expected: &str) -> bool {
        matches!(&self.kind, TokenKind::Word(value) if value.eq_ignore_ascii_case(expected))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TokenKind {
    Word(String),
    String(String),
    Number(String),
    Punctuation(char),
    RightArrow,
}

fn lex_envelope_prefix(sql: &str, file: &str) -> Result<Vec<Token>> {
    let mut lexer = Lexer::new(sql, file);
    let mut tokens = Vec::new();
    while let Some(token) = lexer.next_token()? {
        let is_query = token.is_word("SELECT");
        tokens.push(token);
        if is_query {
            break;
        }
    }
    Ok(tokens)
}

struct Lexer<'a> {
    bytes: &'a [u8],
    file: &'a str,
    offset: usize,
    line: u32,
    column: u32,
}

impl<'a> Lexer<'a> {
    fn new(sql: &'a str, file: &'a str) -> Self {
        Self {
            bytes: sql.as_bytes(),
            file,
            offset: 0,
            line: 1,
            column: 1,
        }
    }

    fn next_token(&mut self) -> Result<Option<Token>> {
        self.skip_trivia()?;
        if self.offset == self.bytes.len() {
            return Ok(None);
        }
        let start = self.offset;
        let start_line = self.line;
        let start_column = self.column;
        let byte = self.bytes[self.offset];
        let kind = if byte.is_ascii_alphabetic() || byte == b'_' {
            self.bump();
            while self
                .bytes
                .get(self.offset)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
            {
                self.bump();
            }
            TokenKind::Word(
                String::from_utf8(self.bytes[start..self.offset].to_vec()).map_err(|error| {
                    CdfError::internal(format!("lex resource identifier: {error}"))
                })?,
            )
        } else if byte.is_ascii_digit() {
            self.bump();
            while self.bytes.get(self.offset).is_some_and(u8::is_ascii_digit) {
                self.bump();
            }
            TokenKind::Number(
                String::from_utf8(self.bytes[start..self.offset].to_vec())
                    .map_err(|error| CdfError::internal(format!("lex resource number: {error}")))?,
            )
        } else if byte == b'\'' {
            self.bump();
            let mut value = String::new();
            loop {
                let Some(next) = self.bytes.get(self.offset).copied() else {
                    return Err(resource_sql_error(
                        "CDF-RESOURCE-STRING",
                        self.file,
                        Some(&ProjectSqlSpan {
                            start_line,
                            start_column,
                            end_line: self.line,
                            end_column: self.column,
                        }),
                        "unterminated single-quoted string",
                    ));
                };
                if next == b'\'' {
                    self.bump();
                    if self.bytes.get(self.offset) == Some(&b'\'') {
                        self.bump();
                        value.push('\'');
                        continue;
                    }
                    break;
                }
                let character = std::str::from_utf8(&self.bytes[self.offset..])
                    .ok()
                    .and_then(|remaining| remaining.chars().next())
                    .ok_or_else(|| {
                        resource_sql_error(
                            "CDF-RESOURCE-UTF8",
                            self.file,
                            None,
                            "resource SQL is not valid UTF-8",
                        )
                    })?;
                value.push(character);
                self.bump_character(character);
            }
            TokenKind::String(value)
        } else if byte == b'=' && self.bytes.get(self.offset + 1) == Some(&b'>') {
            self.bump();
            self.bump();
            TokenKind::RightArrow
        } else if matches!(byte, b'(' | b')' | b',' | b'.') {
            self.bump();
            TokenKind::Punctuation(char::from(byte))
        } else {
            self.bump();
            return Err(resource_sql_error(
                "CDF-RESOURCE-TOKEN",
                self.file,
                Some(&ProjectSqlSpan {
                    start_line,
                    start_column,
                    end_line: start_line,
                    end_column: start_column,
                }),
                "unsupported token in RESOURCE envelope",
            ));
        };
        Ok(Some(Token {
            kind,
            span: ProjectSqlSpan {
                start_line,
                start_column,
                end_line: self.line,
                end_column: self.column.saturating_sub(1),
            },
            start,
        }))
    }

    fn skip_trivia(&mut self) -> Result<()> {
        loop {
            while self
                .bytes
                .get(self.offset)
                .is_some_and(u8::is_ascii_whitespace)
            {
                self.bump();
            }
            if self.bytes.get(self.offset..self.offset + 2) == Some(b"--") {
                while self
                    .bytes
                    .get(self.offset)
                    .is_some_and(|byte| *byte != b'\n')
                {
                    self.bump();
                }
                continue;
            }
            if self.bytes.get(self.offset..self.offset + 2) == Some(b"/*") {
                self.bump();
                self.bump();
                loop {
                    if self.offset == self.bytes.len() {
                        return Err(resource_sql_error(
                            "CDF-RESOURCE-COMMENT",
                            self.file,
                            None,
                            "unterminated RESOURCE envelope comment",
                        ));
                    }
                    if self.bytes.get(self.offset..self.offset + 2) == Some(b"*/") {
                        self.bump();
                        self.bump();
                        break;
                    }
                    self.bump();
                }
                continue;
            }
            return Ok(());
        }
    }

    fn bump(&mut self) {
        let byte = self.bytes[self.offset];
        self.offset += 1;
        if byte == b'\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
    }

    fn bump_character(&mut self, character: char) {
        self.offset += character.len_utf8();
        if character == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
    }
}

fn union_span(left: &ProjectSqlSpan, right: &ProjectSqlSpan) -> ProjectSqlSpan {
    ProjectSqlSpan {
        start_line: left.start_line,
        start_column: left.start_column,
        end_line: right.end_line,
        end_column: right.end_column,
    }
}

fn resource_sql_error(
    code: &str,
    file: &str,
    span: Option<&ProjectSqlSpan>,
    message: impl std::fmt::Display,
) -> CdfError {
    match span {
        Some(span) => CdfError::contract(format!(
            "[{code}] {file}:{}:{}: {message}",
            span.start_line, span.start_column
        )),
        None => CdfError::contract(format!("[{code}] {file}: {message}")),
    }
}
