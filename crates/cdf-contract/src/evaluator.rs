use std::{cmp::Ordering, collections::HashMap, fmt::Write};

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType, TimeUnit};
use cdf_kernel::{CdfError, Result, SourcePosition, source_name};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    policy::RedactionDecision,
    program::{
        ColumnProgram, DedupKeepProgram, MissingColumnBehavior, RowRulePredicate, RowRuleProgram,
        RuleDisposition, RuleOutcome, ValidationProgram,
    },
    schema::ArrowType,
};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractEvaluationContext {
    pub observed_at_ms: Option<i64>,
    pub source_position: Option<SourcePosition>,
}

impl ContractEvaluationContext {
    pub fn observed_at(observed_at_ms: i64) -> Self {
        Self {
            observed_at_ms: Some(observed_at_ms),
            source_position: None,
        }
    }

    pub fn with_source_position(mut self, source_position: Option<SourcePosition>) -> Self {
        self.source_position = source_position;
        self
    }
}

#[derive(Clone, Debug)]
pub struct ContractBatchEvaluation {
    pub accepted_rows: BooleanArray,
    pub quarantine_candidates: Vec<QuarantineCandidate>,
    pub summary: VerdictSummary,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuarantineCandidate {
    pub source_row_ordinal: usize,
    pub rule_id: String,
    pub error_code: String,
    pub source_position: Option<SourcePosition>,
    pub observed_value_redacted: RedactedObservedValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RedactedObservedValue {
    Null,
    Preserved { value: String },
    Hashed { algorithm: String, value: String },
    Omitted,
    Masked { value: String },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerdictSummary {
    pub input_rows: u64,
    pub accepted_rows: u64,
    pub quarantined_rows: u64,
    pub violation_count: u64,
    pub quarantine_candidate_count: u64,
    pub rule_summaries: Vec<RuleVerdictSummary>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleVerdictSummary {
    pub rule_id: String,
    pub error_code: String,
    pub checked_rows: u64,
    pub violation_count: u64,
}

#[derive(Clone, Debug)]
pub struct PackageDedupEvaluation {
    pub retained_rows: Vec<BooleanArray>,
    pub summary: DedupSummary,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DedupSummary {
    pub rule_id: String,
    pub keys: Vec<String>,
    pub keep: DedupKeepProgram,
    pub input_rows: u64,
    pub output_rows: u64,
    pub duplicate_key_count: u64,
    pub dropped_row_count: u64,
    pub dropped_rows: Vec<DedupDroppedRow>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DedupDroppedRow {
    pub package_row_ordinal: u64,
    pub kept_package_row_ordinal: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageDedupRuleSpec {
    pub rule_id: String,
    pub keys: Vec<String>,
    pub keep: DedupKeepProgram,
}

pub fn package_dedup_rule(program: &ValidationProgram) -> Result<Option<PackageDedupRuleSpec>> {
    let Some(rule) = single_dedup_rule(program)? else {
        return Ok(None);
    };
    let (keys, keep) = dedup_rule_parts(rule)?;
    if keys.is_empty() {
        return Err(CdfError::contract(format!(
            "dedup row rule {:?} must declare at least one key",
            rule.rule_id
        )));
    }
    for key in keys {
        let is_variant_output = program
            .residual
            .as_ref()
            .and_then(|residual| residual.capture.as_ref())
            .is_some_and(|capture| capture.variant_column == *key);
        if column_program_for_rule(program, key).is_none() && !is_variant_output {
            return Err(CdfError::contract(format!(
                "dedup row rule {:?} references unknown key {key:?}",
                rule.rule_id
            )));
        }
    }
    Ok(Some(PackageDedupRuleSpec {
        rule_id: rule.rule_id.clone(),
        keys: keys.to_vec(),
        keep: keep.clone(),
    }))
}

pub fn encode_package_dedup_keys(
    program: &ValidationProgram,
    rule: &PackageDedupRuleSpec,
    batch: &RecordBatch,
) -> Result<Vec<Vec<u8>>> {
    let arrays = crate::dedup_key::canonicalize_map_order(dedup_arrays(
        program,
        batch,
        &rule.rule_id,
        &rule.keys,
    )?)?;
    let converter = RowConverter::new(
        arrays
            .iter()
            .map(|array| SortField::new(array.data_type().clone()))
            .collect(),
    )?;
    let rows = converter.convert_columns(&arrays)?;
    Ok((0..batch.num_rows())
        .map(|row| rows.row(row).as_ref().to_vec())
        .collect())
}

pub fn evaluate_record_batch(
    program: &ValidationProgram,
    context: &ContractEvaluationContext,
    batch: &RecordBatch,
) -> Result<ContractBatchEvaluation> {
    validate_covered_batch_schema(program, batch)?;

    let mut accepted = vec![true; batch.num_rows()];
    let mut quarantined = vec![false; batch.num_rows()];
    let mut quarantine_candidates = Vec::new();
    let mut summary = VerdictSummary {
        input_rows: batch.num_rows() as u64,
        ..VerdictSummary::default()
    };

    for rule in &program.row_rules {
        let Some(evaluation) = RuleEvaluation::prepare(program, context, batch, rule)? else {
            continue;
        };
        let mut rule_summary = RuleVerdictSummary {
            rule_id: rule.rule_id.clone(),
            error_code: evaluation.error_code().to_owned(),
            checked_rows: batch.num_rows() as u64,
            violation_count: 0,
        };

        for row in 0..batch.num_rows() {
            if !evaluation.violates(row)? {
                continue;
            }
            rule_summary.violation_count += 1;
            summary.violation_count += 1;
            match program.disposition_for(RuleOutcome::Violation, rule.rule_id.clone()) {
                RuleDisposition::Accept => {}
                RuleDisposition::Quarantine { rule_id } => {
                    accepted[row] = false;
                    quarantined[row] = true;
                    quarantine_candidates.push(QuarantineCandidate {
                        source_row_ordinal: row,
                        rule_id,
                        error_code: evaluation.error_code().to_owned(),
                        source_position: context.source_position.clone(),
                        observed_value_redacted: redacted_observed_value(evaluation.column(), row)?,
                    });
                }
                RuleDisposition::RejectBatch { rule_id } => {
                    return Err(CdfError::contract(format!(
                        "contract reject_batch from row rule {rule_id:?} at row {row}"
                    )));
                }
                RuleDisposition::RejectRun { rule_id } => {
                    return Err(CdfError::contract(format!(
                        "contract reject_run from row rule {rule_id:?} at row {row}"
                    )));
                }
            }
        }
        summary.rule_summaries.push(rule_summary);
    }

    summary.accepted_rows = accepted.iter().filter(|accepted| **accepted).count() as u64;
    summary.quarantined_rows = quarantined
        .iter()
        .filter(|quarantined| **quarantined)
        .count() as u64;
    summary.quarantine_candidate_count = quarantine_candidates.len() as u64;

    Ok(ContractBatchEvaluation {
        accepted_rows: BooleanArray::from(accepted),
        quarantine_candidates,
        summary,
    })
}

pub fn evaluate_package_order_dedup(
    program: &ValidationProgram,
    batches: &[RecordBatch],
) -> Result<Option<PackageDedupEvaluation>> {
    let Some(rule) = package_dedup_rule(program)? else {
        return Ok(None);
    };

    let mut retained = batches
        .iter()
        .map(|batch| vec![false; batch.num_rows()])
        .collect::<Vec<_>>();
    let mut groups = HashMap::<Vec<u8>, Vec<PackageRowRef>>::new();
    let mut package_row_ordinal = 0_u64;

    for (batch_index, batch) in batches.iter().enumerate() {
        let keys = encode_package_dedup_keys(program, &rule, batch)?;
        for (row_index, key) in keys.into_iter().enumerate() {
            if matches!(rule.keep, DedupKeepProgram::Fail) && groups.contains_key(&key) {
                return Err(CdfError::contract(format!(
                    "dedup row rule {:?} found duplicate key at package row {}; keep=fail aborts before destination mutation",
                    rule.rule_id, package_row_ordinal
                )));
            }
            groups.entry(key).or_default().push(PackageRowRef {
                batch_index,
                row_index,
                package_row_ordinal,
            });
            package_row_ordinal += 1;
        }
    }

    let mut duplicate_key_count = 0_u64;
    let mut dropped_rows = Vec::new();
    for rows in groups.values() {
        let kept = match rule.keep {
            DedupKeepProgram::First | DedupKeepProgram::Fail => rows[0],
            DedupKeepProgram::Last => rows[rows.len() - 1],
        };
        retained[kept.batch_index][kept.row_index] = true;
        if rows.len() > 1 {
            duplicate_key_count += 1;
        }
        for row in rows {
            if *row != kept {
                dropped_rows.push(DedupDroppedRow {
                    package_row_ordinal: row.package_row_ordinal,
                    kept_package_row_ordinal: kept.package_row_ordinal,
                });
            }
        }
    }
    dropped_rows.sort_by_key(|row| row.package_row_ordinal);

    let output_rows = retained
        .iter()
        .flatten()
        .filter(|retained| **retained)
        .count() as u64;
    let dropped_row_count = dropped_rows.len() as u64;
    Ok(Some(PackageDedupEvaluation {
        retained_rows: retained.into_iter().map(BooleanArray::from).collect(),
        summary: DedupSummary {
            rule_id: rule.rule_id,
            keys: rule.keys,
            keep: rule.keep,
            input_rows: package_row_ordinal,
            output_rows,
            duplicate_key_count,
            dropped_row_count,
            dropped_rows,
        },
    }))
}

fn validate_covered_batch_schema(program: &ValidationProgram, batch: &RecordBatch) -> Result<()> {
    for field in batch.schema().fields() {
        let field_source_name = source_name(field).unwrap_or_else(|| field.name());
        let Some(column) = column_program_for_field(program, field_source_name) else {
            return Err(CdfError::contract(format!(
                "validation program does not cover field {:?}",
                field.name()
            )));
        };
        let observed = ArrowType::from(field.data_type());
        if observed != column.arrow_type {
            return Err(CdfError::contract(format!(
                "validation program field {:?} expects {:?} but batch has {:?}",
                field.name(),
                column.arrow_type,
                observed
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PackageRowRef {
    batch_index: usize,
    row_index: usize,
    package_row_ordinal: u64,
}

fn single_dedup_rule(program: &ValidationProgram) -> Result<Option<&RowRuleProgram>> {
    let mut rules = program.row_rules.iter().filter(|rule| {
        matches!(
            rule.predicate,
            RowRulePredicate::Dedup { .. } | RowRulePredicate::ExactRowDedup { .. }
        )
    });
    let first = rules.next();
    if let Some(second) = rules.next() {
        return Err(CdfError::contract(format!(
            "multiple dedup row rules are not supported in one validation program: {:?} and {:?}",
            first.expect("second exists only after first").rule_id,
            second.rule_id
        )));
    }
    Ok(first)
}

fn dedup_rule_parts(rule: &RowRuleProgram) -> Result<(&[String], &DedupKeepProgram)> {
    match &rule.predicate {
        RowRulePredicate::Dedup { keys, keep } | RowRulePredicate::ExactRowDedup { keys, keep } => {
            Ok((keys, keep))
        }
        _ => Err(CdfError::internal(
            "dedup rule helper called on non-dedup rule",
        )),
    }
}

fn dedup_arrays(
    program: &ValidationProgram,
    batch: &RecordBatch,
    rule_id: &str,
    keys: &[String],
) -> Result<Vec<arrow_array::ArrayRef>> {
    keys.iter()
        .map(|key| {
            if let Some(column) = resolve_column(program, batch, key)? {
                return Ok(arrow_array::make_array(column.array.to_data()));
            }
            let index = batch.schema().index_of(key).map_err(|_| {
                CdfError::contract(format!(
                    "dedup row rule {:?} references missing final output field {key:?}",
                    rule_id
                ))
            })?;
            Ok(batch.column(index).clone())
        })
        .collect()
}

struct RuleEvaluation<'a> {
    column: EvaluatedColumn<'a>,
    kind: PreparedRuleKind<'a>,
}

impl<'a> RuleEvaluation<'a> {
    fn prepare(
        program: &'a ValidationProgram,
        context: &ContractEvaluationContext,
        batch: &'a RecordBatch,
        rule: &'a crate::program::RowRuleProgram,
    ) -> Result<Option<Self>> {
        let (column_name, kind) = match &rule.predicate {
            RowRulePredicate::Nullability { column } => {
                (column.as_str(), PreparedRuleKind::Nullability)
            }
            RowRulePredicate::Domain { column, allowed } => (
                column.as_str(),
                PreparedRuleKind::Domain {
                    allowed: allowed.iter().map(String::as_str).collect(),
                },
            ),
            RowRulePredicate::Range { column, min, max } => (
                column.as_str(),
                PreparedRuleKind::Range {
                    min: min.as_deref(),
                    max: max.as_deref(),
                },
            ),
            RowRulePredicate::Regex { column, pattern } => (
                column.as_str(),
                PreparedRuleKind::Regex {
                    regex: Regex::new(pattern).map_err(|error| {
                        CdfError::contract(format!(
                            "row rule {:?} has malformed regex: {error}",
                            rule.rule_id
                        ))
                    })?,
                },
            ),
            RowRulePredicate::Freshness { column, max_age_ms } => {
                let observed_at_ms = context.observed_at_ms.ok_or_else(|| {
                    CdfError::contract(format!(
                        "freshness row rule {:?} requires observed_at_ms evaluation context",
                        rule.rule_id
                    ))
                })?;
                let max_age_ms = i64::try_from(*max_age_ms).map_err(|_| {
                    CdfError::contract(format!(
                        "freshness row rule {:?} max_age_ms exceeds i64",
                        rule.rule_id
                    ))
                })?;
                (
                    column.as_str(),
                    PreparedRuleKind::Freshness {
                        observed_at_ms,
                        max_age_ms,
                    },
                )
            }
            RowRulePredicate::Dedup { .. } | RowRulePredicate::ExactRowDedup { .. } => {
                return Ok(None);
            }
        };
        let Some(column) = resolve_column(program, batch, column_name)? else {
            return match rule.missing_column {
                MissingColumnBehavior::Skip => Ok(None),
                MissingColumnBehavior::Error => Err(CdfError::contract(format!(
                    "row rule {:?} references missing field {column_name:?}",
                    rule.rule_id
                ))),
            };
        };
        Ok(Some(Self { column, kind }))
    }

    fn column(&self) -> &EvaluatedColumn<'a> {
        &self.column
    }

    fn error_code(&self) -> &'static str {
        match self.kind {
            PreparedRuleKind::Nullability => "nullability_violation",
            PreparedRuleKind::Domain { .. } => "domain_violation",
            PreparedRuleKind::Range { .. } => "range_violation",
            PreparedRuleKind::Regex { .. } => "regex_violation",
            PreparedRuleKind::Freshness { .. } => "freshness_violation",
        }
    }

    fn violates(&self, row: usize) -> Result<bool> {
        match &self.kind {
            PreparedRuleKind::Nullability => Ok(self.column.array.is_null(row)),
            PreparedRuleKind::Domain { allowed } => {
                let Some(value) = scalar_string(self.column.array, row)? else {
                    return Ok(false);
                };
                Ok(!allowed.iter().any(|allowed| *allowed == value))
            }
            PreparedRuleKind::Range { min, max } => {
                let Some(value) = numeric_scalar(self.column.array, row)? else {
                    return Ok(false);
                };
                Ok(!value.within(*min, *max, self.column.array.data_type())?)
            }
            PreparedRuleKind::Regex { regex } => {
                let Some(value) = string_scalar(self.column.array, row)? else {
                    return Ok(false);
                };
                Ok(!regex.is_match(value))
            }
            PreparedRuleKind::Freshness {
                observed_at_ms,
                max_age_ms,
            } => {
                let Some(timestamp_ms) = timestamp_ms(self.column.array, row)? else {
                    return Ok(true);
                };
                let age_ms = observed_at_ms.saturating_sub(timestamp_ms);
                Ok(age_ms > *max_age_ms)
            }
        }
    }
}

struct EvaluatedColumn<'a> {
    array: &'a dyn Array,
    redaction: &'a RedactionDecision,
}

enum PreparedRuleKind<'a> {
    Nullability,
    Domain {
        allowed: Vec<&'a str>,
    },
    Range {
        min: Option<&'a str>,
        max: Option<&'a str>,
    },
    Regex {
        regex: Regex,
    },
    Freshness {
        observed_at_ms: i64,
        max_age_ms: i64,
    },
}

#[derive(Clone, Copy)]
enum NumericScalar {
    Signed(i128),
    Unsigned(u128),
    Float(f64),
}

impl NumericScalar {
    fn within(self, min: Option<&str>, max: Option<&str>, data_type: &DataType) -> Result<bool> {
        if let Some(min) = min
            && self.compare_literal(min, data_type)?.is_lt()
        {
            return Ok(false);
        }
        if let Some(max) = max
            && self.compare_literal(max, data_type)?.is_gt()
        {
            return Ok(false);
        }
        Ok(true)
    }

    fn compare_literal(self, literal: &str, data_type: &DataType) -> Result<Ordering> {
        Ok(match self {
            Self::Signed(value) => value.cmp(&literal.parse::<i128>().map_err(|error| {
                CdfError::contract(format!(
                    "range literal {literal:?} is not valid for {data_type}: {error}"
                ))
            })?),
            Self::Unsigned(value) => value.cmp(&literal.parse::<u128>().map_err(|error| {
                CdfError::contract(format!(
                    "range literal {literal:?} is not valid for {data_type}: {error}"
                ))
            })?),
            Self::Float(value) => {
                let literal = literal.parse::<f64>().map_err(|error| {
                    CdfError::contract(format!(
                        "range literal {literal:?} is not valid for {data_type}: {error}"
                    ))
                })?;
                value.partial_cmp(&literal).ok_or_else(|| {
                    CdfError::contract(format!("range comparison for {data_type} is not ordered"))
                })?
            }
        })
    }
}

fn resolve_column<'a>(
    program: &'a ValidationProgram,
    batch: &'a RecordBatch,
    column_name: &str,
) -> Result<Option<EvaluatedColumn<'a>>> {
    let Some(program_column) = column_program_for_rule(program, column_name) else {
        return Ok(None);
    };
    let schema = batch.schema();
    let index = schema
        .index_of(&program_column.source_name)
        .or_else(|_| schema.index_of(&program_column.output_name))
        .or_else(|_| {
            schema
                .fields()
                .iter()
                .position(|field| {
                    source_name(field).is_some_and(|name| name == program_column.source_name)
                })
                .ok_or_else(|| {
                    arrow_schema::ArrowError::SchemaError(
                        "source metadata field not found".to_owned(),
                    )
                })
        });
    let Ok(index) = index else {
        return Ok(None);
    };
    Ok(Some(EvaluatedColumn {
        array: batch.column(index).as_ref(),
        redaction: &program_column.redaction,
    }))
}

fn column_program_for_field<'a>(
    program: &'a ValidationProgram,
    field_name: &str,
) -> Option<&'a ColumnProgram> {
    program
        .column_programs
        .iter()
        .find(|column| column.source_name == field_name || column.output_name == field_name)
}

fn column_program_for_rule<'a>(
    program: &'a ValidationProgram,
    column_name: &str,
) -> Option<&'a ColumnProgram> {
    column_program_for_field(program, column_name).or_else(|| {
        program
            .column_programs
            .iter()
            .find(|column| column.source_name == column_name || column.output_name == column_name)
    })
}

fn redacted_observed_value(
    column: &EvaluatedColumn<'_>,
    row: usize,
) -> Result<RedactedObservedValue> {
    let Some(value) = scalar_string(column.array, row)? else {
        return Ok(RedactedObservedValue::Null);
    };
    match column.redaction {
        RedactionDecision::Preserve => Ok(RedactedObservedValue::Preserved { value }),
        RedactionDecision::Hash { algorithm } if algorithm == "sha256" => {
            Ok(RedactedObservedValue::Hashed {
                algorithm: algorithm.clone(),
                value: format!("sha256:{}", sha256_hex(value.as_bytes())),
            })
        }
        RedactionDecision::Hash { algorithm } => Err(CdfError::contract(format!(
            "unsupported quarantine hash algorithm {algorithm:?}"
        ))),
        RedactionDecision::Omit => Ok(RedactedObservedValue::Omitted),
        RedactionDecision::Mask { replacement } => Ok(RedactedObservedValue::Masked {
            value: replacement.clone(),
        }),
    }
}

fn scalar_string(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Boolean => Ok(Some(
            bool_array(array, row, "Boolean")?.value(row).to_string(),
        )),
        DataType::Int8 => Ok(Some(
            primitive_value::<Int8Array>(array, row, "Int8")?.to_string(),
        )),
        DataType::Int16 => Ok(Some(
            primitive_value::<Int16Array>(array, row, "Int16")?.to_string(),
        )),
        DataType::Int32 => Ok(Some(
            primitive_value::<Int32Array>(array, row, "Int32")?.to_string(),
        )),
        DataType::Int64 => Ok(Some(
            primitive_value::<Int64Array>(array, row, "Int64")?.to_string(),
        )),
        DataType::UInt8 => Ok(Some(
            primitive_value::<UInt8Array>(array, row, "UInt8")?.to_string(),
        )),
        DataType::UInt16 => Ok(Some(
            primitive_value::<UInt16Array>(array, row, "UInt16")?.to_string(),
        )),
        DataType::UInt32 => Ok(Some(
            primitive_value::<UInt32Array>(array, row, "UInt32")?.to_string(),
        )),
        DataType::UInt64 => Ok(Some(
            primitive_value::<UInt64Array>(array, row, "UInt64")?.to_string(),
        )),
        DataType::Float32 => Ok(Some(
            primitive_value::<Float32Array>(array, row, "Float32")?.to_string(),
        )),
        DataType::Float64 => Ok(Some(
            primitive_value::<Float64Array>(array, row, "Float64")?.to_string(),
        )),
        DataType::Utf8 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CdfError::internal("Arrow Utf8 array downcast failed"))?
                .value(row)
                .to_owned(),
        )),
        DataType::LargeUtf8 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| CdfError::internal("Arrow LargeUtf8 array downcast failed"))?
                .value(row)
                .to_owned(),
        )),
        DataType::Timestamp(_, _) => Ok(Some(timestamp_ms(array, row)?.unwrap().to_string())),
        other => Err(CdfError::contract(format!(
            "row rule observed value for {other} is not supported"
        ))),
    }
}

fn string_scalar(array: &dyn Array, row: usize) -> Result<Option<&str>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CdfError::internal("Arrow Utf8 array downcast failed"))?
                .value(row),
        )),
        DataType::LargeUtf8 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| CdfError::internal("Arrow LargeUtf8 array downcast failed"))?
                .value(row),
        )),
        other => Err(CdfError::contract(format!(
            "regex row rule requires Utf8 or LargeUtf8, got {other}"
        ))),
    }
}

fn numeric_scalar(array: &dyn Array, row: usize) -> Result<Option<NumericScalar>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Int8 => Ok(Some(NumericScalar::Signed(i128::from(primitive_value::<
            Int8Array,
        >(
            array, row, "Int8",
        )?)))),
        DataType::Int16 => Ok(Some(NumericScalar::Signed(i128::from(primitive_value::<
            Int16Array,
        >(
            array, row, "Int16",
        )?)))),
        DataType::Int32 => Ok(Some(NumericScalar::Signed(i128::from(primitive_value::<
            Int32Array,
        >(
            array, row, "Int32",
        )?)))),
        DataType::Int64 => Ok(Some(NumericScalar::Signed(i128::from(primitive_value::<
            Int64Array,
        >(
            array, row, "Int64",
        )?)))),
        DataType::UInt8 => Ok(Some(NumericScalar::Unsigned(u128::from(
            primitive_value::<UInt8Array>(array, row, "UInt8")?,
        )))),
        DataType::UInt16 => Ok(Some(NumericScalar::Unsigned(u128::from(
            primitive_value::<UInt16Array>(array, row, "UInt16")?,
        )))),
        DataType::UInt32 => Ok(Some(NumericScalar::Unsigned(u128::from(
            primitive_value::<UInt32Array>(array, row, "UInt32")?,
        )))),
        DataType::UInt64 => Ok(Some(NumericScalar::Unsigned(u128::from(
            primitive_value::<UInt64Array>(array, row, "UInt64")?,
        )))),
        DataType::Float32 => Ok(Some(NumericScalar::Float(f64::from(primitive_value::<
            Float32Array,
        >(
            array, row, "Float32",
        )?)))),
        DataType::Float64 => Ok(Some(NumericScalar::Float(primitive_value::<Float64Array>(
            array, row, "Float64",
        )?))),
        other => Err(CdfError::contract(format!(
            "range row rule requires a numeric column, got {other}"
        ))),
    }
}

fn timestamp_ms(array: &dyn Array, row: usize) -> Result<Option<i64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let DataType::Timestamp(unit, _) = array.data_type() else {
        return Err(CdfError::contract(format!(
            "freshness row rule requires a timestamp column, got {}",
            array.data_type()
        )));
    };
    let value = match unit {
        TimeUnit::Second => primitive_value::<TimestampSecondArray>(array, row, "TimestampSecond")?
            .checked_mul(1_000)
            .ok_or_else(|| CdfError::contract("timestamp seconds overflow freshness millis"))?,
        TimeUnit::Millisecond => {
            primitive_value::<TimestampMillisecondArray>(array, row, "TimestampMillisecond")?
        }
        TimeUnit::Microsecond => {
            primitive_value::<TimestampMicrosecondArray>(array, row, "TimestampMicrosecond")?
                / 1_000
        }
        TimeUnit::Nanosecond => {
            primitive_value::<TimestampNanosecondArray>(array, row, "TimestampNanosecond")?
                / 1_000_000
        }
    };
    Ok(Some(value))
}

fn bool_array<'a>(array: &'a dyn Array, _row: usize, label: &str) -> Result<&'a BooleanArray> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| CdfError::internal(format!("Arrow {label} array downcast failed")))
}

trait PrimitiveValue: Array {
    type Native: ToString + Copy;

    fn primitive_value(&self, row: usize) -> Self::Native;
}

macro_rules! impl_primitive_value {
    ($array:ty, $native:ty) => {
        impl PrimitiveValue for $array {
            type Native = $native;

            fn primitive_value(&self, row: usize) -> Self::Native {
                self.value(row)
            }
        }
    };
}

impl_primitive_value!(Int8Array, i8);
impl_primitive_value!(Int16Array, i16);
impl_primitive_value!(Int32Array, i32);
impl_primitive_value!(Int64Array, i64);
impl_primitive_value!(UInt8Array, u8);
impl_primitive_value!(UInt16Array, u16);
impl_primitive_value!(UInt32Array, u32);
impl_primitive_value!(UInt64Array, u64);
impl_primitive_value!(Float32Array, f32);
impl_primitive_value!(Float64Array, f64);
impl_primitive_value!(TimestampSecondArray, i64);
impl_primitive_value!(TimestampMillisecondArray, i64);
impl_primitive_value!(TimestampMicrosecondArray, i64);
impl_primitive_value!(TimestampNanosecondArray, i64);

fn primitive_value<T>(array: &dyn Array, row: usize, label: &str) -> Result<T::Native>
where
    T: PrimitiveValue + 'static,
{
    Ok(array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| CdfError::internal(format!("Arrow {label} array downcast failed")))?
        .primitive_value(row))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
    }
    out
}
