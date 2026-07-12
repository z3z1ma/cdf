use std::collections::HashSet;

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use cdf_kernel::{CdfError, Result, source_name};
use regex::Regex;

use crate::{
    ContractBatchEvaluation, ContractEvaluationContext, MissingColumnBehavior, QuarantineCandidate,
    RedactionDecision, RowDispositionKind, RowRulePredicate, RuleVerdictSummary, ValidationProgram,
    VerdictSummary, evaluator::redacted_observed_array_value,
};

/// A schema-bound validation program. Array resolution, downcast selection, regex compilation,
/// and literal parsing happen once here instead of once per row.
#[derive(Debug)]
pub struct VectorValidationPlan {
    schema: SchemaRef,
    rules: Vec<VectorRule>,
}

#[derive(Debug)]
pub struct VectorValidationEvaluator<'a> {
    program: &'a ValidationProgram,
    plan: Option<VectorValidationPlan>,
}

impl<'a> VectorValidationEvaluator<'a> {
    pub fn new(program: &'a ValidationProgram) -> Self {
        Self {
            program,
            plan: None,
        }
    }

    pub fn evaluate(
        &mut self,
        context: &ContractEvaluationContext,
        batch: &RecordBatch,
    ) -> Result<ContractBatchEvaluation> {
        self.plan_for(batch.schema())?.evaluate(context, batch)
    }

    pub fn evaluate_masks(
        &mut self,
        context: &ContractEvaluationContext,
        batch: &RecordBatch,
    ) -> Result<VectorMaskEvaluation> {
        self.plan_for(batch.schema())?
            .evaluate_masks(context, batch)
    }

    fn plan_for(&mut self, schema: SchemaRef) -> Result<&VectorValidationPlan> {
        if self
            .plan
            .as_ref()
            .is_none_or(|plan| plan.schema.as_ref() != schema.as_ref())
        {
            self.plan = Some(bind_vector_validation_plan(self.program, schema)?);
        }
        Ok(self.plan.as_ref().expect("vector plan was bound"))
    }
}

#[derive(Clone, Debug)]
pub struct VectorMaskEvaluation {
    pub accepted_rows: BooleanBuffer,
    pub quarantined_rows: BooleanBuffer,
    pub rule_masks: Vec<VectorRuleMask>,
    pub summary: VerdictSummary,
}

#[derive(Clone, Debug)]
pub struct VectorRuleMask {
    pub rule_id: String,
    pub error_code: String,
    pub disposition: RowDispositionKind,
    pub violations: BooleanBuffer,
}

#[derive(Debug)]
struct VectorRule {
    rule_id: String,
    column_index: usize,
    redaction: RedactionDecision,
    disposition: RowDispositionKind,
    kind: VectorRuleKind,
}

#[derive(Debug)]
enum VectorRuleKind {
    Nullability,
    Domain(DomainValues),
    Range(RangeBounds),
    Regex(Regex),
    Freshness { max_age_ms: i64, unit: TimeUnit },
}

#[derive(Debug)]
enum DomainValues {
    Boolean(HashSet<bool>),
    Int8(HashSet<i8>),
    Int16(HashSet<i16>),
    Int32(HashSet<i32>),
    Int64(HashSet<i64>),
    UInt8(HashSet<u8>),
    UInt16(HashSet<u16>),
    UInt32(HashSet<u32>),
    UInt64(HashSet<u64>),
    Float32 { bits: HashSet<u32>, nan: bool },
    Float64 { bits: HashSet<u64>, nan: bool },
    Utf8(StringDomain),
    LargeUtf8(StringDomain),
    TimestampMillis(HashSet<i64>),
}

#[derive(Debug)]
enum StringDomain {
    Small(Vec<String>),
    Large(HashSet<String>),
}

impl StringDomain {
    fn new(values: &[String]) -> Self {
        if values.len() <= 8 {
            Self::Small(values.to_vec())
        } else {
            Self::Large(values.iter().cloned().collect())
        }
    }

    #[inline]
    fn contains(&self, value: &str) -> bool {
        match self {
            Self::Small(values) => values.iter().any(|allowed| allowed == value),
            Self::Large(values) => values.contains(value),
        }
    }
}

#[derive(Debug)]
enum RangeBounds {
    Int8(Option<i8>, Option<i8>),
    Int16(Option<i16>, Option<i16>),
    Int32(Option<i32>, Option<i32>),
    Int64(Option<i64>, Option<i64>),
    UInt8(Option<u8>, Option<u8>),
    UInt16(Option<u16>, Option<u16>),
    UInt32(Option<u32>, Option<u32>),
    UInt64(Option<u64>, Option<u64>),
    Float32(Option<f32>, Option<f32>),
    Float64(Option<f64>, Option<f64>),
}

pub fn bind_vector_validation_plan(
    program: &ValidationProgram,
    schema: SchemaRef,
) -> Result<VectorValidationPlan> {
    validate_schema_coverage(program, schema.as_ref())?;
    let disposition = program
        .row_dispositions
        .iter()
        .find(|rule| rule.outcome == crate::RuleOutcome::Violation)
        .map(|rule| rule.disposition.clone())
        .unwrap_or(RowDispositionKind::RejectRun);
    let mut rules = Vec::with_capacity(program.row_rules.len());
    for rule in &program.row_rules {
        let (column_name, predicate) = match &rule.predicate {
            RowRulePredicate::Nullability { column } => {
                (column.as_str(), PredicateBinding::Nullability)
            }
            RowRulePredicate::Domain { column, allowed } => {
                (column.as_str(), PredicateBinding::Domain(allowed))
            }
            RowRulePredicate::Range { column, min, max } => (
                column.as_str(),
                PredicateBinding::Range(min.as_deref(), max.as_deref()),
            ),
            RowRulePredicate::Regex { column, pattern } => {
                (column.as_str(), PredicateBinding::Regex(pattern))
            }
            RowRulePredicate::Freshness { column, max_age_ms } => {
                (column.as_str(), PredicateBinding::Freshness(*max_age_ms))
            }
            RowRulePredicate::Dedup { .. } | RowRulePredicate::ExactRowDedup { .. } => continue,
        };
        let Some((column_index, redaction)) = resolve_column(program, schema.as_ref(), column_name)
        else {
            if rule.missing_column == MissingColumnBehavior::Skip {
                continue;
            }
            return Err(CdfError::contract(format!(
                "row rule {:?} references missing field {column_name:?}",
                rule.rule_id
            )));
        };
        let data_type = schema.field(column_index).data_type();
        let kind = predicate.bind(data_type, &rule.rule_id)?;
        rules.push(VectorRule {
            rule_id: rule.rule_id.clone(),
            column_index,
            redaction,
            disposition: disposition.clone(),
            kind,
        });
    }
    Ok(VectorValidationPlan { schema, rules })
}

impl VectorValidationPlan {
    pub fn evaluate_masks(
        &self,
        context: &ContractEvaluationContext,
        batch: &RecordBatch,
    ) -> Result<VectorMaskEvaluation> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(CdfError::contract(
                "vector validation plan is bound to a different Arrow schema; rebind the plan",
            ));
        }
        let row_count = batch.num_rows();
        let mut accepted = BooleanBuffer::new_set(row_count);
        let mut quarantined = BooleanBuffer::new_unset(row_count);
        let mut rule_masks = Vec::with_capacity(self.rules.len());
        let mut summary = VerdictSummary {
            input_rows: row_count as u64,
            ..VerdictSummary::default()
        };

        for rule in &self.rules {
            let array = batch.column(rule.column_index).as_ref();
            let violations = rule.kind.evaluate(array, context, &rule.rule_id)?;
            let violation_count = violations.count_set_bits() as u64;
            summary.violation_count = summary
                .violation_count
                .checked_add(violation_count)
                .ok_or_else(|| CdfError::data("validation violation count overflowed"))?;
            summary.rule_summaries.push(RuleVerdictSummary {
                rule_id: rule.rule_id.clone(),
                error_code: rule.kind.error_code().to_owned(),
                checked_rows: row_count as u64,
                violation_count,
            });
            if violation_count != 0 {
                match &rule.disposition {
                    RowDispositionKind::Accept => {}
                    RowDispositionKind::Quarantine => {
                        accepted &= &!&violations;
                        quarantined |= &violations;
                    }
                    RowDispositionKind::RejectBatch | RowDispositionKind::RejectRun => {
                        let row = violations
                            .set_indices()
                            .next()
                            .expect("nonempty violation bitmap has a set index");
                        let disposition = if rule.disposition == RowDispositionKind::RejectBatch {
                            "reject_batch"
                        } else {
                            "reject_run"
                        };
                        return Err(CdfError::contract(format!(
                            "contract {disposition} from row rule {:?} at row {row}",
                            rule.rule_id
                        )));
                    }
                }
            }
            rule_masks.push(VectorRuleMask {
                rule_id: rule.rule_id.clone(),
                error_code: rule.kind.error_code().to_owned(),
                disposition: rule.disposition.clone(),
                violations,
            });
        }

        summary.accepted_rows = accepted.count_set_bits() as u64;
        summary.quarantined_rows = quarantined.count_set_bits() as u64;
        summary.quarantine_candidate_count = rule_masks
            .iter()
            .filter(|rule| rule.disposition == RowDispositionKind::Quarantine)
            .try_fold(0_u64, |count, rule| {
                count
                    .checked_add(rule.violations.count_set_bits() as u64)
                    .ok_or_else(|| CdfError::data("quarantine candidate count overflowed"))
            })?;
        Ok(VectorMaskEvaluation {
            accepted_rows: accepted,
            quarantined_rows: quarantined,
            rule_masks,
            summary,
        })
    }

    pub fn evaluate(
        &self,
        context: &ContractEvaluationContext,
        batch: &RecordBatch,
    ) -> Result<ContractBatchEvaluation> {
        let masks = self.evaluate_masks(context, batch)?;
        let mut quarantine_candidates =
            Vec::with_capacity(masks.summary.quarantine_candidate_count as usize);
        for (bound, rule) in self.rules.iter().zip(&masks.rule_masks) {
            if rule.disposition != RowDispositionKind::Quarantine {
                continue;
            }
            let array = batch.column(bound.column_index).as_ref();
            for row in rule.violations.set_indices() {
                quarantine_candidates.push(QuarantineCandidate {
                    source_row_ordinal: row,
                    rule_id: rule.rule_id.clone(),
                    error_code: rule.error_code.clone(),
                    source_position: context.source_position.clone(),
                    observed_value_redacted: redacted_observed_array_value(
                        array,
                        &bound.redaction,
                        row,
                    )?,
                });
            }
        }
        Ok(ContractBatchEvaluation {
            accepted_rows: BooleanArray::new(masks.accepted_rows, None),
            quarantine_candidates,
            summary: masks.summary,
        })
    }
}

enum PredicateBinding<'a> {
    Nullability,
    Domain(&'a [String]),
    Range(Option<&'a str>, Option<&'a str>),
    Regex(&'a str),
    Freshness(u64),
}

impl PredicateBinding<'_> {
    fn bind(self, data_type: &DataType, rule_id: &str) -> Result<VectorRuleKind> {
        match self {
            Self::Nullability => Ok(VectorRuleKind::Nullability),
            Self::Domain(allowed) => Ok(VectorRuleKind::Domain(DomainValues::bind(
                data_type, allowed,
            )?)),
            Self::Range(min, max) => Ok(VectorRuleKind::Range(RangeBounds::bind(
                data_type, min, max,
            )?)),
            Self::Regex(pattern) => {
                if !matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) {
                    return Err(CdfError::contract(format!(
                        "regex row rule requires Utf8 or LargeUtf8, got {data_type}"
                    )));
                }
                Regex::new(pattern)
                    .map(VectorRuleKind::Regex)
                    .map_err(|error| {
                        CdfError::contract(format!(
                            "row rule {rule_id:?} has malformed regex: {error}"
                        ))
                    })
            }
            Self::Freshness(max_age_ms) => {
                let DataType::Timestamp(unit, _) = data_type else {
                    return Err(CdfError::contract(format!(
                        "freshness row rule requires a timestamp column, got {data_type}"
                    )));
                };
                Ok(VectorRuleKind::Freshness {
                    max_age_ms: i64::try_from(max_age_ms).map_err(|_| {
                        CdfError::contract(format!(
                            "freshness row rule {rule_id:?} max_age_ms exceeds i64"
                        ))
                    })?,
                    unit: *unit,
                })
            }
        }
    }
}

impl VectorRuleKind {
    fn error_code(&self) -> &'static str {
        match self {
            Self::Nullability => "nullability_violation",
            Self::Domain(_) => "domain_violation",
            Self::Range(_) => "range_violation",
            Self::Regex(_) => "regex_violation",
            Self::Freshness { .. } => "freshness_violation",
        }
    }

    fn evaluate(
        &self,
        array: &dyn Array,
        context: &ContractEvaluationContext,
        rule_id: &str,
    ) -> Result<BooleanBuffer> {
        match self {
            Self::Nullability => Ok(array
                .nulls()
                .map(|nulls| !nulls.inner())
                .unwrap_or_else(|| BooleanBuffer::new_unset(array.len()))),
            Self::Domain(values) => values.evaluate(array),
            Self::Range(bounds) => bounds.evaluate(array),
            Self::Regex(regex) => match array.data_type() {
                DataType::Utf8 => string_mask::<StringArray>(array, |value| !regex.is_match(value)),
                DataType::LargeUtf8 => {
                    string_mask::<LargeStringArray>(array, |value| !regex.is_match(value))
                }
                other => Err(downcast_error("regex", other)),
            },
            Self::Freshness { max_age_ms, unit } => {
                let observed_at_ms = context.observed_at_ms.ok_or_else(|| {
                    CdfError::contract(format!(
                        "freshness row rule {rule_id:?} requires observed_at_ms evaluation context"
                    ))
                })?;
                timestamp_mask(array, unit, observed_at_ms, *max_age_ms)
            }
        }
    }
}

impl DomainValues {
    fn bind(data_type: &DataType, allowed: &[String]) -> Result<Self> {
        macro_rules! integer {
            ($variant:ident, $ty:ty) => {{
                let mut values = HashSet::with_capacity(allowed.len());
                for literal in allowed {
                    if let Ok(value) = literal.parse::<$ty>()
                        && value.to_string() == *literal
                    {
                        values.insert(value);
                    }
                }
                Self::$variant(values)
            }};
        }
        Ok(match data_type {
            DataType::Boolean => {
                let mut values = HashSet::with_capacity(allowed.len());
                for literal in allowed {
                    if let Ok(value) = literal.parse::<bool>()
                        && value.to_string() == *literal
                    {
                        values.insert(value);
                    }
                }
                Self::Boolean(values)
            }
            DataType::Int8 => integer!(Int8, i8),
            DataType::Int16 => integer!(Int16, i16),
            DataType::Int32 => integer!(Int32, i32),
            DataType::Int64 => integer!(Int64, i64),
            DataType::UInt8 => integer!(UInt8, u8),
            DataType::UInt16 => integer!(UInt16, u16),
            DataType::UInt32 => integer!(UInt32, u32),
            DataType::UInt64 => integer!(UInt64, u64),
            DataType::Float32 => {
                let (bits, nan) = canonical_f32_values(allowed);
                Self::Float32 { bits, nan }
            }
            DataType::Float64 => {
                let (bits, nan) = canonical_f64_values(allowed);
                Self::Float64 { bits, nan }
            }
            DataType::Utf8 => Self::Utf8(StringDomain::new(allowed)),
            DataType::LargeUtf8 => Self::LargeUtf8(StringDomain::new(allowed)),
            DataType::Timestamp(_, _) => {
                let mut values = HashSet::with_capacity(allowed.len());
                for literal in allowed {
                    if let Ok(value) = literal.parse::<i64>()
                        && value.to_string() == *literal
                    {
                        values.insert(value);
                    }
                }
                Self::TimestampMillis(values)
            }
            other => {
                return Err(CdfError::contract(format!(
                    "domain row rule observed value for {other} is not supported"
                )));
            }
        })
    }

    fn evaluate(&self, array: &dyn Array) -> Result<BooleanBuffer> {
        match self {
            Self::Boolean(allowed) => {
                primitive_mask::<BooleanArray>(array, |value| !allowed.contains(&value))
            }
            Self::Int8(allowed) => {
                primitive_mask::<Int8Array>(array, |value| !allowed.contains(&value))
            }
            Self::Int16(allowed) => {
                primitive_mask::<Int16Array>(array, |value| !allowed.contains(&value))
            }
            Self::Int32(allowed) => {
                primitive_mask::<Int32Array>(array, |value| !allowed.contains(&value))
            }
            Self::Int64(allowed) => {
                primitive_mask::<Int64Array>(array, |value| !allowed.contains(&value))
            }
            Self::UInt8(allowed) => {
                primitive_mask::<UInt8Array>(array, |value| !allowed.contains(&value))
            }
            Self::UInt16(allowed) => {
                primitive_mask::<UInt16Array>(array, |value| !allowed.contains(&value))
            }
            Self::UInt32(allowed) => {
                primitive_mask::<UInt32Array>(array, |value| !allowed.contains(&value))
            }
            Self::UInt64(allowed) => {
                primitive_mask::<UInt64Array>(array, |value| !allowed.contains(&value))
            }
            Self::Float32 { bits, nan } => primitive_mask::<Float32Array>(array, |value| {
                if value.is_nan() {
                    !nan
                } else {
                    !bits.contains(&value.to_bits())
                }
            }),
            Self::Float64 { bits, nan } => primitive_mask::<Float64Array>(array, |value| {
                if value.is_nan() {
                    !nan
                } else {
                    !bits.contains(&value.to_bits())
                }
            }),
            Self::Utf8(allowed) => {
                string_mask::<StringArray>(array, |value| !allowed.contains(value))
            }
            Self::LargeUtf8(allowed) => {
                string_mask::<LargeStringArray>(array, |value| !allowed.contains(value))
            }
            Self::TimestampMillis(allowed) => timestamp_domain_mask(array, allowed),
        }
    }
}

impl RangeBounds {
    fn bind(data_type: &DataType, min: Option<&str>, max: Option<&str>) -> Result<Self> {
        macro_rules! bounds {
            ($variant:ident, $ty:ty) => {
                Self::$variant(
                    min.map(|value| parse_literal::<$ty>(value, data_type, "range"))
                        .transpose()?,
                    max.map(|value| parse_literal::<$ty>(value, data_type, "range"))
                        .transpose()?,
                )
            };
        }
        Ok(match data_type {
            DataType::Int8 => bounds!(Int8, i8),
            DataType::Int16 => bounds!(Int16, i16),
            DataType::Int32 => bounds!(Int32, i32),
            DataType::Int64 => bounds!(Int64, i64),
            DataType::UInt8 => bounds!(UInt8, u8),
            DataType::UInt16 => bounds!(UInt16, u16),
            DataType::UInt32 => bounds!(UInt32, u32),
            DataType::UInt64 => bounds!(UInt64, u64),
            DataType::Float32 => bounds!(Float32, f32),
            DataType::Float64 => bounds!(Float64, f64),
            other => {
                return Err(CdfError::contract(format!(
                    "range row rule requires a numeric column, got {other}"
                )));
            }
        })
    }

    fn evaluate(&self, array: &dyn Array) -> Result<BooleanBuffer> {
        match self {
            Self::Int8(min, max) => primitive_range_mask::<Int8Array>(array, *min, *max),
            Self::Int16(min, max) => primitive_range_mask::<Int16Array>(array, *min, *max),
            Self::Int32(min, max) => primitive_range_mask::<Int32Array>(array, *min, *max),
            Self::Int64(min, max) => primitive_range_mask::<Int64Array>(array, *min, *max),
            Self::UInt8(min, max) => primitive_range_mask::<UInt8Array>(array, *min, *max),
            Self::UInt16(min, max) => primitive_range_mask::<UInt16Array>(array, *min, *max),
            Self::UInt32(min, max) => primitive_range_mask::<UInt32Array>(array, *min, *max),
            Self::UInt64(min, max) => primitive_range_mask::<UInt64Array>(array, *min, *max),
            Self::Float32(min, max) => float_mask::<Float32Array>(array, *min, *max),
            Self::Float64(min, max) => float_mask::<Float64Array>(array, *min, *max),
        }
    }
}

trait PrimitiveArray: Array {
    type Native: Copy;
    fn value_at(&self, index: usize) -> Self::Native;
}

macro_rules! primitive_array {
    ($array:ty, $native:ty) => {
        impl PrimitiveArray for $array {
            type Native = $native;
            fn value_at(&self, index: usize) -> Self::Native {
                self.value(index)
            }
        }
    };
}

primitive_array!(BooleanArray, bool);
primitive_array!(Int8Array, i8);
primitive_array!(Int16Array, i16);
primitive_array!(Int32Array, i32);
primitive_array!(Int64Array, i64);
primitive_array!(UInt8Array, u8);
primitive_array!(UInt16Array, u16);
primitive_array!(UInt32Array, u32);
primitive_array!(UInt64Array, u64);
primitive_array!(Float32Array, f32);
primitive_array!(Float64Array, f64);

fn primitive_mask<A>(
    array: &dyn Array,
    mut violates: impl FnMut(A::Native) -> bool,
) -> Result<BooleanBuffer>
where
    A: PrimitiveArray + 'static,
{
    let array = array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| downcast_error("primitive", array.data_type()))?;
    Ok(BooleanBuffer::collect_bool(array.len(), |row| {
        !array.is_null(row) && violates(array.value_at(row))
    }))
}

fn primitive_range_mask<A>(
    array: &dyn Array,
    min: Option<A::Native>,
    max: Option<A::Native>,
) -> Result<BooleanBuffer>
where
    A: PrimitiveArray + 'static,
    A::Native: PartialOrd,
{
    primitive_mask::<A>(array, |value| {
        min.is_some_and(|min| value < min) || max.is_some_and(|max| value > max)
    })
}

trait FloatValue: Copy + PartialOrd {
    fn unordered(self) -> bool;
}
impl FloatValue for f32 {
    fn unordered(self) -> bool {
        self.is_nan()
    }
}
impl FloatValue for f64 {
    fn unordered(self) -> bool {
        self.is_nan()
    }
}

fn float_mask<A>(
    array: &dyn Array,
    min: Option<A::Native>,
    max: Option<A::Native>,
) -> Result<BooleanBuffer>
where
    A: PrimitiveArray + 'static,
    A::Native: FloatValue,
{
    if min.is_none() && max.is_none() {
        return Ok(BooleanBuffer::new_unset(array.len()));
    }
    let array = array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| downcast_error("float", array.data_type()))?;
    let mut values = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        if array.is_null(row) {
            values.push(false);
            continue;
        }
        let value = array.value_at(row);
        if value.unordered() {
            return Err(CdfError::contract(format!(
                "range comparison for {} is not ordered",
                array.data_type()
            )));
        }
        values.push(min.is_some_and(|min| value < min) || max.is_some_and(|max| value > max));
    }
    Ok(BooleanBuffer::from(values))
}

trait StringValues: Array {
    fn string_value(&self, index: usize) -> &str;
}
impl StringValues for StringArray {
    fn string_value(&self, index: usize) -> &str {
        self.value(index)
    }
}
impl StringValues for LargeStringArray {
    fn string_value(&self, index: usize) -> &str {
        self.value(index)
    }
}

fn string_mask<A>(
    array: &dyn Array,
    mut violates: impl FnMut(&str) -> bool,
) -> Result<BooleanBuffer>
where
    A: StringValues + 'static,
{
    let array = array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| downcast_error("string", array.data_type()))?;
    Ok(BooleanBuffer::collect_bool(array.len(), |row| {
        !array.is_null(row) && violates(array.string_value(row))
    }))
}

fn timestamp_domain_mask(array: &dyn Array, allowed: &HashSet<i64>) -> Result<BooleanBuffer> {
    let DataType::Timestamp(unit, _) = array.data_type() else {
        return Err(downcast_error("timestamp", array.data_type()));
    };
    timestamp_values(array, unit, false, |value| !allowed.contains(&value))
}

fn timestamp_mask(
    array: &dyn Array,
    unit: &TimeUnit,
    observed: i64,
    max_age: i64,
) -> Result<BooleanBuffer> {
    timestamp_values(array, unit, true, |value| {
        observed.saturating_sub(value) > max_age
    })
}

fn timestamp_values(
    array: &dyn Array,
    unit: &TimeUnit,
    null_violates: bool,
    mut violates: impl FnMut(i64) -> bool,
) -> Result<BooleanBuffer> {
    macro_rules! timestamp {
        ($array:ty, $convert:expr) => {{
            let array = array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| downcast_error("timestamp", array.data_type()))?;
            let mut values = Vec::with_capacity(array.len());
            for row in 0..array.len() {
                if array.is_null(row) {
                    values.push(null_violates);
                    continue;
                }
                values.push(violates(($convert)(array.value(row))?));
            }
            Ok(BooleanBuffer::from(values))
        }};
    }
    match unit {
        TimeUnit::Second => timestamp!(TimestampSecondArray, |value: i64| value
            .checked_mul(1_000)
            .ok_or_else(|| CdfError::contract("timestamp seconds overflow freshness millis"))),
        TimeUnit::Millisecond => {
            timestamp!(TimestampMillisecondArray, |value: i64| Ok::<i64, CdfError>(
                value
            ))
        }
        TimeUnit::Microsecond => {
            timestamp!(TimestampMicrosecondArray, |value: i64| Ok::<i64, CdfError>(
                value / 1_000
            ))
        }
        TimeUnit::Nanosecond => {
            timestamp!(TimestampNanosecondArray, |value: i64| Ok::<i64, CdfError>(
                value / 1_000_000
            ))
        }
    }
}

fn canonical_f32_values(allowed: &[String]) -> (HashSet<u32>, bool) {
    let mut bits = HashSet::with_capacity(allowed.len());
    let mut nan = false;
    for literal in allowed {
        if let Ok(value) = literal.parse::<f32>()
            && value.to_string() == *literal
        {
            if value.is_nan() {
                nan = true;
            } else {
                bits.insert(value.to_bits());
            }
        }
    }
    (bits, nan)
}

fn canonical_f64_values(allowed: &[String]) -> (HashSet<u64>, bool) {
    let mut bits = HashSet::with_capacity(allowed.len());
    let mut nan = false;
    for literal in allowed {
        if let Ok(value) = literal.parse::<f64>()
            && value.to_string() == *literal
        {
            if value.is_nan() {
                nan = true;
            } else {
                bits.insert(value.to_bits());
            }
        }
    }
    (bits, nan)
}

fn parse_literal<T>(literal: &str, data_type: &DataType, rule: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    literal.parse().map_err(|error| {
        CdfError::contract(format!(
            "{rule} literal {literal:?} is not valid for {data_type}: {error}"
        ))
    })
}

fn resolve_column(
    program: &ValidationProgram,
    schema: &Schema,
    name: &str,
) -> Option<(usize, RedactionDecision)> {
    let column = program
        .column_programs
        .iter()
        .find(|column| column.source_name == name || column.output_name == name)?;
    let index = schema
        .index_of(&column.source_name)
        .or_else(|_| schema.index_of(&column.output_name))
        .ok()
        .or_else(|| {
            schema.fields().iter().position(|field| {
                source_name(field).is_some_and(|source| source == column.source_name)
            })
        })?;
    Some((index, column.redaction.clone()))
}

fn validate_schema_coverage(program: &ValidationProgram, schema: &Schema) -> Result<()> {
    for field in schema.fields() {
        let name = source_name(field).unwrap_or_else(|| field.name());
        let column = program
            .column_programs
            .iter()
            .find(|column| column.source_name == name || column.output_name == name)
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "validation program does not cover field {:?}",
                    field.name()
                ))
            })?;
        let observed = crate::ArrowType::from(field.data_type());
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

fn downcast_error(rule: &str, data_type: &DataType) -> CdfError {
    CdfError::internal(format!(
        "vector {rule} kernel could not bind Arrow {data_type} array"
    ))
}

#[cfg(test)]
mod tests;
