pub use cdf_kernel::{
    CDF_FUNCTION_NAMESPACE, CDF_FUNCTION_VERSION, DATAFUSION_SCALAR_CONFIG_IDENTITY,
    DATAFUSION_SCALAR_FEATURE_SET, DATAFUSION_SCALAR_IMPLEMENTATION_VERSION,
    DATAFUSION_SCALAR_NAMESPACE, DECLARATIVE_EXPRESSION_VERSION, DeclarativeExpression,
    DeclarativeExpressionLiteral, DeclarativeExpressionNode, DeclarativeFunctionReference,
    SCALAR_EXPRESSION_EXECUTOR_VERSION, SCALAR_EXPRESSION_IR_VERSION, ScalarBinaryOperator,
    ScalarCastMode, ScalarColumnDependency, ScalarDependencies, ScalarExpression,
    ScalarExpressionKind, ScalarExpressionNode, ScalarFunctionReference, ScalarFunctionVolatility,
    ScalarType, ScalarUnaryOperator,
};
use std::collections::{BTreeSet, HashMap};

use cdf_kernel::{CanonicalArrowSchema, CdfError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const COMPILED_EXPRESSION_PLAN_VERSION: u16 = 2;
pub const RELATIONAL_EXPRESSION_IR_VERSION: u16 = 1;
pub const DATAFUSION_EXPRESSION_OPTIMIZER: &str = "datafusion-expr-simplifier";
pub const DATAFUSION_EXPRESSION_PIN: &str = DATAFUSION_SCALAR_IMPLEMENTATION_VERSION;
pub const NATIVE_CONTRACT_OPTIMIZER: &str = "cdf-native-contract-lowering";
pub const SOURCE_EXACT_PUSHDOWN_OPTIMIZER: &str = "cdf-source-exact-pushdown";
pub const NATIVE_FILTER_LOWERING_VERSION: &str = "2";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpressionUse {
    Derive,
    Filter,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedExpression {
    pub use_kind: ExpressionUse,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_text: Option<String>,
    pub original: DeclarativeExpression,
    pub expression: ScalarExpression,
    pub optimizer: OptimizerIdentity,
    pub functions: Vec<ScalarFunctionReference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lints: Vec<ExpressionLint>,
}

impl PlannedExpression {
    pub fn validate_recorded(&self) -> Result<()> {
        self.original.validate()?;
        self.expression.validate()?;
        let supported_optimizer = (self.optimizer.name == DATAFUSION_EXPRESSION_OPTIMIZER
            && self.optimizer.version == DATAFUSION_EXPRESSION_PIN)
            || (self.use_kind == ExpressionUse::Filter
                && self.optimizer.name == SOURCE_EXACT_PUSHDOWN_OPTIMIZER
                && self.optimizer.version == CDF_FUNCTION_VERSION);
        if !supported_optimizer {
            return Err(CdfError::contract(
                "recorded scalar optimizer identity is unsupported; run `cdf compile`",
            ));
        }
        if self.functions != self.expression.function_dependencies() {
            return Err(CdfError::contract(
                "recorded scalar function dependency tuple does not match the typed graph",
            ));
        }
        if self.use_kind == ExpressionUse::Filter
            && self.expression.root.scalar_type.data_type != cdf_kernel::CanonicalArrowType::Boolean
        {
            return Err(CdfError::contract(
                "recorded filter expression does not produce Boolean",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedContractExpression {
    pub original: DeclarativeExpression,
    pub optimizer: OptimizerIdentity,
    pub functions: Vec<DeclarativeFunctionReference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lints: Vec<ExpressionLint>,
}

impl PlannedContractExpression {
    pub fn validate_recorded(&self) -> Result<()> {
        self.original.validate()?;
        if self.optimizer.name != NATIVE_CONTRACT_OPTIMIZER
            || self.optimizer.version != CDF_FUNCTION_VERSION
            || !admitted_contract_root(&self.original.root)
            || self.functions != self.original.function_dependencies()
        {
            return Err(CdfError::contract(
                "recorded contract expression identity is stale or unsupported",
            ));
        }
        Ok(())
    }
}

fn admitted_contract_root(node: &DeclarativeExpressionNode) -> bool {
    matches!(
        node,
        DeclarativeExpressionNode::Call { function, .. }
            if matches!(
                function.name.as_str(),
                "is_not_null"
                    | "in_domain"
                    | "in_range"
                    | "matches_regex"
                    | "fresh_within"
                    | "dedup"
                    | "exact_row_dedup"
            )
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizerIdentity {
    pub name: String,
    pub version: String,
}

/// Resolved scalar authority used by execution and replay. This is a current-only artifact; the
/// former untyped expression-v1 graph has no reader.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledExpressionPlan {
    pub version: u16,
    pub scalar_ir_version: u16,
    pub scalar_executor_version: u16,
    pub datafusion_version: String,
    pub datafusion_feature_set: String,
    pub config_identity: String,
    pub native_filter_lowering_version: String,
    pub predicates: Vec<PlannedExpression>,
    pub residuals: Vec<PlannedExpression>,
    pub contracts: Vec<PlannedContractExpression>,
    pub transforms: Vec<PlannedExpression>,
    pub content_sha256: String,
}

impl CompiledExpressionPlan {
    pub fn current(
        predicates: Vec<PlannedExpression>,
        residuals: Vec<PlannedExpression>,
        contracts: Vec<PlannedContractExpression>,
        transforms: Vec<PlannedExpression>,
    ) -> Result<Self> {
        let mut compiled = Self {
            version: COMPILED_EXPRESSION_PLAN_VERSION,
            scalar_ir_version: SCALAR_EXPRESSION_IR_VERSION,
            scalar_executor_version: SCALAR_EXPRESSION_EXECUTOR_VERSION,
            datafusion_version: DATAFUSION_EXPRESSION_PIN.to_owned(),
            datafusion_feature_set: DATAFUSION_SCALAR_FEATURE_SET.to_owned(),
            config_identity: DATAFUSION_SCALAR_CONFIG_IDENTITY.to_owned(),
            native_filter_lowering_version: NATIVE_FILTER_LOWERING_VERSION.to_owned(),
            predicates,
            residuals,
            contracts,
            transforms,
            content_sha256: String::new(),
        };
        compiled.content_sha256 = compiled.compute_content_sha256()?;
        compiled.validate_recorded()?;
        Ok(compiled)
    }

    pub fn validate_recorded(&self) -> Result<()> {
        if self.version != COMPILED_EXPRESSION_PLAN_VERSION
            || self.scalar_ir_version != SCALAR_EXPRESSION_IR_VERSION
            || self.scalar_executor_version != SCALAR_EXPRESSION_EXECUTOR_VERSION
            || self.datafusion_version != DATAFUSION_EXPRESSION_PIN
            || self.datafusion_feature_set != DATAFUSION_SCALAR_FEATURE_SET
            || self.config_identity != DATAFUSION_SCALAR_CONFIG_IDENTITY
            || self.native_filter_lowering_version != NATIVE_FILTER_LOWERING_VERSION
        {
            return Err(CdfError::contract(
                "recorded scalar plan is stale; run `cdf compile`",
            ));
        }
        if self.content_sha256 != self.compute_content_sha256()? {
            return Err(CdfError::contract(
                "recorded compiled expression plan content digest does not match its canonical payload",
            ));
        }
        self.predicates
            .iter()
            .chain(&self.residuals)
            .chain(&self.transforms)
            .try_for_each(PlannedExpression::validate_recorded)?;
        self.contracts
            .iter()
            .try_for_each(PlannedContractExpression::validate_recorded)
    }

    pub fn validate_program_binding(&self, program: &crate::ValidationProgram) -> Result<()> {
        self.validate_recorded()?;
        if program.compiled_expression_plan.as_ref() != Some(self) {
            return Err(CdfError::contract(
                "validation program is not exactly bound to its recorded compiled expression plan",
            ));
        }
        if self.contracts.len() != program.row_rules.len()
            || self
                .contracts
                .iter()
                .zip(&program.row_rules)
                .any(|(planned, rule)| planned.original != rule.expression)
        {
            return Err(CdfError::contract(
                "recorded contract expressions do not match the executable row-rule program",
            ));
        }
        let expression_transforms =
            program
                .transforms
                .iter()
                .filter_map(|transform| match transform {
                    crate::TransformDescription::Derive { column, expression } => {
                        Some((ExpressionUse::Derive, Some(column.as_str()), expression))
                    }
                    crate::TransformDescription::Filter { expression } => {
                        Some((ExpressionUse::Filter, None, expression))
                    }
                    _ => None,
                });
        let expression_transforms = expression_transforms.collect::<Vec<_>>();
        if expression_transforms.len() != self.transforms.len()
            || expression_transforms.iter().zip(&self.transforms).any(
                |((use_kind, source_text, expression), planned)| {
                    planned.use_kind != *use_kind
                        || planned.source_text.as_deref() != *source_text
                        || &planned.original != *expression
                },
            )
        {
            return Err(CdfError::contract(
                "derive/filter transforms do not match their recorded compiled expression plan",
            ));
        }
        Ok(())
    }

    fn compute_content_sha256(&self) -> Result<String> {
        canonical_sha256(&(
            self.version,
            self.scalar_ir_version,
            self.scalar_executor_version,
            &self.datafusion_version,
            &self.datafusion_feature_set,
            &self.config_identity,
            &self.native_filter_lowering_version,
            &self.predicates,
            &self.residuals,
            &self.contracts,
            &self.transforms,
        ))
    }

    pub fn validate_predicate_bindings<'a>(
        &self,
        bindings: impl IntoIterator<Item = (&'a str, &'a DeclarativeExpression, bool)>,
    ) -> Result<()> {
        validate_filter_bindings("scan predicate", bindings, &self.predicates)
    }

    pub fn validate_residual_bindings<'a>(
        &self,
        bindings: impl IntoIterator<Item = (&'a str, &'a DeclarativeExpression)>,
    ) -> Result<()> {
        validate_filter_bindings(
            "residual predicate",
            bindings
                .into_iter()
                .map(|(source, expression)| (source, expression, false)),
            &self.residuals,
        )
    }
}

fn validate_filter_bindings<'a>(
    kind: &str,
    bindings: impl IntoIterator<Item = (&'a str, &'a DeclarativeExpression, bool)>,
    planned: &[PlannedExpression],
) -> Result<()> {
    let bindings = bindings.into_iter().collect::<Vec<_>>();
    if bindings.len() != planned.len() {
        return Err(CdfError::contract(format!(
            "{kind} expressions do not have a one-to-one compiled plan"
        )));
    }
    for ((source, canonical, source_exact), planned) in bindings.into_iter().zip(planned) {
        if planned.use_kind != ExpressionUse::Filter
            || planned.source_text.as_deref() != Some(source)
            || &planned.original != canonical
            || (planned.optimizer.name == SOURCE_EXACT_PUSHDOWN_OPTIMIZER) != source_exact
        {
            return Err(CdfError::contract(format!(
                "{kind} expression {source:?} does not match its compiled plan"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationalExpressionPlan {
    pub version: u16,
    pub scalar_ir_version: u16,
    pub scalar_executor_version: u16,
    pub datafusion_version: String,
    pub datafusion_feature_set: String,
    pub config_identity: String,
    pub input_schema: CanonicalArrowSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<ScalarExpression>,
    pub projection: Vec<ProjectionExpression>,
    pub output_schema: CanonicalArrowSchema,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control_fields: Vec<String>,
    pub content_sha256: String,
}

impl RelationalExpressionPlan {
    pub fn current(
        input_schema: CanonicalArrowSchema,
        filter: Option<ScalarExpression>,
        projection: Vec<ProjectionExpression>,
        output_schema: CanonicalArrowSchema,
        mut control_fields: Vec<String>,
    ) -> Result<Self> {
        control_fields.sort();
        let mut plan = Self {
            version: RELATIONAL_EXPRESSION_IR_VERSION,
            scalar_ir_version: SCALAR_EXPRESSION_IR_VERSION,
            scalar_executor_version: SCALAR_EXPRESSION_EXECUTOR_VERSION,
            datafusion_version: DATAFUSION_EXPRESSION_PIN.to_owned(),
            datafusion_feature_set: DATAFUSION_SCALAR_FEATURE_SET.to_owned(),
            config_identity: DATAFUSION_SCALAR_CONFIG_IDENTITY.to_owned(),
            input_schema,
            filter,
            projection,
            output_schema,
            control_fields,
            content_sha256: String::new(),
        };
        plan.validate_structure()?;
        plan.content_sha256 = plan.compute_content_sha256()?;
        Ok(plan)
    }

    pub fn validate_recorded(&self) -> Result<()> {
        if self.version != RELATIONAL_EXPRESSION_IR_VERSION
            || self.scalar_ir_version != SCALAR_EXPRESSION_IR_VERSION
            || self.scalar_executor_version != SCALAR_EXPRESSION_EXECUTOR_VERSION
            || self.datafusion_version != DATAFUSION_EXPRESSION_PIN
            || self.datafusion_feature_set != DATAFUSION_SCALAR_FEATURE_SET
            || self.config_identity != DATAFUSION_SCALAR_CONFIG_IDENTITY
        {
            return Err(CdfError::contract(
                "recorded relational expression plan is stale; run `cdf compile`",
            ));
        }
        self.validate_structure()?;
        if self.content_sha256 != self.compute_content_sha256()? {
            return Err(CdfError::contract(
                "recorded relational expression plan digest does not match its canonical payload",
            ));
        }
        Ok(())
    }

    fn validate_structure(&self) -> Result<()> {
        let input = self.input_schema.to_arrow()?;
        let output = self.output_schema.to_arrow()?;
        if output.metadata() != input.metadata() {
            return Err(CdfError::contract(
                "relational output schema metadata differs from its input authority",
            ));
        }
        if self.projection.is_empty() {
            return Err(CdfError::contract(
                "relational expression projection cannot be empty",
            ));
        }
        if let Some(filter) = &self.filter {
            filter.validate()?;
            if filter.root.scalar_type.data_type != cdf_kernel::CanonicalArrowType::Boolean {
                return Err(CdfError::contract(
                    "relational expression filter does not produce Boolean",
                ));
            }
            validate_scalar_columns(filter, &input)?;
        }
        if output.fields().len() != self.projection.len() {
            return Err(CdfError::contract(
                "relational projection and output schema cardinality differ",
            ));
        }
        let mut names = BTreeSet::new();
        for (ordinal, (projection, field)) in
            self.projection.iter().zip(output.fields()).enumerate()
        {
            projection.expression.validate()?;
            validate_scalar_columns(&projection.expression, &input)?;
            let metadata_matches = match &projection.expression.root.expression {
                cdf_kernel::ScalarExpressionKind::Column { index, .. } => {
                    output_metadata_matches(field.metadata(), input.field(*index).metadata())
                }
                _ => output_metadata_matches(field.metadata(), &HashMap::new()),
            };
            if projection.ordinal != ordinal
                || projection.name.trim().is_empty()
                || !names.insert(projection.name.clone())
                || field.name() != &projection.name
                || field.data_type() != &projection.expression.root.scalar_type.to_arrow()?
                || field.is_nullable() != projection.expression.root.scalar_type.nullable
                || !metadata_matches
                || projection.lineage != projection.expression.column_dependencies()
            {
                return Err(CdfError::contract(format!(
                    "relational projection at ordinal {ordinal} has inconsistent name, type, nullability, metadata, or lineage"
                )));
            }
        }
        if self
            .control_fields
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(CdfError::contract(
                "control-critical fields are not in canonical lexical order",
            ));
        }
        let mut control_fields = BTreeSet::new();
        for control in &self.control_fields {
            if !control_fields.insert(control) {
                return Err(CdfError::contract(format!(
                    "control-critical field {control:?} is repeated"
                )));
            }
            let input_index = input.index_of(control).map_err(|_| {
                CdfError::contract(format!(
                    "control-critical field {control:?} is absent from the input schema"
                ))
            })?;
            let Some(projected) = self.projection.iter().find(|item| item.name == *control) else {
                return Err(CdfError::contract(format!(
                    "control-critical field {control:?} cannot be removed"
                )));
            };
            let cdf_kernel::ScalarExpressionKind::Column { name, index } =
                &projected.expression.root.expression
            else {
                return Err(CdfError::contract(format!(
                    "control-critical field {control:?} must be an unchanged pass-through column"
                )));
            };
            if name != control
                || *index != input_index
                || projected.expression.root.scalar_type
                    != ScalarType::from_arrow(
                        input.field(input_index).data_type(),
                        input.field(input_index).is_nullable(),
                    )?
                || output.field(projected.ordinal).metadata() != input.field(input_index).metadata()
            {
                return Err(CdfError::contract(format!(
                    "control-critical field {control:?} changed identity, type, or metadata"
                )));
            }
        }
        Ok(())
    }

    fn compute_content_sha256(&self) -> Result<String> {
        canonical_sha256(&(
            self.version,
            self.scalar_ir_version,
            self.scalar_executor_version,
            &self.datafusion_version,
            &self.datafusion_feature_set,
            &self.config_identity,
            &self.input_schema,
            &self.filter,
            &self.projection,
            &self.output_schema,
            &self.control_fields,
        ))
    }
}

fn output_metadata_matches(
    output: &HashMap<String, String>,
    expression_metadata: &HashMap<String, String>,
) -> bool {
    let output = output
        .iter()
        .filter(|(key, _)| key.as_str() != cdf_kernel::SEMANTIC_METADATA_KEY)
        .collect::<Vec<_>>();
    let expression_len = expression_metadata
        .keys()
        .filter(|key| key.as_str() != cdf_kernel::SEMANTIC_METADATA_KEY)
        .count();
    output.len() == expression_len
        && output.into_iter().all(|(key, value)| {
            expression_metadata
                .get(key)
                .is_some_and(|expected| expected == value)
        })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionExpression {
    pub ordinal: usize,
    pub name: String,
    pub expression: ScalarExpression,
    pub lineage: Vec<ScalarColumnDependency>,
}

impl ProjectionExpression {
    pub fn new(ordinal: usize, name: impl Into<String>, expression: ScalarExpression) -> Self {
        Self {
            ordinal,
            name: name.into(),
            lineage: expression.column_dependencies().to_vec(),
            expression,
        }
    }
}

fn validate_scalar_columns(
    expression: &ScalarExpression,
    schema: &arrow_schema::Schema,
) -> Result<()> {
    for dependency in expression.column_dependencies() {
        let field = schema.fields().get(dependency.index).ok_or_else(|| {
            CdfError::contract(format!(
                "scalar input column {:?} has stale ordinal {}",
                dependency.name, dependency.index
            ))
        })?;
        if field.name() != &dependency.name
            || dependency.scalar_type
                != ScalarType::from_arrow(field.data_type(), field.is_nullable())?
        {
            return Err(CdfError::contract(format!(
                "scalar input column {:?} does not match its recorded name/type/nullability",
                dependency.name
            )));
        }
    }
    Ok(())
}

fn canonical_sha256(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| CdfError::internal(format!("serialize expression identity: {error}")))?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpressionLint {
    pub code: ExpressionLintCode,
    pub message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpressionLintCode {
    UnsatisfiableRange,
    AlwaysTrue,
    CursorSubsumed,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract_expression(function: &str) -> DeclarativeExpression {
        DeclarativeExpression::call(
            function,
            vec![DeclarativeExpressionNode::Column {
                name: "id".to_owned(),
            }],
        )
    }

    #[test]
    fn compiled_contract_plan_rejects_function_forgery() {
        let expression = contract_expression("is_not_null");
        let planned = PlannedContractExpression {
            functions: expression.function_dependencies(),
            original: expression,
            optimizer: OptimizerIdentity {
                name: NATIVE_CONTRACT_OPTIMIZER.to_owned(),
                version: CDF_FUNCTION_VERSION.to_owned(),
            },
            lints: Vec::new(),
        };
        let mut forged = planned.clone();
        forged.original = contract_expression("unknown_contract_rule");
        assert!(
            CompiledExpressionPlan::current(Vec::new(), Vec::new(), vec![forged], Vec::new())
                .is_err()
        );
    }

    #[test]
    fn relational_metadata_comparison_is_order_independent_and_ignores_semantics() {
        let mut output = HashMap::new();
        output.insert("source_driver".to_owned(), "postgres".to_owned());
        output.insert("cdf:source_name".to_owned(), "updated_at".to_owned());
        output.insert(
            cdf_kernel::SEMANTIC_METADATA_KEY.to_owned(),
            "time.timestamp@1".to_owned(),
        );
        let mut expression = HashMap::new();
        expression.insert("cdf:source_name".to_owned(), "updated_at".to_owned());
        expression.insert("source_driver".to_owned(), "postgres".to_owned());

        assert!(output_metadata_matches(&output, &expression));
    }
}
