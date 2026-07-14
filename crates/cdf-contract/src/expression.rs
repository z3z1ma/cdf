pub use cdf_kernel::{
    CDF_FUNCTION_NAMESPACE, CDF_FUNCTION_VERSION, EXPRESSION_IR_VERSION, Expression,
    ExpressionLiteral, ExpressionNode, FunctionReference,
};
use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const COMPILED_EXPRESSION_PLAN_VERSION: u16 = 1;
pub const DATAFUSION_EXPRESSION_OPTIMIZER: &str = "datafusion-expr-simplifier";
pub const DATAFUSION_EXPRESSION_PIN: &str = "7ff7278edc1bf7446303bff51e5883a38414bbdf";
pub const NATIVE_CONTRACT_OPTIMIZER: &str = "cdf-native-contract-lowering";
pub const SOURCE_EXACT_PUSHDOWN_OPTIMIZER: &str = "cdf-source-exact-pushdown";
pub const NATIVE_FILTER_LOWERING_VERSION: &str = "1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpressionUse {
    Derive,
    Filter,
    Contract,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpressionFidelity {
    Exact,
    ResidualRequired,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedExpression {
    pub use_kind: ExpressionUse,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_text: Option<String>,
    pub original: Expression,
    pub optimized: Expression,
    pub optimizer: OptimizerIdentity,
    pub functions: Vec<FunctionReference>,
    pub fidelity: ExpressionFidelity,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub residuals: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lints: Vec<ExpressionLint>,
    /// A standards-conformant Substrait encoding is not implemented yet.
    /// `None` is the only supported value until one is round-trip verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub substrait_version: Option<String>,
}

impl PlannedExpression {
    pub fn validate_recorded(&self) -> Result<()> {
        self.original.validate()?;
        self.optimized.validate()?;
        let optimizer_is_datafusion = self.optimizer.name == DATAFUSION_EXPRESSION_OPTIMIZER
            && self.optimizer.version == DATAFUSION_EXPRESSION_PIN;
        let optimizer_is_native_contract = self.optimizer.name == NATIVE_CONTRACT_OPTIMIZER
            && self.optimizer.version == CDF_FUNCTION_VERSION;
        let optimizer_is_source_exact = self.optimizer.name == SOURCE_EXACT_PUSHDOWN_OPTIMIZER
            && self.optimizer.version == CDF_FUNCTION_VERSION
            && self.optimized == self.original;
        let use_is_valid = match self.use_kind {
            ExpressionUse::Derive => optimizer_is_datafusion,
            ExpressionUse::Filter => optimizer_is_datafusion || optimizer_is_source_exact,
            ExpressionUse::Contract => {
                optimizer_is_native_contract
                    && self.optimized == self.original
                    && admitted_contract_root(&self.optimized.root)
            }
        };
        if !use_is_valid {
            return Err(CdfError::contract(
                "recorded expression use, optimizer, and native lowering tuple is not supported by this engine version",
            ));
        }
        let expected_functions = self.optimized.function_dependencies();
        if self.functions != expected_functions
            || self.functions.iter().any(|function| {
                function.namespace != CDF_FUNCTION_NAMESPACE
                    || function.version != CDF_FUNCTION_VERSION
            })
        {
            return Err(CdfError::contract(
                "recorded expression function dependency tuple is stale or unsupported",
            ));
        }
        if self.fidelity != ExpressionFidelity::Exact || !self.residuals.is_empty() {
            return Err(CdfError::contract(
                "identity execution requires exact recorded fidelity with no residuals",
            ));
        }
        if self.substrait_version.is_some() {
            return Err(CdfError::contract(
                "recorded expression claims unsupported Substrait compatibility",
            ));
        }
        Ok(())
    }
}

fn admitted_contract_root(node: &ExpressionNode) -> bool {
    matches!(
        node,
        ExpressionNode::Call { function, .. }
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
pub struct OptimizerIdentity {
    pub name: String,
    pub version: String,
}

/// Parsed, resolved, optimized, and frozen expressions. Execution and replay consume this plan;
/// they never reparse or reoptimize its expressions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledExpressionPlan {
    pub version: u16,
    pub ir_version: u16,
    pub native_filter_lowering_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub substrait_version: Option<String>,
    pub predicates: Vec<PlannedExpression>,
    pub residuals: Vec<PlannedExpression>,
    pub contracts: Vec<PlannedExpression>,
    pub transforms: Vec<PlannedExpression>,
    pub content_sha256: String,
}

impl CompiledExpressionPlan {
    pub fn current(
        predicates: Vec<PlannedExpression>,
        residuals: Vec<PlannedExpression>,
        contracts: Vec<PlannedExpression>,
        transforms: Vec<PlannedExpression>,
    ) -> Result<Self> {
        let mut compiled = Self {
            version: COMPILED_EXPRESSION_PLAN_VERSION,
            ir_version: EXPRESSION_IR_VERSION,
            native_filter_lowering_version: NATIVE_FILTER_LOWERING_VERSION.to_owned(),
            substrait_version: None,
            predicates,
            residuals,
            contracts,
            transforms,
            content_sha256: String::new(),
        };
        compiled.content_sha256 = compiled.compute_content_sha256()?;
        Ok(compiled)
    }

    pub fn validate_recorded(&self) -> Result<()> {
        if self.version != COMPILED_EXPRESSION_PLAN_VERSION
            || self.ir_version != EXPRESSION_IR_VERSION
            || self.native_filter_lowering_version != NATIVE_FILTER_LOWERING_VERSION
            || self.substrait_version.is_some()
        {
            return Err(CdfError::contract(
                "recorded expression compatibility tuple is not supported by this engine version",
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
            .chain(&self.contracts)
            .chain(&self.transforms)
            .try_for_each(PlannedExpression::validate_recorded)
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
                .any(|(planned, rule)| {
                    planned.use_kind != ExpressionUse::Contract
                        || planned.original != rule.expression
                        || planned.optimized != rule.expression
                })
        {
            return Err(CdfError::contract(
                "recorded contract compiled expression plan does not match the executable row-rule program",
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
        let bytes = serde_json::to_vec(&(
            self.version,
            self.ir_version,
            &self.native_filter_lowering_version,
            &self.substrait_version,
            &self.predicates,
            &self.residuals,
            &self.contracts,
            &self.transforms,
        ))
        .map_err(|error| {
            CdfError::internal(format!("serialize compiled expression plan: {error}"))
        })?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }

    pub fn validate_predicate_bindings<'a>(
        &self,
        bindings: impl IntoIterator<Item = (&'a str, &'a Expression, bool)>,
    ) -> Result<()> {
        let bindings = bindings.into_iter().collect::<Vec<_>>();
        validate_filter_bindings("scan predicate", bindings, &self.predicates)
    }

    pub fn validate_residual_bindings<'a>(
        &self,
        bindings: impl IntoIterator<Item = (&'a str, &'a Expression)>,
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
    bindings: impl IntoIterator<Item = (&'a str, &'a Expression, bool)>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn contract_expression(function: &str) -> Expression {
        Expression::call(
            function,
            vec![ExpressionNode::Column {
                name: "id".to_owned(),
            }],
        )
    }

    fn planned_contract(expression: Expression) -> PlannedExpression {
        PlannedExpression {
            use_kind: ExpressionUse::Contract,
            source_text: None,
            functions: expression.function_dependencies(),
            original: expression.clone(),
            optimized: expression,
            optimizer: OptimizerIdentity {
                name: NATIVE_CONTRACT_OPTIMIZER.to_owned(),
                version: CDF_FUNCTION_VERSION.to_owned(),
            },
            fidelity: ExpressionFidelity::Exact,
            residuals: Vec::new(),
            lints: Vec::new(),
            substrait_version: None,
        }
    }

    #[test]
    fn compiled_contract_plan_rejects_optimizer_semantic_and_function_forgery() {
        let original = contract_expression("is_not_null");

        let mut divergent = planned_contract(original.clone());
        divergent.optimized = Expression::call(
            "in_domain",
            vec![
                ExpressionNode::Column {
                    name: "id".to_owned(),
                },
                ExpressionNode::Literal {
                    value: ExpressionLiteral::StringList(vec!["1".to_owned()]),
                },
            ],
        );
        divergent.functions = divergent.optimized.function_dependencies();
        let compiled =
            CompiledExpressionPlan::current(Vec::new(), Vec::new(), vec![divergent], Vec::new())
                .unwrap();
        assert!(compiled.validate_recorded().is_err());

        let mut wrong_optimizer = planned_contract(original);
        wrong_optimizer.optimizer = OptimizerIdentity {
            name: DATAFUSION_EXPRESSION_OPTIMIZER.to_owned(),
            version: DATAFUSION_EXPRESSION_PIN.to_owned(),
        };
        let compiled = CompiledExpressionPlan::current(
            Vec::new(),
            Vec::new(),
            vec![wrong_optimizer],
            Vec::new(),
        )
        .unwrap();
        assert!(compiled.validate_recorded().is_err());

        let unknown = planned_contract(contract_expression("unknown_contract_rule"));
        let compiled =
            CompiledExpressionPlan::current(Vec::new(), Vec::new(), vec![unknown], Vec::new())
                .unwrap();
        assert!(compiled.validate_recorded().is_err());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
