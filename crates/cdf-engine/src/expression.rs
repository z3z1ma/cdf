use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{
    DATAFUSION_EXPRESSION_OPTIMIZER, DATAFUSION_EXPRESSION_PIN, DATAFUSION_SCALAR_CONFIG_IDENTITY,
    DATAFUSION_SCALAR_FEATURE_SET, DATAFUSION_SCALAR_NAMESPACE, DeclarativeExpression,
    DeclarativeExpressionLiteral, DeclarativeExpressionNode, DeclarativeFunctionReference,
    ExpressionLint, ExpressionLintCode, ExpressionUse, NATIVE_CONTRACT_OPTIMIZER,
    OptimizerIdentity, PlannedContractExpression, PlannedExpression, ProjectionExpression,
    RelationalExpressionPlan, SOURCE_EXACT_PUSHDOWN_OPTIMIZER, ScalarBinaryOperator,
    ScalarCastMode, ScalarExpression, ScalarExpressionKind, ScalarExpressionNode,
    ScalarFunctionReference, ScalarFunctionVolatility, ScalarType, ScalarUnaryOperator,
};
use cdf_kernel::{CanonicalArrowSchema, CdfError, Result};
use datafusion::{common::DataFusionError, logical_expr::expr::ScalarFunction};
use datafusion::{
    common::{DFSchema, ScalarValue},
    execution::SessionStateDefaults,
    logical_expr::{
        BinaryExpr, Cast, Expr, ExprSchemable, Operator, TryCast, Volatility,
        simplify::SimplifyContext,
    },
    optimizer::simplify_expressions::ExprSimplifier,
};

/// Child ordinals from the analyzed expression root. D3 records explicit SQL `CAST` nodes here;
/// every unmarked DataFusion `Cast` is an analyzer-inserted implicit coercion.
pub type ExpressionPath = Vec<usize>;

/// Stable authored-source location supplied by the project compiler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpressionSourceLocation {
    pub file: String,
    pub line: u32,
    pub column: u32,
}

impl ExpressionSourceLocation {
    pub fn new(file: impl Into<String>, line: u32, column: u32) -> Result<Self> {
        let location = Self {
            file: file.into(),
            line,
            column,
        };
        location.validate()?;
        Ok(location)
    }

    fn validate(&self) -> Result<()> {
        if self.file.trim().is_empty() || self.line == 0 || self.column == 0 {
            return Err(CdfError::contract(
                "expression source location requires a file and one-based line/column",
            ));
        }
        Ok(())
    }
}

/// Fully resolved and coerced DataFusion scalar authority supplied by the compiler. When
/// DataFusion simplifies the execution graph, `admission_expression` retains the resolved graph
/// from before simplification so deterministic-admission and cast-provenance gates cannot be
/// optimized away. Runtime execution never receives either transient representation.
#[derive(Clone, Debug)]
pub struct AnalyzedScalarExpression {
    pub expression: Expr,
    pub admission_expression: Expr,
    pub explicit_casts: BTreeSet<ExpressionPath>,
    pub source_locations: BTreeMap<ExpressionPath, ExpressionSourceLocation>,
}

impl AnalyzedScalarExpression {
    pub fn new(expression: Expr) -> Self {
        Self {
            admission_expression: expression.clone(),
            expression,
            explicit_casts: BTreeSet::new(),
            source_locations: BTreeMap::new(),
        }
    }

    pub fn with_admission_expression(mut self, expression: Expr) -> Self {
        self.admission_expression = expression;
        self
    }

    pub fn with_explicit_cast(mut self, path: ExpressionPath) -> Self {
        self.explicit_casts.insert(path);
        self
    }

    pub fn with_source_location(
        mut self,
        path: ExpressionPath,
        location: ExpressionSourceLocation,
    ) -> Self {
        self.source_locations.insert(path, location);
        self
    }
}

#[derive(Clone, Debug)]
pub struct AnalyzedProjectionExpression {
    pub name: String,
    pub scalar: AnalyzedScalarExpression,
}

pub fn lower_analyzed_scalar_expression(
    analyzed: &AnalyzedScalarExpression,
    schema: &Schema,
) -> Result<ScalarExpression> {
    for location in analyzed.source_locations.values() {
        location.validate()?;
    }
    let df_schema = DFSchema::try_from(schema.clone()).map_err(datafusion_planning_error)?;
    if analyzed.admission_expression != analyzed.expression {
        // The resolved pre-simplification tree is a mandatory security/provenance gate. Its
        // result is deliberately discarded; only the separately admitted optimized tree becomes
        // durable execution identity.
        lower_graph(
            &analyzed.admission_expression,
            &df_schema,
            &BTreeSet::new(),
            &analyzed.source_locations,
        )?;
    }
    let root = lower_graph(
        &analyzed.expression,
        &df_schema,
        &analyzed.explicit_casts,
        &analyzed.source_locations,
    )?;
    let expression = ScalarExpression::current(root)?;
    // Admission includes exact runtime rebinding. This invokes no optimizer or coercion pass.
    crate::expression_execution::bind_scalar_expression(&expression, schema)?;
    Ok(expression)
}

pub fn compile_relational_expression_plan(
    input_schema: &Schema,
    filter: Option<AnalyzedScalarExpression>,
    projection: Vec<AnalyzedProjectionExpression>,
    control_fields: Vec<String>,
) -> Result<RelationalExpressionPlan> {
    let filter = filter
        .as_ref()
        .map(|expression| lower_analyzed_scalar_expression(expression, input_schema))
        .transpose()?;
    if filter.as_ref().is_some_and(|expression| {
        expression.root.scalar_type.data_type != cdf_kernel::CanonicalArrowType::Boolean
    }) {
        return Err(CdfError::contract(
            "relational filter must resolve to Boolean",
        ));
    }
    let projection = projection
        .into_iter()
        .enumerate()
        .map(|(ordinal, projection)| {
            let expression = lower_analyzed_scalar_expression(&projection.scalar, input_schema)?;
            Ok(ProjectionExpression::new(
                ordinal,
                projection.name,
                expression,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let output_schema = Schema::new_with_metadata(
        projection
            .iter()
            .map(|projection| {
                if let ScalarExpressionKind::Column { index, .. } =
                    projection.expression.root.expression
                {
                    let field = input_schema.fields().get(index).ok_or_else(|| {
                        CdfError::contract(format!(
                            "projection {:?} has stale pass-through ordinal {index}",
                            projection.name
                        ))
                    })?;
                    Ok(field.as_ref().clone().with_name(&projection.name))
                } else {
                    Ok(Field::new(
                        &projection.name,
                        projection.expression.root.scalar_type.to_arrow()?,
                        projection.expression.root.scalar_type.nullable,
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()?,
        input_schema.metadata().clone(),
    );
    RelationalExpressionPlan::current(
        CanonicalArrowSchema::from_arrow(input_schema)?,
        filter,
        projection,
        CanonicalArrowSchema::from_arrow(&output_schema)?,
        control_fields,
    )
}

pub(crate) fn plan_expression(
    expression: DeclarativeExpression,
    use_kind: ExpressionUse,
    schema: &Schema,
) -> Result<PlannedExpression> {
    expression.validate()?;
    let logical = declarative_to_datafusion(&expression.root)?;
    let df_schema =
        Arc::new(DFSchema::try_from(schema.clone()).map_err(datafusion_planning_error)?);
    let context = SimplifyContext::builder()
        .with_schema(Arc::clone(&df_schema))
        .build();
    let simplifier = ExprSimplifier::new(context);
    let coerced = simplifier
        .coerce(logical, df_schema.as_ref())
        .map_err(datafusion_planning_error)?;
    let optimized = simplifier
        .simplify(coerced.clone())
        .map_err(datafusion_planning_error)?;
    let typed = lower_analyzed_scalar_expression(
        &AnalyzedScalarExpression::new(optimized).with_admission_expression(coerced),
        schema,
    )?;
    if use_kind == ExpressionUse::Filter
        && typed.root.scalar_type.data_type != cdf_kernel::CanonicalArrowType::Boolean
    {
        return Err(CdfError::contract(
            "compiled filter expression does not produce Boolean",
        ));
    }
    let mut lints = lint_expression(&expression);
    if is_typed_true(&typed)
        && !lints
            .iter()
            .any(|lint| lint.code == ExpressionLintCode::AlwaysTrue)
    {
        lints.push(ExpressionLint {
            code: ExpressionLintCode::AlwaysTrue,
            message: "expression is provably always true".to_owned(),
        });
    }
    Ok(PlannedExpression {
        use_kind,
        source_text: None,
        original: expression,
        functions: typed.function_dependencies().to_vec(),
        expression: typed,
        optimizer: OptimizerIdentity {
            name: DATAFUSION_EXPRESSION_OPTIMIZER.to_owned(),
            version: DATAFUSION_EXPRESSION_PIN.to_owned(),
        },
        lints,
    })
}

pub(crate) fn record_native_contract_expression(
    expression: DeclarativeExpression,
    schema: &Schema,
) -> Result<PlannedContractExpression> {
    expression.validate()?;
    let function = match &expression.root {
        DeclarativeExpressionNode::Call { function, .. } => function.name.as_str(),
        _ => "",
    };
    if !matches!(
        function,
        "is_not_null"
            | "in_domain"
            | "in_range"
            | "matches_regex"
            | "fresh_within"
            | "dedup"
            | "exact_row_dedup"
    ) {
        return Err(CdfError::contract(format!(
            "contract expression function {function:?} has no admitted native fused lowering"
        )));
    }
    let mut lints = lint_expression(&expression);
    lints.extend(lint_contract_expression(&expression.root, schema));
    Ok(PlannedContractExpression {
        functions: expression.function_dependencies(),
        original: expression,
        optimizer: OptimizerIdentity {
            name: NATIVE_CONTRACT_OPTIMIZER.to_owned(),
            version: cdf_contract::CDF_FUNCTION_VERSION.to_owned(),
        },
        lints,
    })
}

pub(crate) fn record_exact_source_expression(
    expression: DeclarativeExpression,
    schema: &Schema,
) -> Result<PlannedExpression> {
    let mut planned = plan_expression(expression, ExpressionUse::Filter, schema)?;
    planned.optimizer = OptimizerIdentity {
        name: SOURCE_EXACT_PUSHDOWN_OPTIMIZER.to_owned(),
        version: cdf_contract::CDF_FUNCTION_VERSION.to_owned(),
    };
    Ok(planned)
}

pub(crate) fn validate_recorded_expressions(expressions: &[PlannedExpression]) -> Result<()> {
    expressions
        .iter()
        .try_for_each(PlannedExpression::validate_recorded)
}

pub(crate) fn validate_recorded_contract_expressions(
    expressions: &[PlannedContractExpression],
) -> Result<()> {
    expressions
        .iter()
        .try_for_each(PlannedContractExpression::validate_recorded)
}

pub(crate) fn lower_recorded_filter_for_pruning(
    expression: &ScalarExpression,
    schema: &Schema,
) -> Result<Option<Expr>> {
    expression.validate()?;
    for dependency in expression.column_dependencies() {
        let field = schema.fields().get(dependency.index).ok_or_else(|| {
            CdfError::contract(format!(
                "recorded pruning field {:?} has stale ordinal {}",
                dependency.name, dependency.index
            ))
        })?;
        if field.name() != &dependency.name
            || dependency.scalar_type
                != ScalarType::from_arrow(field.data_type(), field.is_nullable())?
        {
            return Err(CdfError::contract(format!(
                "recorded pruning field {:?} changed name, type, or nullability",
                dependency.name
            )));
        }
        if !pruning_data_type_supported(field.data_type()) {
            return Ok(None);
        }
    }
    pruning_node_supported(&expression.root)?
        .then(|| crate::expression_execution::reconstruct_node(&expression.root))
        .transpose()
}

fn pruning_data_type_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
    )
}

fn pruning_node_supported(node: &ScalarExpressionNode) -> Result<bool> {
    if !pruning_data_type_supported(&node.scalar_type.to_arrow()?) {
        return Ok(false);
    }
    Ok(match &node.expression {
        ScalarExpressionKind::Column { .. } | ScalarExpressionKind::Literal { .. } => true,
        ScalarExpressionKind::Unary { operator, argument } => {
            matches!(
                operator,
                ScalarUnaryOperator::Not
                    | ScalarUnaryOperator::IsNull
                    | ScalarUnaryOperator::IsNotNull
            ) && pruning_node_supported(argument)?
        }
        ScalarExpressionKind::Binary {
            operator,
            left,
            right,
        } => {
            matches!(
                operator,
                ScalarBinaryOperator::Equal
                    | ScalarBinaryOperator::NotEqual
                    | ScalarBinaryOperator::Less
                    | ScalarBinaryOperator::LessOrEqual
                    | ScalarBinaryOperator::Greater
                    | ScalarBinaryOperator::GreaterOrEqual
                    | ScalarBinaryOperator::And
                    | ScalarBinaryOperator::Or
            ) && pruning_node_supported(left)?
                && pruning_node_supported(right)?
        }
        ScalarExpressionKind::Cast {
            source_type,
            target_type,
            argument,
            ..
        } => {
            pruning_data_type_supported(&source_type.to_arrow()?)
                && pruning_data_type_supported(&target_type.to_arrow()?)
                && pruning_node_supported(argument)?
        }
        ScalarExpressionKind::Call { .. } => false,
    })
}

pub(crate) fn mark_cursor_subsumed(expressions: &mut [PlannedExpression], cursor_field: &str) {
    for candidate in 0..expressions.len() {
        let Some((field, operator, value)) = comparison(&expressions[candidate].original.root)
        else {
            continue;
        };
        if field != cursor_field {
            continue;
        }
        let subsumed = expressions.iter().enumerate().any(|(other_index, other)| {
            if other_index == candidate {
                return false;
            }
            let Some((other_field, other_operator, other_value)) = comparison(&other.original.root)
            else {
                return false;
            };
            other_field == field && bound_subsumes(other_operator, other_value, operator, value)
        });
        if subsumed {
            expressions[candidate].lints.push(ExpressionLint {
                code: ExpressionLintCode::CursorSubsumed,
                message: format!(
                    "cursor filter on {cursor_field:?} is provably subsumed by a stronger recorded filter"
                ),
            });
        }
    }
}

fn lower_graph(
    expression: &Expr,
    schema: &DFSchema,
    explicit_casts: &BTreeSet<ExpressionPath>,
    source_locations: &BTreeMap<ExpressionPath, ExpressionSourceLocation>,
) -> Result<ScalarExpressionNode> {
    let mut remaining_explicit_casts = explicit_casts.clone();
    let mut failed_path = None;
    let root = lower_node(
        expression,
        schema,
        &mut remaining_explicit_casts,
        &mut Vec::new(),
        &mut failed_path,
    )
    .map_err(|error| {
        attach_expression_location(
            error,
            failed_path.as_deref().unwrap_or_default(),
            source_locations,
        )
    })?;
    if let Some(path) = remaining_explicit_casts.into_iter().next() {
        return Err(attach_expression_location(
            CdfError::contract(format!(
                "explicit CAST provenance at expression path {} does not identify a resolved CAST node",
                display_expression_path(&path)
            )),
            &path,
            source_locations,
        ));
    }
    Ok(root)
}

fn lower_node(
    expression: &Expr,
    schema: &DFSchema,
    explicit_casts: &mut BTreeSet<ExpressionPath>,
    path: &mut ExpressionPath,
    failed_path: &mut Option<ExpressionPath>,
) -> Result<ScalarExpressionNode> {
    let result = lower_node_inner(expression, schema, explicit_casts, path, failed_path);
    if result.is_err() && failed_path.is_none() {
        *failed_path = Some(path.clone());
    }
    result
}

fn lower_node_inner(
    expression: &Expr,
    schema: &DFSchema,
    explicit_casts: &mut BTreeSet<ExpressionPath>,
    path: &mut ExpressionPath,
    failed_path: &mut Option<ExpressionPath>,
) -> Result<ScalarExpressionNode> {
    let scalar_type = expression_type(expression, schema)?;
    match expression {
        Expr::Alias(alias) => lower_node(&alias.expr, schema, explicit_casts, path, failed_path),
        Expr::Column(column) => {
            let index = schema
                .index_of_column(column)
                .map_err(datafusion_planning_error)?;
            Ok(ScalarExpressionNode::column(
                column.name.clone(),
                index,
                scalar_type,
            ))
        }
        Expr::Literal(value, _) => Ok(ScalarExpressionNode::literal(
            scalar_type,
            encode_scalar_literal(value)?,
        )),
        Expr::BinaryExpr(binary) => {
            let left = lower_child(&binary.left, 0, schema, explicit_casts, path, failed_path)?;
            let right = lower_child(&binary.right, 1, schema, explicit_casts, path, failed_path)?;
            ScalarExpressionNode::binary(binary_operator(binary.op)?, left, right, scalar_type)
        }
        Expr::Not(argument) => ScalarExpressionNode::unary(
            ScalarUnaryOperator::Not,
            lower_child(argument, 0, schema, explicit_casts, path, failed_path)?,
            scalar_type,
        ),
        Expr::Negative(argument) => ScalarExpressionNode::unary(
            ScalarUnaryOperator::Negative,
            lower_child(argument, 0, schema, explicit_casts, path, failed_path)?,
            scalar_type,
        ),
        Expr::IsNull(argument) => ScalarExpressionNode::unary(
            ScalarUnaryOperator::IsNull,
            lower_child(argument, 0, schema, explicit_casts, path, failed_path)?,
            scalar_type,
        ),
        Expr::IsNotNull(argument) => ScalarExpressionNode::unary(
            ScalarUnaryOperator::IsNotNull,
            lower_child(argument, 0, schema, explicit_casts, path, failed_path)?,
            scalar_type,
        ),
        Expr::Cast(Cast { expr, .. }) => {
            let mode = if explicit_casts.remove(path) {
                ScalarCastMode::Explicit
            } else {
                ScalarCastMode::Implicit
            };
            ScalarExpressionNode::cast(
                mode,
                lower_child(expr, 0, schema, explicit_casts, path, failed_path)?,
                scalar_type,
            )
        }
        Expr::TryCast(TryCast { expr, .. }) => ScalarExpressionNode::cast(
            ScalarCastMode::Try,
            lower_child(expr, 0, schema, explicit_casts, path, failed_path)?,
            scalar_type,
        ),
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            let builtin = SessionStateDefaults::default_scalar_functions()
                .into_iter()
                .find(|builtin| builtin.as_ref() == func.as_ref())
                .ok_or_else(|| {
                    CdfError::contract(format!(
                        "scalar function {:?} failed admission: it is not the pinned DataFusion built-in implementation",
                        func.name()
                    ))
                })?;
            if builtin.signature().volatility != Volatility::Immutable {
                return Err(CdfError::contract(format!(
                    "scalar function {:?} failed admission: volatility is {:?}, expected Immutable",
                    builtin.name(),
                    builtin.signature().volatility
                )));
            }
            let arguments = args
                .iter()
                .enumerate()
                .map(|(index, argument)| {
                    lower_child(argument, index, schema, explicit_casts, path, failed_path)
                })
                .collect::<Result<Vec<_>>>()?;
            let function = ScalarFunctionReference {
                namespace: DATAFUSION_SCALAR_NAMESPACE.to_owned(),
                canonical_name: builtin.name().to_owned(),
                implementation_version: DATAFUSION_EXPRESSION_PIN.to_owned(),
                feature_set: DATAFUSION_SCALAR_FEATURE_SET.to_owned(),
                config_identity: DATAFUSION_SCALAR_CONFIG_IDENTITY.to_owned(),
                volatility: ScalarFunctionVolatility::Immutable,
                argument_types: arguments
                    .iter()
                    .map(|argument| argument.scalar_type.clone())
                    .collect(),
                return_type: scalar_type.clone(),
            };
            ScalarExpressionNode::call(function, arguments, scalar_type)
        }
        other => Err(CdfError::contract(format!(
            "DataFusion scalar node {} failed admission: the initial CDF scalar IR cannot represent it",
            other.variant_name()
        ))),
    }
}

fn lower_child(
    expression: &Expr,
    ordinal: usize,
    schema: &DFSchema,
    explicit_casts: &mut BTreeSet<ExpressionPath>,
    path: &mut ExpressionPath,
    failed_path: &mut Option<ExpressionPath>,
) -> Result<ScalarExpressionNode> {
    path.push(ordinal);
    let result = lower_node(expression, schema, explicit_casts, path, failed_path);
    path.pop();
    result
}

fn attach_expression_location(
    mut error: CdfError,
    path: &[usize],
    source_locations: &BTreeMap<ExpressionPath, ExpressionSourceLocation>,
) -> CdfError {
    let location = (0..=path.len())
        .rev()
        .find_map(|length| source_locations.get(&path[..length]));
    let path = display_expression_path(path);
    error.message = match location {
        Some(location) => format!(
            "{} at {}:{}:{} (expression path {path})",
            error.message, location.file, location.line, location.column
        ),
        None => format!("{} at expression path {path}", error.message),
    };
    error
}

fn display_expression_path(path: &[usize]) -> String {
    if path.is_empty() {
        return "root".to_owned();
    }
    path.iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(".")
}

fn expression_type(expression: &Expr, schema: &DFSchema) -> Result<ScalarType> {
    let (_, field) = expression
        .to_field(schema)
        .map_err(datafusion_planning_error)?;
    ScalarType::from_arrow(field.data_type(), field.is_nullable())
}

fn encode_scalar_literal(value: &ScalarValue) -> Result<Vec<u8>> {
    let array = value
        .to_array_of_size(1)
        .map_err(datafusion_planning_error)?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).map_err(CdfError::from)?;
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, schema.as_ref()).map_err(|error| {
            CdfError::internal(format!("encode scalar literal schema: {error}"))
        })?;
        writer
            .write(&batch)
            .map_err(|error| CdfError::internal(format!("encode scalar literal batch: {error}")))?;
        writer.finish().map_err(|error| {
            CdfError::internal(format!("finish scalar literal encoding: {error}"))
        })?;
    }
    Ok(bytes)
}

fn declarative_to_datafusion(node: &DeclarativeExpressionNode) -> Result<Expr> {
    match node {
        DeclarativeExpressionNode::Column { name } => Ok(datafusion::logical_expr::col(name)),
        DeclarativeExpressionNode::Literal { value } => {
            Ok(Expr::Literal(declarative_scalar(value)?, None))
        }
        DeclarativeExpressionNode::Call {
            function,
            arguments,
        } => {
            require_current_declarative_function(function)?;
            match (function.name.as_str(), arguments.as_slice()) {
                ("not", [argument]) => {
                    Ok(Expr::Not(Box::new(declarative_to_datafusion(argument)?)))
                }
                ("is_null", [argument]) => {
                    Ok(Expr::IsNull(Box::new(declarative_to_datafusion(argument)?)))
                }
                ("is_not_null", [argument]) => Ok(Expr::IsNotNull(Box::new(
                    declarative_to_datafusion(argument)?,
                ))),
                (name, [left, right]) => Ok(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(declarative_to_datafusion(left)?),
                    declarative_operator(name)?,
                    Box::new(declarative_to_datafusion(right)?),
                ))),
                (name, _) => Err(CdfError::contract(format!(
                    "declarative expression function {name:?} has unsupported arity"
                ))),
            }
        }
        _ => Err(CdfError::contract(
            "declarative expression node is unsupported by this compiler",
        )),
    }
}

fn require_current_declarative_function(function: &DeclarativeFunctionReference) -> Result<()> {
    if function.namespace != cdf_contract::CDF_FUNCTION_NAMESPACE
        || function.version != cdf_contract::CDF_FUNCTION_VERSION
    {
        return Err(CdfError::contract(format!(
            "unsupported declarative function {}.{}@{}",
            function.namespace, function.name, function.version
        )));
    }
    Ok(())
}

fn declarative_operator(name: &str) -> Result<Operator> {
    match name {
        "eq" => Ok(Operator::Eq),
        "neq" => Ok(Operator::NotEq),
        "gt" => Ok(Operator::Gt),
        "gte" => Ok(Operator::GtEq),
        "lt" => Ok(Operator::Lt),
        "lte" => Ok(Operator::LtEq),
        "and" => Ok(Operator::And),
        "or" => Ok(Operator::Or),
        other => Err(CdfError::contract(format!(
            "declarative function {other:?} has no DataFusion scalar lowering"
        ))),
    }
}

fn binary_operator(operator: Operator) -> Result<ScalarBinaryOperator> {
    Ok(match operator {
        Operator::Eq => ScalarBinaryOperator::Equal,
        Operator::NotEq => ScalarBinaryOperator::NotEqual,
        Operator::Lt => ScalarBinaryOperator::Less,
        Operator::LtEq => ScalarBinaryOperator::LessOrEqual,
        Operator::Gt => ScalarBinaryOperator::Greater,
        Operator::GtEq => ScalarBinaryOperator::GreaterOrEqual,
        Operator::Plus => ScalarBinaryOperator::Add,
        Operator::Minus => ScalarBinaryOperator::Subtract,
        Operator::Multiply => ScalarBinaryOperator::Multiply,
        Operator::Divide => ScalarBinaryOperator::Divide,
        Operator::Modulo => ScalarBinaryOperator::Modulo,
        Operator::And => ScalarBinaryOperator::And,
        Operator::Or => ScalarBinaryOperator::Or,
        Operator::IsDistinctFrom => ScalarBinaryOperator::IsDistinctFrom,
        Operator::IsNotDistinctFrom => ScalarBinaryOperator::IsNotDistinctFrom,
        Operator::RegexMatch => ScalarBinaryOperator::RegexMatch,
        Operator::RegexIMatch => ScalarBinaryOperator::RegexInsensitiveMatch,
        Operator::RegexNotMatch => ScalarBinaryOperator::RegexNotMatch,
        Operator::RegexNotIMatch => ScalarBinaryOperator::RegexNotInsensitiveMatch,
        Operator::LikeMatch => ScalarBinaryOperator::Like,
        Operator::ILikeMatch => ScalarBinaryOperator::InsensitiveLike,
        Operator::NotLikeMatch => ScalarBinaryOperator::NotLike,
        Operator::NotILikeMatch => ScalarBinaryOperator::NotInsensitiveLike,
        Operator::BitwiseAnd => ScalarBinaryOperator::BitwiseAnd,
        Operator::BitwiseOr => ScalarBinaryOperator::BitwiseOr,
        Operator::BitwiseXor => ScalarBinaryOperator::BitwiseXor,
        Operator::BitwiseShiftRight => ScalarBinaryOperator::BitwiseShiftRight,
        Operator::BitwiseShiftLeft => ScalarBinaryOperator::BitwiseShiftLeft,
        Operator::StringConcat => ScalarBinaryOperator::StringConcat,
        Operator::AtArrow => ScalarBinaryOperator::ListContains,
        Operator::ArrowAt => ScalarBinaryOperator::ListContainedBy,
        other => {
            return Err(CdfError::contract(format!(
                "DataFusion operator {other:?} failed admission: no executable CDF scalar identity"
            )));
        }
    })
}

fn declarative_scalar(value: &DeclarativeExpressionLiteral) -> Result<ScalarValue> {
    match value {
        DeclarativeExpressionLiteral::Null => Ok(ScalarValue::Null),
        DeclarativeExpressionLiteral::Boolean(value) => Ok(ScalarValue::Boolean(Some(*value))),
        DeclarativeExpressionLiteral::Signed(value) => Ok(ScalarValue::Int64(Some(*value))),
        DeclarativeExpressionLiteral::Unsigned(value) => Ok(ScalarValue::UInt64(Some(*value))),
        DeclarativeExpressionLiteral::Float64Bits(bits) => {
            Ok(ScalarValue::Float64(Some(f64::from_bits(*bits))))
        }
        DeclarativeExpressionLiteral::String(value) => Ok(ScalarValue::Utf8(Some(value.clone()))),
        DeclarativeExpressionLiteral::StringList(_) => Err(CdfError::contract(
            "contract list literals are not scalar SQL expressions",
        )),
        _ => Err(CdfError::contract(
            "declarative literal is unsupported by this compiler",
        )),
    }
}

fn lint_expression(expression: &DeclarativeExpression) -> Vec<ExpressionLint> {
    let mut lints = Vec::new();
    if is_unsatisfiable_range(&expression.root) {
        lints.push(ExpressionLint {
            code: ExpressionLintCode::UnsatisfiableRange,
            message: "expression contains a provably empty range".to_owned(),
        });
    }
    if matches!(
        expression.root,
        DeclarativeExpressionNode::Literal {
            value: DeclarativeExpressionLiteral::Boolean(true)
        }
    ) {
        lints.push(ExpressionLint {
            code: ExpressionLintCode::AlwaysTrue,
            message: "expression is provably always true".to_owned(),
        });
    }
    lints
}

fn is_typed_true(expression: &ScalarExpression) -> bool {
    let cdf_kernel::ScalarExpressionKind::Literal { arrow_ipc } = &expression.root.expression
    else {
        return false;
    };
    crate::expression_execution::decode_scalar_literal(arrow_ipc)
        .ok()
        .is_some_and(|value| value == ScalarValue::Boolean(Some(true)))
}

fn lint_contract_expression(
    node: &DeclarativeExpressionNode,
    schema: &Schema,
) -> Vec<ExpressionLint> {
    let DeclarativeExpressionNode::Call {
        function,
        arguments,
    } = node
    else {
        return Vec::new();
    };
    match (function.name.as_str(), arguments.as_slice()) {
        ("is_not_null", [DeclarativeExpressionNode::Column { name }]) => schema
            .field_with_name(name)
            .ok()
            .filter(|field| !field.is_nullable())
            .map(|_| {
                vec![ExpressionLint {
                    code: ExpressionLintCode::AlwaysTrue,
                    message: format!(
                        "nullability rule for non-nullable field {name:?} is provably always true"
                    ),
                }]
            })
            .unwrap_or_default(),
        (
            "in_range",
            [
                DeclarativeExpressionNode::Column { name },
                DeclarativeExpressionNode::Literal { value: min },
                DeclarativeExpressionNode::Literal { value: max },
            ],
        ) => {
            if matches!(min, DeclarativeExpressionLiteral::Null)
                && matches!(max, DeclarativeExpressionLiteral::Null)
            {
                return vec![ExpressionLint {
                    code: ExpressionLintCode::AlwaysTrue,
                    message: format!("unbounded range rule for {name:?} is always true"),
                }];
            }
            let Some(field) = schema.field_with_name(name).ok() else {
                return Vec::new();
            };
            let (
                DeclarativeExpressionLiteral::String(min),
                DeclarativeExpressionLiteral::String(max),
            ) = (min, max)
            else {
                return Vec::new();
            };
            cdf_contract::range_bounds_are_unsatisfiable(field.data_type(), Some(min), Some(max))
                .unwrap_or(false)
                .then(|| ExpressionLint {
                    code: ExpressionLintCode::UnsatisfiableRange,
                    message: format!("range rule for {name:?} is provably empty"),
                })
                .into_iter()
                .collect()
        }
        _ => Vec::new(),
    }
}

fn is_unsatisfiable_range(node: &DeclarativeExpressionNode) -> bool {
    let DeclarativeExpressionNode::Call {
        function,
        arguments,
    } = node
    else {
        return false;
    };
    if function.name != "and" || arguments.len() != 2 {
        return arguments.iter().any(is_unsatisfiable_range);
    }
    let Some((left_column, left_op, left_value)) = comparison(&arguments[0]) else {
        return false;
    };
    let Some((right_column, right_op, right_value)) = comparison(&arguments[1]) else {
        return false;
    };
    if left_column != right_column {
        return false;
    }
    let (lower_op, lower, upper_op, upper) =
        if matches!(left_op, "gt" | "gte") && matches!(right_op, "lt" | "lte") {
            (left_op, left_value, right_op, right_value)
        } else if matches!(right_op, "gt" | "gte") && matches!(left_op, "lt" | "lte") {
            (right_op, right_value, left_op, left_value)
        } else {
            return false;
        };
    match (lower, upper) {
        (
            DeclarativeExpressionLiteral::Signed(lower),
            DeclarativeExpressionLiteral::Signed(upper),
        ) => lower > upper || (lower == upper && (lower_op == "gt" || upper_op == "lt")),
        (
            DeclarativeExpressionLiteral::Unsigned(lower),
            DeclarativeExpressionLiteral::Unsigned(upper),
        ) => lower > upper || (lower == upper && (lower_op == "gt" || upper_op == "lt")),
        _ => false,
    }
}

fn comparison(
    node: &DeclarativeExpressionNode,
) -> Option<(&str, &str, &DeclarativeExpressionLiteral)> {
    let DeclarativeExpressionNode::Call {
        function,
        arguments,
    } = node
    else {
        return None;
    };
    let [
        DeclarativeExpressionNode::Column { name },
        DeclarativeExpressionNode::Literal { value },
    ] = arguments.as_slice()
    else {
        return None;
    };
    Some((name, function.name.as_str(), value))
}

fn bound_subsumes(
    stronger_operator: &str,
    stronger: &DeclarativeExpressionLiteral,
    candidate_operator: &str,
    candidate: &DeclarativeExpressionLiteral,
) -> bool {
    match (stronger, candidate) {
        (
            DeclarativeExpressionLiteral::Signed(stronger),
            DeclarativeExpressionLiteral::Signed(candidate),
        ) => numeric_bound_subsumes(stronger_operator, *stronger, candidate_operator, *candidate),
        (
            DeclarativeExpressionLiteral::Unsigned(stronger),
            DeclarativeExpressionLiteral::Unsigned(candidate),
        ) => numeric_bound_subsumes(stronger_operator, *stronger, candidate_operator, *candidate),
        _ => false,
    }
}

fn numeric_bound_subsumes<T: Ord>(
    stronger_operator: &str,
    stronger: T,
    candidate_operator: &str,
    candidate: T,
) -> bool {
    match (stronger_operator, candidate_operator) {
        ("gt" | "gte", "gt" | "gte") => {
            stronger > candidate
                || stronger == candidate
                    && (stronger_operator == "gt" || candidate_operator == "gte")
        }
        ("lt" | "lte", "lt" | "lte") => {
            stronger < candidate
                || stronger == candidate
                    && (stronger_operator == "lt" || candidate_operator == "lte")
        }
        _ => false,
    }
}

#[derive(Clone, Copy)]
pub(crate) enum DataFusionErrorPhase {
    Planning,
    Binding,
    Execution,
}

impl DataFusionErrorPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Planning => "analysis",
            Self::Binding => "runtime binding",
            Self::Execution => "execution",
        }
    }
}

pub(crate) fn classify_datafusion_error(
    error: DataFusionError,
    phase: DataFusionErrorPhase,
) -> CdfError {
    if let Some(mut embedded) = embedded_cdf_error(&error) {
        embedded.message = format!(
            "pinned DataFusion scalar {} failed: {}",
            phase.label(),
            embedded.message
        );
        return embedded;
    }
    let rendered = error.strip_backtrace();
    let root = error.find_root();
    let message = format!(
        "pinned DataFusion scalar {} failed ({}): {rendered}",
        phase.label(),
        datafusion_error_variant(root)
    );
    match root {
        DataFusionError::Plan(_)
        | DataFusionError::Configuration(_)
        | DataFusionError::SchemaError(_, _)
        | DataFusionError::NotImplemented(_) => CdfError::contract(message),
        DataFusionError::ResourcesExhausted(_) | DataFusionError::IoError(_) => {
            CdfError::environment(message)
        }
        DataFusionError::Execution(_) | DataFusionError::ArrowError(_, _)
            if matches!(phase, DataFusionErrorPhase::Execution) =>
        {
            CdfError::data(message)
        }
        _ => CdfError::internal(message),
    }
}

fn datafusion_error_variant(error: &DataFusionError) -> &'static str {
    match error {
        DataFusionError::ArrowError(_, _) => "arrow",
        DataFusionError::IoError(_) => "io",
        DataFusionError::NotImplemented(_) => "not_implemented",
        DataFusionError::Internal(_) => "internal",
        DataFusionError::Plan(_) => "plan",
        DataFusionError::Configuration(_) => "configuration",
        DataFusionError::SchemaError(_, _) => "schema",
        DataFusionError::Execution(_) => "execution",
        DataFusionError::ExecutionJoin(_) => "execution_join",
        DataFusionError::ResourcesExhausted(_) => "resources_exhausted",
        DataFusionError::External(_) => "external",
        DataFusionError::Context(_, _) => "context",
        DataFusionError::Substrait(_) => "substrait",
        DataFusionError::Diagnostic(_, _) => "diagnostic",
        DataFusionError::Collection(_) => "collection",
        DataFusionError::Shared(_) => "shared",
        DataFusionError::Ffi(_) => "ffi",
        _ => "dependency",
    }
}

fn embedded_cdf_error(error: &(dyn std::error::Error + 'static)) -> Option<CdfError> {
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(error) = source.downcast_ref::<CdfError>() {
            return Some(error.clone());
        }
        if let Some(error) = source.downcast_ref::<std::io::Error>()
            && let Some(error) = cdf_kernel::embedded_cdf_error(error)
        {
            return Some(error);
        }
        current = source.source();
    }
    None
}

fn datafusion_planning_error(error: DataFusionError) -> CdfError {
    classify_datafusion_error(error, DataFusionErrorPhase::Planning)
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::DataType;
    use cdf_memory::{
        ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator,
        ReservationRequest,
    };
    use datafusion::{
        common::DFSchema,
        logical_expr::{ExprSchemable, col, lit},
    };

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    #[test]
    fn declarative_filter_lowers_to_current_typed_ir() {
        let planned = plan_expression(
            DeclarativeExpression::parse_comparison("id >= 7").unwrap(),
            ExpressionUse::Filter,
            &schema(),
        )
        .unwrap();
        planned.validate_recorded().unwrap();
        assert_eq!(planned.expression.version, 2);
        assert_eq!(planned.expression.column_dependencies()[0].name, "id");
    }

    #[test]
    fn scalar_function_admission_is_registry_and_volatility_based() {
        let lower = SessionStateDefaults::default_scalar_functions()
            .into_iter()
            .find(|function| function.name() == "lower")
            .unwrap();
        let expression = lower.call(vec![col("name")]);
        let typed =
            lower_analyzed_scalar_expression(&AnalyzedScalarExpression::new(expression), &schema())
                .unwrap();
        assert_eq!(typed.function_dependencies()[0].canonical_name, "lower");
        assert_eq!(
            typed.function_dependencies()[0].volatility,
            ScalarFunctionVolatility::Immutable
        );
    }

    #[test]
    fn explicit_and_try_casts_are_distinct_durable_nodes() {
        let explicit = Expr::Cast(Cast::new(Box::new(col("name")), DataType::Int64));
        let typed = lower_analyzed_scalar_expression(
            &AnalyzedScalarExpression::new(explicit).with_explicit_cast(Vec::new()),
            &schema(),
        )
        .unwrap();
        assert!(matches!(
            typed.root.expression,
            cdf_kernel::ScalarExpressionKind::Cast {
                mode: ScalarCastMode::Explicit,
                ..
            }
        ));

        let try_cast = Expr::TryCast(TryCast::new(Box::new(col("name")), DataType::Int64));
        let typed =
            lower_analyzed_scalar_expression(&AnalyzedScalarExpression::new(try_cast), &schema())
                .unwrap();
        assert!(matches!(
            typed.root.expression,
            cdf_kernel::ScalarExpressionKind::Cast {
                mode: ScalarCastMode::Try,
                ..
            }
        ));
    }

    #[test]
    fn relational_plan_executes_filter_before_projection() {
        let plan = compile_relational_expression_plan(
            &schema(),
            Some(AnalyzedScalarExpression::new(col("id").gt(lit(1_i64)))),
            vec![AnalyzedProjectionExpression {
                name: "normalized".to_owned(),
                scalar: AnalyzedScalarExpression::new(col("name")),
            }],
            Vec::new(),
        )
        .unwrap();
        let input = RecordBatch::try_new(
            Arc::new(schema()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let bytes = u64::try_from(input.get_array_memory_size())
            .unwrap()
            .saturating_mul(16);
        let memory = DeterministicMemoryCoordinator::new(bytes, BTreeMap::new()).unwrap();
        let lease = memory
            .try_reserve(
                &ReservationRequest::new(
                    ConsumerKey::new("relational-expression-test", MemoryClass::Transform).unwrap(),
                    bytes,
                )
                .unwrap(),
            )
            .unwrap()
            .unwrap();
        let output = crate::expression_execution::execute_relational_expression_plan(
            &plan,
            &input,
            &lease,
            &cdf_runtime::RunCancellation::default(),
        )
        .unwrap();
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.schema().field(0).name(), "normalized");
    }

    #[test]
    fn analyzed_output_field_agrees_with_recorded_type() {
        let expr = col("id") + lit(1_i64);
        let df_schema = DFSchema::try_from(schema()).unwrap();
        let (_, field) = expr.to_field(&df_schema).unwrap();
        let typed =
            lower_analyzed_scalar_expression(&AnalyzedScalarExpression::new(expr), &schema())
                .unwrap();
        assert_eq!(
            typed.root.scalar_type.to_arrow().unwrap(),
            field.data_type().clone()
        );
        assert_eq!(typed.root.scalar_type.nullable, field.is_nullable());
    }
}
