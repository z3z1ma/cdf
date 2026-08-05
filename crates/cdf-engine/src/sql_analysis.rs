use std::{collections::BTreeMap, sync::Arc};

use arrow_schema::Schema;
use cdf_contract::RelationalExpressionPlan;
use cdf_kernel::{CanonicalArrowSchema, CdfError, Result};
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    datasource::MemTable,
    logical_expr::{Expr as LogicalExpr, LogicalPlan},
    prelude::SessionContext,
    sql::{
        parser::{DFParser, Statement as DataFusionStatement},
        sqlparser::ast::{
            Expr, FunctionArg, FunctionArgExpr, FunctionArgOperator, FunctionArguments,
            GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, SetExpr, Spanned as _,
            Statement, TableFactor, UnaryOperator, Value,
        },
    },
};
use futures_executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number};

use crate::{
    AnalyzedProjectionExpression, AnalyzedScalarExpression, compile_relational_expression_plan,
};

const UPSTREAM_TABLE: &str = "__cdf_upstream";

#[derive(Clone, Copy)]
struct QueryOrigin {
    start_line: u32,
    start_column: u32,
}

impl QueryOrigin {
    fn new(start_line: u32, start_column: u32) -> Result<Self> {
        if start_line == 0 || start_column == 0 {
            return Err(CdfError::internal(
                "project query offset must use one-based line and column",
            ));
        }
        Ok(Self {
            start_line,
            start_column,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSqlSpan {
    pub start_line: u32,
    pub start_column: u32,
    pub end_line: u32,
    pub end_column: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParsedUpstreamRelation {
    pub configured_source: String,
    pub resource_options: BTreeMap<String, serde_json::Value>,
    pub canonical_arguments_hash: String,
    pub span: ProjectSqlSpan,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParsedProjectQuery {
    pub upstream: ParsedUpstreamRelation,
    pub normalized_query: String,
    pub authored_ast_hash: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyzedProjectQuery {
    pub upstream: ParsedUpstreamRelation,
    pub normalized_query: String,
    pub authored_ast_hash: String,
    pub relational_plan: RelationalExpressionPlan,
    pub output_schema: CanonicalArrowSchema,
}

pub fn parse_project_query(sql: &str, file: &str) -> Result<ParsedProjectQuery> {
    parse_project_query_at(sql, file, 1, 1)
}

pub fn parse_project_query_at(
    sql: &str,
    file: &str,
    start_line: u32,
    start_column: u32,
) -> Result<ParsedProjectQuery> {
    let origin = QueryOrigin::new(start_line, start_column)?;
    let (_, upstream, normalized_query) = parse_and_rewrite(sql, file, origin)?;
    let authored_ast_hash = cdf_runtime::artifact_hash(&normalized_query)?;
    Ok(ParsedProjectQuery {
        upstream,
        normalized_query,
        authored_ast_hash,
    })
}

pub fn analyze_project_query(
    sql: &str,
    file: &str,
    input_schema: &Schema,
    control_fields: Vec<String>,
) -> Result<AnalyzedProjectQuery> {
    analyze_project_query_at(sql, file, 1, 1, input_schema, control_fields)
}

pub fn analyze_project_query_at(
    sql: &str,
    file: &str,
    start_line: u32,
    start_column: u32,
    input_schema: &Schema,
    control_fields: Vec<String>,
) -> Result<AnalyzedProjectQuery> {
    let origin = QueryOrigin::new(start_line, start_column)?;
    let (query, upstream, normalized_query) = parse_and_rewrite(sql, file, origin)?;
    let authored_ast_hash = cdf_runtime::artifact_hash(&normalized_query)?;
    let context = SessionContext::new();
    let table = MemTable::try_new(Arc::new(input_schema.clone()), vec![Vec::new()])
        .map_err(datafusion_error)?;
    context
        .register_table(UPSTREAM_TABLE, Arc::new(table))
        .map_err(datafusion_error)?;
    let rewritten_sql = query.to_string();
    let frame = block_on(context.sql(&rewritten_sql)).map_err(|error| {
        sql_error(
            "CDF-SQL-ANALYSIS",
            file,
            format!("DataFusion could not resolve the admitted query: {error}"),
        )
    })?;
    let plan = frame.into_unoptimized_plan();
    let (projection, filter) = admitted_relational_nodes(&plan, file)?;
    let projection = projection
        .expr
        .iter()
        .zip(projection.schema.fields())
        .map(|(expression, field)| {
            Ok(AnalyzedProjectionExpression {
                name: field.name().to_owned(),
                scalar: AnalyzedScalarExpression::new(unqualify_columns(expression.clone())?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let filter = filter
        .map(|expression| unqualify_columns(expression.clone()).map(AnalyzedScalarExpression::new))
        .transpose()?;
    let relational_plan =
        compile_relational_expression_plan(input_schema, filter, projection, control_fields)?;
    let output_schema = relational_plan.output_schema.clone();
    Ok(AnalyzedProjectQuery {
        upstream,
        normalized_query,
        authored_ast_hash,
        relational_plan,
        output_schema,
    })
}

fn offset_span(
    span: &ProjectSqlSpan,
    start_line: u32,
    start_column: u32,
) -> Result<ProjectSqlSpan> {
    let line_offset = start_line - 1;
    let offset_line = |line: u32| {
        line.checked_add(line_offset)
            .ok_or_else(|| CdfError::contract("SQL source line offset overflowed u32"))
    };
    let offset_column = |line: u32, column: u32| {
        if line == 1 {
            column
                .checked_add(start_column - 1)
                .ok_or_else(|| CdfError::contract("SQL source column offset overflowed u32"))
        } else {
            Ok(column)
        }
    };
    Ok(ProjectSqlSpan {
        start_line: offset_line(span.start_line)?,
        start_column: offset_column(span.start_line, span.start_column)?,
        end_line: offset_line(span.end_line)?,
        end_column: offset_column(span.end_line, span.end_column)?,
    })
}

fn parse_and_rewrite(
    sql: &str,
    file: &str,
    origin: QueryOrigin,
) -> Result<(Query, ParsedUpstreamRelation, String)> {
    if sql.trim().is_empty() {
        return Err(sql_error(
            "CDF-SQL-EMPTY",
            file,
            "resource file must contain one SELECT query",
        ));
    }
    let mut statements = DFParser::parse_sql(sql).map_err(|error| {
        sql_error(
            "CDF-SQL-PARSE",
            file,
            format!("could not parse resource SQL: {error}"),
        )
    })?;
    if statements.len() != 1 {
        return Err(sql_error(
            "CDF-SQL-STATEMENT-COUNT",
            file,
            "resource file must contain exactly one statement",
        ));
    }
    let statement = statements.pop_front().ok_or_else(|| {
        sql_error(
            "CDF-SQL-STATEMENT-COUNT",
            file,
            "resource file must contain exactly one statement",
        )
    })?;
    let mut query = match statement {
        DataFusionStatement::Statement(statement) => match *statement {
            Statement::Query(query) => *query,
            _ => {
                return Err(sql_error(
                    "CDF-SQL-STATEMENT",
                    file,
                    "resource file must contain a SELECT query",
                ));
            }
        },
        _ => {
            return Err(sql_error(
                "CDF-SQL-STATEMENT",
                file,
                "DataFusion extension statements are not resource queries",
            ));
        }
    };
    validate_query_shape(&query, file)?;
    let normalized_query = query.to_string();
    let select = match query.body.as_mut() {
        SetExpr::Select(select) => select,
        _ => {
            return Err(sql_error(
                "CDF-SQL-SET-OPERATION",
                file,
                "set operations, VALUES, and parenthesized query bodies are not admitted",
            ));
        }
    };
    if select.from.len() != 1 {
        return Err(sql_error(
            "CDF-SQL-UPSTREAM-COUNT",
            file,
            "resource query requires exactly one FROM upstream(...) relation",
        ));
    }
    let from = select.from.get_mut(0).ok_or_else(|| {
        sql_error(
            "CDF-SQL-UPSTREAM-COUNT",
            file,
            "resource query requires exactly one FROM upstream(...) relation",
        )
    })?;
    if !from.joins.is_empty() {
        return Err(sql_error(
            "CDF-SQL-JOIN",
            file,
            "joins and multiple upstream relations are not admitted",
        ));
    }
    let upstream = parse_upstream_relation(&mut from.relation, file, origin)?;
    Ok((query, upstream, normalized_query))
}

fn validate_query_shape(query: &Query, file: &str) -> Result<()> {
    if query.with.is_some() {
        return Err(sql_error(
            "CDF-SQL-WITH",
            file,
            "WITH queries and subqueries are not admitted",
        ));
    }
    if query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(sql_error(
            "CDF-SQL-QUERY-CLAUSE",
            file,
            "ORDER/LIMIT/FETCH/LOCK/SETTINGS/FORMAT/pipe clauses are not admitted",
        ));
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(sql_error(
            "CDF-SQL-SET-OPERATION",
            file,
            "set operations, VALUES, and parenthesized query bodies are not admitted",
        ));
    };
    let empty_group = matches!(
        &select.group_by,
        GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty()
    );
    if !empty_group
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || !select.optimizer_hints.is_empty()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(sql_error(
            "CDF-SQL-RELATIONAL-SHAPE",
            file,
            "only projection, scalar expressions, one upstream relation, and an optional WHERE filter are admitted",
        ));
    }
    Ok(())
}

fn parse_upstream_relation(
    relation: &mut TableFactor,
    file: &str,
    origin: QueryOrigin,
) -> Result<ParsedUpstreamRelation> {
    let relation_span = located_span(relation.span(), file, origin)?;
    let TableFactor::Table {
        name,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = relation
    else {
        return Err(sql_error_at(
            "CDF-SQL-UPSTREAM-RELATION",
            file,
            &relation_span,
            "FROM must contain exactly upstream(source => 'name', ...)",
        ));
    };
    if !is_unquoted_name(name, "upstream") || args.is_none() {
        return Err(sql_error_at(
            "CDF-SQL-UPSTREAM-RELATION",
            file,
            &relation_span,
            "FROM must contain exactly upstream(source => 'name', ...)",
        ));
    }
    if !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Err(sql_error_at(
            "CDF-SQL-UPSTREAM-MODIFIER",
            file,
            &relation_span,
            "upstream(...) does not admit table hints, versions, partitions, ordinality, paths, samples, or index hints",
        ));
    }
    let table_args = args.take().ok_or_else(|| {
        sql_error_at(
            "CDF-SQL-UPSTREAM-RELATION",
            file,
            &relation_span,
            "upstream requires named arguments",
        )
    })?;
    if table_args.settings.is_some() {
        return Err(sql_error_at(
            "CDF-SQL-UPSTREAM-MODIFIER",
            file,
            &relation_span,
            "upstream arguments do not admit SETTINGS",
        ));
    }
    let mut configured_source = None;
    let mut resource_options = BTreeMap::new();
    for argument in table_args.args {
        let argument_span = located_span(argument.span(), file, origin)?;
        let FunctionArg::Named {
            name,
            arg,
            operator,
        } = argument
        else {
            return Err(sql_error_at(
                "CDF-SQL-UPSTREAM-ARGUMENT",
                file,
                &argument_span,
                "upstream arguments must use unquoted name => value form",
            ));
        };
        if name.quote_style.is_some() || operator != FunctionArgOperator::RightArrow {
            return Err(sql_error_at(
                "CDF-SQL-UPSTREAM-ARGUMENT",
                file,
                &argument_span,
                "upstream arguments must use unquoted name => value form",
            ));
        }
        let name_span = located_span(name.span, file, origin)?;
        validate_token_at("upstream argument", &name.value, file, &name_span)?;
        let FunctionArgExpr::Expr(expression) = arg else {
            return Err(sql_error_at(
                "CDF-SQL-UPSTREAM-VALUE",
                file,
                &argument_span,
                "upstream arguments must be recursive data-only values",
            ));
        };
        let expression_span = located_span(expression.span(), file, origin)?;
        if name.value == "source" {
            if configured_source.is_some() {
                return Err(sql_error_at(
                    "CDF-SQL-SOURCE-DUPLICATE",
                    file,
                    &name_span,
                    "upstream source must appear exactly once",
                ));
            }
            let Expr::Value(value) = expression else {
                return Err(sql_error_at(
                    "CDF-SQL-SOURCE-VALUE",
                    file,
                    &expression_span,
                    "upstream source must be a single-quoted configured-source name",
                ));
            };
            let Value::SingleQuotedString(source) = value.value else {
                return Err(sql_error_at(
                    "CDF-SQL-SOURCE-VALUE",
                    file,
                    &expression_span,
                    "upstream source must be a single-quoted configured-source name",
                ));
            };
            if source.starts_with("secret://") {
                return Err(sql_error_at(
                    "CDF-SQL-UPSTREAM-SECRET",
                    file,
                    &expression_span,
                    "the configured source name cannot be a secret reference; declare the source in cdf.toml",
                ));
            }
            validate_token_at("configured source", &source, file, &expression_span)?;
            configured_source = Some(source);
        } else {
            let value = lower_data_value(expression, file, origin)?;
            if resource_options.insert(name.value.clone(), value).is_some() {
                return Err(sql_error_at(
                    "CDF-SQL-UPSTREAM-DUPLICATE",
                    file,
                    &name_span,
                    format!("upstream argument {:?} appears more than once", name.value),
                ));
            }
        }
    }
    let configured_source = configured_source.ok_or_else(|| {
        sql_error_at(
            "CDF-SQL-SOURCE-MISSING",
            file,
            &relation_span,
            "upstream requires source => '<configured_source>' exactly once",
        )
    })?;
    let canonical_arguments_hash = cdf_runtime::artifact_hash(&resource_options)?;
    *name = ObjectName::from(Ident::new(UPSTREAM_TABLE));
    Ok(ParsedUpstreamRelation {
        configured_source,
        resource_options,
        canonical_arguments_hash,
        span: relation_span,
    })
}

fn lower_data_value(
    expression: Expr,
    file: &str,
    origin: QueryOrigin,
) -> Result<serde_json::Value> {
    let expression_span = located_span(expression.span(), file, origin)?;
    match expression {
        Expr::Value(value) => match value.value {
            Value::SingleQuotedString(value) if value.starts_with("secret://") => {
                Err(sql_error_at(
                    "CDF-SQL-UPSTREAM-SECRET",
                    file,
                    &expression_span,
                    "resource arguments cannot contain secret references; put credentials in the configured source",
                ))
            }
            Value::SingleQuotedString(value) => Ok(serde_json::Value::String(value)),
            Value::Number(value, _) => {
                parse_number(&value, file, &expression_span).map(serde_json::Value::Number)
            }
            Value::Boolean(value) => Ok(serde_json::Value::Bool(value)),
            Value::Null => Ok(serde_json::Value::Null),
            _ => Err(data_value_error(file, &expression_span)),
        },
        Expr::UnaryOp { op, expr } if matches!(op, UnaryOperator::Plus | UnaryOperator::Minus) => {
            let Expr::Value(value) = *expr else {
                return Err(data_value_error(file, &expression_span));
            };
            let Value::Number(value, _) = value.value else {
                return Err(data_value_error(file, &expression_span));
            };
            let signed = if op == UnaryOperator::Minus {
                format!("-{value}")
            } else {
                value
            };
            parse_number(&signed, file, &expression_span).map(serde_json::Value::Number)
        }
        Expr::Array(array) if array.named => array
            .elem
            .into_iter()
            .map(|value| lower_data_value(value, file, origin))
            .collect::<Result<Vec<_>>>()
            .map(serde_json::Value::Array),
        Expr::Function(function)
            if is_unquoted_name(&function.name, "OBJECT")
                && !function.uses_odbc_syntax
                && function.parameters == FunctionArguments::None
                && function.filter.is_none()
                && function.null_treatment.is_none()
                && function.over.is_none()
                && function.within_group.is_empty() =>
        {
            lower_object(function.args, file, origin, &expression_span)
        }
        _ => Err(data_value_error(file, &expression_span)),
    }
}

fn lower_object(
    arguments: FunctionArguments,
    file: &str,
    origin: QueryOrigin,
    object_span: &ProjectSqlSpan,
) -> Result<serde_json::Value> {
    let FunctionArguments::List(arguments) = arguments else {
        return Err(data_value_error(file, object_span));
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return Err(data_value_error(file, object_span));
    }
    let mut object = Map::new();
    for argument in arguments.args {
        let argument_span = located_span(argument.span(), file, origin)?;
        let FunctionArg::Named {
            name,
            arg,
            operator: FunctionArgOperator::RightArrow,
        } = argument
        else {
            return Err(data_value_error(file, &argument_span));
        };
        if name.quote_style.is_some() {
            return Err(data_value_error(file, &argument_span));
        }
        let name_span = located_span(name.span, file, origin)?;
        validate_token_at("OBJECT key", &name.value, file, &name_span)?;
        let FunctionArgExpr::Expr(expression) = arg else {
            return Err(data_value_error(file, &argument_span));
        };
        let value = lower_data_value(expression, file, origin)?;
        if object.insert(name.value.clone(), value).is_some() {
            return Err(sql_error_at(
                "CDF-SQL-OBJECT-DUPLICATE",
                file,
                &name_span,
                format!("OBJECT key {:?} appears more than once", name.value),
            ));
        }
    }
    Ok(serde_json::Value::Object(object))
}

fn admitted_relational_nodes<'a>(
    plan: &'a LogicalPlan,
    file: &str,
) -> Result<(
    &'a datafusion::logical_expr::Projection,
    Option<&'a LogicalExpr>,
)> {
    let LogicalPlan::Projection(projection) = plan else {
        return Err(relational_plan_error(plan, file));
    };
    let (filter, input) = match projection.input.as_ref() {
        LogicalPlan::Filter(filter) => (Some(&filter.predicate), filter.input.as_ref()),
        input => (None, input),
    };
    let input = match input {
        LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
        input => input,
    };
    let LogicalPlan::TableScan(scan) = input else {
        return Err(relational_plan_error(input, file));
    };
    if scan.table_name.table() != UPSTREAM_TABLE {
        return Err(sql_error(
            "CDF-SQL-INTERNAL-RELATION",
            file,
            "analyzed query did not bind the isolated upstream relation",
        ));
    }
    Ok((projection, filter))
}

fn unqualify_columns(expression: LogicalExpr) -> Result<LogicalExpr> {
    expression
        .transform_up(|mut expression| {
            if let LogicalExpr::Column(column) = &mut expression {
                column.relation = None;
            }
            Ok(Transformed::yes(expression))
        })
        .map(|transformed| transformed.data)
        .map_err(datafusion_error)
}

fn relational_plan_error(plan: &LogicalPlan, file: &str) -> CdfError {
    sql_error(
        "CDF-SQL-RELATIONAL-PLAN",
        file,
        format!(
            "DataFusion resolved a {} plan; only projection, optional filter, and one upstream scan are admitted",
            plan.display_indent()
        ),
    )
}

fn parse_number(value: &str, file: &str, span: &ProjectSqlSpan) -> Result<Number> {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| value.as_number().cloned())
        .ok_or_else(|| {
            sql_error_at(
                "CDF-SQL-NUMBER",
                file,
                span,
                format!("numeric resource argument {value:?} is outside the canonical JSON number domain"),
            )
        })
}

fn is_unquoted_name(name: &ObjectName, expected: &str) -> bool {
    matches!(
        name.0.as_slice(),
        [ObjectNamePart::Identifier(identifier)]
            if identifier.quote_style.is_none() && identifier.value == expected
    )
}

fn validate_token_at(kind: &str, value: &str, file: &str, span: &ProjectSqlSpan) -> Result<()> {
    let mut bytes = value.bytes();
    let starts_lowercase = bytes.next().is_some_and(|byte| byte.is_ascii_lowercase());
    if value.len() > 128
        || !starts_lowercase
        || bytes.any(|byte| !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
    {
        return Err(sql_error_at(
            "CDF-SQL-NAME",
            file,
            span,
            format!("{kind} {value:?} must match [a-z][a-z0-9_]{{0,127}}"),
        ));
    }
    Ok(())
}

fn sql_span(
    span: datafusion::sql::sqlparser::tokenizer::Span,
    file: &str,
) -> Result<ProjectSqlSpan> {
    Ok(ProjectSqlSpan {
        start_line: u32::try_from(span.start.line).map_err(|_| span_error(file))?,
        start_column: u32::try_from(span.start.column).map_err(|_| span_error(file))?,
        end_line: u32::try_from(span.end.line).map_err(|_| span_error(file))?,
        end_column: u32::try_from(span.end.column).map_err(|_| span_error(file))?,
    })
}

fn located_span(
    span: datafusion::sql::sqlparser::tokenizer::Span,
    file: &str,
    origin: QueryOrigin,
) -> Result<ProjectSqlSpan> {
    offset_span(
        &sql_span(span, file)?,
        origin.start_line,
        origin.start_column,
    )
}

fn span_error(file: &str) -> CdfError {
    sql_error(
        "CDF-SQL-SPAN",
        file,
        "SQL source location exceeds the supported one-based span domain",
    )
}

fn data_value_error(file: &str, span: &ProjectSqlSpan) -> CdfError {
    sql_error_at(
        "CDF-SQL-UPSTREAM-VALUE",
        file,
        span,
        "resource arguments admit only single-quoted strings, numbers, Boolean, NULL, ARRAY [...], and OBJECT(name => value, ...)",
    )
}

fn datafusion_error(error: datafusion::error::DataFusionError) -> CdfError {
    CdfError::contract(format!("[CDF-SQL-ANALYSIS] {error}"))
}

fn sql_error(code: &str, file: &str, message: impl std::fmt::Display) -> CdfError {
    CdfError::contract(format!("[{code}] {file}: {message}"))
}

fn sql_error_at(
    code: &str,
    file: &str,
    span: &ProjectSqlSpan,
    message: impl std::fmt::Display,
) -> CdfError {
    CdfError::contract(format!(
        "[{code}] {file}:{}:{}: {message}",
        span.start_line, span.start_column
    ))
}
