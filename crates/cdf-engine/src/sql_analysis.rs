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
    let (_, upstream, normalized_query) = parse_and_rewrite(sql, file)?;
    let upstream = offset_upstream(upstream, start_line, start_column)?;
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
    let (query, upstream, normalized_query) = parse_and_rewrite(sql, file)?;
    let upstream = offset_upstream(upstream, start_line, start_column)?;
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
            "CDF-D3-SQL-ANALYSIS",
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

fn offset_upstream(
    mut upstream: ParsedUpstreamRelation,
    start_line: u32,
    start_column: u32,
) -> Result<ParsedUpstreamRelation> {
    if start_line == 0 || start_column == 0 {
        return Err(CdfError::internal(
            "project query offset must use one-based line and column",
        ));
    }
    upstream.span = offset_span(&upstream.span, start_line, start_column)?;
    Ok(upstream)
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

fn parse_and_rewrite(sql: &str, file: &str) -> Result<(Query, ParsedUpstreamRelation, String)> {
    if sql.trim().is_empty() {
        return Err(sql_error(
            "CDF-D3-SQL-EMPTY",
            file,
            "resource file must contain one SELECT query",
        ));
    }
    let mut statements = DFParser::parse_sql(sql).map_err(|error| {
        sql_error(
            "CDF-D3-SQL-PARSE",
            file,
            format!("could not parse resource SQL: {error}"),
        )
    })?;
    if statements.len() != 1 {
        return Err(sql_error(
            "CDF-D3-SQL-STATEMENT-COUNT",
            file,
            "resource file must contain exactly one statement",
        ));
    }
    let statement = statements.pop_front().ok_or_else(|| {
        sql_error(
            "CDF-D3-SQL-STATEMENT-COUNT",
            file,
            "resource file must contain exactly one statement",
        )
    })?;
    let mut query = match statement {
        DataFusionStatement::Statement(statement) => match *statement {
            Statement::Query(query) => *query,
            Statement::CreateTable(_) => {
                return Err(sql_error(
                    "CDF-D3-SQL-CREATE-RESOURCE",
                    file,
                    "CREATE RESOURCE is not current syntax; author a bare SELECT or RESOURCE ... AS SELECT",
                ));
            }
            _ => {
                return Err(sql_error(
                    "CDF-D3-SQL-STATEMENT",
                    file,
                    "resource file must contain a SELECT query",
                ));
            }
        },
        _ => {
            return Err(sql_error(
                "CDF-D3-SQL-STATEMENT",
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
                "CDF-D3-SQL-SET-OPERATION",
                file,
                "set operations, VALUES, and parenthesized query bodies are not admitted",
            ));
        }
    };
    if select.from.len() != 1 {
        return Err(sql_error(
            "CDF-D3-SQL-UPSTREAM-COUNT",
            file,
            "resource query requires exactly one FROM upstream(...) relation",
        ));
    }
    let from = select.from.get_mut(0).ok_or_else(|| {
        sql_error(
            "CDF-D3-SQL-UPSTREAM-COUNT",
            file,
            "resource query requires exactly one FROM upstream(...) relation",
        )
    })?;
    if !from.joins.is_empty() {
        return Err(sql_error(
            "CDF-D3-SQL-JOIN",
            file,
            "joins and multiple upstream relations are not admitted",
        ));
    }
    let upstream = parse_upstream_relation(&mut from.relation, file)?;
    Ok((query, upstream, normalized_query))
}

fn validate_query_shape(query: &Query, file: &str) -> Result<()> {
    if query.with.is_some() {
        return Err(sql_error(
            "CDF-D3-SQL-WITH",
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
            "CDF-D3-SQL-QUERY-CLAUSE",
            file,
            "ORDER/LIMIT/FETCH/LOCK/SETTINGS/FORMAT/pipe clauses are not admitted",
        ));
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(sql_error(
            "CDF-D3-SQL-SET-OPERATION",
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
            "CDF-D3-SQL-RELATIONAL-SHAPE",
            file,
            "only projection, scalar expressions, one upstream relation, and an optional WHERE filter are admitted",
        ));
    }
    Ok(())
}

fn parse_upstream_relation(
    relation: &mut TableFactor,
    file: &str,
) -> Result<ParsedUpstreamRelation> {
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
        return Err(sql_error(
            "CDF-D3-SQL-UPSTREAM-RELATION",
            file,
            "FROM must contain exactly upstream(source => 'name', ...)",
        ));
    };
    if !is_unquoted_name(name, "upstream") || args.is_none() {
        return Err(sql_error(
            "CDF-D3-SQL-UPSTREAM-RELATION",
            file,
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
        return Err(sql_error(
            "CDF-D3-SQL-UPSTREAM-MODIFIER",
            file,
            "upstream(...) does not admit table hints, versions, partitions, ordinality, paths, samples, or index hints",
        ));
    }
    let relation_span = sql_span(name.span(), file)?;
    let table_args = args.take().ok_or_else(|| {
        sql_error(
            "CDF-D3-SQL-UPSTREAM-RELATION",
            file,
            "upstream requires named arguments",
        )
    })?;
    if table_args.settings.is_some() {
        return Err(sql_error(
            "CDF-D3-SQL-UPSTREAM-MODIFIER",
            file,
            "upstream arguments do not admit SETTINGS",
        ));
    }
    let mut configured_source = None;
    let mut resource_options = BTreeMap::new();
    for argument in table_args.args {
        let FunctionArg::Named {
            name,
            arg,
            operator,
        } = argument
        else {
            return Err(sql_error(
                "CDF-D3-SQL-UPSTREAM-ARGUMENT",
                file,
                "upstream arguments must use unquoted name => value form",
            ));
        };
        if name.quote_style.is_some() || operator != FunctionArgOperator::RightArrow {
            return Err(sql_error(
                "CDF-D3-SQL-UPSTREAM-ARGUMENT",
                file,
                "upstream arguments must use unquoted name => value form",
            ));
        }
        validate_token("upstream argument", &name.value, file)?;
        let FunctionArgExpr::Expr(expression) = arg else {
            return Err(sql_error(
                "CDF-D3-SQL-UPSTREAM-VALUE",
                file,
                "upstream arguments must be recursive data-only values",
            ));
        };
        if name.value == "source" {
            if configured_source.is_some() {
                return Err(sql_error(
                    "CDF-D3-SQL-SOURCE-DUPLICATE",
                    file,
                    "upstream source must appear exactly once",
                ));
            }
            let Expr::Value(value) = expression else {
                return Err(sql_error(
                    "CDF-D3-SQL-SOURCE-VALUE",
                    file,
                    "upstream source must be a single-quoted configured-source name",
                ));
            };
            let Value::SingleQuotedString(source) = value.value else {
                return Err(sql_error(
                    "CDF-D3-SQL-SOURCE-VALUE",
                    file,
                    "upstream source must be a single-quoted configured-source name",
                ));
            };
            validate_token("configured source", &source, file)?;
            configured_source = Some(source);
        } else {
            let value = lower_data_value(expression, file)?;
            if resource_options.insert(name.value.clone(), value).is_some() {
                return Err(sql_error(
                    "CDF-D3-SQL-UPSTREAM-DUPLICATE",
                    file,
                    format!("upstream argument {:?} appears more than once", name.value),
                ));
            }
        }
    }
    let configured_source = configured_source.ok_or_else(|| {
        sql_error(
            "CDF-D3-SQL-SOURCE-MISSING",
            file,
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

fn lower_data_value(expression: Expr, file: &str) -> Result<serde_json::Value> {
    match expression {
        Expr::Value(value) => match value.value {
            Value::SingleQuotedString(value) => Ok(serde_json::Value::String(value)),
            Value::Number(value, _) => parse_number(&value, file).map(serde_json::Value::Number),
            Value::Boolean(value) => Ok(serde_json::Value::Bool(value)),
            Value::Null => Ok(serde_json::Value::Null),
            _ => Err(data_value_error(file)),
        },
        Expr::UnaryOp { op, expr } if matches!(op, UnaryOperator::Plus | UnaryOperator::Minus) => {
            let Expr::Value(value) = *expr else {
                return Err(data_value_error(file));
            };
            let Value::Number(value, _) = value.value else {
                return Err(data_value_error(file));
            };
            let signed = if op == UnaryOperator::Minus {
                format!("-{value}")
            } else {
                value
            };
            parse_number(&signed, file).map(serde_json::Value::Number)
        }
        Expr::Array(array) if array.named => array
            .elem
            .into_iter()
            .map(|value| lower_data_value(value, file))
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
            lower_object(function.args, file)
        }
        _ => Err(data_value_error(file)),
    }
}

fn lower_object(arguments: FunctionArguments, file: &str) -> Result<serde_json::Value> {
    let FunctionArguments::List(arguments) = arguments else {
        return Err(data_value_error(file));
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return Err(data_value_error(file));
    }
    let mut object = Map::new();
    for argument in arguments.args {
        let FunctionArg::Named {
            name,
            arg,
            operator: FunctionArgOperator::RightArrow,
        } = argument
        else {
            return Err(data_value_error(file));
        };
        if name.quote_style.is_some() {
            return Err(data_value_error(file));
        }
        validate_token("OBJECT key", &name.value, file)?;
        let FunctionArgExpr::Expr(expression) = arg else {
            return Err(data_value_error(file));
        };
        let value = lower_data_value(expression, file)?;
        if object.insert(name.value.clone(), value).is_some() {
            return Err(sql_error(
                "CDF-D3-SQL-OBJECT-DUPLICATE",
                file,
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
            "CDF-D3-SQL-INTERNAL-RELATION",
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
        "CDF-D3-SQL-RELATIONAL-PLAN",
        file,
        format!(
            "DataFusion resolved a {} plan; only projection, optional filter, and one upstream scan are admitted",
            plan.display_indent()
        ),
    )
}

fn parse_number(value: &str, file: &str) -> Result<Number> {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| value.as_number().cloned())
        .ok_or_else(|| {
            sql_error(
                "CDF-D3-SQL-NUMBER",
                file,
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

fn validate_token(kind: &str, value: &str, file: &str) -> Result<()> {
    let mut bytes = value.bytes();
    let starts_lowercase = bytes.next().is_some_and(|byte| byte.is_ascii_lowercase());
    if value.len() > 128
        || !starts_lowercase
        || bytes.any(|byte| !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
    {
        return Err(sql_error(
            "CDF-D3-SQL-NAME",
            file,
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

fn span_error(file: &str) -> CdfError {
    sql_error(
        "CDF-D3-SQL-SPAN",
        file,
        "SQL source location exceeds the supported one-based span domain",
    )
}

fn data_value_error(file: &str) -> CdfError {
    sql_error(
        "CDF-D3-SQL-UPSTREAM-VALUE",
        file,
        "resource arguments admit only single-quoted strings, numbers, Boolean, NULL, ARRAY [...], and OBJECT(name => value, ...)",
    )
}

fn datafusion_error(error: datafusion::error::DataFusionError) -> CdfError {
    CdfError::contract(format!("[CDF-D3-SQL-ANALYSIS] {error}"))
}

fn sql_error(code: &str, file: &str, message: impl std::fmt::Display) -> CdfError {
    CdfError::contract(format!("[{code}] {file}: {message}"))
}
