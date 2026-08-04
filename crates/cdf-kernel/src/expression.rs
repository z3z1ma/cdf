use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{CdfError, Result};

pub const DECLARATIVE_EXPRESSION_VERSION: u16 = 2;
pub const CDF_FUNCTION_NAMESPACE: &str = "cdf";
pub const CDF_FUNCTION_VERSION: &str = "2";
pub const SCALAR_EXPRESSION_IR_VERSION: u16 = 2;
pub const SCALAR_EXPRESSION_EXECUTOR_VERSION: u16 = 1;
pub const DATAFUSION_SCALAR_NAMESPACE: &str = "datafusion.builtin.scalar";
pub const DATAFUSION_SCALAR_IMPLEMENTATION_VERSION: &str = "54.0.0";
pub const DATAFUSION_SCALAR_FEATURE_SET: &str =
    "default-features=false;crypto,datetime,encoding,math,nested,regex,string,unicode";
pub const DATAFUSION_SCALAR_CONFIG_IDENTITY: &str = "config-options-default-v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DeclarativeExpression {
    pub version: u16,
    pub root: DeclarativeExpressionNode,
}

impl DeclarativeExpression {
    pub fn new(root: DeclarativeExpressionNode) -> Self {
        Self {
            version: DECLARATIVE_EXPRESSION_VERSION,
            root,
        }
    }

    pub fn column(name: impl Into<String>) -> Self {
        Self::new(DeclarativeExpressionNode::Column { name: name.into() })
    }

    pub fn literal(value: DeclarativeExpressionLiteral) -> Self {
        Self::new(DeclarativeExpressionNode::Literal { value })
    }

    pub fn call(name: impl Into<String>, arguments: Vec<DeclarativeExpressionNode>) -> Self {
        Self::new(DeclarativeExpressionNode::Call {
            function: DeclarativeFunctionReference::cdf(name),
            arguments,
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != DECLARATIVE_EXPRESSION_VERSION {
            return Err(CdfError::contract(format!(
                "unsupported expression IR version {}; expected {DECLARATIVE_EXPRESSION_VERSION}",
                self.version
            )));
        }
        self.root.validate()
    }

    pub fn function_dependencies(&self) -> Vec<DeclarativeFunctionReference> {
        let mut functions = BTreeSet::new();
        self.root.collect_functions(&mut functions);
        functions.into_iter().collect()
    }

    pub fn column_dependencies(&self) -> Vec<String> {
        let mut columns = BTreeSet::new();
        self.root.collect_columns(&mut columns);
        columns.into_iter().collect()
    }

    pub fn parse_comparison(input: &str) -> Result<Self> {
        let (index, token, function) = comparison_operator(input)?;
        let column = input[..index].trim();
        let literal = input[index + token.len()..].trim();
        if column.is_empty() || literal.is_empty() {
            return Err(unsupported(input));
        }
        let column = parse_column(column).ok_or_else(|| unsupported(input))?;
        Ok(Self::call(
            function,
            vec![
                DeclarativeExpressionNode::Column { name: column },
                DeclarativeExpressionNode::Literal {
                    value: DeclarativeExpressionLiteral::parse(literal)?,
                },
            ],
        ))
    }

    pub fn comparison(&self) -> Option<(&str, &str, &DeclarativeExpressionLiteral)> {
        let DeclarativeExpressionNode::Call {
            function,
            arguments,
        } = &self.root
        else {
            return None;
        };
        if !function.is_current_cdf() {
            return None;
        }
        let [
            DeclarativeExpressionNode::Column { name },
            DeclarativeExpressionNode::Literal { value },
        ] = arguments.as_slice()
        else {
            return None;
        };
        Some((name, function.name.as_str(), value))
    }

    pub fn comparison_operator(&self) -> Option<&'static str> {
        let (_, function, _) = self.comparison()?;
        match function {
            "eq" => Some("="),
            "neq" => Some("!="),
            "gt" => Some(">"),
            "gte" => Some(">="),
            "lt" => Some("<"),
            "lte" => Some("<="),
            _ => None,
        }
    }
}

fn comparison_operator(input: &str) -> Result<(usize, &'static str, &'static str)> {
    let bytes = input.as_bytes();
    let mut quote = None;
    let mut found = None;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(active) = quote {
            if byte == active {
                if bytes.get(index + 1) == Some(&active) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if matches!(byte, b'\'' | b'"') {
            quote = Some(byte);
            index += 1;
            continue;
        }
        let matched = [
            (">=", "gte"),
            ("<=", "lte"),
            ("!=", "neq"),
            ("=", "eq"),
            (">", "gt"),
            ("<", "lt"),
        ]
        .into_iter()
        .find(|(token, _)| bytes[index..].starts_with(token.as_bytes()));
        if let Some((token, function)) = matched {
            if found.is_some() {
                return Err(unsupported(input));
            }
            found = Some((index, token, function));
            index += token.len();
        } else {
            index += 1;
        }
    }
    if quote.is_some() {
        return Err(unsupported(input));
    }
    found.ok_or_else(|| unsupported(input))
}

fn parse_column(input: &str) -> Option<String> {
    if let Some(inner) = input
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    {
        if inner.is_empty() {
            return None;
        }
        return Some(inner.replace("\"\"", "\""));
    }
    input
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
        .then(|| input.to_owned())
}

fn unsupported(input: &str) -> CdfError {
    CdfError::contract(format!(
        "unsupported declarative expression {input:?}; expected '<column> <op> <literal>'"
    ))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeclarativeExpressionNode {
    Column {
        name: String,
    },
    Literal {
        value: DeclarativeExpressionLiteral,
    },
    Call {
        function: DeclarativeFunctionReference,
        arguments: Vec<DeclarativeExpressionNode>,
    },
}

impl DeclarativeExpressionNode {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Column { name } if name.trim().is_empty() => {
                Err(CdfError::contract("expression column name cannot be empty"))
            }
            Self::Column { .. } | Self::Literal { .. } => Ok(()),
            Self::Call {
                function,
                arguments,
            } => {
                function.validate()?;
                if arguments.is_empty() {
                    return Err(CdfError::contract(format!(
                        "expression function {:?} requires at least one argument",
                        function.name
                    )));
                }
                arguments.iter().try_for_each(Self::validate)
            }
        }
    }

    fn collect_functions(&self, output: &mut BTreeSet<DeclarativeFunctionReference>) {
        if let Self::Call {
            function,
            arguments,
        } = self
        {
            output.insert(function.clone());
            for argument in arguments {
                argument.collect_functions(output);
            }
        }
    }

    fn collect_columns(&self, output: &mut BTreeSet<String>) {
        match self {
            Self::Column { name } => {
                output.insert(name.clone());
            }
            Self::Literal { .. } => {}
            Self::Call { arguments, .. } => {
                for argument in arguments {
                    argument.collect_columns(output);
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DeclarativeFunctionReference {
    pub namespace: String,
    pub name: String,
    pub version: String,
}

impl DeclarativeFunctionReference {
    pub fn cdf(name: impl Into<String>) -> Self {
        Self {
            namespace: CDF_FUNCTION_NAMESPACE.to_owned(),
            name: name.into(),
            version: CDF_FUNCTION_VERSION.to_owned(),
        }
    }

    pub fn is_current_cdf(&self) -> bool {
        self.namespace == CDF_FUNCTION_NAMESPACE && self.version == CDF_FUNCTION_VERSION
    }

    fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty()
            || self.name.trim().is_empty()
            || self.version.trim().is_empty()
        {
            return Err(CdfError::contract(
                "expression function namespace, name, and version are required",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeclarativeExpressionLiteral {
    Null,
    Boolean(bool),
    Signed(i64),
    Unsigned(u64),
    /// Exact IEEE-754 binary64 payload. Bit storage keeps the plan IR hashable and replay-stable.
    Float64Bits(u64),
    String(String),
    StringList(Vec<String>),
}

impl DeclarativeExpressionLiteral {
    pub fn finite_float64(value: f64) -> Result<Self> {
        if !value.is_finite() {
            return Err(CdfError::contract(
                "expression float literals must be finite",
            ));
        }
        Ok(Self::Float64Bits(value.to_bits()))
    }

    pub fn as_float64(&self) -> Option<f64> {
        match self {
            Self::Float64Bits(bits) => Some(f64::from_bits(*bits)),
            _ => None,
        }
    }

    fn parse(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.eq_ignore_ascii_case("null") {
            return Ok(Self::Null);
        }
        if input.eq_ignore_ascii_case("true") {
            return Ok(Self::Boolean(true));
        }
        if input.eq_ignore_ascii_case("false") {
            return Ok(Self::Boolean(false));
        }
        if let Ok(value) = input.parse::<i64>() {
            return Ok(Self::Signed(value));
        }
        if let Ok(value) = input.parse::<u64>() {
            return Ok(Self::Unsigned(value));
        }
        if let Ok(value) = input.parse::<f64>() {
            return Self::finite_float64(value);
        }
        for quote in ['\'', '"'] {
            if let Some(inner) = input
                .strip_prefix(quote)
                .and_then(|value| value.strip_suffix(quote))
            {
                let doubled = format!("{quote}{quote}");
                return Ok(Self::String(inner.replace(&doubled, &quote.to_string())));
            }
        }
        Err(unsupported(input))
    }
}

/// Durable, fully typed scalar authority produced after DataFusion analysis.
///
/// The declaration AST above is intentionally a separate type: it is source/configuration input,
/// while this graph is the only expression representation eligible for identity execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalarExpression {
    pub version: u16,
    pub executor_version: u16,
    pub datafusion_version: String,
    pub datafusion_feature_set: String,
    pub config_identity: String,
    pub root: ScalarExpressionNode,
    pub content_sha256: String,
}

impl ScalarExpression {
    pub fn current(root: ScalarExpressionNode) -> Result<Self> {
        let mut expression = Self {
            version: SCALAR_EXPRESSION_IR_VERSION,
            executor_version: SCALAR_EXPRESSION_EXECUTOR_VERSION,
            datafusion_version: DATAFUSION_SCALAR_IMPLEMENTATION_VERSION.to_owned(),
            datafusion_feature_set: DATAFUSION_SCALAR_FEATURE_SET.to_owned(),
            config_identity: DATAFUSION_SCALAR_CONFIG_IDENTITY.to_owned(),
            root,
            content_sha256: String::new(),
        };
        expression.content_sha256 = expression.compute_content_sha256()?;
        expression.validate()?;
        Ok(expression)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != SCALAR_EXPRESSION_IR_VERSION
            || self.executor_version != SCALAR_EXPRESSION_EXECUTOR_VERSION
            || self.datafusion_version != DATAFUSION_SCALAR_IMPLEMENTATION_VERSION
            || self.datafusion_feature_set != DATAFUSION_SCALAR_FEATURE_SET
            || self.config_identity != DATAFUSION_SCALAR_CONFIG_IDENTITY
        {
            return Err(CdfError::contract(format!(
                "stale scalar expression identity {}/{}; run `cdf compile` to produce {SCALAR_EXPRESSION_IR_VERSION}/{SCALAR_EXPRESSION_EXECUTOR_VERSION}",
                self.version, self.executor_version
            )));
        }
        self.root.validate()?;
        if self.content_sha256 != self.compute_content_sha256()? {
            return Err(CdfError::contract(
                "scalar expression content digest does not match its canonical payload",
            ));
        }
        Ok(())
    }

    pub fn column_dependencies(&self) -> &[ScalarColumnDependency] {
        &self.root.dependencies.columns
    }

    pub fn function_dependencies(&self) -> &[ScalarFunctionReference] {
        &self.root.dependencies.functions
    }

    fn compute_content_sha256(&self) -> Result<String> {
        let bytes = serde_json::to_vec(&(
            self.version,
            self.executor_version,
            &self.datafusion_version,
            &self.datafusion_feature_set,
            &self.config_identity,
            &self.root,
        ))
        .map_err(|error| CdfError::internal(format!("serialize scalar identity: {error}")))?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalarType {
    pub data_type: crate::CanonicalArrowType,
    pub nullable: bool,
}

impl ScalarType {
    pub fn from_arrow(data_type: &arrow_schema::DataType, nullable: bool) -> Result<Self> {
        Ok(Self {
            data_type: crate::CanonicalArrowType::from_arrow(data_type)?,
            nullable,
        })
    }

    pub fn to_arrow(&self) -> Result<arrow_schema::DataType> {
        self.data_type.to_arrow()
    }

    fn validate(&self) -> Result<()> {
        self.to_arrow().map(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalarColumnDependency {
    pub name: String,
    pub index: usize,
    pub scalar_type: ScalarType,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ScalarDependencies {
    pub columns: Vec<ScalarColumnDependency>,
    pub functions: Vec<ScalarFunctionReference>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalarFunctionReference {
    pub namespace: String,
    pub canonical_name: String,
    pub implementation_version: String,
    pub feature_set: String,
    pub config_identity: String,
    pub volatility: ScalarFunctionVolatility,
    pub argument_types: Vec<ScalarType>,
    pub return_type: ScalarType,
}

impl ScalarFunctionReference {
    fn validate(&self) -> Result<()> {
        if self.namespace != DATAFUSION_SCALAR_NAMESPACE
            || self.implementation_version != DATAFUSION_SCALAR_IMPLEMENTATION_VERSION
            || self.feature_set != DATAFUSION_SCALAR_FEATURE_SET
            || self.config_identity != DATAFUSION_SCALAR_CONFIG_IDENTITY
            || self.volatility != ScalarFunctionVolatility::Immutable
            || self.canonical_name.trim().is_empty()
        {
            return Err(CdfError::contract(format!(
                "stale or noncanonical scalar function binding {:?}; run `cdf compile`",
                self.canonical_name
            )));
        }
        self.argument_types
            .iter()
            .try_for_each(ScalarType::validate)?;
        self.return_type.validate()
    }

    fn canonical_key(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|error| {
            CdfError::internal(format!("serialize scalar function identity: {error}"))
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarFunctionVolatility {
    Immutable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalarExpressionNode {
    pub scalar_type: ScalarType,
    pub dependencies: ScalarDependencies,
    pub expression: ScalarExpressionKind,
}

impl ScalarExpressionNode {
    pub fn column(name: impl Into<String>, index: usize, scalar_type: ScalarType) -> Self {
        let name = name.into();
        let dependency = ScalarColumnDependency {
            name: name.clone(),
            index,
            scalar_type: scalar_type.clone(),
        };
        Self {
            scalar_type,
            dependencies: ScalarDependencies {
                columns: vec![dependency],
                functions: Vec::new(),
            },
            expression: ScalarExpressionKind::Column { name, index },
        }
    }

    pub fn literal(scalar_type: ScalarType, arrow_ipc: Vec<u8>) -> Self {
        Self {
            scalar_type,
            dependencies: ScalarDependencies::default(),
            expression: ScalarExpressionKind::Literal { arrow_ipc },
        }
    }

    pub fn unary(
        operator: ScalarUnaryOperator,
        argument: Self,
        scalar_type: ScalarType,
    ) -> Result<Self> {
        let dependencies = dependencies_from_nodes(std::slice::from_ref(&argument), None)?;
        Ok(Self {
            scalar_type,
            dependencies,
            expression: ScalarExpressionKind::Unary {
                operator,
                argument: Box::new(argument),
            },
        })
    }

    pub fn binary(
        operator: ScalarBinaryOperator,
        left: Self,
        right: Self,
        scalar_type: ScalarType,
    ) -> Result<Self> {
        let dependencies = dependencies_from_nodes(&[left.clone(), right.clone()], None)?;
        Ok(Self {
            scalar_type,
            dependencies,
            expression: ScalarExpressionKind::Binary {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            },
        })
    }

    pub fn call(
        function: ScalarFunctionReference,
        arguments: Vec<Self>,
        scalar_type: ScalarType,
    ) -> Result<Self> {
        let dependencies = dependencies_from_nodes(&arguments, Some(&function))?;
        Ok(Self {
            scalar_type,
            dependencies,
            expression: ScalarExpressionKind::Call {
                function,
                arguments,
            },
        })
    }

    pub fn cast(mode: ScalarCastMode, argument: Self, target_type: ScalarType) -> Result<Self> {
        let source_type = argument.scalar_type.clone();
        let dependencies = dependencies_from_nodes(std::slice::from_ref(&argument), None)?;
        Ok(Self {
            scalar_type: target_type.clone(),
            dependencies,
            expression: ScalarExpressionKind::Cast {
                mode,
                source_type,
                target_type,
                argument: Box::new(argument),
            },
        })
    }

    pub fn validate(&self) -> Result<()> {
        self.scalar_type.validate()?;
        let expected = match &self.expression {
            ScalarExpressionKind::Column { name, index } => {
                if name.trim().is_empty() {
                    return Err(CdfError::contract("scalar column name cannot be empty"));
                }
                ScalarDependencies {
                    columns: vec![ScalarColumnDependency {
                        name: name.clone(),
                        index: *index,
                        scalar_type: self.scalar_type.clone(),
                    }],
                    functions: Vec::new(),
                }
            }
            ScalarExpressionKind::Literal { arrow_ipc } => {
                if arrow_ipc.is_empty() {
                    return Err(CdfError::contract(
                        "scalar literal Arrow IPC payload cannot be empty",
                    ));
                }
                ScalarDependencies::default()
            }
            ScalarExpressionKind::Unary { operator, argument } => {
                argument.validate()?;
                if matches!(
                    operator,
                    ScalarUnaryOperator::Not
                        | ScalarUnaryOperator::IsNull
                        | ScalarUnaryOperator::IsNotNull
                ) && self.scalar_type.data_type != crate::CanonicalArrowType::Boolean
                {
                    return Err(CdfError::contract(
                        "Boolean unary operator has a non-Boolean recorded result",
                    ));
                }
                dependencies_from_nodes(std::slice::from_ref(argument.as_ref()), None)?
            }
            ScalarExpressionKind::Binary { left, right, .. } => {
                left.validate()?;
                right.validate()?;
                dependencies_from_nodes(&[left.as_ref().clone(), right.as_ref().clone()], None)?
            }
            ScalarExpressionKind::Call {
                function,
                arguments,
            } => {
                function.validate()?;
                arguments.iter().try_for_each(Self::validate)?;
                let argument_types = arguments
                    .iter()
                    .map(|argument| argument.scalar_type.clone())
                    .collect::<Vec<_>>();
                if function.argument_types != argument_types
                    || function.return_type != self.scalar_type
                {
                    return Err(CdfError::contract(format!(
                        "scalar function {:?} signature does not match its typed node",
                        function.canonical_name
                    )));
                }
                dependencies_from_nodes(arguments, Some(function))?
            }
            ScalarExpressionKind::Cast {
                source_type,
                target_type,
                argument,
                ..
            } => {
                argument.validate()?;
                if source_type != &argument.scalar_type || target_type != &self.scalar_type {
                    return Err(CdfError::contract(
                        "scalar cast source/target identity does not match its typed node",
                    ));
                }
                dependencies_from_nodes(std::slice::from_ref(argument.as_ref()), None)?
            }
        };
        if self.dependencies != expected {
            return Err(CdfError::contract(
                "scalar node dependency identity does not match its expression graph",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScalarExpressionKind {
    Column {
        name: String,
        index: usize,
    },
    Literal {
        arrow_ipc: Vec<u8>,
    },
    Unary {
        operator: ScalarUnaryOperator,
        argument: Box<ScalarExpressionNode>,
    },
    Binary {
        operator: ScalarBinaryOperator,
        left: Box<ScalarExpressionNode>,
        right: Box<ScalarExpressionNode>,
    },
    Call {
        function: ScalarFunctionReference,
        arguments: Vec<ScalarExpressionNode>,
    },
    Cast {
        mode: ScalarCastMode,
        source_type: ScalarType,
        target_type: ScalarType,
        argument: Box<ScalarExpressionNode>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarCastMode {
    Implicit,
    Explicit,
    Try,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarUnaryOperator {
    Not,
    Negative,
    IsNull,
    IsNotNull,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarBinaryOperator {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    And,
    Or,
    IsDistinctFrom,
    IsNotDistinctFrom,
    RegexMatch,
    RegexInsensitiveMatch,
    RegexNotMatch,
    RegexNotInsensitiveMatch,
    Like,
    InsensitiveLike,
    NotLike,
    NotInsensitiveLike,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseShiftRight,
    BitwiseShiftLeft,
    StringConcat,
    ListContains,
    ListContainedBy,
}

fn dependencies_from_nodes(
    nodes: &[ScalarExpressionNode],
    function: Option<&ScalarFunctionReference>,
) -> Result<ScalarDependencies> {
    let mut columns = BTreeMap::<(usize, String), ScalarColumnDependency>::new();
    let mut functions = BTreeMap::<Vec<u8>, ScalarFunctionReference>::new();
    for node in nodes {
        for column in &node.dependencies.columns {
            let key = (column.index, column.name.clone());
            if let Some(existing) = columns.insert(key, column.clone())
                && existing.scalar_type != column.scalar_type
            {
                return Err(CdfError::contract(format!(
                    "scalar column dependency {:?} has inconsistent types",
                    column.name
                )));
            }
        }
        for function in &node.dependencies.functions {
            functions.insert(function.canonical_key()?, function.clone());
        }
    }
    if let Some(function) = function {
        functions.insert(function.canonical_key()?, function.clone());
    }
    Ok(ScalarDependencies {
        columns: columns.into_values().collect(),
        functions: functions.into_values().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::{DeclarativeExpression, DeclarativeExpressionLiteral, DeclarativeExpressionNode};

    #[test]
    fn comparison_parser_ignores_operators_inside_literals_and_decodes_identifiers() {
        let expression = DeclarativeExpression::parse_comparison(r#""Order ID" = 'a>=b'"#).unwrap();
        assert_eq!(
            expression.root,
            DeclarativeExpressionNode::Call {
                function: super::DeclarativeFunctionReference::cdf("eq"),
                arguments: vec![
                    DeclarativeExpressionNode::Column {
                        name: "Order ID".to_owned(),
                    },
                    DeclarativeExpressionNode::Literal {
                        value: DeclarativeExpressionLiteral::String("a>=b".to_owned()),
                    },
                ],
            }
        );

        let float = DeclarativeExpression::parse_comparison("cursor >= -20260701.5").unwrap();
        let (_, _, literal) = float.comparison().unwrap();
        assert_eq!(literal.as_float64(), Some(-20260701.5));
        assert_eq!(float.comparison_operator(), Some(">="));
    }

    #[test]
    fn stale_declarative_expression_v1_deserializes_only_to_fail_current_validation() {
        let stale = DeclarativeExpression {
            version: 1,
            root: DeclarativeExpressionNode::Column {
                name: "id".to_owned(),
            },
        };
        let bytes = serde_json::to_vec(&stale).unwrap();
        let loaded: DeclarativeExpression = serde_json::from_slice(&bytes).unwrap();
        let error = loaded.validate().unwrap_err();
        assert!(
            error
                .message
                .contains("unsupported expression IR version 1")
        );
        assert!(error.message.contains("expected 2"));
    }
}
