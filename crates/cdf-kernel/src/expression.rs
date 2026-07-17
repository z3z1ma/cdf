use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{CdfError, Result};

pub const EXPRESSION_IR_VERSION: u16 = 1;
pub const CDF_FUNCTION_NAMESPACE: &str = "cdf";
pub const CDF_FUNCTION_VERSION: &str = "1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Expression {
    pub version: u16,
    pub root: ExpressionNode,
}

impl Expression {
    pub fn new(root: ExpressionNode) -> Self {
        Self {
            version: EXPRESSION_IR_VERSION,
            root,
        }
    }

    pub fn column(name: impl Into<String>) -> Self {
        Self::new(ExpressionNode::Column { name: name.into() })
    }

    pub fn literal(value: ExpressionLiteral) -> Self {
        Self::new(ExpressionNode::Literal { value })
    }

    pub fn call(name: impl Into<String>, arguments: Vec<ExpressionNode>) -> Self {
        Self::new(ExpressionNode::Call {
            function: FunctionReference::cdf(name),
            arguments,
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != EXPRESSION_IR_VERSION {
            return Err(CdfError::contract(format!(
                "unsupported expression IR version {}; expected {EXPRESSION_IR_VERSION}",
                self.version
            )));
        }
        self.root.validate()
    }

    pub fn function_dependencies(&self) -> Vec<FunctionReference> {
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
                ExpressionNode::Column { name: column },
                ExpressionNode::Literal {
                    value: ExpressionLiteral::parse(literal)?,
                },
            ],
        ))
    }

    pub fn comparison(&self) -> Option<(&str, &str, &ExpressionLiteral)> {
        let ExpressionNode::Call {
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
            ExpressionNode::Column { name },
            ExpressionNode::Literal { value },
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
pub enum ExpressionNode {
    Column {
        name: String,
    },
    Literal {
        value: ExpressionLiteral,
    },
    Call {
        function: FunctionReference,
        arguments: Vec<ExpressionNode>,
    },
}

impl ExpressionNode {
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

    fn collect_functions(&self, output: &mut BTreeSet<FunctionReference>) {
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
pub struct FunctionReference {
    pub namespace: String,
    pub name: String,
    pub version: String,
}

impl FunctionReference {
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
pub enum ExpressionLiteral {
    Null,
    Boolean(bool),
    Signed(i64),
    Unsigned(u64),
    /// Exact IEEE-754 binary64 payload. Bit storage keeps the plan IR hashable and replay-stable.
    Float64Bits(u64),
    String(String),
    StringList(Vec<String>),
}

impl ExpressionLiteral {
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

#[cfg(test)]
mod tests {
    use super::{Expression, ExpressionLiteral, ExpressionNode};

    #[test]
    fn comparison_parser_ignores_operators_inside_literals_and_decodes_identifiers() {
        let expression = Expression::parse_comparison(r#""Order ID" = 'a>=b'"#).unwrap();
        assert_eq!(
            expression.root,
            ExpressionNode::Call {
                function: super::FunctionReference::cdf("eq"),
                arguments: vec![
                    ExpressionNode::Column {
                        name: "Order ID".to_owned(),
                    },
                    ExpressionNode::Literal {
                        value: ExpressionLiteral::String("a>=b".to_owned()),
                    },
                ],
            }
        );

        let float = Expression::parse_comparison("cursor >= -20260701.5").unwrap();
        let (_, _, literal) = float.comparison().unwrap();
        assert_eq!(literal.as_float64(), Some(-20260701.5));
        assert_eq!(float.comparison_operator(), Some(">="));
    }
}
