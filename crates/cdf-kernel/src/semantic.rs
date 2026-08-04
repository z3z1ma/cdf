use std::{collections::BTreeMap, error::Error, fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::Value;

const MAX_REFERENCE_BYTES: usize = 1_024;
const MAX_IDENTIFIER_BYTES: usize = 64;
const MAX_PARAMETERS: usize = 32;
const MAX_STRING_PARAMETER_BYTES: usize = 256;

pub const CDF_PACKAGE_ROW_ORDINAL_SEMANTIC: &str = "cdf.package_row_ordinal@1";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SemanticReference {
    namespace: String,
    name: String,
    version: u32,
    parameters: BTreeMap<String, SemanticParameterValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum SemanticParameterValue {
    String(String),
    Number(String),
    Boolean(bool),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SemanticReferenceError {
    detail: String,
}

impl SemanticReference {
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        version: u32,
        parameters: BTreeMap<String, SemanticParameterValue>,
    ) -> std::result::Result<Self, SemanticReferenceError> {
        let reference = Self {
            namespace: namespace.into(),
            name: name.into(),
            version,
            parameters,
        };
        reference.validate()?;
        Ok(reference)
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn parameters(&self) -> &BTreeMap<String, SemanticParameterValue> {
        &self.parameters
    }

    pub fn parameter(&self, name: &str) -> Option<&SemanticParameterValue> {
        self.parameters.get(name)
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }

    fn validate(&self) -> std::result::Result<(), SemanticReferenceError> {
        validate_identifier("namespace", &self.namespace)?;
        validate_identifier("name", &self.name)?;
        if self.version == 0 {
            return Err(reference_error("definition version must be positive"));
        }
        if self.parameters.len() > MAX_PARAMETERS {
            return Err(reference_error(format!(
                "parameter count exceeds {MAX_PARAMETERS}"
            )));
        }
        for (key, value) in &self.parameters {
            validate_identifier("parameter", key)?;
            value.validate()?;
        }
        let rendered = self.to_string();
        if rendered.len() > MAX_REFERENCE_BYTES {
            return Err(reference_error(format!(
                "canonical reference exceeds {MAX_REFERENCE_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

impl SemanticParameterValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            Self::Number(_) | Self::Boolean(_) => None,
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::Number(_) => "number",
            Self::Boolean(_) => "boolean",
        }
    }

    pub fn validate(&self) -> std::result::Result<(), SemanticReferenceError> {
        match self {
            Self::String(value) if value.len() > MAX_STRING_PARAMETER_BYTES => {
                Err(reference_error(format!(
                    "string parameter exceeds {MAX_STRING_PARAMETER_BYTES} bytes"
                )))
            }
            Self::Number(value) => {
                let parsed = serde_json::from_str::<Value>(value).map_err(|error| {
                    reference_error(format!("invalid canonical JSON number: {error}"))
                })?;
                let canonical = parsed.to_string();
                if !parsed.is_number() || canonical != *value {
                    return Err(reference_error("number parameter is not canonical JSON"));
                }
                Ok(())
            }
            Self::String(_) | Self::Boolean(_) => Ok(()),
        }
    }

    fn render(&self) -> String {
        match self {
            Self::String(value) => serde_json::to_string(value)
                .unwrap_or_else(|_| unreachable!("serializing a Rust string cannot fail")),
            Self::Number(value) => value.clone(),
            Self::Boolean(value) => value.to_string(),
        }
    }
}

impl fmt::Display for SemanticReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}.{}@{}",
            self.namespace, self.name, self.version
        )?;
        if !self.parameters.is_empty() {
            formatter.write_str("(")?;
            for (index, (key, value)) in self.parameters.iter().enumerate() {
                if index > 0 {
                    formatter.write_str(",")?;
                }
                write!(formatter, "{key}={}", value.render())?;
            }
            formatter.write_str(")")?;
        }
        Ok(())
    }
}

impl FromStr for SemanticReference {
    type Err = SemanticReferenceError;

    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        if input.is_empty() {
            return Err(reference_error("reference cannot be empty"));
        }
        if input.len() > MAX_REFERENCE_BYTES {
            return Err(reference_error(format!(
                "reference exceeds {MAX_REFERENCE_BYTES} bytes"
            )));
        }
        if input.chars().any(char::is_whitespace) {
            return Err(reference_error("reference cannot contain whitespace"));
        }

        let (identity, parameter_text) = match input.find('(') {
            Some(open) => {
                if !input.ends_with(')') || input[open + 1..input.len() - 1].contains(['(', ')']) {
                    return Err(reference_error(
                        "reference has malformed parameter delimiters",
                    ));
                }
                (&input[..open], Some(&input[open + 1..input.len() - 1]))
            }
            None if input.contains(')') => {
                return Err(reference_error(
                    "reference has an unmatched closing delimiter",
                ));
            }
            None => (input, None),
        };
        let (qualified_name, version) = identity
            .rsplit_once('@')
            .ok_or_else(|| reference_error("reference must contain an explicit @version"))?;
        if qualified_name.contains('@') {
            return Err(reference_error(
                "reference contains multiple version delimiters",
            ));
        }
        let mut names = qualified_name.split('.');
        let namespace = names
            .next()
            .ok_or_else(|| reference_error("reference namespace is missing"))?;
        let name = names
            .next()
            .ok_or_else(|| reference_error("reference name is missing"))?;
        if names.next().is_some() {
            return Err(reference_error(
                "reference must contain exactly one namespace separator",
            ));
        }
        let parsed_version = version
            .parse::<u32>()
            .map_err(|_| reference_error("definition version must be a positive u32"))?;
        if parsed_version == 0 || parsed_version.to_string() != version {
            return Err(reference_error(
                "definition version must be canonical positive u32 text",
            ));
        }
        let parameters = match parameter_text {
            Some("") => return Err(reference_error("empty parameter lists are not canonical")),
            Some(parameters) => parse_parameters(parameters)?,
            None => BTreeMap::new(),
        };
        let reference = Self::new(namespace, name, parsed_version, parameters)?;
        if reference.to_string() != input {
            return Err(reference_error("reference is not canonically encoded"));
        }
        Ok(reference)
    }
}

impl Serialize for SemanticReference {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for SemanticReference {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

impl fmt::Display for SemanticReferenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for SemanticReferenceError {}

fn parse_parameters(
    input: &str,
) -> std::result::Result<BTreeMap<String, SemanticParameterValue>, SemanticReferenceError> {
    let mut parameters = BTreeMap::new();
    let mut remaining = input;
    let mut previous: Option<String> = None;
    while !remaining.is_empty() {
        let equals = remaining
            .find('=')
            .ok_or_else(|| reference_error("semantic parameter is missing `=`"))?;
        let key = &remaining[..equals];
        validate_identifier("parameter", key)?;
        if previous.as_deref().is_some_and(|prior| prior >= key) {
            return Err(reference_error(
                "semantic parameter keys must be unique and lexically ordered",
            ));
        }
        let value_input = &remaining[equals + 1..];
        let (value, consumed) = parse_parameter_value(value_input)?;
        if parameters.insert(key.to_owned(), value).is_some() {
            return Err(reference_error("semantic parameter key is duplicated"));
        }
        previous = Some(key.to_owned());
        if parameters.len() > MAX_PARAMETERS {
            return Err(reference_error(format!(
                "parameter count exceeds {MAX_PARAMETERS}"
            )));
        }
        remaining = &value_input[consumed..];
        if remaining.is_empty() {
            break;
        }
        remaining = remaining
            .strip_prefix(',')
            .ok_or_else(|| reference_error("semantic parameters must be comma separated"))?;
        if remaining.is_empty() {
            return Err(reference_error(
                "semantic parameter list has a trailing comma",
            ));
        }
    }
    Ok(parameters)
}

fn parse_parameter_value(
    input: &str,
) -> std::result::Result<(SemanticParameterValue, usize), SemanticReferenceError> {
    let mut stream = serde_json::Deserializer::from_str(input).into_iter::<Value>();
    let value = stream
        .next()
        .ok_or_else(|| reference_error("semantic parameter value is missing"))?
        .map_err(|error| reference_error(format!("invalid JSON scalar parameter: {error}")))?;
    let consumed = stream.byte_offset();
    let parameter = match value {
        Value::String(value) => SemanticParameterValue::String(value),
        Value::Number(value) => SemanticParameterValue::Number(value.to_string()),
        Value::Bool(value) => SemanticParameterValue::Boolean(value),
        Value::Null | Value::Array(_) | Value::Object(_) => {
            return Err(reference_error(
                "semantic parameters must be JSON strings, numbers, or booleans",
            ));
        }
    };
    parameter.validate()?;
    if input[..consumed] != parameter.render() {
        return Err(reference_error(
            "semantic parameter value is not canonically encoded",
        ));
    }
    Ok((parameter, consumed))
}

fn validate_identifier(
    label: &str,
    value: &str,
) -> std::result::Result<(), SemanticReferenceError> {
    if value.is_empty()
        || value.len() > MAX_IDENTIFIER_BYTES
        || !value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase())
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(reference_error(format!(
            "{label} must contain 1..={MAX_IDENTIFIER_BYTES} lowercase ASCII letters, digits, or underscores and start with a letter"
        )));
    }
    Ok(())
}

fn reference_error(detail: impl Into<String>) -> SemanticReferenceError {
    SemanticReferenceError {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_reference_round_trips() {
        for value in [
            "cdf.variant@1",
            "cdf.pii@1(class=\"email\")",
            "finance.money@7(currency=\"USD\",strict=true)",
            "measure.ratio@2(scale=1.25)",
        ] {
            let parsed = value.parse::<SemanticReference>().unwrap();
            assert_eq!(parsed.to_string(), value);
            assert_eq!(
                serde_json::from_str::<SemanticReference>(&serde_json::to_string(&parsed).unwrap())
                    .unwrap(),
                parsed
            );
        }
    }

    #[test]
    fn noncanonical_and_legacy_references_fail() {
        for value in [
            "json",
            "pii:email",
            "cdf.variant",
            "cdf.variant@0",
            "cdf.variant@01",
            "CDF.variant@1",
            "cdf.variant@1()",
            "cdf.pii@1(class =\"email\")",
            "cdf.pii@1(class=\"email\",class=\"secret\")",
            "test.value@1(z=true,a=false)",
            "test.value@1(value=null)",
            "test.value@1(value=[1])",
        ] {
            assert!(value.parse::<SemanticReference>().is_err(), "{value}");
        }
    }
}
