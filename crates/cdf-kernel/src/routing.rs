use std::{cmp::Ordering, str::FromStr};

use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Decimal32Array, Decimal64Array, Decimal128Array,
    Decimal256Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    StringViewArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_buffer::i256;
use arrow_schema::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CanonicalArrowDateUnit, CanonicalArrowType, CdfError, OutputBindingId, Result, SchemaHash,
    TargetName,
};

pub const ROUTE_FOLD_VERSION: u16 = 1;
pub const PRIMARY_OUTPUT_BINDING: &str = "primary";
const ROUTE_HASH_HEX_LENGTH: usize = 16;
const PROJECT_TOKEN_MAX_BYTES: usize = 128;

/// Compiled authority for partitioning one logical resource into a bounded target family.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutePlan {
    pub field: String,
    pub maximum_targets: u32,
    pub fold_version: u16,
}

impl RoutePlan {
    pub fn new(field: impl Into<String>, maximum_targets: u32) -> Result<Self> {
        let plan = Self {
            field: field.into(),
            maximum_targets,
            fold_version: ROUTE_FOLD_VERSION,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<()> {
        let bytes = self.field.as_bytes();
        if bytes.is_empty()
            || !bytes[0].is_ascii_alphabetic()
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            return Err(CdfError::contract(
                "route field must be one top-level unquoted ASCII identifier",
            ));
        }
        if self.maximum_targets == 0 {
            return Err(CdfError::contract(
                "routed target family requires MAX TARGETS greater than zero",
            ));
        }
        if self.fold_version != ROUTE_FOLD_VERSION {
            return Err(CdfError::contract(format!(
                "route fold version {} is unsupported; expected {}",
                self.fold_version, ROUTE_FOLD_VERSION
            )));
        }
        Ok(())
    }
}

/// Lossless scalar routing authority. The Arrow type remains part of identity, so values such as
/// integer `1` and string `"1"` cannot silently acquire the same output binding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteScalar {
    pub arrow_type: CanonicalArrowType,
    pub canonical_value: String,
}

impl RouteScalar {
    pub fn from_array(array: &dyn Array, row: usize) -> Result<Self> {
        if row >= array.len() {
            return Err(CdfError::internal(format!(
                "route row {row} exceeds Arrow array length {}",
                array.len()
            )));
        }
        if array.is_null(row) {
            return Err(CdfError::data(
                "route field is null; configure a non-null scalar route value",
            ));
        }
        let scalar = Self {
            arrow_type: CanonicalArrowType::from_arrow(array.data_type())?,
            canonical_value: route_value_text(array, row)?,
        };
        scalar.validate()?;
        Ok(scalar)
    }

    pub fn validate(&self) -> Result<()> {
        validate_route_scalar_text(&self.arrow_type, &self.canonical_value)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self)
            .map_err(|error| CdfError::internal(format!("encode route scalar: {error}")))
    }

    pub fn output_binding(&self, fold_version: u16) -> Result<OutputBindingId> {
        if fold_version != ROUTE_FOLD_VERSION {
            return Err(CdfError::contract(format!(
                "route fold version {fold_version} is unsupported; expected {ROUTE_FOLD_VERSION}"
            )));
        }
        OutputBindingId::new(format!(
            "route_{}",
            route_hash(fold_version, &self.canonical_bytes()?)
        ))
    }

    pub fn route_token(&self, maximum_bytes: Option<usize>) -> Result<String> {
        self.validate()?;
        if is_project_token(&self.canonical_value) {
            if maximum_bytes.is_some_and(|maximum| self.canonical_value.len() > maximum) {
                return Err(CdfError::contract(format!(
                    "exact route token {:?} exceeds the destination identifier budget; shorten the route value or logical target",
                    self.canonical_value
                )));
            }
            return Ok(self.canonical_value.clone());
        }

        let hash = route_hash(ROUTE_FOLD_VERSION, &self.canonical_bytes()?);
        let suffix = &hash[..ROUTE_HASH_HEX_LENGTH];
        let maximum = maximum_bytes.unwrap_or(usize::MAX);
        let minimum = 2_usize
            .checked_add(suffix.len())
            .ok_or_else(|| CdfError::contract("route token length overflow"))?;
        if maximum < minimum {
            return Err(CdfError::contract(format!(
                "destination identifier budget {maximum} cannot preserve the routed target hash suffix"
            )));
        }
        let slug_budget = maximum - 1 - suffix.len();
        let mut slug = route_slug(&self.canonical_value);
        if slug.len() > slug_budget {
            slug.truncate(slug_budget);
            while slug.ends_with('_') {
                slug.pop();
            }
        }
        if slug.is_empty() {
            slug.push('r');
        }
        Ok(format!("{slug}_{suffix}"))
    }

    fn type_key(&self) -> Vec<u8> {
        serde_json::to_vec(&self.arrow_type).unwrap_or_default()
    }

    fn sort_key(&self) -> Vec<u8> {
        route_sort_key(&self.arrow_type, &self.canonical_value)
            .unwrap_or_else(|_| self.canonical_value.as_bytes().to_vec())
    }
}

impl PartialOrd for RouteScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RouteScalar {
    fn cmp(&self, other: &Self) -> Ordering {
        self.type_key()
            .cmp(&other.type_key())
            .then_with(|| self.sort_key().cmp(&other.sort_key()))
    }
}

/// One pre-admitted logical output and destination object in a routed family.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteTargetBinding {
    pub route_value: RouteScalar,
    pub output_binding: OutputBindingId,
    pub route_token: String,
    pub physical_target: TargetName,
    pub schema_hash: SchemaHash,
}

/// Complete, canonically ordered, pre-admitted route map bound into plans, packages, and receipts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteTargetFamily {
    pub route: RoutePlan,
    pub logical_target: TargetName,
    pub identifier_max_length: Option<u16>,
    pub bindings: Vec<RouteTargetBinding>,
}

impl RouteTargetFamily {
    pub fn new<I>(
        route: RoutePlan,
        logical_target: TargetName,
        identifier_max_length: Option<u16>,
        outputs: I,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = (RouteScalar, SchemaHash)>,
    {
        build_family(route, logical_target, identifier_max_length, outputs)
    }

    pub fn validate(&self) -> Result<()> {
        let rebuilt = build_family(
            self.route.clone(),
            self.logical_target.clone(),
            self.identifier_max_length,
            self.bindings
                .iter()
                .map(|binding| (binding.route_value.clone(), binding.schema_hash.clone())),
        )?;
        if rebuilt != *self {
            return Err(CdfError::data(
                "routed target family does not match its deterministic route derivation",
            ));
        }
        Ok(())
    }

    pub fn binding_for(&self, value: &RouteScalar) -> Option<&RouteTargetBinding> {
        self.bindings
            .binary_search_by(|binding| binding.route_value.cmp(value))
            .ok()
            .and_then(|index| self.bindings.get(index))
    }
}

fn build_family<I>(
    route: RoutePlan,
    logical_target: TargetName,
    identifier_max_length: Option<u16>,
    outputs: I,
) -> Result<RouteTargetFamily>
where
    I: IntoIterator<Item = (RouteScalar, SchemaHash)>,
{
    route.validate()?;
    let final_component = target_final_component(&logical_target)?;
    let component_budget = identifier_max_length
        .map(usize::from)
        .map(|maximum| {
            maximum
                .checked_sub(final_component.len() + 2)
                .ok_or_else(|| {
                    CdfError::contract(format!(
                        "logical target component {final_component:?} leaves no room for a routed `__<token>` suffix under the destination's {maximum}-byte identifier limit"
                    ))
                })
        })
        .transpose()?;
    let mut outputs = outputs.into_iter().collect::<Vec<_>>();
    for (value, _) in &outputs {
        value.validate()?;
    }
    outputs.sort_by(|left, right| left.0.cmp(&right.0));
    if outputs.is_empty() {
        return Err(CdfError::contract(
            "routed target family requires at least one explicitly admitted output",
        ));
    }
    if outputs.len() > route.maximum_targets as usize {
        return Err(CdfError::contract(format!(
            "routed target family admits {} outputs, exceeding MAX TARGETS {}",
            outputs.len(),
            route.maximum_targets
        )));
    }

    let mut bindings = Vec::<RouteTargetBinding>::with_capacity(outputs.len());
    for (route_value, schema_hash) in outputs {
        let route_token = route_value.route_token(component_budget)?;
        let physical_target = append_route_token(&logical_target, &route_token)?;
        let output_binding = route_value.output_binding(route.fold_version)?;
        if bindings.iter().any(|prior| {
            prior.route_value == route_value
                || prior.route_token == route_token
                || prior.physical_target == physical_target
                || prior.output_binding == output_binding
        }) {
            return Err(CdfError::contract(format!(
                "distinct typed route values collide at derived token {route_token:?}; change the route value or logical target"
            )));
        }
        bindings.push(RouteTargetBinding {
            route_value,
            output_binding,
            route_token,
            physical_target,
            schema_hash,
        });
    }
    Ok(RouteTargetFamily {
        route,
        logical_target,
        identifier_max_length,
        bindings,
    })
}

fn route_value_text(array: &dyn Array, row: usize) -> Result<String> {
    macro_rules! value {
        ($array:ty) => {{
            array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| CdfError::internal("route Arrow array/type mismatch"))?
                .value(row)
                .to_string()
        }};
    }
    Ok(match array.data_type() {
        DataType::Boolean => value!(BooleanArray),
        DataType::Int8 => value!(Int8Array),
        DataType::Int16 => value!(Int16Array),
        DataType::Int32 => value!(Int32Array),
        DataType::Int64 => value!(Int64Array),
        DataType::UInt8 => value!(UInt8Array),
        DataType::UInt16 => value!(UInt16Array),
        DataType::UInt32 => value!(UInt32Array),
        DataType::UInt64 => value!(UInt64Array),
        DataType::Decimal32(_, _) => value!(Decimal32Array),
        DataType::Decimal64(_, _) => value!(Decimal64Array),
        DataType::Decimal128(_, _) => value!(Decimal128Array),
        DataType::Decimal256(_, _) => value!(Decimal256Array),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CdfError::internal("route Arrow array/type mismatch"))?
            .value(row)
            .to_owned(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .ok_or_else(|| CdfError::internal("route Arrow array/type mismatch"))?
            .value(row)
            .to_owned(),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .ok_or_else(|| CdfError::internal("route Arrow array/type mismatch"))?
            .value(row)
            .to_owned(),
        DataType::Date32 => value!(Date32Array),
        DataType::Date64 => value!(Date64Array),
        DataType::Time32(TimeUnit::Second) => value!(Time32SecondArray),
        DataType::Time32(TimeUnit::Millisecond) => value!(Time32MillisecondArray),
        DataType::Time64(TimeUnit::Microsecond) => value!(Time64MicrosecondArray),
        DataType::Time64(TimeUnit::Nanosecond) => value!(Time64NanosecondArray),
        DataType::Timestamp(TimeUnit::Second, _) => value!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => value!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => value!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => value!(TimestampNanosecondArray),
        other => {
            return Err(CdfError::contract(format!(
                "route field type {other} is not an admitted scalar"
            )));
        }
    })
}

fn validate_route_scalar_text(arrow_type: &CanonicalArrowType, value: &str) -> Result<()> {
    let canonical = match arrow_type {
        CanonicalArrowType::Boolean => value
            .parse::<bool>()
            .map(|parsed| parsed.to_string())
            .map_err(|_| CdfError::data("route Boolean value is not canonical"))?,
        CanonicalArrowType::Int {
            signed: true,
            bits: 8,
        } => canonical_number::<i8>(value)?,
        CanonicalArrowType::Int {
            signed: true,
            bits: 16,
        } => canonical_number::<i16>(value)?,
        CanonicalArrowType::Int {
            signed: true,
            bits: 32,
        }
        | CanonicalArrowType::Time { bits: 32, .. } => canonical_number::<i32>(value)?,
        CanonicalArrowType::Date {
            unit: CanonicalArrowDateUnit::Day,
        } => canonical_number::<i32>(value)?,
        CanonicalArrowType::Int {
            signed: true,
            bits: 64,
        }
        | CanonicalArrowType::Time { bits: 64, .. }
        | CanonicalArrowType::Timestamp { .. }
        | CanonicalArrowType::Date {
            unit: CanonicalArrowDateUnit::Millisecond,
        } => canonical_number::<i64>(value)?,
        CanonicalArrowType::Int {
            signed: false,
            bits: 8,
        } => canonical_number::<u8>(value)?,
        CanonicalArrowType::Int {
            signed: false,
            bits: 16,
        } => canonical_number::<u16>(value)?,
        CanonicalArrowType::Int {
            signed: false,
            bits: 32,
        } => canonical_number::<u32>(value)?,
        CanonicalArrowType::Int {
            signed: false,
            bits: 64,
        } => canonical_number::<u64>(value)?,
        CanonicalArrowType::Decimal { bits: 32, .. } => canonical_number::<i32>(value)?,
        CanonicalArrowType::Decimal { bits: 64, .. } => canonical_number::<i64>(value)?,
        CanonicalArrowType::Decimal { bits: 128, .. } => canonical_number::<i128>(value)?,
        CanonicalArrowType::Decimal { bits: 256, .. } => i256::from_str(value)
            .map(|parsed| parsed.to_string())
            .map_err(|_| CdfError::data("route Decimal256 value is not canonical"))?,
        CanonicalArrowType::Utf8 { .. } | CanonicalArrowType::Utf8View => value.to_owned(),
        other => {
            return Err(CdfError::contract(format!(
                "route field type {other:?} is not an admitted scalar"
            )));
        }
    };
    if canonical != value {
        return Err(CdfError::data(
            "route scalar value is not in canonical Arrow form",
        ));
    }
    Ok(())
}

fn route_sort_key(arrow_type: &CanonicalArrowType, value: &str) -> Result<Vec<u8>> {
    macro_rules! signed {
        ($type:ty) => {{
            let parsed = value
                .parse::<$type>()
                .map_err(|_| CdfError::data("invalid canonical signed route scalar"))?;
            let mut bytes = parsed.to_be_bytes();
            bytes[0] ^= 0x80;
            bytes.to_vec()
        }};
    }
    macro_rules! unsigned {
        ($type:ty) => {{
            value
                .parse::<$type>()
                .map_err(|_| CdfError::data("invalid canonical unsigned route scalar"))?
                .to_be_bytes()
                .to_vec()
        }};
    }
    Ok(match arrow_type {
        CanonicalArrowType::Boolean => vec![u8::from(value == "true")],
        CanonicalArrowType::Int {
            signed: true,
            bits: 8,
        } => signed!(i8),
        CanonicalArrowType::Int {
            signed: true,
            bits: 16,
        } => signed!(i16),
        CanonicalArrowType::Int {
            signed: true,
            bits: 32,
        }
        | CanonicalArrowType::Time { bits: 32, .. }
        | CanonicalArrowType::Decimal { bits: 32, .. }
        | CanonicalArrowType::Date {
            unit: CanonicalArrowDateUnit::Day,
        } => signed!(i32),
        CanonicalArrowType::Int {
            signed: true,
            bits: 64,
        }
        | CanonicalArrowType::Time { bits: 64, .. }
        | CanonicalArrowType::Timestamp { .. }
        | CanonicalArrowType::Decimal { bits: 64, .. }
        | CanonicalArrowType::Date {
            unit: CanonicalArrowDateUnit::Millisecond,
        } => signed!(i64),
        CanonicalArrowType::Int {
            signed: false,
            bits: 8,
        } => unsigned!(u8),
        CanonicalArrowType::Int {
            signed: false,
            bits: 16,
        } => unsigned!(u16),
        CanonicalArrowType::Int {
            signed: false,
            bits: 32,
        } => unsigned!(u32),
        CanonicalArrowType::Int {
            signed: false,
            bits: 64,
        } => unsigned!(u64),
        CanonicalArrowType::Decimal { bits: 128, .. } => signed!(i128),
        CanonicalArrowType::Decimal { bits: 256, .. } => {
            let mut bytes = i256::from_str(value)
                .map_err(|_| CdfError::data("invalid canonical Decimal256 route scalar"))?
                .to_be_bytes();
            bytes[0] ^= 0x80;
            bytes.to_vec()
        }
        CanonicalArrowType::Utf8 { .. } | CanonicalArrowType::Utf8View => value.as_bytes().to_vec(),
        other => {
            return Err(CdfError::contract(format!(
                "route field type {other:?} is not an admitted scalar"
            )));
        }
    })
}

fn canonical_number<T>(value: &str) -> Result<String>
where
    T: FromStr + ToString,
{
    value
        .parse::<T>()
        .map(|parsed| parsed.to_string())
        .map_err(|_| CdfError::data("route scalar value is not canonical"))
}

fn route_hash(fold_version: u16, canonical: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"cdf-route-fold\0");
    digest.update(fold_version.to_be_bytes());
    digest.update(canonical);
    hex::encode(digest.finalize())
}

fn is_project_token(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=PROJECT_TOKEN_MAX_BYTES).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

fn route_slug(value: &str) -> String {
    let mut slug = String::new();
    let mut separated = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character.to_ascii_lowercase());
            separated = false;
        } else if !slug.is_empty() && !separated {
            slug.push('_');
            separated = true;
        }
    }
    while slug.ends_with('_') {
        slug.pop();
    }
    if slug.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        slug.insert_str(0, "r_");
    }
    if slug.is_empty() {
        slug.push_str("route");
    }
    slug
}

fn target_final_component(target: &TargetName) -> Result<&str> {
    let component = target.as_str().rsplit('.').next().unwrap_or_default();
    if component.is_empty() {
        return Err(CdfError::contract(
            "logical target requires a nonempty final component before route derivation",
        ));
    }
    Ok(component)
}

fn append_route_token(target: &TargetName, token: &str) -> Result<TargetName> {
    let (prefix, final_component) = target
        .as_str()
        .rsplit_once('.')
        .map_or((None, target.as_str()), |(prefix, component)| {
            (Some(prefix), component)
        });
    TargetName::new(match prefix {
        Some(prefix) => format!("{prefix}.{final_component}__{token}"),
        None => format!("{final_component}__{token}"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::Field;

    use super::*;

    #[test]
    fn route_plan_requires_a_top_level_field_and_positive_ceiling() {
        assert_eq!(RoutePlan::new("source_table", 256).unwrap().fold_version, 1);
        assert!(RoutePlan::new("nested.field", 1).is_err());
        assert!(RoutePlan::new("source_table", 0).is_err());
    }

    #[test]
    fn scalar_fold_preserves_safe_tokens_and_hashes_other_typed_values() {
        let strings = StringArray::from(vec!["orders", "Sales / West"]);
        let safe = RouteScalar::from_array(&strings, 0).unwrap();
        let folded = RouteScalar::from_array(&strings, 1).unwrap();
        assert_eq!(safe.route_token(Some(40)).unwrap(), "orders");
        let token = folded.route_token(Some(40)).unwrap();
        assert!(token.starts_with("sales_west_"), "{token}");
        assert_eq!(token.len(), "sales_west_".len() + ROUTE_HASH_HEX_LENGTH);

        let integer = RouteScalar::from_array(&Int64Array::from(vec![1]), 0).unwrap();
        let string = RouteScalar::from_array(&StringArray::from(vec!["1"]), 0).unwrap();
        assert_ne!(
            integer.output_binding(ROUTE_FOLD_VERSION).unwrap(),
            string.output_binding(ROUTE_FOLD_VERSION).unwrap()
        );
    }

    #[test]
    fn route_family_is_ordered_bounded_and_identifier_safe() {
        let values = [
            RouteScalar::from_array(&StringArray::from(vec!["west"]), 0).unwrap(),
            RouteScalar::from_array(&StringArray::from(vec!["Sales / East"]), 0).unwrap(),
        ];
        let family = RouteTargetFamily::new(
            RoutePlan::new("route", 2).unwrap(),
            TargetName::new("warehouse.events").unwrap(),
            Some(63),
            values
                .into_iter()
                .rev()
                .map(|value| (value, SchemaHash::new("sha256:output-schema").unwrap())),
        )
        .unwrap();
        assert_eq!(family.bindings.len(), 2);
        assert!(
            family
                .bindings
                .iter()
                .all(|binding| binding.physical_target.as_str().contains("events__"))
        );
        assert!(
            RouteTargetFamily::new(
                RoutePlan::new("route", 1).unwrap(),
                TargetName::new("warehouse.events").unwrap(),
                Some(63),
                family
                    .bindings
                    .iter()
                    .map(|binding| (binding.route_value.clone(), binding.schema_hash.clone())),
            )
            .is_err()
        );
    }

    #[test]
    fn null_nested_and_overlength_exact_routes_fail_closed() {
        let nulls = StringArray::from(vec![None::<&str>]);
        assert!(RouteScalar::from_array(&nulls, 0).is_err());

        let nested = arrow_array::StructArray::from(vec![(
            Arc::new(Field::new("value", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )]);
        assert!(RouteScalar::from_array(&nested, 0).is_err());

        let exact =
            RouteScalar::from_array(&StringArray::from(vec!["very_long_safe_token"]), 0).unwrap();
        assert!(exact.route_token(Some(4)).is_err());
    }
}
