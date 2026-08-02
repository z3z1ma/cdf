//! Borrowed JSON object decoding and shared lexical helpers.

use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor},
};
use serde_json::value::RawValue;

pub(crate) struct BorrowedJsonObject<'a>(pub(crate) Vec<(String, &'a RawValue)>);

impl<'de> Deserialize<'de> for BorrowedJsonObject<'de> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ObjectVisitor;

        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = BorrowedJsonObject<'de>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some(source) = map.next_key::<String>()? {
                    fields.push((source, map.next_value::<&'de RawValue>()?));
                }
                Ok(BorrowedJsonObject(fields))
            }
        }

        deserializer.deserialize_map(ObjectVisitor)
    }
}

pub(crate) fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    &bytes[start..]
}
