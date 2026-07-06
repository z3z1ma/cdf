use arrow_schema::Field;

pub const SEMANTIC_METADATA_KEY: &str = "firn:semantic";
pub const SOURCE_NAME_METADATA_KEY: &str = "firn:source_name";
pub const NULL_ORIGIN_METADATA_KEY: &str = "firn:null_origin";
pub fn with_source_name(field: Field, source_name: impl Into<String>) -> Field {
    with_metadata_value(field, SOURCE_NAME_METADATA_KEY, source_name)
}

pub fn source_name(field: &Field) -> Option<&str> {
    metadata_value(field, SOURCE_NAME_METADATA_KEY)
}

pub fn with_semantic(field: Field, semantic: impl Into<String>) -> Field {
    with_metadata_value(field, SEMANTIC_METADATA_KEY, semantic)
}

pub fn semantic(field: &Field) -> Option<&str> {
    metadata_value(field, SEMANTIC_METADATA_KEY)
}

pub fn with_null_origin(field: Field, null_origin: impl Into<String>) -> Field {
    with_metadata_value(field, NULL_ORIGIN_METADATA_KEY, null_origin)
}

pub fn null_origin(field: &Field) -> Option<&str> {
    metadata_value(field, NULL_ORIGIN_METADATA_KEY)
}

pub fn with_firn_metadata(
    field: Field,
    source_name: Option<impl Into<String>>,
    semantic: Option<impl Into<String>>,
    null_origin: Option<impl Into<String>>,
) -> Field {
    let field = match source_name {
        Some(value) => with_source_name(field, value),
        None => field,
    };
    let field = match semantic {
        Some(value) => with_semantic(field, value),
        None => field,
    };
    match null_origin {
        Some(value) => with_null_origin(field, value),
        None => field,
    }
}

fn with_metadata_value(field: Field, key: &'static str, value: impl Into<String>) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(key.to_owned(), value.into());
    field.with_metadata(metadata)
}

fn metadata_value<'a>(field: &'a Field, key: &'static str) -> Option<&'a str> {
    field.metadata().get(key).map(String::as_str)
}
