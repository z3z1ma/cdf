use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cdf_kernel::{CdfError, Result};
use mongodb::{
    bson::{Bson, Document},
    options::{
        AggregateOptions, Collation, FindOptions, Hint, ReadConcern, ReadConcernLevel,
        ReadPreference, ReadPreferenceOptions, SelectionCriteria,
    },
};
use serde::{Deserialize, Serialize, de};

use crate::query::MongoDbQuery;

const MAXIMUM_NATIVE_INPUT_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_NATIVE_INPUT_DEPTH: usize = 100;
const MAXIMUM_COMMENT_BYTES: usize = 1_024;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MongoDbNativeResourceOptions {
    #[serde(default)]
    pub(crate) filter: Option<String>,
    #[serde(default)]
    pub(crate) pipeline: Option<String>,
    #[serde(default)]
    pub(crate) max_time_ms: Option<u64>,
    #[serde(default)]
    pub(crate) allow_disk_use: bool,
    #[serde(default)]
    pub(crate) hint: Option<String>,
    #[serde(default)]
    pub(crate) collation: Option<String>,
    #[serde(default, rename = "let")]
    pub(crate) let_vars: Option<String>,
    #[serde(default)]
    pub(crate) comment: Option<String>,
    #[serde(default)]
    pub(crate) read_concern: Option<String>,
    #[serde(default)]
    pub(crate) read_preference: Option<String>,
}

#[derive(Clone, Debug)]
enum MongoDbNativeInput {
    Find { filter: Document },
    Aggregate { pipeline: Vec<Document> },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum MongoDbNativeInputArtifact {
    Find { bson_base64: String },
    Aggregate { bson_base64: Vec<String> },
}

impl Serialize for MongoDbNativeInput {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let artifact = match self {
            Self::Find { filter } => MongoDbNativeInputArtifact::Find {
                bson_base64: encode_document(filter).map_err(serde::ser::Error::custom)?,
            },
            Self::Aggregate { pipeline } => MongoDbNativeInputArtifact::Aggregate {
                bson_base64: pipeline
                    .iter()
                    .map(encode_document)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(serde::ser::Error::custom)?,
            },
        };
        artifact.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MongoDbNativeInput {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(
            match MongoDbNativeInputArtifact::deserialize(deserializer)? {
                MongoDbNativeInputArtifact::Find { bson_base64 } => Self::Find {
                    filter: decode_document(&bson_base64).map_err(de::Error::custom)?,
                },
                MongoDbNativeInputArtifact::Aggregate { bson_base64 } => Self::Aggregate {
                    pipeline: bson_base64
                        .iter()
                        .map(|value| decode_document(value))
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(de::Error::custom)?,
                },
            },
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MongoDbNativeExtraction {
    input: MongoDbNativeInput,
    max_time_ms: Option<u64>,
    allow_disk_use: bool,
    hint: Option<Hint>,
    collation: Option<Collation>,
    let_vars: Option<Document>,
    comment: Option<String>,
    read_concern: Option<ReadConcern>,
    read_preference: Option<ReadPreference>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MongoDbNativeExtractionArtifact {
    input: MongoDbNativeInput,
    max_time_ms: Option<u64>,
    allow_disk_use: bool,
    hint: Option<MongoDbHintArtifact>,
    collation: Option<Collation>,
    let_bson_base64: Option<String>,
    comment: Option<String>,
    read_concern: Option<ReadConcern>,
    read_preference: Option<MongoDbReadPreferenceArtifact>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum MongoDbHintArtifact {
    Name { value: String },
    Keys { bson_base64: String },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MongoDbReadPreferenceArtifact {
    mode: MongoDbReadPreferenceMode,
    tag_sets: Option<Vec<BTreeMap<String, String>>>,
    max_staleness_seconds: Option<u64>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum MongoDbReadPreferenceMode {
    Primary,
    Secondary,
    PrimaryPreferred,
    SecondaryPreferred,
    Nearest,
}

impl MongoDbReadPreferenceArtifact {
    fn from_read_preference(preference: &ReadPreference) -> std::result::Result<Self, String> {
        let (mode, options) = match preference {
            ReadPreference::Primary => (MongoDbReadPreferenceMode::Primary, None),
            ReadPreference::Secondary { options } => {
                (MongoDbReadPreferenceMode::Secondary, options.as_ref())
            }
            ReadPreference::PrimaryPreferred { options } => (
                MongoDbReadPreferenceMode::PrimaryPreferred,
                options.as_ref(),
            ),
            ReadPreference::SecondaryPreferred { options } => (
                MongoDbReadPreferenceMode::SecondaryPreferred,
                options.as_ref(),
            ),
            ReadPreference::Nearest { options } => {
                (MongoDbReadPreferenceMode::Nearest, options.as_ref())
            }
            _ => return Err("MongoDB read-preference mode is not supported".to_owned()),
        };
        #[allow(deprecated)]
        if options.is_some_and(|options| options.hedge.is_some()) {
            return Err("MongoDB read-preference hedge is not supported".to_owned());
        }
        Ok(Self {
            mode,
            tag_sets: options.and_then(|options| {
                options.tag_sets.as_ref().map(|sets| {
                    sets.iter()
                        .map(|set| {
                            set.iter()
                                .map(|(name, value)| (name.clone(), value.clone()))
                                .collect()
                        })
                        .collect()
                })
            }),
            max_staleness_seconds: options
                .and_then(|options| options.max_staleness)
                .map(|value| value.as_secs()),
        })
    }

    fn into_read_preference(self) -> std::result::Result<ReadPreference, String> {
        if matches!(self.mode, MongoDbReadPreferenceMode::Primary)
            && (self.tag_sets.is_some() || self.max_staleness_seconds.is_some())
        {
            return Err("MongoDB primary read preference cannot contain options".to_owned());
        }
        let options = if self.tag_sets.is_none() && self.max_staleness_seconds.is_none() {
            None
        } else {
            let mut options = ReadPreferenceOptions::default();
            options.tag_sets = self.tag_sets.map(|sets| {
                sets.into_iter()
                    .map(|set| set.into_iter().collect::<HashMap<_, _>>())
                    .collect()
            });
            options.max_staleness = self.max_staleness_seconds.map(Duration::from_secs);
            Some(options)
        };
        Ok(match self.mode {
            MongoDbReadPreferenceMode::Primary => ReadPreference::primary(),
            MongoDbReadPreferenceMode::Secondary => ReadPreference::secondary(options),
            MongoDbReadPreferenceMode::PrimaryPreferred => {
                ReadPreference::primary_preferred(options)
            }
            MongoDbReadPreferenceMode::SecondaryPreferred => {
                ReadPreference::secondary_preferred(options)
            }
            MongoDbReadPreferenceMode::Nearest => ReadPreference::nearest(options),
        })
    }
}

impl Serialize for MongoDbNativeExtraction {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let hint = self
            .hint
            .as_ref()
            .map(|hint| match hint {
                Hint::Name(value) => Ok(MongoDbHintArtifact::Name {
                    value: value.clone(),
                }),
                Hint::Keys(keys) => Ok(MongoDbHintArtifact::Keys {
                    bson_base64: encode_document(keys).map_err(serde::ser::Error::custom)?,
                }),
                _ => Err(serde::ser::Error::custom(
                    "MongoDB hint variant is not supported",
                )),
            })
            .transpose()?;
        let artifact = MongoDbNativeExtractionArtifact {
            input: self.input.clone(),
            max_time_ms: self.max_time_ms,
            allow_disk_use: self.allow_disk_use,
            hint,
            collation: self.collation.clone(),
            let_bson_base64: self
                .let_vars
                .as_ref()
                .map(encode_document)
                .transpose()
                .map_err(serde::ser::Error::custom)?,
            comment: self.comment.clone(),
            read_concern: self.read_concern.clone(),
            read_preference: self
                .read_preference
                .as_ref()
                .map(MongoDbReadPreferenceArtifact::from_read_preference)
                .transpose()
                .map_err(serde::ser::Error::custom)?,
        };
        artifact.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MongoDbNativeExtraction {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let artifact = MongoDbNativeExtractionArtifact::deserialize(deserializer)?;
        let hint = artifact
            .hint
            .map(|hint| match hint {
                MongoDbHintArtifact::Name { value } => Ok(Hint::Name(value)),
                MongoDbHintArtifact::Keys { bson_base64 } => {
                    decode_document(&bson_base64).map(Hint::Keys)
                }
            })
            .transpose()
            .map_err(de::Error::custom)?;
        let let_vars = artifact
            .let_bson_base64
            .map(|value| decode_document(&value))
            .transpose()
            .map_err(de::Error::custom)?;
        Ok(Self {
            input: artifact.input,
            max_time_ms: artifact.max_time_ms,
            allow_disk_use: artifact.allow_disk_use,
            hint,
            collation: artifact.collation,
            let_vars,
            comment: artifact.comment,
            read_concern: artifact.read_concern,
            read_preference: artifact
                .read_preference
                .map(MongoDbReadPreferenceArtifact::into_read_preference)
                .transpose()
                .map_err(de::Error::custom)?,
        })
    }
}

pub(crate) enum MongoDbReadCommand {
    Find {
        filter: Document,
        options: Box<FindOptions>,
    },
    Aggregate {
        pipeline: Vec<Document>,
        options: Box<AggregateOptions>,
    },
}

impl MongoDbNativeExtraction {
    pub(crate) fn compile(options: MongoDbNativeResourceOptions) -> Result<Self> {
        if options.filter.is_some() && options.pipeline.is_some() {
            return Err(CdfError::contract(
                "MongoDB resource options `filter` and `pipeline` are mutually exclusive",
            ));
        }
        let input = match (options.filter, options.pipeline) {
            (Some(filter), None) => MongoDbNativeInput::Find {
                filter: parse_document("filter", &filter)?,
            },
            (None, Some(pipeline)) => {
                let pipeline = parse_pipeline(&pipeline)?;
                validate_read_only_pipeline(&pipeline)?;
                MongoDbNativeInput::Aggregate { pipeline }
            }
            (None, None) => MongoDbNativeInput::Find {
                filter: Document::new(),
            },
            (Some(_), Some(_)) => unreachable!("mutual exclusion checked above"),
        };
        if !matches!(input, MongoDbNativeInput::Aggregate { .. })
            && (options.allow_disk_use || options.let_vars.is_some())
        {
            return Err(CdfError::contract(
                "MongoDB resource options `allow_disk_use` and `let` require an aggregation pipeline",
            ));
        }
        if let Some(max_time_ms) = options.max_time_ms
            && !(1..=3_600_000).contains(&max_time_ms)
        {
            return Err(CdfError::contract(
                "MongoDB max_time_ms must be in 1..=3600000",
            ));
        }
        if let Some(comment) = options.comment.as_ref()
            && comment.len() > MAXIMUM_COMMENT_BYTES
        {
            return Err(CdfError::contract(
                "MongoDB resource comment must contain at most 1024 UTF-8 bytes",
            ));
        }
        let hint = options.hint.as_deref().map(parse_hint).transpose()?;
        let collation = options
            .collation
            .as_deref()
            .map(parse_collation)
            .transpose()?;
        let let_vars = options
            .let_vars
            .as_deref()
            .map(|value| parse_document("let", value))
            .transpose()?;
        let read_concern = options
            .read_concern
            .as_deref()
            .map(parse_read_concern)
            .transpose()?;
        let read_preference = options
            .read_preference
            .as_deref()
            .map(parse_read_preference)
            .transpose()?;
        let extraction = Self {
            input,
            max_time_ms: options.max_time_ms,
            allow_disk_use: options.allow_disk_use,
            hint,
            collation,
            let_vars,
            comment: options.comment,
            read_concern,
            read_preference,
        };
        extraction.validate()?;
        Ok(extraction)
    }

    pub(crate) fn validate_for_cdc(
        &self,
        bootstrap: crate::driver::MongoDbBootstrap,
    ) -> Result<()> {
        let empty_find =
            matches!(&self.input, MongoDbNativeInput::Find { filter } if filter.is_empty());
        if bootstrap == crate::driver::MongoDbBootstrap::Latest
            && (!empty_find
                || self.max_time_ms.is_some()
                || self.allow_disk_use
                || self.hint.is_some()
                || self.collation.is_some()
                || self.let_vars.is_some())
        {
            return Err(CdfError::contract(
                "MongoDB CDC with `bootstrap => 'latest'` does not accept resource-level snapshot filter, pipeline, max_time_ms, allow_disk_use, hint, collation, or let options; use change_pipeline for read-only change-event filtering",
            ));
        }
        if self
            .read_concern
            .as_ref()
            .is_some_and(|concern| concern.level != ReadConcernLevel::Majority)
        {
            return Err(CdfError::contract(
                "MongoDB change streams accept only majority read concern or the server default",
            ));
        }
        if bootstrap == crate::driver::MongoDbBootstrap::Snapshot
            && self
                .read_preference
                .as_ref()
                .is_some_and(|preference| !matches!(preference, ReadPreference::Primary))
        {
            return Err(CdfError::contract(
                "MongoDB CDC snapshot bootstrap requires primary read preference so the snapshot cannot lag behind its change-stream handoff token",
            ));
        }
        Ok(())
    }

    pub(crate) fn change_stream_read_concern(&self) -> Option<ReadConcern> {
        self.read_concern.clone()
    }

    pub(crate) fn change_stream_selection_criteria(&self) -> Option<SelectionCriteria> {
        self.read_preference
            .clone()
            .map(SelectionCriteria::ReadPreference)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(max_time_ms) = self.max_time_ms
            && !(1..=3_600_000).contains(&max_time_ms)
        {
            return Err(CdfError::contract(
                "MongoDB compiled max_time_ms must be in 1..=3600000",
            ));
        }
        if self
            .comment
            .as_ref()
            .is_some_and(|value| value.len() > MAXIMUM_COMMENT_BYTES)
        {
            return Err(CdfError::contract(
                "MongoDB compiled comment must contain at most 1024 UTF-8 bytes",
            ));
        }
        match &self.input {
            MongoDbNativeInput::Find { filter } => {
                validate_document_bound("filter", filter)?;
                if self.allow_disk_use || self.let_vars.is_some() {
                    return Err(CdfError::contract(
                        "MongoDB compiled find input cannot use allow_disk_use or let",
                    ));
                }
            }
            MongoDbNativeInput::Aggregate { pipeline } => {
                validate_pipeline_bound(pipeline)?;
                validate_read_only_pipeline(pipeline)?;
            }
        }
        if let Some(let_vars) = self.let_vars.as_ref() {
            validate_document_bound("let", let_vars)?;
        }
        if let Some(hint) = self.hint.as_ref() {
            match hint {
                Hint::Name(name) if name.is_empty() => {
                    return Err(CdfError::contract(
                        "MongoDB compiled hint name must be nonempty",
                    ));
                }
                Hint::Keys(keys) if keys.is_empty() => {
                    return Err(CdfError::contract(
                        "MongoDB compiled hint document must be nonempty",
                    ));
                }
                Hint::Keys(keys) => validate_document_bound("hint", keys)?,
                _ => {}
            }
        }
        if self
            .collation
            .as_ref()
            .is_some_and(|collation| collation.locale.is_empty())
        {
            return Err(CdfError::contract(
                "MongoDB compiled collation locale must be nonempty",
            ));
        }
        if let Some(read_concern) = self.read_concern.as_ref() {
            match &read_concern.level {
                ReadConcernLevel::Local
                | ReadConcernLevel::Majority
                | ReadConcernLevel::Linearizable
                | ReadConcernLevel::Available
                | ReadConcernLevel::Snapshot => {}
                _ => {
                    return Err(CdfError::contract(
                        "MongoDB compiled read_concern is not supported",
                    ));
                }
            }
        }
        if let Some(read_preference) = self.read_preference.as_ref() {
            validate_read_preference_options(read_preference)?;
        }
        Ok(())
    }

    pub(crate) fn validate_for_descriptor(
        &self,
        descriptor: &cdf_kernel::ResourceDescriptor,
    ) -> Result<()> {
        self.validate()?;
        if descriptor.cursor.is_some() && self.is_nondeterministic() {
            return Err(CdfError::contract(
                "MongoDB aggregation uses nondeterministic output and cannot drive an incremental cursor; use a bounded full replacement or a deterministic pipeline",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_portable(&self) -> Result<()> {
        if let MongoDbNativeInput::Aggregate { pipeline } = &self.input
            && pipeline_has_collection_dependencies(pipeline)
        {
            return Err(CdfError::contract(
                "MongoDB aggregation references additional collections that are not yet independently attested; run it locally or remove the cross-collection stages before exporting a portable plan",
            ));
        }
        Ok(())
    }

    pub(crate) fn identity_hash(&self) -> Result<String> {
        cdf_runtime::artifact_hash(self)
    }

    pub(crate) fn redacted_summary(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "input_kind": match self.input {
                MongoDbNativeInput::Find { .. } => "find",
                MongoDbNativeInput::Aggregate { .. } => "aggregate",
            },
            "native_input_sha256": cdf_runtime::artifact_hash(&self.input)?,
            "pipeline_stages": match &self.input {
                MongoDbNativeInput::Find { .. } => 0,
                MongoDbNativeInput::Aggregate { pipeline } => pipeline.len(),
            },
            "max_time_ms": self.max_time_ms,
            "allow_disk_use": self.allow_disk_use,
            "hint": self.hint.is_some(),
            "collation": self.collation.is_some(),
            "let": self.let_vars.is_some(),
            "comment": self.comment.as_ref().map(|value| value.len()),
            "read_concern": self.read_concern.is_some(),
            "read_preference": self.read_preference.is_some(),
        }))
    }

    pub(crate) fn execution_command(
        &self,
        query: MongoDbQuery,
        cursor_batch_rows: u32,
    ) -> MongoDbReadCommand {
        match &self.input {
            MongoDbNativeInput::Find { filter } => {
                let mut options = self.find_options(cursor_batch_rows);
                options.projection = Some(query.projection);
                options.sort = (!query.sort.is_empty()).then_some(query.sort);
                options.limit = query.limit;
                MongoDbReadCommand::Find {
                    filter: combine_filters(filter.clone(), query.filter),
                    options: Box::new(options),
                }
            }
            MongoDbNativeInput::Aggregate { pipeline } => {
                let mut pipeline = pipeline.clone();
                append_outer_stages(&mut pipeline, query);
                MongoDbReadCommand::Aggregate {
                    pipeline,
                    options: Box::new(self.aggregate_options(cursor_batch_rows)),
                }
            }
        }
    }

    pub(crate) fn discovery_command(
        &self,
        maximum_records: u64,
        cursor_batch_rows: u32,
    ) -> Result<MongoDbReadCommand> {
        let limit = i64::try_from(maximum_records)
            .map_err(|_| CdfError::contract("MongoDB discovery record bound exceeds i64"))?;
        Ok(match &self.input {
            MongoDbNativeInput::Find { filter } => {
                let mut options = self.find_options(cursor_batch_rows);
                options.sort = Some(mongodb::bson::doc! {"_id": 1_i32});
                options.limit = Some(limit);
                MongoDbReadCommand::Find {
                    filter: filter.clone(),
                    options: Box::new(options),
                }
            }
            MongoDbNativeInput::Aggregate { pipeline } => {
                let mut pipeline = pipeline.clone();
                pipeline.push(mongodb::bson::doc! {"$limit": limit});
                MongoDbReadCommand::Aggregate {
                    pipeline,
                    options: Box::new(self.aggregate_options(cursor_batch_rows)),
                }
            }
        })
    }

    fn find_options(&self, cursor_batch_rows: u32) -> FindOptions {
        let mut options = FindOptions::default();
        options.batch_size = Some(cursor_batch_rows);
        options.max_time = self.max_time_ms.map(Duration::from_millis);
        options.hint = self.hint.clone();
        options.collation = self.collation.clone();
        options.comment = self.comment.clone().map(Bson::String);
        options.read_concern = self.read_concern.clone();
        options.selection_criteria = self
            .read_preference
            .clone()
            .map(SelectionCriteria::ReadPreference);
        options
    }

    fn aggregate_options(&self, cursor_batch_rows: u32) -> AggregateOptions {
        let mut options = AggregateOptions::default();
        options.batch_size = Some(cursor_batch_rows);
        options.max_time = self.max_time_ms.map(Duration::from_millis);
        options.allow_disk_use = Some(self.allow_disk_use);
        options.hint = self.hint.clone();
        options.collation = self.collation.clone();
        options.let_vars = self.let_vars.clone();
        options.comment = self.comment.clone().map(Bson::String);
        options.read_concern = self.read_concern.clone();
        options.selection_criteria = self
            .read_preference
            .clone()
            .map(SelectionCriteria::ReadPreference);
        options
    }

    fn is_nondeterministic(&self) -> bool {
        match &self.input {
            MongoDbNativeInput::Find { .. } => false,
            MongoDbNativeInput::Aggregate { pipeline } => pipeline
                .iter()
                .any(|stage| document_contains_key(stage, &["$sample", "$rand", "$sampleRate"])),
        }
    }
}

fn append_outer_stages(pipeline: &mut Vec<Document>, query: MongoDbQuery) {
    if !query.filter.is_empty() {
        pipeline.push(mongodb::bson::doc! {"$match": query.filter});
    }
    if !query.sort.is_empty() {
        pipeline.push(mongodb::bson::doc! {"$sort": query.sort});
    }
    if !query.projection.is_empty() {
        pipeline.push(mongodb::bson::doc! {"$project": query.projection});
    }
    if let Some(limit) = query.limit {
        pipeline.push(mongodb::bson::doc! {"$limit": limit});
    }
}

fn combine_filters(base: Document, outer: Document) -> Document {
    if base.is_empty() {
        return outer;
    }
    if outer.is_empty() {
        return base;
    }
    mongodb::bson::doc! {"$and": [base, outer]}
}

pub(crate) fn parse_pipeline(value: &str) -> Result<Vec<Document>> {
    let value = parse_ordered_json("pipeline", value)?;
    let OrderedJson::Array(stages) = value else {
        return Err(CdfError::contract(
            "MongoDB resource pipeline must be an Extended JSON array",
        ));
    };
    let mut pipeline = Vec::with_capacity(stages.len());
    for stage in stages {
        let Bson::Document(stage) = stage.into_bson("pipeline")? else {
            return Err(CdfError::contract(
                "MongoDB resource pipeline entries must be Extended JSON documents",
            ));
        };
        pipeline.push(stage);
    }
    validate_pipeline_bound(&pipeline)?;
    Ok(pipeline)
}

fn parse_document(label: &str, value: &str) -> Result<Document> {
    let value = parse_ordered_json(label, value)?;
    let Bson::Document(document) = value.into_bson(label)? else {
        return Err(CdfError::contract(format!(
            "MongoDB resource {label} must be an Extended JSON document"
        )));
    };
    validate_document_bound(label, &document)?;
    Ok(document)
}

fn parse_hint(value: &str) -> Result<Hint> {
    let value = parse_ordered_json("hint", value)?;
    match value.into_bson("hint")? {
        Bson::String(name) if !name.is_empty() => Ok(Hint::Name(name)),
        Bson::Document(keys) if !keys.is_empty() => Ok(Hint::Keys(keys)),
        _ => Err(CdfError::contract(
            "MongoDB resource hint must be a nonempty Extended JSON string or document",
        )),
    }
}

fn parse_collation(value: &str) -> Result<Collation> {
    let value = parse_ordered_json("collation", value)?;
    let OrderedJson::Object(fields) = &value else {
        return Err(CdfError::contract(
            "MongoDB resource collation must be an Extended JSON document",
        ));
    };
    const FIELDS: &[&str] = &[
        "locale",
        "strength",
        "caseLevel",
        "caseFirst",
        "numericOrdering",
        "alternate",
        "maxVariable",
        "normalization",
        "backwards",
    ];
    reject_unknown_fields("collation", fields, FIELDS)?;
    serde_json::from_value(value.into_json()).map_err(|_| {
        CdfError::contract("MongoDB resource collation is not a valid MongoDB collation document")
    })
}

fn parse_read_concern(value: &str) -> Result<ReadConcern> {
    if !matches!(
        value,
        "local" | "majority" | "linearizable" | "available" | "snapshot"
    ) {
        return Err(CdfError::contract(
            "MongoDB read_concern must be local, majority, linearizable, available, or snapshot",
        ));
    }
    Ok(ReadConcern::custom(value))
}

fn parse_read_preference(value: &str) -> Result<ReadPreference> {
    let value = parse_ordered_json("read_preference", value)?;
    let OrderedJson::Object(fields) = &value else {
        return Err(CdfError::contract(
            "MongoDB read_preference must be an Extended JSON document",
        ));
    };
    reject_unknown_fields(
        "read_preference",
        fields,
        &["mode", "tagSets", "maxStalenessSeconds"],
    )?;
    let preference: ReadPreference = serde_json::from_value(value.into_json()).map_err(|_| {
        CdfError::contract(
            "MongoDB read_preference is not a valid MongoDB read-preference document",
        )
    })?;
    validate_read_preference_options(&preference)?;
    Ok(preference)
}

fn validate_read_preference_options(preference: &ReadPreference) -> Result<()> {
    let options = match preference {
        ReadPreference::Primary => None,
        ReadPreference::Secondary { options }
        | ReadPreference::PrimaryPreferred { options }
        | ReadPreference::SecondaryPreferred { options }
        | ReadPreference::Nearest { options } => options.as_ref(),
        _ => {
            return Err(CdfError::contract(
                "MongoDB compiled read_preference mode is not supported",
            ));
        }
    };
    let Some(options) = options else {
        return Ok(());
    };
    if options
        .max_staleness
        .is_some_and(|value| value < Duration::from_secs(90))
    {
        return Err(CdfError::contract(
            "MongoDB read_preference maxStalenessSeconds must be at least 90",
        ));
    }
    if let Some(tag_sets) = &options.tag_sets
        && (tag_sets.len() > 100
            || tag_sets.iter().any(|tags| {
                tags.len() > 100
                    || tags.iter().any(|(name, value)| {
                        name.is_empty()
                            || name.len() > 1_024
                            || value.len() > 1_024
                            || name.contains('\0')
                            || value.contains('\0')
                    })
            }))
    {
        return Err(CdfError::contract(
            "MongoDB read_preference tagSets exceed the 100-set, 100-tag, or 1024-byte tag bound",
        ));
    }
    #[allow(deprecated)]
    if options.hedge.is_some() {
        return Err(CdfError::contract(
            "MongoDB read_preference hedge is not supported",
        ));
    }
    Ok(())
}

fn reject_unknown_fields(
    label: &str,
    fields: &[(String, OrderedJson)],
    accepted: &[&str],
) -> Result<()> {
    let mut unknown = fields
        .iter()
        .map(|(name, _)| name.as_str())
        .filter(|name| !accepted.contains(name))
        .collect::<Vec<_>>();
    unknown.sort_unstable();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(CdfError::contract(format!(
            "MongoDB resource {label} contains unsupported fields: {}",
            unknown.join(", ")
        )))
    }
}

fn validate_pipeline_bound(pipeline: &[Document]) -> Result<()> {
    let bytes = mongodb::bson::serialize_to_vec(&mongodb::bson::doc! {"pipeline": pipeline})
        .map_err(|_| CdfError::contract("MongoDB resource pipeline cannot be encoded as BSON"))?;
    if bytes.len() > MAXIMUM_NATIVE_INPUT_BYTES {
        return Err(CdfError::contract(
            "MongoDB resource pipeline exceeds the 16 MiB command input bound",
        ));
    }
    Ok(())
}

fn encode_document(document: &Document) -> std::result::Result<String, String> {
    let bytes = mongodb::bson::serialize_to_vec(document)
        .map_err(|_| "MongoDB native document cannot be encoded as BSON".to_owned())?;
    Ok(BASE64_STANDARD.encode(bytes))
}

fn decode_document(value: &str) -> std::result::Result<Document, String> {
    let bytes = BASE64_STANDARD
        .decode(value)
        .map_err(|_| "MongoDB native document contains invalid base64".to_owned())?;
    if bytes.len() > MAXIMUM_NATIVE_INPUT_BYTES {
        return Err("MongoDB native document exceeds the 16 MiB BSON input bound".to_owned());
    }
    let raw = mongodb::bson::raw::RawDocumentBuf::from_bytes(bytes)
        .map_err(|_| "MongoDB native document contains malformed BSON".to_owned())?;
    validate_raw_native_document(&raw, 0)?;
    Document::try_from(raw)
        .map_err(|_| "MongoDB native document contains malformed BSON".to_owned())
}

fn validate_raw_native_document(
    document: &mongodb::bson::raw::RawDocument,
    depth: usize,
) -> std::result::Result<(), String> {
    if depth > MAXIMUM_NATIVE_INPUT_DEPTH {
        return Err("MongoDB native document exceeds the 100-level BSON nesting bound".to_owned());
    }
    let mut names = BTreeSet::new();
    for element in document {
        let (name, value) =
            element.map_err(|_| "MongoDB native document contains malformed BSON".to_owned())?;
        if !names.insert(name.as_str().to_owned()) {
            return Err("MongoDB native document contains duplicate field names".to_owned());
        }
        match value {
            mongodb::bson::RawBsonRef::Document(document) => {
                validate_raw_native_document(document, depth + 1)?;
            }
            mongodb::bson::RawBsonRef::Array(array) => {
                validate_raw_native_array(array, depth + 1)?;
            }
            mongodb::bson::RawBsonRef::JavaScriptCodeWithScope(value) => {
                validate_raw_native_document(value.scope, depth + 1)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_raw_native_array(
    array: &mongodb::bson::raw::RawArray,
    depth: usize,
) -> std::result::Result<(), String> {
    if depth > MAXIMUM_NATIVE_INPUT_DEPTH {
        return Err("MongoDB native document exceeds the 100-level BSON nesting bound".to_owned());
    }
    for value in array {
        match value.map_err(|_| "MongoDB native document contains malformed BSON".to_owned())? {
            mongodb::bson::RawBsonRef::Document(document) => {
                validate_raw_native_document(document, depth + 1)?;
            }
            mongodb::bson::RawBsonRef::Array(array) => {
                validate_raw_native_array(array, depth + 1)?;
            }
            mongodb::bson::RawBsonRef::JavaScriptCodeWithScope(value) => {
                validate_raw_native_document(value.scope, depth + 1)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_document_bound(label: &str, document: &Document) -> Result<()> {
    let bytes = mongodb::bson::serialize_to_vec(document).map_err(|_| {
        CdfError::contract(format!(
            "MongoDB resource {label} cannot be encoded as BSON"
        ))
    })?;
    if bytes.len() > MAXIMUM_NATIVE_INPUT_BYTES {
        return Err(CdfError::contract(format!(
            "MongoDB resource {label} exceeds the 16 MiB BSON input bound"
        )));
    }
    Ok(())
}

fn validate_read_only_pipeline(pipeline: &[Document]) -> Result<()> {
    for stage in pipeline {
        if stage.len() != 1
            || stage
                .keys()
                .next()
                .is_none_or(|name| !name.starts_with('$'))
        {
            return Err(CdfError::contract(
                "MongoDB resource pipeline entries must contain exactly one aggregation stage operator",
            ));
        }
        if document_contains_key(stage, &["$out", "$merge", "$changeStream"]) {
            return Err(CdfError::contract(
                "MongoDB resource pipeline contains a write or change-stream stage; finite extraction accepts read-only aggregation stages",
            ));
        }
    }
    Ok(())
}

fn pipeline_has_collection_dependencies(pipeline: &[Document]) -> bool {
    pipeline
        .iter()
        .any(|stage| document_contains_key(stage, &["$lookup", "$unionWith", "$graphLookup"]))
}

fn document_contains_key(document: &Document, names: &[&str]) -> bool {
    document.iter().any(|(name, value)| {
        names.contains(&name.as_str())
            || match value {
                Bson::Document(value) => document_contains_key(value, names),
                Bson::Array(values) => values.iter().any(|value| match value {
                    Bson::Document(value) => document_contains_key(value, names),
                    Bson::Array(values) => array_contains_key(values, names),
                    _ => false,
                }),
                _ => false,
            }
    })
}

fn array_contains_key(values: &[Bson], names: &[&str]) -> bool {
    values.iter().any(|value| match value {
        Bson::Document(value) => document_contains_key(value, names),
        Bson::Array(values) => array_contains_key(values, names),
        _ => false,
    })
}

#[derive(Clone, Debug)]
enum OrderedJson {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

impl OrderedJson {
    fn into_bson(self, label: &str) -> Result<Bson> {
        match self {
            Self::Null => Ok(Bson::Null),
            Self::Bool(value) => Ok(Bson::Boolean(value)),
            Self::I64(value) => i32::try_from(value)
                .map(Bson::Int32)
                .or_else(|_| Ok(Bson::Int64(value))),
            Self::U64(value) => {
                let value = i64::try_from(value).map_err(|_| {
                    CdfError::contract(format!(
                        "MongoDB resource {label} contains an integer outside the BSON Int64 domain"
                    ))
                })?;
                i32::try_from(value)
                    .map(Bson::Int32)
                    .or_else(|_| Ok(Bson::Int64(value)))
            }
            Self::F64(value) if value.is_finite() => Ok(Bson::Double(value)),
            Self::F64(_) => Err(CdfError::contract(format!(
                "MongoDB resource {label} contains a non-finite number"
            ))),
            Self::String(value) => Ok(Bson::String(value)),
            Self::Array(values) => values
                .into_iter()
                .map(|value| value.into_bson(label))
                .collect::<Result<Vec<_>>>()
                .map(Bson::Array),
            Self::Object(values) => {
                if is_extended_json_wrapper(&values) {
                    let json = Self::Object(values.clone()).into_json();
                    return serde_json::from_value(json).map_err(|_| {
                        CdfError::contract(format!(
                            "MongoDB resource {label} contains malformed Extended JSON"
                        ))
                    });
                }
                let mut document = Document::new();
                for (name, value) in values {
                    if name.contains('\0') {
                        return Err(CdfError::contract(format!(
                            "MongoDB resource {label} contains a field name with a null byte"
                        )));
                    }
                    document.insert(name, value.into_bson(label)?);
                }
                Ok(Bson::Document(document))
            }
        }
    }

    fn into_json(self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => serde_json::Value::Bool(value),
            Self::I64(value) => serde_json::json!(value),
            Self::U64(value) => serde_json::json!(value),
            Self::F64(value) => serde_json::json!(value),
            Self::String(value) => serde_json::Value::String(value),
            Self::Array(values) => {
                serde_json::Value::Array(values.into_iter().map(Self::into_json).collect())
            }
            Self::Object(values) => serde_json::Value::Object(
                values
                    .into_iter()
                    .map(|(name, value)| (name, value.into_json()))
                    .collect(),
            ),
        }
    }
}

fn is_extended_json_wrapper(values: &[(String, OrderedJson)]) -> bool {
    let Some((first, _)) = values.first() else {
        return false;
    };
    matches!(
        first.as_str(),
        "$oid"
            | "$numberInt"
            | "$numberLong"
            | "$numberDouble"
            | "$numberDecimal"
            | "$binary"
            | "$date"
            | "$timestamp"
            | "$regularExpression"
            | "$code"
            | "$symbol"
            | "$undefined"
            | "$minKey"
            | "$maxKey"
            | "$dbPointer"
    )
}

fn parse_ordered_json(label: &str, value: &str) -> Result<OrderedJson> {
    if value.len() > MAXIMUM_NATIVE_INPUT_BYTES {
        return Err(CdfError::contract(format!(
            "MongoDB resource {label} exceeds the 16 MiB input bound"
        )));
    }
    let mut deserializer = serde_json::Deserializer::from_str(value);
    let parsed = OrderedJson::deserialize(&mut deserializer).map_err(|error| {
        CdfError::contract(format!(
            "MongoDB resource {label} is not valid duplicate-free Extended JSON: {error}"
        ))
    })?;
    deserializer.end().map_err(|error| {
        CdfError::contract(format!(
            "MongoDB resource {label} has trailing JSON input: {error}"
        ))
    })?;
    validate_json_depth(&parsed, 1, label)?;
    Ok(parsed)
}

fn validate_json_depth(value: &OrderedJson, depth: usize, label: &str) -> Result<()> {
    if depth > MAXIMUM_NATIVE_INPUT_DEPTH {
        return Err(CdfError::contract(format!(
            "MongoDB resource {label} exceeds the 100-level BSON nesting bound"
        )));
    }
    match value {
        OrderedJson::Array(values) => {
            for value in values {
                validate_json_depth(value, depth + 1, label)?;
            }
        }
        OrderedJson::Object(values) => {
            for (_, value) in values {
                validate_json_depth(value, depth + 1, label)?;
            }
        }
        _ => {}
    }
    Ok(())
}

impl<'de> Deserialize<'de> for OrderedJson {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct OrderedJsonVisitor;

        impl<'de> de::Visitor<'de> for OrderedJsonVisitor {
            type Value = OrderedJson;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON value with unique object keys")
            }

            fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
                Ok(OrderedJson::Null)
            }

            fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
                Ok(OrderedJson::Null)
            }

            fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(OrderedJson::Bool(value))
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(OrderedJson::I64(value))
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(OrderedJson::U64(value))
            }

            fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E> {
                Ok(OrderedJson::F64(value))
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OrderedJson::String(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
                Ok(OrderedJson::String(value))
            }

            fn visit_seq<A>(self, mut values: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut result = Vec::new();
                while let Some(value) = values.next_element()? {
                    result.push(value);
                }
                Ok(OrderedJson::Array(result))
            }

            fn visit_map<A>(self, mut values: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut result = Vec::new();
                let mut names = BTreeSet::new();
                while let Some((name, value)) = values.next_entry::<String, OrderedJson>()? {
                    if !names.insert(name.clone()) {
                        return Err(de::Error::custom(format!("duplicate object key `{name}`")));
                    }
                    result.push((name, value));
                }
                Ok(OrderedJson::Object(result))
            }
        }

        deserializer.deserialize_any(OrderedJsonVisitor)
    }
}
