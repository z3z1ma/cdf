use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{SOURCE_POSITION_VERSION, require_text, validate_sha256};
use crate::{CdfError, Result};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "snake_case")]
pub enum CommittedLogPosition {
    #[serde(rename = "postgresql")]
    PostgreSql(PostgresCommitPosition),
    #[serde(rename = "mysql")]
    MySql(MySqlCommitPosition),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "snake_case")]
pub enum CommittedLogScope {
    #[serde(rename = "postgresql")]
    PostgreSql(PostgresLogScope),
    #[serde(rename = "mysql")]
    MySql(MySqlLogScope),
}

impl CommittedLogScope {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::PostgreSql(scope) => scope.validate(),
            Self::MySql(scope) => scope.validate(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommittedLogProtocol {
    #[serde(rename = "postgresql")]
    PostgreSql,
    #[serde(rename = "mysql")]
    MySql,
}

impl CommittedLogPosition {
    pub const fn protocol(&self) -> CommittedLogProtocol {
        match self {
            Self::PostgreSql(_) => CommittedLogProtocol::PostgreSql,
            Self::MySql(_) => CommittedLogProtocol::MySql,
        }
    }

    pub const fn version(&self) -> u16 {
        match self {
            Self::PostgreSql(position) => position.version,
            Self::MySql(position) => position.version,
        }
    }

    pub fn scope(&self) -> CommittedLogScope {
        match self {
            Self::PostgreSql(position) => CommittedLogScope::PostgreSql(position.scope.clone()),
            Self::MySql(position) => CommittedLogScope::MySql(position.scope.clone()),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::PostgreSql(position) => position.validate(),
            Self::MySql(position) => position.validate(),
        }
    }

    pub fn same_scope(&self, other: &Self) -> Result<bool> {
        self.validate()?;
        other.validate()?;
        Ok(match (self, other) {
            (Self::PostgreSql(left), Self::PostgreSql(right)) => left.scope == right.scope,
            (Self::MySql(left), Self::MySql(right)) => left.scope == right.scope,
            _ => false,
        })
    }

    pub fn equivalent(&self, other: &Self) -> Result<bool> {
        Ok(self.same_scope(other)? && self == other)
    }

    pub fn reaches(&self, target: &Self) -> Result<bool> {
        if !self.same_scope(target)? {
            return Ok(false);
        }
        match (self, target) {
            (Self::PostgreSql(observed), Self::PostgreSql(target)) => {
                if observed.end_lsn == target.end_lsn && observed != target {
                    return Err(CdfError::data(
                        "PostgreSQL committed log positions conflict at one end LSN",
                    ));
                }
                Ok(observed.end_lsn >= target.end_lsn)
            }
            (Self::MySql(observed), Self::MySql(target)) => {
                let observed_coordinate = (observed.file_sequence, observed.end_log_position);
                let target_coordinate = (target.file_sequence, target.end_log_position);
                if observed_coordinate == target_coordinate && observed != target {
                    return Err(CdfError::data(
                        "MySQL committed log positions conflict at one binlog coordinate",
                    ));
                }
                Ok(observed_coordinate >= target_coordinate
                    && parse_gtid_set(&observed.executed_gtid_set)?
                        .contains_set(&parse_gtid_set(&target.executed_gtid_set)?))
            }
            _ => Ok(false),
        }
    }

    pub fn join_successor(&self, successor: &Self) -> Result<Self> {
        if !self.same_scope(successor)? {
            return Err(CdfError::data(
                "committed log positions cannot join across protocols or scopes",
            ));
        }
        match (self, successor) {
            (Self::PostgreSql(previous), Self::PostgreSql(next)) => {
                if next.end_lsn < previous.end_lsn {
                    return Err(CdfError::data(
                        "PostgreSQL committed log position regressed",
                    ));
                }
                if next.end_lsn == previous.end_lsn && next != previous {
                    return Err(CdfError::data(
                        "PostgreSQL committed log position has conflicting evidence at one end LSN",
                    ));
                }
            }
            (Self::MySql(previous), Self::MySql(next)) => {
                let previous_coordinate = (previous.file_sequence, previous.end_log_position);
                let next_coordinate = (next.file_sequence, next.end_log_position);
                if next_coordinate < previous_coordinate {
                    return Err(CdfError::data("MySQL committed log position regressed"));
                }
                let previous_set = parse_gtid_set(&previous.executed_gtid_set)?;
                let next_set = parse_gtid_set(&next.executed_gtid_set)?;
                if !next_set.contains_set(&previous_set) {
                    return Err(CdfError::data(
                        "MySQL committed log position regressed its executed GTID set",
                    ));
                }
                if next_coordinate > previous_coordinate
                    && previous_set
                        .contains_transaction(&parse_transaction_gtid(&next.transaction_gtid)?)
                {
                    return Err(CdfError::data(
                        "MySQL advancing commit position repeated an already executed transaction GTID",
                    ));
                }
                if next_coordinate == previous_coordinate && next != previous {
                    return Err(CdfError::data(
                        "MySQL committed log position has conflicting evidence at one binlog coordinate",
                    ));
                }
            }
            _ => {
                return Err(CdfError::data(
                    "committed log positions cannot join across protocols",
                ));
            }
        }
        Ok(successor.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresCommitPosition {
    pub version: u16,
    pub scope: PostgresLogScope,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub xid: u32,
}

impl PostgresCommitPosition {
    fn validate(&self) -> Result<()> {
        validate_position_version(self.version)?;
        self.scope.validate()?;
        if self.commit_lsn == 0 {
            return Err(CdfError::contract(
                "PostgreSQL commit LSN must be greater than zero",
            ));
        }
        if self.end_lsn < self.commit_lsn {
            return Err(CdfError::contract(
                "PostgreSQL end LSN must be greater than or equal to its commit LSN",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PostgresLogScope {
    pub system_identifier: String,
    pub database_oid: u32,
    pub slot: String,
    pub output_plugin: String,
    pub semantics_sha256: String,
}

impl PostgresLogScope {
    fn validate(&self) -> Result<()> {
        validate_positive_decimal("PostgreSQL system identifier", &self.system_identifier)?;
        if self.database_oid == 0 {
            return Err(CdfError::contract(
                "PostgreSQL database OID must be greater than zero",
            ));
        }
        require_text("PostgreSQL logical replication slot", &self.slot)?;
        require_text("PostgreSQL output plugin", &self.output_plugin)?;
        validate_sha256("PostgreSQL capture semantics", &self.semantics_sha256)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MySqlCommitPosition {
    pub version: u16,
    pub scope: MySqlLogScope,
    pub binlog_file: String,
    pub file_sequence: u64,
    pub end_log_position: u64,
    pub executed_gtid_set: String,
    pub transaction_gtid: String,
}

impl MySqlCommitPosition {
    fn validate(&self) -> Result<()> {
        validate_position_version(self.version)?;
        self.scope.validate()?;
        let parsed_sequence =
            parse_binlog_file_sequence(&self.scope.binlog_basename, &self.binlog_file)?;
        if parsed_sequence != self.file_sequence {
            return Err(CdfError::contract(format!(
                "MySQL binlog file sequence {} does not match parsed filename sequence {parsed_sequence}",
                self.file_sequence
            )));
        }
        if self.end_log_position < 4 {
            return Err(CdfError::contract(
                "MySQL end log position must be at least the first legal binlog event position 4",
            ));
        }
        let executed = parse_gtid_set(&self.executed_gtid_set)?;
        let transaction = parse_transaction_gtid(&self.transaction_gtid)?;
        if !executed.contains_transaction(&transaction) {
            return Err(CdfError::contract(
                "MySQL transaction GTID is absent from the executed GTID set",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MySqlLogScope {
    pub source_binding: String,
    pub active_server_uuid: String,
    pub binlog_basename: String,
    pub semantics_sha256: String,
}

impl MySqlLogScope {
    fn validate(&self) -> Result<()> {
        require_text("MySQL source binding", &self.source_binding)?;
        validate_uuid("MySQL active server UUID", &self.active_server_uuid)?;
        require_text("MySQL binlog basename", &self.binlog_basename)?;
        if self.binlog_basename.ends_with('.') {
            return Err(CdfError::contract(
                "MySQL binlog basename must not include its numeric file suffix",
            ));
        }
        validate_sha256("MySQL capture semantics", &self.semantics_sha256)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "snake_case")]
pub enum ResumeTokenPosition {
    #[serde(rename = "mongodb_change_stream")]
    MongoChangeStream(MongoChangeStreamResumeToken),
}

impl ResumeTokenPosition {
    pub const fn version(&self) -> u16 {
        match self {
            Self::MongoChangeStream(position) => position.version,
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::MongoChangeStream(position) => position.validate(),
        }
    }

    pub fn same_scope(&self, other: &Self) -> Result<bool> {
        self.validate()?;
        other.validate()?;
        Ok(match (self, other) {
            (Self::MongoChangeStream(left), Self::MongoChangeStream(right)) => {
                left.scope == right.scope
            }
        })
    }

    pub fn equivalent(&self, other: &Self) -> Result<bool> {
        if !self.same_scope(other)? {
            return Ok(false);
        }
        Ok(match (self, other) {
            (Self::MongoChangeStream(left), Self::MongoChangeStream(right)) => {
                left.token_bson_base64 == right.token_bson_base64
                    && left.token_sha256 == right.token_sha256
                    && left.resume_mode == right.resume_mode
            }
        })
    }

    pub fn same_restart_scope(&self, other: &Self) -> Result<bool> {
        if !self.same_scope(other)? {
            return Ok(false);
        }
        Ok(match (self, other) {
            (Self::MongoChangeStream(left), Self::MongoChangeStream(right)) => {
                left.resume_mode == right.resume_mode
            }
        })
    }

    pub fn ordered_prefix_successor(&self, successor: &Self) -> Result<Self> {
        if !self.same_scope(successor)? {
            return Err(CdfError::data(
                "MongoDB resume tokens cannot advance across change-stream scopes",
            ));
        }
        Ok(successor.clone())
    }

    pub fn reaches(&self, target: &Self) -> Result<bool> {
        self.equivalent(target)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MongoChangeStreamResumeToken {
    pub version: u16,
    pub scope: MongoChangeStreamScope,
    pub token_bson_base64: String,
    pub token_sha256: String,
    pub resume_mode: MongoResumeMode,
    pub token_source: MongoResumeTokenSource,
}

impl MongoChangeStreamResumeToken {
    fn validate(&self) -> Result<()> {
        validate_position_version(self.version)?;
        self.scope.validate()?;
        validate_sha256("MongoDB resume token", &self.token_sha256)?;
        let decoded = STANDARD.decode(&self.token_bson_base64).map_err(|error| {
            CdfError::contract(format!(
                "MongoDB resume token must be canonical standard base64: {error}"
            ))
        })?;
        if decoded.is_empty() {
            return Err(CdfError::contract(
                "MongoDB resume token BSON bytes must not be empty",
            ));
        }
        let token_document =
            bson::deserialize_from_slice::<bson::Document>(&decoded).map_err(|error| {
                CdfError::contract(format!("MongoDB resume token is malformed BSON: {error}"))
            })?;
        if token_document.is_empty() {
            return Err(CdfError::contract(
                "MongoDB resume token BSON document must contain source-issued token data",
            ));
        }
        if STANDARD.encode(&decoded) != self.token_bson_base64 {
            return Err(CdfError::contract(
                "MongoDB resume token must use canonical padded standard base64",
            ));
        }
        let observed_hash = format!("sha256:{}", hex::encode(Sha256::digest(&decoded)));
        if observed_hash != self.token_sha256 {
            return Err(CdfError::contract(
                "MongoDB resume token SHA-256 does not match its BSON bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MongoChangeStreamScope {
    pub source_binding: String,
    pub watch_level: MongoWatchLevel,
    pub database: Option<String>,
    pub collection: Option<String>,
    pub pipeline_sha256: String,
    pub options_sha256: String,
}

impl MongoChangeStreamScope {
    fn validate(&self) -> Result<()> {
        require_text("MongoDB source binding", &self.source_binding)?;
        validate_sha256("MongoDB change-stream pipeline", &self.pipeline_sha256)?;
        validate_sha256("MongoDB change-stream options", &self.options_sha256)?;
        match self.watch_level {
            MongoWatchLevel::Cluster if self.database.is_none() && self.collection.is_none() => {}
            MongoWatchLevel::Database
                if self.database.as_deref().is_some_and(valid_text)
                    && self.collection.is_none() => {}
            MongoWatchLevel::Collection
                if self.database.as_deref().is_some_and(valid_text)
                    && self.collection.as_deref().is_some_and(valid_text) => {}
            MongoWatchLevel::Cluster => {
                return Err(CdfError::contract(
                    "cluster-level MongoDB change streams cannot bind a database or collection",
                ));
            }
            MongoWatchLevel::Database => {
                return Err(CdfError::contract(
                    "database-level MongoDB change streams require only a database target",
                ));
            }
            MongoWatchLevel::Collection => {
                return Err(CdfError::contract(
                    "collection-level MongoDB change streams require database and collection targets",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MongoWatchLevel {
    Cluster,
    Database,
    Collection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MongoResumeMode {
    ResumeAfter,
    StartAfter,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MongoResumeTokenSource {
    Event,
    PostBatch,
}

fn validate_position_version(version: u16) -> Result<()> {
    if version != SOURCE_POSITION_VERSION {
        return Err(CdfError::contract(format!(
            "source position version {version} is unsupported; expected {SOURCE_POSITION_VERSION}; regenerate current project and checkpoint artifacts"
        )));
    }
    Ok(())
}

fn valid_text(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(char::is_control)
}

fn validate_positive_decimal(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value == "0"
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(CdfError::contract(format!(
            "{label} must be canonical positive decimal text"
        )));
    }
    Ok(())
}

fn validate_uuid(label: &str, value: &str) -> Result<()> {
    let canonical = value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'),
        });
    if !canonical {
        return Err(CdfError::contract(format!(
            "{label} must be a canonical lowercase UUID"
        )));
    }
    Ok(())
}

fn parse_binlog_file_sequence(basename: &str, filename: &str) -> Result<u64> {
    let suffix = filename
        .strip_prefix(basename)
        .and_then(|suffix| suffix.strip_prefix('.'))
        .ok_or_else(|| {
            CdfError::contract(format!(
                "MySQL binlog filename `{filename}` does not match basename `{basename}`"
            ))
        })?;
    if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(CdfError::contract(
            "MySQL binlog filename requires a decimal numeric suffix",
        ));
    }
    suffix.parse::<u64>().map_err(|error| {
        CdfError::contract(format!(
            "MySQL binlog filename sequence does not fit u64: {error}"
        ))
    })
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct GtidSource {
    uuid: String,
    tag: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GtidInterval {
    start: u64,
    end: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GtidSet(BTreeMap<GtidSource, Vec<GtidInterval>>);

impl GtidSet {
    fn contains_transaction(&self, transaction: &(GtidSource, u64)) -> bool {
        self.0.get(&transaction.0).is_some_and(|intervals| {
            intervals
                .iter()
                .any(|interval| interval.start <= transaction.1 && transaction.1 <= interval.end)
        })
    }

    fn contains_set(&self, other: &Self) -> bool {
        other.0.iter().all(|(source, required)| {
            self.0.get(source).is_some_and(|available| {
                required.iter().all(|required| {
                    available.iter().any(|available| {
                        available.start <= required.start && available.end >= required.end
                    })
                })
            })
        })
    }
}

fn parse_gtid_set(value: &str) -> Result<GtidSet> {
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        return Err(CdfError::contract(
            "MySQL executed GTID set must be nonempty canonical text without whitespace",
        ));
    }
    let mut sources = BTreeMap::new();
    let mut previous_uuid = None;
    for entry in value.split(',') {
        let groups = parse_gtid_uuid_set(entry)?;
        let uuid = &groups[0].0.uuid;
        if previous_uuid
            .as_ref()
            .is_some_and(|previous| previous >= uuid)
        {
            return Err(CdfError::contract(
                "MySQL executed GTID set UUID entries must be unique and canonically sorted",
            ));
        }
        previous_uuid = Some(uuid.clone());
        for (source, intervals) in groups {
            if sources.insert(source, intervals).is_some() {
                return Err(CdfError::contract(
                    "MySQL executed GTID set source/tag groups must be unique",
                ));
            }
        }
    }
    Ok(GtidSet(sources))
}

fn parse_transaction_gtid(value: &str) -> Result<(GtidSource, u64)> {
    if value.chars().any(char::is_whitespace) || value.contains(',') {
        return Err(CdfError::contract(
            "MySQL transaction GTID must be one canonical GTID",
        ));
    }
    let groups = parse_gtid_uuid_set(value)?;
    let [(source, intervals)] = groups.as_slice() else {
        return Err(CdfError::contract(
            "MySQL transaction GTID must contain one sequence number, not a range",
        ));
    };
    let [interval] = intervals.as_slice() else {
        return Err(CdfError::contract(
            "MySQL transaction GTID must contain one sequence number, not a range",
        ));
    };
    if interval.start != interval.end {
        return Err(CdfError::contract(
            "MySQL transaction GTID must contain one sequence number, not a range",
        ));
    }
    Ok((source.clone(), interval.start))
}

fn parse_gtid_uuid_set(value: &str) -> Result<Vec<(GtidSource, Vec<GtidInterval>)>> {
    let parts = value.split(':').collect::<Vec<_>>();
    if parts.len() < 2 {
        return Err(CdfError::contract(
            "MySQL GTID entry requires a source UUID and sequence interval",
        ));
    }
    validate_uuid("MySQL GTID source UUID", parts[0])?;
    let mut groups = Vec::new();
    let mut previous_source = None;
    let mut index = 1;
    while index < parts.len() {
        let tag = if looks_like_gtid_interval(parts[index]) {
            None
        } else {
            validate_gtid_tag(parts[index])?;
            let tag = Some(parts[index].to_owned());
            index += 1;
            tag
        };
        let interval_start = index;
        while index < parts.len() && looks_like_gtid_interval(parts[index]) {
            index += 1;
        }
        if interval_start == index {
            return Err(CdfError::contract(
                "MySQL tagged GTID group requires at least one sequence interval",
            ));
        }
        let source = GtidSource {
            uuid: parts[0].to_owned(),
            tag,
        };
        if previous_source
            .as_ref()
            .is_some_and(|previous| previous >= &source)
        {
            return Err(CdfError::contract(
                "MySQL GTID tags must be unique and canonically sorted within each UUID entry",
            ));
        }
        let intervals = parse_gtid_intervals(&parts[interval_start..index])?;
        previous_source = Some(source.clone());
        groups.push((source, intervals));
    }
    Ok(groups)
}

fn validate_gtid_tag(value: &str) -> Result<()> {
    let mut bytes = value.bytes();
    if value.len() > 32
        || !bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(CdfError::contract(
            "MySQL GTID tag must be a 1-32 byte ASCII letter/underscore identifier",
        ));
    }
    Ok(())
}

fn looks_like_gtid_interval(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'-')
}

fn parse_gtid_intervals(parts: &[&str]) -> Result<Vec<GtidInterval>> {
    let mut intervals = Vec::with_capacity(parts.len());
    for part in parts {
        let mut bounds = part.split('-');
        let start = parse_positive_sequence(bounds.next().unwrap_or_default())?;
        let end = bounds.next().map_or(Ok(start), parse_positive_sequence)?;
        if bounds.next().is_some() || (part.contains('-') && end <= start) {
            return Err(CdfError::contract(
                "MySQL GTID interval must be one positive sequence or ascending range",
            ));
        }
        if intervals.last().is_some_and(|previous: &GtidInterval| {
            previous.end.checked_add(1).is_none_or(|next| next >= start)
        }) {
            return Err(CdfError::contract(
                "MySQL GTID intervals must be sorted, disjoint, and coalesced",
            ));
        }
        intervals.push(GtidInterval { start, end });
    }
    Ok(intervals)
}

fn parse_positive_sequence(value: &str) -> Result<u64> {
    if value.is_empty()
        || value == "0"
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(CdfError::contract(
            "MySQL GTID sequence must be canonical positive decimal text",
        ));
    }
    let sequence = value.parse::<u64>().map_err(|error| {
        CdfError::contract(format!("MySQL GTID sequence does not fit u64: {error}"))
    })?;
    if sequence > i64::MAX as u64 {
        return Err(CdfError::contract(
            "MySQL GTID sequence exceeds the native positive signed-64 range",
        ));
    }
    Ok(sequence)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorKind, SourcePosition};

    const HASH_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const UUID_A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    const UUID_B: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    fn postgres(end_lsn: u64) -> SourcePosition {
        SourcePosition::committed_log(CommittedLogPosition::PostgreSql(PostgresCommitPosition {
            version: SOURCE_POSITION_VERSION,
            scope: PostgresLogScope {
                system_identifier: "7421938841407953395".to_owned(),
                database_oid: 16_384,
                slot: "cdf_orders".to_owned(),
                output_plugin: "pgoutput".to_owned(),
                semantics_sha256: HASH_A.to_owned(),
            },
            commit_lsn: end_lsn.saturating_sub(1).max(1),
            end_lsn,
            xid: 42,
        }))
    }

    fn mysql(
        file_sequence: u64,
        end_log_position: u64,
        executed: &str,
        transaction_sequence: u64,
    ) -> SourcePosition {
        SourcePosition::committed_log(CommittedLogPosition::MySql(MySqlCommitPosition {
            version: SOURCE_POSITION_VERSION,
            scope: MySqlLogScope {
                source_binding: "orders-primary".to_owned(),
                active_server_uuid: UUID_A.to_owned(),
                binlog_basename: "mysql-bin".to_owned(),
                semantics_sha256: HASH_A.to_owned(),
            },
            binlog_file: format!("mysql-bin.{file_sequence:06}"),
            file_sequence,
            end_log_position,
            executed_gtid_set: executed.to_owned(),
            transaction_gtid: format!("{UUID_A}:{transaction_sequence}"),
        }))
    }

    fn mongo(bytes: &[u8], mode: MongoResumeMode) -> SourcePosition {
        SourcePosition::resume_token(ResumeTokenPosition::MongoChangeStream(
            MongoChangeStreamResumeToken {
                version: SOURCE_POSITION_VERSION,
                scope: MongoChangeStreamScope {
                    source_binding: "orders-stream".to_owned(),
                    watch_level: MongoWatchLevel::Collection,
                    database: Some("sales".to_owned()),
                    collection: Some("orders".to_owned()),
                    pipeline_sha256: HASH_A.to_owned(),
                    options_sha256: HASH_B.to_owned(),
                },
                token_bson_base64: STANDARD.encode(bytes),
                token_sha256: format!("sha256:{}", hex::encode(Sha256::digest(bytes))),
                resume_mode: mode,
                token_source: MongoResumeTokenSource::Event,
            },
        ))
    }

    #[test]
    fn postgres_positions_round_trip_full_lsn_and_join_monotonically() {
        let first = postgres(u64::MAX - 1);
        let terminal = postgres(u64::MAX);
        first.validate().unwrap();
        terminal.validate().unwrap();
        assert_eq!(
            serde_json::to_value(&first).unwrap()["protocol"],
            "postgresql"
        );
        assert!(terminal.reaches(&first).unwrap());
        assert_eq!(
            SourcePosition::join_ordered(Some(&first), std::slice::from_ref(&terminal)).unwrap(),
            terminal
        );
        assert!(SourcePosition::join_ordered(Some(&terminal), &[first]).is_err());
        let json = serde_json::to_vec(&terminal).unwrap();
        assert_eq!(
            serde_json::from_slice::<SourcePosition>(&json).unwrap(),
            terminal
        );

        let mut conflicting = terminal.clone();
        let SourcePosition::Log(position) = &mut conflicting else {
            unreachable!();
        };
        let CommittedLogPosition::PostgreSql(position) = position.as_mut() else {
            unreachable!();
        };
        position.xid = 43;
        assert!(conflicting.reaches(&terminal).is_err());
    }

    #[test]
    fn mysql_positions_require_canonical_gtid_superset_and_transaction_membership() {
        let first = mysql(7, 120, &format!("{UUID_A}:1-3,{UUID_B}:blue:1:4-5"), 3);
        let terminal = mysql(8, 44, &format!("{UUID_A}:1-5,{UUID_B}:blue:1:4-6"), 5);
        first.validate().unwrap();
        terminal.validate().unwrap();
        assert_eq!(serde_json::to_value(&first).unwrap()["protocol"], "mysql");
        assert!(terminal.reaches(&first).unwrap());
        assert_eq!(
            SourcePosition::join_ordered(Some(&first), std::slice::from_ref(&terminal)).unwrap(),
            terminal
        );

        let mut regression = terminal.clone();
        let SourcePosition::Log(position) = &mut regression else {
            unreachable!();
        };
        let CommittedLogPosition::MySql(regressed_position) = position.as_mut() else {
            unreachable!();
        };
        regressed_position.executed_gtid_set = format!("{UUID_A}:1-2");
        regressed_position.transaction_gtid = format!("{UUID_A}:2");
        assert_eq!(
            SourcePosition::join_ordered(Some(&first), &[regression])
                .unwrap_err()
                .kind,
            ErrorKind::Data
        );

        let mut conflicting = terminal.clone();
        let SourcePosition::Log(log) = &mut conflicting else {
            unreachable!();
        };
        let CommittedLogPosition::MySql(position) = log.as_mut() else {
            unreachable!();
        };
        position.transaction_gtid = format!("{UUID_B}:blue:6");
        assert!(conflicting.reaches(&terminal).is_err());

        let earlier_tagged =
            parse_gtid_set(&format!("{UUID_A}:1-3:Domain_1:1-3:11:Domain_2:8-10")).unwrap();
        let later_tagged =
            parse_gtid_set(&format!("{UUID_A}:1-5:Domain_1:1-3:11-12:Domain_2:8-12")).unwrap();
        assert!(later_tagged.contains_set(&earlier_tagged));
        assert!(later_tagged.contains_transaction(
            &parse_transaction_gtid(&format!("{UUID_A}:Domain_2:12")).unwrap()
        ));
        assert!(parse_gtid_set(&format!("{UUID_A}:{}:1", "a".repeat(33))).is_err());
        assert!(parse_gtid_set(&format!("{UUID_A}:1-1")).is_err());
    }

    #[test]
    fn mongo_tokens_round_trip_exact_bytes_and_never_invent_order() {
        let token = mongo(
            &[
                22, 0, 0, 0, 2, b'_', b'd', b'a', b't', b'a', 0, 6, 0, 0, 0, b't', b'o', b'k',
                b'e', b'n', 0, 0,
            ],
            MongoResumeMode::ResumeAfter,
        );
        token.validate().unwrap();
        let json = serde_json::to_vec(&token).unwrap();
        assert_eq!(
            serde_json::from_slice::<SourcePosition>(&json).unwrap(),
            token
        );
        assert!(token.reaches(&token).unwrap());

        let distinct = mongo(
            &[
                22, 0, 0, 0, 2, b'_', b'd', b'a', b't', b'a', 0, 6, 0, 0, 0, b'o', b't', b'h',
                b'e', b'r', 0, 0,
            ],
            MongoResumeMode::StartAfter,
        );
        assert!(!distinct.reaches(&token).unwrap());
        assert!(
            SourcePosition::join_ordered(Some(&token), std::slice::from_ref(&distinct)).is_err()
        );
        assert_eq!(token.advance_ordered_prefix(&distinct).unwrap(), distinct);

        let mut same_restart_authority = token.clone();
        let SourcePosition::ResumeToken(resume_token) = &mut same_restart_authority else {
            unreachable!();
        };
        let ResumeTokenPosition::MongoChangeStream(position) = resume_token.as_mut();
        position.token_source = MongoResumeTokenSource::PostBatch;
        assert!(same_restart_authority.equivalent(&token).unwrap());

        let mut tampered = token.clone();
        let SourcePosition::ResumeToken(resume_token) = &mut tampered else {
            unreachable!();
        };
        let ResumeTokenPosition::MongoChangeStream(tampered) = resume_token.as_mut();
        tampered.token_bson_base64 = STANDARD.encode([9, 9, 9]);
        assert!(tampered.validate().is_err());

        let empty_document = mongo(&[5, 0, 0, 0, 0], MongoResumeMode::ResumeAfter);
        assert!(empty_document.validate().is_err());
    }

    #[test]
    fn legacy_generic_log_shape_has_no_current_reader() {
        let error = serde_json::from_value::<SourcePosition>(serde_json::json!({
            "kind": "log",
            "version": 1,
            "log": "orders",
            "offset": 42,
            "sequence": "abc"
        }))
        .unwrap_err();
        assert!(!error.to_string().is_empty());
    }
}
