use std::{
    fmt,
    io::{BufReader, Read},
    marker::PhantomData,
};

use cdf_kernel::{CdfError, Result};
use cdf_package_contract::{
    FileEntry, LifecycleState, MANIFEST_VERSION, SegmentEntry, SignatureSlot,
};
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use sha2::{Digest, Sha256};

use crate::json::json_error;

/// Hashes the exact canonical identity object stored in `manifest.json` without retaining it.
///
/// Current-format manifests write optional archive metadata before identity. Archive metadata is
/// deliberately outside package identity, so it is skipped before hashing the balanced identity
/// object byte-for-byte.
pub fn stored_manifest_identity_hash(reader: impl Read) -> Result<String> {
    let mut reader = BufReader::new(reader);
    expect_byte(&mut reader, b'{', "package manifest object")?;
    let mut key = read_top_level_key(&mut reader)?;
    expect_byte(&mut reader, b':', "package manifest field separator")?;
    if key == "archives" {
        let first = read_non_whitespace_byte(&mut reader)?.ok_or_else(|| {
            CdfError::data("package manifest archive metadata ended before its value")
        })?;
        skip_balanced_object(&mut reader, first, "archive metadata")?;
        expect_byte(&mut reader, b',', "package manifest archive separator")?;
        key = read_top_level_key(&mut reader)?;
        expect_byte(&mut reader, b':', "package manifest field separator")?;
    }
    if key != "identity" {
        return Err(CdfError::data(format!(
            "canonical package manifest must begin with optional archives then identity; observed {key:?}"
        )));
    }
    let first = read_non_whitespace_byte(&mut reader)?
        .ok_or_else(|| CdfError::data("package manifest identity ended before its object"))?;
    if first != b'{' {
        return Err(CdfError::data(
            "package manifest identity must be a JSON object",
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update([first]);
    hash_balanced_object(&mut reader, &mut hasher, "identity")?;
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

fn read_top_level_key(reader: &mut impl Read) -> Result<String> {
    expect_byte(reader, b'"', "package manifest field name")?;
    let mut key = String::new();
    loop {
        let byte = read_byte(reader)?
            .ok_or_else(|| CdfError::data("package manifest field name ended before `\"`"))?;
        match byte {
            b'"' => return Ok(key),
            b'\\' => {
                return Err(CdfError::data(
                    "canonical package manifest top-level field names cannot be escaped",
                ));
            }
            byte if byte.is_ascii() && !byte.is_ascii_control() => key.push(char::from(byte)),
            _ => {
                return Err(CdfError::data(
                    "canonical package manifest top-level field name is not ASCII",
                ));
            }
        }
    }
}

fn expect_byte(reader: &mut impl Read, expected: u8, label: &str) -> Result<()> {
    let observed = read_non_whitespace_byte(reader)?
        .ok_or_else(|| CdfError::data(format!("{label} ended before byte {expected:?}")))?;
    if observed != expected {
        return Err(CdfError::data(format!(
            "{label} expected byte {expected:?}, observed {observed:?}"
        )));
    }
    Ok(())
}

fn read_non_whitespace_byte(reader: &mut impl Read) -> Result<Option<u8>> {
    loop {
        match read_byte(reader)? {
            Some(byte) if byte.is_ascii_whitespace() => {}
            other => return Ok(other),
        }
    }
}

fn skip_balanced_object(reader: &mut impl Read, first: u8, label: &str) -> Result<()> {
    if first != b'{' {
        return Err(CdfError::data(format!(
            "canonical package manifest {label} must be a JSON object"
        )));
    }
    consume_balanced_object(reader, |_| {}, label)
}

fn hash_balanced_object(reader: &mut impl Read, hasher: &mut Sha256, label: &str) -> Result<()> {
    consume_balanced_object(reader, |byte| hasher.update([byte]), label)
}

fn consume_balanced_object(
    reader: &mut impl Read,
    mut consume: impl FnMut(u8),
    label: &str,
) -> Result<()> {
    let mut depth = 1_u64;
    let mut in_string = false;
    let mut escaped = false;
    while depth > 0 {
        let byte = read_byte(reader)?
            .ok_or_else(|| CdfError::data(format!("package manifest {label} ended early")))?;
        consume(byte);
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| CdfError::data("manifest JSON nesting overflowed u64"))?;
            }
            b'}' => depth -= 1,
            _ => {}
        }
    }
    Ok(())
}

/// Constant-cardinality facts from a package manifest.
///
/// File and segment entries remain in canonical `manifest.json`; callers visit them through
/// [`visit_package_manifest`] instead of retaining them in this header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageManifestHeader {
    pub manifest_version: u16,
    pub package_hash: String,
    pub identity: ManifestIdentityHeader,
    pub lifecycle: LifecycleState,
    pub signature: SignatureSlot,
    pub has_archives: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestIdentityHeader {
    pub manifest_version: u16,
    pub package_id: String,
    pub layout: Vec<String>,
}

/// Parses one canonical package manifest while visiting file and segment entries in stored order.
///
/// The parser never materializes either cardinality-sized array. Callback failure stops parsing
/// immediately and is returned as a data error.
pub fn visit_package_manifest(
    reader: impl Read,
    file_visitor: &mut dyn FnMut(FileEntry) -> Result<()>,
    segment_visitor: &mut dyn FnMut(SegmentEntry) -> Result<()>,
) -> Result<PackageManifestHeader> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let mut callback_error = None;
    let parsed = PackageManifestSeed {
        file_visitor,
        segment_visitor,
        callback_error: &mut callback_error,
    }
    .deserialize(&mut deserializer);
    if let Some(error) = callback_error {
        return Err(error);
    }
    let header = parsed.map_err(json_error)?;
    deserializer.end().map_err(json_error)?;
    if header.manifest_version != MANIFEST_VERSION
        || header.identity.manifest_version != MANIFEST_VERSION
    {
        return Err(CdfError::data(format!(
            "package manifest/storage version must be {MANIFEST_VERSION}; observed manifest {} identity {}",
            header.manifest_version, header.identity.manifest_version
        )));
    }
    Ok(header)
}

struct PackageManifestSeed<'a> {
    file_visitor: &'a mut dyn FnMut(FileEntry) -> Result<()>,
    segment_visitor: &'a mut dyn FnMut(SegmentEntry) -> Result<()>,
    callback_error: &'a mut Option<CdfError>,
}

impl<'de> DeserializeSeed<'de> for PackageManifestSeed<'_> {
    type Value = PackageManifestHeader;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(PackageManifestVisitor {
            file_visitor: self.file_visitor,
            segment_visitor: self.segment_visitor,
            callback_error: self.callback_error,
        })
    }
}

struct PackageManifestVisitor<'a> {
    file_visitor: &'a mut dyn FnMut(FileEntry) -> Result<()>,
    segment_visitor: &'a mut dyn FnMut(SegmentEntry) -> Result<()>,
    callback_error: &'a mut Option<CdfError>,
}

impl<'de> Visitor<'de> for PackageManifestVisitor<'_> {
    type Value = PackageManifestHeader;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a CDF package manifest object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut archives = None;
        let mut identity = None;
        let mut lifecycle = None;
        let mut manifest_version = None;
        let mut package_hash = None;
        let mut signature = None;

        while let Some(field) = map.next_key::<String>()? {
            match field.as_str() {
                "archives" => {
                    require_absent(&archives, "archives")?;
                    map.next_value::<IgnoredAny>()?;
                    archives = Some(true);
                }
                "identity" => {
                    require_absent(&identity, "identity")?;
                    identity = Some(map.next_value_seed(ManifestIdentitySeed {
                        file_visitor: self.file_visitor,
                        segment_visitor: self.segment_visitor,
                        callback_error: self.callback_error,
                    })?);
                }
                "lifecycle" => {
                    require_absent(&lifecycle, "lifecycle")?;
                    lifecycle = Some(map.next_value()?);
                }
                "manifest_version" => {
                    require_absent(&manifest_version, "manifest_version")?;
                    manifest_version = Some(map.next_value()?);
                }
                "package_hash" => {
                    require_absent(&package_hash, "package_hash")?;
                    package_hash = Some(map.next_value()?);
                }
                "signature" => {
                    require_absent(&signature, "signature")?;
                    signature = Some(map.next_value()?);
                }
                unknown => return Err(de::Error::unknown_field(unknown, MANIFEST_FIELDS)),
            }
        }

        Ok(PackageManifestHeader {
            manifest_version: required(manifest_version, "manifest_version")?,
            package_hash: required(package_hash, "package_hash")?,
            identity: required(identity, "identity")?,
            lifecycle: required(lifecycle, "lifecycle")?,
            signature: required(signature, "signature")?,
            has_archives: archives.unwrap_or(false),
        })
    }
}

struct ManifestIdentitySeed<'a> {
    file_visitor: &'a mut dyn FnMut(FileEntry) -> Result<()>,
    segment_visitor: &'a mut dyn FnMut(SegmentEntry) -> Result<()>,
    callback_error: &'a mut Option<CdfError>,
}

impl<'de> DeserializeSeed<'de> for ManifestIdentitySeed<'_> {
    type Value = ManifestIdentityHeader;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ManifestIdentityVisitor {
            file_visitor: self.file_visitor,
            segment_visitor: self.segment_visitor,
            callback_error: self.callback_error,
        })
    }
}

struct ManifestIdentityVisitor<'a> {
    file_visitor: &'a mut dyn FnMut(FileEntry) -> Result<()>,
    segment_visitor: &'a mut dyn FnMut(SegmentEntry) -> Result<()>,
    callback_error: &'a mut Option<CdfError>,
}

impl<'de> Visitor<'de> for ManifestIdentityVisitor<'_> {
    type Value = ManifestIdentityHeader;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a CDF package manifest identity object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut files = None;
        let mut layout = None;
        let mut manifest_version = None;
        let mut package_id = None;
        let mut segments = None;

        while let Some(field) = map.next_key::<String>()? {
            match field.as_str() {
                "files" => {
                    require_absent(&files, "files")?;
                    map.next_value_seed(EntrySequenceSeed::<FileEntry> {
                        visitor: self.file_visitor,
                        callback_error: self.callback_error,
                        marker: PhantomData,
                    })?;
                    files = Some(());
                }
                "layout" => {
                    require_absent(&layout, "layout")?;
                    layout = Some(map.next_value()?);
                }
                "manifest_version" => {
                    require_absent(&manifest_version, "manifest_version")?;
                    manifest_version = Some(map.next_value()?);
                }
                "package_id" => {
                    require_absent(&package_id, "package_id")?;
                    package_id = Some(map.next_value()?);
                }
                "segments" => {
                    require_absent(&segments, "segments")?;
                    map.next_value_seed(EntrySequenceSeed::<SegmentEntry> {
                        visitor: self.segment_visitor,
                        callback_error: self.callback_error,
                        marker: PhantomData,
                    })?;
                    segments = Some(());
                }
                unknown => return Err(de::Error::unknown_field(unknown, IDENTITY_FIELDS)),
            }
        }

        required(files, "files")?;
        required(segments, "segments")?;
        Ok(ManifestIdentityHeader {
            manifest_version: required(manifest_version, "manifest_version")?,
            package_id: required(package_id, "package_id")?,
            layout: required(layout, "layout")?,
        })
    }
}

struct EntrySequenceSeed<'a, T> {
    visitor: &'a mut dyn FnMut(T) -> Result<()>,
    callback_error: &'a mut Option<CdfError>,
    marker: PhantomData<T>,
}

impl<'de, T> DeserializeSeed<'de> for EntrySequenceSeed<'_, T>
where
    T: serde::Deserialize<'de>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(EntrySequenceVisitor {
            visitor: self.visitor,
            callback_error: self.callback_error,
        })
    }
}

struct EntrySequenceVisitor<'a, T> {
    visitor: &'a mut dyn FnMut(T) -> Result<()>,
    callback_error: &'a mut Option<CdfError>,
}

impl<'de, T> Visitor<'de> for EntrySequenceVisitor<'_, T>
where
    T: serde::Deserialize<'de>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical manifest entry array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(entry) = sequence.next_element()? {
            if let Err(error) = (self.visitor)(entry) {
                *self.callback_error = Some(error);
                return Err(de::Error::custom("package manifest visitor stopped"));
            }
        }
        Ok(())
    }
}

fn require_absent<E, T>(value: &Option<T>, field: &'static str) -> std::result::Result<(), E>
where
    E: de::Error,
{
    if value.is_some() {
        return Err(de::Error::duplicate_field(field));
    }
    Ok(())
}

fn required<E, T>(value: Option<T>, field: &'static str) -> std::result::Result<T, E>
where
    E: de::Error,
{
    value.ok_or_else(|| de::Error::missing_field(field))
}

const MANIFEST_FIELDS: &[&str] = &[
    "archives",
    "identity",
    "lifecycle",
    "manifest_version",
    "package_hash",
    "signature",
];
const IDENTITY_FIELDS: &[&str] = &[
    "files",
    "layout",
    "manifest_version",
    "package_id",
    "segments",
];

const FILES_ARRAY_PREFIX: &[u8] = b"\"identity\":{\"files\":[";
const IDENTITY_ARRAY_ANCHOR: &[u8] = FILES_ARRAY_PREFIX;
const SEGMENTS_ARRAY_PREFIX: &[u8] = b"\"segments\":[";
const MAX_CANONICAL_FILE_ENTRY_BYTES: usize = 1024 * 1024;
const MAX_CANONICAL_SEGMENT_ENTRY_BYTES: usize = 4096;

struct CanonicalManifestArrayStream<R, T> {
    reader: BufReader<R>,
    entry: Vec<u8>,
    anchor: Option<&'static [u8]>,
    prefix: &'static [u8],
    entry_name: &'static str,
    maximum_entry_bytes: usize,
    started: bool,
    finished: bool,
    expect_comma: bool,
    marker: PhantomData<T>,
}

impl<R: Read, T> CanonicalManifestArrayStream<R, T>
where
    T: serde::de::DeserializeOwned,
{
    fn new(
        reader: R,
        anchor: Option<&'static [u8]>,
        prefix: &'static [u8],
        entry_name: &'static str,
        maximum_entry_bytes: usize,
    ) -> Self {
        Self {
            reader: BufReader::new(reader),
            entry: Vec::with_capacity(512),
            anchor,
            prefix,
            entry_name,
            maximum_entry_bytes,
            started: false,
            finished: false,
            expect_comma: false,
            marker: PhantomData,
        }
    }

    fn locate_array(&mut self) -> Result<()> {
        if let Some(anchor) = self.anchor {
            locate_token(&mut self.reader, anchor, "identity")?;
        }
        locate_token(&mut self.reader, self.prefix, self.entry_name)?;
        self.started = true;
        Ok(())
    }

    fn next_entry(&mut self) -> Result<Option<T>> {
        if self.finished {
            return Ok(None);
        }
        if !self.started {
            self.locate_array()?;
        }

        let first = loop {
            let byte = read_byte(&mut self.reader)?.ok_or_else(|| {
                CdfError::data(format!(
                    "package manifest {} array ended before `]`",
                    self.entry_name
                ))
            })?;
            if byte.is_ascii_whitespace() {
                continue;
            }
            if self.expect_comma {
                if byte == b']' {
                    self.finished = true;
                    return Ok(None);
                }
                if byte != b',' {
                    return Err(CdfError::data(format!(
                        "package manifest {} entries require canonical comma separation",
                        self.entry_name
                    )));
                }
                self.expect_comma = false;
                continue;
            }
            break byte;
        };
        if first == b']' {
            self.finished = true;
            return Ok(None);
        }
        if first != b'{' {
            return Err(CdfError::data(format!(
                "package manifest {} entry must be a JSON object",
                self.entry_name
            )));
        }

        self.entry.clear();
        self.entry.push(first);
        let mut depth = 1_u32;
        let mut in_string = false;
        let mut escaped = false;
        while depth > 0 {
            let byte = read_byte(&mut self.reader)?.ok_or_else(|| {
                CdfError::data(format!(
                    "package manifest {} entry ended before its closing object",
                    self.entry_name
                ))
            })?;
            if self.entry.len() == self.maximum_entry_bytes {
                return Err(CdfError::data(format!(
                    "package manifest {} entry exceeds the current-format structural ceiling of {} bytes",
                    self.entry_name, self.maximum_entry_bytes
                )));
            }
            self.entry.push(byte);
            if in_string {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_string = false;
                }
                continue;
            }
            match byte {
                b'"' => in_string = true,
                b'{' => {
                    depth = depth
                        .checked_add(1)
                        .ok_or_else(|| CdfError::data("manifest JSON nesting overflowed u32"))?;
                }
                b'}' => depth -= 1,
                _ => {}
            }
        }
        self.expect_comma = true;
        serde_json::from_slice(&self.entry)
            .map(Some)
            .map_err(json_error)
    }
}

fn locate_token(reader: &mut impl Read, token: &[u8], label: &str) -> Result<()> {
    let mut matched = 0;
    let mut preceding_backslashes = 0_usize;
    loop {
        let byte = read_byte(reader)?.ok_or_else(|| {
            CdfError::data(format!(
                "package manifest omitted its canonical {label} array"
            ))
        })?;
        let unescaped = preceding_backslashes.is_multiple_of(2);
        if byte == token[matched] && (matched > 0 || unescaped) {
            matched += 1;
            if matched == token.len() {
                return Ok(());
            }
        } else {
            matched = usize::from(byte == token[0] && unescaped);
        }
        if byte == b'\\' {
            preceding_backslashes += 1;
        } else {
            preceding_backslashes = 0;
        }
    }
}

/// Pull-based reader over the canonical manifest file array.
///
/// One file record is retained at a time. The ceiling is an artifact safety bound for one path
/// record, not a file-count, throughput, or concurrency limit.
pub struct ManifestFileStream<R>(CanonicalManifestArrayStream<R, FileEntry>);

impl<R: Read> ManifestFileStream<R> {
    pub fn new(reader: R) -> Self {
        Self(CanonicalManifestArrayStream::new(
            reader,
            None,
            FILES_ARRAY_PREFIX,
            "files",
            MAX_CANONICAL_FILE_ENTRY_BYTES,
        ))
    }
}

impl<R: Read> Iterator for ManifestFileStream<R> {
    type Item = Result<FileEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_entry().transpose()
    }
}

/// Pull-based reader over the canonical manifest segment array.
///
/// CDF-generated segment identifiers are package path components, so one canonical record has a
/// fixed structural ceiling. This is an artifact safety bound, not a throughput/concurrency cap.
pub struct ManifestSegmentStream<R>(CanonicalManifestArrayStream<R, SegmentEntry>);

impl<R: Read> ManifestSegmentStream<R> {
    pub fn new(reader: R) -> Self {
        Self(CanonicalManifestArrayStream::new(
            reader,
            Some(IDENTITY_ARRAY_ANCHOR),
            SEGMENTS_ARRAY_PREFIX,
            "segments",
            MAX_CANONICAL_SEGMENT_ENTRY_BYTES,
        ))
    }
}

impl<R: Read> Iterator for ManifestSegmentStream<R> {
    type Item = Result<SegmentEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_entry().transpose()
    }
}

fn read_byte(reader: &mut impl Read) -> Result<Option<u8>> {
    let mut byte = [0_u8; 1];
    match reader.read(&mut byte) {
        Ok(0) => Ok(None),
        Ok(1) => Ok(Some(byte[0])),
        Ok(_) => unreachable!("one-byte buffer cannot read more than one byte"),
        Err(error) => Err(CdfError::internal(format!(
            "read package manifest: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use cdf_kernel::SegmentId;
    use cdf_package_contract::{
        LifecycleState, ManifestArchives, ManifestIdentity, PackageManifest, PackageStatus,
        SignatureSlot,
    };

    use super::*;
    use crate::json::{manifest_identity_hash, write_package_manifest_canonical};

    fn fixture() -> PackageManifest {
        PackageManifest {
            manifest_version: MANIFEST_VERSION,
            package_hash: format!("sha256:{}", "a".repeat(64)),
            identity: ManifestIdentity {
                manifest_version: MANIFEST_VERSION,
                package_id: "pkg-stream".to_owned(),
                layout: vec!["data/".to_owned()],
                files: vec![
                    FileEntry {
                        path: "data/000.arrow".to_owned(),
                        byte_count: 10,
                        sha256: "1".repeat(64),
                    },
                    FileEntry {
                        path: "trace.jsonl".to_owned(),
                        byte_count: 20,
                        sha256: "2".repeat(64),
                    },
                ],
                segments: vec![SegmentEntry {
                    segment_id: SegmentId::new("segment-00000000000000000000").unwrap(),
                    path: "data/000.arrow".to_owned(),
                    package_row_ord_start: 0,
                    row_count: 3,
                    byte_count: 10,
                    sha256: "1".repeat(64),
                }],
            },
            lifecycle: LifecycleState {
                status: PackageStatus::Packaged,
            },
            signature: SignatureSlot {
                signing_input: format!("sha256:{}", "a".repeat(64)),
                value: None,
            },
            archives: None,
        }
    }

    #[test]
    fn manifest_parser_visits_cardinality_entries_without_storing_them_in_header() {
        let manifest = fixture();
        let mut bytes = Vec::new();
        write_package_manifest_canonical(&manifest, &mut bytes).unwrap();
        let mut files = Vec::new();
        let mut segments = Vec::new();
        let header = visit_package_manifest(
            Cursor::new(bytes),
            &mut |entry| {
                files.push(entry);
                Ok(())
            },
            &mut |entry| {
                segments.push(entry);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(header.manifest_version, manifest.manifest_version);
        assert_eq!(header.package_hash, manifest.package_hash);
        assert_eq!(header.identity.package_id, manifest.identity.package_id);
        assert_eq!(header.identity.layout, manifest.identity.layout);
        assert_eq!(header.lifecycle, manifest.lifecycle);
        assert_eq!(header.signature, manifest.signature);
        assert!(!header.has_archives);
        assert_eq!(files, manifest.identity.files);
        assert_eq!(segments, manifest.identity.segments);
    }

    #[test]
    fn stored_identity_hash_ignores_optional_archive_metadata() {
        let mut manifest = fixture();
        let expected = manifest_identity_hash(&manifest.identity).unwrap();
        for archives in [None, Some(ManifestArchives { parquet: None })] {
            manifest.archives = archives;
            let mut bytes = Vec::new();
            write_package_manifest_canonical(&manifest, &mut bytes).unwrap();
            assert_eq!(
                stored_manifest_identity_hash(Cursor::new(bytes)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn manifest_parser_stops_on_entry_consumer_failure() {
        let manifest = fixture();
        let mut bytes = Vec::new();
        write_package_manifest_canonical(&manifest, &mut bytes).unwrap();
        let mut visited = 0;
        let error = visit_package_manifest(
            Cursor::new(bytes),
            &mut |_| {
                visited += 1;
                Err(CdfError::data("stop manifest visit"))
            },
            &mut |_| Ok(()),
        )
        .unwrap_err();

        assert_eq!(visited, 1);
        assert!(error.to_string().contains("stop manifest visit"));
    }

    #[test]
    fn manifest_segment_stream_yields_only_the_canonical_array() {
        let mut manifest = fixture();
        manifest.identity.package_id = "escaped-\"segments\":[-decoy".to_owned();
        let mut bytes = Vec::new();
        write_package_manifest_canonical(&manifest, &mut bytes).unwrap();

        let segments = ManifestSegmentStream::new(Cursor::new(bytes))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(segments, manifest.identity.segments);
    }

    #[test]
    fn manifest_file_stream_stops_after_the_canonical_file_array() {
        let manifest = fixture();
        let mut bytes = Vec::new();
        write_package_manifest_canonical(&manifest, &mut bytes).unwrap();

        let files = ManifestFileStream::new(Cursor::new(bytes))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(files, manifest.identity.files);
    }

    #[test]
    fn manifest_segment_stream_rejects_records_above_the_artifact_ceiling() {
        let mut manifest = fixture();
        manifest.identity.segments[0].segment_id =
            SegmentId::new("s".repeat(MAX_CANONICAL_SEGMENT_ENTRY_BYTES)).unwrap();
        let mut bytes = Vec::new();
        write_package_manifest_canonical(&manifest, &mut bytes).unwrap();

        let error = ManifestSegmentStream::new(Cursor::new(bytes))
            .next()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("structural ceiling"));
    }
}
