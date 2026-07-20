use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use cdf_kernel::{
    CdfError, Result, SourcePosition, SourceReplayRetention, SourceReplayRetentionStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{SpillBudgetCoordinator, SpillReservation};

const ROLLING_REPLAY_MANIFEST_VERSION: u16 = 1;
const MANIFEST_FILE: &str = "manifest.json";
const MANIFEST_TEMP_FILE: &str = ".manifest.json.tmp";

/// Explicit retention knobs for one non-pausable source replay window.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollingReplayLimits {
    pub maximum_bytes: u64,
    pub maximum_age_milliseconds: u64,
    pub maximum_units: u64,
}

impl RollingReplayLimits {
    pub fn validate(self) -> Result<()> {
        if self.maximum_bytes == 0 || self.maximum_age_milliseconds == 0 || self.maximum_units == 0
        {
            return Err(CdfError::contract(
                "rolling replay limits require nonzero byte, age, and unit-count bounds",
            ));
        }
        Ok(())
    }
}

/// One source-encoded replay unit retained below the uncommitted high frontier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RollingReplayUnit {
    pub ordinal: u64,
    pub position: SourcePosition,
    pub observed_at_unix_milliseconds: u64,
    pub byte_count: u64,
    pub content_sha256: String,
    pub path: PathBuf,
}

/// Runtime-owned disk authority for opaque, source-encoded replay units.
///
/// The source owns its wire/batch encoding. This store owns bounded disk accounting, crash-safe
/// publication, and checkpoint-gated eviction. A committed low watermark must exactly match a
/// retained unit; the store never guesses source ordering semantics.
pub struct RollingReplayStore {
    root: PathBuf,
    spill: Arc<dyn SpillBudgetCoordinator>,
    state: Mutex<RollingReplayState>,
}

struct RollingReplayState {
    manifest: RollingReplayManifest,
    reservation: Option<SpillReservation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RollingReplayManifest {
    version: u16,
    limits: RollingReplayLimits,
    next_ordinal: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    committed_low_watermark: Option<SourcePosition>,
    entries: Vec<RollingReplayManifestEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    retired: Vec<RollingReplayManifestEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RollingReplayManifestEntry {
    ordinal: u64,
    position: SourcePosition,
    observed_at_unix_milliseconds: u64,
    byte_count: u64,
    content_sha256: String,
    file_name: String,
}

impl RollingReplayStore {
    pub fn create(
        root: impl AsRef<Path>,
        limits: RollingReplayLimits,
        spill: Arc<dyn SpillBudgetCoordinator>,
    ) -> Result<Self> {
        limits.validate()?;
        let root = root.as_ref().to_path_buf();
        create_private_directory(&root)?;
        let manifest_path = root.join(MANIFEST_FILE);
        if manifest_path.exists() {
            return Err(CdfError::contract(format!(
                "rolling replay store {} already exists; recover it instead of replacing retained source positions",
                root.display()
            )));
        }
        refuse_nonempty_uninitialized_directory(&root)?;
        let store = Self {
            root,
            spill,
            state: Mutex::new(RollingReplayState {
                manifest: RollingReplayManifest {
                    version: ROLLING_REPLAY_MANIFEST_VERSION,
                    limits,
                    next_ordinal: 0,
                    committed_low_watermark: None,
                    entries: Vec::new(),
                    retired: Vec::new(),
                },
                reservation: None,
            }),
        };
        {
            let state = store.lock_state()?;
            store.persist_manifest(&state)?;
        }
        Ok(store)
    }

    pub fn recover(
        root: impl AsRef<Path>,
        expected_limits: RollingReplayLimits,
        spill: Arc<dyn SpillBudgetCoordinator>,
    ) -> Result<Self> {
        expected_limits.validate()?;
        let root = root.as_ref().to_path_buf();
        validate_private_directory(&root)?;
        remove_manifest_temp(&root)?;
        let manifest_path = root.join(MANIFEST_FILE);
        validate_regular_file(&manifest_path, "rolling replay manifest")?;
        let bytes = fs::read(&manifest_path).map_err(|error| {
            CdfError::internal(format!(
                "failed to read rolling replay manifest {}: {error}",
                manifest_path.display()
            ))
        })?;
        let mut manifest: RollingReplayManifest =
            serde_json::from_slice(&bytes).map_err(|error| {
                CdfError::data(format!(
                    "rolling replay manifest {} is invalid: {error}",
                    manifest_path.display()
                ))
            })?;
        validate_manifest(&manifest, expected_limits)?;

        let live_bytes = verify_manifest_entries(&root, &manifest.entries, true)?;
        manifest
            .retired
            .retain(|entry| root.join(&entry.file_name).exists());
        let retired_bytes = verify_manifest_entries(&root, &manifest.retired, false)?;
        let retained_bytes = live_bytes
            .checked_add(retired_bytes)
            .ok_or_else(|| CdfError::data("rolling replay recovery byte count overflow"))?;
        let reservation = reserve_initial(&spill, retained_bytes)?;
        let store = Self {
            root,
            spill,
            state: Mutex::new(RollingReplayState {
                manifest: manifest.clone(),
                reservation,
            }),
        };
        {
            let mut state = store.lock_state()?;
            cleanup_retired(&store.root, &mut state)?;
            manifest = state.manifest.clone();
            store.persist_manifest(&state)?;
        }
        cleanup_unreferenced_entry_files(&store.root, &manifest)?;
        Ok(store)
    }

    pub fn append(
        &self,
        position: SourcePosition,
        observed_at_unix_milliseconds: u64,
        payload: &[u8],
    ) -> Result<RollingReplayUnit> {
        position.validate()?;
        if observed_at_unix_milliseconds == 0 || payload.is_empty() {
            return Err(CdfError::contract(
                "rolling replay append requires a positive observation time and nonempty encoded payload",
            ));
        }
        let byte_count =
            u64::try_from(payload.len()).map_err(|error| CdfError::internal(error.to_string()))?;
        let mut state = self.lock_state()?;
        validate_append_order(&state.manifest, &position, observed_at_unix_milliseconds)?;
        enforce_age_bound(&state.manifest, observed_at_unix_milliseconds)?;
        let retained = reserved_bytes(&state);
        let retained_units = u64::try_from(
            state
                .manifest
                .entries
                .len()
                .saturating_add(state.manifest.retired.len()),
        )
        .map_err(|error| CdfError::internal(error.to_string()))?;
        if retained_units >= state.manifest.limits.maximum_units {
            return Err(replay_capacity_error(&state.manifest));
        }
        let next = retained
            .checked_add(byte_count)
            .ok_or_else(|| CdfError::data("rolling replay byte count overflow"))?;
        if next > state.manifest.limits.maximum_bytes {
            return Err(replay_capacity_error(&state.manifest));
        }
        grow_reservation(&self.spill, &mut state.reservation, byte_count)?;

        let ordinal = state.manifest.next_ordinal;
        let file_name = format!("entry-{ordinal:020}.bin");
        let final_path = self.root.join(&file_name);
        let temp_path = self.root.join(format!(".{file_name}.tmp"));
        let content_sha256 = format!("sha256:{:x}", Sha256::digest(payload));
        if let Err(error) = write_atomic_payload(&temp_path, &final_path, payload) {
            shrink_reservation(&mut state.reservation, byte_count);
            return Err(error);
        }
        let entry = RollingReplayManifestEntry {
            ordinal,
            position: position.clone(),
            observed_at_unix_milliseconds,
            byte_count,
            content_sha256: content_sha256.clone(),
            file_name,
        };
        state.manifest.next_ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("rolling replay ordinal overflow"))?;
        state.manifest.entries.push(entry.clone());
        if let Err(error) = self.persist_manifest(&state) {
            state.manifest.entries.pop();
            state.manifest.next_ordinal = ordinal;
            // The payload may already be durable. Only release its reservation if cleanup proves
            // the bytes are gone; ambiguous cleanup retains capacity conservatively.
            if fs::remove_file(&final_path).is_ok() {
                shrink_reservation(&mut state.reservation, byte_count);
                let _ = sync_directory(&self.root);
            }
            return Err(error);
        }
        Ok(unit_from_entry(&self.root, &entry))
    }

    pub fn replay_units(&self) -> Result<Vec<RollingReplayUnit>> {
        let state = self.lock_state()?;
        Ok(state
            .manifest
            .entries
            .iter()
            .map(|entry| unit_from_entry(&self.root, entry))
            .collect())
    }

    pub fn committed_low_watermark(&self) -> Result<Option<SourcePosition>> {
        Ok(self.lock_state()?.manifest.committed_low_watermark.clone())
    }

    /// Commits and evicts the exact retained prefix through `frontier`.
    pub fn commit_low_watermark(&self, frontier: &SourcePosition) -> Result<()> {
        frontier.validate()?;
        let mut state = self.lock_state()?;
        if state.manifest.committed_low_watermark.as_ref() == Some(frontier) {
            return Ok(());
        }
        let index = state
            .manifest
            .entries
            .iter()
            .position(|entry| &entry.position == frontier)
            .ok_or_else(|| {
                CdfError::data(
                    "rolling replay checkpoint frontier is not an exact retained source position; retain the source unit until its receipt-verified checkpoint commits",
                )
            })?;
        let previous = state.manifest.clone();
        let retired = state.manifest.entries.drain(..=index).collect::<Vec<_>>();
        state.manifest.retired.extend(retired);
        state.manifest.committed_low_watermark = Some(frontier.clone());
        if let Err(error) = self.persist_manifest(&state) {
            state.manifest = previous;
            return Err(error);
        }
        cleanup_retired(&self.root, &mut state)?;
        self.persist_manifest(&state)?;
        Ok(())
    }

    pub fn validate_age(&self, now_unix_milliseconds: u64) -> Result<()> {
        if now_unix_milliseconds == 0 {
            return Err(CdfError::contract(
                "rolling replay age validation requires a positive current time",
            ));
        }
        enforce_age_bound(&self.lock_state()?.manifest, now_unix_milliseconds)
    }

    pub fn retained_bytes(&self) -> Result<u64> {
        let state = self.lock_state()?;
        Ok(reserved_bytes(&state))
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, RollingReplayState>> {
        self.state
            .lock()
            .map_err(|_| CdfError::internal("rolling replay store lock is poisoned"))
    }

    fn persist_manifest(&self, state: &RollingReplayState) -> Result<()> {
        let temp = self.root.join(MANIFEST_TEMP_FILE);
        let final_path = self.root.join(MANIFEST_FILE);
        match fs::remove_file(&temp) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(CdfError::internal(format!(
                    "failed to remove stale rolling replay manifest temporary file {}: {error}",
                    temp.display()
                )));
            }
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(|error| {
                CdfError::internal(format!(
                    "failed to open rolling replay manifest temporary file {}: {error}",
                    temp.display()
                ))
            })?;
        serde_json::to_writer(&mut file, &state.manifest)
            .map_err(|error| CdfError::internal(error.to_string()))?;
        file.sync_all().map_err(|error| {
            CdfError::internal(format!(
                "failed to persist rolling replay manifest {}: {error}",
                temp.display()
            ))
        })?;
        fs::rename(&temp, &final_path).map_err(|error| {
            CdfError::internal(format!(
                "failed to publish rolling replay manifest {}: {error}",
                final_path.display()
            ))
        })?;
        sync_directory(&self.root)
    }
}

impl SourceReplayRetention for RollingReplayStore {
    fn status(&self) -> Result<SourceReplayRetentionStatus> {
        let state = self.lock_state()?;
        let status = SourceReplayRetentionStatus {
            maximum_bytes: state.manifest.limits.maximum_bytes,
            maximum_age_milliseconds: state.manifest.limits.maximum_age_milliseconds,
            maximum_units: state.manifest.limits.maximum_units,
            retained_bytes: reserved_bytes(&state),
            retained_units: u64::try_from(
                state
                    .manifest
                    .entries
                    .len()
                    .saturating_add(state.manifest.retired.len()),
            )
            .map_err(|error| CdfError::internal(error.to_string()))?,
            committed_low_watermark: state.manifest.committed_low_watermark.clone(),
        };
        status.validate()?;
        Ok(status)
    }

    fn commit_checkpoint_frontier(&self, frontier: &SourcePosition) -> Result<()> {
        self.commit_low_watermark(frontier)
    }
}

fn validate_manifest(
    manifest: &RollingReplayManifest,
    expected_limits: RollingReplayLimits,
) -> Result<()> {
    if manifest.version != ROLLING_REPLAY_MANIFEST_VERSION || manifest.limits != expected_limits {
        return Err(CdfError::data(
            "rolling replay manifest version or configured byte/time limits do not match recovery authority",
        ));
    }
    expected_limits.validate()?;
    if let Some(frontier) = &manifest.committed_low_watermark {
        frontier.validate()?;
    }
    validate_manifest_entry_sequence(&manifest.entries)?;
    validate_manifest_entry_sequence(&manifest.retired)?;
    let mut file_names = std::collections::BTreeSet::new();
    let mut maximum_ordinal = None;
    for entry in manifest.entries.iter().chain(&manifest.retired) {
        entry.position.validate()?;
        validate_entry_file_name(&entry.file_name, entry.ordinal)?;
        crate::validate_artifact_hash("rolling replay entry", &entry.content_sha256)?;
        if entry.byte_count == 0 || entry.observed_at_unix_milliseconds == 0 {
            return Err(CdfError::data(
                "rolling replay manifest entries require nonzero bytes and observation times",
            ));
        }
        if !file_names.insert(entry.file_name.as_str()) {
            return Err(CdfError::data(
                "rolling replay manifest contains duplicate entry identities",
            ));
        }
        maximum_ordinal =
            Some(maximum_ordinal.map_or(entry.ordinal, |maximum: u64| maximum.max(entry.ordinal)));
    }
    if maximum_ordinal.is_some_and(|ordinal| manifest.next_ordinal <= ordinal) {
        return Err(CdfError::data(
            "rolling replay manifest next ordinal does not follow retained entries",
        ));
    }
    Ok(())
}

fn validate_manifest_entry_sequence(entries: &[RollingReplayManifestEntry]) -> Result<()> {
    let mut previous_ordinal = None;
    let mut previous_time = None;
    for entry in entries {
        if previous_ordinal.is_some_and(|previous| entry.ordinal <= previous)
            || previous_time.is_some_and(|previous| entry.observed_at_unix_milliseconds < previous)
        {
            return Err(CdfError::data(
                "rolling replay manifest entries are not in strict ordinal and monotone time order",
            ));
        }
        previous_ordinal = Some(entry.ordinal);
        previous_time = Some(entry.observed_at_unix_milliseconds);
    }
    Ok(())
}

fn validate_append_order(
    manifest: &RollingReplayManifest,
    position: &SourcePosition,
    observed_at_unix_milliseconds: u64,
) -> Result<()> {
    if manifest
        .entries
        .last()
        .is_some_and(|entry| entry.position == *position)
    {
        return Err(CdfError::data(
            "rolling replay source position was appended twice",
        ));
    }
    if manifest
        .entries
        .last()
        .is_some_and(|entry| entry.observed_at_unix_milliseconds > observed_at_unix_milliseconds)
    {
        return Err(CdfError::data(
            "rolling replay observation time moved backwards",
        ));
    }
    Ok(())
}

fn enforce_age_bound(manifest: &RollingReplayManifest, now: u64) -> Result<()> {
    let Some(oldest) = manifest.entries.first() else {
        return Ok(());
    };
    let age = now
        .checked_sub(oldest.observed_at_unix_milliseconds)
        .ok_or_else(|| CdfError::data("rolling replay observation time moved backwards"))?;
    if age > manifest.limits.maximum_age_milliseconds {
        return Err(CdfError::data(format!(
            "rolling replay retention reached its configured {} ms age bound with {} uncommitted bytes; increase the replay-retention age/disk knobs, speed up destination settlement, or use a pausable source",
            manifest.limits.maximum_age_milliseconds,
            manifest
                .entries
                .iter()
                .map(|entry| entry.byte_count)
                .sum::<u64>()
        )));
    }
    Ok(())
}

fn replay_capacity_error(manifest: &RollingReplayManifest) -> CdfError {
    CdfError::data(format!(
        "rolling replay retention reached its configured {} byte or {} unit bound; increase the replay-retention/disk-budget/unit knobs, increase source replay-unit size, speed up destination settlement, or use a pausable source",
        manifest.limits.maximum_bytes, manifest.limits.maximum_units
    ))
}

fn reserve_initial(
    spill: &Arc<dyn SpillBudgetCoordinator>,
    bytes: u64,
) -> Result<Option<SpillReservation>> {
    if bytes == 0 {
        return Ok(None);
    }
    spill.try_reserve(bytes)?.map(Some).ok_or_else(|| {
        CdfError::data(format!(
            "rolling replay recovery requires {bytes} bytes but the configured shared disk budget cannot admit them"
        ))
    })
}

fn grow_reservation(
    spill: &Arc<dyn SpillBudgetCoordinator>,
    reservation: &mut Option<SpillReservation>,
    bytes: u64,
) -> Result<()> {
    match reservation {
        Some(reservation) => {
            if reservation.try_grow(bytes)? {
                Ok(())
            } else {
                Err(CdfError::data(format!(
                    "rolling replay append requires {bytes} bytes but the configured shared disk budget is exhausted"
                )))
            }
        }
        None => {
            *reservation = spill.try_reserve(bytes)?;
            if reservation.is_none() {
                return Err(CdfError::data(format!(
                    "rolling replay append requires {bytes} bytes but the configured shared disk budget is exhausted"
                )));
            }
            Ok(())
        }
    }
}

fn shrink_reservation(reservation: &mut Option<SpillReservation>, bytes: u64) {
    let clear = if let Some(active) = reservation.as_mut() {
        active.shrink(bytes);
        active.bytes() == 0
    } else {
        false
    };
    if clear {
        *reservation = None;
    }
}

fn reserved_bytes(state: &RollingReplayState) -> u64 {
    state
        .reservation
        .as_ref()
        .map_or(0, SpillReservation::bytes)
}

fn cleanup_retired(root: &Path, state: &mut RollingReplayState) -> Result<()> {
    let retired = std::mem::take(&mut state.manifest.retired);
    let mut remaining = Vec::new();
    for entry in retired {
        let path = root.join(&entry.file_name);
        match fs::remove_file(&path) {
            Ok(()) => shrink_reservation(&mut state.reservation, entry.byte_count),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                shrink_reservation(&mut state.reservation, entry.byte_count);
            }
            Err(_) => remaining.push(entry),
        }
    }
    state.manifest.retired = remaining;
    sync_directory(root)
}

fn verify_manifest_entries(
    root: &Path,
    entries: &[RollingReplayManifestEntry],
    required: bool,
) -> Result<u64> {
    let mut total = 0_u64;
    for entry in entries {
        let path = root.join(&entry.file_name);
        if !path.exists() && !required {
            continue;
        }
        validate_regular_file(&path, "rolling replay entry")?;
        let mut file = File::open(&path).map_err(|error| {
            CdfError::internal(format!(
                "failed to read rolling replay entry {}: {error}",
                path.display()
            ))
        })?;
        let byte_count = file
            .metadata()
            .map_err(|error| CdfError::internal(error.to_string()))?
            .len();
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|error| CdfError::internal(error.to_string()))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        let sha256 = format!("sha256:{:x}", hasher.finalize());
        if byte_count != entry.byte_count || sha256 != entry.content_sha256 {
            return Err(CdfError::data(format!(
                "rolling replay entry {} does not match its recorded byte count and checksum",
                path.display()
            )));
        }
        total = total
            .checked_add(byte_count)
            .ok_or_else(|| CdfError::data("rolling replay recovery byte count overflow"))?;
    }
    Ok(total)
}

fn cleanup_unreferenced_entry_files(root: &Path, manifest: &RollingReplayManifest) -> Result<()> {
    let referenced = manifest
        .entries
        .iter()
        .chain(&manifest.retired)
        .map(|entry| entry.file_name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for entry in fs::read_dir(root).map_err(|error| {
        CdfError::internal(format!(
            "failed to inspect rolling replay directory {}: {error}",
            root.display()
        ))
    })? {
        let entry = entry.map_err(|error| CdfError::internal(error.to_string()))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let orphan_payload = name.starts_with("entry-")
            && name.ends_with(".bin")
            && !referenced.contains(name.as_ref());
        let interrupted_payload = name.starts_with(".entry-") && name.ends_with(".bin.tmp");
        if orphan_payload || interrupted_payload {
            fs::remove_file(entry.path()).map_err(|error| {
                CdfError::internal(format!(
                    "failed to remove orphan rolling replay entry {}: {error}",
                    entry.path().display()
                ))
            })?;
        }
    }
    sync_directory(root)
}

fn unit_from_entry(root: &Path, entry: &RollingReplayManifestEntry) -> RollingReplayUnit {
    RollingReplayUnit {
        ordinal: entry.ordinal,
        position: entry.position.clone(),
        observed_at_unix_milliseconds: entry.observed_at_unix_milliseconds,
        byte_count: entry.byte_count,
        content_sha256: entry.content_sha256.clone(),
        path: root.join(&entry.file_name),
    }
}

fn validate_entry_file_name(file_name: &str, ordinal: u64) -> Result<()> {
    if file_name != format!("entry-{ordinal:020}.bin") {
        return Err(CdfError::data(
            "rolling replay manifest contains a noncanonical entry file name",
        ));
    }
    Ok(())
}

fn write_atomic_payload(temp: &Path, final_path: &Path, payload: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp)
        .map_err(|error| {
            CdfError::internal(format!(
                "failed to create rolling replay temporary payload {}: {error}",
                temp.display()
            ))
        })?;
    if let Err(error) = file.write_all(payload).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = fs::remove_file(temp);
        return Err(CdfError::internal(format!(
            "failed to persist rolling replay payload {}: {error}",
            temp.display()
        )));
    }
    drop(file);
    if let Err(error) = fs::rename(temp, final_path) {
        let _ = fs::remove_file(temp);
        return Err(CdfError::internal(format!(
            "failed to publish rolling replay payload {}: {error}",
            final_path.display()
        )));
    }
    Ok(())
}

fn refuse_nonempty_uninitialized_directory(root: &Path) -> Result<()> {
    let mut entries = fs::read_dir(root).map_err(|error| {
        CdfError::internal(format!(
            "failed to inspect rolling replay directory {}: {error}",
            root.display()
        ))
    })?;
    if entries
        .next()
        .transpose()
        .map_err(|error| CdfError::internal(error.to_string()))?
        .is_some()
    {
        return Err(CdfError::contract(format!(
            "rolling replay directory {} is not empty; use a dedicated empty directory or recover its manifest",
            root.display()
        )));
    }
    Ok(())
}

fn create_private_directory(root: &Path) -> Result<()> {
    fs::create_dir_all(root).map_err(|error| {
        CdfError::internal(format!(
            "failed to create rolling replay directory {}: {error}",
            root.display()
        ))
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(root, fs::Permissions::from_mode(0o700)).map_err(|error| {
            CdfError::internal(format!(
                "failed to secure rolling replay directory {}: {error}",
                root.display()
            ))
        })?;
    }
    validate_private_directory(root)
}

fn validate_private_directory(root: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(root).map_err(|error| {
        CdfError::internal(format!(
            "failed to inspect rolling replay directory {}: {error}",
            root.display()
        ))
    })?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(CdfError::data(
            "rolling replay root must be a real private directory, not a symlink",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(CdfError::data(
                "rolling replay root permissions must exclude group and world access",
            ));
        }
    }
    Ok(())
}

fn validate_regular_file(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        CdfError::data(format!(
            "{label} {} is unavailable: {error}",
            path.display()
        ))
    })?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(CdfError::data(format!(
            "{label} {} must be a regular file, not a symlink",
            path.display()
        )));
    }
    Ok(())
}

fn remove_manifest_temp(root: &Path) -> Result<()> {
    let temp = root.join(MANIFEST_TEMP_FILE);
    match fs::remove_file(&temp) {
        Ok(()) => sync_directory(root),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(CdfError::internal(format!(
            "failed to remove interrupted rolling replay manifest {}: {error}",
            temp.display()
        ))),
    }
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            CdfError::internal(format!(
                "failed to sync rolling replay directory {}: {error}",
                path.display()
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_kernel::{CursorPosition, CursorValue, SOURCE_POSITION_VERSION};

    fn position(value: i64) -> SourcePosition {
        SourcePosition::Cursor(CursorPosition {
            version: SOURCE_POSITION_VERSION,
            field: "offset".to_owned(),
            value: CursorValue::I64(value),
        })
    }

    fn limits() -> RollingReplayLimits {
        RollingReplayLimits {
            maximum_bytes: 8,
            maximum_age_milliseconds: 100,
            maximum_units: 3,
        }
    }

    #[test]
    fn byte_and_age_limits_fail_before_unbounded_growth() {
        let temp = tempfile::tempdir().unwrap();
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(crate::FixedSpillBudget::new(64).unwrap());
        let store = RollingReplayStore::create(temp.path(), limits(), spill.clone()).unwrap();
        store.append(position(1), 100, b"1234").unwrap();
        store.append(position(2), 150, b"5678").unwrap();
        let capacity = store.append(position(3), 160, b"9").unwrap_err();
        assert!(capacity.message.contains("8 byte or 3 unit bound"));
        let age = store.validate_age(201).unwrap_err();
        assert!(age.message.contains("configured 100 ms age bound"));
        assert_eq!(store.retained_bytes().unwrap(), 8);
        assert_eq!(spill.snapshot().current_bytes, 8);
    }

    #[test]
    fn checkpoint_low_watermark_atomically_evicts_only_its_exact_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(crate::FixedSpillBudget::new(64).unwrap());
        let store = RollingReplayStore::create(temp.path(), limits(), spill.clone()).unwrap();
        store.append(position(1), 100, b"11").unwrap();
        store.append(position(2), 110, b"22").unwrap();
        store.append(position(3), 120, b"33").unwrap();

        assert!(store.commit_low_watermark(&position(4)).is_err());
        store.commit_low_watermark(&position(2)).unwrap();
        assert_eq!(store.committed_low_watermark().unwrap(), Some(position(2)));
        assert_eq!(
            store
                .replay_units()
                .unwrap()
                .into_iter()
                .map(|unit| unit.position)
                .collect::<Vec<_>>(),
            vec![position(3)]
        );
        assert_eq!(store.retained_bytes().unwrap(), 2);
        assert_eq!(spill.snapshot().current_bytes, 2);
    }

    #[test]
    fn recovery_revalidates_payloads_and_reacquires_shared_budget() {
        let temp = tempfile::tempdir().unwrap();
        let first_spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(crate::FixedSpillBudget::new(64).unwrap());
        {
            let store = RollingReplayStore::create(temp.path(), limits(), first_spill).unwrap();
            store.append(position(1), 100, b"one").unwrap();
            store.append(position(2), 110, b"two").unwrap();
        }
        let recovered_spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(crate::FixedSpillBudget::new(64).unwrap());
        let recovered =
            RollingReplayStore::recover(temp.path(), limits(), recovered_spill.clone()).unwrap();
        assert_eq!(recovered.replay_units().unwrap().len(), 2);
        assert_eq!(recovered.retained_bytes().unwrap(), 6);
        assert_eq!(recovered_spill.snapshot().current_bytes, 6);
        drop(recovered);
        assert_eq!(recovered_spill.snapshot().current_bytes, 0);

        fs::write(temp.path().join("entry-00000000000000000000.bin"), b"bad").unwrap();
        let corrupt_spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(crate::FixedSpillBudget::new(64).unwrap());
        let error = match RollingReplayStore::recover(temp.path(), limits(), corrupt_spill) {
            Ok(_) => panic!("corrupt replay payload must fail recovery"),
            Err(error) => error,
        };
        assert!(error.message.contains("checksum"));
    }

    #[test]
    fn recovery_completes_checkpoint_eviction_interrupted_after_manifest_publish() {
        let temp = tempfile::tempdir().unwrap();
        let first_spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(crate::FixedSpillBudget::new(64).unwrap());
        {
            let store = RollingReplayStore::create(temp.path(), limits(), first_spill).unwrap();
            store.append(position(1), 100, b"11").unwrap();
            store.append(position(2), 110, b"22").unwrap();
            store.append(position(3), 120, b"33").unwrap();
            let mut state = store.lock_state().unwrap();
            let retired = state.manifest.entries.drain(..2).collect::<Vec<_>>();
            state.manifest.retired.extend(retired);
            state.manifest.committed_low_watermark = Some(position(2));
            store.persist_manifest(&state).unwrap();
        }

        let recovered_spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(crate::FixedSpillBudget::new(64).unwrap());
        let recovered =
            RollingReplayStore::recover(temp.path(), limits(), recovered_spill.clone()).unwrap();
        assert_eq!(
            recovered.committed_low_watermark().unwrap(),
            Some(position(2))
        );
        assert_eq!(
            recovered
                .replay_units()
                .unwrap()
                .into_iter()
                .map(|unit| unit.position)
                .collect::<Vec<_>>(),
            vec![position(3)]
        );
        assert_eq!(recovered.retained_bytes().unwrap(), 2);
        assert_eq!(recovered_spill.snapshot().current_bytes, 2);
        assert!(!temp.path().join("entry-00000000000000000000.bin").exists());
        assert!(!temp.path().join("entry-00000000000000000001.bin").exists());
    }

    #[test]
    fn failed_checkpoint_manifest_publish_keeps_the_in_memory_frontier_uncommitted() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("active");
        let moved = parent.path().join("moved");
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(crate::FixedSpillBudget::new(64).unwrap());
        let store = RollingReplayStore::create(&root, limits(), spill).unwrap();
        store.append(position(1), 100, b"11").unwrap();
        store.append(position(2), 110, b"22").unwrap();
        fs::rename(&root, &moved).unwrap();

        assert!(store.commit_low_watermark(&position(1)).is_err());
        assert_eq!(store.committed_low_watermark().unwrap(), None);
        assert_eq!(store.replay_units().unwrap().len(), 2);

        fs::rename(&moved, &root).unwrap();
        store.commit_low_watermark(&position(1)).unwrap();
        assert_eq!(store.committed_low_watermark().unwrap(), Some(position(1)));
        assert_eq!(store.replay_units().unwrap().len(), 1);
    }

    #[test]
    fn unit_count_is_an_explicit_metadata_bound() {
        let temp = tempfile::tempdir().unwrap();
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(crate::FixedSpillBudget::new(64).unwrap());
        let limits = RollingReplayLimits {
            maximum_bytes: 64,
            maximum_age_milliseconds: 100,
            maximum_units: 2,
        };
        let store = RollingReplayStore::create(temp.path(), limits, spill).unwrap();
        store.append(position(1), 100, b"1").unwrap();
        store.append(position(2), 110, b"2").unwrap();
        let error = store.append(position(3), 120, b"3").unwrap_err();
        assert!(error.message.contains("64 byte or 2 unit bound"));
    }
}
