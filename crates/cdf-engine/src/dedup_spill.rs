use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use cdf_contract::DedupKeepProgram;
use cdf_kernel::{CdfError, Result};
use cdf_runtime::{SpillBudgetCoordinator, SpillReservation};

const DEFAULT_SORT_MEMORY_BYTES: usize = 8 * 1024 * 1024;
const MERGE_FAN_IN: u64 = 32;
const MAX_KEY_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DedupDecision {
    pub ordinal: u64,
    pub kept_ordinal: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DedupIndexSummary {
    pub input_rows: u64,
    pub output_rows: u64,
    pub duplicate_key_count: u64,
    pub dropped_row_count: u64,
    pub spill_bytes: u64,
}

pub(crate) struct ExternalDedupIndex {
    root: PathBuf,
    keys: Option<BufWriter<BudgetedFile>>,
    reservation: Arc<Mutex<SpillReservation>>,
    input_rows: u64,
    sort_memory_bytes: usize,
}

pub(crate) struct ExternalDedupDecisions {
    reader: DecisionReader,
    pub summary: DedupIndexSummary,
    _index: ExternalDedupIndex,
}

impl ExternalDedupIndex {
    pub fn create(root: impl AsRef<Path>, budget: Arc<dyn SpillBudgetCoordinator>) -> Result<Self> {
        Self::create_with_sort_memory(root, budget, DEFAULT_SORT_MEMORY_BYTES)
    }

    fn create_with_sort_memory(
        root: impl AsRef<Path>,
        budget: Arc<dyn SpillBudgetCoordinator>,
        sort_memory_bytes: usize,
    ) -> Result<Self> {
        if sort_memory_bytes == 0 {
            return Err(CdfError::contract(
                "external dedup sort memory must be nonzero",
            ));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir(&root).map_err(|error| io_error("create dedup scratch", &root, error))?;
        set_owner_only(&root)?;
        let reservation = budget.try_reserve(1)?.ok_or_else(|| {
            CdfError::data(format!(
                "dedup spill requires scratch bytes but the shared {}-byte spill budget is exhausted; increase the spill budget or reduce concurrent spill operators",
                budget.snapshot().budget_bytes
            ))
        })?;
        let reservation = Arc::new(Mutex::new(reservation));
        let keys = BufWriter::new(BudgetedFile::create(
            root.join("keys.unsorted"),
            Arc::clone(&reservation),
        )?);
        Ok(Self {
            root,
            keys: Some(keys),
            reservation,
            input_rows: 0,
            sort_memory_bytes,
        })
    }

    pub fn push_keys(&mut self, keys: &[Vec<u8>]) -> Result<()> {
        let writer = self
            .keys
            .as_mut()
            .ok_or_else(|| CdfError::internal("dedup key spool is already finalized"))?;
        for key in keys {
            write_key_record(
                writer,
                &KeyRecord {
                    key: key.clone(),
                    ordinal: self.input_rows,
                },
            )?;
            self.input_rows = self
                .input_rows
                .checked_add(1)
                .ok_or_else(|| CdfError::data("dedup package row ordinal overflowed u64"))?;
        }
        Ok(())
    }

    pub fn finish(mut self, keep: DedupKeepProgram) -> Result<ExternalDedupDecisions> {
        let mut keys = self
            .keys
            .take()
            .ok_or_else(|| CdfError::internal("dedup key spool is already finalized"))?;
        keys.flush()
            .map_err(|error| io_error("flush dedup key spool", self.root.as_path(), error))?;
        drop(keys);
        let (level, count) = self.create_key_runs()?;
        let sorted_keys = if count == 0 {
            let path = self.root.join("keys-empty.sorted");
            BudgetedFile::create(path.clone(), Arc::clone(&self.reservation))?;
            path
        } else {
            self.merge_key_levels(level, count)?
        };
        let winners = self.root.join("winners.sorted");
        let (duplicate_key_count, output_rows) =
            self.write_winners(&sorted_keys, &winners, keep)?;
        let decisions_unsorted = self.root.join("decisions.unsorted");
        self.write_unsorted_decisions(&sorted_keys, &winners, &decisions_unsorted)?;
        let (decision_level, decision_count) = self.create_decision_runs(&decisions_unsorted)?;
        let decisions = if decision_count == 0 {
            let path = self.root.join("decisions-empty.sorted");
            BudgetedFile::create(path.clone(), Arc::clone(&self.reservation))?;
            path
        } else {
            self.merge_decision_levels(decision_level, decision_count)?
        };
        let dropped_row_count = self.input_rows.saturating_sub(output_rows);
        let spill_bytes = self.reservation.lock().unwrap().bytes();
        let reader = DecisionReader::open(&decisions)?;
        Ok(ExternalDedupDecisions {
            reader,
            summary: DedupIndexSummary {
                input_rows: self.input_rows,
                output_rows,
                duplicate_key_count,
                dropped_row_count,
                spill_bytes,
            },
            _index: self,
        })
    }

    fn create_key_runs(&self) -> Result<(u32, u64)> {
        let mut reader = KeyReader::open(&self.root.join("keys.unsorted"))?;
        let mut run = 0_u64;
        loop {
            let mut records = Vec::new();
            let mut bytes = 0_usize;
            while bytes < self.sort_memory_bytes {
                let Some(record) = reader.next()? else { break };
                bytes = bytes.saturating_add(record.key.len() + 24);
                records.push(record);
            }
            if records.is_empty() {
                break;
            }
            records.sort_unstable_by(key_record_cmp);
            let path = self.key_run_path(0, run);
            let mut writer =
                BufWriter::new(BudgetedFile::create(path, Arc::clone(&self.reservation))?);
            for record in records {
                write_key_record(&mut writer, &record)?;
            }
            writer
                .flush()
                .map_err(|error| io_error("flush dedup key run", self.root.as_path(), error))?;
            run += 1;
        }
        Ok((0, run))
    }

    fn merge_key_levels(&self, mut level: u32, mut count: u64) -> Result<PathBuf> {
        while count > 1 {
            let next_count = count.div_ceil(MERGE_FAN_IN);
            for output in 0..next_count {
                let start = output * MERGE_FAN_IN;
                let end = (start + MERGE_FAN_IN).min(count);
                let inputs = (start..end)
                    .map(|run| self.key_run_path(level, run))
                    .collect::<Vec<_>>();
                merge_key_runs(
                    &inputs,
                    self.key_run_path(level + 1, output),
                    Arc::clone(&self.reservation),
                )?;
            }
            level += 1;
            count = next_count;
        }
        Ok(self.key_run_path(level, 0))
    }

    fn write_winners(
        &self,
        sorted_keys: &Path,
        output: &Path,
        keep: DedupKeepProgram,
    ) -> Result<(u64, u64)> {
        let mut reader = KeyReader::open(sorted_keys)?;
        let mut writer = BufWriter::new(BudgetedFile::create(
            output.to_path_buf(),
            Arc::clone(&self.reservation),
        )?);
        let mut current = reader.next()?;
        let mut duplicate_keys = 0_u64;
        let mut output_rows = 0_u64;
        while let Some(first) = current.take() {
            let key = first.key;
            let first_ordinal = first.ordinal;
            let mut last_ordinal = first_ordinal;
            let mut count = 1_u64;
            loop {
                match reader.next()? {
                    Some(next) if next.key == key => {
                        last_ordinal = next.ordinal;
                        count += 1;
                    }
                    next => {
                        current = next;
                        break;
                    }
                }
            }
            if count > 1 {
                duplicate_keys += 1;
                if keep == DedupKeepProgram::Fail {
                    return Err(CdfError::contract(format!(
                        "dedup found duplicate key at package row {last_ordinal}; keep=fail aborts before package segment persistence"
                    )));
                }
            }
            let kept = match keep {
                DedupKeepProgram::First | DedupKeepProgram::Fail => first_ordinal,
                DedupKeepProgram::Last => last_ordinal,
            };
            write_key_record(&mut writer, &KeyRecord { key, ordinal: kept })?;
            output_rows += 1;
        }
        writer
            .flush()
            .map_err(|error| io_error("flush dedup winners", output, error))?;
        Ok((duplicate_keys, output_rows))
    }

    fn write_unsorted_decisions(
        &self,
        sorted_keys: &Path,
        winners: &Path,
        output: &Path,
    ) -> Result<()> {
        let mut keys = KeyReader::open(sorted_keys)?;
        let mut winners = KeyReader::open(winners)?;
        let mut winner = winners.next()?;
        let mut writer = BufWriter::new(BudgetedFile::create(
            output.to_path_buf(),
            Arc::clone(&self.reservation),
        )?);
        while let Some(record) = keys.next()? {
            while winner
                .as_ref()
                .is_some_and(|winner| winner.key < record.key)
            {
                winner = winners.next()?;
            }
            let kept = winner
                .as_ref()
                .filter(|winner| winner.key == record.key)
                .ok_or_else(|| CdfError::internal("dedup winner join omitted a key"))?
                .ordinal;
            write_decision(
                &mut writer,
                DedupDecision {
                    ordinal: record.ordinal,
                    kept_ordinal: kept,
                },
            )?;
        }
        writer
            .flush()
            .map_err(|error| io_error("flush dedup decisions", output, error))
    }

    fn create_decision_runs(&self, input: &Path) -> Result<(u32, u64)> {
        let per_run = (self.sort_memory_bytes / 16).max(1);
        let mut reader = DecisionReader::open(input)?;
        let mut run = 0_u64;
        loop {
            let mut records = Vec::with_capacity(per_run);
            while records.len() < per_run {
                let Some(record) = reader.next()? else { break };
                records.push(record);
            }
            if records.is_empty() {
                break;
            }
            records.sort_unstable_by_key(|record| record.ordinal);
            let path = self.decision_run_path(0, run);
            let mut writer =
                BufWriter::new(BudgetedFile::create(path, Arc::clone(&self.reservation))?);
            for record in records {
                write_decision(&mut writer, record)?;
            }
            writer.flush().map_err(|error| {
                io_error("flush dedup decision run", self.root.as_path(), error)
            })?;
            run += 1;
        }
        Ok((0, run))
    }

    fn merge_decision_levels(&self, mut level: u32, mut count: u64) -> Result<PathBuf> {
        while count > 1 {
            let next_count = count.div_ceil(MERGE_FAN_IN);
            for output in 0..next_count {
                let start = output * MERGE_FAN_IN;
                let end = (start + MERGE_FAN_IN).min(count);
                let inputs = (start..end)
                    .map(|run| self.decision_run_path(level, run))
                    .collect::<Vec<_>>();
                merge_decision_runs(
                    &inputs,
                    self.decision_run_path(level + 1, output),
                    Arc::clone(&self.reservation),
                )?;
            }
            level += 1;
            count = next_count;
        }
        Ok(self.decision_run_path(level, 0))
    }

    fn key_run_path(&self, level: u32, run: u64) -> PathBuf {
        self.root.join(format!("keys-l{level:03}-r{run:012}.run"))
    }

    fn decision_run_path(&self, level: u32, run: u64) -> PathBuf {
        self.root
            .join(format!("decisions-l{level:03}-r{run:012}.run"))
    }
}

impl ExternalDedupDecisions {
    pub fn next(&mut self) -> Result<Option<DedupDecision>> {
        self.reader.next()
    }
}

impl Drop for ExternalDedupIndex {
    fn drop(&mut self) {
        self.keys.take();
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct KeyRecord {
    key: Vec<u8>,
    ordinal: u64,
}

fn key_record_cmp(left: &KeyRecord, right: &KeyRecord) -> Ordering {
    left.key
        .cmp(&right.key)
        .then(left.ordinal.cmp(&right.ordinal))
}

#[derive(Eq, PartialEq)]
struct KeyHeapItem {
    record: KeyRecord,
    reader: usize,
}

impl Ord for KeyHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        key_record_cmp(&other.record, &self.record).then(other.reader.cmp(&self.reader))
    }
}

impl PartialOrd for KeyHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn merge_key_runs(
    inputs: &[PathBuf],
    output: PathBuf,
    reservation: Arc<Mutex<SpillReservation>>,
) -> Result<()> {
    let mut readers = inputs
        .iter()
        .map(|path| KeyReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = reader.next()? {
            heap.push(KeyHeapItem {
                record,
                reader: index,
            });
        }
    }
    let mut writer = BufWriter::new(BudgetedFile::create(output.clone(), reservation)?);
    while let Some(item) = heap.pop() {
        write_key_record(&mut writer, &item.record)?;
        if let Some(record) = readers[item.reader].next()? {
            heap.push(KeyHeapItem {
                record,
                reader: item.reader,
            });
        }
    }
    writer
        .flush()
        .map_err(|error| io_error("flush merged key run", &output, error))
}

#[derive(Eq, PartialEq)]
struct DecisionHeapItem {
    record: DedupDecision,
    reader: usize,
}

impl Ord for DecisionHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .ordinal
            .cmp(&self.record.ordinal)
            .then(other.reader.cmp(&self.reader))
    }
}

impl PartialOrd for DecisionHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn merge_decision_runs(
    inputs: &[PathBuf],
    output: PathBuf,
    reservation: Arc<Mutex<SpillReservation>>,
) -> Result<()> {
    let mut readers = inputs
        .iter()
        .map(|path| DecisionReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = reader.next()? {
            heap.push(DecisionHeapItem {
                record,
                reader: index,
            });
        }
    }
    let mut writer = BufWriter::new(BudgetedFile::create(output.clone(), reservation)?);
    while let Some(item) = heap.pop() {
        write_decision(&mut writer, item.record)?;
        if let Some(record) = readers[item.reader].next()? {
            heap.push(DecisionHeapItem {
                record,
                reader: item.reader,
            });
        }
    }
    writer
        .flush()
        .map_err(|error| io_error("flush merged decision run", &output, error))
}

struct KeyReader(BufReader<File>);

impl KeyReader {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self(BufReader::new(
            File::open(path).map_err(|error| io_error("open key run", path, error))?,
        )))
    }

    fn next(&mut self) -> Result<Option<KeyRecord>> {
        let Some(length) = read_u32_or_eof(&mut self.0)? else {
            return Ok(None);
        };
        let length = usize::try_from(length)
            .map_err(|_| CdfError::data("dedup key length exceeds platform usize"))?;
        if length > MAX_KEY_BYTES {
            return Err(CdfError::data(format!(
                "dedup encoded key length {length} exceeds {MAX_KEY_BYTES}-byte safety bound"
            )));
        }
        let mut key = vec![0; length];
        self.0
            .read_exact(&mut key)
            .map_err(|error| CdfError::data(format!("read dedup key bytes: {error}")))?;
        let ordinal = read_u64(&mut self.0)?;
        Ok(Some(KeyRecord { key, ordinal }))
    }
}

struct DecisionReader(BufReader<File>);

impl DecisionReader {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self(BufReader::new(File::open(path).map_err(|error| {
            io_error("open decision run", path, error)
        })?)))
    }

    fn next(&mut self) -> Result<Option<DedupDecision>> {
        let Some(ordinal) = read_u64_or_eof(&mut self.0)? else {
            return Ok(None);
        };
        Ok(Some(DedupDecision {
            ordinal,
            kept_ordinal: read_u64(&mut self.0)?,
        }))
    }
}

fn write_key_record(writer: &mut impl Write, record: &KeyRecord) -> Result<()> {
    let length = u32::try_from(record.key.len())
        .map_err(|_| CdfError::data("dedup key exceeds u32 encoded length"))?;
    writer
        .write_all(&length.to_le_bytes())
        .and_then(|_| writer.write_all(&record.key))
        .and_then(|_| writer.write_all(&record.ordinal.to_le_bytes()))
        .map_err(|error| CdfError::data(format!("write dedup key record: {error}")))
}

fn write_decision(writer: &mut impl Write, decision: DedupDecision) -> Result<()> {
    writer
        .write_all(&decision.ordinal.to_le_bytes())
        .and_then(|_| writer.write_all(&decision.kept_ordinal.to_le_bytes()))
        .map_err(|error| CdfError::data(format!("write dedup decision: {error}")))
}

fn read_u32_or_eof(reader: &mut impl Read) -> Result<Option<u32>> {
    let mut bytes = [0; 4];
    match reader.read_exact(&mut bytes) {
        Ok(()) => Ok(Some(u32::from_le_bytes(bytes))),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(None),
        Err(error) => Err(CdfError::data(format!("read dedup u32: {error}"))),
    }
}

fn read_u64_or_eof(reader: &mut impl Read) -> Result<Option<u64>> {
    let mut bytes = [0; 8];
    match reader.read_exact(&mut bytes) {
        Ok(()) => Ok(Some(u64::from_le_bytes(bytes))),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(None),
        Err(error) => Err(CdfError::data(format!("read dedup u64: {error}"))),
    }
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
    read_u64_or_eof(reader)?.ok_or_else(|| CdfError::data("dedup record is truncated"))
}

struct BudgetedFile {
    file: File,
    reservation: Arc<Mutex<SpillReservation>>,
}

impl BudgetedFile {
    fn create(path: PathBuf, reservation: Arc<Mutex<SpillReservation>>) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|error| io_error("create dedup spill file", &path, error))?;
        Ok(Self { file, reservation })
    }
}

impl Write for BudgetedFile {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let additional = u64::try_from(buffer.len())
            .map_err(|_| std::io::Error::other("spill write exceeds u64"))?;
        let mut reservation = self.reservation.lock().unwrap();
        if !reservation
            .try_grow(additional)
            .map_err(|error| std::io::Error::other(error.to_string()))?
        {
            return Err(std::io::Error::new(
                ErrorKind::StorageFull,
                "shared spill budget exhausted before dedup write",
            ));
        }
        let result = self.file.write(buffer);
        match result {
            Ok(written) => {
                let unused = additional.saturating_sub(written as u64);
                if unused > 0 {
                    reservation.shrink(unused);
                }
                Ok(written)
            }
            Err(error) => {
                reservation.shrink(additional);
                Err(error)
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

fn set_owner_only(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error("set dedup scratch permissions", path, error))?;
    }
    Ok(())
}

fn io_error(action: &str, path: &Path, error: std::io::Error) -> CdfError {
    CdfError::data(format!("{action} {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decisions(keep: DedupKeepProgram) -> (Vec<DedupDecision>, DedupIndexSummary) {
        let temp = tempfile::tempdir().unwrap();
        let budget: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(16 * 1024 * 1024).unwrap());
        let mut index =
            ExternalDedupIndex::create_with_sort_memory(temp.path().join("spill"), budget, 48)
                .unwrap();
        index
            .push_keys(&[
                b"b".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"a".to_vec(),
            ])
            .unwrap();
        let mut output = index.finish(keep).unwrap();
        let summary = output.summary.clone();
        let mut decisions = Vec::new();
        while let Some(decision) = output.next().unwrap() {
            decisions.push(decision);
        }
        (decisions, summary)
    }

    #[test]
    fn external_runs_preserve_first_and_last_semantics_in_ordinal_order() {
        let (first, first_summary) = decisions(DedupKeepProgram::First);
        assert_eq!(
            first
                .iter()
                .map(|item| item.kept_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1, 0, 3, 1]
        );
        assert_eq!(first_summary.output_rows, 3);
        assert_eq!(first_summary.duplicate_key_count, 2);

        let (last, last_summary) = decisions(DedupKeepProgram::Last);
        assert_eq!(
            last.iter()
                .map(|item| item.kept_ordinal)
                .collect::<Vec<_>>(),
            vec![2, 4, 2, 3, 4]
        );
        assert_eq!(last_summary.dropped_row_count, 2);
    }

    #[test]
    fn fail_mode_certifies_before_returning_any_decision() {
        let temp = tempfile::tempdir().unwrap();
        let budget: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(1024 * 1024).unwrap());
        let mut index =
            ExternalDedupIndex::create_with_sort_memory(temp.path().join("spill"), budget, 32)
                .unwrap();
        index
            .push_keys(&[b"same".to_vec(), b"same".to_vec()])
            .unwrap();
        assert!(index.finish(DedupKeepProgram::Fail).is_err());
    }

    #[test]
    fn disk_exhaustion_is_clean_and_scratch_cleanup_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("spill");
        let budget: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(32).unwrap());
        let mut index = ExternalDedupIndex::create_with_sort_memory(&root, budget, 16).unwrap();
        index.push_keys(&[vec![7; 64]]).unwrap();
        assert!(index.finish(DedupKeepProgram::First).is_err());
        assert!(!root.exists());
    }
}
