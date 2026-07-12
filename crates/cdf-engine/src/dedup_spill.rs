use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use cdf_contract::DedupKeepProgram;
use cdf_kernel::{CdfError, Result, SourcePosition};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest};
use cdf_runtime::{SpillBudgetCoordinator, SpillReservation};

const DEFAULT_SORT_MEMORY_BYTES: usize = 8 * 1024 * 1024;
const MERGE_FAN_IN: u64 = 32;
const MAX_KEY_BYTES: usize = 32 * 1024 * 1024;
const FAST_PATH_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PayloadMetadata {
    partition_ordinal: u32,
    output_position: Option<SourcePosition>,
}

pub(crate) struct DedupPayload {
    pub partition_ordinal: u32,
    pub output_position: Option<SourcePosition>,
    pub batch: arrow_array::RecordBatch,
}

pub(crate) struct DedupPayloadSpool {
    owner: Arc<ScratchOwner>,
    reservation: Arc<Mutex<SpillReservation>>,
    writer: Option<arrow_ipc::writer::StreamWriter<BudgetedSpillFile>>,
    metadata: BufWriter<BudgetedSpillFile>,
    schema: Option<arrow_schema::SchemaRef>,
    pub input_bytes: u64,
}

pub(crate) struct DedupPayloadReader {
    _owner: Arc<ScratchOwner>,
    _reservation: Arc<Mutex<SpillReservation>>,
    reader: arrow_ipc::reader::StreamReader<BufReader<File>>,
    metadata: BufReader<File>,
}

impl DedupPayloadSpool {
    pub fn create(root: impl AsRef<Path>, budget: Arc<dyn SpillBudgetCoordinator>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir(&root).map_err(|error| io_error("create payload scratch", &root, error))?;
        set_owner_only(&root)?;
        let owner = Arc::new(ScratchOwner { root: root.clone() });
        let reservation = Arc::new(Mutex::new(
            budget.try_reserve(1)?.ok_or_else(|| {
                CdfError::data(format!(
                    "dedup payload spill requires scratch bytes but the shared {}-byte spill budget is exhausted",
                    budget.snapshot().budget_bytes
                ))
            })?,
        ));
        let metadata = BufWriter::new(BudgetedSpillFile::create(
            root.join("payload-metadata.jsonl"),
            Arc::clone(&reservation),
        )?);
        Ok(Self {
            owner,
            reservation,
            writer: None,
            metadata,
            schema: None,
            input_bytes: 0,
        })
    }

    pub fn push(
        &mut self,
        partition_ordinal: u32,
        output_position: Option<SourcePosition>,
        batch: &arrow_array::RecordBatch,
    ) -> Result<()> {
        if let Some(schema) = &self.schema {
            if schema.as_ref() != batch.schema().as_ref() {
                return Err(CdfError::data(
                    "dedup payload batches must share the compiled output schema",
                ));
            }
        } else {
            self.schema = Some(batch.schema());
            self.writer = Some(
                arrow_ipc::writer::StreamWriter::try_new(
                    BudgetedSpillFile::create(
                        self.owner.root.join("payload.arrow"),
                        Arc::clone(&self.reservation),
                    )?,
                    batch.schema().as_ref(),
                )
                .map_err(CdfError::from)?,
            );
        }
        self.writer
            .as_mut()
            .ok_or_else(|| CdfError::internal("dedup payload writer was not initialized"))?
            .write(batch)
            .map_err(CdfError::from)?;
        let mut metadata = serde_json::to_vec(&PayloadMetadata {
            partition_ordinal,
            output_position,
        })
        .map_err(|error| {
            CdfError::internal(format!("serialize dedup payload metadata: {error}"))
        })?;
        metadata.push(b'\n');
        self.metadata
            .write_all(&metadata)
            .map_err(|error| CdfError::data(format!("write dedup payload metadata: {error}")))?;
        self.input_bytes = self
            .input_bytes
            .saturating_add(batch.get_array_memory_size() as u64);
        Ok(())
    }

    pub fn finish(mut self) -> Result<Option<DedupPayloadReader>> {
        let Some(mut writer) = self.writer.take() else {
            return Ok(None);
        };
        writer.finish().map_err(CdfError::from)?;
        drop(writer);
        self.metadata
            .flush()
            .map_err(|error| CdfError::data(format!("flush dedup payload metadata: {error}")))?;
        Ok(Some(DedupPayloadReader {
            _owner: Arc::clone(&self.owner),
            _reservation: Arc::clone(&self.reservation),
            reader: arrow_ipc::reader::StreamReader::try_new(
                BufReader::new(
                    File::open(self.owner.root.join("payload.arrow"))
                        .map_err(|error| io_error("open dedup payload", &self.owner.root, error))?,
                ),
                None,
            )
            .map_err(CdfError::from)?,
            metadata: BufReader::new(
                File::open(self.owner.root.join("payload-metadata.jsonl")).map_err(|error| {
                    io_error("open dedup payload metadata", &self.owner.root, error)
                })?,
            ),
        }))
    }
}

impl DedupPayloadReader {
    pub fn next(&mut self) -> Result<Option<DedupPayload>> {
        let Some(batch) = self.reader.next().transpose().map_err(CdfError::from)? else {
            let mut trailing = String::new();
            if self
                .metadata
                .read_line(&mut trailing)
                .map_err(|error| CdfError::data(format!("read dedup metadata tail: {error}")))?
                != 0
            {
                return Err(CdfError::data(
                    "dedup payload metadata contains more records than the Arrow spool",
                ));
            }
            return Ok(None);
        };
        let mut line = String::new();
        if self
            .metadata
            .read_line(&mut line)
            .map_err(|error| CdfError::data(format!("read dedup payload metadata: {error}")))?
            == 0
        {
            return Err(CdfError::data(
                "dedup Arrow payload contains more batches than its metadata spool",
            ));
        }
        let metadata: PayloadMetadata = serde_json::from_str(&line)
            .map_err(|error| CdfError::data(format!("decode dedup payload metadata: {error}")))?;
        Ok(Some(DedupPayload {
            partition_ordinal: metadata.partition_ordinal,
            output_position: metadata.output_position,
            batch,
        }))
    }
}

struct ScratchOwner {
    root: PathBuf,
}

impl Drop for ScratchOwner {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

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
    keys: Option<BufWriter<BudgetedSpillFile>>,
    reservation: Arc<Mutex<SpillReservation>>,
    memory: Option<Arc<dyn MemoryCoordinator>>,
    memory_lease: Option<MemoryLease>,
    fast_keys: Option<Vec<Vec<u8>>>,
    fast_bytes: u64,
    input_rows: u64,
    sort_memory_bytes: usize,
    maximum_key_bytes: usize,
    merge_fan_in: u64,
}

pub(crate) struct ExternalDedupDecisions {
    source: DecisionSource,
    pub summary: DedupIndexSummary,
    _index: ExternalDedupIndex,
}

enum DecisionSource {
    File(DecisionReader),
    Memory(std::vec::IntoIter<DedupDecision>),
}

impl ExternalDedupIndex {
    pub fn create(
        root: impl AsRef<Path>,
        budget: Arc<dyn SpillBudgetCoordinator>,
        memory: Option<Arc<dyn MemoryCoordinator>>,
    ) -> Result<Self> {
        Self::create_with_sort_memory(root, budget, memory, DEFAULT_SORT_MEMORY_BYTES)
    }

    fn create_with_sort_memory(
        root: impl AsRef<Path>,
        budget: Arc<dyn SpillBudgetCoordinator>,
        memory: Option<Arc<dyn MemoryCoordinator>>,
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
        let keys = BufWriter::new(BudgetedSpillFile::create(
            root.join("keys.unsorted"),
            Arc::clone(&reservation),
        )?);
        let memory_lease = match &memory {
            Some(memory) => memory.try_reserve(&ReservationRequest::new(
                ConsumerKey::new("dedup-in-memory-index", MemoryClass::Validation)?,
                1,
            )?)?,
            None => None,
        };
        let fast_keys = memory_lease.as_ref().map(|_| Vec::new());
        Ok(Self {
            root,
            keys: Some(keys),
            reservation,
            memory,
            memory_lease,
            fast_keys,
            fast_bytes: 0,
            input_rows: 0,
            sort_memory_bytes,
            maximum_key_bytes: 0,
            merge_fan_in: MERGE_FAN_IN,
        })
    }

    #[cfg(test)]
    pub fn push_keys(&mut self, keys: &[Vec<u8>]) -> Result<()> {
        self.push_owned_keys(keys.iter().cloned())
    }

    pub fn push_owned_keys(&mut self, keys: impl IntoIterator<Item = Vec<u8>>) -> Result<()> {
        for key in keys {
            self.maximum_key_bytes = self.maximum_key_bytes.max(key.len());
            let key_bytes = u64::try_from(key.len())
                .map_err(|_| CdfError::data("dedup key length exceeds u64"))?
                .saturating_add(96);
            let next_fast_bytes = self.fast_bytes.saturating_add(key_bytes);
            let retain_fast = next_fast_bytes <= FAST_PATH_MAX_BYTES
                && self
                    .memory_lease
                    .as_ref()
                    .is_some_and(|lease| lease.reconcile(next_fast_bytes.max(1)).is_ok());
            if retain_fast {
                self.fast_keys
                    .as_mut()
                    .expect("fast keys exist with their lease")
                    .push(key);
                self.fast_bytes = next_fast_bytes;
            } else {
                self.transition_fast_keys_to_spill()?;
                let record = KeyRecord {
                    key,
                    ordinal: self.input_rows,
                };
                write_key_record(
                    self.keys.as_mut().ok_or_else(|| {
                        CdfError::internal("dedup key spool is already finalized")
                    })?,
                    &record,
                )?;
            }
            self.input_rows = self
                .input_rows
                .checked_add(1)
                .ok_or_else(|| CdfError::data("dedup package row ordinal overflowed u64"))?;
        }
        Ok(())
    }

    fn transition_fast_keys_to_spill(&mut self) -> Result<()> {
        let Some(keys) = self.fast_keys.take() else {
            return Ok(());
        };
        let writer = self
            .keys
            .as_mut()
            .ok_or_else(|| CdfError::internal("dedup key spool is already finalized"))?;
        for (ordinal, key) in keys.into_iter().enumerate() {
            write_key_record(
                writer,
                &KeyRecord {
                    key,
                    ordinal: ordinal as u64,
                },
            )?;
        }
        self.memory_lease = None;
        self.fast_bytes = 0;
        Ok(())
    }

    pub fn finish(mut self, keep: DedupKeepProgram) -> Result<ExternalDedupDecisions> {
        if let Some(keys) = self.fast_keys.take() {
            return self.finish_fast(keys, keep);
        }
        let mut keys = self
            .keys
            .take()
            .ok_or_else(|| CdfError::internal("dedup key spool is already finalized"))?;
        keys.flush()
            .map_err(|error| io_error("flush dedup key spool", self.root.as_path(), error))?;
        drop(keys);
        self.reserve_sort_working_set()?;
        let (level, count) = self.create_key_runs()?;
        let sorted_keys = if count == 0 {
            let path = self.root.join("keys-empty.sorted");
            BudgetedSpillFile::create(path.clone(), Arc::clone(&self.reservation))?;
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
            BudgetedSpillFile::create(path.clone(), Arc::clone(&self.reservation))?;
            path
        } else {
            self.merge_decision_levels(decision_level, decision_count)?
        };
        let dropped_row_count = self.input_rows.saturating_sub(output_rows);
        let spill_bytes = self.reservation.lock().unwrap().bytes();
        let reader = DecisionReader::open(&decisions)?;
        Ok(ExternalDedupDecisions {
            source: DecisionSource::File(reader),
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

    fn finish_fast(
        self,
        keys: Vec<Vec<u8>>,
        keep: DedupKeepProgram,
    ) -> Result<ExternalDedupDecisions> {
        let mut groups = HashMap::<&[u8], (u64, u64, u64)>::new();
        for (ordinal, key) in keys.iter().enumerate() {
            let ordinal = ordinal as u64;
            groups
                .entry(key.as_slice())
                .and_modify(|group| {
                    group.1 = ordinal;
                    group.2 += 1;
                })
                .or_insert((ordinal, ordinal, 1));
        }
        let duplicate_key_count = groups.values().filter(|group| group.2 > 1).count() as u64;
        if keep == DedupKeepProgram::Fail && duplicate_key_count > 0 {
            return Err(CdfError::contract(
                "dedup found a duplicate key; keep=fail aborts before package segment persistence",
            ));
        }
        let mut decisions = Vec::with_capacity(keys.len());
        for (ordinal, key) in keys.iter().enumerate() {
            let group = groups
                .get(key.as_slice())
                .ok_or_else(|| CdfError::internal("dedup fast-path winner is missing"))?;
            let kept_ordinal = match keep {
                DedupKeepProgram::First | DedupKeepProgram::Fail => group.0,
                DedupKeepProgram::Last => group.1,
            };
            decisions.push(DedupDecision {
                ordinal: ordinal as u64,
                kept_ordinal,
            });
        }
        let output_rows = groups.len() as u64;
        let spill_bytes = self.reservation.lock().unwrap().bytes();
        Ok(ExternalDedupDecisions {
            source: DecisionSource::Memory(decisions.into_iter()),
            summary: DedupIndexSummary {
                input_rows: self.input_rows,
                output_rows,
                duplicate_key_count,
                dropped_row_count: self.input_rows.saturating_sub(output_rows),
                spill_bytes,
            },
            _index: self,
        })
    }

    fn reserve_sort_working_set(&mut self) -> Result<()> {
        let maximum_key_bytes = self.maximum_key_bytes.max(1);
        let working_set = self
            .sort_memory_bytes
            .max(maximum_key_bytes.saturating_mul(2))
            .saturating_add(64 * 1024);
        self.merge_fan_in = MERGE_FAN_IN
            .min(u64::try_from((working_set / maximum_key_bytes).max(2)).unwrap_or(MERGE_FAN_IN));
        if let Some(memory) = &self.memory {
            let bytes = u64::try_from(working_set)
                .map_err(|_| CdfError::data("dedup sort working set exceeds u64"))?;
            let request = ReservationRequest::new(
                ConsumerKey::new("dedup-external-sort", MemoryClass::Validation)?,
                bytes,
            )?
            .as_minimum_working_set();
            self.memory_lease = Some(memory.try_reserve(&request)?.ok_or_else(|| {
                CdfError::data(format!(
                    "dedup external sort requires {bytes} bytes for its largest encoded key and merge heap but the shared memory budget is exhausted; reduce jobs or raise the memory budget"
                ))
            })?);
        }
        Ok(())
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
            let mut writer = BufWriter::new(BudgetedSpillFile::create(
                path,
                Arc::clone(&self.reservation),
            )?);
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
            let next_count = count.div_ceil(self.merge_fan_in);
            for output in 0..next_count {
                let start = output * self.merge_fan_in;
                let end = (start + self.merge_fan_in).min(count);
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
        let mut writer = BufWriter::new(BudgetedSpillFile::create(
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
        let mut writer = BufWriter::new(BudgetedSpillFile::create(
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
            let mut writer = BufWriter::new(BudgetedSpillFile::create(
                path,
                Arc::clone(&self.reservation),
            )?);
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
            let next_count = count.div_ceil(self.merge_fan_in);
            for output in 0..next_count {
                let start = output * self.merge_fan_in;
                let end = (start + self.merge_fan_in).min(count);
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
        match &mut self.source {
            DecisionSource::File(reader) => reader.next(),
            DecisionSource::Memory(decisions) => Ok(decisions.next()),
        }
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
    let mut writer = BufWriter::new(BudgetedSpillFile::create(output.clone(), reservation)?);
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
    let mut writer = BufWriter::new(BudgetedSpillFile::create(output.clone(), reservation)?);
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

pub(crate) struct BudgetedSpillFile {
    file: File,
    reservation: Arc<Mutex<SpillReservation>>,
}

impl BudgetedSpillFile {
    pub(crate) fn create(path: PathBuf, reservation: Arc<Mutex<SpillReservation>>) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|error| io_error("create spill file", &path, error))?;
        Ok(Self { file, reservation })
    }
}

impl Write for BudgetedSpillFile {
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
                "shared spill budget exhausted before write",
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
    use std::{collections::HashMap, time::Instant};

    use arrow_array::{ArrayRef, BinaryArray, RecordBatch};

    fn decisions(keep: DedupKeepProgram) -> (Vec<DedupDecision>, DedupIndexSummary) {
        let temp = tempfile::tempdir().unwrap();
        let budget: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(16 * 1024 * 1024).unwrap());
        let mut index = ExternalDedupIndex::create_with_sort_memory(
            temp.path().join("spill"),
            budget,
            None,
            48,
        )
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
        let mut index = ExternalDedupIndex::create_with_sort_memory(
            temp.path().join("spill"),
            budget,
            None,
            32,
        )
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
        let mut index =
            ExternalDedupIndex::create_with_sort_memory(&root, budget, None, 16).unwrap();
        index.push_keys(&[vec![7; 64]]).unwrap();
        assert!(index.finish(DedupKeepProgram::First).is_err());
        assert!(!root.exists());
    }

    #[test]
    fn external_index_matches_reference_across_chunking_and_skew() {
        let mut seed = 0x5eed_u64;
        let keys = (0..2_000)
            .map(|row| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                if row % 11 == 0 {
                    vec![0; 256]
                } else {
                    (seed % 137).to_le_bytes().to_vec()
                }
            })
            .collect::<Vec<_>>();
        for keep in [DedupKeepProgram::First, DedupKeepProgram::Last] {
            let expected = reference_decisions(&keys, keep.clone());
            for chunk in [1, 3, 17, 257, 2_000] {
                let temp = tempfile::tempdir().unwrap();
                let budget: Arc<dyn SpillBudgetCoordinator> =
                    Arc::new(cdf_runtime::FixedSpillBudget::new(64 * 1024 * 1024).unwrap());
                let mut index = ExternalDedupIndex::create_with_sort_memory(
                    temp.path().join("spill"),
                    budget,
                    None,
                    4 * 1024,
                )
                .unwrap();
                for keys in keys.chunks(chunk) {
                    index.push_keys(keys).unwrap();
                }
                let mut actual = index.finish(keep.clone()).unwrap();
                let mut decisions = Vec::new();
                while let Some(decision) = actual.next().unwrap() {
                    decisions.push(decision.kept_ordinal);
                }
                assert_eq!(decisions, expected, "chunk={chunk}, keep={keep:?}");
            }
        }
    }

    #[test]
    fn in_memory_pressure_transitions_losslessly_to_external_runs() {
        let temp = tempfile::tempdir().unwrap();
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(128 * 1024 * 1024).unwrap());
        let memory_impl = Arc::new(
            cdf_memory::DeterministicMemoryCoordinator::new(
                16 * 1024 * 1024,
                std::collections::BTreeMap::new(),
            )
            .unwrap(),
        );
        let blocker = memory_impl
            .try_reserve(
                &ReservationRequest::new(
                    ConsumerKey::new("test-blocker", MemoryClass::Control).unwrap(),
                    15 * 1024 * 1024,
                )
                .unwrap(),
            )
            .unwrap()
            .unwrap();
        let memory: Arc<dyn MemoryCoordinator> = memory_impl.clone();
        let mut index = ExternalDedupIndex::create_with_sort_memory(
            temp.path().join("spill"),
            spill,
            Some(memory),
            8 * 1024 * 1024,
        )
        .unwrap();
        let keys = (0..2_048)
            .map(|row| {
                let mut key = vec![u8::try_from(row % 251).unwrap(); 1024];
                key.extend_from_slice(&(row as u64).to_le_bytes());
                key
            })
            .collect::<Vec<_>>();
        index.push_keys(&keys).unwrap();
        drop(blocker);

        let mut decisions = index.finish(DedupKeepProgram::First).unwrap();
        let mut rows = 0_u64;
        while let Some(decision) = decisions.next().unwrap() {
            assert_eq!(decision.ordinal, decision.kept_ordinal);
            rows += 1;
        }
        assert_eq!(rows, keys.len() as u64);
        drop(decisions);
        assert_eq!(memory_impl.snapshot().current_bytes, 0);
        assert!(memory_impl.snapshot().peak_bytes >= 15 * 1024 * 1024);
    }

    #[test]
    #[ignore = "release-mode A6 crossover benchmark"]
    fn dedup_external_merge_crossover_benchmark() {
        let cases = [
            ("all_unique", benchmark_keys(250_000, |row| row as u64)),
            (
                "uniform_50pct",
                benchmark_keys(250_000, |row| (row / 2) as u64),
            ),
            (
                "high_skew",
                benchmark_keys(250_000, |row| (row % 17) as u64),
            ),
            ("all_identical", benchmark_keys(250_000, |_| 1)),
            (
                "wide_composite",
                (0..100_000)
                    .map(|row| {
                        let mut key = vec![u8::try_from(row % 251).unwrap(); 1024];
                        key.extend_from_slice(&(row as u64).to_le_bytes());
                        key
                    })
                    .collect(),
            ),
        ];
        let mut reports = Vec::new();
        for (name, keys) in cases {
            let reference_started = Instant::now();
            std::hint::black_box(reference_decisions(&keys, DedupKeepProgram::First));
            let reference_ns = reference_started.elapsed().as_nanos();

            let fast_temp = tempfile::tempdir().unwrap();
            let fast_spill: Arc<dyn SpillBudgetCoordinator> =
                Arc::new(cdf_runtime::FixedSpillBudget::new(4 * 1024 * 1024 * 1024).unwrap());
            let fast_memory: Arc<dyn MemoryCoordinator> = Arc::new(
                cdf_memory::DeterministicMemoryCoordinator::new(
                    512 * 1024 * 1024,
                    std::collections::BTreeMap::new(),
                )
                .unwrap(),
            );
            let fast_started = Instant::now();
            let mut fast = ExternalDedupIndex::create(
                fast_temp.path().join("spill"),
                fast_spill,
                Some(fast_memory),
            )
            .unwrap();
            for chunk in keys.chunks(8_192) {
                fast.push_keys(chunk).unwrap();
            }
            let mut fast_decisions = fast.finish(DedupKeepProgram::First).unwrap();
            while fast_decisions.next().unwrap().is_some() {}
            let fast_ns = fast_started.elapsed().as_nanos();

            let temp = tempfile::tempdir().unwrap();
            let budget: Arc<dyn SpillBudgetCoordinator> =
                Arc::new(cdf_runtime::FixedSpillBudget::new(4 * 1024 * 1024 * 1024).unwrap());
            let external_started = Instant::now();
            let mut index =
                ExternalDedupIndex::create(temp.path().join("spill"), budget, None).unwrap();
            for chunk in keys.chunks(8_192) {
                index.push_keys(chunk).unwrap();
            }
            let mut decisions = index.finish(DedupKeepProgram::First).unwrap();
            while decisions.next().unwrap().is_some() {}
            let external_ns = external_started.elapsed().as_nanos();
            reports.push(serde_json::json!({
                "case": name,
                "rows": keys.len(),
                "reference_hash_ns": reference_ns,
                "accounted_fast_ns": fast_ns,
                "external_merge_ns": external_ns,
                "fast_over_reference": fast_ns as f64 / reference_ns as f64,
                "external_over_reference": external_ns as f64 / reference_ns as f64,
                "fast_spill_bytes": fast_decisions.summary.spill_bytes,
                "spill_bytes": decisions.summary.spill_bytes,
            }));
        }
        println!("{}", serde_json::to_string_pretty(&reports).unwrap());
    }

    #[test]
    #[ignore = "slow A6 constant-memory stress; set CDF_A6_STRESS_GIB=100 for closure"]
    fn dedup_payload_constant_memory_stress() {
        const GIB: u64 = 1024 * 1024 * 1024;
        const CHUNK_BYTES: usize = 8 * 1024 * 1024;
        let gib = std::env::var("CDF_A6_STRESS_GIB")
            .ok()
            .map(|value| value.parse::<u64>().unwrap())
            .unwrap_or(1);
        assert!((1..=100).contains(&gib));
        let logical_bytes = gib * GIB;
        let rows = logical_bytes.div_ceil(CHUNK_BYTES as u64);
        let temp = tempfile::tempdir().unwrap();
        let spill_budget = logical_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(GIB))
            .unwrap();
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(spill_budget).unwrap());
        let memory_impl = Arc::new(
            cdf_memory::DeterministicMemoryCoordinator::new(
                128 * 1024 * 1024,
                std::collections::BTreeMap::new(),
            )
            .unwrap(),
        );
        let blocker = memory_impl
            .try_reserve(
                &ReservationRequest::new(
                    ConsumerKey::new("stress-force-external", MemoryClass::Control).unwrap(),
                    128 * 1024 * 1024,
                )
                .unwrap(),
            )
            .unwrap()
            .unwrap();
        let memory: Arc<dyn MemoryCoordinator> = memory_impl.clone();
        let mut payload =
            DedupPayloadSpool::create(temp.path().join("payload"), Arc::clone(&spill)).unwrap();
        let mut index =
            ExternalDedupIndex::create(temp.path().join("index"), Arc::clone(&spill), Some(memory))
                .unwrap();
        let mut bytes = vec![0x5a; CHUNK_BYTES];
        let started = Instant::now();
        for ordinal in 0..rows {
            bytes[..8].copy_from_slice(&ordinal.to_le_bytes());
            let array: ArrayRef = Arc::new(BinaryArray::from_vec(vec![bytes.as_slice()]));
            let batch = RecordBatch::try_from_iter([("payload", array)]).unwrap();
            payload.push(0, None, &batch).unwrap();
            index
                .push_owned_keys(std::iter::once(ordinal.to_le_bytes().to_vec()))
                .unwrap();
        }
        let mut payload = payload.finish().unwrap().unwrap();
        drop(blocker);
        let mut decisions = index.finish(DedupKeepProgram::First).unwrap();
        let mut observed_rows = 0_u64;
        let mut observed_bytes = 0_u64;
        while let Some(item) = payload.next().unwrap() {
            let decision = decisions.next().unwrap().unwrap();
            assert_eq!(decision.ordinal, observed_rows);
            assert_eq!(decision.kept_ordinal, observed_rows);
            observed_rows += item.batch.num_rows() as u64;
            observed_bytes =
                observed_bytes.saturating_add(item.batch.get_array_memory_size() as u64);
        }
        assert!(decisions.next().unwrap().is_none());
        assert_eq!(observed_rows, rows);
        assert!(observed_bytes >= logical_bytes);
        assert!(memory_impl.snapshot().peak_bytes <= 128 * 1024 * 1024);
        let spill_peak_bytes = spill.snapshot().peak_bytes;
        assert!(spill_peak_bytes >= logical_bytes);
        eprintln!(
            "logical_gib={gib} rows={rows} observed_bytes={observed_bytes} elapsed_ns={} managed_peak_bytes={} spill_peak_bytes={spill_peak_bytes} index_spill_bytes={}",
            started.elapsed().as_nanos(),
            memory_impl.snapshot().peak_bytes,
            decisions.summary.spill_bytes,
        );
    }

    fn benchmark_keys(rows: usize, value: impl Fn(usize) -> u64) -> Vec<Vec<u8>> {
        (0..rows)
            .map(|row| value(row).to_le_bytes().to_vec())
            .collect()
    }

    fn reference_decisions(keys: &[Vec<u8>], keep: DedupKeepProgram) -> Vec<u64> {
        let mut winners = HashMap::<&[u8], u64>::new();
        for (ordinal, key) in keys.iter().enumerate() {
            let ordinal = ordinal as u64;
            match keep {
                DedupKeepProgram::First => {
                    winners.entry(key).or_insert(ordinal);
                }
                DedupKeepProgram::Last => {
                    winners.insert(key, ordinal);
                }
                DedupKeepProgram::Fail => unreachable!(),
            }
        }
        keys.iter().map(|key| winners[key.as_slice()]).collect()
    }
}
