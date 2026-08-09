use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, HashMap},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use cdf_contract::DedupKeepProgram;
use cdf_kernel::{CdfError, PackageSegmentKind, Result, SourcePosition};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest};
use cdf_runtime::{SpillBudgetCoordinator, SpillReservation};

const DEFAULT_SORT_MEMORY_BYTES: usize = 8 * 1024 * 1024;
const MERGE_FAN_IN: u64 = 32;
const EFFECT_MERGE_FAN_IN: usize = 4;
const MAX_KEY_BYTES: usize = 32 * 1024 * 1024;
const FAST_PATH_MAX_BYTES: u64 = 64 * 1024 * 1024;

type EffectFamily = (PackageSegmentKind, String);

trait MutexFailStop<T> {
    fn lock_fail_stop(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexFailStop<T> for Mutex<T> {
    fn lock_fail_stop(&self) -> MutexGuard<'_, T> {
        match self.lock() {
            Ok(guard) => guard,
            Err(_) => panic!("dedup-spill invariant lock is poisoned; refusing recovery"),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PayloadMetadata {
    kind: PackageSegmentKind,
    schema_hash: String,
    partition_ordinal: u64,
    output_position: Option<SourcePosition>,
}

pub(crate) struct DedupPayload {
    pub kind: PackageSegmentKind,
    pub partition_ordinal: u64,
    pub output_position: Option<SourcePosition>,
    pub batch: arrow_array::RecordBatch,
    pub keys: Vec<Vec<u8>>,
}

pub(crate) struct DedupPayloadSpool {
    owner: Arc<ScratchOwner>,
    reservation: Arc<Mutex<SpillReservation>>,
    writers: BTreeMap<EffectFamily, arrow_ipc::writer::StreamWriter<BudgetedSpillFile>>,
    metadata: BufWriter<BudgetedSpillFile>,
    keys: BufWriter<BudgetedSpillFile>,
    schemas: BTreeMap<EffectFamily, arrow_schema::SchemaRef>,
    input_rows: u64,
    pub input_bytes: u64,
}

pub(crate) struct DedupPayloadReader {
    _owner: Arc<ScratchOwner>,
    _reservation: Arc<Mutex<SpillReservation>>,
    readers: BTreeMap<EffectFamily, arrow_ipc::reader::StreamReader<BufReader<File>>>,
    metadata: BufReader<File>,
    keys: KeyReader,
    next_ordinal: u64,
}

pub(crate) struct EffectSortSpool {
    owner: Arc<ScratchOwner>,
    reservation: Arc<Mutex<SpillReservation>>,
    memory: Arc<dyn MemoryCoordinator>,
    runs: BTreeMap<EffectFamily, Vec<EffectRun>>,
    schemas: BTreeMap<EffectFamily, arrow_schema::SchemaRef>,
    next_run: u64,
    terminal_position: Option<SourcePosition>,
}

pub(crate) struct EffectSortReader {
    _owner: Arc<ScratchOwner>,
    _reservation: Arc<Mutex<SpillReservation>>,
    families: std::collections::VecDeque<(PackageSegmentKind, EffectRunReader)>,
    terminal_position: Option<SourcePosition>,
    previous_key: Option<Vec<u8>>,
}

pub(crate) struct EffectSortedBatch {
    pub kind: PackageSegmentKind,
    pub batch: arrow_array::RecordBatch,
    pub output_position: Option<SourcePosition>,
}

#[derive(Clone)]
struct EffectRun {
    arrow: PathBuf,
    keys: PathBuf,
    maximum_batch_bytes: u64,
}

struct EffectRunReader {
    batches: arrow_ipc::reader::StreamReader<BufReader<File>>,
    keys: KeyReader,
    current_batch: Option<arrow_array::RecordBatch>,
    next_row: usize,
}

struct EffectMergeRow {
    key: Vec<u8>,
    batch: arrow_array::RecordBatch,
    reader: usize,
    last_in_batch: bool,
}

impl PartialEq for EffectMergeRow {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.reader == other.reader
    }
}

impl Eq for EffectMergeRow {}

impl Ord for EffectMergeRow {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then(other.reader.cmp(&self.reader))
    }
}

impl PartialOrd for EffectMergeRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl EffectSortSpool {
    pub fn create(
        root: impl AsRef<Path>,
        budget: Arc<dyn SpillBudgetCoordinator>,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir(&root)
            .map_err(|error| io_error("create effect-sort scratch", &root, error))?;
        set_owner_only(&root)?;
        let owner = Arc::new(ScratchOwner { root });
        let reservation = Arc::new(Mutex::new(budget.try_reserve(1)?.ok_or_else(|| {
            CdfError::data(format!(
                "keyed-effect ordering requires scratch bytes but the shared {}-byte spill budget is exhausted",
                budget.snapshot().budget_bytes
            ))
        })?));
        Ok(Self {
            owner,
            reservation,
            memory,
            runs: BTreeMap::new(),
            schemas: BTreeMap::new(),
            next_run: 0,
            terminal_position: None,
        })
    }

    pub fn push(
        &mut self,
        kind: PackageSegmentKind,
        output_position: Option<SourcePosition>,
        batch: arrow_array::RecordBatch,
        keys: Vec<Vec<u8>>,
    ) -> Result<()> {
        if !matches!(
            kind,
            PackageSegmentKind::Upsert | PackageSegmentKind::Delete
        ) {
            return Err(CdfError::internal(
                "canonical keyed-effect ordering received an ordinary row batch",
            ));
        }
        if batch.num_rows() == 0 || batch.num_rows() != keys.len() {
            return Err(CdfError::internal(
                "canonical keyed-effect sort batch and key counts are inconsistent",
            ));
        }
        let family = effect_family(kind, batch.schema().as_ref())?;
        match self.schemas.get(&family) {
            Some(schema) if schema.as_ref() != batch.schema().as_ref() => {
                return Err(CdfError::data(
                    "one keyed-effect family changed Arrow schema during canonical ordering",
                ));
            }
            Some(_) => {}
            None => {
                self.schemas.insert(family.clone(), batch.schema());
            }
        }
        let retained = u64::try_from(batch.get_array_memory_size())
            .map_err(|_| CdfError::data("keyed-effect sort batch bytes exceed u64"))?;
        let working_set = retained
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add((keys.len() as u64).saturating_mul(16)))
            .ok_or_else(|| CdfError::data("keyed-effect sort working set overflowed u64"))?
            .max(1);
        let request = ReservationRequest::new(
            ConsumerKey::new("keyed-effect-run-sort", MemoryClass::Validation)?,
            working_set,
        )?
        .as_minimum_working_set();
        let _lease = self.memory.try_reserve(&request)?.ok_or_else(|| {
            CdfError::data(format!(
                "canonical keyed-effect ordering requires {working_set} bytes for one decoded source batch; reduce source batch size or raise the memory budget"
            ))
        })?;

        let mut order = (0..keys.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|left, right| keys[*left].cmp(&keys[*right]).then(left.cmp(right)));
        let indices = order
            .iter()
            .map(|index| {
                u32::try_from(*index)
                    .map_err(|_| CdfError::data("keyed-effect sort row index exceeds u32"))
            })
            .collect::<Result<Vec<_>>>()?;
        let indices = arrow_array::UInt32Array::from(indices);
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                arrow_select::take::take(column.as_ref(), &indices, None).map_err(CdfError::from)
            })
            .collect::<Result<Vec<_>>>()?;
        let sorted =
            arrow_array::RecordBatch::try_new(batch.schema(), columns).map_err(CdfError::from)?;
        let sorted_keys = order
            .into_iter()
            .map(|index| keys[index].clone())
            .collect::<Vec<_>>();
        let mut run = self.new_run(&family, 0, self.next_run)?;
        self.next_run = self
            .next_run
            .checked_add(1)
            .ok_or_else(|| CdfError::data("keyed-effect run ordinal overflowed u64"))?;
        run.maximum_batch_bytes = write_effect_run(
            &run,
            Arc::clone(&self.reservation),
            batch.schema().as_ref(),
            std::iter::once((sorted_keys, sorted)),
        )?;
        self.runs.entry(family).or_default().push(run);
        if output_position.is_some() {
            self.terminal_position = output_position;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<Option<EffectSortReader>> {
        if self.runs.values().all(Vec::is_empty) {
            return Ok(None);
        }
        let mut families = std::collections::VecDeque::new();
        for kind in [PackageSegmentKind::Upsert, PackageSegmentKind::Delete] {
            let kind_families = self
                .runs
                .keys()
                .filter(|family| family.0 == kind)
                .cloned()
                .collect::<Vec<_>>();
            for family in kind_families {
                let mut runs = self
                    .runs
                    .remove(&family)
                    .ok_or_else(|| CdfError::internal("keyed-effect sort family disappeared"))?;
                let schema = self.schemas.get(&family).ok_or_else(|| {
                    CdfError::internal("keyed-effect sort family omitted its Arrow schema")
                })?;
                let mut level = 1_u32;
                while runs.len() > 1 {
                    let mut next = Vec::with_capacity(runs.len().div_ceil(EFFECT_MERGE_FAN_IN));
                    for group in runs.chunks(EFFECT_MERGE_FAN_IN) {
                        let mut output = self.new_run(&family, level, self.next_run)?;
                        self.next_run = self.next_run.checked_add(1).ok_or_else(|| {
                            CdfError::data("keyed-effect run ordinal overflowed u64")
                        })?;
                        output.maximum_batch_bytes = merge_effect_runs(
                            group,
                            &output,
                            schema.as_ref(),
                            Arc::clone(&self.reservation),
                            Arc::clone(&self.memory),
                        )?;
                        for input in group {
                            remove_effect_run(input, &self.reservation)?;
                        }
                        next.push(output);
                    }
                    runs = next;
                    level = level
                        .checked_add(1)
                        .ok_or_else(|| CdfError::data("keyed-effect merge level overflowed u32"))?;
                }
                let run = runs.pop().ok_or_else(|| {
                    CdfError::internal("keyed-effect sort family lost its final run")
                })?;
                families.push_back((kind, EffectRunReader::open(&run)?));
            }
        }
        Ok(Some(EffectSortReader {
            _owner: Arc::clone(&self.owner),
            _reservation: Arc::clone(&self.reservation),
            families,
            terminal_position: self.terminal_position,
            previous_key: None,
        }))
    }

    fn new_run(&self, family: &EffectFamily, level: u32, ordinal: u64) -> Result<EffectRun> {
        let kind = match family.0 {
            PackageSegmentKind::Upsert => "upsert",
            PackageSegmentKind::Delete => "delete",
            PackageSegmentKind::Row => {
                return Err(CdfError::internal(
                    "ordinary rows cannot create an effect sort run",
                ));
            }
        };
        let schema = family_path_component(&family.1);
        let stem = format!("{kind}-{schema}-l{level:03}-r{ordinal:012}");
        Ok(EffectRun {
            arrow: self.owner.root.join(format!("{stem}.arrow")),
            keys: self.owner.root.join(format!("{stem}.keys")),
            maximum_batch_bytes: 0,
        })
    }
}

impl EffectSortReader {
    pub fn next(&mut self) -> Result<Option<EffectSortedBatch>> {
        loop {
            let Some((kind, reader)) = self.families.front_mut() else {
                return Ok(None);
            };
            let Some(batch) = reader
                .batches
                .next()
                .transpose()
                .map_err(|error| scratch_arrow_error("read keyed-effect sort run", error))?
            else {
                if reader.keys.next()?.is_some() {
                    return Err(CdfError::internal(
                        "keyed-effect sort run contains excess encoded keys",
                    ));
                }
                self.families.pop_front();
                self.previous_key = None;
                continue;
            };
            for _ in 0..batch.num_rows() {
                let key = reader.keys.next()?.ok_or_else(|| {
                    CdfError::internal("keyed-effect sort run ended its key stream early")
                })?;
                if self
                    .previous_key
                    .as_ref()
                    .is_some_and(|previous| previous >= &key.key)
                {
                    return Err(CdfError::internal(
                        "canonical keyed-effect run is not strictly exact-key sorted",
                    ));
                }
                self.previous_key = Some(key.key);
            }
            return Ok(Some(EffectSortedBatch {
                kind: *kind,
                batch,
                output_position: self.terminal_position.clone(),
            }));
        }
    }
}

impl EffectRunReader {
    fn open(run: &EffectRun) -> Result<Self> {
        Ok(Self {
            batches: arrow_ipc::reader::StreamReader::try_new(
                BufReader::new(File::open(&run.arrow).map_err(|error| {
                    scratch_read_io_error("open keyed-effect Arrow run", &run.arrow, error)
                })?),
                None,
            )
            .map_err(|error| scratch_arrow_error("open keyed-effect Arrow run", error))?,
            keys: KeyReader::open(&run.keys)?,
            current_batch: None,
            next_row: 0,
        })
    }

    fn next_row(&mut self) -> Result<Option<(Vec<u8>, arrow_array::RecordBatch, bool)>> {
        if self
            .current_batch
            .as_ref()
            .is_none_or(|batch| self.next_row == batch.num_rows())
        {
            self.current_batch = self
                .batches
                .next()
                .transpose()
                .map_err(|error| scratch_arrow_error("read keyed-effect merge run", error))?;
            self.next_row = 0;
            let Some(batch) = self.current_batch.as_ref() else {
                if self.keys.next()?.is_some() {
                    return Err(CdfError::internal(
                        "keyed-effect merge run contains excess encoded keys",
                    ));
                }
                return Ok(None);
            };
            if batch.num_rows() == 0 {
                return Err(CdfError::internal(
                    "keyed-effect merge run contains an empty Arrow batch",
                ));
            }
        }
        let key = self.keys.next()?.ok_or_else(|| {
            CdfError::internal("keyed-effect merge run ended its key stream early")
        })?;
        let batch = self
            .current_batch
            .as_ref()
            .ok_or_else(|| CdfError::internal("keyed-effect merge batch disappeared"))?
            .slice(self.next_row, 1);
        self.next_row += 1;
        let last_in_batch = self
            .current_batch
            .as_ref()
            .is_some_and(|current| self.next_row == current.num_rows());
        Ok(Some((key.key, batch, last_in_batch)))
    }
}

fn write_effect_run(
    run: &EffectRun,
    reservation: Arc<Mutex<SpillReservation>>,
    schema: &arrow_schema::Schema,
    batches: impl IntoIterator<Item = (Vec<Vec<u8>>, arrow_array::RecordBatch)>,
) -> Result<u64> {
    let mut arrow = arrow_ipc::writer::StreamWriter::try_new(
        BudgetedSpillFile::create(run.arrow.clone(), Arc::clone(&reservation))?,
        schema,
    )
    .map_err(CdfError::from)?;
    let mut keys = BufWriter::new(BudgetedSpillFile::create(
        run.keys.clone(),
        Arc::clone(&reservation),
    )?);
    let mut ordinal = 0_u64;
    let mut maximum_batch_bytes = 0_u64;
    for (batch_keys, batch) in batches {
        if batch_keys.len() != batch.num_rows() {
            return Err(CdfError::internal(
                "keyed-effect run batch and key counts differ",
            ));
        }
        arrow
            .write(&batch)
            .map_err(|error| scratch_arrow_error("write keyed-effect Arrow run", error))?;
        maximum_batch_bytes = maximum_batch_bytes.max(
            u64::try_from(batch.get_array_memory_size())
                .map_err(|_| CdfError::data("keyed-effect run batch bytes exceed u64"))?,
        );
        for key in batch_keys {
            write_key_record(&mut keys, &KeyRecord { key, ordinal })?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| CdfError::data("keyed-effect run row count overflowed u64"))?;
        }
    }
    arrow
        .finish()
        .map_err(|error| scratch_arrow_error("finish keyed-effect Arrow run", error))?;
    keys.flush()
        .map_err(|error| io_error("flush keyed-effect key run", &run.keys, error))?;
    Ok(maximum_batch_bytes)
}

fn merge_effect_runs(
    inputs: &[EffectRun],
    output: &EffectRun,
    schema: &arrow_schema::Schema,
    reservation: Arc<Mutex<SpillReservation>>,
    memory: Arc<dyn MemoryCoordinator>,
) -> Result<u64> {
    let input_bytes = inputs.iter().try_fold(0_u64, |total, run| {
        total
            .checked_add(run.maximum_batch_bytes)
            .ok_or_else(|| CdfError::data("keyed-effect merge working set overflowed u64"))
    })?;
    let output_bytes = input_bytes;
    let working_set = input_bytes
        .checked_add(output_bytes)
        .ok_or_else(|| CdfError::data("keyed-effect merge working set overflowed u64"))?
        .max(1);
    let request = ReservationRequest::new(
        ConsumerKey::new("keyed-effect-run-merge", MemoryClass::Validation)?,
        working_set,
    )?
    .as_minimum_working_set();
    let _lease = memory.try_reserve(&request)?.ok_or_else(|| {
        CdfError::data(format!(
            "canonical keyed-effect merge requires {working_set} bytes for its bounded fan-in; reduce source batch size or raise the memory budget"
        ))
    })?;
    let schema_ref = Arc::new(schema.clone());
    let mut readers = inputs
        .iter()
        .map(EffectRunReader::open)
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (reader, input) in readers.iter_mut().enumerate() {
        if let Some((key, batch, last_in_batch)) = input.next_row()? {
            heap.push(EffectMergeRow {
                key,
                batch,
                reader,
                last_in_batch,
            });
        }
    }
    let mut output_batches = Vec::new();
    let mut output_keys = Vec::new();
    let mut arrow = arrow_ipc::writer::StreamWriter::try_new(
        BudgetedSpillFile::create(output.arrow.clone(), Arc::clone(&reservation))?,
        schema,
    )
    .map_err(CdfError::from)?;
    let mut keys = BufWriter::new(BudgetedSpillFile::create(
        output.keys.clone(),
        Arc::clone(&reservation),
    )?);
    let mut ordinal = 0_u64;
    let mut maximum_batch_bytes = 0_u64;
    let mut previous = None::<Vec<u8>>;
    while let Some(item) = heap.pop() {
        if previous.as_ref().is_some_and(|key| key >= &item.key) {
            return Err(CdfError::internal(
                "keyed-effect merge inputs are not globally unique and sorted",
            ));
        }
        previous = Some(item.key.clone());
        output_keys.push(item.key);
        output_batches.push(item.batch);
        if output_batches.len() == 4_096 || item.last_in_batch {
            flush_effect_merge_output(
                &schema_ref,
                &mut output_batches,
                &mut output_keys,
                &mut arrow,
                &mut keys,
                &mut ordinal,
                &mut maximum_batch_bytes,
            )?;
        }
        if let Some((key, batch, last_in_batch)) = readers[item.reader].next_row()? {
            heap.push(EffectMergeRow {
                key,
                batch,
                reader: item.reader,
                last_in_batch,
            });
        }
    }
    flush_effect_merge_output(
        &schema_ref,
        &mut output_batches,
        &mut output_keys,
        &mut arrow,
        &mut keys,
        &mut ordinal,
        &mut maximum_batch_bytes,
    )?;
    arrow
        .finish()
        .map_err(|error| scratch_arrow_error("finish merged keyed-effect run", error))?;
    keys.flush()
        .map_err(|error| io_error("flush merged keyed-effect key run", &output.keys, error))?;
    Ok(maximum_batch_bytes)
}

#[allow(clippy::too_many_arguments)]
fn flush_effect_merge_output(
    schema: &arrow_schema::SchemaRef,
    batches: &mut Vec<arrow_array::RecordBatch>,
    batch_keys: &mut Vec<Vec<u8>>,
    arrow: &mut arrow_ipc::writer::StreamWriter<BudgetedSpillFile>,
    keys: &mut BufWriter<BudgetedSpillFile>,
    ordinal: &mut u64,
    maximum_batch_bytes: &mut u64,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }
    let batch =
        arrow_select::concat::concat_batches(schema, batches.iter()).map_err(CdfError::from)?;
    *maximum_batch_bytes = (*maximum_batch_bytes).max(
        u64::try_from(batch.get_array_memory_size())
            .map_err(|_| CdfError::data("merged effect batch bytes exceed u64"))?,
    );
    arrow
        .write(&batch)
        .map_err(|error| scratch_arrow_error("write merged keyed-effect run", error))?;
    for key in batch_keys.drain(..) {
        write_key_record(
            keys,
            &KeyRecord {
                key,
                ordinal: *ordinal,
            },
        )?;
        *ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("merged keyed-effect row count overflowed u64"))?;
    }
    batches.clear();
    Ok(())
}

fn remove_effect_run(run: &EffectRun, reservation: &Arc<Mutex<SpillReservation>>) -> Result<()> {
    let bytes = [&run.arrow, &run.keys]
        .into_iter()
        .try_fold(0_u64, |total, path| {
            total
                .checked_add(
                    fs::metadata(path)
                        .map_err(|error| io_error("stat consumed keyed-effect run", path, error))?
                        .len(),
                )
                .ok_or_else(|| CdfError::data("keyed-effect run byte count overflowed u64"))
        })?;
    for path in [&run.arrow, &run.keys] {
        fs::remove_file(path)
            .map_err(|error| io_error("remove consumed keyed-effect run", path, error))?;
    }
    reservation.lock_fail_stop().shrink(bytes);
    Ok(())
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
        let keys = BufWriter::new(BudgetedSpillFile::create(
            root.join("payload-keys.bin"),
            Arc::clone(&reservation),
        )?);
        Ok(Self {
            owner,
            reservation,
            writers: BTreeMap::new(),
            metadata,
            keys,
            schemas: BTreeMap::new(),
            input_rows: 0,
            input_bytes: 0,
        })
    }

    pub fn push(
        &mut self,
        kind: PackageSegmentKind,
        partition_ordinal: u64,
        output_position: Option<SourcePosition>,
        keys: &[Vec<u8>],
        batch: &arrow_array::RecordBatch,
    ) -> Result<()> {
        if keys.len() != batch.num_rows() {
            return Err(CdfError::internal(
                "dedup payload key count does not match its Arrow row count",
            ));
        }
        let family = effect_family(kind, batch.schema().as_ref())?;
        if let Some(schema) = self.schemas.get(&family) {
            if schema.as_ref() != batch.schema().as_ref() {
                return Err(CdfError::internal(
                    "canonical schema hash collision in dedup payload family",
                ));
            }
        } else {
            self.schemas.insert(family.clone(), batch.schema());
            self.writers.insert(
                family.clone(),
                arrow_ipc::writer::StreamWriter::try_new(
                    BudgetedSpillFile::create(
                        self.owner.root.join(payload_path(&family)),
                        Arc::clone(&self.reservation),
                    )?,
                    batch.schema().as_ref(),
                )
                .map_err(CdfError::from)?,
            );
        }
        self.writers
            .get_mut(&family)
            .ok_or_else(|| CdfError::internal("dedup payload writer was not initialized"))?
            .write(batch)
            .map_err(|error| scratch_arrow_error("write dedup payload", error))?;
        let mut metadata = serde_json::to_vec(&PayloadMetadata {
            kind,
            schema_hash: family.1,
            partition_ordinal,
            output_position,
        })
        .map_err(|error| {
            CdfError::internal(format!("serialize dedup payload metadata: {error}"))
        })?;
        metadata.push(b'\n');
        self.metadata
            .write_all(&metadata)
            .map_err(|error| io_error("write dedup payload metadata", &self.owner.root, error))?;
        for key in keys {
            write_key_record(
                &mut self.keys,
                &KeyRecord {
                    key: key.clone(),
                    ordinal: self.input_rows,
                },
            )?;
            self.input_rows = self
                .input_rows
                .checked_add(1)
                .ok_or_else(|| CdfError::data("dedup payload row count overflowed u64"))?;
        }
        self.input_bytes = self
            .input_bytes
            .saturating_add(batch.get_array_memory_size() as u64);
        Ok(())
    }

    pub fn finish(mut self) -> Result<Option<DedupPayloadReader>> {
        if self.writers.is_empty() {
            return Ok(None);
        }
        for writer in self.writers.values_mut() {
            writer
                .finish()
                .map_err(|error| scratch_arrow_error("finish dedup payload", error))?;
        }
        self.writers.clear();
        self.metadata
            .flush()
            .map_err(|error| io_error("flush dedup payload metadata", &self.owner.root, error))?;
        self.keys
            .flush()
            .map_err(|error| io_error("flush dedup payload keys", &self.owner.root, error))?;
        let readers = self
            .schemas
            .keys()
            .map(|family| {
                Ok((
                    family.clone(),
                    arrow_ipc::reader::StreamReader::try_new(
                        BufReader::new(
                            File::open(self.owner.root.join(payload_path(family))).map_err(
                                |error| {
                                    scratch_read_io_error(
                                        "open dedup payload",
                                        &self.owner.root,
                                        error,
                                    )
                                },
                            )?,
                        ),
                        None,
                    )
                    .map_err(|error| scratch_arrow_error("open dedup payload stream", error))?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Some(DedupPayloadReader {
            _owner: Arc::clone(&self.owner),
            _reservation: Arc::clone(&self.reservation),
            readers,
            metadata: BufReader::new(
                File::open(self.owner.root.join("payload-metadata.jsonl")).map_err(|error| {
                    scratch_read_io_error("open dedup payload metadata", &self.owner.root, error)
                })?,
            ),
            keys: KeyReader::open(&self.owner.root.join("payload-keys.bin"))?,
            next_ordinal: 0,
        }))
    }
}

impl DedupPayloadReader {
    pub fn next(&mut self) -> Result<Option<DedupPayload>> {
        let mut line = String::new();
        if self
            .metadata
            .read_line(&mut line)
            .map_err(|error| scratch_stream_io_error("read dedup payload metadata", error))?
            == 0
        {
            for reader in self.readers.values_mut() {
                if reader
                    .next()
                    .transpose()
                    .map_err(|error| scratch_arrow_error("read dedup payload tail", error))?
                    .is_some()
                {
                    return Err(CdfError::internal(
                        "CDF-managed dedup Arrow spool contains more batches than its metadata",
                    ));
                }
            }
            if self.keys.next()?.is_some() {
                return Err(CdfError::internal(
                    "CDF-managed dedup payload contains excess encoded keys",
                ));
            }
            return Ok(None);
        }
        let metadata = decode_payload_metadata(&line)?;
        let family = (metadata.kind, metadata.schema_hash);
        let batch = self
            .readers
            .get_mut(&family)
            .ok_or_else(|| CdfError::internal("dedup metadata names an absent effect spool"))?
            .next()
            .transpose()
            .map_err(|error| scratch_arrow_error("read dedup payload stream", error))?
            .ok_or_else(|| {
                CdfError::internal("CDF-managed dedup metadata outlived its Arrow effect spool")
            })?;
        let mut keys = Vec::with_capacity(batch.num_rows());
        for _ in 0..batch.num_rows() {
            let record = self.keys.next()?.ok_or_else(|| {
                CdfError::internal("CDF-managed dedup payload key stream ended early")
            })?;
            if record.ordinal != self.next_ordinal {
                return Err(CdfError::internal(
                    "CDF-managed dedup payload keys are not in input order",
                ));
            }
            self.next_ordinal += 1;
            keys.push(record.key);
        }
        Ok(Some(DedupPayload {
            kind: family.0,
            partition_ordinal: metadata.partition_ordinal,
            output_position: metadata.output_position,
            batch,
            keys,
        }))
    }
}

fn payload_path(family: &EffectFamily) -> String {
    let kind = match family.0 {
        PackageSegmentKind::Row => "rows",
        PackageSegmentKind::Upsert => "upserts",
        PackageSegmentKind::Delete => "deletes",
    };
    format!("payload-{kind}-{}.arrow", family_path_component(&family.1))
}

fn effect_family(kind: PackageSegmentKind, schema: &arrow_schema::Schema) -> Result<EffectFamily> {
    Ok((
        kind,
        cdf_kernel::canonical_arrow_schema_hash(schema)?
            .as_str()
            .to_owned(),
    ))
}

fn family_path_component(schema_hash: &str) -> &str {
    schema_hash.strip_prefix("sha256:").unwrap_or(schema_hash)
}

fn decode_payload_metadata(line: &str) -> Result<PayloadMetadata> {
    serde_json::from_str(line).map_err(|error| {
        CdfError::internal(format!(
            "decode CDF-managed dedup payload metadata: {error}"
        ))
    })
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
                let fast_keys = self.fast_keys.as_mut().ok_or_else(|| {
                    CdfError::internal("dedup fast-key storage is missing while its lease is live")
                })?;
                fast_keys.push(key);
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
        let spill_bytes = self.reservation.lock_fail_stop().bytes();
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
        let spill_bytes = self.reservation.lock_fail_stop().bytes();
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
        Ok(Self(BufReader::new(File::open(path).map_err(|error| {
            scratch_read_io_error("open key run", path, error)
        })?)))
    }

    fn next(&mut self) -> Result<Option<KeyRecord>> {
        let Some(length) = read_u32_or_eof(&mut self.0)? else {
            return Ok(None);
        };
        let length = usize::try_from(length).map_err(|_| {
            CdfError::internal("CDF-managed dedup key length exceeds platform usize")
        })?;
        if length > MAX_KEY_BYTES {
            return Err(CdfError::internal(format!(
                "CDF-managed dedup key length {length} exceeds {MAX_KEY_BYTES}-byte safety bound"
            )));
        }
        let mut key = vec![0; length];
        self.0
            .read_exact(&mut key)
            .map_err(|error| scratch_stream_io_error("read dedup key bytes", error))?;
        let ordinal = read_u64(&mut self.0)?;
        Ok(Some(KeyRecord { key, ordinal }))
    }
}

struct DecisionReader(BufReader<File>);

impl DecisionReader {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self(BufReader::new(File::open(path).map_err(|error| {
            scratch_read_io_error("open decision run", path, error)
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
        .map_err(|error| scratch_io_error("write dedup key record", error))
}

fn write_decision(writer: &mut impl Write, decision: DedupDecision) -> Result<()> {
    writer
        .write_all(&decision.ordinal.to_le_bytes())
        .and_then(|_| writer.write_all(&decision.kept_ordinal.to_le_bytes()))
        .map_err(|error| scratch_io_error("write dedup decision", error))
}

fn read_u32_or_eof(reader: &mut impl Read) -> Result<Option<u32>> {
    read_scratch_bytes_or_eof(reader, "read dedup u32").map(|bytes| bytes.map(u32::from_le_bytes))
}

fn read_u64_or_eof(reader: &mut impl Read) -> Result<Option<u64>> {
    read_scratch_bytes_or_eof(reader, "read dedup u64").map(|bytes| bytes.map(u64::from_le_bytes))
}

fn read_scratch_bytes_or_eof<const N: usize>(
    reader: &mut impl Read,
    action: &str,
) -> Result<Option<[u8; N]>> {
    let mut bytes = [0; N];
    let read = reader
        .read(&mut bytes)
        .map_err(|error| scratch_io_error(action, error))?;
    if read == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut bytes[read..]).map_err(|error| {
        if error.kind() == ErrorKind::UnexpectedEof {
            CdfError::internal(format!("{action}: CDF-managed scratch record is truncated"))
        } else {
            scratch_io_error(action, error)
        }
    })?;
    Ok(Some(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
    read_u64_or_eof(reader)?
        .ok_or_else(|| CdfError::internal("CDF-managed dedup record is truncated"))
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
        let mut reservation = self.reservation.lock_fail_stop();
        if !reservation
            .try_grow(additional)
            .map_err(std::io::Error::other)?
        {
            return Err(std::io::Error::other(CdfError::data(
                "shared spill budget exhausted before dedup scratch write",
            )));
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
    if let Some(error) = embedded_cdf_error(&error) {
        return error;
    }
    CdfError::environment(format!(
        "{action} {}: {error}; check scratch storage, permissions, free space, and process file limits before retrying",
        path.display()
    ))
}

fn scratch_io_error(action: &str, error: std::io::Error) -> CdfError {
    if let Some(error) = embedded_cdf_error(&error) {
        return error;
    }
    if matches!(
        error.kind(),
        ErrorKind::UnexpectedEof
            | ErrorKind::InvalidData
            | ErrorKind::NotADirectory
            | ErrorKind::IsADirectory
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::internal(format!("{action}: invalid CDF-managed scratch: {error}"))
    } else {
        CdfError::environment(format!(
            "{action}: {error}; check scratch storage and retry"
        ))
    }
}

fn scratch_read_io_error(action: &str, path: &Path, error: std::io::Error) -> CdfError {
    if matches!(
        error.kind(),
        ErrorKind::NotFound
            | ErrorKind::UnexpectedEof
            | ErrorKind::InvalidData
            | ErrorKind::NotADirectory
            | ErrorKind::IsADirectory
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::internal(format!(
            "{action} {}: invalid CDF-managed scratch: {error}",
            path.display()
        ))
    } else {
        io_error(action, path, error)
    }
}

fn scratch_stream_io_error(action: &str, error: std::io::Error) -> CdfError {
    if let Some(error) = embedded_cdf_error(&error) {
        return error;
    }
    if matches!(
        error.kind(),
        ErrorKind::NotFound
            | ErrorKind::UnexpectedEof
            | ErrorKind::InvalidData
            | ErrorKind::NotADirectory
            | ErrorKind::IsADirectory
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::internal(format!("{action}: invalid CDF-managed scratch: {error}"))
    } else {
        CdfError::environment(format!(
            "{action}: {error}; check scratch storage and retry"
        ))
    }
}

fn embedded_cdf_error(error: &std::io::Error) -> Option<CdfError> {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<CdfError>())
        .cloned()
}

fn scratch_arrow_error(action: &str, error: arrow_schema::ArrowError) -> CdfError {
    match error {
        arrow_schema::ArrowError::IoError(_, io_error) => scratch_io_error(action, io_error),
        error => CdfError::internal(format!("{action} in CDF-managed scratch: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, time::Instant};

    use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};

    #[test]
    fn spill_payload_and_effect_sort_preserve_heterogeneous_routed_schemas() {
        let temp = tempfile::tempdir().unwrap();
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(64 * 1024 * 1024).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            cdf_memory::DeterministicMemoryCoordinator::new(
                64 * 1024 * 1024,
                std::collections::BTreeMap::new(),
            )
            .unwrap(),
        );
        let one_column = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef,
        )])
        .unwrap();
        let two_columns = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef),
            (
                "source_collection",
                Arc::new(StringArray::from(vec!["orders"])) as ArrayRef,
            ),
        ])
        .unwrap();

        let mut payload =
            DedupPayloadSpool::create(temp.path().join("payload"), Arc::clone(&spill)).unwrap();
        payload
            .push(PackageSegmentKind::Upsert, 0, None, &[vec![2]], &one_column)
            .unwrap();
        payload
            .push(
                PackageSegmentKind::Upsert,
                0,
                None,
                &[vec![1]],
                &two_columns,
            )
            .unwrap();
        let mut payload = payload.finish().unwrap().unwrap();
        assert_eq!(payload.next().unwrap().unwrap().batch.num_columns(), 1);
        assert_eq!(payload.next().unwrap().unwrap().batch.num_columns(), 2);
        assert!(payload.next().unwrap().is_none());

        let mut sorter =
            EffectSortSpool::create(temp.path().join("effects"), spill, memory).unwrap();
        sorter
            .push(PackageSegmentKind::Upsert, None, one_column, vec![vec![2]])
            .unwrap();
        sorter
            .push(PackageSegmentKind::Upsert, None, two_columns, vec![vec![1]])
            .unwrap();
        let mut reader = sorter.finish().unwrap().unwrap();
        let mut column_counts = Vec::new();
        while let Some(effect) = reader.next().unwrap() {
            column_counts.push(effect.batch.num_columns());
        }
        column_counts.sort_unstable();
        assert_eq!(column_counts, vec![1, 2]);
    }

    #[test]
    fn effect_sort_spill_orders_each_typed_family_across_merge_levels() {
        let temp = tempfile::tempdir().unwrap();
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(64 * 1024 * 1024).unwrap());
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            cdf_memory::DeterministicMemoryCoordinator::new(
                64 * 1024 * 1024,
                std::collections::BTreeMap::new(),
            )
            .unwrap(),
        );
        let mut sorter =
            EffectSortSpool::create(temp.path().join("effects"), spill, memory).unwrap();

        for (kind, values) in [
            (PackageSegmentKind::Upsert, vec![6_i64, 1, 5, 2, 4, 3]),
            (PackageSegmentKind::Delete, vec![9_i64, 7, 8]),
        ] {
            for value in values {
                let array: ArrayRef = Arc::new(Int64Array::from(vec![value]));
                let batch = RecordBatch::try_from_iter([("id", array)]).unwrap();
                sorter
                    .push(kind, None, batch, vec![value.to_be_bytes().to_vec()])
                    .unwrap();
            }
        }

        let mut reader = sorter.finish().unwrap().unwrap();
        let mut observed = Vec::new();
        while let Some(effect) = reader.next().unwrap() {
            let values = effect
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            observed.extend(values.values().iter().map(|value| (effect.kind, *value)));
        }
        assert_eq!(
            observed,
            vec![
                (PackageSegmentKind::Upsert, 1),
                (PackageSegmentKind::Upsert, 2),
                (PackageSegmentKind::Upsert, 3),
                (PackageSegmentKind::Upsert, 4),
                (PackageSegmentKind::Upsert, 5),
                (PackageSegmentKind::Upsert, 6),
                (PackageSegmentKind::Delete, 7),
                (PackageSegmentKind::Delete, 8),
                (PackageSegmentKind::Delete, 9),
            ]
        );
    }

    #[test]
    fn corrupt_private_scratch_is_internal_while_clean_eof_is_empty() {
        let error = read_u32_or_eof(&mut std::io::Cursor::new(vec![1_u8, 2]))
            .expect_err("partial private framing must fail");
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
        assert!(error.message.contains("truncated"));
        assert_eq!(
            read_u32_or_eof(&mut std::io::Cursor::new(Vec::<u8>::new())).unwrap(),
            None
        );

        let error = decode_payload_metadata("{not-json").unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
        assert!(error.message.contains("CDF-managed"));

        let root = tempfile::tempdir().unwrap();
        let missing = match KeyReader::open(&root.path().join("missing.keys")) {
            Ok(_) => panic!("missing private run must fail"),
            Err(error) => error,
        };
        assert_eq!(missing.kind, cdf_kernel::ErrorKind::Internal);

        let invalid_utf8 = scratch_stream_io_error(
            "read dedup payload metadata",
            std::io::Error::new(ErrorKind::InvalidData, "invalid utf-8"),
        );
        assert_eq!(invalid_utf8.kind, cdf_kernel::ErrorKind::Internal);

        let directory = scratch_stream_io_error(
            "read dedup payload metadata",
            std::io::Error::new(ErrorKind::IsADirectory, "is a directory"),
        );
        assert_eq!(directory.kind, cdf_kernel::ErrorKind::Internal);
    }

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
    fn configured_spill_exhaustion_is_data_and_cleanup_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("spill");
        let budget: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(32).unwrap());
        let mut index =
            ExternalDedupIndex::create_with_sort_memory(&root, budget, None, 16).unwrap();
        index.push_keys(&[vec![7; 64]]).unwrap();
        let error = match index.finish(DedupKeepProgram::First) {
            Ok(_) => panic!("configured spill exhaustion must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("spill budget"));
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
            payload
                .push(
                    PackageSegmentKind::Row,
                    0,
                    None,
                    &[ordinal.to_le_bytes().to_vec()],
                    &batch,
                )
                .unwrap();
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
