use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use cdf_kernel::{CdfError, Result};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest};
use cdf_runtime::{SpillBudgetCoordinator, SpillReservation};

use crate::{
    dedup_spill::BudgetedSpillFile,
    variant_capture::{ResidualDecisionArtifact, residual_decision_cmp},
};

const MERGE_FAN_IN: u64 = 32;
const MERGE_MEMORY_BYTES: u64 = 8 * 1024 * 1024;

pub(crate) struct ResidualDecisionRuns {
    root: PathBuf,
    reservation: Arc<Mutex<SpillReservation>>,
    memory_lease: Option<MemoryLease>,
    run_count: u64,
}

pub(crate) struct ResidualDecisionReader {
    _runs: ResidualDecisionRuns,
    reader: Option<BufReader<File>>,
}

impl ResidualDecisionRuns {
    pub(crate) fn create(
        root: impl AsRef<Path>,
        spill: Arc<dyn SpillBudgetCoordinator>,
        memory: Option<Arc<dyn MemoryCoordinator>>,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir(&root).map_err(|error| {
            CdfError::data(format!("create residual spill {}: {error}", root.display()))
        })?;
        let reservation = spill
            .try_reserve(1)?
            .ok_or_else(|| CdfError::data("residual decision spill budget is exhausted"))?;
        let memory_lease = match memory {
            Some(memory) => Some(
                memory
                    .try_reserve(&ReservationRequest::new(
                        ConsumerKey::new("residual-decision-sort", MemoryClass::Validation)?,
                        MERGE_MEMORY_BYTES,
                    )?)?
                    .ok_or_else(|| {
                        CdfError::data(
                            "residual decision external sort requires 8 MiB of managed memory",
                        )
                    })?,
            ),
            None => None,
        };
        Ok(Self {
            root,
            reservation: Arc::new(Mutex::new(reservation)),
            memory_lease,
            run_count: 0,
        })
    }

    pub(crate) fn push(&mut self, mut decisions: Vec<ResidualDecisionArtifact>) -> Result<()> {
        if decisions.is_empty() {
            return Ok(());
        }
        decisions.sort_by(residual_decision_cmp);
        let path = self.run_path(0, self.run_count);
        let mut writer = BufWriter::new(BudgetedSpillFile::create(
            path,
            Arc::clone(&self.reservation),
        )?);
        for decision in decisions {
            serde_json::to_writer(&mut writer, &decision)
                .map_err(|error| CdfError::data(format!("write residual decision: {error}")))?;
            writer
                .write_all(b"\n")
                .map_err(|error| CdfError::data(format!("write residual decision: {error}")))?;
        }
        writer
            .flush()
            .map_err(|error| CdfError::data(format!("flush residual decision run: {error}")))?;
        self.run_count += 1;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<Option<ResidualDecisionReader>> {
        if self.run_count == 0 {
            return Ok(None);
        }
        let mut level = 0_u32;
        let mut count = self.run_count;
        while count > 1 {
            let next_count = count.div_ceil(MERGE_FAN_IN);
            for output in 0..next_count {
                let start = output * MERGE_FAN_IN;
                let end = (start + MERGE_FAN_IN).min(count);
                let inputs = (start..end)
                    .map(|run| self.run_path(level, run))
                    .collect::<Vec<_>>();
                merge_runs(
                    &inputs,
                    self.run_path(level + 1, output),
                    Arc::clone(&self.reservation),
                )?;
            }
            level += 1;
            count = next_count;
        }
        let reader =
            BufReader::new(File::open(self.run_path(level, 0)).map_err(|error| {
                CdfError::data(format!("open residual decision result: {error}"))
            })?);
        self.run_count = 1;
        Ok(Some(ResidualDecisionReader {
            _runs: self,
            reader: Some(reader),
        }))
    }

    fn run_path(&self, level: u32, run: u64) -> PathBuf {
        self.root.join(format!("run-{level:03}-{run:012}.jsonl"))
    }
}

impl Drop for ResidualDecisionRuns {
    fn drop(&mut self) {
        let _ = self.memory_lease.take();
        let _ = fs::remove_dir_all(&self.root);
    }
}

impl ResidualDecisionReader {
    pub(crate) fn next(&mut self) -> Result<Option<ResidualDecisionArtifact>> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(None);
        };
        let mut line = String::new();
        if reader
            .read_line(&mut line)
            .map_err(|error| CdfError::data(format!("read residual decision: {error}")))?
            == 0
        {
            self.reader = None;
            return Ok(None);
        }
        serde_json::from_str(&line)
            .map(Some)
            .map_err(|error| CdfError::data(format!("decode residual decision: {error}")))
    }
}

struct HeapItem {
    decision: ResidualDecisionArtifact,
    reader: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.reader == other.reader && self.decision == other.decision
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        residual_decision_cmp(&other.decision, &self.decision)
            .then_with(|| other.reader.cmp(&self.reader))
    }
}

fn merge_runs(
    inputs: &[PathBuf],
    output: PathBuf,
    reservation: Arc<Mutex<SpillReservation>>,
) -> Result<()> {
    let mut readers = inputs
        .iter()
        .map(|path| {
            File::open(path).map(BufReader::new).map_err(|error| {
                CdfError::data(format!("open residual run {}: {error}", path.display()))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (reader, source) in readers.iter_mut().enumerate() {
        if let Some(decision) = read_decision(source)? {
            heap.push(HeapItem { decision, reader });
        }
    }
    let mut writer = BufWriter::new(BudgetedSpillFile::create(output, reservation)?);
    while let Some(item) = heap.pop() {
        serde_json::to_writer(&mut writer, &item.decision)
            .map_err(|error| CdfError::data(format!("merge residual decision: {error}")))?;
        writer
            .write_all(b"\n")
            .map_err(|error| CdfError::data(format!("merge residual decision: {error}")))?;
        if let Some(decision) = read_decision(&mut readers[item.reader])? {
            heap.push(HeapItem {
                decision,
                reader: item.reader,
            });
        }
    }
    writer
        .flush()
        .map_err(|error| CdfError::data(format!("flush residual merge: {error}")))
}

fn read_decision(reader: &mut BufReader<File>) -> Result<Option<ResidualDecisionArtifact>> {
    let mut line = String::new();
    if reader
        .read_line(&mut line)
        .map_err(|error| CdfError::data(format!("read residual run: {error}")))?
        == 0
    {
        return Ok(None);
    }
    serde_json::from_str(&line)
        .map(Some)
        .map_err(|error| CdfError::data(format!("decode residual run: {error}")))
}
