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
        fs::create_dir(&root)
            .map_err(|error| residual_io("create residual spill", &root, error))?;
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
                .map_err(|error| residual_json_write("write residual decision", error))?;
            writer
                .write_all(b"\n")
                .map_err(|error| residual_io("write residual decision", &self.root, error))?;
        }
        writer
            .flush()
            .map_err(|error| residual_io("flush residual decision run", &self.root, error))?;
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
        let result_path = self.run_path(level, 0);
        let reader = BufReader::new(File::open(&result_path).map_err(|error| {
            residual_scratch_read("open residual decision result", &result_path, error)
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
        if reader.read_line(&mut line).map_err(|error| {
            residual_scratch_read("read residual decision", Path::new("<stream>"), error)
        })? == 0
        {
            self.reader = None;
            return Ok(None);
        }
        serde_json::from_str(&line).map(Some).map_err(|error| {
            CdfError::internal(format!(
                "decode CDF-managed residual decision scratch: {error}"
            ))
        })
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
            File::open(path)
                .map(BufReader::new)
                .map_err(|error| residual_scratch_read("open residual run", path, error))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (reader, source) in readers.iter_mut().enumerate() {
        if let Some(decision) = read_decision(source)? {
            heap.push(HeapItem { decision, reader });
        }
    }
    let mut writer = BufWriter::new(BudgetedSpillFile::create(output.clone(), reservation)?);
    while let Some(item) = heap.pop() {
        serde_json::to_writer(&mut writer, &item.decision)
            .map_err(|error| residual_json_write("merge residual decision", error))?;
        writer
            .write_all(b"\n")
            .map_err(|error| residual_io("merge residual decision", &output, error))?;
        if let Some(decision) = read_decision(&mut readers[item.reader])? {
            heap.push(HeapItem {
                decision,
                reader: item.reader,
            });
        }
    }
    writer
        .flush()
        .map_err(|error| residual_io("flush residual merge", &output, error))
}

fn read_decision(reader: &mut BufReader<File>) -> Result<Option<ResidualDecisionArtifact>> {
    let mut line = String::new();
    if reader
        .read_line(&mut line)
        .map_err(|error| residual_scratch_read("read residual run", Path::new("<stream>"), error))?
        == 0
    {
        return Ok(None);
    }
    serde_json::from_str(&line).map(Some).map_err(|error| {
        CdfError::internal(format!("decode CDF-managed residual decision run: {error}"))
    })
}

fn residual_io(action: &str, path: &Path, error: std::io::Error) -> CdfError {
    if let Some(error) = embedded_cdf_source(&error) {
        return error;
    }
    CdfError::environment(format!(
        "{action} {}: {error}; check scratch storage, local permissions, free space, and process file limits before retrying",
        path.display()
    ))
}

fn residual_scratch_read(action: &str, path: &Path, error: std::io::Error) -> CdfError {
    if let Some(error) = embedded_cdf_source(&error) {
        return error;
    }
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidData
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::internal(format!(
            "{action} {}: invalid CDF-managed scratch: {error}",
            path.display()
        ))
    } else {
        residual_io(action, path, error)
    }
}

fn residual_json_write(action: &str, error: serde_json::Error) -> CdfError {
    if let Some(error) = embedded_cdf_source(&error) {
        return error;
    }
    if error.is_io() {
        CdfError::environment(format!(
            "{action}: {error}; check scratch storage, free space, and process file limits before retrying"
        ))
    } else {
        CdfError::internal(format!("{action}: {error}"))
    }
}

fn embedded_cdf_source(error: &(dyn std::error::Error + 'static)) -> Option<CdfError> {
    let mut source = Some(error);
    while let Some(current) = source {
        if let Some(error) = current.downcast_ref::<CdfError>() {
            return Some(error.clone());
        }
        if let Some(error) = current
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .and_then(|error| error.downcast_ref::<CdfError>())
        {
            return Some(error.clone());
        }
        source = current.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::variant_capture::{
        FieldTypeEvidenceArtifact, ResidualRuntimeVerdict, ResidualTypedProjection,
    };
    use cdf_contract::{CanonicalArrowType, RedactionDecision};
    use cdf_kernel::BatchId;

    fn decision() -> ResidualDecisionArtifact {
        ResidualDecisionArtifact {
            version: 1,
            observation_id: None,
            batch_id: BatchId::new("batch-1").unwrap(),
            source_row_ordinal: 0,
            source_path: vec!["value".to_owned()],
            observed_field: FieldTypeEvidenceArtifact {
                arrow_type: CanonicalArrowType::Boolean,
                nullable: false,
                semantic: None,
                metadata: Default::default(),
            },
            expected_field: None,
            verdict: ResidualRuntimeVerdict::Captured,
            rule_id: "residual".to_owned(),
            residual_encoding: "json-v1".to_owned(),
            typed_projection: ResidualTypedProjection::Absent,
            redaction: RedactionDecision::Preserve,
        }
    }

    #[test]
    fn governed_residual_spill_exhaustion_remains_data() {
        let root = tempfile::tempdir().unwrap();
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(32).unwrap());
        let mut runs = ResidualDecisionRuns::create(root.path().join("runs"), spill, None).unwrap();

        let error = runs.push(vec![decision()]).unwrap_err();

        assert_eq!(
            error.kind,
            cdf_kernel::ErrorKind::Data,
            "unexpected residual spill error: {error:?}"
        );
        assert!(error.message.contains("spill budget"));
    }

    #[test]
    fn missing_and_invalid_private_runs_are_internal() {
        let root = tempfile::tempdir().unwrap();
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(1024 * 1024).unwrap());
        let mut runs = ResidualDecisionRuns::create(root.path().join("runs"), spill, None).unwrap();
        runs.push(vec![decision()]).unwrap();
        fs::remove_file(runs.run_path(0, 0)).unwrap();
        let error = match runs.finish() {
            Ok(_) => panic!("missing private run must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);

        let invalid = root.path().join("invalid.jsonl");
        fs::write(&invalid, [0xff, b'\n']).unwrap();
        let mut reader = BufReader::new(File::open(invalid).unwrap());
        let error = read_decision(&mut reader).unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
        assert!(error.message.contains("CDF-managed scratch"));

        let directory = residual_scratch_read(
            "read residual run",
            Path::new("<stream>"),
            std::io::Error::new(std::io::ErrorKind::IsADirectory, "is a directory"),
        );
        assert_eq!(directory.kind, cdf_kernel::ErrorKind::Internal);
    }
}
