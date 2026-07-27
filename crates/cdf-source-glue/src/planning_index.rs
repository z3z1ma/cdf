use std::sync::Arc;

use cdf_kernel::{CdfError, Result};
use cdf_memory::MemoryCoordinator;
use cdf_runtime::{RunCancellation, SpillBudgetCoordinator};
use cdf_task_store::{
    CanonicalTaskSetLimits, ExternalTaskSetArtifact, ExternalTaskStore, TaskSetLimits,
    TypedCanonicalTaskSetBuilder,
};

use crate::{
    GLUE_TASK_SET_TYPE, GlueObjectTask, GlueSourceOptions, GlueTaskAuthority,
    task_reader::GlueTaskCodec,
};

/// Source-owned typed index over the shared canonical task-planning lifecycle.
///
/// The lower builder owns spill admission, canonical ordering, ordinal assignment, publication,
/// and cleanup. Glue retains the object key, duplicate semantics, estimates, and typed records.
pub struct GluePlanningIndex {
    builder: TypedCanonicalTaskSetBuilder<GlueTaskCodec>,
    object_count: u64,
    estimated_bytes: u64,
}

impl GluePlanningIndex {
    pub fn create(
        store: &ExternalTaskStore,
        source: &GlueSourceOptions,
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
        cancellation: RunCancellation,
    ) -> Result<Self> {
        let builder = TypedCanonicalTaskSetBuilder::new(
            store,
            GLUE_TASK_SET_TYPE,
            CanonicalTaskSetLimits {
                tasks: TaskSetLimits {
                    maximum_task_bytes: source.maximum_task_bytes,
                    maximum_authority_bytes: source.maximum_task_authority_bytes,
                    writer_buffer_bytes: source.task_writer_buffer_bytes,
                },
                maximum_sort_key_bytes: source.maximum_task_bytes,
                index_cache_bytes: source.maximum_task_bytes.clamp(64 * 1024, 8 * 1024 * 1024),
                spill_growth_bytes: source.planning_spill_growth_bytes,
                minimum_initial_spill_bytes: source.planning_spill_growth_bytes,
            },
            memory,
            spill,
            cancellation,
            GlueTaskCodec,
        )?;
        Ok(Self {
            builder,
            object_count: 0,
            estimated_bytes: 0,
        })
    }

    pub fn insert(&mut self, task: GlueObjectTask) -> Result<()> {
        let size_bytes = task.file.size_bytes;
        if self
            .builder
            .push_idempotent_by(task, |task| task.file.path.as_bytes())?
        {
            self.object_count = self
                .object_count
                .checked_add(1)
                .ok_or_else(|| CdfError::data("Glue planning object count overflowed"))?;
            self.estimated_bytes = self
                .estimated_bytes
                .checked_add(size_bytes)
                .ok_or_else(|| CdfError::data("Glue planning byte estimate overflowed"))?;
        }
        Ok(())
    }

    pub fn object_count(&self) -> Result<u64> {
        Ok(self.object_count)
    }

    pub fn estimated_bytes(&self) -> Result<u64> {
        Ok(self.estimated_bytes)
    }

    pub fn finalize(self, authority: &GlueTaskAuthority) -> Result<ExternalTaskSetArtifact> {
        self.builder.finalize(authority)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cdf_kernel::{CompiledScanIntent, ContentStoreNamespace, FilePosition};
    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
    use cdf_runtime::{FixedSpillBudget, SpillBudgetCoordinator};

    use super::*;
    use crate::{GLUE_TASK_AUTHORITY_VERSION, GLUE_TASK_VERSION, GlueFormatMapping};

    fn source(spill_growth_bytes: u64) -> GlueSourceOptions {
        let source: GlueSourceOptions = serde_json::from_value(serde_json::json!({
            "region": "us-west-2",
            "maximum_task_bytes": 4096,
            "maximum_task_authority_bytes": 4096,
            "task_writer_buffer_bytes": 8192,
            "planning_spill_growth_bytes": spill_growth_bytes
        }))
        .unwrap();
        source.validate().unwrap();
        source
    }

    fn authority() -> GlueTaskAuthority {
        GlueTaskAuthority {
            version: GLUE_TASK_AUTHORITY_VERSION,
            region: "us-west-2".to_owned(),
            catalog_id: None,
            database: "analytics".to_owned(),
            table: "events".to_owned(),
            table_generation: "fixture-generation".to_owned(),
            partition_expression: None,
            scan_intent: CompiledScanIntent::full_scan(),
        }
    }

    fn task(index: u64) -> GlueObjectTask {
        GlueObjectTask {
            version: GLUE_TASK_VERSION,
            canonical_ordinal: u64::MAX,
            file: FilePosition {
                path: format!("s3://fixture/events/{index:08}.parquet"),
                size_bytes: index + 1,
                source_generation: Some(format!("generation-{index}")),
                etag: None,
                object_version: None,
                sha256: None,
            },
            format: GlueFormatMapping {
                format_id: "parquet".to_owned(),
                options: serde_json::json!({}),
            },
            data_columns: vec!["id".to_owned()],
            partition_values: Vec::new(),
        }
    }

    #[test]
    fn high_cardinality_identity_is_order_and_spill_threshold_invariant() {
        const TASKS: u64 = 5_000;
        let mut references = Vec::new();
        for (growth, reverse) in [(16 * 1024, true), (64 * 1024, false)] {
            let root = tempfile::tempdir().unwrap();
            let store = ExternalTaskStore::new(
                root.path(),
                ContentStoreNamespace::new("glue-high-cardinality").unwrap(),
            )
            .unwrap();
            let memory: Arc<dyn MemoryCoordinator> =
                Arc::new(DeterministicMemoryCoordinator::new(256 * 1024, BTreeMap::new()).unwrap());
            let spill: Arc<dyn SpillBudgetCoordinator> =
                Arc::new(FixedSpillBudget::new(16 * 1024 * 1024).unwrap());
            let mut index = GluePlanningIndex::create(
                &store,
                &source(growth),
                Arc::clone(&memory),
                Arc::clone(&spill),
                RunCancellation::default(),
            )
            .unwrap();
            if reverse {
                for ordinal in (0..TASKS).rev() {
                    index.insert(task(ordinal)).unwrap();
                }
            } else {
                for ordinal in 0..TASKS {
                    index.insert(task(ordinal)).unwrap();
                }
            }
            index.insert(task(0)).unwrap();
            let mut conflict = task(0);
            conflict.file.size_bytes += 1;
            assert!(
                index
                    .insert(conflict)
                    .unwrap_err()
                    .message
                    .contains("conflicting payloads")
            );
            assert_eq!(index.object_count().unwrap(), TASKS);
            assert_eq!(index.estimated_bytes().unwrap(), TASKS * (TASKS + 1) / 2);
            let artifact = index.finalize(&authority()).unwrap();
            assert_eq!(artifact.task_count, TASKS);
            assert_eq!(memory.snapshot().current_bytes, 0);
            assert!(memory.snapshot().peak_bytes <= 256 * 1024);
            assert_eq!(spill.snapshot().current_bytes, 0);
            assert!(spill.snapshot().peak_bytes <= 16 * 1024 * 1024);
            references.push(artifact.reference);
        }
        assert_eq!(references[0], references[1]);
    }
}
