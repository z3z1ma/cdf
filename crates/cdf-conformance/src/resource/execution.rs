use std::collections::{BTreeMap, BTreeSet};

use cdf_kernel::{
    Batch, FileManifest, PartitionId, PartitionPlan, ResourceDescriptor, ResourceStream,
    ScanRequest, SchemaHash, ScopeKind, SourcePosition,
};
use futures_util::StreamExt;

use super::{
    assert_descriptor_schema_coherence, assert_partition_plans, assert_request_targets_resource,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceExecutionConformanceCase {
    pub request: ScanRequest,
    pub expected_partition_ids: Vec<PartitionId>,
    pub expected_total_rows: u64,
    pub expected_partition_rows: Option<BTreeMap<PartitionId, u64>>,
    pub expected_schema_hash: SchemaHash,
    pub source_position_requirement: SourcePositionRequirement,
}

impl ResourceExecutionConformanceCase {
    pub fn new<I>(
        request: ScanRequest,
        expected_schema_hash: SchemaHash,
        expected_partition_ids: I,
        expected_total_rows: u64,
    ) -> Self
    where
        I: IntoIterator<Item = PartitionId>,
    {
        Self {
            request,
            expected_partition_ids: expected_partition_ids.into_iter().collect(),
            expected_total_rows,
            expected_partition_rows: None,
            expected_schema_hash,
            source_position_requirement: SourcePositionRequirement::NotRequired,
        }
    }

    pub fn with_expected_partition_rows<I>(mut self, expected_partition_rows: I) -> Self
    where
        I: IntoIterator<Item = (PartitionId, u64)>,
    {
        self.expected_partition_rows = Some(expected_partition_rows.into_iter().collect());
        self
    }

    pub fn with_source_position_requirement(
        mut self,
        requirement: SourcePositionRequirement,
    ) -> Self {
        self.source_position_requirement = requirement;
        self
    }

    pub fn require_file_manifest_positions(self) -> Self {
        self.with_source_position_requirement(SourcePositionRequirement::FileManifest)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SourcePositionRequirement {
    #[default]
    NotRequired,
    FileManifest,
}

pub async fn assert_resource_stream_execution_conformance<R, I>(resource: &R, cases: I)
where
    R: ResourceStream + ?Sized,
    I: IntoIterator<Item = ResourceExecutionConformanceCase>,
{
    let cases = cases.into_iter().collect::<Vec<_>>();
    assert!(
        !cases.is_empty(),
        "resource execution conformance requires representative scan cases"
    );

    assert_descriptor_schema_coherence(resource);

    for case in &cases {
        assert_execution_case_shape(case);
        assert_request_targets_resource(resource.descriptor(), &case.request);

        let partitions = resource
            .plan_partitions(&case.request)
            .unwrap_or_else(|error| panic!("resource partition planning failed: {error}"));
        assert_partition_plans(resource.descriptor(), None, &case.request, &partitions);
        assert_planned_partitions_match_expectation(case, &partitions);

        let mut seen_batch_ids = BTreeSet::new();
        let mut actual_partition_rows = partitions
            .iter()
            .map(|partition| (partition.partition_id.clone(), 0_u64))
            .collect::<BTreeMap<_, _>>();

        for partition in partitions {
            let mut stream = resource
                .open(partition.clone())
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "resource open failed for partition `{}`: {error}",
                        partition.partition_id
                    )
                });

            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap_or_else(|error| {
                    panic!(
                        "resource batch stream failed for partition `{}`: {error}",
                        partition.partition_id
                    )
                });
                let row_count = assert_batch_header_and_payload(
                    resource.descriptor(),
                    case,
                    &partition,
                    &batch,
                    &mut seen_batch_ids,
                );
                *actual_partition_rows
                    .get_mut(&partition.partition_id)
                    .expect("planned partition rows are initialized") += row_count;
            }
        }

        assert_partition_rows(case, &actual_partition_rows);
    }
}

fn assert_execution_case_shape(case: &ResourceExecutionConformanceCase) {
    assert!(
        !case.expected_partition_ids.is_empty(),
        "execution conformance requires at least one expected partition id"
    );
    let expected_ids = partition_id_set(&case.expected_partition_ids);
    assert_eq!(
        expected_ids.len(),
        case.expected_partition_ids.len(),
        "expected partition ids must be unique"
    );

    if let Some(expected_rows) = &case.expected_partition_rows {
        assert_eq!(
            expected_rows.keys().cloned().collect::<BTreeSet<_>>(),
            expected_ids,
            "per-partition row expectations must cover exactly the expected partitions"
        );
        assert_eq!(
            expected_rows.values().sum::<u64>(),
            case.expected_total_rows,
            "per-partition row expectations must sum to the total row expectation"
        );
    }
}

fn assert_planned_partitions_match_expectation(
    case: &ResourceExecutionConformanceCase,
    partitions: &[PartitionPlan],
) {
    assert_eq!(
        partitions
            .iter()
            .map(|partition| partition.partition_id.clone())
            .collect::<BTreeSet<_>>(),
        partition_id_set(&case.expected_partition_ids),
        "planned partition ids must match execution conformance expectations"
    );
}

fn assert_batch_header_and_payload(
    descriptor: &ResourceDescriptor,
    case: &ResourceExecutionConformanceCase,
    partition: &PartitionPlan,
    batch: &Batch,
    seen_batch_ids: &mut BTreeSet<cdf_kernel::BatchId>,
) -> u64 {
    assert_eq!(
        batch.header.resource_id, descriptor.resource_id,
        "batch resource id must match the candidate resource"
    );
    assert_eq!(
        batch.header.partition_id, partition.partition_id,
        "batch partition id must match the opened partition"
    );
    assert!(
        !batch.header.batch_id.as_str().trim().is_empty(),
        "batch id must not be empty"
    );
    assert!(
        seen_batch_ids.insert(batch.header.batch_id.clone()),
        "batch id `{}` is emitted more than once",
        batch.header.batch_id
    );
    assert_eq!(
        batch.header.observed_schema_hash, case.expected_schema_hash,
        "batch observed schema hash must match the expected resource schema hash"
    );

    let record_batch = batch
        .record_batch()
        .unwrap_or_else(|| panic!("resource execution requires RecordBatch payloads at MVP"));
    assert_eq!(
        batch.header.row_count,
        record_batch.num_rows() as u64,
        "batch row_count must match the in-memory RecordBatch"
    );
    assert_eq!(
        batch.header.byte_count,
        record_batch.get_array_memory_size() as u64,
        "batch byte_count must match the in-memory RecordBatch"
    );
    assert_source_position(descriptor, case, partition, batch);

    record_batch.num_rows() as u64
}

fn assert_source_position(
    descriptor: &ResourceDescriptor,
    case: &ResourceExecutionConformanceCase,
    partition: &PartitionPlan,
    batch: &Batch,
) {
    if case.source_position_requirement == SourcePositionRequirement::FileManifest
        || descriptor.state_scope.kind() == ScopeKind::File
        || partition.scope.kind() == ScopeKind::File
        || matches!(
            &partition.start_position,
            Some(SourcePosition::FileManifest(_))
        )
    {
        let Some(SourcePosition::FileManifest(manifest)) = &batch.header.source_position else {
            panic!("file-scoped resource batches must carry a FileManifest source position");
        };
        assert_file_manifest(manifest);
    }
}

fn assert_file_manifest(manifest: &FileManifest) {
    assert!(
        !manifest.files.is_empty(),
        "FileManifest source positions must include at least one file"
    );
    for file in &manifest.files {
        assert!(
            !file.path.trim().is_empty(),
            "FileManifest file path must not be empty"
        );
        assert!(
            file.size_bytes > 0,
            "FileManifest file size evidence must be nonzero"
        );
        let Some(sha256) = &file.sha256 else {
            panic!("FileManifest file must carry SHA-256 evidence");
        };
        assert!(
            sha256.len() == 64
                && sha256
                    .chars()
                    .all(|character| character.is_ascii_hexdigit()),
            "FileManifest SHA-256 evidence must be a 64-character hex digest"
        );
    }
}

fn assert_partition_rows(
    case: &ResourceExecutionConformanceCase,
    actual_partition_rows: &BTreeMap<PartitionId, u64>,
) {
    let actual_total_rows = actual_partition_rows.values().sum::<u64>();
    assert_eq!(
        actual_total_rows, case.expected_total_rows,
        "executed partition union row count must match the total expectation"
    );
    if let Some(expected_partition_rows) = &case.expected_partition_rows {
        assert_eq!(
            actual_partition_rows, expected_partition_rows,
            "executed partition row counts must match per-partition expectations"
        );
    }
}

fn partition_id_set(partition_ids: &[PartitionId]) -> BTreeSet<PartitionId> {
    partition_ids.iter().cloned().collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::Arc,
    };

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use cdf_kernel::{
        BatchId, BatchPayload, BatchStats, BoxFuture, CdfError, FilePosition, PayloadRef,
        ResourceId, Result, SchemaSource, ScopeKey, TrustLevel, WriteDisposition,
    };
    use futures_util::stream;

    use super::*;

    #[test]
    fn sound_resource_passes_execution_conformance() {
        let resource = FaultyExecutionResource::sound();
        futures_executor::block_on(assert_resource_stream_execution_conformance(
            &resource,
            [case()],
        ));
    }

    #[test]
    fn negative_self_tests_prove_execution_harness_catches_contract_violations() {
        for fault in [
            Fault::WrongResourceId,
            Fault::WrongPartitionId,
            Fault::DuplicateBatchId,
            Fault::BadRowCount,
            Fault::BadByteCount,
            Fault::BadSchemaHash,
            Fault::MissingExpectedPartition,
            Fault::DuplicatePartitionData,
            Fault::MissingFilePosition,
            Fault::NonRecordBatchPayload,
        ] {
            assert_execution_harness_panics(FaultyExecutionResource::with_fault(fault));
        }
    }

    #[test]
    fn case_shape_negative_self_tests_prove_expectation_guardrails_are_active() {
        let p0 = PartitionId::new("file-part-0").unwrap();
        let p1 = PartitionId::new("file-part-1").unwrap();
        let p2 = PartitionId::new("file-part-2").unwrap();

        let duplicate_expected_partition = ResourceExecutionConformanceCase::new(
            request(),
            schema_hash(),
            [p0.clone(), p1.clone(), p1.clone()],
            4,
        )
        .with_expected_partition_rows([(p0.clone(), 2), (p1.clone(), 2)])
        .require_file_manifest_positions();
        assert_execution_case_panics(duplicate_expected_partition, "duplicate expected partition");

        let wrong_expected_partition =
            ResourceExecutionConformanceCase::new(request(), schema_hash(), [p0, p2], 4)
                .require_file_manifest_positions();
        assert_execution_case_panics(wrong_expected_partition, "wrong expected partition");
    }

    #[test]
    fn source_position_triggers_independently_require_file_manifests() {
        for scenario in [
            SourcePositionScenario {
                name: "explicit requirement",
                requirement: SourcePositionRequirement::FileManifest,
                descriptor_scope: ScopeKey::Resource,
                partition_scope: ScopeKey::Resource,
                start_position: None,
            },
            SourcePositionScenario {
                name: "descriptor file scope",
                requirement: SourcePositionRequirement::NotRequired,
                descriptor_scope: file_scope("descriptor.csv"),
                partition_scope: ScopeKey::Resource,
                start_position: None,
            },
            SourcePositionScenario {
                name: "partition file scope",
                requirement: SourcePositionRequirement::NotRequired,
                descriptor_scope: ScopeKey::Resource,
                partition_scope: file_scope("partition.csv"),
                start_position: None,
            },
            SourcePositionScenario {
                name: "start-position file manifest",
                requirement: SourcePositionRequirement::NotRequired,
                descriptor_scope: ScopeKey::Resource,
                partition_scope: ScopeKey::Resource,
                start_position: Some(file_position("start-position.csv")),
            },
        ] {
            assert_source_position_panics(scenario, batch_without_position());
        }
    }

    #[test]
    fn bad_file_manifest_contents_fail_source_position_honesty() {
        for (name, manifest) in [
            (
                "empty file list",
                FileManifest {
                    version: 1,
                    files: Vec::new(),
                },
            ),
            (
                "empty path",
                FileManifest {
                    version: 1,
                    files: vec![FilePosition {
                        path: String::new(),
                        size_bytes: 42,
                        source_generation: None,
                        etag: None,
                        object_version: None,
                        sha256: Some("0".repeat(64)),
                    }],
                },
            ),
            (
                "zero size",
                FileManifest {
                    version: 1,
                    files: vec![FilePosition {
                        path: "orders.csv".to_owned(),
                        size_bytes: 0,
                        source_generation: None,
                        etag: None,
                        object_version: None,
                        sha256: Some("0".repeat(64)),
                    }],
                },
            ),
            (
                "missing sha256",
                FileManifest {
                    version: 1,
                    files: vec![FilePosition {
                        path: "orders.csv".to_owned(),
                        size_bytes: 42,
                        source_generation: None,
                        etag: None,
                        object_version: None,
                        sha256: None,
                    }],
                },
            ),
            (
                "bad sha256",
                FileManifest {
                    version: 1,
                    files: vec![FilePosition {
                        path: "orders.csv".to_owned(),
                        size_bytes: 42,
                        source_generation: None,
                        etag: None,
                        object_version: None,
                        sha256: Some("not-a-sha256".to_owned()),
                    }],
                },
            ),
        ] {
            let mut batch = batch_without_position();
            batch.header.source_position = Some(SourcePosition::FileManifest(manifest));
            assert_source_position_panics(
                SourcePositionScenario {
                    name,
                    requirement: SourcePositionRequirement::FileManifest,
                    descriptor_scope: ScopeKey::Resource,
                    partition_scope: ScopeKey::Resource,
                    start_position: None,
                },
                batch,
            );
        }
    }

    fn assert_execution_harness_panics(resource: FaultyExecutionResource) {
        let fault = resource.fault.expect("faulty resource must carry a fault");
        let result = catch_unwind(AssertUnwindSafe(|| {
            futures_executor::block_on(assert_resource_stream_execution_conformance(
                &resource,
                [case()],
            ));
        }));
        assert!(result.is_err(), "fault {fault:?} passed conformance");
    }

    fn assert_execution_case_panics(
        conformance_case: ResourceExecutionConformanceCase,
        case_name: &str,
    ) {
        let resource = FaultyExecutionResource::sound();
        let result = catch_unwind(AssertUnwindSafe(|| {
            futures_executor::block_on(assert_resource_stream_execution_conformance(
                &resource,
                [conformance_case],
            ));
        }));
        assert!(result.is_err(), "case `{case_name}` passed conformance");
    }

    fn assert_source_position_panics(scenario: SourcePositionScenario, batch: Batch) {
        let descriptor = descriptor_with_scope(scenario.descriptor_scope);
        let mut conformance_case = case();
        conformance_case.source_position_requirement = scenario.requirement;
        let partition = PartitionPlan {
            partition_id: PartitionId::new("file-part-0").unwrap(),
            scope: scenario.partition_scope,
            start_position: scenario.start_position,
            metadata: BTreeMap::new(),
        };

        let result = catch_unwind(AssertUnwindSafe(|| {
            assert_source_position(&descriptor, &conformance_case, &partition, &batch);
        }));
        assert!(
            result.is_err(),
            "source-position scenario `{}` passed conformance",
            scenario.name
        );
    }

    #[derive(Debug)]
    struct SourcePositionScenario {
        name: &'static str,
        requirement: SourcePositionRequirement,
        descriptor_scope: ScopeKey,
        partition_scope: ScopeKey,
        start_position: Option<SourcePosition>,
    }

    fn case() -> ResourceExecutionConformanceCase {
        let p0 = PartitionId::new("file-part-0").unwrap();
        let p1 = PartitionId::new("file-part-1").unwrap();
        ResourceExecutionConformanceCase::new(request(), schema_hash(), [p0.clone(), p1.clone()], 4)
            .with_expected_partition_rows([(p0, 2), (p1, 2)])
            .require_file_manifest_positions()
    }

    fn request() -> ScanRequest {
        ScanRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: file_scope("orders.csv"),
        }
    }

    #[derive(Clone, Debug)]
    struct FaultyExecutionResource {
        descriptor: ResourceDescriptor,
        schema: SchemaRef,
        fault: Option<Fault>,
    }

    #[derive(Clone, Copy, Debug)]
    enum Fault {
        WrongResourceId,
        WrongPartitionId,
        DuplicateBatchId,
        BadRowCount,
        BadByteCount,
        BadSchemaHash,
        MissingExpectedPartition,
        DuplicatePartitionData,
        MissingFilePosition,
        NonRecordBatchPayload,
    }

    impl FaultyExecutionResource {
        fn sound() -> Self {
            Self {
                descriptor: descriptor_with_scope(file_scope("orders.csv")),
                schema: schema(),
                fault: None,
            }
        }

        fn with_fault(fault: Fault) -> Self {
            let mut resource = Self::sound();
            resource.fault = Some(fault);
            resource
        }
    }

    fn descriptor_with_scope(state_scope: ScopeKey) -> ResourceDescriptor {
        ResourceDescriptor {
            resource_id: ResourceId::new("orders").unwrap(),
            schema_source: SchemaSource::Declared {
                schema_hash: schema_hash(),
                source: "fixture:orders".to_owned(),
            },
            primary_key: vec!["id".to_owned()],
            merge_key: Vec::new(),
            cursor: None,
            write_disposition: WriteDisposition::Append,
            deduplication: None,
            contract: None,
            state_scope,
            freshness: None,
            trust_level: TrustLevel::Experimental,
        }
    }

    impl ResourceStream for FaultyExecutionResource {
        fn descriptor(&self) -> &ResourceDescriptor {
            &self.descriptor
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
            let ids = if matches!(self.fault, Some(Fault::MissingExpectedPartition)) {
                vec!["file-part-0"]
            } else {
                vec!["file-part-0", "file-part-1"]
            };

            ids.into_iter()
                .map(|id| {
                    Ok(PartitionPlan {
                        partition_id: PartitionId::new(id)?,
                        scope: ScopeKey::File {
                            path: format!("fixture/{id}.csv"),
                        },
                        start_position: Some(file_position(id)),
                        metadata: BTreeMap::from([(
                            "resource_id".to_owned(),
                            self.descriptor.resource_id.as_str().to_owned(),
                        )]),
                    })
                })
                .collect()
        }

        fn open(
            &self,
            partition: PartitionPlan,
        ) -> BoxFuture<'_, Result<cdf_kernel::OpenedPartitionStream>> {
            let fault = self.fault;
            Box::pin(async move {
                let batches = batches_for_partition(&partition.partition_id, fault)?;
                let stream =
                    Box::pin(stream::iter(batches.into_iter().map(Ok))) as cdf_kernel::BatchStream;
                Ok(cdf_kernel::OpenedPartitionStream::without_completion(
                    stream,
                ))
            })
        }
    }

    fn batches_for_partition(
        partition_id: &PartitionId,
        fault: Option<Fault>,
    ) -> Result<Vec<Batch>> {
        let rows = match partition_id.as_str() {
            "file-part-0" => [1, 2],
            "file-part-1" => [3, 4],
            other => return Err(CdfError::internal(format!("unexpected partition {other}"))),
        };
        let mut batches = vec![batch(partition_id, &rows, 1, fault)?];
        if matches!(fault, Some(Fault::DuplicatePartitionData))
            && partition_id.as_str() == "file-part-0"
        {
            batches.push(batch(partition_id, &rows, 2, None)?);
        }
        Ok(batches)
    }

    fn batch(
        opened_partition_id: &PartitionId,
        rows: &[i64; 2],
        ordinal: usize,
        fault: Option<Fault>,
    ) -> Result<Batch> {
        let batch_id_partition = if matches!(fault, Some(Fault::DuplicateBatchId)) {
            "file-part-0"
        } else {
            opened_partition_id.as_str()
        };
        let mut batch = Batch::from_record_batch(
            BatchId::new(format!("{batch_id_partition}-{ordinal}"))?,
            if matches!(fault, Some(Fault::WrongResourceId)) {
                ResourceId::new("other.orders")?
            } else {
                ResourceId::new("orders")?
            },
            if matches!(fault, Some(Fault::WrongPartitionId)) {
                PartitionId::new("wrong-partition")?
            } else {
                opened_partition_id.clone()
            },
            if matches!(fault, Some(Fault::BadSchemaHash)) {
                SchemaHash::new("sha256:bad-schema")?
            } else {
                schema_hash()
            },
            record_batch(opened_partition_id.as_str(), rows),
        )?;
        if matches!(fault, Some(Fault::BadRowCount)) {
            batch.header.row_count += 1;
        }
        if matches!(fault, Some(Fault::BadByteCount)) {
            batch.header.byte_count += 1;
        }
        if !matches!(fault, Some(Fault::MissingFilePosition)) {
            batch.header.source_position = Some(file_position(opened_partition_id.as_str()));
        }
        if matches!(fault, Some(Fault::NonRecordBatchPayload)) {
            batch.payload = BatchPayload::Reference(PayloadRef {
                uri: "memory://not-a-record-batch".to_owned(),
                byte_count: batch.header.byte_count,
                sha256: Some("0".repeat(64)),
            });
        }
        batch.header.stats = BatchStats::default();
        Ok(batch)
    }

    fn batch_without_position() -> Batch {
        batch(
            &PartitionId::new("file-part-0").unwrap(),
            &[1, 2],
            1,
            Some(Fault::MissingFilePosition),
        )
        .unwrap()
    }

    fn record_batch(partition: &str, rows: &[i64; 2]) -> RecordBatch {
        let ids: ArrayRef = Arc::new(Int64Array::from(rows.to_vec()));
        let partitions: ArrayRef = Arc::new(StringArray::from(vec![partition; rows.len()]));
        RecordBatch::try_new(schema(), vec![ids, partitions]).unwrap()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("partition", DataType::Utf8, false),
        ]))
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::new("sha256:execution-conformance").unwrap()
    }

    fn file_scope(path: &str) -> ScopeKey {
        ScopeKey::File {
            path: format!("fixture/{path}"),
        }
    }

    fn file_position(path: &str) -> SourcePosition {
        SourcePosition::FileManifest(FileManifest {
            version: 1,
            files: vec![FilePosition {
                path: path.to_owned(),
                size_bytes: 42,
                source_generation: None,
                etag: None,
                object_version: None,
                sha256: Some("0".repeat(64)),
            }],
        })
    }
}
