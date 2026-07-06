use std::collections::BTreeMap;

use arrow_schema::SchemaRef;
use firn_kernel::{
    BatchStream, BoxFuture, FirnError, PartitionPlan, ResourceDescriptor, ResourceStream, Result,
    ScanRequest,
};
use futures_util::stream;

use crate::{FileSource, read_file_source};

#[derive(Clone, Debug)]
pub struct FileResource {
    source: FileSource,
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    partition: PartitionPlan,
}

impl FileResource {
    pub fn new(source: FileSource) -> Result<Self> {
        let read = read_file_source(&source)?;
        let schema = read
            .batches
            .first()
            .and_then(|batch| batch.record_batch())
            .map(|batch| batch.schema())
            .ok_or_else(|| FirnError::data("file resource did not produce a RecordBatch schema"))?;
        let start_position = read
            .batches
            .first()
            .and_then(|batch| batch.header.source_position.clone());
        let partition = PartitionPlan {
            partition_id: source.options.partition_id.clone(),
            scope: read.descriptor.state_scope.clone(),
            start_position,
            metadata: BTreeMap::from([(
                "resource_id".to_owned(),
                source.options.resource_id.as_str().to_owned(),
            )]),
        };
        Ok(Self {
            source,
            descriptor: read.descriptor,
            schema,
            partition,
        })
    }

    pub fn source(&self) -> &FileSource {
        &self.source
    }

    pub fn partition(&self) -> &PartitionPlan {
        &self.partition
    }
}

impl ResourceStream for FileResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        if request.resource_id != self.descriptor.resource_id {
            return Err(FirnError::contract("file resource id mismatch"));
        }
        Ok(vec![self.partition.clone()])
    }

    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>> {
        let source = self.source.clone();
        let expected_partition = self.partition.clone();
        Box::pin(async move {
            if partition.partition_id != expected_partition.partition_id {
                return Err(FirnError::contract("file resource partition id mismatch"));
            }
            if partition.scope != expected_partition.scope {
                return Err(FirnError::contract(
                    "file resource partition scope mismatch",
                ));
            }
            let read = read_file_source(&source)?;
            Ok(Box::pin(stream::iter(read.batches.into_iter().map(Ok))) as BatchStream)
        })
    }
}
