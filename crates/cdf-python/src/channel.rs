use crate::*;

#[derive(Clone, Debug)]
pub struct BoundaryChannel {
    max_bytes: u64,
    queued_bytes: u64,
    batches: VecDeque<Batch>,
}

impl BoundaryChannel {
    pub fn new(max_bytes: u64) -> Result<Self> {
        if max_bytes == 0 {
            return Err(CdfError::contract(
                "boundary channel byte limit must be greater than zero",
            ));
        }
        Ok(Self {
            max_bytes,
            queued_bytes: 0,
            batches: VecDeque::new(),
        })
    }

    pub fn queued_bytes(&self) -> u64 {
        self.queued_bytes
    }

    pub fn len(&self) -> usize {
        self.batches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn try_push(&mut self, batch: Batch) -> Result<()> {
        let batch_bytes = batch.header.byte_count;
        if batch_bytes > self.max_bytes {
            return Err(CdfError::data(format!(
                "batch {} has {} bytes, exceeding Python boundary channel limit of {} bytes",
                batch.header.batch_id, batch_bytes, self.max_bytes
            )));
        }
        if self.queued_bytes.saturating_add(batch_bytes) > self.max_bytes {
            return Err(CdfError::rate_limited(
                "Python boundary channel is byte-bound and currently full",
                None,
            ));
        }
        self.queued_bytes += batch_bytes;
        self.batches.push_back(batch);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<Batch> {
        let batch = self.batches.pop_front()?;
        self.queued_bytes = self.queued_bytes.saturating_sub(batch.header.byte_count);
        Some(batch)
    }
}
