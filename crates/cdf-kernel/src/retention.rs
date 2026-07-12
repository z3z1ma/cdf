use std::{any::Any, sync::Arc};

use crate::{CdfError, Result};

#[derive(Clone)]
pub struct PayloadRetention {
    owner: Arc<dyn Any + Send + Sync>,
    bytes: u64,
}

impl std::fmt::Debug for PayloadRetention {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PayloadRetention")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl PayloadRetention {
    pub fn new(owner: Arc<dyn Any + Send + Sync>, bytes: u64) -> Result<Self> {
        if bytes == 0 {
            return Err(CdfError::contract(
                "payload retention must account for nonzero bytes",
            ));
        }
        Ok(Self { owner, bytes })
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.owner)
    }
}
