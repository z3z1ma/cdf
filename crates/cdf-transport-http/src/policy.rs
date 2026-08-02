//! Fixed HTTP byte-source chunk and progress-deadline policy.

use std::time::Duration;

pub(crate) const MINIMUM_CHUNK_BYTES: u64 = 8 * 1024;
pub(crate) const MAXIMUM_CHUNK_BYTES: u64 = 32 * 1024 * 1024;
pub(crate) const FILE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const FILE_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
