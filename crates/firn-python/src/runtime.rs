use crate::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Watchdog {
    timeout_ms: u64,
    started_at_ms: u64,
}

impl Watchdog {
    pub fn new(timeout_ms: u64, started_at_ms: u64) -> Result<Self> {
        if timeout_ms == 0 {
            return Err(FirnError::contract(
                "python watchdog must be greater than zero milliseconds",
            ));
        }
        Ok(Self {
            timeout_ms,
            started_at_ms,
        })
    }

    pub fn check(&self, now_ms: u64) -> Result<()> {
        let elapsed = now_ms.saturating_sub(self.started_at_ms);
        if elapsed > self.timeout_ms {
            Err(FirnError::transient(format!(
                "Python resource watchdog exceeded {} ms",
                self.timeout_ms
            )))
        } else {
            Ok(())
        }
    }
}
