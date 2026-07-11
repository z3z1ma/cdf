use std::{collections::BTreeMap, time::Instant};

use cdf_runtime::{
    AdmissionLimits, AdmissionRequest, CanonicalPartitionOrdinal, FairAdmissionController,
};

const RESOURCES: usize = 64;
const REQUESTS_PER_ROUND: usize = 4_096;
const ROUNDS: usize = 100;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut controller = FairAdmissionController::new(AdmissionLimits {
        jobs: 64,
        memory_bytes: 64 * 1024 * 1024,
        cpu_slots: 64,
        io_permits: 64,
        connection_permits: 64,
        quota_limits: BTreeMap::from([("shared-origin".to_owned(), 32)]),
    })?;
    let started = Instant::now();
    let mut admitted = 0_u64;
    for round in 0..ROUNDS {
        for request in 0..REQUESTS_PER_ROUND {
            controller.enqueue(AdmissionRequest {
                resource: format!("resource-{:02}", request % RESOURCES),
                ordinal: CanonicalPartitionOrdinal::new(u32::try_from(
                    round * REQUESTS_PER_ROUND + request,
                )?),
                memory_bytes: 1024 * 1024,
                cpu_slots: 1,
                io_permits: 1,
                connection_permits: 1,
                quota_authority: Some("shared-origin".to_owned()),
                scope_lease: None,
            })?;
        }
        while controller.snapshot().queued > 0 {
            let permit = controller
                .try_admit_next()
                .expect("one request must remain eligible when permits release immediately");
            controller.release(permit)?;
            admitted += 1;
        }
    }
    let elapsed = started.elapsed();
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.active, 0);
    assert_eq!(snapshot.queued, 0);
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "schema_version": 1,
            "resources": RESOURCES,
            "requests": admitted,
            "elapsed_ns": elapsed.as_nanos(),
            "nanoseconds_per_admission": elapsed.as_nanos() as f64 / admitted as f64,
            "admissions_per_second": admitted as f64 / elapsed.as_secs_f64(),
        }))?
    );
    Ok(())
}
