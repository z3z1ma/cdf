Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 C2 shared run work admission

## Observation

CLI execution and every registered file-format unit now share one invocation-local leaf-work ceiling. A run provisions the smaller of the explicit `--jobs` value and host CPU slots before source/destination resolution, then tightens that same shared object to the final scheduler result after source, destination, memory, and host capabilities are joined. The ceiling cannot increase or change after work begins.

Parent partition/format orchestration does not retain a permit while awaiting nested units. Every serial or concurrent registered decode unit holds exactly one permit until its decode stream reaches EOF or fails, including downstream backpressure. This removes the multiplicative `partition_jobs * unit_jobs` admission defect without creating a nested-semaphore deadlock or branching on a format/source identity.

## Procedure

- Added neutral `RunWorkPermit` lifecycle to `ExecutionServices` using only the runtime crate's existing futures/std boundary; no Tokio or adapter dependency entered the neutral runtime.
- Made acquisition cancellable, clone-shared, and wake blocked leaves on RAII release.
- Made final ceiling reduction fail if work already started and reject any attempted increase.
- Routed CLI run source construction, destination construction, scheduler resolution, and project execution through one invocation-local `ExecutionServices` clone.
- Acquired the permit inside the generic registered-format unit producer for both serial and parallel unit execution.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime run_work_admission --locked`: 1 passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files --locked`: 48 passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli run_local_parquet_discover_autopins_and_commits_pinned_schema --locked`: 1 passed and exercised an actual discover/plan/run/receipt/checkpoint path.
- `CARGO_BUILD_JOBS=12 cargo check -p cdf-source-files -p cdf-cli --all-targets --locked`: passed.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-source-files -p cdf-cli --all-targets --no-deps --locked -- -D warnings`: passed.

The deterministic permit law tightens 3 to 2, admits exactly two leaves, observes the third pending, rejects tightening with active work, wakes the third on release, cancels a blocked fourth, returns to zero, then permits a pre-execution tightening to 1.

## What this supports or challenges

This supports C2's global nested-work ceiling and makes `--jobs` truthful for registered file codecs across multiple concurrently opened partitions. It also provides the neutral admission hook future source/foreign-stream implementations can consume without adding runtime branches.

## Limits

This milestone does not close C2. Fair queued ordering, partition retries/reattest, exact limit/speculation, jobs 1/N package goldens across several simultaneously active multi-unit files, and CPU-vs-I/O executor separation remain open. Segment encoding already consumes the host-global CPU pool rather than this source leaf permit; the host pool remains the CPU oversubscription authority.
