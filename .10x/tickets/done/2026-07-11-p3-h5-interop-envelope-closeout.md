Status: done
Created: 2026-07-11
Updated: 2026-07-25
Parent: .10x/tickets/done/2026-07-10-p3-ws-h-interop-boundaries.md
Depends-On: .10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md, .10x/tickets/done/2026-07-11-p3-h3-subprocess-stream-supervision.md, .10x/tickets/done/2026-07-11-p3-f3-stress-generators-laws.md

# P3 H5: interop conformance and envelope closeout

## Scope

Run the shared foreign producer matrix, constant-memory/chaos/jobs laws, publish honest mode-specific envelope guidance, reconcile the interop triage, and close WS-H only from raw evidence.

## Acceptance criteria

- Implemented Python/subprocess modes pass shared semantics, memory, cancellation, redaction, recovery, and determinism conformance.
- Performance docs/sheets distinguish verified zero-copy, IPC, and row compatibility with host labels.
- WASM remains prospective/unknown where not executable.
- Interop triage is terminal by absorption and all claims link raw evidence.

## Evidence expectations

Full reports/profiles/copy proofs, stress/chaos/package hashes, generated envelope cells, docs diff, triage reconciliation, and adversarial performance/security review.

## Explicit exclusions

No Wasmtime host or native-speed guarantee for compatibility rows.

## Blockers

None. H2, H3, and F3 are complete. WASM remains prospective and outside the implemented-runtime
envelope.

## Journal

- 2026-07-19: H2's adversarial review assigned two program-level conformance cells here rather than hiding them at the Python adapter: preserve `ForeignBatchOutcome` transfer/copy telemetry through ordinary runtime batches into explain/run evidence, and prove Arrow C release callbacks execute exactly once across producer deletion, cancellation, downstream-thread destruction, and error paths. H2 supplies real >2 MiB PyArrow alias/lifetime/cross-thread evidence but does not claim these remaining shared telemetry/release cells.
- 2026-07-19: H2's closure review assigned the in-process native-memory envelope here. Calibrate PyArrow/CPython native scratch and transient pre-admission C Data retention separately from admitted zero-copy payload, record isolated process RSS for admitted and hostile oversized candidates, populate descriptor/headroom evidence from the measurement, and state plainly that only H3's isolated process boundary can enforce a total memory ceiling over arbitrary producer code before it yields. H2 leases every admitted payload/conversion window and emits no oversized candidate, but does not claim control over pre-yield allocation or retention before imported size is observable.
- 2026-07-18: H3's closure review assigned the remaining control-plane retention law here. Singer/Airbyte schema, catalog, state, trace, and unknown metadata are emitted as small owned control events rather than memory-lease-bearing batch outcomes. The shared matrix must prove consumers process these events immediately without unbounded retention; if that law is falsified, introduce one neutral accounted control envelope rather than a subprocess-specific queue or protocol cap. H3 already proves row payloads, parser scratch, pipe chunks, and diagnostics are independently bounded.
- 2026-07-25: Activated after F3/WS-F closed the aggregate process-tree and exact scale authority.
  H5 will first trace the implemented neutral projection and current reports, then execute only the
  missing shared cells. It will not add a runtime merely to populate prospective WASM numbers or
  infer zero-copy from Arrow compatibility.
- 2026-07-25: Added executor-neutral source-boundary capability and actual-transfer types to
  `cdf-kernel`; `cdf-foreign-stream` now aliases that vocabulary rather than owning a parallel
  runtime model. Successful EOF projects transfer mode, rows, logical bytes, copy classification,
  and control count into `PartitionCompletion`. Generic engine/project/CLI orchestration carries
  the report outside package identity. Registry and window decorators forward the capability;
  the product-spine test caught and prevented decorator erasure.
- 2026-07-25: Rejected unsafe test interposition on Arrow FFI `private_data` after an isolated
  SIGBUS. The final exact-once proof uses Arrow's supported owner-backed payload buffers. Producer
  deletion, downstream cancellation, import rejection, downstream-thread destruction, and early
  stream cancellation each release the owned payload exactly once without mutating opaque FFI
  state.
- 2026-07-25: The Singer control-flood cell emitted 2,049 ordered owned control facts. The
  dedicated `subprocess-protocol-control` consumer peaked at 4,096 bytes and returned to zero;
  independently leased JSON decoder windows were not falsely attributed to control retention.
- 2026-07-25: Published `.10x/evidence/2026-07-25-p3-h5-interop-envelope.md` and
  `docs/interop-boundaries.md`. Python row compatibility reached 2.815M rows/s at the current 8K
  default. The subprocess 524,288-row cells measured 33.599ms Arrow IPC / 20.75MiB managed peak
  and 60.156ms NDJSON / 57.54MiB managed peak. Subprocess and production Python Arrow C remain
  honestly `copy_unknown`; dedicated PyArrow cells alone claim verified alias/lifetime behavior.
  WASM remains prospective and unmeasured.

## References

- `.10x/specs/foreign-stream-interop.md`

## Evidence

- `.10x/evidence/2026-07-25-p3-h5-interop-envelope.md` maps the implementation, exact commands,
  local host labels, raw release observations, control retention, copy claims, and limits.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-python arrow_capsule --locked -j
  12` — passed six ownership tests.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel -p cdf-foreign-stream -p
  cdf-python -p cdf-subprocess -p cdf-engine --lib --locked --no-fail-fast -j 12` — passed 349
  tests with 15 deliberate slow/evidence ignores.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-cli
  python_resource_plan_preview_run_and_replay_use_the_product_spine --locked -j 12` — passed with
  a one-row early preview followed by a complete two-row package/receipt/checkpoint/replay. The
  plan and run JSON assertions exercise planned and actual boundary evidence through the real
  decorators.
- The two release commands and exact results are retained in the evidence record and public docs.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-foreign-stream -p
  cdf-python -p cdf-subprocess -p cdf-runtime -p cdf-engine -p cdf-project -p cdf-cli
  --all-targets --locked -j 12 -- -D warnings` — passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed at closure.

## Review

Fresh-hat adversarial review traced the complete capability and actual-evidence paths from source
descriptor through both generic decorators, engine planning/execution, project reporting, and
human/JSON CLI output. It specifically attempted to find a source-specific runtime branch,
identity-bearing telemetry, native hot-loop overhead, unearned zero-copy language, control-event
retention, FFI double release, early-preview failure, and missing decorator forwarding.

The review found and repaired two significant issues before closure: generic decorators initially
erased boundary capabilities, and test-only callback interposition touched opaque FFI state and
crashed. The final code forwards capability generically, records actual evidence only once per
foreign outcome and partition EOF, leaves native per-batch paths untouched, and uses supported
allocation ownership for release proof. Verdict: **pass**. Residual risk is explicit: arbitrary
in-process producer allocation before yield cannot be governed, so total hostile-producer memory
requires the isolated subprocess boundary.

## Retrospective

Interop measurement belongs in two planes: immutable plan capabilities and invocation-local
observations. Putting both in one artifact would either lie before execution or contaminate
deterministic identity. EOF-bound `PartitionCompletion` was the existing seam that made the split
small and composable.

The failed FFI instrumentation was useful: opaque `private_data` is not a test hook. Proving release
through the payload owner's supported deallocation path exercises the same lifetime while removing
an unnecessary unsafe surface. The product test also reinforced that trait decorators must forward
new capability methods explicitly; a default method can otherwise fail open by silently erasing
facts.
