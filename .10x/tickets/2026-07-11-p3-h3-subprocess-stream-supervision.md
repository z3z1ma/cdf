Status: active
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md
Depends-On: .10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-b3-arrow-ipc-codecs.md, .10x/tickets/done/2026-07-11-p3-b5-json-codecs.md

# P3 H3: streaming supervised subprocess/protocol boundary

## Scope

Replace `wait_with_output` and materialized decoding with concurrent incremental stdout/stderr/control supervision, bounded IPC/row framing, process-tree budgets/groups, structured cancellation/reaping, typed state, shared reconciliation, and Singer/Airbyte compatibility streaming.

## Acceptance criteria

- Arbitrarily long stdout/stderr runs stay bounded and stdout backpressure reaches the child.
- Arrow IPC batches decode/publish incrementally; NDJSON/Singer/Airbyte use bounded row windows.
- Nonzero exit/protocol failure after data cannot gate the epoch; retry/recovery is deterministic.
- Timeout/cancel kills/reaps descendants, releases leases, preserves bounded redacted diagnostics, and leaves no checkpoint ahead of receipts.
- IPC/row throughput and copy/memory costs are reported separately.

## Evidence expectations

Adversarial child fixtures (stderr flood, stalls, truncation, signals, descendants, malformed/state ordering), process-tree/RSS traces, package/checkpoint inspection, before/after benchmarks, and supervision/security review.

## Explicit exclusions

No arbitrary shell parsing, ambient secret injection, or Wasmtime.

## Blockers

None. H1, runtime ledger, injected execution host, and streaming codecs are done; this ticket is executable.

## References

- `.10x/specs/foreign-stream-interop.md`

## Journal

- 2026-07-18: Activated after H1/H2 made the neutral foreign stream vocabulary available. First implementation slice stayed intentionally narrow and architectural: added a reusable `cdf-runtime::decode_format_stream` helper that decodes any `ByteSource` into a `FormatBatchStream` against an already-compiled schema without performing discovery/current-schema pre-scan, and added a one-shot subprocess stdout `ByteSource` behind `run_stdout_adapter_streaming`. The subprocess source starts the child lazily when the batch stream is polled, accounts stdout chunks as they are read, supervises bounded stderr concurrently, records exit/stderr through a completion handle after terminal drain, and fails nonzero exit with redacted stderr context. The legacy `run_bounded_command`/unpinned `run_stdout_adapter` path remains for tiny probes and compatibility collectors; it is not claimed as H3 closure.
- 2026-07-18: Reworked the subprocess stdout source to read the OS pipe on a Tokio task into a one-item accounted channel. This preserves pipe backpressure for slow codec consumers and avoids requiring codec-side blocking helper threads to poll Tokio I/O directly. An Arrow IPC unknown-length subprocess-stream experiment hung under test and was not retained; Arrow IPC subprocess streaming remains a named residual until its codec termination model can prove bounded finite-stream behavior without lying about exact content length or risking a hang.
- 2026-07-18: Removed the ambiguous public `run_stdout_adapter` name. The whole-stdout compatibility path is now explicitly `run_bounded_stdout_adapter`; the schema-pinned incremental path remains `run_stdout_adapter_streaming`. No compatibility alias was retained.
- 2026-07-19: H2 removed its inert in-process Python watchdog after adversarial review proved it could not interrupt a callable, iterator `next`, or Arrow stream callback blocked before yielding. This ticket's isolated process group is the authority for foreign code requiring bounded force-authorized termination. Acceptance must include a Python child stalled before its first frame, not merely a child stalled between already-emitted frames.
- 2026-07-20: Removed the Arrow IPC stream codec's synchronous `Read`/thread/channel bridge. The old bridge could deadlock a current-thread executor while `StreamReader` blocked waiting for a Tokio-owned subprocess pipe. The codec now drives Arrow's push-based `StreamDecoder` directly from accounted async byte chunks, retains transport leases through zero-copy buffer ownership, validates the physical schema before every emitted batch, and rejects truncated or over-bound streams before terminal success. A generic `ByteSource::maximum_sequential_bytes` authority distinguishes exact object length, finite unknown-length producers, and truly unbounded streams without fabricating content size. The subprocess source binds that authority to its compiled stdout boundary. Current-thread conformance now streams an unknown-length Arrow IPC child to completion; the existing 64-batch constant-memory codec law remains green. Process-tree termination and neutral foreign-event publication remain open.
- 2026-07-20: Replaced the subprocess-only completion handle with `cdf-foreign-stream`'s neutral producer/open/events/termination contract. `SubprocessProducer` now declares isolated-process, pipe-backpressured Arrow IPC or row-compatible transfer; emits accounted outcomes and exactly one terminal; and retains independent cancel-and-join authority. A nonzero exit after an already-emitted Arrow batch becomes a failed terminal, so the shared foreign-event adapter releases the batch but fails the stream before gating. Dropping or cancelling a stream before its first frame terminates and joins the invocation rather than detaching it. Copy telemetry remains deliberately `copy_unknown` until H1/H5 instrumentation can prove byte counts; Arrow output size is not mislabeled as bytes copied.
- 2026-07-20: Centralized Unix subprocess process-group ownership for both the tiny bounded probe and streaming producer paths. Timeout, cancellation, consumer drop, decoder failure, and successful-parent residual descendants all receive cooperative group termination followed by the configured `termination_grace` and forced termination when necessary. Stderr now drains to EOF even during floods while retaining only the configured diagnostic ring, records discarded bytes, and redacts injected environment values before diagnostics/errors. Conformance kills a forked descendant on both timeout and pre-first-frame cancellation. Singer/Airbyte incremental typed control/state framing and the H3 throughput/copy report remain the closure residuals.
- 2026-07-20: Adversarial lifecycle review falsified the first process-group implementation: a successful direct parent could exit while a descendant retained inherited stdout/stderr, so cleanup after pipe EOF could never run. Reworked both collector and streaming paths so direct-child exit triggers residual-group termination before awaiting pipe EOF; cleanup signal/wait/still-alive failures are now propagated rather than discarded. `InvocationTermination::join` now awaits the actual producer task through explicit join evidence after leases/task state drop, including panic detection. Streaming stdout no longer inherits the bounded collector's 64 MiB total cap: in-flight chunk size and optional total-transfer policy are independent knobs, and the default total-transfer policy is unlimited. Truncated diagnostic boundaries redact retained secret prefixes. Unsupported non-Unix process-tree authority now fails explicitly instead of claiming force termination. Incremental Singer/Airbyte control/state framing, a configured inherited child-memory limit, and the H3 throughput/copy report remain closure residuals.

## Evidence

- 2026-07-18 schema-pinned stdout streaming slice:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-subprocess ndjson_stdout_adapter_streams_with_compiled_schema_without_reserving_stdout_ceiling --lib -j 12` — passed before lockfile refresh. Proves a compiled-schema NDJSON subprocess stream succeeds with a 256 MiB stdout allowance under a 96 MiB memory budget; the previous whole-stdout reservation model would reject that shape before reading.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-subprocess --lib --locked -j 12` — passed, 14 passed. Covers the new streaming stdout guard plus existing stderr capture, Arrow IPC bounded compatibility, nonzero exit, timeout, cancellation, protocol parsing, and package write/replay tests.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime bounded_format --lib --locked -j 12` — passed, 3 passed. Proves the existing bounded memory source behavior remains intact after factoring out the stream helper.
  - `CARGO_BUILD_JOBS=12 cargo check -p cdf-python -p cdf-conformance --locked -j 12` — passed. Proves Python interpreter probing and subprocess conformance callers still compile against the compatibility APIs.
  - `cargo fmt --all` — passed.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-subprocess -p cdf-python -p cdf-conformance --all-targets --locked -j 12 -- -D warnings` — passed.
  - Rejected experiment: `CARGO_BUILD_JOBS=12 cargo test -p cdf-subprocess stdout_adapter_streams --lib --locked -j 12` hung in the Arrow IPC unknown-length streaming case even after decoupling subprocess pipe reads behind a bounded channel. The Arrow IPC widening patch and test were removed rather than weakening the timeout, lying about exact length, or shipping a possible hang.
  - `run_stdout_adapter` rename evidence is included in the focused `cdf-subprocess` test/clippy runs after the rename; no downstream production crate references the removed name.
- Limit: no throughput envelope, process-tree descendant reaping, Arrow IPC unbounded stream length, Singer/Airbyte streaming state/control, nonzero-after-emitted-batch checkpoint proof, or copy/memory-cost lab report is claimed by this slice.
- 2026-07-20 finite unknown-length Arrow IPC slice:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-arrow-ipc --lib --locked -j 12` — passed, 3 passed and 1 deliberate release benchmark ignored. The 64-batch stream law retains less than the full payload and returns the memory ledger to zero.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-subprocess arrow_ipc_stdout_adapter_streams_unknown_length_without_executor_deadlock --lib --locked -j 12` — passed on Tokio's current-thread runtime. This directly falsifies the prior deadlock and proves the finite stdout boundary is distinct from exact content length.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-format-arrow-ipc --all-targets --all-features --locked -j 12 -- -D warnings` — passed before the subprocess conformance addition; closure CI will cover the complete batch.
- 2026-07-20 neutral producer and process-tree slice:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-subprocess --lib --locked -j 12` — passed, 20 tests. Covers incremental Arrow IPC/NDJSON outcomes, current-thread execution, nonzero exit after data, stderr flood drainage/truncation, exact environment-value redaction, timeout/cancellation, and forked descendant termination before the first frame.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-subprocess -p cdf-python -p cdf-conformance --all-targets --all-features --locked -j 12 -- -D warnings` — passed. Proves the new public neutral producer, the retained bounded interpreter probe, and downstream protocol conformance compile warning-free together.
  - Limit: process groups contain ordinary descendants but cannot prevent an actively hostile child from creating a new OS session; stronger sandbox/cgroup containment belongs to the explicit security capability boundary, not an undocumented promise here. Singer/Airbyte still use bounded whole-message compatibility collectors and no H3 throughput/copy envelope is yet claimed.
- 2026-07-20 lifecycle falsification repairs:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-subprocess -p cdf-format-arrow-ipc --lib --locked -j 12` — passed, 24 subprocess tests and three Arrow IPC tests; one release benchmark remains deliberately ignored. New cases prove a successful parent cannot hang behind inherited descendant pipes, TERM-resistant groups reach forced termination, truncated secret prefixes do not survive diagnostics, a dropped stream joins and releases every ledger lease, and a multi-chunk NDJSON stream exceeds the collector cap while retaining bounded in-flight memory.
  - The Arrow IPC stream codec now treats the total byte limit as optional and still rejects an explicitly configured overrun; framing state remains bounded by the ordinary decoder/input leases.

## Review

Pass for the schema-pinned NDJSON stdout streaming milestone; concerns remain for H3 closure. The new path is expressed through the runtime `ByteSource`/format-driver boundary, not a subprocess-specific decoder or destination/source branch. It does not change performance defaults for existing file/HTTP paths. The Arrow IPC hang was correctly rejected. The significant remaining risk is that public compatibility helpers still expose bounded `Vec<Batch>`/whole protocol readers for unpinned and Singer/Airbyte modes; those must either become explicit capped compatibility collectors or move to streaming protocol readers before H3 can close.

The first lifecycle/process-group slice failed adversarial review because cleanup followed pipe EOF rather than direct-parent exit, exposed join authority did not join the spawned task, cleanup errors were discarded, the default collector cap leaked into streaming, truncated secrets could expose a prefix, and non-Unix behavior overclaimed force termination. The repair addresses those findings and adds direct falsification cases. Child-process memory enforcement remains a declared closure gap rather than being silently represented as `Some` in the neutral descriptor.

## Retrospective

The right seam was not “teach subprocess how to decode NDJSON,” it was “make the runtime bounded-format helper streamable when schema is already compiled.” Cold discovery still needs a finite schema barrier; pretending otherwise would recreate the P2 double-read confusion in process form. The streaming path therefore starts with compiled-schema execution, while the remaining H3 work should migrate protocol control/state framing onto the same stream rather than adding another collector.
