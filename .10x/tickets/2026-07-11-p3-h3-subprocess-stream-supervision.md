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

## Evidence

- 2026-07-18 schema-pinned stdout streaming slice:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-subprocess ndjson_stdout_adapter_streams_with_compiled_schema_without_reserving_stdout_ceiling --lib -j 12` — passed before lockfile refresh. Proves a compiled-schema NDJSON subprocess stream succeeds with a 256 MiB stdout allowance under a 96 MiB memory budget; the previous whole-stdout reservation model would reject that shape before reading.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-subprocess --lib --locked -j 12` — passed, 14 passed. Covers the new streaming stdout guard plus existing stderr capture, Arrow IPC bounded compatibility, nonzero exit, timeout, cancellation, protocol parsing, and package write/replay tests.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime bounded_format --lib --locked -j 12` — passed, 3 passed. Proves the existing bounded memory source behavior remains intact after factoring out the stream helper.
  - `CARGO_BUILD_JOBS=12 cargo check -p cdf-python -p cdf-conformance --locked -j 12` — passed. Proves Python interpreter probing and subprocess conformance callers still compile against the compatibility APIs.
  - `cargo fmt --all` — passed.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-subprocess -p cdf-python -p cdf-conformance --all-targets --locked -j 12 -- -D warnings` — passed.
- Limit: no throughput envelope, process-tree descendant reaping, Arrow IPC unbounded stream length, Singer/Airbyte streaming state/control, nonzero-after-emitted-batch checkpoint proof, or copy/memory-cost lab report is claimed by this slice.

## Review

Pass for the schema-pinned stdout streaming milestone; concerns remain for H3 closure. The new path is expressed through the runtime `ByteSource`/format-driver boundary, not a subprocess-specific decoder or destination/source branch. It does not change performance defaults for existing file/HTTP paths. The significant remaining risk is that public compatibility helpers still expose bounded `Vec<Batch>`/whole protocol readers for unpinned and Singer/Airbyte modes; those must either become explicit capped compatibility collectors or move to streaming protocol readers before H3 can close.

## Retrospective

The right seam was not “teach subprocess how to decode NDJSON,” it was “make the runtime bounded-format helper streamable when schema is already compiled.” Cold discovery still needs a finite schema barrier; pretending otherwise would recreate the P2 double-read confusion in process form. The streaming path therefore starts with compiled-schema execution, while the remaining H3 work should migrate protocol control/state framing onto the same stream rather than adding another collector.
