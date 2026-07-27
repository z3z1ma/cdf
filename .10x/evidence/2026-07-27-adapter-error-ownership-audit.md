Status: recorded
Created: 2026-07-27
Updated: 2026-07-27

# Adapter error ownership audit

## Observation

The source, format, transform, transport, destination, Python, subprocess, and foreign-stream
adapter families contain 135 Rust source files. The complete audit began with 359 textual Internal
construction lines: 358 `CdfError::internal`/`.internal` lines plus one direct
`CdfError::new(ErrorKind::Internal, ...)` line. After semantic reclassification, 333 construction
lines remain and all belong to CDF-owned invariant families; no retained production site
constructs Internal directly from a host filesystem, file-descriptor, process, executable,
HTTP-client-runtime, local destination I/O, or external HTTP status failure.

The retained count is an inventory, not a target metric. The change also adds an Internal
round-trip fixture and an explicit asynchronous object-store task-failure classification, so the
before/after difference is not the number of reclassified failures.

## Procedure

Run from the repository root:

```sh
rg --files crates |
  awk -F/ '/^crates\/cdf-(source|format|transform|transport|dest|python|subprocess|foreign-stream)/ && /\.rs$/ {print}' \
  > /tmp/d1b-adapter-rust-files.txt
xargs rg -n 'CdfError::internal|\.internal\(' \
  < /tmp/d1b-adapter-rust-files.txt \
  > /tmp/d1b-internal-sites.txt
xargs rg -n 'ErrorKind::Internal' \
  < /tmp/d1b-adapter-rust-files.txt \
  > /tmp/d1b-internal-kind-sites.txt
wc -l /tmp/d1b-adapter-rust-files.txt /tmp/d1b-internal-sites.txt
cut -d: -f1 /tmp/d1b-internal-sites.txt | sort | uniq -c | sort -k2
```

The second scan is required because direct `CdfError::new(ErrorKind::Internal, ...)` construction
does not use the convenience method. At the final state its three matches are a kind conversion and
two test assertions, not constructors. The final construction inventory groups as follows:

| Family | Files | Retained sites | Classification |
| --- | ---: | ---: | --- |
| Destination | 20 | 104 | Receipt/package/mirror/commit invariants, Arrow downcasts, counter/identity bounds, deterministic serialization, private-scratch disappearance/corruption, and one asynchronous object-store task failure |
| Foreign stream | 1 | 3 | Poisoned ownership and completion-before-report invariants |
| Format | 8 | 54 | Decoder/writer state, Arrow builder/downcast, bounded-window, and deterministic serialization invariants |
| Python | 4 | 16 | Plan serialization, conversion windows, invocation ownership, poisoned state, and test sentinels |
| Source | 25 | 118 | Canonical plan/task/identity invariants, decoder/reader lifecycle, accounting, deterministic serialization, poisoned work queues, and test sentinels |
| Subprocess | 2 | 19 | Missing configured pipes/PID, producer task panic/join, lifecycle ownership, and protocol decoder invariants |
| Transform | 7 | 16 | Decoder initialization, impossible codec statuses, frame/accounting, and output-bound invariants |
| HTTP transport | 1 | 3 | Missing memory/hash authority and impossible nonempty-frame state |
| **Total** | **68 files with sites** | **333** | **CDF-owned invariant or test-only sentinel** |

The other 67 adapter Rust files contain no textual Internal construction site. Each retained site is
reviewable at its exact path and line through the generated inventory. Manual inspection included
the construction statement and its surrounding control flow; keyword search was used only to find
candidates, never to decide ownership.

The semantic changes were:

- `cdf-subprocess`: OS spawn, pipe read, wait, kill, process-group inspection/signaling, and
  unquiesced-process failures are Environment with executable/permissions/process-limit
  remediation. Absent pipes/PIDs, lifecycle duplication, task joins, and producer panics remain
  Internal.
- `cdf-source-files`: failure to obtain the current working directory for a relative root is
  Environment with an absolute-root recovery path.
- `cdf-transport-http`: failure to construct the host HTTP clients is Environment with
  TLS/resolver/runtime remediation.
- `cdf-dest-duckdb`: local sidecar `std::io::Error` is Environment; native database and destination
  failures keep their existing typed ownership.
- `cdf-dest-parquet`: local create/open/read/write/sync/lock/atomic-install/delete failures are
  Environment. Missing/truncated/invalid/wrong-shape destination artifacts remain Destination,
  remote provider semantics remain Destination, missing/corrupt CDF-private encoded scratch and
  asynchronous task failure remain Internal, and embedded typed CDF errors win before I/O/provider
  fallback.
- `cdf-source-iceberg`: the Iceberg wrapper now walks its source chain and preserves an embedded
  typed `CdfError`, then classifies raw external-source I/O as Data or Environment, before applying
  coarse upstream retry/kind mapping.
- `cdf-transport-http`: unexpected final response statuses are external protocol Data, including
  unusual success statuses; only CDF-owned response streaming/accounting invariants remain
  Internal.

## What it supports or challenges

This supports every acceptance criterion in
`.10x/tickets/done/2026-07-26-prewave-d1b-adapter-error-audit.md` concerning site coverage and ownership
classification. It also challenges any future audit that treats Internal-count reduction as
success: the correct invariant is ownership, not a smaller number.

## Limits

The textual inventory covers both convenience construction idioms and separately scans direct
`ErrorKind::Internal` use. It does not prove that a helper named differently cannot manufacture an
Internal error; strict review therefore also inspected changed helper boundaries and adapter error
conversion paths. Runtime behavior is supported separately by the owning ticket's focused suites
and CLI rendering evidence.
