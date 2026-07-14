Status: recorded
Created: 2026-07-13
Updated: 2026-07-13

# FX1 registry-owned format inference and deep-validation parity

## Observation

Undeclared file formats now remain unresolved in the declarative grammar and are selected by installed `FormatRegistry` descriptor extensions during source-plan compilation. The compiled source artifact pins the canonical driver descriptor and options, while inferred-versus-explicit provenance survives repeated compilation. Generic deep validation resolves the same source runtime used by plan/run and reads record-isolation behavior from the selected descriptor rather than recognizing JSON-family ids.

Binary discovery performs bounded registry-derived strong-magic confirmation after payload-free inventory. A contradictory extension/magic observation names the resource, file, declared/inferred format, extension signal, magic signal, and fixes. Probe-byte evidence now counts the bounded ranges actually read, or one full source transfer when discovery required a spool; it no longer reports every untransformed remote object as fully transferred.

## Procedure

From the repository root:

```text
CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-source-files -p cdf-declarative -p cdf-project -p cdf-cli --all-targets -- -D warnings
```

Result: passed.

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files --lib
```

Result: 32 passed, 0 failed. This includes registry inference, repeated-compilation provenance, metadata-only remote inventory, external codec/transform composition, direct remote Arrow IPC, bounded local/remote Parquet, streamed JSON/CSV/NDJSON, transformed-spool discovery, generation checks, and zero-residual-memory provider laws.

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-project --lib http_parquet_schema_discovery_uses_bounded_ranges_without_artifacts
```

Result: 1 passed, 0 failed. The HTTP Parquet probe remained bounded and wrote no artifacts.

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib validate_deep_
```

Result: 5 passed, 0 failed. Deep validation preserved no-write discovery, malformed-input failure, governed row-local mismatch classification, physical/declared type diagnostics, destination compatibility, and full contradictory binary-format signals.

Static searches found no first-party format inference function or first-party format-id branch in the declarative compiler, project discovery adapter, or deep-validation implementation. `git diff --check` passed.

## What this supports or challenges

This supports FX1 acceptance that format selection and format-specific capabilities are registry-owned, executable source plans pin codec semantics, and generic orchestration does not branch on first-party format identity. It also restores preview/deep-validation use of the real resolved source boundary after dependency-free file shims were deleted.

It does not close FX1: the monolithic `cdf-formats` parser/dispatch surface and project-level external codec over a remote provider remain. It also does not solve cold-run/pinned-run schema lifecycle reuse. The bounded strong-magic confirmation and the driver discovery observation are still separate reads; `.10x/tickets/2026-07-13-p0-single-crossing-schema-admission.md` owns observation reuse and removal of pre-extraction schema passes.

## Limits

The focused tests do not claim every workspace test or live cloud transport passed. Strong-magic confirmation currently applies when the selected driver declares magic; ambiguous text detection remains driver-owned. Full payload-spool reuse across discovery and extraction is not implemented by this slice.
