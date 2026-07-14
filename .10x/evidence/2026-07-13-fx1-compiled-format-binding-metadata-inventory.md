Status: recorded
Created: 2026-07-13
Updated: 2026-07-13
Relates-To: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md

# Compiled format binding and metadata-only remote inventory

## Observation

Declarative compilation now emits a source-neutral `SourceCompileRequest`; product composition resolves that request through the injected `SourceRegistry`. The file driver compiles a neutral `CompiledFormatBinding` containing the complete registered descriptor and canonical options, serializes it in the source physical plan, and verifies the exact binding against the execution registry before opening a file.

File partition inventory no longer performs format or compression payload probes. Registry extensions provide metadata-time evidence and content detection is explicitly deferred to the admitted stream. The `remote_inventory_never_reads_payload_for_format_or_compression_detection` transport counter observes zero range reads for an object-store `.ndjson.gz` candidate while still resolving the registered format, transform, generation, and partition metadata.

Project discovery now has one registered-format adapter for local and remote candidates. Snapshot metadata records the dynamic format id and canonical option hash rather than switching on Parquet/Arrow identities. Explicit file sampling is accepted for row formats and remains governed by the format driver's bounded discovery implementation.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo check -p cdf-runtime -p cdf-source-files -p cdf-declarative -p cdf-project -p cdf-cli -p cdf-conformance -j12` — passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime -p cdf-source-files -p cdf-declarative -p cdf-project -j12` — passed: runtime 36/36 non-ignored, runtime build graph 1/1, file source 30/30, declarative 81/81, project 177/177, doc tests 0 failures.
- `cargo fmt --all` and `git diff --check` — passed.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-source-files -p cdf-declarative -p cdf-project -p cdf-cli -p cdf-conformance --all-targets -j12 -- -D warnings` — passed. Two pre-existing current-toolchain lints in REST/Postgres were corrected mechanically; conformance's large matrix enum now boxes concrete test fixtures instead of carrying nearly 2 KiB inline.

## What this supports or challenges

This supports FX1's registry-driven plan/execution boundary and directly answers two aggregate-review findings: executable plans now pin codec version/options/detection/access/unit/memory semantics, and project/file inventory no longer carries first-party format metadata branches or hidden remote magic ranges.

It also establishes the first executable part of the single-crossing program: remote ordinary inventory is payload-free. Malformed or contradictory content now fails on the admitted codec stream before a partial batch, rather than by downloading probe bytes during inventory.

## Limits

FX1 is not closed. Declarative extension inference and the monolithic `cdf-formats` dependency remain to be removed, and project-level remote external-codec conformance remains outstanding. SA2 still owns payload-free local identity (local planning currently hashes file contents), the observation cache, and exact cache invalidation. SA3 owns eliminating the remaining discover-then-decode second opening when no compiler physical observation exists.
