Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-08-p2-ws-d1-file-glob-partition-planning.md
Verdict: pass

# P2 WS-D1 file glob partition planning review

## Target

Review of the D1 implementation in:

- `crates/cdf-declarative/src/file_runtime.rs`
- `crates/cdf-declarative/src/compiled.rs`
- `crates/cdf-declarative/src/tests.rs`

## Assumptions tested

- File planning must resolve actual local matches for D1, not retain a synthetic `files` partition for multi-match globs.
- Runtime and preview must not independently choose different files from the same glob.
- Partition validation must fail closed before reads when a caller mutates glob, resource, scope, path, id, or size evidence.
- Single-file callers should remain stable enough for existing tests and product surfaces.
- Manifest incrementality, compression, remotes, schema variance, and coalescing must not be smuggled into D1.

## Findings

No blocking findings.

The implementation centralizes local glob resolution in `file_runtime`, reuses it from planning and open/preview validation, and deletes the old single-file-only runtime gate. It preserves the single-file partition id `files`, but multi-file partitions use deterministic path-derived ids and reject the legacy id during open. This is a conservative compatibility choice and does not weaken D1 because multi-file partitions are still independent and file-scoped.

The validation path re-resolves the current local glob and requires the planned root-relative `path` to still be produced by that glob. It also checks resource id, glob, scope, partition id shape, and byte size before reading. It intentionally does not serialize the absolute source root into partition metadata, which keeps plan/package identity independent of temporary checkout or fixture roots.

Parent review found two implementation issues after the worker pass and both are resolved: multi-file opens no longer accept the compatibility `files` id, and conformance golden packages no longer bake dynamic absolute roots into partition identity. The live local-file DuckDB, Parquet, and Postgres goldens were regenerated and rerun after adding the new `path`/`bytes` metadata to their fixture plans.

Parent quality review found one static-analysis issue in D1 test code: Semgrep rejected the original `std::env::temp_dir()` helper as an unsafe shared temporary-directory pattern. The tests now use `tempfile::TempDir`, and the Semgrep rerun is clean.

## Residual risk

Planning now stats local files and reads directory contents during `plan_partitions`/`negotiate` for file resources. That is intended by D1 and the file-source decision, but broader callers with fake file resources will need real fixtures when they start exercising file planning. The `cdf-declarative` crate tests were updated where this surfaced.

`modified_ms` is metadata only in D1; it is not yet a manifest identity policy. Later FileManifest work must decide exact identity precedence and persisted state behavior.

CodeQL's Rust extractor still reports large diagnostic macro-resolution noise for this repository, but the mandated reusable CodeQL script exited `0` and produced no closure-blocking security result for D1.

## Verdict

Pass. The D1 scope is implemented and evidenced without implementing the explicitly deferred manifest, compression, remote transport, schema variance, no-op rerun, or large-N coalescing work.
