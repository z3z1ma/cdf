Status: recorded
Created: 2026-07-27
Updated: 2026-07-27

# Product error ownership audit

## Observation

The frozen pre-repair D1c inventory contains 203 Rust files across 16 product/governance crate
roots. It contains 344 `CdfError::internal` construction sites across 64 files and 59 direct
`ErrorKind::Internal`/`Internal` matches. Every direct-kind match was inspected as a mapping,
branch, or assertion; none was an untracked constructor.

The review-driven wrapper repair also changed `crates/cdf-kernel/src/error.rs`, outside the frozen
16-root inventory. That supporting change does not add or reclassify an Internal construction
site; it makes the shared `embedded_cdf_error` helper traverse nested `std::io::Error` payloads.

## Procedure

The durable newline-delimited file manifest and per-site ledger are:

- `.10x/evidence/.storage/2026-07-27-d1c-rust-files.txt`
- `.10x/evidence/.storage/2026-07-27-d1c-internal-site-ledger.tsv`

The original null-delimited working manifest was retained during execution at
`/tmp/cdf-d1c-files.pIj5WZ`. The pre-repair inventory commands were:

```sh
tr '\0' '\n' </tmp/cdf-d1c-files.pIj5WZ | sed '/^$/d' | wc -l
xargs -0 rg -n 'CdfError::internal' </tmp/cdf-d1c-files.pIj5WZ | wc -l
xargs -0 rg -l 'CdfError::internal' </tmp/cdf-d1c-files.pIj5WZ | wc -l
xargs -0 rg -n 'ErrorKind::Internal|\bInternal\b' </tmp/cdf-d1c-files.pIj5WZ | wc -l
```

Final per-root Internal site/file counts:

| Crate root | Sites | Files |
| --- | ---: | ---: |
| `cdf-aws` | 0 | 0 |
| `cdf-bench-core` | 0 | 0 |
| `cdf-bench-measure` | 0 | 0 |
| `cdf-benchmarks` | 6 | 2 |
| `cdf-builtin-drivers` | 3 | 1 |
| `cdf-cli` | 49 | 16 |
| `cdf-cli-benchmarks` | 0 | 0 |
| `cdf-cli-core` | 3 | 2 |
| `cdf-conformance` | 17 | 12 |
| `cdf-contract` | 26 | 6 |
| `cdf-declarative` | 14 | 2 |
| `cdf-expression` | 0 | 0 |
| `cdf-postgres` | 0 | 0 |
| `cdf-project` | 158 | 18 |
| `cdf-state-sqlite` | 68 | 5 |
| `cdf-wasm` | 0 | 0 |

Verification observations:

- `cargo test -p cdf-state-sqlite --lib -- --test-threads=1`: 66 passed before the final
  ownership-provenance repair. The post-repair all-target gate reached 66 passes and one
  test-assertion wording failure; that assertion was corrected, but the requested immediate
  checkpoint interrupted its rerun. This is not claimed as final passing evidence.
- `cargo test -p cdf-cli --lib -- --test-threads=1`: 296 passed.
- `cargo test -p cdf-benchmarks --lib -- --test-threads=1`: 27 passed.
- `cargo test -p cdf-benchmarks --test fixtures -- --test-threads=1`: 7 passed.
- The strict all-target/all-feature Clippy gate for the 16 roots plus the supporting `cdf-kernel`
  change passed with `-D warnings`.
- The generated command/error reference freshness check passed.
- `cargo fmt --all -- --check` and `git diff --check` passed.
- The broad `cdf-project` gate passed 255 tests before reporting five failures. Four stale
  assertions exposed by the no-clobber/read-shape hardening were repaired and their focused tests
  pass. The remaining `recorded_http_multifile_packages_are_jobs_invariant` timing assertion is
  the known pre-D1c intermittent integration baseline and still reproduced in isolation.
- The broad `cdf-conformance` gate advanced through its product, golden, live-run, replay,
  property, resource, and run-matrix cells but did not complete: the
  `cross_destination_generic_runtime_stage_chaos_persists_output` cell made no progress for more
  than two minutes and the command was terminated. The spawned Postgres service was stopped
  explicitly. Focused D1c conformance regressions, including the MVP fixture, REST example,
  registry references, and deeply nested destination ownership test, passed.
- `graphify update .` could not run because the `graphify` executable is unavailable.

## What it supports or challenges

This supports complete ownership of the D1c Internal-site inventory and the repaired product
boundaries: configured path versus private `.cdf` state, host versus artifact failures, private
SQLite corruption, raw diagnostic readers, immutable artifact publication, and typed wrapper
provenance.

Review challenged full-history private-state scans on ordinary store open. The implementation now
keeps ordinary opens schema-only, validates rows when typed APIs consume them, and reserves
whole-store `validate_integrity` scans for raw diagnostic/recovery paths.

## Limits

The final site counts, durable-ledger reproduction command, and the post-repair verification batch
remain to be refreshed after this checkpoint. The audit is semantic and repository-local; it does
not inject every platform-specific filesystem
or device failure. Explicit whole-store integrity diagnostics remain proportional to retained
history, and content root-member validation can be superlinear for a single large inline root.
Z1 owns representative diagnostic-path measurement. The two broad-suite limits above are not
claimed as passing evidence.
