Status: recorded
Created: 2026-07-27
Updated: 2026-07-27

# Product error ownership audit

## Observation

The final frozen D1c inventory contains 203 Rust files across 16 product/governance crate roots.
It contains 345 `CdfError::internal` construction sites across 64 files and 60 direct
`ErrorKind::Internal`/`Internal` matches. Every direct-kind match was inspected as a mapping,
branch, or assertion; none was an untracked constructor.

The review-driven wrapper repair also changed `crates/cdf-kernel/src/error.rs`, outside the frozen
16-root inventory. That supporting change does not add or reclassify an Internal construction
site; it makes the shared `embedded_cdf_error` helper traverse nested `std::io::Error` payloads.

## Procedure

The durable newline-delimited file manifest and per-site ledger are:

- `.10x/evidence/.storage/2026-07-27-d1c-rust-files.txt`
  (`sha256:344dfb005e2db9cae3f464f027d947841c33daeb515eba8c14c17e98053d6e45`)
- `.10x/evidence/.storage/2026-07-27-d1c-internal-site-ledger.tsv`
  (`sha256:743d4751cefb058efe737f77ca6b4e6989848c7464b27363b7d74c52ceb0554c`)

From the repository root, the exact construction procedure is:

```sh
audit_roots=(
  crates/cdf-aws crates/cdf-bench-core crates/cdf-bench-measure crates/cdf-benchmarks
  crates/cdf-builtin-drivers crates/cdf-cli crates/cdf-cli-benchmarks crates/cdf-cli-core
  crates/cdf-conformance crates/cdf-contract crates/cdf-declarative crates/cdf-expression
  crates/cdf-postgres crates/cdf-project crates/cdf-state-sqlite crates/cdf-wasm
)
LC_ALL=C rg --files -0 -g '*.rs' "${audit_roots[@]}" |
  LC_ALL=C sort -z |
  tr '\0' '\n' > .10x/evidence/.storage/2026-07-27-d1c-rust-files.txt
{
  while IFS= read -r audit_file; do
    rg -n -H -- 'CdfError::internal' "$audit_file"
  done < .10x/evidence/.storage/2026-07-27-d1c-rust-files.txt |
    awk '{print "constructor\tretained_internal\t" $0}'
  while IFS= read -r audit_file; do
    rg -n -H -- 'ErrorKind::Internal|\bInternal\b' "$audit_file"
  done < .10x/evidence/.storage/2026-07-27-d1c-rust-files.txt |
    awk '{print "direct_kind_supplement\tmapping_branch_or_assertion\t" $0}'
} | LC_ALL=C sort > .10x/evidence/.storage/2026-07-27-d1c-internal-site-ledger.tsv
```

The procedure reproduced both checked-in files byte-for-byte. Final per-root Internal site/file
counts:

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
| `cdf-project` | 159 | 18 |
| `cdf-state-sqlite` | 68 | 5 |
| `cdf-wasm` | 0 | 0 |

Verification observations:

- `cargo test -p cdf-state-sqlite --all-targets`: 67 passed after the final
  ownership-provenance and bounded-row-authority repairs.
- `cargo test -p cdf-cli --lib --quiet -- --test-threads=1`: 297 passed after the final
  configured-path fixture correction.
- `cargo test -p cdf-cli resume -- --test-threads=1`: 17 passed after removing the redundant
  ownership argument from the context-bearing resume helper.
- `cargo test -p cdf-benchmarks --lib -- --test-threads=1`: 27 passed.
- `cargo test -p cdf-benchmarks --test fixtures -- --test-threads=1`: 7 passed.
- The strict all-target/all-feature Clippy gate for the 16 roots plus the supporting `cdf-kernel`
  change passed with `-D warnings`.
- The final affected-root strict Clippy rerun for `cdf-kernel`, `cdf-state-sqlite`, `cdf-project`,
  and `cdf-cli` passed with all targets, all features, and `-D warnings`.
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

The audit is semantic and repository-local; it does not inject every platform-specific filesystem
or device failure. Explicit whole-store integrity diagnostics remain proportional to retained
history, and content root-member validation can be superlinear for a single large inline root.
Z1 owns representative diagnostic-path measurement. The two broad-suite limits above are not
claimed as passing evidence.
