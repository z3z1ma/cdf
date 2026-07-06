Status: open
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-05-package-builder-reader.md, .10x/tickets/done/2026-07-06-prepared-package-chaos-conformance.md

# Implement golden-package conformance foundation

## Scope

Implement the first reusable golden-package determinism conformance slice under `firn-conformance`: a harness that builds fixed package fixtures through public `firn-package` APIs and compares the resulting package evidence hash-by-hash against committed expectations.

Own `crates/firn-conformance/src/golden_package/` and the thin export from `crates/firn-conformance/src/lib.rs`. Add one committed prepared-orders expectation fixture, preferably `crates/firn-conformance/golden/prepared-orders-v1/expected.json`, so the expected evidence is committed rather than recomputed. The worker may add scoped `firn-conformance` dev dependencies only if tests import them directly.

The first foundation should reuse the package semantics already implemented in `firn-package`: canonical JSON manifest identity, required package layout, LZ4 Arrow IPC segments, identity file entries, segment entries, lifecycle status, signature signing input, and package verification. It should not reimplement package hashing.

## Acceptance criteria

- `firn-conformance` exposes a reusable golden-package harness that can build at least one deterministic prepared-orders package fixture from a declared fixture spec and compare actual package evidence to committed expected evidence.
- The committed expected evidence includes the manifest/package hash, package status, signature signing input, identity layout, identity file paths, file byte counts, file SHA-256 values, segment ids, segment paths, segment row counts, segment byte counts, and segment SHA-256 values.
- The harness compares actual evidence hash-by-hash and reports precise assertion failures for at least package hash mismatch, missing/extra identity files, changed file hash or byte count, changed segment hash or byte count, changed segment row count, changed lifecycle status, and changed signing input.
- The fixture proves deterministic package generation across 100 repeated builds in separate temporary directories on the current OS. Use the current known fixed fixture hash from `.10x/evidence/2026-07-06-package-builder-reader.md` only if the conformance fixture intentionally matches that source fixture; otherwise record the new committed expected hash in the test/harness data and evidence.
- The fixture verifies the package with `firn-package` verification before comparing golden evidence, so tampered packages fail both integrity and golden checks where applicable.
- Negative self-tests or deliberately corrupted expected evidence prove the harness would fail if package hash, file path set, file hash, file byte count, segment hash, segment row count, lifecycle status, or signing input comparisons were skipped.
- `crates/firn-conformance/src/lib.rs` remains a thin module/export root.

## Evidence expectations

Record focused `cargo fmt --all -- --check`, `git diff --check`, `cargo test -p firn-conformance --locked --no-fail-fast`, `cargo clippy -p firn-conformance --all-targets --locked -- -D warnings`, and `cargo test -p firn-package --locked --no-fail-fast`.

Run bounded mutation testing over the new golden-package conformance module when feasible, with `firn-conformance` as the test oracle. If mutation tooling is structurally blocked or too slow, record the exact limit and harden with negative self-tests before closure.

Significant closure must follow `QUALITY.md`. Reuse the CodeQL database path from `.10x/knowledge/quality-gate-execution.md` and parallelize independent checks where practical.

## Explicit exclusions

No live `firn run` orchestration, no DataFusion execution, no source execution, no cross-OS CI workflow changes, no dependency pin upgrade, no archive persistence contract, no `firn package archive` behavior, no CLI command changes, no MVP killer-demo harness, no chaos killpoints beyond package hash determinism checks, no golden update command, and no production package hashing changes unless the current public API cannot express the conformance invariant; any such need must be recorded as a blocker before editing outside `crates/firn-conformance/**`.

The broader `.10x/tickets/2026-07-05-conformance-chaos-golden.md` parent still owns full golden-package release gates across operating systems, 100-run stability, live run fixtures, full lifecycle chaos, resource data completeness, live Postgres conformance, and MVP killer-demo evidence.

## References

- `firn-the-book-of-the-system.md` Chapter 11 package layout/hash identity, Chapter 19.3 golden packages and determinism, Chapter 22 MVP scope, and Chapter 23 package determinism spike.
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/done/2026-07-05-package-builder-reader.md`
- `.10x/evidence/2026-07-06-package-builder-reader.md`
- `.10x/reviews/2026-07-06-package-builder-reader-review.md`
- `.10x/tickets/done/2026-07-06-prepared-package-chaos-conformance.md`

## Progress and notes

- 2026-07-06: Split from the conformance/chaos/golden parent after prepared-package chaos conformance closed. Current `firn-package` unit tests already prove one fixed package hash, but `firn-conformance` does not yet own a reusable golden-package harness or committed expected evidence. The book and active specs make the first conformance-owned hash-by-hash fixture comparison clear enough to execute without inventing live run, archive, or CLI behavior.
- 2026-07-06: Explorer Dalton independently confirmed no blocker for a prepared-package golden foundation and recommended a committed `prepared-orders-v1` expected fixture, 100 local regenerations, package verification before comparison, and negative self-tests for corrupted expected hash/file evidence. Full Chapter 19/22 source-run golden determinism remains later work because `firn run`, seeded run IDs/ULIDs, plan-text fixtures, and cross-OS CI are not available in this slice.
- 2026-07-06: Do not implement in the ticket-creation turn. Assign to a worker in a later turn with the references above and a write boundary of `crates/firn-conformance/**` plus scoped fixture expectation files if needed.

## Blockers

None for the first golden-package conformance foundation.
