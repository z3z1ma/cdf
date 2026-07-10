Status: done
Created: 2026-07-09
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/decisions/multi-file-discovery-aggregation-and-budget.md, .10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md

# P2 WS-A10b aggregate schema join core

## Scope

Implement the pure, format-neutral aggregate-schema join and per-file schema verdict model over ordered physical Arrow schemas. It must reuse the ratified reconciliation widening lattice rather than create a second type system.

## Acceptance criteria

- Equal fields preserve; one-direction transitive lossless widening selects the wider type; pairs without a ratified path are incompatible.
- List/struct/map children recurse through the same rule.
- Fields missing from any compatible file become nullable, retain deterministic null-origin evidence, and produce a per-file missing-null decision.
- Source field identity is the unnormalized source name. Candidate order is canonical location order and aggregate field order is first appearance.
- Reserved CDF metadata is regenerated; non-reserved metadata is retained only when identical across applicable candidates; conflicts produce deterministic per-file metadata-variance evidence.
- The result contains one aggregate/effective schema plus total per-file/field verdicts suitable for the discovery manifest and later coercion compilation.
- Unsupported signed/unsigned, integer/float, decimal/timezone, dictionary, extension, or nested joins fail as incompatible unless already present in the active widening lattice.
- Input permutation after canonical sorting produces identical schema, verdict, and canonical evidence.

## Evidence expectations

Unit and property tests over generated Arrow schemas, widening composition reuse, nested/missing/metadata/collision adversarial cases, canonical serialization fixtures, and no-I/O review.

## Explicit exclusions

No file I/O, candidate listing, snapshot writes, batch array materialization, package changes, file quarantine, or CLI behavior.

## Progress and notes

- 2026-07-09: Opened as the pure semantic lane parallel to A10a.
- 2026-07-10: Implemented the pure aggregate Arrow schema join in `crates/cdf-contract/src/aggregation.rs` and exported the existing B2 lossless-widening predicate for reuse. Candidates are canonicalized by location; unnormalized source names own identity and first appearance owns aggregate order. The join recurses through struct/list/map shapes without admitting container-family, map-sortedness, struct-order, present-nullability, or other unratified changes. Missing fields widen only aggregate nullability and receive deterministic `cdf:null_origin` metadata. Reserved CDF metadata is regenerated, while conflicting non-reserved field and schema metadata produces deterministic per-file variance evidence.
- 2026-07-10: Added total per-file/field verdicts with pass/coerced/fatal outcomes, incompatibility reports, canonical serialization coverage, adversarial nested/type/nullability/metadata/collision cases, and property coverage for signed widening composition and input permutation. The implementation performs no I/O and does not materialize arrays.
- 2026-07-10: Verification passed: `cargo check -p cdf-contract --all-targets --locked`; `cargo test -p cdf-contract --locked --no-fail-fast` (55 passed); focused `cargo test -p cdf-contract --locked aggregation -- --nocapture` (9 passed); `cargo clippy -p cdf-contract --all-targets --locked -- -D warnings`; `cargo fmt --all -- --check`; and `git diff --check`.
- 2026-07-10: Parent review found that the first implementation incorrectly keyed physical candidates only by `Field::name()`. Repaired the identity boundary to use authoritative `cdf:source_name` metadata with the Arrow field name solely as fallback, consistently across top-level and nested order, matching, collision detection, verdict paths, and emitted aggregate field names. Added a positive regression where distinct physical top-level and nested names share source identity, plus a negative regression for distinct physical fields colliding on one source identity.
- 2026-07-10: Post-repair verification passed: focused aggregation tests (10 passed); full `cdf-contract` tests (59 passed); all-target check; strict all-target clippy; workspace formatting check; and diff check.
- 2026-07-10: Parent integration verification and adversarial review passed. Evidence: `.10x/evidence/2026-07-10-p2-a10a-a10b-rp1-integration.md`. Review: `.10x/reviews/2026-07-10-p2-a10a-a10b-rp1-integration-review.md`.

## Blockers

None.
