Status: done
Created: 2026-07-09
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/decisions/multi-file-discovery-aggregation-and-budget.md, .10x/specs/sampled-schema-discovery-coverage.md, .10x/tickets/done/2026-07-08-p2-ws-a1-schema-source-model-snapshot-foundation.md, .10x/tickets/done/2026-07-09-p2-ws-a8-autopin-lockfile-no-pin.md

# P2 WS-A10a discovery manifest artifact and executor budget

## Scope

Add the backward-compatible serialized foundation for multi-file discovery: validated executor budgets, typed candidate identity/strength/probe/verdict entries, a canonical content-addressed discovery-manifest sidecar, and optional snapshot/lock references that preserve every legacy v1 byte/hash contract.

This child changes artifact/model plumbing only. It does not enumerate multiple files or change discovery results.

## Acceptance criteria

- A validated executor discovery budget defaults to 64 MiB per file, 128 MiB total in flight, and 8 probes; zero, overflow, and per-file-greater-than-total shapes fail precisely.
- The resolved budget is part of canonical discovery-manifest evidence but not candidate membership or schema-join semantics.
- The manifest has deterministic versioned canonical JSON and hash identity over resource, baseline/effective schema references when present, coverage mode, selector evidence when sampled, resolved budget, normalizer/policy versions, and sorted candidate/probe/verdict entries.
- Candidate identity explicitly records transport, canonical location, size/mtime when present, identity value and strength, participation, metadata variance, and verdict. Physical-schema hash/probe bytes/schema verdict are required only for probed candidates and forbidden for unprobed candidates.
- The sidecar is written atomically under a content-addressed `.cdf/schemas/` path; unsafe paths, missing files, tampering, and hash mismatch fail hydration.
- Snapshot/lock references gain optional manifest fields with serde defaults/omission so every existing v1 artifact and byte-stability test remains unchanged.
- Public/compiler models below the CLI remain executor/transport neutral.

## Evidence expectations

Canonical round-trip/hash fixtures, v1 compatibility fixtures, unrelated-lock preservation, atomic write/tamper/path tests, budget validation tests, semver checks, and artifact-focused adversarial review.

## Explicit exclusions

No candidate enumeration, schema joining, format probing, CLI reporting, package stamping, runtime coercion, quarantine, checkpoint, or conformance behavior.

## Progress and notes

- 2026-07-09: Opened after the user ratified `.10x/decisions/multi-file-discovery-aggregation-and-budget.md`.
- 2026-07-09: Paused before implementation. The user proposed explicit sampled discovery with runtime residual capture and later promotion. The manifest must be able to represent an unprobed candidate without inventing a physical-schema hash or discovery verdict, and must distinguish sampled baseline coverage from exhaustive coverage. See `.10x/research/2026-07-09-sampled-discovery-variant-promotion.md`.
- 2026-07-09: The user ratified the architectural recommendation in `.10x/decisions/explicit-sampled-discovery-and-residual-promotion.md`. Exact selector evidence remains a blocker because it determines the durable manifest fields and validation invariants.
- 2026-07-10: `stratified-hash-v1` and probed/unprobed validation are ratified in `.10x/specs/sampled-schema-discovery-coverage.md`; this artifact foundation is executable again.
- 2026-07-10: Implemented the executor-neutral serialized foundation. `DiscoveryExecutorBudget` has validated 64 MiB/file, 128 MiB in-flight, and 8-probe defaults and rejects zero, per-file-above-total, and scheduled-byte overflow shapes. The versioned discovery manifest records baseline/effective hashes, coverage and selector evidence, resolved budget, normalizer/policy versions, sorted bounded candidate identities, participation, metadata variance, probe evidence, and typed schema verdicts; sampled/exhaustive and probed/unprobed invariants fail closed.
- 2026-07-10: Added canonical content hashing and atomic content-addressed sidecar storage under `.cdf/schemas/`, including deterministic ordering, no-overwrite behavior, and hydration rejection for unsafe paths, missing files, tampering, hash/reference mismatch, and cross-resource linkage. Manifest-linked schema snapshots use version 2 hash input that binds the exact sidecar hash/path, while the existing constructor and artifact version remain byte/hash-exact version 1.
- 2026-07-10: Preserved public Rust and serialized compatibility by carrying the optional typed manifest reference through validated reserved keys in the existing snapshot-reference metadata map. An initial direct-field implementation was rejected by `cargo-semver-checks`; the repaired shape leaves `SchemaSnapshotReference` and `SchemaSnapshotArtifact` struct literals unchanged, omits all manifest keys for legacy snapshots/locks, and passes all 196 semver checks in both `cdf-kernel` and `cdf-project`.
- 2026-07-10: Verification passed `cargo test -p cdf-kernel -p cdf-project --lib --locked --offline --no-fail-fast` (11/11 and 119/119), `cargo check --workspace --all-targets --locked --offline`, scoped all-target clippy with `-D warnings`, scoped rustfmt, `git diff --check`, and separate `cargo semver-checks check-release --baseline-rev HEAD` runs for both owning crates. Focused fixtures prove the exact legacy v1 hash, deterministic manifest identity, sampled participation honesty, atomic round-trip, and v2 missing/tampered sidecar failure.
- 2026-07-10: Parent review found that same-directory temporary-file `rename` still had replace semantics after the pre-read. Repaired publication to use a synced temporary file plus atomic no-clobber hard-link installation. A concurrent winner is accepted only when the installed bytes are identical; conflicting bytes fail without replacement. Unsupported filesystems fail closed with an explicit hard-link requirement rather than using a partial-write fallback; Unix directory metadata is synced after publication, while non-Unix retains the synced-file/no-clobber guarantee available through portable `std` APIs. A barrier-driven regression proves identical concurrent installs converge, conflicting installs preserve exactly one complete winner, and temporary files are cleaned up.
- 2026-07-10: Post-review verification passed `cdf-kernel` 11/11 and `cdf-project` 120/120, workspace all-target check, scoped all-target clippy with `-D warnings`, scoped rustfmt, `git diff --check`, and both 196/196 semver suites.
- 2026-07-10: Parent integration verification and adversarial review passed. Evidence: `.10x/evidence/2026-07-10-p2-a10a-a10b-rp1-integration.md`. Review: `.10x/reviews/2026-07-10-p2-a10a-a10b-rp1-integration-review.md`. Retrospective publication guidance: `.10x/knowledge/content-addressed-sidecar-publication.md`.

## Blockers

None.
