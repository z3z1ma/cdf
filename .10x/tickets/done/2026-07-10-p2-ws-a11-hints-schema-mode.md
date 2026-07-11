Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p2-ws-a-discovery-compiler-stage.md
Depends-On: .10x/specs/data-onramp-schema-intelligence.md

# P2 WS-A11 Hints schema mode

## Scope

Complete Tier-0 `SchemaSource::Hints`: declarative syntax, validation, bounded discovery, shared reconciliation, snapshot/manifest rebuilding, lock pinning, plan/run execution, and evidence.

## Acceptance criteria

- `schema_mode = "hints"` requires a schema block and records its hash as hint authority.
- Discovery observes source physical schema and reconciles it against hints through the shared type policy.
- The reconciled snapshot preserves multi-file discovery-manifest identity and is written before its lock reference.
- Plan auto-pins and run consumes the pinned Hints snapshot without converting its identity to Discovered.
- Lossless physical-to-hint widening materializes in package output; incompatible/lossy mappings retain ordinary policy behavior.

## Blockers

None.

## Progress and notes

- 2026-07-10: Implementation uses the generic discovery artifacts and reconciler; no format-specific hints path was added. A review caught and repaired the manifest-metadata double-insertion boundary while rebuilding the reconciled snapshot.
- 2026-07-10: Closed after the three-crate test suite, focused declaration and Parquet widening tests, and strict all-target lint passed. Evidence: `.10x/evidence/2026-07-10-p2-a11-hints-schema-mode.md`. Review: `.10x/reviews/2026-07-10-p2-a11-hints-schema-mode-review.md`.
