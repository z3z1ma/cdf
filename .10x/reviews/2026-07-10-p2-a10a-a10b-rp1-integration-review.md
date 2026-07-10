Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-09-p2-ws-a10a-discovery-manifest-artifact-budget.md, .10x/tickets/done/2026-07-09-p2-ws-a10b-aggregate-schema-join-core.md, .10x/tickets/done/2026-07-10-p2-rp1-residual-envelope-codec.md
Verdict: pass

# P2 A10a/A10b/RP1 integration review

## Target

Adversarial review of the discovery-manifest artifact and executor budget, aggregate-schema join core, and canonical residual envelope codec.

## Assumptions tested

- A manifest sidecar can be additive without changing legacy snapshot bytes or Rust construction APIs.
- Content addressing alone prevents concurrent publication from replacing another writer's bytes.
- Aggregate field identity survives plan-time identifier normalization.
- Multi-file aggregation reuses rather than forks the schema reconciliation lattice.
- Exact residual JSON can represent Arrow values without display-string loss.
- Moving the existing engine capture boundary to the codec does not implicitly widen live verdict behavior.

## Findings

- Significant, repaired: adding optional public fields to `SchemaSnapshotReference` and `SchemaSnapshotArtifact` broke downstream struct literals despite serde defaults. Manifest references now use validated reserved metadata keys; all legacy bytes and 196 semver checks in each affected public crate pass.
- Significant, repaired: the first aggregate join keyed fields only by normalized Arrow names and overwrote source identity. Every top-level and nested lookup now uses authoritative `cdf:source_name` with Arrow name only as fallback; positive rename and negative collision regressions pass.
- Significant, repaired: same-directory temporary-file rename could still replace a concurrent winner. Publication now installs a synced temporary file with atomic no-clobber hard-link semantics, accepts an existing target only when bytes match, and fails closed where that guarantee is unavailable. Barrier-driven concurrent regressions pass.
- Pass: the aggregate join calls the existing lossless-widening predicate and does not introduce a second type system. Unsupported cross-family and semantic joins remain fatal.
- Pass: per-file/field verdicts are total and deterministic; candidate sorting and first-source-appearance ordering survive input permutation.
- Pass: the residual codec has a closed versioned descriptor and canonical bytes, rejects unsupported or noncanonical input with typed errors, and reconstructs exact Arrow arrays across the specified vocabulary.
- Pass: engine integration changes only `_cdf_variant` encoding and metadata. Existing capture selection, quarantine interaction, and evolution evidence stay unchanged.
- Pass: workspace, semver, docs, strict lint, dependency, audit, and artifact-integrity gates are green.

## Verdict

Pass. All material findings were repaired and regression-tested. The three child tickets are safe to close as foundations for the next integration tranche.

## Residual risk

The A10a manifest validator proves recorded selector evidence is internally valid but intentionally does not perform `stratified-hash-v1` candidate selection; A10g owns that executable behavior. A10c/A10d own live multi-file discovery and effective-schema runtime integration. RP2 owns live residual verdict routing. None of these deferred surfaces is claimed by this review.
