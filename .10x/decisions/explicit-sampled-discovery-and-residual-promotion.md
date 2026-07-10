Status: active
Created: 2026-07-09
Updated: 2026-07-09

# Explicit sampled discovery and residual promotion

## Context

P2 A10 established exhaustive multi-file Parquet and Arrow IPC metadata discovery as the default. During the implementation pause, the user challenged an overly broad interpretation that sampling itself was forbidden and proposed a stronger governed path: allow an explicitly sampled pin, process every runtime partition against that baseline, preserve nonconforming values in `_cdf_variant`, rediscover later, and promote retained residuals into typed destination columns.

`VISION.md` §7.5 already defines `_cdf_variant` as the honest representation of unknown or contract-violating substructure and makes later promotion a recorded contract-evolution event. Research in `.10x/research/2026-07-09-sampled-discovery-variant-promotion.md` confirmed that the missing pieces are truthful sampled-coverage evidence, scalar/path residual capture, stable correction identity, and a plan/package/gate promotion workflow.

## Decision

CDF supports an explicit sampled discovery coverage mode. Exhaustive metadata discovery remains the default for Parquet and Arrow IPC. Executor budget exhaustion never silently changes exhaustive discovery into sampling.

A sampled snapshot is a pinned baseline, not a claim that all matched data conforms. Its plan, snapshot, and package evidence MUST identify coverage as sampled; distinguish matched, probed, and unprobed candidates; record the deterministic selector and resolved limits; and never invent a physical-schema hash or discovery verdict for an unprobed candidate.

Every processed runtime partition still enters the ordinary reconciliation and validation program. Sampling weakens only plan-time observation, never runtime verdicts, package evidence, destination receipts, checkpoint gating, or replay.

When the validation program can isolate a nonconforming field or path and all required control/identity fields remain valid, residual capture MAY preserve the accepted typed projection of that row and place the original nonconforming value into the final nullable `_cdf_variant` column. Unknown fields and scalar/path mismatches are in scope, extending the existing nested Struct/List/Map slice. Unsafe partial acceptance remains row- or file-quarantine with a named rule.

`_cdf_variant` is a portable semantic `json` projection with versioned, canonical, type-aware encoding for values ordinary JSON cannot faithfully represent. Exact replay evidence remains in the package while retained. No value may be silently stringified in a way that loses reconstructability.

Promotion is explicit, plan-first, and evidence-preserving. It compiles a schema diff and residual extraction/coercion program, creates new immutable correction or rematerialization packages, obtains destination receipts, and passes the normal checkpoint gate. It never rewrites an old package, silently mutates a pin, or bypasses destination capability sheets.

CDF-loaded rows MAY use a framework-owned immutable row-provenance address for correction. This is operational identity, not a user-declared merge key; append remains keyless. A destination that cannot persist and target that address MUST use a declared correction-sidecar or versioned-rematerialization strategy rather than an unsafe UPDATE. Promotion from residual bytes is available only while an exact retained package or a sufficient destination residual representation remains readable; retention/GC reports that capability honestly.

The promotion command is a schema-evolution surface, distinct from the existing cursor/source re-extraction meaning of `cdf backfill`. Its exact CLI grammar and crash-safe transaction ordering are governed by a focused specification before implementation.

## Alternatives considered

Forbid sampling entirely.

- Rejected. Sampling is a useful bounded planning heuristic at very large cardinality. Correctness comes from truthful coverage evidence plus total runtime reconciliation, not from pretending plan-time observation must always be exhaustive.

Automatically sample when an exhaustive budget is exceeded.

- Rejected. That silently weakens evidence and makes equal configuration produce different semantic coverage under different executor resources.

Quarantine every row containing an unknown value.

- Rejected. It discards a valid typed projection when the offending path can be isolated safely and contradicts the vision's variant-capture purpose.

Update prior append rows using inferred business keys.

- Rejected. Append requires no semantic key, and inferred keys would create nondeterministic or incorrect mutations. Correction identity belongs to framework provenance.

Overload `cdf backfill` for residual promotion.

- Rejected. Existing backfill means bounded source/cursor re-extraction. Residual promotion reads governed package/destination evidence and changes schema authority; conflating them would obscure plans and recovery.

## Consequences

The discovery manifest needs explicit coverage/participation states and optional probe/schema fields. The validation program needs residual-capture verdicts at field/path grain and a canonical type-aware envelope. Plans/packages need correction lineage and stable row-provenance references. Destination sheets need correction capabilities. Retention/GC must expose whether residual promotion remains executable.

Before implementation, focused specifications must close the deterministic sampling selector, control-field safety rules, row-provenance encoding and persistence, destination fallback behavior, promotion transaction/crash ordering, and retention interaction. Those details are intentionally not inferred by this architectural decision.
