Status: open
Created: 2026-08-03
Updated: 2026-08-03
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`

# C1 semantic registry core and consumer migration

## Scope

Implement the canonical semantic-reference and data-only registry kernel, the six ratified
built-in definition families, exact resolution/validation/hash behavior, and direct migration of
all current producers and behavior consumers.

The executor owns the smallest acyclic crate/module placement supported by the current workspace
dependency graph. The intended boundary is a focused data-only `cdf-semantic` crate below
contract/declarative/project/adapters and above `cdf-kernel`; a materially smaller boundary is
acceptable only if it preserves one resolver without moving project/driver code into the kernel.

## Non-goals

- project-defined semantic definition files (C2);
- lock/manifest snapshot publication (D1);
- SQL semantic annotation syntax (D3);
- Python/Wasm predicates or externally loaded registries;
- broad destination mapping rewrites unrelated to existing semantic behavior.

## Acceptance criteria

1. Canonical reference parsing/rendering enforces the active grammar, parameter schema, ordering,
   bounds, and round-trip laws; aliases and unversioned forms do not exist.
2. Built-in catalog construction validates uniqueness and produces stable definition hashes for
   `cdf.variant@1`, `cdf.package_row_ordinal@1`, parameterized `cdf.pii@1`, and all three
   PostgreSQL exact-text definitions.
3. Every current producer writes only the canonical references. Old semantic strings and the
   configurable variant invalid state are removed directly with no compatibility reader.
4. Declarative/authored unknowns fail `Contract`; source-observed unknowns fail `Data`; compiled
   runtime absence fails `Internal` at the appropriate existing boundaries.
5. Contract redaction uses resolved privacy classification rather than `pii:` prefix inference and
   preserves all current PII verdict actions across Arrow types.
6. Variant/package-row-ordinal ownership uses resolved canonical definitions plus existing shape
   fences and cannot be forged by an unrelated semantic.
7. PostgreSQL JSON/JSONB/NUMERIC exact-value discovery, mapping, physical-provenance validation,
   binary COPY, replay, and correction behavior remain lossless through adapter-owned mapping
   profile ids; unknown semantics do not silently invoke native reconstruction.
8. Destination semantic mapping resolution is deterministic, most-specific, ambiguity rejecting,
   and permits base Arrow fallback only when the definition says so.
9. Focused tests cover valid/invalid grammar, hash determinism, unknown ownership, direct migration,
   redaction equivalence, destination ambiguity, and PostgreSQL exact-value equivalence.
10. Formatting, `git diff --check`, affected-crate tests/checks, and affected-crate strict Clippy
    pass. No whole-workspace test suite is run.

## References

- `.10x/specs/semantic-type-registry.md`
- `.10x/decisions/semantic-reference-registry-and-unknown-policy.md`
- `.10x/research/2026-08-03-semantic-authority-inventory.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`
- `.10x/knowledge/type-policy-authority.md`

## Assumptions

- Canonical grammar, built-in ids, fail-closed unknown behavior, project-definition staging, and
  mapping-selector boundary are user-ratified in the governing records.
- Arrow remains the canonical type system; semantics cannot change physical values at runtime.
- Existing PII actions and PostgreSQL exact-value fidelity are behavior to preserve, not legacy
  spellings to preserve.

## Journal

- 2026-08-03: Opened after C0 inventory and user ratification. No product code changed in this
  shaping turn.

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending one independent lane-boundary red-team review.

## Retrospective

Pending execution.
