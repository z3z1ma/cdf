Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md

# P3 B2: ranged row-group-parallel Parquet codec

## Scope

Move Parquet behind the format driver, implement bounded footer/page metadata, projection/predicate pushdown, deterministic row-group units and parallel ranged decode for local/remote sources, and remove collected `FormatRead` batches.

## Acceptance criteria

- Multi-file and row-group jobs preserve file/row-group/row order and whole-file manifest completion.
- Projection/predicate fidelity and schema/physical provenance are exact and conformance-tested.
- Remote decode uses overlapping bounded ranges without full download when the server supports ranges; fallback is explicit spool.
- Parquet reaches the ratified envelope and jobs-invariance hashes match.

## Evidence expectations

Raw arrow-rs roofline, TLC/nested/wide datasets, range trace, malformed footer/page fuzzing, pushdown equivalence, memory/cancellation, and local/remote profiles.

## Explicit exclusions

No Parquet destination writer.

## Blockers

Depends on L5, FX1, segmentation, and the execution host.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
