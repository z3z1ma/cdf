Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md
Verdict: pass

# P3 A6 spillable package dedup review

## Target

Final-output placement, `cdf-dedup-key-v1`, in-memory/external winner selection, Arrow payload spool/rejoin, v2 provenance shards, spill/memory admission, and 100 GiB stress through commit `fecc4159` plus the closure delta.

## Findings

No critical or significant A6-scoped defect remains.

- Placement: dedup consumes accepted rows only after variant/residual materialization, normalization, and compiled output-schema conformance.
- Equality: non-map leaves are byte-for-byte pinned Arrow RowConverter semantics; dictionaries decode/remap logical values; nested maps recursively sort exact encoded keys independent of physical order. Duplicate/null map keys fail before indexing.
- Slice isolation: lists, views, maps, dense unions, and dictionaries normalize referenced children so invalid unselected backing values cannot reject selected rows.
- Winner authority: first/last/fail operate on canonical ordinals; fail certifies uniqueness before output; external merge decisions return in ordinal order.
- Boundedness: fast keys transition losslessly under pressure, external runs have capped fan-in/sort memory, payloads stream through Arrow IPC, provenance shards are deterministic, and the 100 GiB run held 37.24 MiB RSS.
- Failure behavior: disk exhaustion, cancellation/drop cleanup, owner-only scratch, and shared spill-budget exhaustion are tested; legacy v1 provenance remains readable.

## Verdict

Pass. A6 is complete and removes A5's package-global constant-memory blocker.

## Residual risk

The 100 GiB cell is a local wide-binary/all-unique workload. Cross-format and remote overlap belong to WS-B/G, while the permanent 1 TB end-to-end law belongs to WS-F. A5 must carry upstream memory ownership into this already-bounded barrier rather than reconstructing it.
