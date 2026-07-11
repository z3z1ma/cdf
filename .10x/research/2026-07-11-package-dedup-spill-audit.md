Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Package dedup spill audit

## Question

How can keyed and exact-row package dedup preserve current package-order semantics at terabyte scale with bounded memory, complete provenance, and jobs invariance?

## Sources and methods

Inspected the validation-program compiler/evaluator, engine pending-dedup path, keyed/exact-row decisions, live-path tickets/evidence, normalization/variant execution order, tests for first/last/fail/null behavior, and active memory/operator/segmentation specifications.

## Findings

The evaluator receives all accepted batches at once. It converts selected Arrow columns into owned row keys, stores every distinct key and every row reference in a `HashMap<OwnedRow, Vec<PackageRowRef>>`, allocates retained bitmaps for all batches, then builds and sorts a `Vec<DedupDroppedRow>`. The engine separately retains all accepted batches and variant values. Payload, keys, row references, bitmaps, and provenance all scale with package size.

Current semantics are package-scoped and canonical-order based: keyed merge rules support `first`, `last`, and `fail`; explicit append exact-row keeps first; destination state is not consulted; replay consumes already-deduped package segments. Nulls are typed identity values in the current evaluator. At most one package dedup rule is compiled.

The exact-row decision says identity is every normalized output field including nulls/nested values. Current execution evaluates accepted Arrow rows before appending residual/variant values and before output normalization/schema conformance. Therefore rows that differ only in `_cdf_variant` can be incorrectly considered duplicates. This is active-record/source drift, not merely a performance problem.

The summary artifact embeds every `(dropped ordinal, kept ordinal)` pair in one JSON vector. Even if data/key state spills, constructing that vector violates bounded metadata and produces inefficient evidence for high duplicate counts.

A bounded algorithm needs a canonical payload spool and an external decision index. Each accepted post-output-shape row receives its canonical package row ordinal. Key identity records can use an internal typed key encoding plus ordinal; equality must be exact after hash collisions. A bounded partitioned-hash/radix algorithm or external merge algorithm can decide winners, then emit decisions sorted by ordinal. Sequentially joining that decision stream against the canonical payload spool restores retained rows in canonical order before segment assembly.

`keep=fail` cannot certify success or permit destination staging until the entire barrier proves uniqueness. `keep=last` cannot release an earlier row before the end. A uniform barrier avoids mode-dependent package/staging crash behavior; a fast in-memory path may avoid disk when the complete state fits its ledger grant.

## Conclusion

Implement dedup as an explicit spillable ordered barrier after accepted rows have their final package output shape and before canonical segment assembly. Use an accounted in-memory fast path that transitions losslessly to budgeted canonical payload/key spools and a measured bounded external decision algorithm. Rejoin decisions by canonical ordinal.

Replace the unbounded dropped-row JSON vector in a new artifact version with a bounded aggregate `dedup-summary.json` plus deterministically sharded Parquet provenance containing the two ordinals. Preserve old artifact reading for replay/inspection compatibility.

## Limits

WS-L/A6 must benchmark partitioned hash/radix versus external merge under uniform, skewed, duplicate-heavy, nested, and wide keys before selecting the external algorithm. The internal key encoding is not artifact identity, but its equality results require exhaustive conformance against the current logical semantics and all supported Arrow types.
