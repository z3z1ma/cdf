Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Spillable package dedup

## Purpose and scope

This specification governs keyed merge and append exact-row dedup placement, typed equality, canonical order, memory/spill execution, evidence artifacts, failure behavior, and conformance. Destination merge conflict handling remains a downstream safety mechanism, not semantic authority.

## Semantic input and identity

The dedup barrier MUST consume only rows accepted by all preceding row verdicts. Residual/variant materialization, normalization, and compiled output schema conformance MUST complete before the barrier. Exact-row identity MUST include every final output field. Keyed rules MUST resolve each compiled key to exactly one final field through `cdf:source_name` and normalized-name authority; missing or ambiguous keys fail before extraction.

Canonical package row ordinal MUST derive from plan partition ordinal plus local accepted-row order before dedup. `first` retains the lowest ordinal per typed key; `last` retains the highest; `fail` returns a `Contract` error when any typed key repeats and MUST do so before a package output segment or destination staged segment is durable.

Typed equality MUST use versioned `cdf-dedup-key-v1` semantics and cover the complete supported Arrow vocabulary. Hash collisions MUST be exact-compared. If an Arrow type has no v1 encoding, planning fails rather than falling back to display/JSON text.

`cdf-dedup-key-v1` is defined as follows:

- null equals null only within the same compiled field type; null differs from every non-null;
- integers, booleans, decimals, dates, times, timestamps, durations, intervals, fixed-size binary, and binary/string variants compare their exact typed logical value; precision/scale, unit, timezone, width, and field type are schema authority and cannot vary row-to-row;
- float16/32/64 use IEEE 754 `totalOrder`, matching Arrow `RowConverter`: negative and positive zero differ, and NaN sign/payload distinctions are preserved;
- dictionaries compare decoded logical values, never dictionary indices or dictionary ordering;
- lists/fixed-size lists compare length, element order, nested nulls, and recursively encoded values;
- structs compare fields in compiled schema order and recursively encoded values;
- unions compare type id then the recursively encoded selected value;
- maps require valid non-null unique keys and compare logical entries independent of physical entry order by sorting exact encoded keys, then comparing recursively encoded values; `keys_sorted` is an optimization assertion, not equality authority.

Map duplicate/null-key violations fail the row/package through the compiled data-error policy before dedup. Conformance MUST freeze golden vectors for every clause and compare supported scalar behavior byte-for-byte with the pinned Arrow `RowConverter` where it already supplies the encoding.

## Memory and spill

The barrier MUST reserve payload, key encoding, hash/index, decision, sort/merge, and output working sets through the unified ledger. It MAY finish in memory only while the complete package state remains inside its grant. On pressure it MUST transition to typed disk-budgeted spill without rereading the source.

Spill payload MUST retain final-output Arrow values, canonical ordinals/ranges, schema identity, and source-position authority needed by later segment assembly. Key records MUST retain exact collision-comparison bytes and ordinal. Winner computation MUST be bounded under high cardinality and adversarial skew; recursive partitioning/merge fan-in and file handles have explicit caps.

The final decision stream MUST be ordered by canonical package row ordinal. A sequential bounded join against payload emits retained rows in original canonical order and emits dropped provenance in dropped-row ordinal order. Empty output remains valid when semantics permit. Spill partitioning, run sizes, merge fan-in, fast/spill crossover, memory budget, jobs, and cancellation timing MUST NOT enter package identity.

Disk budget MUST be reserved before spill growth. Exhaustion fails cleanly with required/available bytes, largest consumer, spill path class, and remediation. Scratch paths use owner-only permissions and opaque components, never serialize source values in names, and have idempotent cleanup/recovery ownership.

## Evidence artifacts

Artifact v2 `stats/dedup-summary.json` MUST contain rule id, key field authority, keep mode, input/output rows, duplicate-key count, dropped-row count, provenance format/version, shard count, and deterministic shard identities. It MUST NOT inline a cardinality-sized dropped-row array.

Each `stats/dedup-dropped/part-<ordinal>.parquet` shard MUST contain `package_row_ordinal:uint64` and `kept_package_row_ordinal:uint64`, sorted by dropped ordinal. Shard boundaries MUST use a plan-recorded row target and deterministic oversize rule, never live pressure. Files and summary participate in package identity. Readers MUST accept legacy v1 inline summaries and v2; replay MUST never reevaluate dedup.

## Performance and conformance

L5/A6 MUST compare bounded collision-safe partitioned hash/radix and external merge implementations on uniform high-cardinality, Zipf skew, all-identical, all-unique, wide composite, nested exact-row, and duplicate-heavy inputs. Selection MUST record CPU, peak RSS, spill bytes/write amplification, device utilization, key-encoding rate, and end-to-end barrier throughput.

Permanent properties MUST compare in-memory and forced-spill outputs/evidence across random batch boundaries, jobs, memory budgets above minimum, spill fan-in, hash collision injection, and cancellation. Results MUST match a simple reference evaluator for generated supported Arrow arrays. The 100 GB constant-memory stress law MUST include a forced-spill dedup case.

## Explicit exclusions

This spec does not deduplicate against prior packages/destination state, create approximate dedup, define cross-resource identity, claim secure scratch deletion, or permit destination-specific equality.
