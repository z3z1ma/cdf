Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Data onramp schema discovery and reconciliation

## Context

The P2 data-onramp directive names three smoke-test failures that all begin with schema handling:

- CDF already has `SchemaSource::Discovered` in the kernel, but live plan/run paths still require a declared schema hash for important source kinds.
- Declarative types are a small subset of Arrow, with no decimal or nested vocabulary even though `VISION.md` Chapter 7 makes Arrow the closed type system.
- Parquet preserves physical schema while NDJSON is driven by declared schema, so declared schema and observed schema compete instead of reconciling through one model.

`VISION.md` sections 7.1-7.5, 8.2, 11.2, and 19.3 already define the target: Arrow schemas with metadata, `Declared | Hints | Discover`, validation programs that serialize verdict-bearing coercions, and semantic lockfile entries.

## Decision

Discovery is a compiler stage. A resource may use a declared schema, hints, or discover mode. Discover mode performs an explicit, bounded, plan-time discovery probe, distinct from no-I/O pushdown negotiation. Discovery probes may do source-specific bounded I/O where the probe contract names the bound: Parquet and Arrow IPC read schema/footer metadata, CSV/JSON/NDJSON sample bounded rows, SQL reads catalogs, and REST samples a recorded page.

Discovery output is a pinned schema snapshot. The snapshot is an Arrow schema with metadata, serialized under `.cdf/schemas/<resource>@<hash>.json`, referenced from `cdf.lock`, stamped into plans, and included in package identity just as declared schema hashes are. Runs execute against the pinned snapshot. Unpinned discover resources auto-probe and pin on first `plan` or `run` unless an inspection flag such as `--no-pin` explicitly asks not to write. Discovery is cached by source content identity where the source can provide one.

Observed physical schema is fact. Declared schema, hints, and pinned snapshots are constraints and projections over that fact. Reconciliation happens in one shared compiler stage for every format and produces a verdict-bearing coercion plan that is serialized into the validation program and package evidence.

Lossless widenings are automatic and recorded. The widening lattice includes signed and unsigned integer widths through 64-bit, `float32 -> float64`, integer to decimal where precision suffices, and `date32 -> timestamp` at the declared unit. String parse-coercions, including timezone-less strings to timestamps, are opt-in through `coerce_types`; they are never automatic width widenings. Lossy mappings require the existing `allow_lossy_mapping` allowance and otherwise fail plan time with both types and both fixes named.

Physical provenance is preserved. Every reconciled field that differs from the source physical field carries metadata naming the observed physical type, in addition to `cdf:source_name` and related CDF metadata, so evidence never loses what the source actually said.

The declarative schema vocabulary expands to the closed Arrow vocabulary required by `VISION.md` Chapter 7: signed and unsigned integer widths, float16/32/64, decimal128/256 with precision and scale, dates, times, timestamps with unit and optional timezone, durations, binary and large variants, utf8 and large_utf8, and nested list/struct/map forms. TOML may expose ergonomic string forms and structured forms, but both compile to Arrow data types and the published declarative JSON Schema must be regenerated when the vocabulary changes.

## Alternatives considered

Keep declared schemas mandatory for plan/run.

- Rejected because it makes the user transcribe metadata CDF can already read, contradicting `VISION.md` section 8.2 and the P2 golden paths.

Infer continuously like dlt.

- Rejected because CDF discovery is pinned once, then drift becomes a governed contract event. Silent perpetual mutation would weaken package determinism and reviewable plans.

Keep format-specific schema behavior.

- Rejected because NDJSON, Parquet, REST, SQL, and future sources must produce one reconciliation verdict model. Two truths create format-specific surprises.

## Consequences

Schema-source implementation work must update kernel/project/declarative models rather than only patching individual format readers.

The first child tickets may introduce transitional enum or serialization compatibility, but the final product behavior is pinned `Declared | Hints | Discover` semantics with a snapshot hash that survives in plans, lockfiles, and packages.

Conformance must own discovery snapshots, widening-lattice properties, plan/run package identity, and drift behavior before P2 workstreams close.
