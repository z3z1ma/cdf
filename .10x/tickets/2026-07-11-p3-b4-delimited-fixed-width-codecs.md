Status: active
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md

# P3 B4: delimited and fixed-width codecs

## Scope

Implement streaming CSV/TSV/custom dialect parsing with safe quote-aware chunk parallelism and an explicit-layout fixed-width codec across catalog encodings, including bounded dialect/layout discovery suggestions.

## Acceptance criteria

- Pinned options cover catalog semantics; runtime never re-guesses dialect/layout.
- Quoted newlines, ragged/short/long rows, multibyte boundaries, null tokens, comments, and malformed records obey exact quarantine/error policy.
- Parallel and sequential results/package hashes match; unsafe split inputs automatically use sequential decode.
- CSV meets the envelope and fixed-width reaches the selected reference ratio.

## Evidence expectations

arrow-csv/reference benchmarks, adversarial dialect/encoding corpus, split-boundary properties, fixed-layout goldens, memory/cancellation, and jobs invariance.

## Explicit exclusions

No spreadsheet parsing.

## Blockers

Depends on transforms, FX1, and L5.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`

## Progress and notes

- 2026-07-12: Landed the prerequisite physical-schema authority needed by row codecs. Decode requests now receive the exact planned Arrow schema from the effective schema catalog rather than only its hash, so CSV/fixed-width execution can decode against pinned discovery without runtime reinference. Parquet/Arrow IPC/file-source tests and strict affected-cone Clippy pass. Evidence: `.10x/evidence/2026-07-12-fx1-physical-decode-schema-authority.md`.
- 2026-07-12: Added `cdf-format-delimited::CsvFormatDriver` using Arrow CSV's push decoder over accounted chunks. Schema inference retains bounded source chunks through the shared neutral `AccountedChunksReader`; execution streams batches with pre-admitted output leases. CLI/project/source registries compose it, local discover/run passes, and the source CSV fallback now fails closed. The ticket remains open for TSV/PSV/custom options, fixed-width, multiline/oversized-record RSS and fuzz proof, parallel safe-boundary chunking, and the ≥0.6x/400 MB/s envelope.
- 2026-07-18: Closed the registered delimited-dialect slice without extending source/runtime enum wiring. `cdf-format-delimited` now has one `DelimitedFormatDriver` implementation with descriptors for `csv`, `tsv`, `psv`, and explicit `delimited`; `CsvFormatDriver` remains a type alias for existing call sites. Dialect options (`delimiter`, `header`, `header_validation`, `quote`, `escape`, `terminator`, `comment`, `truncated_rows`) are canonicalized by the format driver and compiled into the decode session once, so execution never re-guesses dialect. CLI, source-files test, project test, and benchmark registries register the same dialect surface. `cdf add` extension inference now maps `.tsv`/`.tab` to `tsv` and `.psv` to `psv` instead of silently compiling TSV as CSV. B4 remains active for fixed-width, chunk-parallel CSV, RSS/fuzz proof, and envelope/reference closure.
- 2026-07-19: The first EC2 CSV comparison exposed a benchmark bias rather than a CDF bottleneck: `ReferenceWorkload::ArrowCsv` called Arrow's schema inference with no record bound, so the nominal raw reference scanned the entire 232,675,583-byte file once for inference and then reopened and decoded it. Its three wall samples were 2.480s, 2.438s, and 2.426s, while the already-pinned CDF package path took 1.129s, 1.074s, and 1.066s. Added an explicit positive `infer_rows` authority to the reference workload so its discovery work is bounded and recorded. This changes benchmark truth only; no product hot path changed. The corrected reference and CDF aggregate envelope remain to be measured on a clean EC2 revision.

## Evidence

- 2026-07-18 delimited-dialect slice:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-delimited --lib --locked -j 12` — passed, 3 passed. Proves canonical options, custom-delimiter requirement, empty schema-bearing CSV batch behavior, and chunked TSV streaming decode.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files add_infers_registered_delimited_format_ids_by_extension --lib --locked -j 12` — passed, 1 passed. Proves add-time inference maps `.csv`, `.tsv`/`.tab`, and `.psv` to registered format ids.
  - `CARGO_BUILD_JOBS=12 cargo check -p cdf-source-files -p cdf-cli -p cdf-project -p cdf-benchmarks --locked -j 12` — passed. Proves the product, project/test, and benchmark registries compile with the expanded delimited surface.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-format-delimited -p cdf-source-files -p cdf-cli -p cdf-project -p cdf-benchmarks --all-targets --locked -j 12 -- -D warnings` — passed. Proves strict lint cleanliness for the affected cone.
  - Limit: this slice does not claim fixed-width support, safe quote-aware chunk-parallel splitting, malformed corpus/fuzz proof, or the CSV throughput envelope; no Parquet/DuckDB hot-path default changed, so no TLC timing gate is claimed.
- 2026-07-19 CSV reference-bias correction:
  - `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-benchmarks references::tests --lib --locked -j 12` — passed, 9 passed. Proves the reference runner cone remains valid after making bounded CSV inference explicit.
  - Limit: the pre-change EC2 observation is diagnostic evidence of the old full-inference bias, not acceptance evidence for the corrected CSV envelope.

## Review

Pass for the delimited-dialect slice. The change stays behind the native format-driver boundary, does not add source/destination-specific orchestration branches, does not change CSV's default canonical options or streaming decode path, and fixes a half-wired `.tsv` inference smell. The ticket remains active for the larger B4 acceptance criteria.

## Retrospective

Extension inference is part of the public source experience, so it must agree with the registry. A format id like `csv` can share implementation with `tsv` and `psv`, but the compiled descriptor should still name the actual dialect so package identity and diagnostics say what the operator meant.
