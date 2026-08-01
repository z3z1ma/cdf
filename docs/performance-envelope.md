# CDF Terabyte-Scale Performance Envelope

> **P3 IMPLEMENTATION CLOSEOUT — green results, partial cells, accepted residuals, and unproven ambitions are shown without relabeling.**

This document is generated from a host-labelled reconciliation manifest and the exact registered destination-path report; edit those inputs, not this file.

## Evidence authority

- Reconciliation manifest: `sha256:7510529b772c6f9b5cedf6bf4f8438708a3b824c92780f40abda899d143dbdb5`
- Pre-optimization baseline: [docs/performance-baseline.md](performance-baseline.md)
- Comparisons are host- and mode-specific. A row is never promoted by combining measurements from different hosts.
- `partial`, `accepted residual`, and `not demonstrated` are deliberately non-green.

## Performance target matrix

| Workload | Target | Absolute result | Relative result | Memory | Host / mode | Status |
|---|---|---|---|---|---|---|
| Parquet file/glob → package | ≥0.7× raw Arrow; ≥1.5 GB/s aggregate; row-group parallel | 425.1 MB logical decoded in 113.9 ms (3.73 GB/s logical); no like-for-like 1.5 GB/s physical package cell | 0.90× raw Arrow decode; governed FineWeb path 0.779× favorable same-data reference | bounded row-group frontier; 8.59 GB scale curve stayed below 1.6 GB RSS | Apple M5 Pro local release controls / warm local | partial |
| CSV → package | ≥0.6× raw Arrow; ≥400 MB/s aggregate | 257.0 MB/s over four files | 0.680× raw Arrow CSV with the same bounded inference pass | one-pass sequential decode; no retained parallel pre-scan | EC2 c7i.4xlarge / 16 vCPU / tuned gp3 / warm local, median-of-3 | partial |
| NDJSON/JSON decode | ≥300–500 MB/s aggregate; ≥3× deleted DOM path | 450.4 MiB/s and 9.3 million rows/s | 3.10× conservative deleted-DOM control; full inference 1.02× raw Arrow | streamed tape decode with accounted windows | Apple M5 Pro local release control / warm isolated codec | green |
| Contract validation | ≥1 GB/s/core at 64k rows | 3.016–7.254 GB/s across all 12 gated cells | 18.1%–43.5% of 16.689 GB/s memcpy roofline | selected evidence materialization measured separately from hot masks | EC2 c7i.4xlarge / taskset one core / warm isolated kernel, median-of-7 | green |
| Package build/hash | ≥70% sequential-write roofline; hashing ≤5% wall | 1,033.8 MiB/s over three alternating 32 GiB samples | 0.903× direct-I/O device write; SHA-256 attributable wall 0.06% | 1,024 independently synced segments per sample; each sample exceeded host RAM | EC2 c7i.4xlarge / tuned gp3 / sustained cold-capacity release | green |
| Package → DuckDB | ≥1 million TLC rows/s; ≥5× removed scalar path | 4,014,348 rows/s over 41,169,720 rows; 10.256 s median | stock public-ABI parallel scanner; materially above the removed scalar/appender floor | bounded by compiled DuckDB admission and cgroup evidence | EC2 c7i.4xlarge / host-class-649c6f28be3544c8 / warm local full product, median-of-3 | green |
| Package → PostgreSQL | binary COPY; ≥2× CSV COPY | 1,801,714 rows/s local binary COPY control; 672,531 rows/s full-year direct-target sample | 3.00× retained CSV COPY control | Arrow batches encoded incrementally; no full-package text staging | EC2 destination matrix plus local temporary PostgreSQL control / loopback release | green |
| Package → Parquet | ≥60% device-write roofline | 4,953,986 rows/s over full-year TLC; selected-path report 1,362 MiB/s | isolated writer 0.786× raw durable write; selected path clears 0.60 floor | bounded staged row-group writers; no full-table buffer | EC2 destination matrix plus Apple M5 Pro writer control / warm local release | green |
| Full-year TLC HTTPS → DuckDB | ≤1.5× download + native ingest; I/O dominated | 15.783 s with explicit complete spool; 2,608,431 rows/s; exact 41,169,720 rows | 19.4% faster than unchanged 19.580 s overlap default; original composite target remains red | 3.91 GiB process peak; 6.22 GiB cgroup peak; zero OOM/pressure/spill | EC2 c7i.4xlarge / Hugging Face public mirror / live uncontrolled network | accepted residual |
| 1 TiB synthetic glob → Parquet | default budget; stable RSS; linear scaling to device saturation | 1.0086 TiB and 5.436 billion rows in 499.07 s; 2.222 GB/s logical; 10.892 million rows/s | 5.40× the identical prior governed run; 6.78 equivalent cores and 84.8% of the physical-core roofline | 3.923 GB process peak under default 4 GiB; 3.163/3.651 GB managed peak; zero spill/OOM | EC2 c7i.4xlarge / 250 GiB tuned gp3 / generated default-policy scale run / 5 GiB cgroup | partial |
| Correctness/evidence overhead | ≤10% versus equivalent raw read + write | hashing 0.06%; CLI rendering -0.44% within variance; validation and destination work reported separately | no single semantically equivalent whole-product raw comparator exists | correctness structures share the governed memory ledger | mixed, explicitly separated component controls / component controls only | not demonstrated |

## Architecture and correctness matrix

| Workload | Target | Absolute result | Relative result | Memory | Host / mode | Status |
|---|---|---|---|---|---|---|
| Constant process memory | RSS is a function of policy, not input size; clean failure below minimum | 5/20/100 GiB under 2 GiB with 1.54–1.58 GiB RSS; 1 TiB at 3.923 GB RSS under default 4 GiB | flat memory curve across 20× input growth | 64 MiB policy fails typed Data before artifacts; forced spill laws clean up | EC2 c7i.4xlarge / cgroup-enforced release | green |
| Jobs invariance | fixed inputs produce identical package/receipt/state semantics at jobs 1/N | format, destination, REST, SQL, failure, drain, and isolated-worker matrices pass | jobs 1/2/auto/4 retain exact logical artifacts | task/permit/frontier peaks remain within admitted authorities | deterministic fixtures plus Apple M5 Pro scale curve / multi-job conformance | green |
| Core native format engines | streaming, fail-closed, registry-owned Parquet/CSV/JSON/NDJSON and transforms | all core engines and gzip/zstd/bzip2/xz/lz4/snappy/brotli transforms are terminal | adding a codec does not extend generic source/runtime match trees | byte sources, transforms, codecs, and batches share accounted ownership | cross-platform conformance plus named release controls / native registry | green |
| Implemented foreign boundaries | honest transfer/copy/memory/cancellation evidence for Python and subprocess | Python 2.82 million rows/s at 8,192 rows; subprocess IPC 524,288 rows in 33.6 ms | IPC and row compatibility are reported separately; WASM remains unmeasured | exact Python release; bounded control retention; copy-unknown remains explicit | aarch64-apple-darwin local release / boundary micro/macro controls | observed |
| Bounded and drain-epoch execution | finite and unbounded-capability inputs share deterministic package/gate semantics | monotone receipt-gated watermarks and exact late-data outcomes pass | jobs/recovery/replay invariance retained | bounded epoch state and replay retention | deterministic conformance / finite drain epochs | green |
| Evidence-driven pruning | verified statistics may skip work but never produce identity-bearing bytes | verified-package segment planner streams retained/skipped decisions | missing/unsupported evidence conservatively retains every segment | one caller-sized shared-memory reservation | deterministic package/engine conformance / analysis-only DataFusion bridge | green |

## Destination bulk-path matrix

| Destination | Path | Cell | Evidence version | Host class | Target | Observation | Status | Evidence |
|---|---|---|---|---|---:|---:|---|---|
| duckdb | `canonical_segment_scan` | eligible (tlc-v1) | `p3-d14-stock-scan-2026-07-19-v1` | `host-class-649c6f28be3544c8` | ≥1M rows/s; ≥5× scalar appender | 1103.67 MiB/s | observed | [record](../.10x/evidence/2026-07-12-p3-d5-destination-matrix.md) |
| duckdb | `canonical_segment_scan` | schema-ineligible (decimal256-v1) | `p3-d14-stock-scan-2026-07-19-v1` | `host-class-649c6f28be3544c8` | ≥1M rows/s; ≥5× scalar appender | — | ineligible: schema fixture is rejected during bulk-path preflight | [record](../.10x/evidence/2026-07-12-p3-d5-destination-matrix.md) |
| parquet_object_store | `arrow_ipc_to_parquet_none` | eligible (wide-entropy-v1) | `p3-parquet-compression-2026-07-26-v1` | `host-class-649c6f28be3544c8` | ≥60% device-write roofline | 74.33 MiB/s | observed | [record](../.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md) |
| parquet_object_store | `arrow_ipc_to_parquet_none` | schema-ineligible (month-day-nano-interval-v1) | `p3-parquet-compression-2026-07-26-v1` | `host-class-649c6f28be3544c8` | ≥60% device-write roofline | — | ineligible: schema fixture is rejected during bulk-path preflight | [record](../.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md) |
| parquet_object_store | `arrow_ipc_to_parquet_snappy` | eligible (wide-entropy-v1) | `p3-parquet-compression-2026-07-26-v1` | `host-class-649c6f28be3544c8` | ≥60% device-write roofline | 8.39 MiB/s | observed | [record](../.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md) |
| parquet_object_store | `arrow_ipc_to_parquet_snappy` | schema-ineligible (month-day-nano-interval-v1) | `p3-parquet-compression-2026-07-26-v1` | `host-class-649c6f28be3544c8` | ≥60% device-write roofline | — | ineligible: schema fixture is rejected during bulk-path preflight | [record](../.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md) |
| parquet_object_store | `arrow_ipc_to_parquet_lz4_raw` | eligible (wide-entropy-v1) | `p3-parquet-compression-2026-07-26-v1` | `host-class-649c6f28be3544c8` | ≥60% device-write roofline | 3.14 MiB/s | observed | [record](../.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md) |
| parquet_object_store | `arrow_ipc_to_parquet_lz4_raw` | schema-ineligible (month-day-nano-interval-v1) | `p3-parquet-compression-2026-07-26-v1` | `host-class-649c6f28be3544c8` | ≥60% device-write roofline | — | ineligible: schema fixture is rejected during bulk-path preflight | [record](../.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md) |
| parquet_object_store | `arrow_ipc_to_parquet_zstd` | eligible (wide-entropy-v1) | `p3-parquet-compression-2026-07-26-v1` | `host-class-649c6f28be3544c8` | ≥60% device-write roofline | 0.79 MiB/s | observed | [record](../.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md) |
| parquet_object_store | `arrow_ipc_to_parquet_zstd` | schema-ineligible (month-day-nano-interval-v1) | `p3-parquet-compression-2026-07-26-v1` | `host-class-649c6f28be3544c8` | ≥60% device-write roofline | — | ineligible: schema fixture is rejected during bulk-path preflight | [record](../.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md) |
| postgres | `copy_binary` | eligible (tpch-orders-v1) | `p3-d3-2026-07-11-v1` | `host-class-649c6f28be3544c8` | binary COPY; ≥2× CSV COPY | 184.90 MiB/s | observed | [record](../.10x/evidence/2026-07-12-p3-d5-destination-matrix.md) |
| postgres | `copy_binary` | schema-ineligible (time32-microsecond-invalid-v1) | `p3-d3-2026-07-11-v1` | `host-class-649c6f28be3544c8` | binary COPY; ≥2× CSV COPY | — | ineligible: schema fixture is rejected during bulk-path preflight | [record](../.10x/evidence/2026-07-12-p3-d5-destination-matrix.md) |

## Evidence, references, and bias

### Parquet file/glob → package — partial

- Evidence: [.10x/evidence/2026-07-11-p3-parquet-stream-byte-first-segments.md](../.10x/evidence/2026-07-11-p3-parquet-stream-byte-first-segments.md)
- Reference: arrow-rs Parquet decode and favorable same-data Parquet rewrite
- Bias/limit: The logical decode rate omits package persistence; the governed path includes destination work. No cross-host synthesis is used.
- Sources: [.10x/tickets/done/2026-07-11-p3-b2-parquet-codec.md](../.10x/tickets/done/2026-07-11-p3-b2-parquet-codec.md), [.10x/evidence/2026-07-14-p3-c4-fineweb-local-scaling.md](../.10x/evidence/2026-07-14-p3-c4-fineweb-local-scaling.md)
- Residual: The ratio target and row-group architecture are demonstrated; a comparable physical-byte package cell at 1.5 GB/s is not.

### CSV → package — partial

- Evidence: [.10x/evidence/2026-07-19-p3-b4-csv-envelope.md](../.10x/evidence/2026-07-19-p3-b4-csv-envelope.md)
- Reference: arrow-csv 59.1.0 with identical 1,000-record inference
- Bias/limit: Hard-linked warm files minimize device pressure; CDF additionally validates, segments, compresses, hashes, and finalizes.
- Sources: [.10x/evidence/.storage/2026-07-19-p3-b4-csv-reference-bounded-runs.jsonl](../.10x/evidence/.storage/2026-07-19-p3-b4-csv-reference-bounded-runs.jsonl), [.10x/evidence/.storage/2026-07-19-p3-b4-csv-cdf-bounded-runs.jsonl](../.10x/evidence/.storage/2026-07-19-p3-b4-csv-cdf-bounded-runs.jsonl), [.10x/evidence/.storage/2026-07-19-p3-b4-csv-multi-runs.jsonl](../.10x/evidence/.storage/2026-07-19-p3-b4-csv-multi-runs.jsonl)
- Residual: The roofline ratio is green; the 400 MB/s absolute ambition is not.

### NDJSON/JSON decode — green

- Evidence: [.10x/tickets/done/2026-07-11-p3-b5-json-codecs.md](../.10x/tickets/done/2026-07-11-p3-b5-json-codecs.md)
- Reference: conservative full-DOM compatibility shape and raw Arrow inference
- Bias/limit: Codec controls exclude complete package fixed costs; the tiny product macro remains separately recorded and is not used as codec evidence.
- Sources: [.10x/tickets/done/2026-07-11-p3-b5-json-codecs.md](../.10x/tickets/done/2026-07-11-p3-b5-json-codecs.md), [.10x/evidence/2026-07-12-p3-b5-streaming-json-document-driver.md](../.10x/evidence/2026-07-12-p3-b5-streaming-json-document-driver.md)

### Contract validation — green

- Evidence: [.10x/evidence/2026-07-19-p3-v3-validation-envelope.md](../.10x/evidence/2026-07-19-p3-v3-validation-envelope.md)
- Reference: same-host memcpy roofline
- Bias/limit: Only data-inspecting 64k hot-kernel cells gate; boundary and evidence-materialization cells remain trend-only.
- Sources: [.10x/evidence/.storage/2026-07-19-p3-v3-validation-envelope-ec2.json](../.10x/evidence/.storage/2026-07-19-p3-v3-validation-envelope-ec2.json), [.10x/evidence/.storage/2026-07-19-p3-v3-validation-envelope-ec2.time](../.10x/evidence/.storage/2026-07-19-p3-v3-validation-envelope-ec2.time)

### Package build/hash — green

- Evidence: [.10x/evidence/2026-07-19-p3-e4-package-io-envelope.md](../.10x/evidence/2026-07-19-p3-e4-package-io-envelope.md)
- Reference: fio direct sequential write and exact hash-free durability control
- Bias/limit: Local filesystem durability cell; object-store multipart throughput is governed by transport/destination evidence.
- Sources: [.10x/evidence/.storage/2026-07-19-p3-e4-ipc-package-writer-sustained.log](../.10x/evidence/.storage/2026-07-19-p3-e4-ipc-package-writer-sustained.log), [.10x/evidence/.storage/2026-07-19-p3-e4-fio-direct.json](../.10x/evidence/.storage/2026-07-19-p3-e4-fio-direct.json)

### Package → DuckDB — green

- Evidence: [.10x/evidence/2026-07-12-p3-d5-destination-matrix.md](../.10x/evidence/2026-07-12-p3-d5-destination-matrix.md)
- Reference: removed scalar/appender controls and same-package scanner controls
- Bias/limit: Local DuckDB materialization; wide 2,052-column schemas have a separately recorded native sink floor.
- Sources: [.10x/evidence/.storage/2026-07-19-p3-d14-stock-default-full-year-three-sample.json](../.10x/evidence/.storage/2026-07-19-p3-d14-stock-default-full-year-three-sample.json), [.10x/evidence/.storage/p3-destination-matrix-ec2-current.json](../.10x/evidence/.storage/p3-destination-matrix-ec2-current.json)

### Package → PostgreSQL — green

- Evidence: [.10x/evidence/2026-07-12-p3-d5-destination-matrix.md](../.10x/evidence/2026-07-12-p3-d5-destination-matrix.md)
- Reference: same-schema CSV COPY compatibility control
- Bias/limit: Loopback/local server evidence; remote network latency and server tuning are not generalized.
- Sources: [.10x/evidence/.storage/2026-07-19-p3-d16-postgres-direct-copy-fresh.json](../.10x/evidence/.storage/2026-07-19-p3-d16-postgres-direct-copy-fresh.json), [.10x/evidence/.storage/p3-destination-matrix-ec2-current.json](../.10x/evidence/.storage/p3-destination-matrix-ec2-current.json)

### Package → Parquet — green

- Evidence: [.10x/evidence/2026-07-15-p3-d8-parquet-staged-ingress.md](../.10x/evidence/2026-07-15-p3-d8-parquet-staged-ingress.md)
- Reference: equal-byte durable raw writes
- Bias/limit: Full-year row rate and isolated device ratio are distinct labelled cells, not combined into one synthetic ratio.
- Sources: [.10x/evidence/.storage/2026-07-18-p3-d15-ec2-parquet-full-year-current.json](../.10x/evidence/.storage/2026-07-18-p3-d15-ec2-parquet-full-year-current.json), [.10x/evidence/.storage/p3-destination-matrix-ec2-current.json](../.10x/evidence/.storage/p3-destination-matrix-ec2-current.json)

### Full-year TLC HTTPS → DuckDB — accepted residual

- Evidence: [.10x/tickets/done/2026-07-11-p3-g4-tlc-remote-io-envelope.md](../.10x/tickets/done/2026-07-11-p3-g4-tlc-remote-io-envelope.md)
- Reference: same-host local CDF 10.256 s and parallel curl 2.26 s
- Bias/limit: Public CDN timing is uncontrolled. The faster complete-spool strategy is explicit, not a speculative default heuristic.
- Sources: [.10x/evidence/.storage/2026-07-19-p3-g4-hf-complete-final-clean.json](../.10x/evidence/.storage/2026-07-19-p3-g4-hf-complete-final-clean.json), [.10x/evidence/.storage/2026-07-19-p3-g4-hf-stock-full-year-smoke.json](../.10x/evidence/.storage/2026-07-19-p3-g4-hf-stock-full-year-smoke.json), [.10x/evidence/.storage/2026-07-19-p3-g4-local-auto-final-clean.json](../.10x/evidence/.storage/2026-07-19-p3-g4-local-auto-final-clean.json)
- Residual: The original 1.5× download-plus-native composite and separate live S3/GCS/Azure cells were not met.
- Acceptance authority: [.10x/tickets/done/2026-07-11-p3-g4-tlc-remote-io-envelope.md](../.10x/tickets/done/2026-07-11-p3-g4-tlc-remote-io-envelope.md)

### 1 TiB synthetic glob → Parquet — partial

- Evidence: [.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md](../.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md)
- Reference: identical 2026-07-25 governed baseline; no synthetic unique-byte device roofline
- Bias/limit: The generator uses repeated content and hard links, and Zstd compressed the destination to 46.4 MB; this proves logical work, CPU scheduling, lifecycle, and memory, not unique-source-byte or cold-device throughput.
- Sources: [.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-summary.json](../.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-summary.json), [.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-package-verify.json](../.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-package-verify.json), [.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-process-time.txt](../.10x/evidence/.storage/2026-07-26-parquet-parallel-1t-process-time.txt)
- Residual: Default-budget completion and near physical-core saturation are green; unique-source-byte cold-device saturation and the SMT scaling knee were not demonstrated by this repeated-content hard-link fixture.

### Correctness/evidence overhead — not demonstrated

- Evidence: [.10x/evidence/2026-07-19-p3-e4-package-io-envelope.md](../.10x/evidence/2026-07-19-p3-e4-package-io-envelope.md)
- Reference: exact hash-free durability control and progress-disabled product control
- Bias/limit: Adding unlike component percentages would create a false whole-product scalar. The global 10% claim is therefore withheld.
- Sources: [.10x/evidence/.storage/2026-07-19-p3-e4-ipc-package-writer-sustained.log](../.10x/evidence/.storage/2026-07-19-p3-e4-ipc-package-writer-sustained.log), [.10x/evidence/.storage/2026-07-25-cx4-hosted-cli-overhead.json](../.10x/evidence/.storage/2026-07-25-cx4-hosted-cli-overhead.json)
- Residual: A whole-product semantically equivalent raw comparator remains unavailable; CDF makes no ≤10% aggregate claim.

### Constant process memory — green

- Evidence: [.10x/evidence/2026-07-25-p3-f3-constant-memory-matrix.md](../.10x/evidence/2026-07-25-p3-f3-constant-memory-matrix.md)
- Reference: input-size geometric curve and explicit impossible-budget control
- Bias/limit: Repeated-content fixtures prove memory/lifecycle slope, not unique-byte device capacity.
- Sources: [.10x/evidence/.storage/2026-07-25-p3-f3-ec2-5g-a-summary.json](../.10x/evidence/.storage/2026-07-25-p3-f3-ec2-5g-a-summary.json), [.10x/evidence/.storage/2026-07-25-p3-f3-ec2-20g-summary.json](../.10x/evidence/.storage/2026-07-25-p3-f3-ec2-20g-summary.json), [.10x/evidence/.storage/2026-07-25-p3-f3-ec2-100g-summary.json](../.10x/evidence/.storage/2026-07-25-p3-f3-ec2-100g-summary.json), [.10x/evidence/.storage/2026-07-25-p3-f3-ec2-too-small.json](../.10x/evidence/.storage/2026-07-25-p3-f3-ec2-too-small.json), [.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md](../.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md)

### Jobs invariance — green

- Evidence: [.10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md](../.10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md)
- Reference: jobs=1 semantic artifacts
- Bias/limit: Scale timing is host-specific; identity equivalence is the portable law.
- Sources: [.10x/evidence/2026-07-14-p3-c4-fineweb-local-scaling.md](../.10x/evidence/2026-07-14-p3-c4-fineweb-local-scaling.md), [.10x/tickets/done/2026-07-11-p3-c5-isolated-worker-equivalence.md](../.10x/tickets/done/2026-07-11-p3-c5-isolated-worker-equivalence.md)

### Core native format engines — green

- Evidence: [.10x/tickets/done/2026-07-10-p3-ws-b-format-decode-engines.md](../.10x/tickets/done/2026-07-10-p3-ws-b-format-decode-engines.md)
- Reference: deleted monolithic/source-owned parser paths
- Bias/limit: Enterprise long-tail codecs are parked and are not claimed by P3.
- Sources: [.10x/tickets/done/2026-07-11-p3-b2-parquet-codec.md](../.10x/tickets/done/2026-07-11-p3-b2-parquet-codec.md), [.10x/tickets/done/2026-07-11-p3-b5-json-codecs.md](../.10x/tickets/done/2026-07-11-p3-b5-json-codecs.md), [.10x/evidence/2026-07-12-p3-b1-streaming-transform-product-composition.md](../.10x/evidence/2026-07-12-p3-b1-streaming-transform-product-composition.md)

### Implemented foreign boundaries — observed

- Evidence: [.10x/evidence/2026-07-25-p3-h5-interop-envelope.md](../.10x/evidence/2026-07-25-p3-h5-interop-envelope.md)
- Reference: mode-specific transfer reports, not a fabricated zero-copy baseline
- Bias/limit: These are boundary results, not EC2 whole-product promotion claims.
- Sources: [.10x/evidence/2026-07-25-p3-h5-interop-envelope.md](../.10x/evidence/2026-07-25-p3-h5-interop-envelope.md), [.10x/tickets/done/2026-07-10-p3-ws-h-interop-boundaries.md](../.10x/tickets/done/2026-07-10-p3-ws-h-interop-boundaries.md)

### Bounded and drain-epoch execution — green

- Evidence: [.10x/evidence/2026-07-19-p3-a9-watermark-late-data-conformance.md](../.10x/evidence/2026-07-19-p3-a9-watermark-late-data-conformance.md)
- Reference: bounded execution package/receipt/checkpoint law
- Bias/limit: Resident supervision and distributed scheduling remain parked.
- Sources: [.10x/evidence/2026-07-19-p3-a9-watermark-late-data-conformance.md](../.10x/evidence/2026-07-19-p3-a9-watermark-late-data-conformance.md), [.10x/tickets/done/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md](../.10x/tickets/done/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md)

### Evidence-driven pruning — green

- Evidence: [.10x/tickets/done/2026-07-12-p3-j1-evidence-statistics-pruning.md](../.10x/tickets/done/2026-07-12-p3-j1-evidence-statistics-pruning.md)
- Reference: unpruned verified package scan
- Bias/limit: Broader DataFusion expression/catalog/scheduling adoption remains parked.
- Sources: [.10x/tickets/done/2026-07-12-p3-j1-evidence-statistics-pruning.md](../.10x/tickets/done/2026-07-12-p3-j1-evidence-statistics-pruning.md), [.10x/evidence/.storage/p3-j0-statistics-envelope-macos.json](../.10x/evidence/.storage/p3-j0-statistics-envelope-macos.json)
