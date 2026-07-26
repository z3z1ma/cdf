Status: recorded
Created: 2026-07-25
Updated: 2026-07-25
Relates-To: .10x/tickets/done/2026-07-11-p3-z2-scale-demo-adversarial-review.md

# P3 Z2 scale demonstrations and adversarial review

## Observation

P3 closes with two reproducible scale demonstrations and an adversarial matrix over the
implemented product. The observations are deliberately not relabelled: the full-year remote TLC
run is an exact, bounded success but misses the original composite roofline; the 1.0086 TiB run is
a complete constant-memory product proof but its hard-linked input is not a unique-byte device
saturation proof.

### Full-year TLC over HTTPS to DuckDB

The terminal remote observation used clean revision
`bf46c16469325e1e93f5d8ce642e4ab14dd4eb9c` on
`host-class-649c6f28be3544c8`, with a warm I/O label and
`gnu-time-v-child-process` measurement. The source authority was:

```toml
[source.tlc]
kind = "files"
root = "https://huggingface.co/datasets/Lucky239/taxi-data/resolve/main"
egress_allowlist = ["huggingface.co", "us.aws.cdn.hf.co"]

[resource.yellow]
glob = "yellow_tripdata_2024-{01..12}.parquet"
spool_mode = "complete"
```

The exact product workload was `cdf run tlc.yellow --json --progress never --color never
--unicode never` through the L6 `measure-cdf` runner. The explicit complete-spool strategy is an
operator knob; overlap remains the unchanged default. The source objects are the twelve named
2024 monthly Parquet objects at that root. The command completed all `41,169,720` rows in
`15.783324269s` at `2,608,431` rows/s. Peak process RSS was `3,912,744,960` bytes; peak cgroup
memory was `6,217,789,440` bytes; pressure, OOM, and spill counters were zero.

The retained phase profile is:

| Phase | Aggregate duration | Bytes |
|---|---:|---:|
| source read | 5.604s | 693,001,713 |
| decode | 11.668s | 5,849,047,168 |
| validation/normalization | 0.243s | 11,868,646,460 |
| segment encode | 11.154s | 1,678,253,294 |
| persist/hash | 1.773s | 1,678,253,294 |
| package finalize | 0.007s | 1,678,610,437 |
| destination ingress | 0.082s | 1,678,253,294 |
| package execution | 7.006s | 1,678,253,294 |
| destination write/receipt | 6.579s | 1,678,253,294 |
| checkpoint gate | 0.001s | 0 |

Those durations overlap; they MUST NOT be summed as wall time. The raw observation is
`.10x/evidence/.storage/2026-07-19-p3-g4-hf-complete-final-clean.json`. Its comparison authority is
`.10x/evidence/.storage/2026-07-19-p3-g4-hf-stock-full-year-smoke.json`: unchanged-default remote
execution was `19.580053641s`, so the explicit complete-spool mode improved this finite multi-file
workload by 19.4%. The final local stock-scanner median was `10.255642670s`, retained at
`.10x/evidence/.storage/2026-07-19-p3-d14-stock-default-full-year-three-sample.json`.

The before picture is not reconstructed: the first governed remote path took `92.58s`, while its
same-object raw transfer took `19.116s`, and the later twelve-file path timed out after serialized
progress. The complete investigation, raw parallel-curl controls, exact source configuration,
rejected regressions, package production, receipt-gated completion, and the user-accepted
composite-target residual are in
`.10x/tickets/done/2026-07-11-p3-g4-tlc-remote-io-envelope.md`.

Artifact replay was exercised on the same 41,169,720-row/215-segment full-year TLC package:

```text
tools/p3-ec2-benchmark-host.sh measure-cdf \
  target/cdf-benchmarks/g4-local-package-replay-duckdb-measured.json \
  nyc_tlc_yellow_2024 tlc_local_package_replay_duckdb -- \
  replay package \
  /home/ec2-user/cdf-bench/repo/target/cdf-benchmarks/\
g4-scheduler-default-20260718085558/local-workspace/.cdf/packages/\
pkg-tlc-yellow-56794-1784364958724043936 \
  --to duckdb://.cdf/replay-package.duckdb \
  --json --progress never --color never --unicode never
```

That historical appender-era replay completed in `45.971040657s`; it proves verified artifact
replay, not current scanner throughput. Raw evidence:
`.10x/evidence/.storage/2026-07-18-p3-g4-ec2-local-package-replay-duckdb-measured.json`.
The live run reports are emitted only after package finalization, verified receipt, and checkpoint
commit. The final machine report retained exact completion and phase evidence but not the
ephemeral package/receipt/checkpoint identifiers; the replay artifact above is the durable
full-year package identity pointer. No identifier is invented here.

### 1.0086 TiB Parquet to governed Parquet

The terminal scale run used the generated `constant-memory-parquet-v1` fixture: 1,024 files,
5,308,416 rows and 1,082,939,544 logical bytes per file, for 5,435,817,984 rows and
1,108,930,093,056 logical bytes in total. It ran on a `c7i.4xlarge` (16 logical CPUs, 32 GiB RAM,
tuned 2 TiB gp3 root volume) in a `MemoryMax=5G`, `MemorySwapMax=0` systemd scope:

```text
systemd-run --user --wait --collect \
  --property=MemoryMax=5G \
  --property=MemorySwapMax=0 \
  bash -lc \
  'cd /home/ec2-user/cdf-bench/repo &&
   tools/run-constant-memory-stress.sh
     target/cdf-benchmarks/f4-1t-default 1024 1073741824 default'
```

The `default` argument deliberately omitted a product memory override. The complete governed path
finished in 2,694.863 seconds at 411.5 MB/s logical throughput and 2.017 million rows/s. Process
RSS peaked at 2,035,298,304 bytes under the default 4 GiB product policy. Managed memory peaked at
2,490,367,380 bytes of a 3,650,722,202-byte pool and returned to zero. The cgroup reported no OOM
or OOM-kill event. No algorithmic spill was needed; forced-spill equivalence and exhausted-spill
cleanup are independently proven by F3.

The terminal identities are:

- package `sha256:078686f3b6d38b4416c695c6fd585b3d35bb8458da26b7cba6785f9ebf3f21e0`;
- receipt `parquet:rows:sha256:078686f3b6d38b4416c695c6fd585b3d35bb8458da26b7cba6785f9ebf3f21e0`;
- checkpoint `checkpoint-stress-rows-66921-1785049966748480525`;
- 5,120 canonical segments and 5,135 independently verified identity files.

The compact machine summary is
`.10x/evidence/.storage/2026-07-25-p3-f4-ec2-1t-summary.json`; the compressed full run, package
verification, process/cgroup samples, generator record, and cold 1,024-file discovery observation
are linked from `.10x/evidence/2026-07-25-p3-f4-one-tib-closeout.md`. The generated data and
destination were deleted with the benchmark host tranche; no giant dataset is committed and the
EC2 instance is terminated.

The run proves constant process memory, governed lifecycle, package/receipt/checkpoint integrity,
and logical-work scaling. The hard-link generator uses 53,012,194 unique physical source bytes and
54,284,486,656 represented physical bytes; it therefore does not prove one TiB of unique-byte cold
device saturation. The accepted device/concurrency authority is the C4/D8 FineWeb curve and
isolated writer roofline, not a relabelled claim about this memory run.

## Adversarial review

The review traced each category through the generic runtime topology—source task authority,
bounded decode publication, validation/normalization, canonical segment persistence, capability-
selected destination ingress, receipt verification, and checkpoint gate—and then attempted to
falsify performance or correctness with the strongest retained observation for that category.

| Category | Falsification evidence | Result and residual |
|---|---|---|
| Tiny-file/high-cardinality | `.10x/tickets/done/2026-07-11-p3-e4-package-io-envelope.md`, `.10x/tickets/done/2026-07-11-p3-g1-streaming-transport-byte-sources.md`, F4 1,024-file/5,120-segment observation | Bounded streaming manifest/listing paths hold. One-million-entry controls exist. No claim for unbounded-horizon constant metadata. |
| Wide/nested | `.10x/tickets/done/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md`, `.10x/tickets/done/2026-07-11-p3-b5-json-codecs.md` | 2,052-column DuckDB survival and statistics-pruned null columns are explicit; native DuckDB remains the wide sink floor. Nested JSON remains on the codec path. |
| High compression | `.10x/evidence/2026-07-25-p3-f3-constant-memory-matrix.md`, `.10x/tickets/done/2026-07-10-p3-ws-b-format-decode-engines.md` | Streaming gzip/object-store cancellation and backpressure hold. No full-payload decompression buffer. |
| Malformed/quarantine-heavy | `.10x/evidence/2026-07-11-p3-v2-constant-memory-quarantine-closeout.md`, `.10x/tickets/done/2026-07-13-p0-sa5-fixed-schema-admission-conformance.md` | Fixed-schema admission, bounded quarantine, and terminal quarantine authority are total. |
| All-unique dedup | `.10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md`, F3 `append_exact_row_dedup_compiles_and_drops_only_complete_duplicates` | Spillable exact-row authority preserves unique rows and has forced-spill equivalence. |
| Skew | `.10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md` | Generated skew/failure/filter/limit seeds are invariant at jobs 1/2/4/8; scheduling changes no package identity. |
| Slow destination | `.10x/tickets/done/2026-07-14-p3-d7-persistent-staged-ingress-stream.md`, C4/F3 backpressure evidence | Generic bounded staged ingress prevents unaccounted accumulation; the adapter declares the pressure/capability. |
| Remote latency | `.10x/tickets/done/2026-07-11-p3-g2-range-readahead-spool-controller.md`, G4 evidence above | Finite full scans have explicit complete spooling; overlap remains default. Provider/live breadth is not generalized from one mirror. |
| Foreign boundary | `.10x/evidence/2026-07-25-p3-h5-interop-envelope.md` | Python/subprocess modes expose measured copy/control/lane truth. WASM remains explicitly unimplemented, not implied. |
| Mixed schema | `.10x/tickets/done/2026-07-13-p0-sa5-fixed-schema-admission-conformance.md`, `.10x/tickets/done/2026-07-19-preview-terminal-quarantine-fixture-authority.md` | Runtime observation selects from the fixed admission program; it cannot mutate the schema epoch. |

Architecture inspection found no source or destination identity branch in generic orchestration.
Native file formats enter through the codec/byte-source registry, partition work through canonical
task authority, and destinations through declared ingress capabilities. The sole DuckDB product
path is destination-local; superseded appender, nanoarrow, custom-runtime, and compatibility
paths are deleted.

## Findings

- No critical or high correctness, performance, or architecture finding remains unresolved in
  the implemented P3 scope.
- Accepted residual: the `15.783s` remote TLC cell does not meet the original raw-plus-native
  composite target. G4 records the user's explicit acceptance after the final spool
  falsification; no slower default shipped.
- Visible non-green result: aggregate whole-product correctness/evidence overhead is not
  demonstrated because no semantically equivalent raw comparator exists. Component overhead
  cells remain measured; Z1 refuses to invent an aggregate.
- Visible partial result: the one-TiB run proves constant memory and governed logical scale, not
  unique-byte device saturation or portable linear core scaling.
- Visible partial result: CSV reaches the retained relative arrow-csv ratio but not the original
  absolute 400 MB/s ambition on the recorded host.
- No-action rationale: distributed execution, resident unbounded supervision, and WASM execution
  are outside P3's explicit scope. The canonical task, epoch, and foreign-stream seams are retained
  without claiming those products exist.

These residuals are preserved in the generated `docs/performance-envelope.md`; none is hidden by a
baseline reset or a terminal status.

## Procedure

1. Read the terminal Z1 envelope and every raw source named by its reconciliation fixture.
2. Reconciled G4, D14, F3, F4, C4, H5, and fixed-schema-admission evidence against the exact Z2
   acceptance categories.
3. Traced the execution topology and extension seams through the terminal workstream tickets
   rather than treating isolated tests as global proof.
4. Verified that every raw path cited above exists in the repository and that no giant generated
   data is committed.
5. Verified that the benchmark host lifecycle closed in
   `.10x/tickets/done/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md`; no EC2 host is retained
   for this closure.

## What it supports or challenges

This supports terminal closure of the implemented P3 architecture and its two scale laws. It
challenges the original all-green wording: four cells remain partial, accepted-residual, or not
demonstrated, and the final envelope says so. It also challenges any claim that a successful
logical one-TiB run proves cold one-TiB device throughput.

## Limits

This is a reconciliation and adversarial review over retained evidence, not another expensive
benchmark tranche. Host-specific measurements do not imply all hardware classes. Public endpoint
behavior may change. The exact remote TLC machine report did not retain ephemeral package,
receipt, and checkpoint identifiers; the separate full-year replay artifact and product success
gate are the durable authority for that lifecycle. The review does not claim distributed,
resident-streaming, WASM, or exabyte-scale execution.
