Status: recorded
Created: 2026-07-27
Updated: 2026-07-27

# D3 CLI conformance evidence

## Observation

Commit `dc4b94f0` centralizes the shared report headings `Summary`, `Proof`, `Effects`,
`Recovery`, and `Attention`; preserves outcome-first rendering and JSON isolation; and adds
current-state conformance across every named report family. The existing nonblocking progress
path remained above its recorded floor, and a 10,000-row static report completed full document
construction and rendering in a local median of 4.5571 milliseconds.

Fresh product smokes exercised both a local 2 GiB Parquet source and the public
`github.userdata` HTTPS source. The public run committed 5,000 rows across five partitions and
rendered its package, verified DuckDB receipt, committed checkpoint, and exact inspect command.

## Procedure

### Static and behavioral gates

```text
cargo fmt --all
DUCKDB_LIB_DIR="$PWD/target/debug/deps" \
  DYLD_LIBRARY_PATH="$PWD/target/debug/deps" \
  cargo test -p cdf-cli-core --all-features --locked
DUCKDB_LIB_DIR="$PWD/target/debug/deps" \
  DYLD_LIBRARY_PATH="$PWD/target/debug/deps" \
  cargo test -p cdf-cli --lib --locked
DUCKDB_LIB_DIR="$PWD/target/debug/deps" \
  DYLD_LIBRARY_PATH="$PWD/target/debug/deps" \
  cargo clippy -p cdf-cli-core -p cdf-cli -p cdf-cli-benchmarks \
  --all-features --all-targets --locked -- -D warnings
cargo check -p cdf-cli-benchmarks --benches --locked
git diff --check
```

Observed:

- 55/55 `cdf-cli-core` tests passed.
- 299/299 `cdf-cli` library tests passed.
- The new family matrix rendered inspect, plan, execute, mutate, recover, list, no-op, warning,
  and failure documents at 40, 80, and 160 columns in TTY and headless modes with both ASCII and
  Unicode policy, no ANSI under no-color, no lost facts, and no width overflow.
- Exact rich and headless primitive snapshots, JSON-isolation tests, progressive-disclosure
  tests, the renderer migration gate, strict Clippy, benchmark compilation, formatting, and diff
  checks passed.
- A static search found no direct construction of the five shared headings and no remaining
  `Writes` section heading in CLI Rust source.

### Performance

```text
DUCKDB_LIB_DIR="$PWD/target/release/deps:$PWD/target/debug/deps" \
  DYLD_LIBRARY_PATH="$PWD/target/release/deps:$PWD/target/debug/deps" \
  cargo bench -p cdf-cli-benchmarks --bench cli_renderer --locked
```

Local cell: commit `dc4b94f0`, Criterion release profile, Apple M5 Pro, 18 logical CPUs, 24 GiB
RAM, macOS Darwin 25.5.0. Criterion used 100 measured samples after its default warm-up.

| Workload | Median | Throughput | Criterion comparison |
|---|---:|---:|---|
| one-million-event iteration floor | 465.06 us | 2.1503 Gevents/s | within noise |
| one million buffered events | 50.101 ms | 19.960 Mevents/s | 3.5% faster than the recorded floor |
| 10,000 high-partition events | 516.48 us | 19.362 Mevents/s | within noise |
| 10,000-row prebuilt headless report | 3.3964 ms | 2.9443 Mrows/s | first recorded cell |
| 10,000-row build and render | 4.5571 ms | 2.1944 Mrows/s | first recorded cell |

The full-lifecycle cell constructs and formats all 10,000 rows inside each measured iteration.
An untimed preflight asserts exactly 10,000 rendered resource rows plus the first and last row
identifiers. The buffered-event median varied from 45.112 milliseconds in the immediately prior
same-commit run to 50.101 milliseconds after recompilation; no progress code changed between the
runs. The retained result is still faster than the 51.920-millisecond recorded floor, so this
local movement is disclosed as same-host variance rather than a performance claim.

The governing hosted enabled-versus-disabled result remains
`.10x/evidence/2026-07-25-cli-hosted-conformance.md`: `-0.4407%` median delta on the P3 reference
host against the maximum `+1%` overhead criterion. D3 did not change the progress hot path; its
production changes are named constructors for existing panel titles and the `Writes` to
`Effects` vocabulary replacement.

### Product smoke

The current debug binary was built from `dc4b94f0`.

For the local smoke, a fresh temporary project copied the existing C4 project configuration and
hard-linked one 2 GiB FineWeb Parquet file. The 80-column ASCII/no-color plan discovered one
partition and nine fields, rendered a bounded append plan, and ended with
`Next: cdf run fineweb.documents`.

For the public HTTPS smoke, the existing `/Users/alexanderbut/code_projects/tmp` project planned
and ran `github.userdata` with headless progress. The run recorded:

- five HTTPS partitions and 15 source requests;
- 5,000 rows and five canonical segments;
- package `pkg-github-userdata-50904-1785207618170768000`;
- verified DuckDB receipt
  `duckdb:userdata:sha256:6a3b1fabaeb36c793b856665fa78c0d556ea34ae4bbda853fbdf80a732026533`;
- committed checkpoint `checkpoint-github-userdata-50904-1785207618170768000`;
- copyable `cdf inspect run run-ee76cbfed32d2624422a925eaaa86348`.

An initial local smoke against the old shared project failed before planning because its
discovery manifest used artifact version 1 while the current binary requires version 2. The
separate failure recording preserves the exact typed data error and remediation. Repeating the
smoke in a fresh project removed that environmental mismatch without changing code.

## Artifacts

- `.10x/evidence/.storage/2026-07-27-d3-local-plan.txt`
  (`sha256:245518eb7232c4658f29efc335ee39a099246264a909e4669db7606c9fd1aa7a`)
- `.10x/evidence/.storage/2026-07-27-d3-public-https-plan.txt`
  (`sha256:6a40a2894b06ac17f5fbd85544f643b9a07a9c54f73affc6bb7a745a9b94cf5b`)
- `.10x/evidence/.storage/2026-07-27-d3-public-https-run.txt`
  (`sha256:cc4681449e3daf80d25f86a80088368460498c48afefaa449a264b440b938b5e`)
- `.10x/evidence/.storage/2026-07-27-d3-stale-local-project-failure.txt`
  (`sha256:1fc6ff5ca30edd0d9828712f34abd27b2dda174a3cedd558d0a38043b353f723`)

## What this supports

- D3's shared hierarchy and vocabulary criterion.
- D3's terminal-policy, width, family-coverage, and progressive-disclosure criterion.
- D3's million-event and representative large-static-report performance criterion.
- D3's real local and public-HTTPS smoke criterion, including package/receipt/checkpoint facts.

## Limits

The family matrix is a deterministic renderer conformance test over representative documents,
not an end-to-end invocation of every command. Existing command-level rich/headless tests and the
typed-report migration gate cover production composition. The local Criterion run diagnoses
renderer regressions but does not replace the hosted P3 overhead cell. The local plan used one
hard-linked 2 GiB partition and did not execute destination mutation. The public run used the
existing five-partition fixture and network conditions observed on 2026-07-27.
`graphify update .` could not run because the `graphify` executable is unavailable in this
environment; the parent already records this program-wide tooling limit.
