Status: recorded
Created: 2026-07-25
Updated: 2026-07-25

# CLI experience rewrite evidence

## Observation

CDF now presents one coherent terminal language across command families: borderless summaries,
outcome-first status, explicit `Next:` actions, structured `error[CODE]` blocks, progressive
normal/verbose disclosure, and a nonblocking live progress subscriber. The machine JSON envelope
remains isolated from human rendering.

## Procedure

### Static and behavioral gates

```text
cargo fmt --all
CARGO_BUILD_JOBS=12 cargo clippy -p cdf-cli-core -p cdf-cli \
  -p cdf-cli-benchmarks --all-targets --all-features --locked -j 12 -- -D warnings
CARGO_BUILD_JOBS=12 cargo nextest run -p cdf-cli-core --all-features \
  -p cdf-cli --lib -j 12 --no-fail-fast
```

Observed on 2026-07-25:

- strict Clippy passed;
- 320/320 CLI and CLI-core tests passed in 42.504 seconds;
- committed generated help, completion, and man-page artifacts matched their generators;
- the automated terminal matrix covered 40/80/160 columns, TTY/headless, ASCII/Unicode,
  no-color, JSON isolation, progressive disclosure, and copyable error/action text.

### Progress cost and boundedness

```text
CARGO_BUILD_JOBS=12 cargo bench -p cdf-cli-benchmarks \
  --bench cli_renderer --locked -j 12
```

Observed medians:

- one-million-event iteration floor: 476.33 microseconds;
- one million governed buffered events: 51.920 milliseconds, 19.260 million events/second;
- 10,000 high-partition events: 515.45 microseconds, 19.400 million events/second;
- Criterion reported no performance regression for either governed benchmark.

The slow-terminal law submitted 10,000 events to a capacity-one live sink backed by a
25-millisecond writer. Submission completed below 250 milliseconds, ordinary overflow was
dropped/coalesced, and the terminal event survived through the dedicated bounded terminal slot.
Separate tests prove bounded run-sequence and live headless coalescing state across 1,000 runs.

### Real-project smoke

The actual debug binary was built and exercised in `/Users/alexanderbut/code_projects/tmp`,
using the existing public `github.userdata` HTTPS Parquet resource:

```text
CARGO_BUILD_JOBS=12 cargo build -p cdf-cli --locked -j 12
COLUMNS=80 target/debug/cdf --progress never --color never --unicode never \
  plan github.userdata
COLUMNS=80 /usr/bin/time -p target/debug/cdf --progress always \
  --color never --unicode never run github.userdata
```

The stable plan rendered a 15-line normal decision summary and `Next: cdf run github.userdata`;
verbose retained discovery, contract, migration, scheduling, and destination proof. JSON stayed a
pure machine envelope.

The run completed successfully:

- five remote partitions;
- 5,000 rows;
- five canonical segments;
- verified DuckDB receipt and committed checkpoint;
- 875 milliseconds recorded execution time and 1.85 seconds process wall time;
- eight phase-bounded headless progress lines followed by the final outcome, summary, proof, and
  copyable inspect command.

An unknown `plna` command rendered `error[CDF-CLI-USAGE]`, the causal parser message, a concrete
help summary, and a copyable `cdf help <command>` correction without ANSI escapes at 40 columns.

## What this supports

- CX2's compact renderer, progressive disclosure, error grammar, Unicode-width, redaction, and
  JSON-isolation acceptance criteria.
- CX3's rate-limited/coalesced live subscriber, bounded memory, nonblocking slow-terminal law,
  terminal-event preservation, and real run-path parity.
- CX4's million-event and high-partition benchmark criteria.

## Limits

This local evidence does not by itself prove CX4's enabled-versus-disabled overhead threshold on
the P3 hosted reference workload, nor does it replace the canonical hosted terminal recordings.
CX4 remains active for those two observations and the final adversarial conformance review.
