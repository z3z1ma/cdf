Status: active
Created: 2026-08-03
Updated: 2026-08-03

# ClickHouse merge modes

## Context

CDF's original ClickHouse destination contract excluded merge because eventual
`ReplacingMergeTree` compaction cannot provide immediate physical uniqueness. The user explicitly
prioritized ClickHouse-native throughput and accepted that visibility tradeoff, while requiring an
opt-in path for guaranteed immediate uniqueness.

## Decision

ClickHouse merge defaults to `replacing_merge_tree`: direct acknowledged ArrowStream insertion into
an existing capability-proven ReplacingMergeTree layout, with logical winner verification through
`FINAL` and explicit eventual-physical-uniqueness metadata.

An environment destination policy may select `atomic_copy_on_write`. That mode builds the complete
merged target with server-side operations and publishes it using `EXCHANGE TABLES`. It provides
immediate atomic uniqueness at full-target rewrite cost.

CDF never changes an existing target engine to enable either mode.

## Alternatives considered

### Atomic copy-on-write only

This preserves immediate uniqueness and the simplest verification model, but forces full-target
read/write amplification for every merge. It rejects ClickHouse's principal throughput advantage
for update-heavy analytical ingestion and was rejected as the default.

### ReplacingMergeTree only

This maximizes ingestion throughput but gives operators no immediate-uniqueness option. It was
rejected because governed consumers may require one physically visible row per key immediately
after commit.

### Synchronous mutations or OPTIMIZE FINAL

Delete/update mutations and forced final compaction combine high latency with weaker crash and
publication boundaries. They were rejected in favor of either honest native eventual semantics or
one atomic metadata exchange.

## Consequences

The destination exposes two deliberately different merge visibility contracts. Planning,
documentation, and receipts must name the selected mode. The default may expose duplicate physical
versions to ordinary reads until background compaction; consumers requiring current winners must
use `FINAL` or select atomic copy-on-write. Atomic mode is substantially more expensive but exposes
the complete old or complete new target, never an intermediate merge.

Supporting a versioned ReplacingMergeTree later requires new monotonic-version authority; it must
not be inferred from package hashes, wall-clock timing, or an operator's arbitrary version column.
