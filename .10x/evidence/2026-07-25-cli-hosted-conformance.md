Status: recorded
Created: 2026-07-25
Updated: 2026-07-25

# Hosted CLI conformance and overhead evidence

## Observation

The coordinated CLI experience passes its hosted end-to-end overhead gate and its canonical
normal, verbose, redirected, JSON, replay, and failure recording matrix on the tuned P3 benchmark
host. Enabling governed progress did not produce a measurable slowdown.

The same run also proved the generic oversized-singleton staging repair: the 2.147 GB FineWeb
source completed with default memory, segmentation, destination, and concurrency settings after
the prior revision had rejected a legal 276,210,169-byte retained segment against DuckDB's
268,435,456-byte concurrent ownership window.

## Procedure

Host authority:

- EC2 `c7i.4xlarge`, host class `host-class-73b34264145bfec4`;
- gp3 root volume configured for 16,000 IOPS and 1,000 MiB/s;
- 16 logical / 8 physical CPU cores, approximately 30 GiB RAM, no swap;
- clean release revision `28765dbf2b75c951a7bebc76c2c912ca659f58a1`;
- downloaded prebuilt DuckDB linkage and the repository release profile.

The strict benchmark preflight passed with matched local, synchronized, and built revisions.
FineWeb's 2,147,509,487-byte Parquet source was acquired once before the timed region. Each sample
used a fresh project state and DuckDB destination while reusing the source bytes. Samples ran in
balanced order with `--color never --unicode never`:

```text
cdf --progress never  run fineweb_local.documents
cdf --progress always run fineweb_local.documents
```

Observed wall-time samples:

| Progress | Samples (seconds) | Median |
|---|---|---:|
| never | 24.03, 22.69, 22.52 | 22.69 |
| always | 22.59, 22.81, 21.70 | 22.59 |

The enabled/disabled median delta was `-0.4407%`. This is ordinary sample variance, not a speedup
claim; it passes the maximum `+1%` overhead criterion. Progress-enabled samples wrote 2,216 bytes
to stderr while preserving the final result on stdout. Disabled samples wrote zero stderr bytes.

Before the paired matrix, the exact formerly failing default run completed:

- 1.1 million rows;
- 2.1 GiB logical data;
- 14 canonical segments;
- 23.22 seconds process wall time;
- verified DuckDB receipt and committed checkpoint;
- 6,545,948 KiB maximum process RSS and zero swaps.

Canonical recordings used an explicit 100×40 pseudo-terminal:

- normal plan;
- verbose plan;
- redirected headless run;
- pure JSON plan with empty stderr;
- replay into a second DuckDB database through a fresh checkpoint store;
- parser failure with stable code and copyable remediation.

The fresh replay store is deliberate: the package replay conformance contract isolates
destination idempotency from an already-committed checkpoint id. Reusing the original committed
checkpoint store fails closed by design.

## Artifacts

- `.10x/evidence/.storage/2026-07-25-cx4-hosted-cli-overhead.json`
  (`sha256:5bc9f20d5010483aede44593bb35267e49b54182e1b34fe64fed061ea20c42b9`)
- `.10x/evidence/.storage/2026-07-25-cx4-hosted-cli-artifacts.tar.gz`
  (`sha256:df566218e7bf9656c197d5f114715bfd122aba8cbfb849e8bba2b3f623a96f14`)

The archive contains the machine summary, raw timing files, representative enabled/disabled
stdout/stderr, the recording manifest, terminal transcripts, redirected channels, JSON output,
replay transcript, and failure transcript.

## What this supports

- CX4's hosted end-to-end overhead threshold.
- CX4's canonical channel, disclosure, replay, and failure recordings.
- The staged oversized-singleton repair's real-product acceptance criterion.
- WS9's aggregate CLI conformance and negligible-overhead closure.

## Limits

The negative timing delta is not evidence that rendering improves execution. FineWeb is a
columnar local-source/DuckDB workload; the permanent million-event, high-partition, slow-terminal,
width, Unicode, redaction, and JSON-isolation tests cover the renderer-specific adversaries.
