Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b5-json-codecs.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# B5 streaming NDJSON driver evidence

## What was observed

NDJSON source execution still used `cdf-formats` full-file reads and materialized all decoded batches even though the format registry and streaming byte/transform contracts were available.

## Procedure

- Added parser-local `cdf-format-json` with an `ndjson` driver.
- Discovery opens bounded sequential chunks, retains their existing memory leases, and exposes them through a zero-copy `BufRead` adapter to Arrow schema inference.
- Decode uses Arrow JSON's push/tape decoder on arbitrary accounted chunks, pre-reserves each output batch, emits an incremental `PhysicalDecodeStream`, preserves source position, and releases leases on drop/error.
- Registered the driver in CLI, source, and project test composition roots.
- Routed project NDJSON discovery through the registered-format adapter and made the legacy source fallback fail closed.
- Passed source gzip object-store execution, project gzip object-store discover/pin/run, local bounded discovery, affected all-target Clippy, and workspace checks.
- Removed an unnecessary engine dev dependency after it pulled DataFusion into the leaf all-target graph.

## What this supports or challenges

NDJSON no longer requires a package-sized byte buffer or batch vector on the live file path. Compression, local/remote spool, discovery, and execution compose through the same registry driver.

## Limits

No throughput claim is made yet. Arrow decoder native allocations and oversized single-row behavior still require RSS/fuzz evidence; JSON documents and semantic row-local quarantine parity remain open B5 work.
