Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p3-b4-delimited-fixed-width-codecs.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# B4 streaming CSV driver evidence

## What was observed

CSV remained in the monolithic `cdf-formats` full-file source path and project discovery had a concrete CSV adapter despite the live registry boundary.

## Procedure

- Added parser-local `cdf-format-delimited` with no engine, DataFusion, source, project, CLI, transport, or destination dependency.
- Added runtime-neutral `AccountedChunksReader`, an owning zero-copy `Read + BufRead` view over lease-owned byte chunks; migrated NDJSON discovery to the same primitive.
- Implemented bounded CSV inference with Arrow CSV `Format` and incremental execution with Arrow CSV `Decoder`.
- Registered CSV in standard and test composition roots, routed project discovery through the registered adapter, and made the source legacy fallback fail closed.
- Replaced the old project probe-name assertion with registered-driver authority.
- Passed local CSV discovery/execution, manifest-position and zero-memory assertions, project discovery, affected workspace checks, and all-target Clippy with warnings denied.

## What this supports or challenges

CSV discovery and execution now share one driver object and no package-sized source buffer or batch vector is required on the live path. The shared accounted reader prevents parser crates from inventing unowned discovery buffers.

## Limits

Only default headered comma-separated CSV is admitted by the current empty option schema. No throughput, malformed multiline, oversized record, parallel chunking, or fixed-width claim is made.
