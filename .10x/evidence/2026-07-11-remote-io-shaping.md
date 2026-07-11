Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/generation-bound-overlapped-io.md, .10x/specs/remote-local-io-overlap.md, .10x/tickets/2026-07-10-p3-ws-g-remote-io-overlap.md

# Remote I/O shaping evidence

## What was observed

Transport listings and range/HTTP responses are collected into vectors; object operations hide a private runtime/mutex; remote non-Parquet fully spools before decode; generation preconditions, per-origin pooling/controller, readahead, and network/decode overlap are absent.

## Procedure

Traced local/HTTP/object-store list/metadata/range/spool and Parquet paths, then compared identity/concurrency/memory behavior to source/format/host/scheduler contracts.

## What this supports

Generation-bound async byte sources, streaming listing/body APIs, pooled per-origin adaptive admission, bounded spool/cache, and full pipeline overlap.

## Limits

This is shaping evidence. Network/device/controller defaults and provider behavior require G1-G4 measurements.
