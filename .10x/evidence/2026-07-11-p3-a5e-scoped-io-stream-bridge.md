Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a5e-streaming-graph-integration.md, .10x/specs/execution-host-structured-runtime.md

# Scoped I/O producer stream bridge

## What was observed

`ExecutionServices::spawn_io_stream` now runs an asynchronous producer inside an injected structured I/O scope and exposes its output as a bounded `Stream` to engine-side executors. The bridge does not collect the producer output. It joins the structured scope after channel exhaustion, surfaces the producer error as the stream terminal error, and cancels the scope when a consumer drops early.

The primitive is generic over item type and contains no source, format, transport, or destination branch.

## Procedure

- `cargo test -p cdf-engine scoped_io_stream_bridges_tokio_without_materializing_and_joins_errors --lib`
- `cargo clippy -p cdf-runtime -p cdf-engine --all-targets -- -D warnings`

Both commands passed. The test uses a channel capacity of one, delivers two values incrementally, then separately proves that an I/O producer error is observed through the consumer stream after its preceding value.

## What this supports

Tokio-native transports and decoders can feed the existing kernel/engine pull stream without a private runtime, blocking adapter, or whole-input `Vec`. The same bridge can host remote reads, long-lived streaming sources, and future foreign runtimes behind typed producers.

## Limits

The item bound combines with item-owned ledger leases to establish byte pressure; the bridge itself does not estimate unaccounted item sizes. Shared conformance must reject producers that send unaccounted payloads.
