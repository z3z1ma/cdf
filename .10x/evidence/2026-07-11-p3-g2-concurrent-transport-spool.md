Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-g1-streaming-transport-byte-sources.md, .10x/tickets/2026-07-11-p3-g2-range-readahead-spool-controller.md, .10x/tickets/done/2026-07-11-p3-c2-parallel-frontier-execution.md

# Concurrent transport and remote spool milestone

## What was observed

The live file transport dependency no longer has `Arc<Mutex<Box<dyn FileTransport>>>`. `FileTransport` and `HttpFileTransport` require `Send + Sync` and expose shared operations. Reqwest's blocking client, object-store handles, local filesystem transport, and recording fixtures implement the same contract. Mutable fixture response queues retain narrow internal mutexes; production transport selection and transfers do not share a global lock.

Remote HTTP/object-store partition opening now creates an injected I/O scope with a two-batch bounded output edge. Partition validation, generation-bound sequential spool, registered/native or row-format stream creation, and forwarding execute in that scope. Local paths bypass the outer scope and retain the direct registered-format stream. Scope ids are fixed-length hashes of resource/partition identity, avoiding unbounded runtime labels.

The remote scope is created eagerly by `ResourceStream::open`, before its returned future is polled. This matters because C2 fills its frontier by calling `open` for every admitted ordinal, while canonical polling may return an earlier ready future without polling all later futures. Eager scope creation makes admission start work rather than merely allocate dormant futures.

## Procedure

- `cargo test -p cdf-source-files --locked` — 17 passed.
- `cargo test -p cdf-project http_parquet_auto_pin_plan_preview_and_run_use_file_runtime --locked` — 1 passed in 0.40 seconds on the final run.
- `cargo check --workspace --all-targets --locked` — passed; unrelated existing test-only `unused_mut` warnings remained outside the changed crates.
- `cargo clippy -p cdf-source-files -p cdf-declarative -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings` — passed.
- `cargo fmt --all` and `git diff --check` — passed.

The permanent concurrency test starts two independent callers against one `FileRuntimeDependencies` transport and observes peak concurrent entry of two. Source coverage includes local Parquet, remote Arrow IPC, object-store gzip NDJSON, recursive object-store listing, HTTP range/identity/egress/auth laws, and bounded spool rejection. The project fixture exercises HTTP Parquet auto-pin, plan, preview, run, package, destination, and checkpoint behavior through the real recording HTTP transport.

## What this supports or challenges

This supports the claim that the scheduler can overlap independent remote file partitions without a transport-wide critical section, while preserving bounded forwarding and local-path performance shape.

## Limits

Reqwest file transfer remains a blocking client running inside an injected I/O worker rather than a native async body stream. Remote registered formats currently traverse the outer two-item spool edge and their own two-item codec edge. G1 owns async provider replacement and G2 owns measured high-BDP admission/coalescing/cache work; no final throughput claim is made here.
