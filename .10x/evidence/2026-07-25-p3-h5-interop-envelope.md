Status: recorded
Created: 2026-07-25
Updated: 2026-07-25

# P3 H5 implemented interop envelope

## Observation

Implemented Python and subprocess producers now project through one
source-neutral runtime boundary. Plans expose transferable boundary
capabilities. Successful partition completion carries invocation-local actual
transfer/copy/control telemetry into the project run report and CLI without
changing plan, package, manifest, or replay identity.

Python Arrow C ownership tests prove imported payload allocation is released
exactly once after producer deletion, downstream cancellation, import failure,
cross-thread destruction, and early stream cancellation. The proof uses Arrow's
supported owner-backed buffer model rather than interposing on opaque FFI
private data.

Control-event retention is constant: a real Singer producer emitted 2,049
ordered control events while the dedicated control consumer peaked at 4,096
bytes and returned to zero. Other decoder windows remain independently
accounted and are not attributed to control retention.

## Procedure

All commands ran from the repository root on the local
`aarch64-apple-darwin` development host:

```text
DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-python arrow_capsule --locked -j 12
CARGO_BUILD_JOBS=12 cargo test -p cdf-foreign-stream --locked -j 12
CARGO_BUILD_JOBS=12 cargo test -p cdf-subprocess singer_control_flood_is_projected_immediately_with_constant_managed_retention --locked -j 12
DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-cli python_resource_plan_preview_run_and_replay_use_the_product_spine --locked -j 12
CARGO_BUILD_JOBS=12 cargo test -p cdf-python --release dict_row_batch_curve_reports_throughput_without_changing_defaults --locked -j 12 -- --ignored --nocapture
CARGO_BUILD_JOBS=12 cargo test -p cdf-subprocess --release subprocess_stream_release_envelope_reports_ipc_and_row_modes_separately --locked -j 12 -- --ignored --nocapture
```

The Python release curve over one million two-field rows reported:

```text
batch_rows=1024  elapsed_ms=368 rows_per_second=2711781 outcomes=977 peak_boundary_bytes=68894
batch_rows=8192  elapsed_ms=355 rows_per_second=2815176 outcomes=123 peak_boundary_bytes=549150
batch_rows=65536 elapsed_ms=394 rows_per_second=2537356 outcomes=16  peak_boundary_bytes=4391198
```

The subprocess release envelope over 524,288 rows reported:

```text
arrow_ipc_stream: 8 batches, 68107968 logical bytes, 13534583 ns first batch,
33599000 ns total, 14813896 source bytes, 20750593 managed peak bytes,
8 copy-unknown batches

row_compat_ndjson: 8 batches, 14681824 logical bytes, 11126042 ns first batch,
60156375 ns total, 21384698 source bytes, 57540609 managed peak bytes,
8 copy-unknown batches
```

The product-spine test proves the compiled Python plan reports Arrow C plus row
compatibility on the blocking lane and the actual run reports its one known-copy
row batch. The same test completes preview, package, receipt, checkpoint, and
replay.

## What this supports

- Actual foreign transfer and copy evidence survives the ordinary runtime
  decorators and reaches explain/run reporting without a Python or subprocess
  branch in generic orchestration.
- Python release/lifetime behavior is exact across success, cancellation, error,
  producer deletion, and downstream-thread destruction.
- Owned control facts are processed without an unbounded queue.
- Python row windows and subprocess IPC/row modes have separate, host-labelled
  performance and memory evidence.
- WASM remains honestly prospective and unmeasured.

## Limits

The local release cells are boundary measurements, not EC2 whole-product
promotion results. Arrow IPC and subprocess row compatibility remain
`copy_unknown`; serialized/output byte counts are not silently relabelled as
copy counts. In-process Python can allocate native memory before yielding an
observable Arrow object, so only admitted payload and CDF-owned conversion
windows are enforceable. The isolated subprocess boundary remains the authority
when a total arbitrary-producer memory ceiling is required.
