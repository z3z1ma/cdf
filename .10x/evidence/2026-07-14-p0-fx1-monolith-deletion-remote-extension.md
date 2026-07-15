Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P0 FX1 monolith deletion and remote extension proof

## Observation

The original `cdf-formats` parser aggregation crate no longer exists in the workspace, lockfile, production source graph, or conformance registry. Its live consumers now use neutral `cdf-runtime` format contracts and dependency-isolated codec crates:

- `cdf-subprocess` composes the Arrow IPC stream and NDJSON drivers over one finite, ledger-owned `MemoryByteSource` boundary;
- Singer and Airbyte row groups use the same NDJSON driver rather than a private parser path;
- REST page reconciliation uses the NDJSON driver plus shared `cdf-contract` schema reconciliation, preserving the pinned output schema, physical observation identity, typed coercion plan, and row-local residual evidence;
- the conformance property corpus uses the registered NDJSON driver directly;
- the deleted crate's P2 coverage references now point at the shared contract, codec, and file-runtime laws that own the behavior.

`cdf-runtime::BoundedFormatRequest` and `decode_bounded_format` provide the finite-payload bridge. The source owns one content-addressed ledger lease; codecs receive only neutral `ByteSource`, memory, cancellation, schema, and position contracts; decode units release proven no-lookback frontiers; emitted batches retain their output leases through the kernel payload lifetime. The API is explicitly not an unbounded-stream abstraction.

A project-level external codec fixture proves the missing extension law over a remote provider. `project_external_mock` is registered only in the test composition registry, compiles from an ordinary declarative file resource, inventories through an injected HTTP transport, discovers and pins a hash-addressed schema, plans one remote partition, previews one row, and runs through package construction, DuckDB receipt verification, and checkpoint commit. No first-party format or provider branch was added.

## Procedure

1. Workspace and source residue:

   ```text
   cargo metadata --no-deps --format-version 1 --locked | jq -r '.packages[].name' | sort | rg '^cdf-format|^cdf-formats'
   cdf-format-arrow-ipc
   cdf-format-delimited
   cdf-format-json
   cdf-format-parquet

   rg -n 'cdf-formats|cdf_formats' Cargo.toml Cargo.lock crates --glob '*.toml' --glob '*.rs'
   # no matches
   ```

   This proves the workspace and live Rust/Cargo graph contain only dependency-isolated codec crates. Historical `.10x` records intentionally retain the former crate name as history; the active coverage matrix was updated to current owners.

2. Codec dependency isolation:

   ```text
   cargo tree -p cdf-format-json --edges normal --prefix none | rg '^cdf-format-'
   cdf-format-json ...

   cargo tree -p cdf-format-arrow-ipc --edges normal --prefix none | rg '^cdf-format-'
   cdf-format-arrow-ipc ...

   cargo tree -p cdf-subprocess --edges normal --prefix none | rg '^cdf-format-'
   cdf-format-arrow-ipc ...
   cdf-format-json ...
   ```

   Each codec leaf contains no sibling codec. The subprocess product intentionally composes exactly the two protocols it accepts and no Parquet/delimited parser family.

3. First-party format and file-runtime compatibility:

   ```text
   CARGO_BUILD_JOBS=12 cargo test \
     -p cdf-source-files -p cdf-format-parquet -p cdf-format-arrow-ipc \
     -p cdf-format-delimited -p cdf-format-json --lib --locked --no-fail-fast
   ```

   Result: source-files 48/48, Parquet 3/3, Arrow IPC 1/1 with one explicit performance ignore, delimited 1/1, and JSON 6/6 passed. The source cases include local/remote Parquet, direct remote Arrow IPC, CSV, JSON document, NDJSON retained-discovery handoff, gzip Parquet composition, object-store gzip NDJSON, generation reattestation, bounded/evicting spool laws, and the external codec plus transform law.

4. Neutral bounded consumers and parser adversaries:

   ```text
   CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime bounded_format --locked --no-fail-fast
   CARGO_BUILD_JOBS=12 cargo test -p cdf-subprocess --lib --locked --no-fail-fast
   CARGO_BUILD_JOBS=12 cargo test -p cdf-source-rest --lib --locked --no-fail-fast
   CARGO_BUILD_JOBS=12 cargo test -p cdf-declarative rest_runtime --lib --locked --no-fail-fast
   CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance property_fuzz --locked --no-fail-fast
   ```

   Result: 1/1, 12/12, 7/7, 33/33, and 20/20 passed. This includes malformed/truncated stdout, timeout/nonzero exit, protocol state, JSON framing, scalar drift/residual evidence, multi-page reconciliation, required fields, pagination, coercion policy, and adversarial bytes without panics or partial accepted reads.

5. Remote project extension law:

   ```text
   CARGO_BUILD_JOBS=12 cargo test -p cdf-project \
     project_external_codec_discovers_pins_previews_and_runs_over_remote_provider \
     --locked --no-fail-fast
   ```

   Result: 1/1 passed. The test asserts external driver identity in snapshot and partition evidence, persisted schema pin, preview rows, package rows/segments, and actual sequential remote-provider use.

6. Conformance ownership and physical-schema semantics:

   ```text
   CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance resource::execution --locked --no-fail-fast
   CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance \
     p2_friction_registry_maps_closed_slices_to_tests_and_open_rows_to_tickets \
     --locked --no-fail-fast
   CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance \
     p2_registry_named_tests_resolve_to_test_functions --locked --no-fail-fast
   CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance \
     mvp_acceptance_demo_fixture_proves_rest_duckdb_recovery_replay_and_drift \
     --locked --no-fail-fast
   ```

   Result: 5/5, 1/1, 1/1, and 1/1 passed. Execution conformance can now distinguish a pinned effective schema from each batch's truthful physical observation hash instead of asserting they are the same identity.

7. Build and lint graph:

   ```text
   CARGO_BUILD_JOBS=12 cargo check -p cdf-project -p cdf-conformance \
     -p cdf-declarative --all-targets --locked
   CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-format-arrow-ipc \
     -p cdf-format-json -p cdf-subprocess -p cdf-source-rest \
     --all-targets --no-deps --locked -- -D warnings
   cargo fmt --all -- --check
   git diff --check
   ```

   Result: all exited 0. The first check traversed the complete affected product/conformance dependency graph. Strict leaf Clippy covers every changed production implementation crate.

8. Closure-review repairs and permanent regression laws:

   ```text
   CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime bounded_format --locked --no-fail-fast
   CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime --test build_graph --locked --no-fail-fast
   CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance \
     bounded_ndjson_decode_releases_input_and_output_leases --locked --no-fail-fast
   CARGO_BUILD_JOBS=12 cargo test -p cdf-project \
     generic_discovery_builds_deterministic_snapshot_without_transport_identity \
     --locked --no-fail-fast
   ```

   Result: 3/3 bounded-source laws, 2/2 build-graph laws, 1/1 end-to-end codec lease law, and 1/1 generic snapshot law passed. The laws reject payloads larger than the shared budget, observe cancellation before open and between stream polls, release source and decoded-batch leases to zero, prove every first-party codec is isolated from sibling codecs and upper product layers, and prove generic schema snapshots contain no transport identity. Fast CI, README, and `VISION.md` now name the neutral runtime and isolated codec/source crates; the removed aggregation crate survives only in historical records and one negative graph assertion.

## What this supports or challenges

This supports every FX1 acceptance criterion: open registry routing, neutral contracts, shared reconciliation, deterministic logical units, five-format behavior, parser-local build domains, local mock codec/transform composition, and remote project discover/pin/preview/run composition. It challenges the earlier review finding that `cdf-formats` remained a live parser/dispatch boundary: the package and all 3,126 tracked lines in its manifest/source/tests are deleted, while its still-valid laws have named current owners.

## Limits

- H3 still owns replacing `wait_with_output` with truly incremental, backpressured subprocess stdout/stderr. This slice accounts a completed finite payload before codec ownership; it does not claim arbitrarily long child output is constant-memory.
- The project/conformance aggregate suites currently contain failures from independent, active artifact/golden/source-admission migrations. They were not used as FX1 closure evidence. The focused extension, format, REST, subprocess, property, P2 registry, project remote, build, and lint laws above are green.
- Performance envelopes, new native formats, transform throughput, projection/predicate optimization, and unbounded producer protocols remain their P3 WS-B/H owners.
