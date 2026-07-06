Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-dlt-shim-preview.md

# dlt shim preview evidence

## What was observed

Scoped dlt preview support was implemented under the Python bridge and typed SDK boundary:

- `crates/firn-python/src/dlt.rs` adds deterministic dlt shim metadata parsing, migration-table divergence records, dlt write-disposition/key/incremental/contract hint mapping into Firn descriptors, and a committed-checkpoint-head state view for dlt current-state semantics.
- `crates/firn-python/src/bridge.rs` adds `PythonResourceBridge::batches_from_dlt_resource` and `batches_from_dlt_source`, preserving the existing dict/Arrow boundary and returning normal `PythonBatchRead` batches/descriptors.
- `python/firn_sdk/dlt.py` adds typed preview decorators and fixture helpers for `resource`, `source`, `incremental`, `current.state`, and source binding without requiring a live dlt runtime dependency.
- `crates/firn-python/src/tests.rs` adds representative Rust fixture tests for descriptor hint mapping, source expansion, migration-table snapshots, and committed-head current-state views.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn` after implementation and after the lifetime/test-fixture fix:

```text
cargo fmt -p firn-python
cargo fmt -p firn-python -- --check
python3 -m compileall -q python/firn_sdk python/examples
uvx pyright python/firn_sdk python/examples
PYTHONPATH=python python3 - <<'PY'
from firn_sdk import dlt

@dlt.resource(
    name='orders',
    primary_key='id',
    merge_key=('id', 'region'),
    write_disposition={'disposition': 'merge', 'strategy': 'scd2'},
    schema_contract='freeze',
    incremental=dlt.incremental('updated_at', initial_value='2026-01-01T00:00:00Z'),
)
def orders():
    yield {'id': 1, 'region': 'us', 'updated_at': '2026-07-01T00:00:00Z'}

metadata = getattr(orders, '__firn_dlt_metadata__')
assert metadata['primary_key'] == 'id'
assert metadata['merge_key'] == ('id', 'region')
assert metadata['write_disposition']['disposition'] == 'merge'
assert metadata['schema_contract'] == 'freeze'
assert metadata['incremental']['cursor_path'] == 'updated_at'
print('dlt metadata fixture ok')
PY
git diff --check -- crates/firn-python python/firn_sdk
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo test -p firn-python --locked --no-fail-fast
cargo clippy -p firn-python --all-targets --locked -- -D warnings
```

## Results

- `cargo fmt -p firn-python`: passed.
- `cargo fmt -p firn-python -- --check`: passed.
- `python3 -m compileall -q python/firn_sdk python/examples`: passed.
- `uvx pyright python/firn_sdk python/examples`: passed with `0 errors, 0 warnings, 0 informations`.
- The `PYTHONPATH=python` dlt metadata fixture probe passed and printed `dlt metadata fixture ok`.
- `git diff --check -- crates/firn-python python/firn_sdk`: passed.
- Initial `cargo fmt --all -- --check` failed before a scoped formatting verdict because parallel out-of-scope CLI split work had `mod tests` but no `crates/firn-cli/src/tests.rs`. After that split work progressed, the same command passed.
- Initial `cargo test -p firn-python --locked --no-fail-fast` and `cargo clippy -p firn-python --all-targets --locked -- -D warnings` failed before compiling `firn-python` because `crates/firn-formats/src/types.rs` derived `Clone, Debug` for `FormatRead` while `firn_contract::ObservedSchema` did not implement those traits. After that split work progressed, Cargo reached the dlt shim and exposed two in-scope issues: an explicit lifetime was needed on `materialize_dlt_resource`, and embedded Python Rust tests could not import `firn_sdk` without `PYTHONPATH`.
- After fixing the dlt bridge lifetime and making the Rust fixture modules self-contained, `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo test -p firn-python --locked --no-fail-fast`: passed; 19 unit tests passed and 0 doctests ran.
- `cargo clippy -p firn-python --all-targets --locked -- -D warnings`: passed.

## What this supports or challenges

This supports the ticket acceptance criteria:

- dlt-like primary key, merge key, incremental cursor, write disposition, and contract-mode hints are mapped into Firn descriptors/contracts in the Rust fixture tests.
- `dlt.current.state` preview semantics are represented as a committed-checkpoint-head state view in `dlt_current_state_view_reads_committed_checkpoint_heads`.
- Divergences from live dlt behavior are serialized as migration-table data and covered by the descriptor mapping snapshot test.
- Shim resources leave Python through `PythonBatchRead` descriptors and batches, preserving the native Firn Python bridge path for downstream planning, packaging, and checkpointing.
- The typed SDK shim remains compile-clean and pyright-clean without requiring a live dlt runtime dependency.

## Limits

No live dlt runtime behavior was tested; the preview intentionally uses deterministic shim metadata and records dlt runtime divergence as migration-table data. The state model proves committed-head view behavior, not mutable live dlt state writes outside Firn's checkpoint/receipt path.
