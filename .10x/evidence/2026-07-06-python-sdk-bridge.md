Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-python-sdk-bridge.md

# Python SDK and bridge evidence

## What was observed

`crates/firn-python` now implements a PyO3-based Python authoring boundary. Dict yields are converted through the existing `firn-formats` NDJSON inference path into kernel `Batch` values. Arrow PyCapsule-speaking objects are detected through `__arrow_c_array__` and `__arrow_c_stream__`; the bridge imports them through `pyo3-arrow` 0.19.0 on PyO3 0.29.0 rather than hand-written firn-owned FFI.

The crate also models interpreter resolution and free-threaded/GIL semantics, watchdog timeout checks, byte-bounded boundary channels, deterministic fixture hashes over in-memory Arrow IPC bytes, and a redaction-aware Python context surface backed by `firn-http` secret/redaction primitives.

The Python SDK files under `python/firn_sdk/` are typed, include `py.typed`, and expose Protocols for HTTP, secrets, cursor, logger, Arrow PyCapsule exports, row yields, and the `@resource` decorator. `python/examples/github_issues.py` type-checks against the SDK.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn` after implementation:

```text
cargo fmt -p firn-python
cargo test -p firn-python --locked --no-fail-fast
cargo clippy -p firn-python --all-targets --locked -- -D warnings
python3 -m compileall -q python/firn_sdk python/examples
uvx pyright python/firn_sdk python/examples
git diff --check
cargo deny check advisories
cargo audit
osv-scanner scan source -r .
cargo tree -p firn-python --locked | rg "pyo3|arrow-pyarrow|pyo3-arrow|numpy"
python3 - <<'PY'
import sys, sysconfig
print(sys.version)
print('sys.executable', sys.executable)
print('Py_GIL_DISABLED', sysconfig.get_config_var('Py_GIL_DISABLED'))
print('gil_enabled', sys._is_gil_enabled() if hasattr(sys, '_is_gil_enabled') else 'unknown')
try:
    import pyarrow
except Exception as exc:
    print('pyarrow', type(exc).__name__, exc)
else:
    print('pyarrow', pyarrow.__version__)
PY
rg -n "unsafe|from_raw|transmute|MaybeUninit|arrow-pyarrow" crates/firn-python/src crates/firn-python/Cargo.toml || true
```

## Results

- `cargo fmt -p firn-python`: passed.
- `cargo test -p firn-python --locked --no-fail-fast`: passed; 16 unit tests passed and 0 doctests ran.
- `cargo clippy -p firn-python --all-targets --locked -- -D warnings`: passed.
- `python3 -m compileall -q python/firn_sdk python/examples`: passed.
- `uvx pyright python/firn_sdk python/examples`: passed with `0 errors, 0 warnings, 0 informations`.
- `git diff --check`: passed.
- `cargo deny check advisories`: passed with `advisories ok`.
- `cargo audit`: passed with exit code 0; no vulnerabilities reported.
- `osv-scanner scan source -r .`: passed with `No issues found`.
- `cargo tree -p firn-python --locked | rg ...`: showed `pyo3 v0.29.0` and `pyo3-arrow v0.19.0`; no `arrow-pyarrow` entry remained.
- Local interpreter probe: CPython `3.14.6`, executable `/opt/homebrew/opt/python@3.14/bin/python3.14`, `Py_GIL_DISABLED 0`, `gil_enabled True`, and `pyarrow ModuleNotFoundError No module named 'pyarrow'`.
- Firn-owned unsafe/source-surface search over `crates/firn-python/src` and `crates/firn-python/Cargo.toml` returned no matches.

An initial supply-chain attempt using `arrow-pyarrow` pulled `pyo3 v0.28.3`. `cargo deny check advisories`, `cargo audit`, and OSV all failed on `RUSTSEC-2026-0176` and `RUSTSEC-2026-0177`. The bridge was changed to `pyo3-arrow v0.19.0` with `pyo3 v0.29.0`; the advisory checks then passed.

## What this supports or challenges

This supports the ticket acceptance criteria:

- Dict rows enter kernel batches through the same NDJSON/Arrow inference path as Tier 4 row-shaped inputs.
- Arrow PyCapsule/C Data Interface boundaries are modeled and imported via `pyo3-arrow` without firn-owned unsafe FFI.
- `firn-sdk` is typed, has `py.typed`, and the example resource is pyright-clean.
- Deterministic fixture hashing is stable across modeled GIL and free-threaded execution semantics, and tests prove effective parallelism is 1 on GIL builds and greater than 1 only when the interpreter is modeled as free-threaded with the GIL disabled.
- Context secret/log handling uses `firn-http` `SecretProvider`, `SecretValue`, `Redactor`, and `TraceEvent` so secret material is redacted in traces and logs.
- Boundary channel tests prove byte accounting rejects full queues by bytes rather than message count.

No evidence challenged the implemented scope after the PyO3 0.28 dependency was removed.

## Limits

The local interpreter is a GIL-enabled CPython build, so actual free-threaded parallel execution could not be witnessed locally. The local Python environment does not have `pyarrow`, so the test that would exercise a Python-library `__arrow_c_stream__` yield returns early when `pyarrow` is unavailable; the Rust import path still compiles through `pyo3-arrow` and is covered by boundary detection/model tests. This evidence therefore supports local GIL correctness, typed SDK cleanliness, dependency safety, and modeled free-threaded semantics, but not measured free-threaded wall-clock parallelism on a 3.14t interpreter.

After moving the ticket to `done`, a scoped worker found old-path references outside its write boundary. Later parent record maintenance repaired the live parent and dlt ticket references.
