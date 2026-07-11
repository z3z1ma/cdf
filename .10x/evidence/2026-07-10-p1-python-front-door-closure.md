Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws7-python-front-door.md, .10x/tickets/done/2026-07-08-p1-product-ws7a-python-resource-resolution-plan-preview.md, .10x/tickets/done/2026-07-08-p1-product-ws7b-python-run-spine.md, .10x/tickets/done/2026-07-08-p1-product-ws7c-python-interpreter-ci-matrix.md, .10x/tickets/done/2026-07-08-p1-product-ws7d-dlt-ga-gap-integration.md

# P1 Python front-door closure evidence

## What was observed

Commits `edc8468e`, `daff44b6`, `fa1b8092`, and `02420904` make `python://` resources first-class trait-backed project resources. Inspect and plan import descriptor/schema metadata without invoking the row callable; preview invokes one bounded batch without package, destination, checkpoint, or ledger writes; run enters the ordinary package/receipt/checkpoint spine; replay succeeds after the Python source is replaced with a module that raises on import.

The kernel `ResourceStream::validate_runtime_dependencies` hook now owns adapter preflight. `ProjectRunSource`, CLI run/plan/preview, and inspect consume `dyn QueryableResource`; no Python variant or Python branch remains in generic orchestration or command execution. The conformance matrix registers Python as a source archetype and exercises the same destination protocol as file, REST, and SQL sources.

The strict `.github/workflows/python-interpreters.yml` matrix targets CPython `3.14` and `3.14t`. Both jobs run the bridge, imported dlt, product-spine, and free-threaded doctor tests, upload a deterministic fixture hash, and a dependent job requires byte-identical hashes with `cmp`. GitHub's official `actions/setup-python` documentation confirms the `3.14t` suffix is supported; PyO3 0.29 documentation confirms CPython 3.14 free-threaded builds are supported. The local host supplies only its attached GIL interpreter, so local evidence is the deterministic fixture plus workflow-source validation; hosted matrix output will be produced by the checked-in required workflow rather than fabricated here.

The imported `cdf_sdk.dlt` test uses real `@dlt.resource` and `@dlt.source` decorators. It maps primary and merge keys, exact incremental cursor and lag, merge/SCD2 compatibility data, freeze/evolve contract hints, source scope, and batch output. Unselected and `write_disposition="skip"` resources are filtered without execution. The migration table retains the explicit rule that dlt destination delegation is unsupported because CDF owns packages, receipts, checkpoints, replay, and commits.

## Procedure and results

- `cargo test -p cdf-cli --locked python_resource_ --no-fail-fast` — 4 passed.
- `cargo test -p cdf-python --locked --no-fail-fast` — the pre-closure full suite passed; the added imported-decorator test also passed independently.
- `CDF_PYTHON_FIXTURE_HASH_OUTPUT=/tmp/cdf-python-fixture-hash cargo test -p cdf-python --locked concurrency_semantics_are_identical_for_fixture_hashes` — passed and wrote `sha256:8517462c215fdcff5f90758bdf5c2835543387f6bec3c9181908248f612ea0fa`.
- `cargo test -p cdf-conformance --locked p2_preview_run_parity_law_covers_supported_archetypes -- --nocapture` — passed with file, Python, REST, and SQL.
- `cargo test -p cdf-conformance --locked run_matrix_file_python_rest_sql_source_cells_persist_output -- --nocapture` — passed: 32 executed cells and 4 destination-sheet exclusions; Python owned 8 executed cells and the one legitimate Parquet-merge exclusion.
- Every executed matrix cell asserted plan honesty, package verification, trait-level receipt verification, receipt-before-checkpoint gating, duplicate no-op behavior, and artifact replay identity across DuckDB, filesystem Parquet, and Postgres.
- `cargo test -p cdf-project --locked --no-fail-fast` first exposed two stale message assertions after validation moved to the trait; both were repaired and rerun successfully. The remaining 167 tests had passed in that run.
- `cargo clippy -p cdf-kernel -p cdf-declarative -p cdf-project -p cdf-python -p cdf-cli --all-targets --locked -- -D warnings` — passed.
- `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings` — passed.
- Generated error documentation was refreshed and `--docs-only --check` was clean before the architectural refactor; the refactor added no CLI error mapping.
- `git diff --check` and the closed-enum/source-branch searches were clean before each commit.

## What this supports

This supports every acceptance criterion in WS7A-WS7D and the parent: product resolution, honest no-write plan, bounded no-write preview, general-spine execution, destination breadth, package and receipt verification, checkpoint gating, replay/source independence, interpreter remediation, deterministic cross-interpreter CI, and realistically imported dlt compatibility.

## Limits

The local machine cannot execute the hosted `3.14t` job. The workflow is fail-closed and has no allowed-failure path, so unavailability or semantic drift is visible as CI failure. Python remains an authoring/interchange adapter; runtime scheduling and subprocess/WASM interop remain outside P1.
