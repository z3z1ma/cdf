Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-05-dlt-shim-preview.md
Verdict: pass

# dlt shim preview closure review

## Target

Review of the scoped dlt preview implementation under `crates/firn-python/**`, the typed Python SDK shim under `python/firn_sdk/**`, and evidence in `.10x/evidence/2026-07-06-dlt-shim-preview.md`.

## Assumptions tested

- The preview must not require or emulate a live dlt runtime.
- dlt-like metadata must become Firn descriptor data rather than a second execution model.
- `dlt.current.state` must remain a ledger-backed committed-head view, not a mutable bypass around checkpoint commits.
- Divergences from dlt behavior must be explicit data, not hidden behavior.
- Source expansion must still yield normal Python bridge batches so downstream planning, packaging, and checkpoints see native Firn resource output.

## Findings

No blocking findings.

Two in-scope issues surfaced during recheck and were fixed before closure:

- `materialize_dlt_resource` needed an explicit Python lifetime on its returned `Bound`.
- Rust dlt fixture modules initially imported `firn_sdk`, which made `cargo test -p firn-python` depend on `PYTHONPATH`; the tests now attach deterministic shim metadata directly and the SDK surface is verified separately by compileall, pyright, and a `PYTHONPATH=python` metadata probe.

## Verdict

Pass. The implementation satisfies the preview ticket within its explicit exclusions. The Rust tests cover descriptor mapping, source expansion, migration-table divergence data, and committed-head state views. The required formatting, workspace check, scoped Rust test, scoped clippy, compileall, and pyright gates all pass.

## Residual risk

This remains a preview shim. It does not test a live dlt runtime, does not delegate to dlt destinations, and does not attempt bug-for-bug dlt state mutation semantics. Those limits are intentional and represented in migration-table data and evidence.
