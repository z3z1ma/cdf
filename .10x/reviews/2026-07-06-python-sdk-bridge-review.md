Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-05-python-sdk-bridge.md
Verdict: pass

# Python SDK and bridge closure review

## Target

Review of the `firn-python` implementation, typed `firn-sdk` files, and verification evidence for `.10x/tickets/done/2026-07-05-python-sdk-bridge.md`.

## Assumptions tested

- Python remains an authoring/interchange boundary and does not move execution semantics into the kernel.
- Dict rows do not create a new row runtime; they cross into Arrow-backed kernel batches at the boundary.
- Arrow PyCapsule import uses a maintained bridge dependency rather than firn-owned unsafe FFI.
- Secret and log surfaces reuse `firn-http` redaction primitives.
- GIL/free-threaded behavior is modeled conservatively when the local interpreter cannot prove free-threaded runtime behavior.
- Dependency changes pass advisory scanners.

## Findings

No blocking findings.

The first PyCapsule bridge dependency choice (`arrow-pyarrow` with PyO3 0.28.3) failed advisory scans on two PyO3 RustSec advisories. That issue was resolved before closure by switching to `pyo3-arrow` 0.19.0 on PyO3 0.29.0 and rerunning `cargo deny check advisories`, `cargo audit`, and OSV successfully.

## Verdict

Pass. The implementation satisfies the ticket with focused surfaces in `crates/firn-python/**` and typed Python SDK files. Evidence in `.10x/evidence/2026-07-06-python-sdk-bridge.md` maps to the acceptance criteria and records local limits for free-threaded execution and missing `pyarrow`.

## Residual risk

Actual free-threaded parallel execution and Python-library PyCapsule import should be re-run on an environment with a 3.14t interpreter and an Arrow-producing Python library such as `pyarrow` or `arro3-core`. This is a verification-environment limit, not an implementation blocker for the current local ticket because the bridge avoids unratified runtime behavior and records the limit in evidence.

After the ticket was moved to `done`, a scoped worker left some parent/sibling references unchanged because its write boundary excluded those records. Later parent record maintenance repaired the live parent and dlt ticket references.
