Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-09-p2-ws-a10e-file-quarantine-processed-positions.md, .10x/tickets/done/2026-07-10-p2-rp2-residual-verdict-runtime-package.md
Verdict: pass

# P2 A10e and RP2 runtime outcomes adversarial review

## Target

Terminal multi-file quarantine/processed positions and residual capture/quarantine through compiler, engine, package, destinations, replay, project runtime, CLI, and conformance. The review applies `.10x/knowledge/source-destination-extension-invariant.md` as a P0 criterion.

## Assumptions tested

- File quarantine cannot bypass the receipt/checkpoint gate or invent data segments.
- A zero-data replace cannot truncate a target or advance a replace pointer.
- Processed identity is exact and reattested at execution, not copied blindly from planning.
- Repeated partitions do not multiply source metadata work or bind conflicting identities.
- Residual capture does not admit control-critical values or leak PII.
- `_cdf_variant` is framework-owned by exact metadata, not name alone.
- Dedup remains over governed user columns and residual materialization happens once.
- Contract hashes, observed physical evidence, and destination output schema are not conflated.
- Legacy plans cannot gain trusted authority during deserialization.
- Adding a source or destination does not require generic format/driver branches.
- Current conformance constructs the same plans as production.

## Findings

- Significant, repaired: processed file state originally depended on accepted output segments, making all-quarantine runs impossible. Typed processed-observation evidence now aggregates independently and is required for zero-segment state advancement.
- Significant, repaired: destination empty-package paths could imply normal replace/data effects. DuckDB now has an explicit `NoData` effect; Parquet omits objects and replace pointers; Postgres omits target DDL/write SQL while retaining receipt/state mirrors.
- Significant, repaired: repeated terminal observations initially re-probed per partition and could carry conflicting bindings. Plans bind an opaque source-neutral observation identity, reject conflicts, and cache one execution attestation.
- Significant, repaired: discovery execution reused a hidden metadata budget. The resolved per-executor budget is plan evidence and tampering is rejected.
- Significant, repaired: residual execution materialized `_cdf_variant` before package-order dedup, causing the user validation program to see a framework column. Residual values now remain an aligned sidecar through dedup and materialize exactly once at final output.
- Significant, repaired: contract code retained a disabled duplicate canonical Arrow model. The duplicate was deleted; the kernel owns the recursive IR and contract aliases it.
- Significant, repaired: project/destination planning independently reconstructed output schema. The engine plan now owns exact output schema authority, validates zero-row plans, and destinations consume it.
- Significant, repaired: runtime physical/source metadata caused emitted-schema drift. The engine validates and serializes physical coercion evidence before rebinding arrays to the compiled destination schema.
- Significant, repaired: Postgres initially duplicated residual field literals to bypass its reserved prefix. Exact framework-field classification is centralized in `cdf-contract`; impostors with wrong name, type, nullability, semantic, or encoding remain rejected.
- Significant, repaired: new schema authority made legacy plan JSON fail deserialization. Authority is honestly optional for legacy inspection, while execution and destination preflight fail before mutation when it is absent.
- Significant, repaired: conformance hand-authored current `EnginePlan` JSON and therefore missed new authority fields. Live-run and run-matrix fixtures now invoke the production compiler/planner; deterministic goldens were regenerated through the repository workflow and pass 100-run checks.
- Pass: generic kernel/engine code contains no Parquet/Arrow IPC dispatch or concrete destination branch for these behaviors. Formats emit neutral candidates; source adapters bind observations; destination drivers consume shared plan/capability contracts.
- Pass: redaction tests scan the package tree for PII sentinels, and unsupported residual encoding becomes a named quarantine verdict rather than an internal error.
- Pass: crash/recovery tests cover durable package, receipt-before-checkpoint, source deletion, exact-identity rerun, and changed-identity retry.

## Verdict

Pass. All material correctness, evidence, commit-gate, schema-authority, privacy, and extension-cost findings were repaired. Parent-observed workspace verification passed 883/883.

## Residual risk

Preview/run terminal-quarantine parity is intentionally not claimed here and is evidenced by `.10x/tickets/done/2026-07-09-p2-ws-a10f-multifile-discovery-runtime-conformance.md`; remaining remote cells are owned by `.10x/tickets/done/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md`. Remote transport behavior remains WS-E. Promotion/readback/correction behavior remains RP5-RP10. These are downstream graph owners, not unresolved acceptance criteria for A10e or RP2.
