Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-source-files/src/runtime.rs, crates/cdf-project/src/schema_discovery.rs
Verdict: pass

# FX1 registered format discovery review

## Findings

No critical or significant unresolved finding in the registered binary slice.

Discovery now consumes the same descriptor and driver object as execution. The driver remains below project code and receives only the neutral byte source and bounded request. Content identity and driver semantic version are visible in snapshot evidence. The transformed path remains checksum-gated before discovery accepts bytes.

The uncompressed Parquet remote special case is a measured capability optimization, not semantic dispatch: it uses bounded generation-aware ranges because the current synchronous transport cannot yet implement neutral `ByteSource`. G1 owns its replacement. Row discovery and format confirmation remain explicit blockers and are not represented as complete.

## Verdict

Pass. The change removes duplicated binary discovery authority without adding a compatibility shim or weakening bounded discovery.

## Residual risk

Remote non-Parquet registered discovery currently spools the verified object, so it is correct but not the final bounded/overlapped implementation. G1/G2 remain the owner.

