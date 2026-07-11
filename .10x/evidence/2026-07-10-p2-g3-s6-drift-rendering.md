Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-ws-g3-s6-drift-verdict-rendering.md

# P2 G3 S6 drift rendering evidence

## Observation

`ProjectRunReport` carries typed `TerminalSchemaObservationQuarantine` values cloned from `EnginePlan.effective_schema_evidence`; the CLI does not reopen or reinterpret package JSON. JSON output serializes complete verdicts. Human output uses dedicated panels so narrow terminals retain full remediation text rather than truncating a wide table.

The governed incompatible-Parquet fixture completes successfully with zero accepted segments, writes the named quarantine artifact, commits receipt/checkpoint semantics unchanged, and renders `a.parquet`, `VendorID`, canonical observed/effective types, `schema-observation:incompatible`, evolve policy, and the three remediation branches.

## Procedure

- `cargo test -p cdf-project -p cdf-cli`: 169 project tests, 266 CLI tests, and the doctor environment integration test passed.
- `cargo test -p cdf-conformance p2_`: 9 P2 matrix/conformance tests passed with S6 covered.
- Focused S6 regression `governed_evolve_quarantines_incompatible_file_with_exact_arrow_field_evidence` passed for JSON, human, and package artifact assertions.
- Strict clippy was run; its only finding was a mechanical single-element pending-scenario loop after S6 promotion, repaired to a direct S4 assertion before closure.

## Limits

Row-level residual rendering remains separate from terminal file/schema quarantine verdicts.
