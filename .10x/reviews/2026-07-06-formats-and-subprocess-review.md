Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-05-formats-and-subprocess.md
Verdict: pass

# Formats and subprocess review

## Target

Review of the MVP `firn-formats` and `firn-subprocess` implementation after the Parquet supply-chain split.

## Assumptions tested

- Arrow IPC stream reads must preserve schema and field metadata when converting to kernel batches.
- NDJSON inference must feed the same contract-observed schema path as other Arrow-backed inputs.
- CSV and JSON file sources should produce resource descriptors and batches with file-source positions.
- Subprocess stdout adapters must preserve stderr trace context and map timeout, exit, and malformed output into the shared error taxonomy.
- Adapter output must remain package-compatible.
- Known supply-chain scanner failures must not be hidden behind a passing ticket.

## Findings

None for the retained scope.

## Verdict

Pass. Focused tests cover Arrow IPC schema preservation, NDJSON contract integration, CSV/JSON file-source batches, package write/replay compatibility, subprocess stderr/exit/timeout/malformed-output behavior, and Parquet blocker reporting. The attempted `parquet` dependency was removed after scanners reported `RUSTSEC-2024-0436`; the unresolved Parquet requirement is owned by `.10x/tickets/2026-07-06-parquet-format-source-supply-chain.md`.

## Residual risk

MVP Parquet file-source behavior is not complete. The risk is not accepted as done; it is blocked under a separate ticket pending a ratified supply-chain path.
