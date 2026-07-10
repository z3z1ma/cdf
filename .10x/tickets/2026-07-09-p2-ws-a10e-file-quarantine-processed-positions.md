Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/tickets/2026-07-09-p2-ws-a10d-effective-schema-runtime-evidence.md, .10x/decisions/data-onramp-file-source-transport-manifest.md

# P2 WS-A10e file quarantine and processed positions

## Scope

Add terminal file-level contract verdicts, quarantine evidence, and processed-file positions independent of accepted output segments so incompatible drift completes through the normal package/receipt/checkpoint gate, including all-quarantine runs.

## Acceptance criteria

- Incompatible/narrowing files receive a stable named file-level rule with path, physical versus baseline/effective type, policy, and remediation; they never surface as an internal stack error.
- `freeze` quarantines any schema deviation from baseline; `evolve` quarantines only differences without a ratified compatible join/coercion.
- Quarantine artifacts record exact runtime file identity and per-field evidence without leaking sensitive locations/values.
- Admitted and quarantined processed positions exist independently of output segments and aggregate deterministically into checkpoint state.
- An all-quarantine run produces a valid evidence package, destination receipt, and committed checkpoint without inventing data rows/segments.
- A quarantined identity advances only after receipt verification and checkpoint commit; an unchanged identity is skipped later, while a changed identity retries.
- Removed files never delete destination data or historical manifest entries.
- Crash/replay/recovery tests cover failures before package, after package, after receipt, and before checkpoint.

## Evidence expectations

Mixed and all-quarantine runs, package/quarantine/receipt/checkpoint inspection, manifest rerun/change tests, crash matrix, redaction, replay, destination conformance, and adversarial review.

## Explicit exclusions

No row-level policy redesign, destination quarantine-sheet expansion beyond existing sheets, cloud transport implementation, or HTTP enumeration.

## Progress and notes

- 2026-07-09: Opened from the ratified gate-backed quarantine advancement rule.

## Blockers

Depends on A10d.
