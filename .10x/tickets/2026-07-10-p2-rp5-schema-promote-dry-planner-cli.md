Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp1-residual-envelope-codec.md, .10x/tickets/done/2026-07-10-p2-rp3-correction-capabilities-row-provenance.md, .10x/tickets/done/2026-07-10-p2-rp4-schema-scope-lease-lock-cas.md, .10x/tickets/done/2026-07-10-p2-ws-a10g-explicit-sampled-binary-discovery.md

# P2 RP5 schema promote dry planner and CLI

## Scope

Implement `cdf schema promote RESOURCE [--type JSON_POINTER=ARROW_TYPE ...]` as a strictly no-write planner. Inventory verified residual evidence, fresh/pinned schema facts, target mappings/correction sheets, affected addresses, migrations, and recovery constraints; render P1 human/JSON output.

## Acceptance criteria

- Dry planning writes no snapshots, lockfiles, packages, destinations, checkpoints, leases, or ledger events.
- Plans list residual paths/type sets/counts, proposed types, affected packages/rows/targets, evidence availability, correction strategy, migrations, conflicts, and recovery command.
- Only unambiguous compatible fresh discovery may auto-propose a type; ambiguous/lossy cases require `--type`, and lossy also requires existing allowance.
- Unknown paths, invalid Arrow types, unsupported mappings/strategies, tombstone-only evidence, and stale pin authority produce precise errors.
- Plan artifacts carry old/new schema hashes, exact lock precondition, destination sheet hashes, and deterministic promotion identity.
- Command/help/JSON schema and command-specific diagnostics are covered.

## Evidence expectations

No-write filesystem/destination/ledger assertions, deterministic plan goldens, retained/tombstone/missing inventories, destination strategy matrix, CLI rendering, and adversarial review.

## Explicit exclusions

No `--execute`, correction writes, lease acquisition, lockfile mutation, or GC change.

## Progress and notes

- 2026-07-10: Opened after CLI and dry-plan semantics were confirmed.

## Blockers

Depends on RP1/RP3/RP4 and A10g.
