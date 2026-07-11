Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p2-ws-h-scaffolding-id-model-two-minute-path.md
Depends-On: .10x/specs/data-onramp-source-experience-cli.md

# P2 WS-H5 REST add

## Scope

Extend `cdf add` to public REST endpoint URLs without guessing selector or cursor semantics.

## Acceptance criteria

- REST classification is explicit through the complete `--records`, `--cursor`, `--cursor-param` flag set.
- Partial semantics fail before network or writes.
- Stable HTTPS/loopback endpoints derive origin, path, egress allowlist, and best-effort cursor configuration without URL secrets.
- Add performs bounded REST discovery, pins the snapshot, writes schema-free TOML and lock state, and the result plans normally.

## Explicit exclusions

Interactive prompts and secret-backed REST auth creation are excluded; existing declarative auth remains available for manual configuration.

## Evidence expectations

Production HTTP fixture, generated-config assertions, plan integration, no-write negative test, full CLI suite and strict lint.

## Blockers

None.

## Progress and notes

- 2026-07-10: Implemented and closed with `.10x/evidence/2026-07-10-p2-h5-rest-add.md` and `.10x/reviews/2026-07-10-p2-h5-rest-add-review.md`.
