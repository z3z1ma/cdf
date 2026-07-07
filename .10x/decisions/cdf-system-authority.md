Status: active
Created: 2026-07-05
Updated: 2026-07-05

# Adopt the cdf book as the ratified system contract

## Context

The repository currently contains `VISION.md` and no pre-existing `.10x/` records. The user requested implementation of the entire system described by the book, explicitly instructed use of 10x, and ratified anything unambiguous or clear enough from the book. The book says it will eventually be removed from the repository once the system is complete, so durable project memory cannot depend on chat or on the book remaining present forever.

## Decision

Implement cdf according to the book's clear contracts, with active `.10x/` specifications and tickets serving as the durable execution graph. Until a specific behavior is copied into or superseded by an active `.10x/` record, `VISION.md` remains the canonical source for that behavior. When an active decision or specification is more specific than the book, the active record governs implementation.

The implementation will proceed by subagent-owned executable child tickets under `.10x/tickets/2026-07-05-implement-cdf-system.md`. The parent agent acts as orchestrator: it assigns child tickets, integrates outputs, records evidence, performs review, and reconciles the record graph.

The system will be implemented in roadmap order: MVP first, then fast-follow and post-MVP surfaces, unless a dependency edge requires an earlier seam. "Entire system" includes MVP, fast-follow, and beyond-MVP features described by the book; the book's own cutline only controls sequencing, not deletion of later scope.

## Alternatives Considered

- Implement directly from the book without records. Rejected because the book is large, will be removed later, and direct implementation would force future agents to rediscover authority.
- Treat the book as a loose inspiration. Rejected because the user ratified clear book behavior.
- Build only the MVP. Rejected as final scope because the user requested the entire system. Accepted only as the first milestone.

## Consequences

This creates a spec-first gate: code implementation must wait until the focused specifications and executable tickets exist. The book can be removed only after active records and implemented artifacts contain the behavior needed by future maintainers. If source, tests, or later user input conflict with active records, the conflict must be named and resolved by superseding records rather than silently choosing.

