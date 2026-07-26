Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Preserve vision authority at an empty executable backlog

## Context

The stabilization graph established by
`.10x/decisions/superseded/cdf-system-authority-bounded-backlog.md` is terminal. P0, P1, P2, and
P3 have closed their current cuts with evidence; unprioritized long-horizon capabilities are
indexed by `.10x/knowledge/active-backlog-and-future-roadmap.md`. Keeping a completed program
described as the "current aggregate execution graph" would make the active authority and the
executable backlog disagree.

## Decision

`VISION.md` remains authoritative until a more specific active decision or specification
supersedes it. Active `.10x/` records remain CDF's durable institutional-memory system.

The executable backlog MAY be empty. An active or pending coverage-matrix row, specification,
decision, research record, or roadmap entry does not by itself authorize implementation and MUST
NOT require an open reminder ticket.

A new executable ticket or program MUST be opened deliberately from a current product need,
revalidated temporal evidence, concrete acceptance criteria, and ratified semantics. It MUST be
the smallest bounded owner that can produce a complete outcome. Historical terminal programs are
evidence and context; they MUST NOT be reopened merely to reuse their names.

The terminal stabilization graph is
`.10x/tickets/done/2026-07-25-stabilization-steady-state-program.md`. The future roadmap is the
only aggregate index while no successor program is active.

## Alternatives Considered

- Keep the terminal stabilization program active as an evergreen parent. Rejected because it
  would turn a bounded closure program back into an unbounded monolith and make zero backlog
  impossible.
- Open placeholder tickets for every future vision row. Rejected because placeholders falsely
  communicate priority, readiness, and ownership.
- Treat an empty backlog as completion of the full vision. Rejected because the coverage matrix
  and roadmap intentionally preserve incomplete long-horizon capabilities.

## Consequences

The active ticket directory is a trustworthy queue rather than a product wish list. CDF can reach
a stable steady state without erasing its longer vision. Future work begins with an explicit
activation act and current evidence, while terminal programs remain auditable and immutable.
