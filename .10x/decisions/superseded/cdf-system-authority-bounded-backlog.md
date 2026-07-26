Status: superseded
Created: 2026-07-25
Updated: 2026-07-26

# Preserve vision authority while separating execution from roadmap

## Context

The original authority decision at
`.10x/decisions/superseded/cdf-system-authority.md` correctly made `VISION.md` and active `.10x/`
records the system contract, but it also coupled execution to one monolithic parent ticket and
treated the entire long-horizon vision as one continuously active backlog. Weeks of implementation
left that graph mixing current stabilization, completed work, speculative connectors, future
runtimes, and research reminders. The user ratified pruning the executable backlog while
preserving future product direction.

## Decision

`VISION.md` remains authoritative until a more specific active decision or specification
supersedes it. Active `.10x/` records remain the durable execution and institutional-memory
system.

Executable tickets MUST represent currently prioritized, bounded, dependency-ready outcomes.
Long-horizon vision commitments that are not currently executable MUST remain visible in active
specifications, decisions, research, the coverage matrix, and
`.10x/knowledge/active-backlog-and-future-roadmap.md`; they MUST NOT remain open merely as
reminders.

`.10x/tickets/done/2026-07-25-stabilization-steady-state-program.md` is the current aggregate execution
graph. Later feature programs are activated deliberately from current product need and temporal
evidence. Cancellation of a speculative or over-broad ticket parks its implementation graph; it
does not reject the capability or weaken the vision.

## Alternatives Considered

- Keep the original monolithic parent active until every long-horizon capability ships. Rejected
  because it makes the active backlog unbounded and obscures the actual critical path.
- Delete future scope from records. Rejected because it would discard ratified direction and force
  later rediscovery.
- Keep reminder tickets open but mark them low priority. Rejected because open status would still
  falsely communicate executable ownership and dependencies.

## Consequences

The active backlog becomes a trustworthy execution queue. Parked capabilities retain their
architecture, research, and reactivation tests without blocking P1/P3 closure. The coverage
matrix may remain `pending` for a vision commitment even when no active implementation ticket
exists, provided the roadmap names its activation boundary. Future programs must revalidate
temporal facts and open the smallest bounded owner rather than reopening historical monoliths.
