Status: active
Created: 2026-07-10
Updated: 2026-08-06

# Fenced leases and state-backed schema publication

Lease expiry time is store authority. Public lease operations never accept an executor-supplied
current timestamp: a caller could advance time and seize a live lease. Production stores own their
clock; deterministic conformance injects a controllable clock behind the store.

Fencing tokens increase monotonically per scope and persist across reopen. They are meaningful only
inside the stable authority-domain id that issued them. A token from another SQLite/Postgres store
is incomparable and must be rejected, even if its integer happens to match.

Schema versions are immutable. Schema-head mutation is a state transaction that validates exact
authority key, prior generation/hash, active owner/token, and version existence at the same
consistency boundary that writes the next head/history event. A caller-side `assert_current` is
useful diagnosis, never publication authority.

First-use batch establishment validates every proposed absent key and creates every version/head in
one transaction. One conflict writes none. Repeating an identical proposal is idempotent only when
the existing version is byte-identical and every requested key matches; a different proposal fails.

Promotion additionally requires a settlement barrier. Ordinary runs hold a short renewable
generation-bound permit only across destination mutation, receipt verification, and checkpoint
commit. Promotion fences new permits, drains or expires existing ones, establishes the complete old
generation cutoff, settles corrections, and atomically publishes the next head plus publication
event. An expired or stale executor cannot commit an old generation after publication.

The former filesystem lockfile CAS procedure is historical and no longer governs product schema
authority. Generic project-file publication still uses temporary bytes, sync, exact expectations,
atomic install, and cleanup where authored/generated multi-file writes require it, but no project
file is the schema commit point.
