Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Fenced leases and lockfile publication

Lease expiry time is store authority. Public lease operations must not accept an executor-supplied current timestamp: a caller could advance time and seize an otherwise live lease. Production stores own their clock; deterministic conformance injects a controllable clock behind the store.

Fencing tokens are monotonically increasing per scope and persist across store reopen. A guarded mutation validates the current unexpired owner/token immediately before publication; a stale owner never publishes even if it prepared bytes while its lease was live.

Exact byte/hash comparison is not atomic by itself. Every CDF writer of the same authority file must share one mutation guard from prior-authority validation through atomic install and directory sync. Both ordinary writes and compare-and-swap use:

1. exact expected authority or no-clobber creation precondition;
2. same-filesystem temporary bytes;
3. file sync;
4. final authority/fence validation;
5. atomic install;
6. parent-directory sync where supported;
7. deterministic temporary cleanup.

Advisory coordination cannot control non-cooperating editors or external processes. Capability reporting must state that boundary rather than claim universal filesystem CAS. Remote/distributed stores implement the same kernel lease contract with store-authoritative time and their own transactional publication primitive.
