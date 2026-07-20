Status: superseded
Created: 2026-07-18
Updated: 2026-07-18

# Inherited subprocess address-space limit

## Context

The foreign-stream contract requires a subprocess host to declare and enforce a configured child-memory sub-budget. CDF's managed ledger cannot account allocations made inside an arbitrary executable. The process-tree constant-memory decision assigns aggregate Linux enforcement and measurement to a run cgroup, but H3 still needs a portable inherited fence for each child and its descendants. Tokio and the standard library expose no safe command-builder API for setting an `rlimit` between `fork` and `exec`.

## Decision

`cdf-subprocess` accepts an optional `maximum_child_address_space_bytes`. On supported Unix hosts it installs `RLIMIT_AS` immediately before `exec`; descendants inherit the limit. The neutral foreign-memory descriptor records the same configured upper bound as `child_process_bytes`. This is explicitly a per-process virtual-address-space fence, not aggregate process-tree RSS proof. Linux cgroup `memory.max` remains the aggregate authority; macOS remains measurement-only when this optional fence is absent.

The implementation uses one narrowly scoped `CommandExt::pre_exec` block. Its closure captures only a copied `u64`, reads the existing hard limit, clamps rather than raises it, and calls rustix's allocation-free `setrlimit` wrapper. It performs no lock acquisition, heap allocation, logging, or access to shared Rust state after `fork`. A real child reports its inherited limit in conformance; boundary validation rejects zero and unrepresentable limits. The existing adversarial/property suite covers arbitrary supervision boundaries without invoking undefined post-fork behavior.

## Alternatives considered

- Rely only on the global cgroup: rejected because standalone subprocess use would declare no child fence and non-Linux hosts cannot join that authority.
- Apply `prlimit` from the parent after spawn: rejected because the child can execute and allocate before the parent wins the race.
- Wrap every command in a shell or external `prlimit` binary: rejected because it adds quoting/injection risk, platform/tooling dependencies, and violates the no-shell-parsing boundary.
- Treat `RLIMIT_AS` as aggregate RSS: rejected as false. Multiple descendants each inherit the same per-process ceiling; aggregate enforcement remains cgroup-owned.

## Consequences

The configured fence is visible in the neutral capability contract and inherited without per-batch overhead. Very small address-space limits can prevent the dynamic loader or runtime from starting; that is a clean child failure with the ordinary redacted diagnostic path. OpenBSD cannot express `RLIMIT_AS` through the pinned portable API and rejects a configured fence before spawn. Any future Windows subprocess host needs an equivalent Job Object memory authority before it can claim this capability.

## Supersession reason

An immediate macOS execution probe returned `EINVAL` from `setrlimit(RLIMIT_AS)` even though the constant is exposed. The supposed portable-Unix fence was therefore false. `.10x/decisions/linux-subprocess-address-space-limit.md` replaces this decision with Linux-only enforcement and explicit rejection elsewhere.
