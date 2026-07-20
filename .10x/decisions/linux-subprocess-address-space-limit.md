Status: active
Created: 2026-07-18
Updated: 2026-07-18

# Linux subprocess address-space limit

## Context

The foreign-stream contract needs an enforceable configured child sub-budget, while the process-tree constant-memory decision reserves aggregate enforcement for Linux cgroups. A live macOS probe falsified the prior assumption that `RLIMIT_AS` is a portable Unix authority: `setrlimit` returned `EINVAL` despite exposing the resource constant.

## Decision

`cdf-subprocess` accepts an optional `maximum_child_address_space_bytes` only on Linux. Immediately before `exec`, it sets the child's soft and hard `RLIMIT_AS` to the minimum of the configured limit and any inherited finite soft or hard limit. CDF therefore never weakens a stricter administrator/parent boundary. Descendants inherit the per-process fence. The neutral descriptor records the configured ceiling as `child_process_bytes`; the inherited effective value may be stricter.

The implementation uses one narrowly scoped `CommandExt::pre_exec` block. The closure captures only a copied `u64`, reads one rlimit, and calls rustix's allocation-free `setrlimit`; it allocates nothing, takes no lock, emits no diagnostic, and observes no shared Rust state after `fork`. Linux conformance launches a real shell that reports its inherited limit; a focused policy test proves a stricter inherited soft limit cannot be raised. Supervision boundary tests cover zero and infinity sentinels. Aggregate process-tree memory remains cgroup-owned and is never inferred from this per-process limit.

Non-Linux hosts reject a configured child address-space limit before spawn. An absent limit remains honest as `child_process_bytes = None`; macOS measurement evidence belongs to WS-F and does not masquerade as enforcement.

## Alternatives considered

- Parent-side `prlimit`: rejected because it races child execution.
- Shell or external `prlimit` wrapper: rejected because it introduces parsing, injection, and host-tool dependencies.
- Claim the fence on macOS because the constant compiles: rejected by executable evidence.
- Treat per-process address space as aggregate RSS: rejected because descendants receive independent ceilings and virtual address space is not resident memory.

## Consequences

Linux gets a zero-hot-path-cost inherited safety fence plus the stronger aggregate cgroup authority. Other platforms fail a requested enforcement policy rather than weakening it silently. Very small limits may prevent dynamic program startup and surface as an ordinary redacted spawn/exit failure.
