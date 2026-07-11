Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md
Verdict: pass

# P3 A2 architecture review

## Target

The neutral memory contract, default DataFusion adapter, discovery scheduler migration, working-set declarations, and budget policy.

## Findings

### Resolved — significant: attempt to inject an unknown sub-cap could wait forever

The first coordinator treated an undeclared borrowing tag as zero available bytes, which could create an unwakeable future. Deterministic and DataFusion coordinators now reject undeclared sub-caps immediately with a contract error.

### Resolved — significant: shared totals did not attribute DataFusion consumers

The initial adapter correctly shared byte authority but only named CDF consumers. Its coordinated `MemoryPool` now observes DataFusion grow/shrink, records typed query-engine current/peak facts, and wakes waiters on release.

### No concern: dependency direction

`cdf-memory` contains no executor or product dependency. DataFusion adaptation lives exclusively in `cdf-engine`; project discovery consumes only the neutral coordinator. The source/destination extension profile is implementation-neutral.

## Verdict

Pass. The implementation establishes one finite authority without leaking DataFusion, makes weighted limits executable rather than decorative, and preserves deterministic output order.

## Residual risk

Ledger balance does not prove RSS completeness. Production operator conversion and the 100 GiB/1 TiB stress laws remain explicitly owned by A5/A6/WS-F and are not claimed by A2.
