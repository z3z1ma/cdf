Status: active
Created: 2026-07-21
Updated: 2026-07-21

# Byte-first canonical segmentation v3

## Context

The v2 32 MiB target was calibrated on the 19-column TLC schema. A real FQ12 Iceberg run with
3,513,266 rows and 2,052 mostly sparse columns produced 1,188 canonical IPC segments and a 2.73 GiB
package from 42 MiB of compressed source objects. Each roughly 2.6 MiB encoded segment repeated
about 1.5 MiB of schema framing. Fixed encoder, hashing, publication, manifest, and destination work
therefore dominated a policy that was intended to amortize those costs.

The operator also lacked a supported way to select a different deterministic policy. Editing an
engine constant is not a tuning model, and live pressure cannot change identity-bearing boundaries.
CDF has no deployed artifacts requiring v2 compatibility.

## Decision

Canonical segmentation v3:

- uses a 256 MiB logical target and maximum, with 4,194,304 target/maximum row backstops;
- retains the independently adaptive 8,192–65,536 row and 1–32 MiB microbatch envelope;
- records all eight row/byte segment and microbatch values in every plan and package;
- exposes those eight values as `plan`, `explain`, `preview`, `run`, and `backfill` CLI knobs;
- validates the complete relationship before source contact;
- uses `partition-segment-ordinal-v3`; and
- preserves partition-local deterministic assembly, typed position authority, jobs invariance, and
  the existing LZ4 Arrow IPC artifact format.

The default target equals the maximum deliberately. The maximum is a safety envelope rather than a
second performance target; operators who need a distinct ceiling can set one explicitly. The 256 MiB
default is large enough to amortize wide-schema framing while allowing at least fifteen canonical
segment workers inside the ordinary 4 GiB memory budget before other graph reservations.

## Alternatives Considered

- Keep 32/64 MiB and add knobs only: rejected because the real wide-schema run proves the default
  itself is pathological.
- Default to 512 MiB or 1 GiB: rejected for now because graph admission would reduce ordinary 4 GiB
  hosts to seven or three maximum-size segment workers. Both remain explicit tuning choices.
- Derive canonical boundaries from schema width at runtime: rejected because execution-time
  adaptation would make package identity depend on observations and scheduling. A future compiler
  may choose a recorded policy from plan-time evidence under a separately versioned decision.
- Change IPC compression/framing: rejected because segment granularity is independently fixable and
  artifact encoding requires its own roofline evidence.

## Consequences

Package hashes intentionally change. There is one current policy and no v2 reader shim. Wider
segments increase per-worker memory authority, so the graph scheduler admits against the recorded
maximum. Operators can tune every relevant boundary without changing code, and replay consumes the
recorded policy rather than current defaults.

This decision supersedes `.10x/decisions/superseded/byte-first-canonical-segmentation-v2.md`.
