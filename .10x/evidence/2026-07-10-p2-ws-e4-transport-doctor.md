Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-ws-e4-transport-doctor-probes.md, .10x/tickets/done/2026-07-08-p2-ws-e-remote-transports.md

# P2 WS-E4 transport doctor evidence

## What was observed

`cdf doctor` emits one `file_transport:<resource-id>` check per configured remote file resource. The check invokes ordinary partition resolution, therefore exercising credential/provider construction, egress, listing or finite HTTP enumeration, metadata identity, and format confirmation. Failure remains isolated to the check, makes doctor nonzero, and creates no state, package, or destination artifacts. Details contain only resource id, transport kind, and match count.

## Procedure

- `cargo test -p cdf-cli doctor_remote_transport_probe -- --nocapture` — passed.
- `cargo test -p cdf-cli doctor_skips_duckdb_drift -- --nocapture` — passed.
- `cargo test -p cdf-cli` — 260 unit tests, 1 environment integration test, and doc tests passed.
- `cargo clippy -p cdf-cli --all-targets -- -D warnings` — passed before the final full-suite run; the final changes were formatting-clean and recompiled by the suite.
- Targeted exhaustive-evolve and sampled-quarantine regression tests both passed after repair.

## What this supports

Doctor and execution cannot drift into separate transport semantics. The command remains read-only and its remote failures are actionable without leaking source URLs or credentials into structured details.

## Limits

CI exercises denial and provider-neutral fixture success in the transport crates. Real provider reachability remains a nightly WS-I concern.
