Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-h2-cdf-add-single-file-parquet.md
Verdict: pass

# P2 WS-H2 cdf add single-file Parquet review

## Target

Review of the H2 implementation and evidence for scoped `cdf add` support over single-file local and deterministic HTTPS Parquet resources.

## Findings

- Pass: The command is intentionally narrow and fails closed outside single-file Parquet, which matches H2 exclusions and avoids encoding unfinished multi-file/cloud semantics into scaffolding.
- Pass: Dry-run behavior is tested as no-write, and non-dry-run writes the expected project config, schema snapshot, and lockfile path rather than bypassing the plan/package evidence model.
- Pass: Append remains keyless and the tests guard against fake primary-key scaffolding.
- Pass: Signed/query URLs are rejected without leaking query content, preserving the renderer/redaction guardrail for the new source doorway.
- Pass: Generated completions/help/man artifacts were refreshed and freshness-tested after the new command changed the clap surface.

## Residual risk

H2 is not the complete P2 happy path. Public TLC S1/S2, HTTP glob/template enumeration, cloud transports, multi-file manifest behavior over remotes, REST/Postgres `cdf add`, ad-hoc mode, and the recorded terminal session remain owned by active P2 workstreams.

## Verdict

Pass. The H2 ticket is supported by focused tests, generated-artifact freshness, broad pre-repair workspace gates, and explicit residual ownership for the remaining P2 scope.
