# Quickstart

This quickstart starts from a clean checkout, builds the current `cdf` binary,
creates a local scaffold, runs one file resource into DuckDB, inspects system
history, freezes the contract, replays the package from a clean replay ledger,
and then points at the conformance-owned proof for crash/resume and drift
quarantine.

## Prerequisites

- A checkout of this repository.
- The pinned Rust toolchain available through `cargo`.
- Local commands run from the repository root.

Generated command reference pages are not available yet; WS6B owns them:
[`2026-07-08-p1-product-ws6b-generated-reference-freshness.md`](../.10x/tickets/2026-07-08-p1-product-ws6b-generated-reference-freshness.md).
The command snippets below were checked against the current parser/help surface.

## Build the CLI

```bash
cargo build -p cdf-cli --locked
export CDF="$PWD/target/debug/cdf"
```

Expected:

```text
Finished `dev` profile ...
```

## Create a local project

```bash
WORKDIR="$(mktemp -d)"
"$CDF" init "$WORKDIR" --name docs_quickstart
printf '%s\n' \
  '{"id":1,"updated_at":1}' \
  '{"id":2,"updated_at":2}' \
  > "$WORKDIR/data/events.ndjson"
```

Expected output is abbreviated because the temporary path differs:

```text
initialized CDF project docs_quickstart at ...: created cdf.toml, README.md, resources, resources/files.toml, data; replaced none; skipped none
```

`cdf init` currently creates `README.md`, `cdf.toml`, `resources/files.toml`,
and `data/`. It does not create `.cdf/`, packages, checkpoints, destination
files, lockfiles, or data files.

## Validate

```bash
"$CDF" --project "$WORKDIR" validate
```

Expected:

```text
validated project docs_quickstart env dev: 1 declarative resource(s), 0 external resource(s), 0 secret reference(s)
```

## Plan

`cdf plan local.events` can derive a default target from the resource id. This
quickstart passes an explicit target so the later replay output is stable.

```bash
"$CDF" --project "$WORKDIR" plan local.events --target local_events
```

Expected:

```text
plan local.events to local_events: 1 partition(s), 0 pushed predicate(s), 0 inexact, 0 unsupported, 1 migration preview item(s), guarantee effectively_once_per_package
```

Planning does not write package bytes. The plan output is intentionally shorter
than the final generated reference and renderer work planned under WS3/WS6B.

## Run

`cdf run local.events` can derive a default pipeline, target, package id, and
checkpoint id. This quickstart pins them so the package path and state commands
are deterministic.

```bash
"$CDF" --project "$WORKDIR" run \
  --resource local.events \
  --pipeline local.events \
  --target local_events \
  --package-id quickstart-001 \
  --checkpoint-id quickstart-cp-001
```

Expected output includes a generated run id and package hash:

```text
ran resource local.events as run run-... into package sha256:... for target local_events; checkpoint quickstart-cp-001 committed after destination receipt verification, crossing the commit gate
```

This command creates local runtime artifacts under `$WORKDIR/.cdf/`.

## Query System History

`cdf sql` currently queries CDF local system history mounted from packages and
the SQLite checkpoint store. It is not a direct SQL prompt into the destination
table.

```bash
"$CDF" --project "$WORKDIR" sql \
  'select package_id, status, segment_count, receipt_count from packages order by package_id'

"$CDF" --project "$WORKDIR" sql \
  'select checkpoint_id, status, is_head from checkpoints order by sequence'
```

Expected:

```text
sql returned 1 row(s) from local system history
sql returned 1 row(s) from local system history
```

## Inspect Package and State

```bash
"$CDF" --project "$WORKDIR" inspect package "$WORKDIR/.cdf/packages/quickstart-001"
"$CDF" --project "$WORKDIR" state history --pipeline local.events --resource local.events
```

Expected:

```text
package sha256:... status checkpointed
1 checkpoint(s)
```

## Freeze and Test the Contract

```bash
"$CDF" --project "$WORKDIR" contract freeze local.events
"$CDF" --project "$WORKDIR" contract test local.events
```

Expected:

```text
froze 1 contract snapshot(s) in cdf.lock
contract test: 1 passed, 0 drifted
```

`contract freeze` and `contract test` prove schema/policy/program drift against
`cdf.lock`; they do not themselves execute row fixtures or write quarantine
artifacts. Drift quarantine is implemented and verified through conformance, as
shown below.

## Replay From a Clean Ledger

For this quickstart, replay the package from a second clean project/ledger into
a second local DuckDB database.

```bash
REPLAY_WORKDIR="$(mktemp -d)"
"$CDF" init "$REPLAY_WORKDIR" --name docs_quickstart_replay

"$CDF" --project "$REPLAY_WORKDIR" replay package \
  "$WORKDIR/.cdf/packages/quickstart-001" \
  --to duckdb://.cdf/replay.duckdb

"$CDF" --project "$REPLAY_WORKDIR" state history \
  --pipeline local.events \
  --resource local.events
```

Expected:

```text
initialized CDF project docs_quickstart_replay at ...
replayed package sha256:... into destination duckdb target local_events; receipt ... duplicate=false no_op=false; checkpoint quickstart-cp-001 status committed; package status checkpointed
1 checkpoint(s)
```

Current same-ledger replay of this exact package would collide on the checkpoint
id. The conformance MVP harness proves duplicate/no-op destination replay using
the lower artifact replay API where checkpoint id reuse does not obscure the
idempotency assertion.

## Crash/Resume and Drift Quarantine Proof

The public CLI does not expose a test-only crash flag. The current deterministic
proof for crash/resume, package replay, duplicate replay, and drift quarantine is
the conformance-owned MVP fixture:

```bash
cargo test -p cdf-conformance mvp_acceptance_demo --locked
```

Expected:

```text
test mvp_acceptance_demo::mvp_acceptance_demo_fixture_proves_rest_duckdb_recovery_replay_and_drift ... ok
```

That fixture uses a GitHub-Issues-shaped REST resource without live network
dependency, simulates a crash after destination receipt verification and before
checkpoint commit, runs `cdf resume` without new source contact, replays into a
second DuckDB database, and composes the DuckDB drift-quarantine proof. The
public runnable example projects are owned by
[WS6C](../.10x/tickets/2026-07-08-p1-product-ws6c-runnable-examples-conformance.md).

## Clean Up

```bash
rm -rf "$WORKDIR" "$REPLAY_WORKDIR"
```
