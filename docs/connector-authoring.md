# Connector authoring and certification

CDF connectors are adapters around one shared runtime. Add source-specific contact, decoding, and
position semantics inside `cdf-source-<id>`, or destination-specific physical staging and commit
semantics inside `cdf-dest-<id>`. Enroll the adapter at the built-in catalog leaf and add one
data-driven conformance fixture. Do not add connector-name branches to generic orchestration.

## Shortest correct path

1. Add the connector leaf crate and its protocol-specific tests.
2. Add one construction row to `cdf-builtin-drivers` and update its catalog fixture.
3. Add one source fixture row and source shard, or one destination fixture row and chaos shard, to
   `cdf-conformance`.
4. Run the repository-owned certificate from the repository root:

   ```bash
   python3 tools/certify-connector.py --kind source --id <id> \
     --report target/quality/connector-<id>.json

   # or
   python3 tools/certify-connector.py --kind destination --id <id> \
     --report target/quality/connector-<id>.json
   ```

The command writes child logs to stderr and one JSON report to stdout and the optional report
path. It requires `cargo-nextest` and the same local Postgres availability as the conformance
matrix (`TEST_DATABASE_URL`, or local `initdb`/`pg_ctl`). It configures the repository's downloaded
DuckDB linkage when that artifact already exists.

The connector-only profile runs formatting, identity-named fixture laws, ordinary conformance,
the selected source or destination matrix slice, and the applicable extension graph, product,
and crash/recovery laws. A successful report proves only those named laws at the report's Git
merge base and HEAD.

## Changed-file budget

The connector-only profile accepts these surfaces:

- `crates/cdf-source-<id>/**` or `crates/cdf-dest-<id>/**`;
- root dependency manifests and lockfile;
- the `cdf-builtin-drivers` manifest, construction leaf, and catalog fixture;
- the `cdf-conformance` manifest, matching connector fixture, and the source/destination catalog
  and shard manifests required for that direction;
- `docs/**`, `.10x/tickets/**`, and `.10x/evidence/**`.

Every other changed file is reported as generic core ownership. If a real connector exposes a core
gap, repair it explicitly and rerun with `--core-impact`. That acknowledgement is not a waiver: it
keeps every connector law and adds the broader engine/runtime/project/CLI regression profile plus
strict workspace all-feature Clippy.

```bash
python3 tools/certify-connector.py --kind source --id <id> --core-impact \
  --report target/quality/connector-<id>-core-impact.json
```

Use `--base <revision>` when `origin/main` is not the intended integration base. The classifier
always includes committed changes since the merge base, staged and unstaged changes, deletions,
and untracked files; there is no changed-file override.

## Lifecycle boundary

An adapter must not copy or locally reimplement any of these shared lifecycles:

- canonical package/segment identity or package replay;
- durable destination receipt verification;
- checkpoint proposal or commit ordering;
- scheduler, memory, blocking-lane, or jobs admission;
- generic retry, cancellation, duplicate, or crash recovery.

Declare capabilities truthfully and let the shared planner, runtime, package, settlement, and
conformance authorities consume them. Format parsing and physical source/destination operations
belong in the adapter; lifecycle ownership does not.

Nebula is the synthetic source authoring proof. Quasar is the synthetic destination authoring
proof. They remain test fixtures, not production connectors.
