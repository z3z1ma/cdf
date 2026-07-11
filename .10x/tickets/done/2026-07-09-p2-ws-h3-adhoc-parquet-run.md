Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-h-scaffolding-id-model-two-minute-path.md
Depends-On: .10x/decisions/data-onramp-source-identity-preview-disposition.md, .10x/specs/data-onramp-source-experience-cli.md, .10x/tickets/done/2026-07-09-p2-ws-h2-cdf-add-single-file-parquet.md, .10x/tickets/done/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md, .10x/tickets/done/2026-07-09-p2-ws-a8-autopin-lockfile-no-pin.md

# P2 WS-H3 evidence-preserving ad-hoc Parquet run

## Scope

Implement the first ad-hoc execution slice for a single local or stable public HTTPS Parquet file:

`cdf run <url-or-path> --to <destination>`

The command must synthesize and persist a real declarative resource under `.cdf/adhoc/`, discover and pin its schema, compile a normal plan, produce and verify a package/destination receipt/checkpoint through the ordinary run spine, and render the equivalent `cdf add` command for making the resource permanent.

Reuse the H2 target classification, URL redaction, bounded HTTP Parquet discovery, and generated resource TOML. Ad-hoc mode is front-end sugar only; it must not introduce an execution bypass.

## Acceptance criteria

- `cdf run <local.parquet> --to duckdb://...` and the deterministic HTTPS equivalent succeed without a pre-existing resource file.
- A stable synthetic resource id and TOML are persisted under `.cdf/adhoc/`; rerunning the same canonical input reuses the same identity rather than accumulating random resources.
- Discovery writes a deterministic schema snapshot and semantic lock reference, and the package/checkpoint use that pinned schema hash.
- The ordinary plan/package/receipt verification/checkpoint gate and run-ledger events are used; no ad-hoc-only commit path exists.
- Human and JSON output identify the synthesized resource/artifact paths and render a secret-safe `cdf add <id> <url-or-path>` command.
- Explicit `--to` remains required for ad-hoc mode and is not confused with an existing compiled resource id.
- Signed URL query/fragment material and local path secrets are not persisted or rendered; unsupported/sensitive locations fail before `.cdf/adhoc/` mutation.
- Failure/retry leaves normal recoverable package/run-ledger evidence and never advances a checkpoint without verified receipt.

## Evidence expectations

CLI tests for local and deterministic HTTPS fixtures, stable identity/rerun, signed-URL redaction/no-write, package/receipt/checkpoint inspection, run-ledger events, recovery semantics, human/JSON rendering, and workspace quality gates.

## Explicit exclusions

This ticket does not add CSV/JSON/NDJSON/Arrow/Postgres/REST ad-hoc execution, infer remote credentials, support HTTP globs/templates, change `cdf add`, or make `--to` optional. Broader ad-hoc source coverage remains WS-H scope after the corresponding discovery/transport shapes stabilize.

## Progress and notes

- 2026-07-09: Opened after H2, E2, and A8 stabilized the local/HTTPS single-Parquet generation, bounded discovery, pinning, and lockfile shapes that ad-hoc mode can reuse without a parallel pipeline.
- 2026-07-09: Implemented `cdf run <local-or-stable-http(s)-parquet> --to <destination>` as front-end synthesis only. The command derives a stable `adhoc.parquet_<sha256-prefix>` id from the canonical input, writes one deterministic `.cdf/adhoc/<resource>.toml`, compiles that declarative resource into the current run context, hydrates an existing locked snapshot on rerun, and then enters the unchanged discovery/plan/package/destination/receipt/checkpoint/run-ledger spine. No ad-hoc commit or checkpoint path was added.
- 2026-07-09: Local inputs are hard-linked when possible (copied as a portable fallback) to a hashed `.cdf/adhoc/data/<resource>.parquet` artifact before resource persistence. The original canonical path is used only as an in-memory identity preimage, so path secret sentinels do not enter TOML, lockfile, schema/package/state/destination artifacts, JSON, human output, or the rendered `cdf add` command. Stable HTTP(S) inputs reuse H2 URL classification, egress allowlisting, signed query/fragment rejection, bounded footer discovery, and range-backed execution. Missing `--to`, signed URLs, and unsupported schemes fail before `.cdf/adhoc` mutation.
- 2026-07-09: Added additive human/JSON `adhoc` reporting for the synthesized id, config path, optional safe staged-source path, reuse status, and executable `cdf add <id> <safe-url-or-staged-path>` command. Repeating the same canonical local input reuses the id/config/snapshot instead of accumulating resources. The pinned schema hash is asserted across lockfile, package receipt, verified DuckDB receipt, and committed checkpoint; run-ledger success and receipt events are asserted directly.
- 2026-07-09: Added failure/retry evidence through the ordinary spine. A DuckDB uniqueness failure leaves an integrity-verifiable package, `PackageFinalized` then terminal `RunFailed` ledger evidence, no destination-receipt/checkpoint events, no package receipt, and no checkpoint head. Removing the external blocker and rerunning the same input reuses the stable ad-hoc identity and commits through the normal verified-receipt gate.
- 2026-07-09: Focused verification passed `cargo test -p cdf-cli run_adhoc_ --locked -- --nocapture` (4/4), including local/rerun/human+JSON, deterministic HTTP, no-write security rejections, and destination failure/retry. The existing command-error precedence was restored after integration caught project loading ahead of missing-resource validation; `migrated_command_family_errors_include_code_and_remediation` passes. Full verification passed `cargo test -p cdf-cli --lib --locked --no-fail-fast` (236/236), `cargo clippy -p cdf-cli --all-targets --locked --no-deps -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check`.
- 2026-07-09: Parent adversarial review failed the first implementation on three boundary cases despite the ordinary execution spine remaining intact: HTTP(S) URL userinfo could be persisted/rendered; a preconfigured resource with the predictable synthesized id could shadow the generated resource and receive writes; and invalid local-path errors could render path secrets before staging. Closure is blocked pending fail-closed, pre-mutation collision/userinfo checks and secret-safe invalid-path diagnostics with targeted no-write regressions. The review also noted that the current retry proof uses a new package rather than resuming the failed `Loading` package; repair must either exercise the supported normal resume path or record that exact exclusion without overstating recovery.
- 2026-07-09: Repaired the secret boundary before mutation. HTTP(S) locations with any username or password now fail before `.cdf/adhoc` creation, URL redaction removes userinfo as well as query/fragment material, and malformed URLs fall back to a fully redacted label rather than the original input. Ad-hoc local-path classification now uses a dedicated redacted diagnostic mode for missing files, directories, wrong extensions, canonicalization failures, non-UTF-8 paths, and project-relative conversion; `cdf add` retains its existing local-path diagnostics.
- 2026-07-09: Repaired synthesized-id authority. Immediately after deriving `adhoc.parquet_<sha256-prefix>` and before staging or config writes, ad-hoc synthesis now rejects any already compiled resource with that id. The run can therefore no longer append a duplicate and then select the pre-existing first match while reporting the generated artifact. A malicious/preconfigured exact-id regression proves the project tree, ad-hoc directory, package root, destination, and checkpoint state remain unchanged.
- 2026-07-09: Added no-write security regressions for valid URL userinfo, malformed credential-bearing URLs, missing secret-bearing local paths, wrong-extension secret-bearing files, directory-shaped secret-bearing paths, and the preconfigured synthetic-id collision. Focused H3 verification now passes 6/6; the migrated command-precedence regression passes; and the existing `cdf add` focused tests pass 4/4.
- 2026-07-09: Same-package `cdf resume` is explicitly not claimed by this slice. H3 requires an explicit ad-hoc `--to`, while the current resume command accepts no destination override and resolves only the selected environment destination; replaying the failed package through resume could therefore target a different destination. The tested retry intentionally creates a fresh package after removing the blocker. The failed `Loading` package remains integrity-verifiable and generically replayable, but same-package ad-hoc resume requires a separately ratified durable destination binding or an explicit resume destination override plus conformance coverage.
- 2026-07-09: Final repair verification passed `cargo nextest run -p cdf-cli --locked --no-fail-fast` (240/240), `cargo test -p cdf-cli --lib --locked run_adhoc_ -- --nocapture` (6/6), `cargo test -p cdf-cli --lib --locked add_ -- --nocapture` (4/4), and the migrated command-error precedence regression (1/1). Quality gates passed `cargo fmt --all -- --check`, `git diff --check`, and `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`. The ticket remains open for independent adversarial re-review; no implementation blocker remains for its bounded scope.
- 2026-07-09: Independent re-review passed with no findings after inspecting the repaired secret/id boundaries, ordinary evidence spine, and explicit fresh-package retry limit. Parent integration evidence is `.10x/evidence/2026-07-09-p2-h3-a9-integration.md`; review is `.10x/reviews/2026-07-09-p2-h3-a9-integration-review.md`. This bounded H3 slice is complete.

## Blockers

None for the bounded local/stable-HTTPS Parquet slice.
