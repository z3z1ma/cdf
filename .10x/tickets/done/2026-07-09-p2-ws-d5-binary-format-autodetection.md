Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md
Depends-On: .10x/decisions/data-onramp-file-source-transport-manifest.md, .10x/specs/data-onramp-file-sources-transports.md, .10x/tickets/done/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md, .10x/tickets/done/2026-07-09-p2-ws-a9-local-arrow-ipc-discover-run.md

# P2 WS-D5 binary file format auto-detection

## Scope

Make `format` optional for the two file formats with deterministic existing extension and magic evidence:

- Parquet: `.parquet`, confirmed by the Parquet file magic expected by the existing footer/ranged reader.
- Arrow IPC file framing: `.arrow`, confirmed by Arrow IPC file magic.

Inference must feed the same compiled `FileResourcePlan` and preview/run/discovery front end as an explicit declaration. Explicit format selects the decoder but does not suppress safety confirmation: an explicit declaration that conflicts with extension or magic fails before extraction/package/destination/checkpoint writes and names every observed signal.

Local single files and local globs are in scope. Direct single-file HTTPS Parquet is in scope through the E2 ranged transport. Compression suffix stripping is in scope only where an already-supported binary format can be confirmed after the existing decompression/range boundary; this ticket must not add compressed Parquet or Arrow IPC support that D4/A9 explicitly excludes.

## Acceptance criteria

- A file resource whose `format` is omitted compiles when its selected path/glob has the record-backed `.parquet` or `.arrow` extension.
- Plan/deep-validate resolves matched files and confirms magic before preview or package execution; every file in a modest-N local glob must agree.
- Missing format with unknown, extensionless, mixed-extension, or ambiguous globs fails with a concrete `format = ...` remediation.
- Extension/magic mismatch and explicit-format/extension/magic mismatch errors name the resource, file, declared/inferred format, extension signal, and magic signal.
- Local Parquet and Arrow IPC plan/preview/run behavior is identical with explicit and inferred format, including schema discovery/pinning where supported.
- Direct HTTPS Parquet uses bounded ranged reads for confirmation and does not download the full object during planning.
- Preview and run consume the single resolved format decision; neither re-infers differently.
- Declarative JSON Schema reflects optional format and existing explicit declarations remain backward compatible.

## Evidence expectations

Declarative compile/runtime tests, local Parquet/Arrow IPC CLI parity, multi-file mismatch/no-write cases, deterministic HTTPS ranged fixture requests, deep-validate errors, JSON Schema freshness, and applicable parser/input/transport quality profiles.

## Explicit exclusions

CSV, JSON, NDJSON, Arrow IPC stream framing, `.ipc`/`.feather` aliases, compressed Parquet/IPC, HTTP template enumeration, and content-only inference without a recognized extension are excluded. Their signal/ambiguity contracts require separate records; this ticket must not guess them.

## Progress and notes

- 2026-07-09: Opened after source inspection confirmed `ResourceDeclaration.format` is already optional but compilation rejects `None`, local/HTTP runtime paths already inspect file bytes for compression/Parquet, and current CLI fixtures use `.parquet` plus `.arrow` as the canonical binary extensions.
- 2026-07-09: Implemented deterministic binary inference at declarative compilation. An omitted format resolves only when the resource glob ends in the ratified `.parquet` or `.arrow` extension; unknown, extensionless, text, and unratified alias globs fail with an explicit `format = "..."` remediation. The compiled file plan retains one concrete format plus whether it was declared, so discovery, deep validation, preview, and run cannot choose different decoders.
- 2026-07-09: Local partition resolution now confirms every matched binary file with extension and head/footer magic before producing partitions. Parquet requires `PAR1`; Arrow IPC file framing requires `ARROW1`; stream framing is identified and rejected explicitly. Partition evidence records the resolved format, declaration provenance, extension signal, and magic signal, and open revalidates that evidence. Explicit binary declarations remain authoritative when an extension signal is absent, but contradictory recognized extensions or magic fail with resource/file/all-signal diagnostics. Existing explicit text-format partition compatibility is unchanged.
- 2026-07-09: HTTP(S) single-file Parquet confirmation uses exactly two bounded six-byte range reads over the existing transport facade and never downloads the object for format confirmation. Compressed Parquet/IPC and remote Arrow IPC remain explicitly excluded. JSON Schema coverage confirms `ResourceDeclaration.format` remains optional.
- 2026-07-09: Product-path coverage now exercises omitted-format Parquet deep validation, omitted-format Arrow IPC discover/pin/show/diff/plan/preview/run, omitted-format Parquet auto-pin/package run, and deep-validation mismatch diagnostics with no schema/package/destination/checkpoint writes. Lower coverage proves modest-N glob confirmation, Parquet/Arrow preview-run parity, explicit/unknown-extension compatibility, explicit mismatch behavior, unknown/alias rejection, and bounded HTTPS requests.
- 2026-07-09: Verification passed: full `cdf-declarative` tests (87/87); full `cdf-project` tests (115/115); full `cdf-cli` nextest (241/241); the combined affected nextest reached 442/443 before a deterministic HTTP fixture exhausted its pre-D5 request cap, then that fixture was raised and passed both focused and in the full CLI rerun; affected all-target check and warnings-denied Clippy; workspace formatting; and `git diff --check`. The ticket remains open and unmoved for parent integration and independent adversarial review.
- 2026-07-09: Reviewer repair closed the declared-schema remote Arrow IPC gap at the shared HTTP partition-resolution seam. HTTP(S) format support is now checked before transport metadata or range access, so inferred or explicit Arrow IPC (and every other unsupported HTTP format) fails during plan/deep validation rather than deferring to preview/run; single-file HTTPS Parquet remains supported. A product regression proves both `cdf plan` and `cdf validate --deep` reject a declared-schema `https://.../events.arrow` resource without network-derived errors or any project/runtime writes, while the local Arrow lifecycle and bounded HTTPS Parquet tests remain green.
- 2026-07-09: Replaced schema-discovery-specific format mismatch prefixes with command-neutral `file format confirmation failed` wording valid in plan, deep validation, preview, and run. Framing details and all declared/inferred/extension/magic signals remain present. Repair verification passed focused remote-Arrow, malformed/stream Arrow, HTTPS Parquet, local Arrow lifecycle, and mismatch tests; full affected nextest passed 329/329; affected all-target check and warnings-denied Clippy, workspace formatting, and `git diff --check` passed. Ticket remains open and unmoved for parent re-review.
- 2026-07-09: Closed after independent re-review passed and final integration passed 809/809 workspace tests plus formatting, check, Clippy, docs, semver, dependency/security scans, and coverage. Evidence: `.10x/evidence/2026-07-09-p2-d5-i5-integration.md`. Review: `.10x/reviews/2026-07-09-p2-d5-i5-integration-review.md`.

## Blockers

None. A9 is closed and both binary formats meet this ticket's acceptance criteria.
