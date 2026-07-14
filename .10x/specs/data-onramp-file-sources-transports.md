Status: active
Created: 2026-07-08
Updated: 2026-07-10

# Data onramp file sources and transports

## Purpose and scope

This specification governs P2 file-source planning, globs, `FileManifest` incrementality, compression, format detection, remote transports, and per-file schema variance. It refines `.10x/specs/resource-authoring-planning-batches.md`, `.10x/specs/checkpoint-state-commit-gate.md`, and `.10x/specs/project-cli-observability-security.md`.

## Behavior

A file resource MAY name a local path, local glob, remote URL, or remote prefix plus glob. Globs matching multiple files MUST plan file partitions rather than failing package-producing execution. Partition order MUST be deterministic.

Every file partition MUST carry source-position evidence sufficient to build or update a `FileManifest`: path or URL, byte size when known, checksum or stable ETag when known, and modification time when available. Source-position evidence MUST be recorded in package/checkpoint artifacts through the normal commit gate.

`FileManifest` incrementality is the default for append file resources. A repeated plan over unchanged files MUST be a no-op or a visibly empty run, not a duplicate load. Replace resources MAY opt out where the target semantics require reloading all matched files.

Compression MUST be transparent for gzip and zstd in auto mode. Detection uses extension plus magic bytes. Implementations MUST stream decompression and MUST NOT fully buffer compressed input when a streaming decoder is available.

Format detection MUST use extension plus magic-byte confirmation. Explicit format declarations override inference, and conflicts fail at plan time with a remedial error.

Remote transports MUST use one file facade for `file://` and implicit local files, `https://`, `s3://`, `gs://`, and `az://`. Secret references, egress allowlists, ranged reads, streaming reads, and spool decisions belong to the facade. HTTP(S) glob support is limited to ratified template/range enumeration and must not pretend arbitrary web servers support list operations.

P3 generation binding, streaming/paged I/O, connection/range control, spool/cache, and overlap mechanics MUST follow `.10x/specs/remote-local-io-overlap.md` without changing this source-selection/state contract.

Per-file schema variance MUST produce contract verdicts. In `evolve`, compatible file schemas union with recorded widenings. In `freeze`, incompatible files or rows quarantine with source position and remediation. Unclassified mixed-schema crashes are not acceptable P2 behavior.

Multi-file file resources MUST be discoverable and pinnable as one resource. Discovery MUST retain deterministic per-file content/schema provenance and MUST NOT reject a resource merely because more than one file matched. A single-file fast path MAY optimize the one-entry case only if it is the same aggregation abstraction and produces equivalent evidence.

File discovery budgets are per executor. The default executor permits 64 MiB of metadata for one file, 128 MiB total in-flight metadata, and 8 concurrent probes. An embedding runtime MAY override those values explicitly; the resolved budget MUST be plan/package evidence and MUST affect only resource scheduling or explicit limit failure, never file eligibility, schema semantics, or activation/change of sampling. Independent file/within-file coverage and explicit sampling are governed by `.10x/specs/schema-discovery-and-stream-admission.md`.

The candidate, probe, manifest, and verdict model MUST be transport- and executor-neutral. Local, Azure/object-store, HTTP, Python, WASM, and future distributed integrations adapt into the same facts. No correctness rule may depend on the CLI process or one transport implementation.

## Acceptance criteria

- A six-month public HTTPS Parquet glob plans one partition per month and records manifest state after run.
- A seventh matching file is the only file planned on the next run; no-change reruns are fast no-ops.
- Gzip NDJSON and zstd inputs decode without manual preprocessing.
- HTTP ranged Parquet discovery reads the footer/schema without downloading data pages.
- `s3://`, `gs://`, `az://`, local, and HTTPS sources share conformance coverage at the facade layer.

## Explicit exclusions

Zip archive member semantics, unbounded file tailing, and distributed scheduling are out of scope for P2 unless later tickets explicitly ratify them.

Logical file partitions MUST remain stable under executor work packing. An executor MAY schedule multiple logical partitions together, but MUST preserve their individual partition ids, schema-observation bindings, source positions, retry outcomes, and deterministic segment assignment. Zip implementation remains gated by `.10x/decisions/logical-file-partitions-executor-packing-and-zip-trigger.md`.
