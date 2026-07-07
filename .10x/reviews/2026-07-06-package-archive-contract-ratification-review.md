Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/specs/package-lifecycle-determinism.md
Verdict: pass

# Package archive contract ratification review

## Target

Review of the active package lifecycle spec update made for `.10x/tickets/done/2026-07-06-package-archive-contract-ratification.md`.

## Findings

No blocking findings.

The spec now resolves the previously blocked execution semantics: archive path naming, manifest metadata schema, identity/hash/signing participation, lifecycle status behavior, replay preference, rerun/idempotency, crash-safety, and CLI contract. The selected contract preserves D-4 by keeping Arrow IPC canonical and preserves D-10 by keeping the existing signing input tied to `manifest.identity`.

## Assumptions tested

The review checked the main wrong-premise risk: treating `PackageStatus::Archived` as the result of `cdf package archive`. Current source rejects archived packages from replay, while the book says replay prefers IPC when present. The spec avoids that conflict by making Parquet archive creation a status-preserving sidecar operation and reserving `archived` for retention/GC tombstones.

The review also checked the identity risk: adding archive files to `ManifestIdentity` would change package hash and receipt identity after the package already exists. The spec avoids that by placing `archives.parquet` outside identity while still requiring verification of present archive metadata.

## Verdict

Pass. The spec is concrete enough for the implementation child to proceed, and the residual work is owned by `.10x/tickets/done/2026-07-06-package-archive-persistence-cli.md`.

## Residual risk

The eventual implementation must handle old manifests without `archives`, canonical JSON stability for the new metadata, and atomic filesystem replacement across supported platforms. Those are implementation risks covered by the new child ticket's acceptance criteria and evidence expectations.
