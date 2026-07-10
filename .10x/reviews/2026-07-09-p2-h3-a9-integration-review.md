Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-h3-adhoc-parquet-run.md, .10x/tickets/done/2026-07-09-p2-ws-a9-local-arrow-ipc-discover-run.md
Verdict: pass

# P2 H3 and A9 integration review

## Findings

The first A9 review failed with one significant finding: normal runtime partition resolution hashed the entire IPC file before the ostensibly bounded schema probe, and multi-file rejection hashed every match. Minor gaps also existed around unsupported framing/compression and package/receipt/checkpoint/coercion proof. The repair introduced a discovery-only contained candidate enumerator, actual seekable-probe byte accounting exposed through generic discovery, expanded no-write failures, and direct evidence inspection. Independent re-review found no critical, significant, or minor issues.

The first H3 review failed with three significant findings: URL userinfo could be persisted/rendered, predictable synthesized ids could be shadowed by configured resources, and invalid local paths could leak secret-bearing path text. The repair rejects userinfo and id collisions before mutation, adds an ad-hoc-only redacted local diagnostic path while preserving `cdf add`, and tests all named zero-write cases. Independent re-review found no critical, significant, or minor issues.

## Assumptions tested

- Discovery boundedness was tested through the generic CLI path rather than inferred from a low-level reader.
- Runtime `FileManifest` exactness was checked after separating discovery enumeration from runtime hashing.
- H3 was traced into the ordinary package/receipt/checkpoint/ledger path; there is no ad-hoc commit bypass.
- Secret safety was challenged with URL userinfo, malformed credential URLs, signed query material, unsupported schemes, valid and invalid local secret-bearing paths, and generated-artifact scans.
- Synthesized-id authority was challenged with an exact predictable configured collision before any stage/config/package/destination/state write.
- Pinned Arrow drift was tested through both preview and run; lossless widening and serialized coercion evidence were tested separately.

## Verdict

Pass. H3 and A9 meet their bounded ticket contracts after repair. Their tickets may close with `.10x/evidence/2026-07-09-p2-h3-a9-integration.md`.

## Residual risk

H3's file-tree snapshot helper records files rather than empty directories, but explicit directory assertions and inspected pre-write control flow cover the security cases. Same-package ad-hoc resume remains intentionally unsupported pending durable destination binding or a resume destination override. A9 byte accounting measures logical reads performed by CDF rather than kernel read-ahead. A10 and I5 are distinct active follow-ups and do not weaken this verdict.
