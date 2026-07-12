Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-a12-byte-first-segments-shared-arrow-accounting.md, .10x/tickets/2026-07-11-p3-b2-parquet-codec.md
Verdict: pass

# P3 Parquet stream and byte-first segment review

## Target

Streaming Parquet publication, generic format-stream routing, canonical segmentation v2, mandatory plan policy, shared Arrow allocation accounting, and CLI phase telemetry in the current worktree.

## Findings

No critical or significant findings remain.

The initial implementation placed a Parquet branch in `cdf-source-files`; review rejected that extension smell. The repair introduced one format-owned stream entry point and deleted the source-owned Parquet execution branch and the helper test that encoded it. New streaming formats can replace their collected implementation inside `cdf-formats` without adding source/runtime wiring.

The 64-MiB target experiment correctly failed because retained memory exceeded the currently declared 64-MiB verified stream window. V2 therefore uses a 32-MiB target and 64-MiB maximum, preserving headroom rather than weakening admission. Shared-buffer accounting uses each Arrow buffer's base allocation pointer and maximum observed capacity/extent, recurses through null and child buffers, and adds container overhead. The permanent shared-array test catches the prior double count.

Removing missing-policy and write-disposition defaults intentionally rejects pre-v2 serialized plans. This matches the user's explicit no-compatibility authority and leaves no v1 constructor or namespace in production source. LZ4 was retained because removing it did not improve the measured end-to-end path.

## Verdict

Pass. The changes improve throughput and bounded-memory correctness while strengthening the source/format extension boundary.

## Residual risk

Sequential LZ4 encode and per-segment durability remain measurable costs, but they are explicitly owned by E4/C2 and are not hidden as closure. HTTP open/download and codec decode need separate permanent phase attribution under G3. These are existing active owners, not untracked residual work.
