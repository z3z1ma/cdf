Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/cancelled/2026-07-11-p3-b4-delimited-fixed-width-codecs.md

# Physical decode schema authority

## What was observed

`PhysicalDecodeRequest` no longer accepts an isolated observed-schema hash. It carries the exact planned physical Arrow `SchemaRef`. Parquet and Arrow IPC derive the canonical expected hash from that schema for source attestation and batch headers, eliminating the possibility that a caller supplies mutually inconsistent schema and hash authorities.

`FileResource` retains `EffectiveSchemaRuntime` behind `Arc` and resolves each partition's physical schema hash through its validated schema catalog. `PhysicalSchemaAuthority` keeps the reference and hash cohesive through the file runtime. If no catalog schema exists, the registered driver performs bounded discovery and must match any planned partition hash before decode.

## Procedure

- `cargo test -p cdf-format-parquet -p cdf-format-arrow-ipc -p cdf-source-files --locked`
  - Result: Parquet driver, Arrow IPC driver, 17 file-source tests, and doc tests passed; one release-only performance test remained intentionally ignored.
- `cargo clippy -p cdf-runtime -p cdf-format-parquet -p cdf-format-arrow-ipc -p cdf-source-files --all-targets --locked -- -D warnings`
  - Result: passed after replacing the proliferating hash/schema argument pair with one cohesive authority value.
- `git diff --check`
  - Result: passed before commit.

## What this supports or challenges

This supports FX1's neutral format-driver contract and B4's pinned-schema execution requirement. Row codecs can now consume the plan's exact physical schema without loading project snapshot files or re-running inference inside codec crates.

## Limits

No CSV/fixed-width driver is included in this slice. The no-catalog bounded-discovery fallback remains necessary for direct driver/test use and must match a planned hash when one exists.
