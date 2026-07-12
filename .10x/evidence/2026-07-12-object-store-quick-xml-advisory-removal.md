Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/done/2026-07-12-p0-object-store-quick-xml-advisory.md, .10x/decisions/object-store-security-pin.md

# Object-store quick-xml advisory removal

## What was observed

All four CDF-owned `object_store` declarations resolve the exact Apache revision `c7316d29face118e7409eead0cda098f38589428` (`object_store 0.14.1`). The locked cloud XML parser is `quick-xml 0.41.0`. No first-party manifest declares object-store 0.13.2 or Reqwest 0.12. DataFusion retains one featureless transitive object-store 0.13.2 edge; it does not resolve quick-xml.

## Procedure and results

- `cargo check -p cdf-source-files -p cdf-dest-parquet -p cdf-project -p cdf-declarative --all-targets --locked`: passed.
- `cargo check -p cdf-cli --all-targets --locked`: passed after aligning the direct CLI client to Reqwest 0.13.4.
- `cargo test -p cdf-source-files -p cdf-dest-parquet -p cdf-declarative --locked`: passed (126 tests passed, one release benchmark ignored, no failures).
- `cargo test -p cdf-source-files --locked object_store_provider_urls_build_through_the_shared_parser -- --nocapture`: passed for `s3://`, `gs://`, and `az://` provider construction. The Azure case explicitly supplies its required account-name option.
- `cargo clippy -p cdf-source-files -p cdf-dest-parquet -p cdf-declarative -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- `cargo deny --locked check`: `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo vet --locked --no-minimize-exemptions`: succeeded under the repository's exemption-based policy (2 fully audited, 486 exempted).
- `cargo tree -i quick-xml --locked`: reported only `quick-xml 0.41.0 -> object_store 0.14.1 -> CDF-owned consumers`.
- `rg 'object_store = .*0\\.13\\.2|reqwest = .*0\\.12' --glob Cargo.toml`: no first-party declarations.

## What this supports

RUSTSEC-2026-0195 is absent from the locked graph, every CDF-owned provider uses one fixed pin, provider URL configuration remains available for all three cloud schemes, and the affected dependency cone is warning-clean.

## Limits

The provider-construction test is hermetic and does not authenticate to live S3/GCS/Azure services. Cargo Vet success records explicit trust exemptions; it is not a claim that every transitive dependency received a line-by-line audit. DataFusion's isolated featureless object-store 0.13.2 package remains until its upstream tuple advances.
