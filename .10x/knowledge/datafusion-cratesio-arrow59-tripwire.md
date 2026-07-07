Status: active
Created: 2026-07-07
Updated: 2026-07-07

# DataFusion crates.io Arrow 59 tripwire

CDF currently uses Apache DataFusion from git rev `7ff7278edc1bf7446303bff51e5883a38414bbdf` to keep the engine on an Arrow 59 tuple. That pin is temporary.

Run this tripwire weekly during dependency review until the git pin is removed.

## Source of truth

Use crates.io resolution, not GitHub branch state, as the publication trigger.

Recommended check:

```sh
cargo info datafusion --registry crates-io
tmpdir="$(mktemp -d)"
mkdir -p "$tmpdir/src"
: > "$tmpdir/src/lib.rs"
printf '[package]\nname = "cdf-datafusion-tripwire"\nversion = "0.0.0"\nedition = "2024"\n\n[dependencies]\ndatafusion = { version = "*", default-features = false }\n' > "$tmpdir/Cargo.toml"
cargo metadata --manifest-path "$tmpdir/Cargo.toml" --format-version 1 | jq -r '.packages[] | select(.name=="datafusion" or .name=="arrow-array" or .name=="parquet") | [.name,.version,(.source // "path")] | @tsv'
```

Expected current non-triggering shape on 2026-07-07: `cargo info datafusion --registry crates-io` reports crates.io `datafusion 54.0.0`, and the temporary manifest resolves `datafusion 54.0.0` with `arrow-array 58.3.0`. A crates.io release is a trigger only when the temporary manifest resolves `datafusion` from `registry+https://github.com/rust-lang/crates.io-index` and the resolved Arrow crates are on `59.x`.

## Trigger action

The day crates.io publishes a DataFusion release that resolves to Arrow `59.x`, open a migration ticket titled like `Replace DataFusion git pin with crates.io Arrow 59 release`.

The ticket should require:

- replacing the git dependency in `crates/cdf-engine/Cargo.toml` with the crates.io DataFusion release;
- updating `Cargo.lock`;
- removing `publish = false` from crate manifests only if the release ticket also proves no disallowed git/path dependency shape remains for the crates being published;
- removing `deny.toml` `allow-git = ["https://github.com/apache/datafusion.git"]` if no other active decision still needs it;
- updating `supply-chain/config.toml` only if cargo-vet reports new unknowns;
- proving the tuple with `cargo metadata --locked --format-version 1`, `cargo tree --workspace --locked -i datafusion@<version>`, and `cargo tree --workspace --locked -i arrow-array@59.x`;
- running focused engine/package/conformance/golden-package checks and supply-chain gates.

If the newest crates.io DataFusion release resolves to Arrow 60 or another non-59 major, do not migrate under this tripwire. Open a dependency tuple shaping ticket instead, because that is a new minor-tuple decision.
