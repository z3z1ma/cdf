# Release and Install

CDF publishes checksummed binary prereleases for mainstream macOS, Linux, and
Windows targets. The shell installer supports macOS and Linux and installs into
`$HOME/.local` by default without privilege escalation.

## Install the Current Prerelease

```bash
curl -fsSL https://raw.githubusercontent.com/z3z1ma/cdf/main/tools/install-cdf.sh \
  | bash -s -- --version 0.2.0-alpha.1
```

Add the installation directory to `PATH` if needed, then verify:

```bash
export PATH="$HOME/.local/bin:$PATH"
cdf version
```

Use `--dry-run` to inspect URLs and paths without writing anything, or choose a
different prefix:

```bash
curl -fsSL https://raw.githubusercontent.com/z3z1ma/cdf/main/tools/install-cdf.sh \
  | bash -s -- --version 0.2.0-alpha.1 --prefix "$HOME/.cdf" --dry-run
```

Every archive has an adjacent `.sha256` file and the installer verifies it
before extracting or writing the binary. Release binaries statically link the
pinned DuckDB runtime and archives include the matching Python runtime beside
`cdf`, plus generated completions and man pages.

## Build from Source

```bash
CARGO_BUILD_JOBS="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.logicalcpu)" \
  cargo build -p cdf-cli --release --locked --features bundled-duckdb
target/release/cdf version
```

Developer builds download the pinned prebuilt DuckDB runtime and link it
dynamically. No system DuckDB installation or source compilation is required
for ordinary local iteration; the release workflow deliberately builds DuckDB
from source and links it statically for portable artifacts.

## Publication Boundary

Crates.io publication remains disabled while distributable crates depend on
pinned git sources. That does not block the checksummed binary prerelease
channel.

The governing policy is
[`versioning-lts-release-policy.md`](../../.10x/specs/versioning-lts-release-policy.md).
