Status: recorded
Created: 2026-07-26
Updated: 2026-07-26
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws8-release-engineering.md

# P1 WS8 hosted release evidence

## Observation

CDF `v0.2.0-alpha.1` is a published GitHub prerelease built from
`2928ee09be0a2a907a14775a85a6ad54500a8541`. The hosted workflow built all five
mainstream targets with the first-party `bundled-duckdb` feature, verified that no target linked a
dynamic DuckDB library, packaged the required Python runtime and license, verified all checksums,
and published the complete bundle.

The published aarch64 macOS artifact also passed the actual shell installer on a clean temporary
prefix. It ran with `DYLD_LIBRARY_PATH` and `DYLD_FALLBACK_LIBRARY_PATH` removed, both staged Mach-O
files passed strict code-signature verification, and `otool` showed the binary resolving Python
through `@rpath/libpython3.14.dylib` with no DuckDB dynamic dependency.

## Hosted procedure

The release was dispatched through:

```text
gh workflow run release-artifacts.yml \
  --ref main \
  -f version=0.2.0-alpha.1 \
  -f upload_github_prerelease=true
```

Authoritative hosted run:

- Run: <https://github.com/z3z1ma/cdf/actions/runs/30196650532>
- Result: `success`
- Started: `2026-07-26T09:32:42Z`
- Completed: `2026-07-26T10:29:47Z`
- Head: `2928ee09be0a2a907a14775a85a6ad54500a8541`

Every job passed:

| Job | Result | Completed |
|---|---|---|
| Release metadata | success | `09:32:52Z` |
| Canonical CLI artifacts | success | `09:33:36Z` |
| aarch64 Linux | success | `09:42:58Z` |
| x86_64 Linux | success | `09:44:09Z` |
| aarch64 macOS | success | `10:01:23Z` |
| Windows MSVC | success | `10:22:09Z` |
| x86_64 macOS | success | `10:29:04Z` |
| Verify release bundle | success | `10:29:32Z` |
| GitHub prerelease | success | `10:29:47Z` |

The Windows job located MSVC `dumpbin`, inspected the executable's dependents, and passed the
static-DuckDB check. Both macOS jobs passed staged-binary execution after relocating and ad-hoc
signing the embedded Python runtime. Both Linux jobs passed `ldd` inspection and native archive
verification.

## Published artifacts

Release:
<https://github.com/z3z1ma/cdf/releases/tag/v0.2.0-alpha.1>

The release is a non-draft prerelease whose tag resolves exactly to the hosted run head. It
contains five archives, five adjacent checksum files, and `SHA256SUMS`:

```text
ab9ad5ef2cf52bdbc8f50b988711796403739d794bc64fcef08cf0e52e093d91  cdf-0.2.0-alpha.1-aarch64-apple-darwin.tar.gz
c14cec1484beb05703a69bb9731323b5d110e571aa2ba0c494f04cc25543247f  cdf-0.2.0-alpha.1-aarch64-unknown-linux-gnu.tar.gz
020814e0f8f8f22a7fe9b95fc6ee47b4b0bad14f6cb654fcd40063556e4655af  cdf-0.2.0-alpha.1-x86_64-apple-darwin.tar.gz
10699aee08f314a8fa2c1235785a585e7f4e9ffce48934b40945799dec7baec5  cdf-0.2.0-alpha.1-x86_64-pc-windows-msvc.tar.gz
0ffcee186e0bb87f5cd894510dfd9185edf22a1b75f1f524024e1fc82ad6d55c  cdf-0.2.0-alpha.1-x86_64-unknown-linux-gnu.tar.gz
```

The hosted bundle verifier recomputed all five digests. A separate download of the published
aarch64 macOS archive recomputed
`ab9ad5ef2cf52bdbc8f50b988711796403739d794bc64fcef08cf0e52e093d91` and passed its adjacent
checksum.

Each archive contains:

- the target executable;
- the required Python shared runtime and `THIRD_PARTY_LICENSES/Python.txt`;
- Apache `LICENSE`;
- the versioned changelog excerpt and release metadata;
- bash, zsh, fish, and PowerShell completions;
- generated man pages for the root and command tree.

The release metadata job also proved crates.io publication remains disabled while the distributable
graph contains disallowed immutable source pins.

## Installer proof

The actual published artifact was installed with:

```text
tools/install-cdf.sh --version 0.2.0-alpha.1 --prefix <clean-temp-prefix>
env -u DYLD_LIBRARY_PATH -u DYLD_FALLBACK_LIBRARY_PATH \
  <clean-temp-prefix>/bin/cdf version
```

Observed:

```text
Installed cdf 0.2.0-alpha.1 to <clean-temp-prefix>/bin/cdf
cdf 0.2.0-alpha.1
```

Strict `codesign --verify` passed for both `cdf` and `libpython3.14.dylib`. `otool -L` named
`@rpath/libpython3.14.dylib` plus only operating-system libraries; it named no DuckDB library.

## What this supports

- WS8's green hosted-pipeline criterion.
- A complete versioned prerelease cut across all five ratified targets.
- A real checksummed install channel against the published artifact.
- Generated completion and man-page inclusion.
- Static DuckDB for release/CI while retaining the prebuilt dynamic optimization for development
  and benchmark builds.
- The active versioning/LTS policy's prerelease, target, artifact, and no-crates.io boundaries.

## Limits

Only the current-host aarch64 macOS archive was installed after publication. The hosted jobs are
the native execution and dependency-inspection authorities for the other four targets. The
artifacts are ad-hoc signed and not notarized; signing/notarization remains outside the active
initial install-channel policy. Node 20 action deprecation warnings were informational and did not
affect any job result.
