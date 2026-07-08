#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Verify CDF release metadata before building or packaging artifacts.

Usage:
  verify-release-metadata.sh VERSION [--write-changelog-excerpt PATH]

The check fails closed when the workspace version, changelog section, license
file, or DataFusion-git publication boundary is inconsistent with the active
release policy.
USAGE
}

workspace_version() {
  awk '
    /^\[workspace\.package\]/ { in_section = 1; next }
    /^\[/ { in_section = 0 }
    in_section && $1 == "version" {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' Cargo.toml
}

write_changelog_excerpt() {
  local version output
  version="$1"
  output="$2"

  awk -v version="$version" '
    $0 == "## [" version "] - " substr($0, length("## [" version "] - ") + 1) {
      capture = 1
    }
    capture && /^## / && $0 != "## [" version "] - " substr($0, length("## [" version "] - ") + 1) {
      exit
    }
    capture { print }
  ' CHANGELOG.md >"$output"

  [[ -s "$output" ]] || die "CHANGELOG.md has no section for version $version"
}

version="${1:-}"
[[ -n "$version" ]] || {
  usage
  die 'VERSION is required'
}
shift || true

excerpt_path=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --write-changelog-excerpt)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--write-changelog-excerpt requires a path'
      excerpt_path="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

case "$version" in
  v*) die "version must not include leading v: $version" ;;
esac
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  die "version is not a supported semver-like release version: $version"
fi

cargo_version="$(workspace_version)"
[[ -n "$cargo_version" ]] || die 'workspace package version not found in Cargo.toml'
[[ "$cargo_version" == "$version" ]] || die "workspace version $cargo_version does not match release version $version"

[[ -f LICENSE ]] || die 'LICENSE file is required for release artifacts'
grep -qx 'license = "Apache-2.0"' Cargo.toml || die 'workspace license must remain Apache-2.0'
grep -q "^## \\[$version\\] - " CHANGELOG.md || die "CHANGELOG.md has no dated section for $version"

if [[ -n "$excerpt_path" ]]; then
  mkdir -p "$(dirname "$excerpt_path")"
  write_changelog_excerpt "$version" "$excerpt_path"
fi

if grep -R 'datafusion = { git = "https://github.com/apache/datafusion.git"' crates/*/Cargo.toml >/dev/null 2>&1; then
  missing_publish_false="$(
    find crates -mindepth 2 -maxdepth 2 -name Cargo.toml -print | while IFS= read -r manifest; do
      if ! grep -qx 'publish = false' "$manifest"; then
        printf '%s\n' "$manifest"
      fi
    done
  )"
  [[ -z "$missing_publish_false" ]] || die "DataFusion git pin is active; crate publication remains disabled, but these manifests lack publish = false: $missing_publish_false"
fi

printf 'release metadata ok for %s\n' "$version"
