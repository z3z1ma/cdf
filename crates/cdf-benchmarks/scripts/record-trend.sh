#!/usr/bin/env bash
set -euo pipefail

suite="${1:-smoke}"
out="${2:-target/cdf-benchmarks/trends/${suite}.jsonl}"

cargo run -p cdf-benchmarks --bin cdf-benchmark-trend --locked -- --suite "${suite}" --out "${out}"
