#!/usr/bin/env python3
"""Generate or verify CDF's production allocation-owner matrix.

The managed half is derived from production Rust `ReservationRequest::new(...)` sites. The
non-ledger half is an explicit, reviewable inventory because native libraries,
children, and external staging cannot be discovered soundly from Rust syntax.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLASSIFICATIONS = ROOT / "tools" / "memory-owner-classifications.json"
OUTPUT = ROOT / "docs" / "memory-allocation-owners.md"
CONSUMER_CALL = re.compile(r"\bConsumerKey\s*::\s*new\b")
REQUEST_CALL = re.compile(r"\bReservationRequest\s*::\s*new\b")


def mask_rust(source: str) -> str:
    """Mask comments and literals while retaining byte offsets and newlines."""

    chars = list(source)
    index = 0
    state = "code"
    block_depth = 0
    raw_hashes = 0
    while index < len(chars):
        current = source[index]
        following = source[index + 1] if index + 1 < len(chars) else ""
        if state == "code":
            if current == "/" and following == "/":
                chars[index] = chars[index + 1] = " "
                index += 2
                state = "line_comment"
                continue
            if current == "/" and following == "*":
                chars[index] = chars[index + 1] = " "
                index += 2
                block_depth = 1
                state = "block_comment"
                continue
            if current == '"':
                chars[index] = " "
                index += 1
                state = "string"
                continue
            if current == "'" and following and following != "s":
                # Lifetimes are deliberately left visible. Character literals always
                # close within a few bytes and may contain an escaped character.
                close = index + 2 if following != "\\" else index + 3
                if close < len(chars) and source[close] == "'":
                    for offset in range(index, close + 1):
                        chars[offset] = " "
                    index = close + 1
                    continue
            if current == "r":
                match = re.match(r'r(#{0,255})"', source[index:])
                if match:
                    raw_hashes = len(match.group(1))
                    for offset in range(index, index + len(match.group(0))):
                        chars[offset] = " "
                    index += len(match.group(0))
                    state = "raw_string"
                    continue
            index += 1
            continue
        if state == "line_comment":
            if current == "\n":
                state = "code"
            else:
                chars[index] = " "
            index += 1
            continue
        if state == "block_comment":
            if current == "/" and following == "*":
                chars[index] = chars[index + 1] = " "
                block_depth += 1
                index += 2
            elif current == "*" and following == "/":
                chars[index] = chars[index + 1] = " "
                block_depth -= 1
                index += 2
                if block_depth == 0:
                    state = "code"
            else:
                if current != "\n":
                    chars[index] = " "
                index += 1
            continue
        if state == "string":
            if current == "\\":
                chars[index] = " "
                if index + 1 < len(chars):
                    chars[index + 1] = " "
                index += 2
            elif current == '"':
                chars[index] = " "
                index += 1
                state = "code"
            else:
                if current != "\n":
                    chars[index] = " "
                index += 1
            continue
        if state == "raw_string":
            close = '"' + ("#" * raw_hashes)
            if source.startswith(close, index):
                for offset in range(index, index + len(close)):
                    chars[offset] = " "
                index += len(close)
                state = "code"
            else:
                if current != "\n":
                    chars[index] = " "
                index += 1
    return "".join(chars)


def matching_delimiter(masked: str, opening: int) -> int:
    pairs = {"(": ")", "{": "}", "[": "]"}
    expected = pairs[masked[opening]]
    depth = 0
    for index in range(opening, len(masked)):
        if masked[index] == masked[opening]:
            depth += 1
        elif masked[index] == expected:
            depth -= 1
            if depth == 0:
                return index
    raise ValueError(f"unclosed delimiter at byte {opening}")


def without_test_items(source: str) -> tuple[str, str]:
    """Blank items whose cfg predicate requires `test`."""

    masked = mask_rust(source)
    mutable = list(source)
    cursor = 0
    marker = re.compile(r"#\s*\[\s*cfg\s*\(([^\]]*)\)\s*\]")
    while match := marker.search(masked, cursor):
        condition = re.sub(r"\s+", "", match.group(1))
        test_only = condition == "test" or (
            condition.startswith("all(")
            and re.search(r"(?:^|[,(])test(?:[,)]|$)", condition) is not None
            and "not(test)" not in condition
        )
        if not test_only:
            cursor = match.end()
            continue
        item_start = match.start()
        brace = masked.find("{", match.end())
        semicolon = masked.find(";", match.end())
        if semicolon != -1 and (brace == -1 or semicolon < brace):
            item_end = semicolon
        elif brace != -1:
            item_end = matching_delimiter(masked, brace)
        else:
            raise ValueError("cfg(test) attribute has no following Rust item")
        for index in range(item_start, item_end + 1):
            if mutable[index] != "\n":
                mutable[index] = " "
        cursor = item_end + 1
    product = "".join(mutable)
    return product, mask_rust(product)


def call_spans(masked: str, pattern: re.Pattern[str]) -> list[tuple[int, int, int]]:
    spans = []
    cursor = 0
    while match := pattern.search(masked, cursor):
        start = match.start()
        opening = masked.find("(", match.end())
        if opening == -1:
            raise ValueError(f"{pattern.pattern} at byte {start} has no call delimiter")
        closing = matching_delimiter(masked, opening)
        spans.append((start, opening, closing))
        cursor = closing + 1
    return spans


def validate_consumer_key_name_is_not_aliased(masked: str, path: Path) -> None:
    aliases = [
        r"\bConsumerKey\s+as\s+[A-Za-z_][A-Za-z0-9_]*",
        r"\btype\s+[A-Za-z_][A-Za-z0-9_]*\s*=\s*(?:cdf_memory\s*::\s*)?ConsumerKey\b",
    ]
    for alias in aliases:
        if re.search(alias, masked):
            raise ValueError(
                f"{path}: ConsumerKey aliases are forbidden because they escape the allocation-owner audit"
            )


def consumer_from_expression(expression: str) -> tuple[str, str]:
    masked = mask_rust(expression)
    calls = call_spans(masked, CONSUMER_CALL)
    if not calls:
        return f"`{compact(expression)}`", "Inherited"
    if len(calls) != 1:
        raise ValueError(f"reservation key expression contains {len(calls)} ConsumerKey constructors")
    _, opening, closing = calls[0]
    arguments = split_arguments(expression, masked, opening, closing)
    if len(arguments) != 2:
        raise ValueError("ConsumerKey::new must have exactly two arguments")
    class_match = re.search(r"MemoryClass\s*::\s*([A-Za-z0-9_]+)", arguments[1])
    if not class_match:
        raise ValueError("ConsumerKey::new has no explicit MemoryClass")
    return owner_name(arguments[0]), class_match.group(1)


def self_test() -> None:
    """Exercise syntax variants whose omission would silently weaken the audit."""

    source = """
const DECOY: &str = r#"ReservationRequest::new(ConsumerKey::new(\"decoy\", MemoryClass::Source), 1)"#;
#[cfg(all(test, unix))]
fn test_only() {
    ReservationRequest::new(ConsumerKey::new("test", MemoryClass::Source), 1);
}
#[cfg(any(test, unix))]
fn production_on_unix() {
    ReservationRequest :: new(
        ConsumerKey
            :: new("live", MemoryClass::Source),
        2,
    );
}
fn unrelated() { OtherConsumerKey::new("not-cdf", MemoryClass::Source); }
"""
    product, masked = without_test_items(source)
    requests = call_spans(masked, REQUEST_CALL)
    consumers = call_spans(masked, CONSUMER_CALL)
    if len(requests) != 1 or len(consumers) != 1 or '"live"' not in product:
        raise RuntimeError("memory-owner scanner self-test failed to isolate production calls")
    if '"test"' in product or "decoy" not in source:
        raise RuntimeError("memory-owner scanner self-test failed cfg/literal handling")
    alias = mask_rust("use cdf_memory::ConsumerKey as HiddenKey;")
    try:
        validate_consumer_key_name_is_not_aliased(alias, Path("self-test.rs"))
    except ValueError:
        pass
    else:
        raise RuntimeError("memory-owner scanner self-test failed to reject ConsumerKey alias")


def split_arguments(source: str, masked: str, opening: int, closing: int) -> list[str]:
    arguments = []
    start = opening + 1
    stack: list[str] = []
    pairs = {"(": ")", "{": "}", "[": "]"}
    for index in range(opening + 1, closing):
        char = masked[index]
        if char in pairs:
            stack.append(pairs[char])
        elif stack and char == stack[-1]:
            stack.pop()
        elif char == "," and not stack:
            arguments.append(source[start:index].strip())
            start = index + 1
    tail = source[start:closing].strip()
    if tail:
        arguments.append(tail)
    return arguments


def compact(expression: str) -> str:
    return re.sub(r"\s+", " ", expression).strip()


def owner_name(expression: str) -> str:
    literal = re.fullmatch(r'"([^"\\]*)"', compact(expression))
    return literal.group(1) if literal else f"`{compact(expression)}`"


def managed_rows() -> list[dict[str, object]]:
    groups: dict[tuple[str, str, str], dict[str, object]] = {}
    for path in sorted((ROOT / "crates").glob("*/src/**/*.rs")):
        if path.name == "tests.rs":
            continue
        source = path.read_text(encoding="utf-8")
        product, masked = without_test_items(source)
        validate_consumer_key_name_is_not_aliased(masked, path)
        for _, opening, closing in call_spans(masked, REQUEST_CALL):
            request_args = split_arguments(product, masked, opening, closing)
            if len(request_args) != 2:
                raise ValueError(f"{path}: ReservationRequest::new must have exactly two arguments")
            owner, memory_class = consumer_from_expression(request_args[0])
            bound = compact(request_args[1])
            crate = path.relative_to(ROOT).parts[1]
            relative = path.relative_to(ROOT).as_posix()
            key = (crate, owner, memory_class)
            row = groups.setdefault(
                key,
                {
                    "crate": crate,
                    "owner": key[1],
                    "class": memory_class,
                    "paths": set(),
                    "bounds": set(),
                    "sites": 0,
                },
            )
            row["paths"].add(relative)
            row["bounds"].add(bound)
            row["sites"] += 1
    rows = []
    for row in groups.values():
        row["paths"] = sorted(row["paths"])
        row["bounds"] = sorted(row["bounds"])
        rows.append(row)
    return sorted(rows, key=lambda row: (row["crate"], row["class"], row["owner"]))


def load_nonledger_rows() -> list[dict[str, str]]:
    payload = json.loads(CLASSIFICATIONS.read_text(encoding="utf-8"))
    rows = payload.get("owners")
    if not isinstance(rows, list) or not rows:
        raise ValueError("memory owner classifications require a nonempty owners array")
    required = {"owner", "class", "boundary", "bound_authority", "evidence", "status"}
    seen = set()
    for row in rows:
        if set(row) != required or any(not isinstance(row[key], str) or not row[key] for key in required):
            raise ValueError(f"invalid non-ledger owner row: {row!r}")
        if row["owner"] in seen:
            raise ValueError(f"duplicate non-ledger owner {row['owner']!r}")
        seen.add(row["owner"])
        if row["status"] not in {"bounded", "measured", "open"}:
            raise ValueError(f"invalid non-ledger status for {row['owner']!r}")
    return sorted(rows, key=lambda row: (row["class"], row["owner"]))


def escape(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def render() -> str:
    managed = managed_rows()
    nonledger = load_nonledger_rows()
    lines = [
        "<!-- Generated by tools/generate-memory-owner-matrix.py; do not edit. -->",
        "# Memory allocation owners",
        "",
        "This matrix separates every `ReservationRequest::new` site discovered in production Rust source from allocations that require an explicit native, child-process, metadata, or external-storage authority. `Inherited` means the reservation receives an already-validated typed `ConsumerKey`; the row still records the concrete byte-bound expression at the allocation site.",
        "",
        f"Managed reservation declarations: **{len(managed)}** grouped rows across **{sum(int(row['sites']) for row in managed)}** production call sites.",
        "",
        "## Managed ledger owners",
        "",
        "| Crate | Owner | Class | Bound authority | Production path(s) | Sites |",
        "|---|---|---|---|---|---:|",
    ]
    for row in managed:
        lines.append(
            "| "
            + " | ".join(
                escape(value)
                for value in (
                    row["crate"],
                    row["owner"],
                    row["class"],
                    "; ".join(row["bounds"]),
                    "<br>".join(row["paths"]),
                    row["sites"],
                )
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Non-ledger and external owners",
            "",
            "| Owner | Class | Boundary | Bound authority | Evidence | Status |",
            "|---|---|---|---|---|---|",
        ]
    )
    for row in nonledger:
        lines.append(
            "| "
            + " | ".join(
                escape(row[key])
                for key in ("owner", "class", "boundary", "bound_authority", "evidence", "status")
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "`open` is a closure blocker, not a soft warning. A row may become `bounded` only when code admits it under the named authority, and `measured` only when reproducible host evidence falsifies the bound.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail when the committed matrix is stale")
    args = parser.parse_args()
    self_test()
    generated = render()
    if args.check:
        existing = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
        if existing != generated:
            print(
                "docs/memory-allocation-owners.md is stale; run tools/generate-memory-owner-matrix.py",
                file=sys.stderr,
            )
            return 1
        return 0
    OUTPUT.write_text(generated, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
