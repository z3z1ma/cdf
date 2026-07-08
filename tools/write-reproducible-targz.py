#!/usr/bin/env python3
"""Write a deterministic .tar.gz archive from a staged directory."""

from __future__ import annotations

import gzip
import os
import stat
import sys
import tarfile
from pathlib import Path


def die(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)


def normalized_mode(path: Path) -> int:
    mode = path.stat().st_mode
    if path.is_dir():
        return 0o755
    if mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH):
        return 0o755
    return 0o644


def add_entry(tar: tarfile.TarFile, path: Path, arcname: str) -> None:
    info = tar.gettarinfo(str(path), arcname)
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    info.mode = normalized_mode(path)

    if path.is_dir():
        info.type = tarfile.DIRTYPE
        info.size = 0
        tar.addfile(info)
        return

    if not path.is_file():
        die(f"unsupported non-file archive entry: {path}")

    with path.open("rb") as handle:
        tar.addfile(info, handle)


def iter_entries(root: Path) -> list[Path]:
    entries: list[Path] = []
    for current, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(dirnames)
        filenames.sort()
        current_path = Path(current)
        for dirname in dirnames:
            entries.append(current_path / dirname)
        for filename in filenames:
            entries.append(current_path / filename)
    return sorted(entries, key=lambda path: path.relative_to(root).as_posix())


def write_archive(stage_dir: Path, archive_path: Path) -> None:
    if not stage_dir.is_dir():
        die(f"stage directory does not exist: {stage_dir}")

    archive_path.parent.mkdir(parents=True, exist_ok=True)
    if archive_path.exists():
        archive_path.unlink()

    root_name = stage_dir.name
    with archive_path.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as gzip_file:
            with tarfile.open(fileobj=gzip_file, mode="w", format=tarfile.USTAR_FORMAT) as tar:
                add_entry(tar, stage_dir, root_name)
                for path in iter_entries(stage_dir):
                    rel = path.relative_to(stage_dir).as_posix()
                    add_entry(tar, path, f"{root_name}/{rel}")


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        die("usage: write-reproducible-targz.py STAGE_DIR ARCHIVE_PATH")
    write_archive(Path(argv[1]), Path(argv[2]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
