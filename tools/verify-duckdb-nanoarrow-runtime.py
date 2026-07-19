#!/usr/bin/env python3
"""Verify the exact DuckDB/nanoarrow runtime exported by a CDF release build."""

from __future__ import annotations

import argparse
import ctypes
import pathlib


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--library", required=True, type=pathlib.Path)
    parser.add_argument("--duckdb-version", default="v1.5.4")
    parser.add_argument("--nanoarrow-version", default="0.8.0")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    library_path = args.library.resolve(strict=True)
    library = ctypes.CDLL(str(library_path))

    library.duckdb_library_version.restype = ctypes.c_char_p
    observed_duckdb = library.duckdb_library_version().decode("utf-8")
    if observed_duckdb != args.duckdb_version:
        raise SystemExit(
            f"DuckDB version mismatch: observed {observed_duckdb!r}, "
            f"expected {args.duckdb_version!r}"
        )

    database = ctypes.c_void_p()
    connection = ctypes.c_void_p()
    library.duckdb_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
    library.duckdb_open.restype = ctypes.c_int
    library.duckdb_connect.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
    library.duckdb_connect.restype = ctypes.c_int
    library.duckdb_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    library.duckdb_query.restype = ctypes.c_int
    library.duckdb_disconnect.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    library.duckdb_close.argtypes = [ctypes.POINTER(ctypes.c_void_p)]

    if library.duckdb_open(None, ctypes.byref(database)) != 0:
        raise SystemExit("DuckDB in-memory database open failed")
    try:
        if library.duckdb_connect(database, ctypes.byref(connection)) != 0:
            raise SystemExit("DuckDB in-memory connection failed")
        try:
            expected = args.nanoarrow_version.replace("'", "''")
            query = (
                "CREATE TEMP TABLE cdf_nanoarrow_runtime_smoke AS "
                "SELECT CASE WHEN nanoarrow_version() = "
                f"'{expected}' THEN 1 ELSE error('unexpected nanoarrow version') END AS ok"
            )
            if library.duckdb_query(connection, query.encode("utf-8"), None) != 0:
                raise SystemExit(
                    "statically linked nanoarrow is unavailable or reports the wrong version"
                )
        finally:
            library.duckdb_disconnect(ctypes.byref(connection))
    finally:
        library.duckdb_close(ctypes.byref(database))

    print(
        f"verified DuckDB {observed_duckdb} with statically linked "
        f"nanoarrow {args.nanoarrow_version}: {library_path}"
    )


if __name__ == "__main__":
    main()
