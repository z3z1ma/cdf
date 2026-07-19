Status: done
Created: 2026-07-18
Updated: 2026-07-18

# DuckDB nanoarrow default embedding

## Question

Can CDF make nanoarrow `0.8.0` with Arrow IPC LZ4 support the zero-configuration DuckDB ingress path without restoring per-Cargo-build DuckDB compilation, weakening the destination/runtime boundary, or depending on a mutable network extension at run time?

## Sources and Methods

- DuckDB extension overview and built-in extension semantics: <https://duckdb.org/docs/stable/extensions/overview>
- DuckDB extension build/configuration and static-link behavior: <https://duckdb.org/docs/lts/dev/building/building_extensions>
- DuckDB advanced installation methods: <https://duckdb.org/docs/current/extensions/advanced_installation_methods>
- DuckDB extension distribution and signing: <https://duckdb.org/docs/current/extensions/extension_distribution>
- DuckDB extension security model: <https://duckdb.org/docs/current/operations_manual/securing_duckdb/securing_extensions>
- DuckDB community nanoarrow entry: <https://duckdb.org/community_extensions/extensions/nanoarrow>
- `duckdb-nanoarrow` source at revision `42e4199a67c4cd0789087562a025e87e7130fdc3`: <https://github.com/paleolimbot/duckdb-nanoarrow/tree/42e4199a67c4cd0789087562a025e87e7130fdc3>
- Apache nanoarrow `0.8.0` revision `a579fbf5d192e85b6249935e117de7d02a6dc4e9`: <https://github.com/apache/arrow-nanoarrow/releases/tag/apache-arrow-nanoarrow-0.8.0>
- `libduckdb-sys 1.10504.0` linked/prebuilt library behavior: <https://docs.rs/crate/libduckdb-sys/1.10504.0>
- Built and inspected both loadable and statically linked variants on the controlled P3 EC2 host against DuckDB `v1.5.4` revision `08e34c447bae34eaee3723cac61f2878b6bdf787`. The extension source was patched only to pin nanoarrow `0.8.0` by archive SHA-256 and enable `NANOARROW_IPC_WITH_LZ4=ON`.
- Compared exact library sizes, dynamic dependencies, extension discovery, and the full CDF path. Product performance evidence is owned by `.10x/tickets/done/2026-07-18-p3-d14-duckdb-nanoarrow-080-lz4-revalidation.md`.

## Findings

1. DuckDB's extension configuration already supports the desired architectural shape. A `duckdb_extension_load(...)` without `DONT_LINK` builds a static extension and links it into DuckDB. The controlled build produced a `libduckdb.so` in which `nanoarrow` was already loaded and `SELECT nanoarrow_version()` returned `0.8.0` without `INSTALL`, `LOAD`, network access, or unsigned-extension enablement.
2. Static linkage is materially safer than making CDF silently load an unsigned extension. DuckDB's default accepts only core/community signatures; enabling `allow_unsigned_extensions` broadens the process-wide load policy. A CDF-owned digest check constrains the file CDF chooses, but it does not make DuckDB's resulting process policy equivalent to a statically linked extension.
3. Static linkage must be a release-artifact concern, not a normal Cargo-build concern. `libduckdb-sys` can link a supplied `DUCKDB_LIB_DIR` before considering `DUCKDB_DOWNLOAD_LIB`, while ordinary development builds can keep `DUCKDB_DOWNLOAD_LIB=1`. Therefore release CI can build the custom library once per target, compile CDF against it, and package it beside the binary without making every local profile compile DuckDB C++.
4. The custom Linux `libduckdb.so` was `56,664,744` bytes versus `70,458,712` bytes for DuckDB's official prebuilt library on the same host. Embedding the extension does not inherently enlarge the distributed library relative to the current artifact.
5. The loadable extension was `12,064,926` bytes before stripping and includes DuckDB-facing symbols that the static extension archive does not duplicate. Post-build `strip` invalidates the DuckDB extension footer, so loadable-artifact stripping must happen before metadata/footer generation.
6. The initial extension-repository builds dynamically linked `liblz4.so.1` because nanoarrow `0.8.0` resolves LZ4 through the platform CMake/pkg-config package. The retained release builder instead compiles pinned LZ4 `1.9.4` as PIC static code, passes that exact archive to nanoarrow, and rejects any resulting dynamic LZ4 dependency. The controlled Linux artifact's `readelf` table contains no `liblz4` entry.
7. DuckDB loadable extensions are tied to the exact DuckDB version and platform. Static release linkage has the same source tuple obligation but collapses runtime selection to the already-pinned CDF release library, which is easier to make reproducible and easier to report in receipts.
8. The current community nanoarrow artifact remains an unsuitable default for CDF's canonical segments: the tested community revision vendors pre-`0.8.0` nanoarrow and lacks the LZ4 build flag. CDF may move back to the community-signed artifact only after its exact release proves LZ4 compatibility and matches the product performance gate.

## Conclusions

- The preferred default is a CDF release-built custom DuckDB shared library with nanoarrow statically linked, nanoarrow `0.8.0` and LZ4 source pinned, and the library packaged exactly where CDF already expects its DuckDB runtime dependency.
- Ordinary Cargo builds continue using the official prebuilt DuckDB library. They retain the appender unless an explicitly digest-pinned loadable extension is configured; no hidden download or source compile occurs.
- CDF records whether the nanoarrow ingress came from `statically_linked` or `digest_pinned_loadable`. The loadable override remains an explicit development/diagnostic path, not the product default.
- Runtime capability selection stays inside `cdf-dest-duckdb`. Generic orchestration continues to see a staged-segment destination and operation-specific blocking lanes only.

## Limits

- Linux x86-64 is the only controlled host proven so far. The release matrix must build and smoke the exact static tuple on Linux ARM64, macOS x86-64/ARM64, and Windows x86-64 before claiming all release artifacts carry nanoarrow by default.
- The static-LZ4 release builder is proven on controlled Linux x86-64. The release matrix must still prove its native CMake/output and dependency checks on Linux ARM64, macOS x86-64/ARM64, and Windows x86-64.
- This research establishes mechanism and boundary, not the final multi-sample product result. D14 owns the promotion evidence and default decision.
