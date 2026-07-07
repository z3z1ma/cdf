Status: active
Created: 2026-07-06
Updated: 2026-07-06

# Rust Crate Organization

CDF crates SHOULD avoid monolithic `src/lib.rs` files when behavior grows beyond a small boundary.

`lib.rs` SHOULD usually contain crate documentation, shared imports/constants when they genuinely serve the whole crate, public module declarations or include map, and re-exports that define the public API. Implementation should be split into focused files by responsibility, such as `models.rs`, `planning.rs`, `receipts.rs`, `secrets.rs`, `tests.rs`, or destination-specific `ddl.rs`/`dml.rs`.

Prefer stable public APIs while splitting. Refactors that only improve file organization should not rename public symbols, change semantics, or widen visibility unless the owning ticket requires it.

Tests should move out of large crate roots into `tests.rs` or focused test modules when that makes the implementation easier to navigate.
