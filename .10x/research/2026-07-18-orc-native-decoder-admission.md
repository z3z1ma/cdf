Status: done
Created: 2026-07-18
Updated: 2026-07-18

# Native ORC decoder admission

## Question

Can the current Rust ORC ecosystem implement B7's stripe-parallel native codec while
preserving CDF's exact-range, constant-memory, and dependency-isolation laws?

## Sources and methods

- Inspected `orc-rust` 0.8.0 from crates.io and its linked
  `datafusion-contrib/orc-rust` repository, including `reader`, `metadata`, `stripe`,
  `compression`, `arrow_reader`, `async_arrow_reader`, and nested/string decoders.
- Inspected `datafusion-orc` 0.9.0, the DataFusion 54 adapter, including its object-store
  reader and physical scan construction.
- Compared the exposed APIs and allocation sites with
  `.10x/specs/native-format-codec-runtime.md` and B7's stripe-unit contract.
- Primary sources: <https://github.com/datafusion-contrib/orc-rust>,
  <https://docs.rs/orc-rust/0.8.0/orc_rust/arrow_reader/struct.ArrowReaderBuilder.html>,
  and <https://github.com/datafusion-contrib/datafusion-orc> (inspected 2026-07-18).

## Findings

`orc-rust` 0.8.0 is the sole credible native Arrow reader. It exposes public file and
stripe metadata, synchronous and asynchronous chunk-reader traits, exact byte-range
access, projection, byte-range stripe selection, row selection, and row-index predicate
pruning. Its format coverage includes all ORCv1 types and Zlib, Snappy, LZO, LZ4, and
Zstd compression. These are the right semantics for one deterministic CDF decode unit
per stripe.

The current ownership model is not admissible unchanged:

- `Stripe::new_async` reads every selected stream into an owned `Bytes` map before
  Arrow decoding. A CDF `AsyncChunkReader` adapter can validate and lease each range,
  but the dependency API cannot carry those leases with the returned buffers or expose
  a pull-based stream working set.
- File metadata contains a producer-controlled compression block size. Zlib and Zstd
  use unbounded `read_to_end`; Snappy resizes to the embedded decoded length; LZO
  materializes a complete result; LZ4 allocates the declared maximum. The implementation
  does not uniformly reject output exceeding the declared block size before allocation.
- String/binary and nested decoders allocate from decoded length/count streams. CDF can
  reject retained batches after they are yielded, but that is too late to contain a
  hostile transient allocation.
- Metadata/footer and row-index paths also use complete decompression into `Vec` values.
- `datafusion-orc` 0.9.0 depends on `orc-rust` 0.8 and merely supplies DataFusion/object-
  store integration. It adds no bounded decoder authority and would import the entire
  DataFusion engine into an identity-bearing codec path contrary to the selective-
  adoption rule.

The issues are repairable upstream: request/result buffers need an ownership token or
caller allocator; compression needs a mandatory decoded ceiling checked while writing;
stripe stream retention needs an explicit aggregate working-set bound; length/count
decoders need checked output ceilings; and footer/index protobuf decoding needs bounded
messages. A generic process-isolated codec worker is an alternative only if measured
startup/IPC cost and CDF's source/credential/artifact authority remain neutral.

## Conclusions

Do not add `orc-rust` or `datafusion-orc` to production yet. Their semantic coverage is
good enough, but a wrapper would create the appearance of constant memory while leaving
dependency-owned allocations outside the ledger. B7 should remain blocked on bounded
upstream APIs or a generic measured isolation mode. A B7-specific subprocess, full-file
spool, DataFusion scan branch, or retained fork would be a leaky half measure.

## Limits

This is source/API admission evidence, not a throughput benchmark or corpus correctness
result. It does not prove that every valid ORC file triggers the risks, nor that an
upstream patch would meet B7's roofline. No dependency or product code was changed.
