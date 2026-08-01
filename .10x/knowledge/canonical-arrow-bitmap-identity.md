Status: active
Created: 2026-07-31
Updated: 2026-07-31

# Canonical Arrow bitmap identity

Logical Arrow equality does not imply byte-identical IPC. Boolean values and validity masks are
bit-packed into byte buffers, and bits beyond the logical array length are unspecified. For a
byte-aligned slice, Arrow IPC may shallow-slice the final byte and therefore preserve adjacent
out-of-range bits. A sliced all-valid validity mask may also remain present even though a freshly
built equivalent array has no null buffer. Both cases can produce different package bytes for the
same rows.

CDF canonical microbatching therefore treats these representations as copy-requiring:

- a Boolean value buffer with a nonzero logical bit offset;
- a validity mask with a nonzero bit offset;
- a retained validity mask whose logical null count is zero;
- a byte-aligned Boolean or validity slice with nonzero trailing padding bits.

The copy is part of the memory construction plan; it must make the zero-copy predicate false so
the caller reserves the replacement working set. Concatenated fragments already allocate a
canonical replacement. Exact unsliced batches with canonical padding retain the zero-copy path.

Regression evidence must compare serialized IPC bytes against a freshly constructed logical
equivalent, not only decoded arrays. The fixed-seed execution-shape property additionally compares
decoded segment batches, package hash, segment entries, lineage, execution profile, positions, and
terminal quarantine authority. A deliberately faulty identity snapshot proves that surface is
actually observed.

This is an identity boundary, not an Arrow-format rewrite. Do not normalize every nullable batch
speculatively; preserve exact zero-copy inputs and copy only representations whose bitmap storage
can change canonical bytes.
