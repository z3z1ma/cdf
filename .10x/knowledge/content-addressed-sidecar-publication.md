Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Content-addressed sidecar publication

Content addressing does not make an overwrite-capable rename safe under concurrency. Two writers can both observe absence and the later rename can replace the earlier target, hiding a hash/path collision or corrupted producer.

For immutable sidecars under `.cdf/`, publication must:

1. Canonicalize and hash the complete bytes before naming the target.
2. Write and sync a unique temporary file in the target directory.
3. Install with an atomic no-clobber primitive.
4. If the target already exists, accept it only after verifying byte identity.
5. Fail closed when the filesystem cannot provide the required no-clobber guarantee.
6. Remove temporary files and, where supported, sync the parent directory.

Tests must force concurrent identical and conflicting publishers through a barrier. They must prove identical writers converge, conflicting writers preserve exactly one complete winner, and no temporary files remain.

Public serialized model evolution has a separate Rust compatibility constraint: `serde(default)` preserves deserialization but adding a public struct field still breaks downstream struct literals. Prefer validated extensibility slots already present in the public shape, or explicitly version a breaking API.
