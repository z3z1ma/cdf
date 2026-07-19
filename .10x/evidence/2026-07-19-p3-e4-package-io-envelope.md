Status: recorded
Created: 2026-07-19
Updated: 2026-07-19

# P3 E4 package I/O envelope

## Observation

On the dedicated `c7i.4xlarge` P3 host (`i-05011a85b7f2a33fe`, 16 logical CPUs, tuned gp3 root volume), the production LZ4 Arrow IPC artifact writer sustained 1,033.8 MiB/s across three alternating 32 GiB samples. The otherwise identical hash-free control had a 31.695912041-second median; the production hash-while-write path had a 31.714295798-second median. The attributable SHA-256 wall fraction was 0.06%.

An 8 GiB `fio 3.32` direct-I/O sequential-write control on the same volume measured 1,200,885,585 bytes/s, or 1,145.25 MiB/s. Production package persistence therefore reached 0.903x the direct device roofline, above the 0.70 target.

The sustained fixture wrote 34,377,828,352 high-entropy IPC bytes per sample as 1,024 independently synced segment files. The 16 per-wave worker payloads and their constituent batches are distinct; the fixed wave fixture repeats to isolate writer cost. It used all 16 admitted CPUs, 64 waves, 64k-row batches, canonical Arrow IPC file framing, LZ4 frame compression, hash-while-write SHA-256, file sync, atomic rename, and containing-directory sync. Each sample exceeded the host's 30 GiB RAM, so the result cannot be a page-cache-only throughput claim.

## Procedure

The permanent ignored release test is `storage::tests::arrow_ipc_package_writer_roofline` in `cdf-package`. It compares the production writer with a private hash-free control using the same schema, batches, IPC options, output lengths, and durability protocol. Parallel workers receive distinct high-entropy batches; the raw control writes the exact encoded payloads. The sustained cell was run with:

```text
CDF_E4_BENCH_DIR=/home/ec2-user/cdf-bench/repo/target/cdf-benchmarks
CDF_E4_SUSTAINED_GIB=32
CDF_E4_SUSTAINED_SAMPLES=3
cargo test --release -p cdf-package arrow_ipc_package_writer_roofline --locked -- --ignored --nocapture
```

The direct roofline used `fio --rw=write --direct=1 --ioengine=libaio --iodepth=32 --bs=1M --size=8G` on the same filesystem.

Before the sustained run, an isolated backend comparison hashed the same 512 MiB payload with `sha2/asm`, `aws-lc-rs`, and `ring`. All three measured 1.453 GiB/s to three decimals. The alternate dependencies and test code were removed; no product dependency or fallback remains. This is a measured no-action result: independent segment hashes already parallelize above the named device roofline, and changing the backend cannot improve wall time on this host.

Raw artifacts:

- `.10x/evidence/.storage/2026-07-19-p3-e4-ipc-package-writer-sustained.log`
- `.10x/evidence/.storage/2026-07-19-p3-e4-fio-direct.json`
- `.10x/evidence/.storage/2026-07-19-p3-e4-sha256-backends.log`
- `.10x/evidence/.storage/2026-07-19-p3-e4-raw-hashing-writer.log`

## What it supports or challenges

- Supports the package-build target: 0.903x sustained direct-write roofline.
- Supports the hashing budget: 0.06% measured steady-state wall versus the exact hash-free control.
- Supports retaining SHA-256 and the current sink: replacement backends tied, while aggregate segment hashing is storage-bound.
- Completes the original package-I/O triage alongside E1's zero-reread/failpoint evidence, E2's streaming manifest/golden/million-entry construction evidence, and E3's bounded capability-rooted verification evidence.
- Challenges cache-sized and one-core controls as closure evidence. A single 128 MiB segment attributed roughly 36-45% of its writer wall to hashing, but that cost disappears once independent segments saturate sustained storage. The metric that matters for terabyte runs is the latter.

## Limits

This proves the named EC2 host class and gp3 volume, not every future device. A host sustaining materially more than approximately 5.5 GiB/s of package writes may expose aggregate SHA throughput as a bottleneck; that observation is the trigger for a multi-buffer or pipelined-hash investigation. No such machinery is justified on current evidence. Object-store multipart persistence has its own transport roofline and remains governed by destination/remote-I/O evidence.

The first sustained attempt correctly failed because the default `/tmp` is a 16 GiB tmpfs. The permanent benchmark now accepts `CDF_E4_BENCH_DIR`; the successful result used the benchmark volume. The failed tmpfs attempt is not performance evidence.
