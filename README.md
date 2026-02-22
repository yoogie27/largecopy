# largecopy

<p align="center">
  <img src="https://github.com/yoogie27/largecopy/raw/main/logo/logo.jpg" alt="LargeCopy Logo" width="128" height="128">
</p>

<p align="center">
  <strong>Fast. Reliable. Restartable.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Windows-0078D4?logoColor=white" alt="Windows" />
  <img src="https://img.shields.io/badge/Alternative_to-Robocopy-orange" alt="AlternativeTo" />
  <a href="https://opensource.org/licenses/MIT" target="_blank">
    <img src="https://img.shields.io/badge/License-MIT-yellow" alt="AlternativeTo" /></a>
</p>

---


### High-Performance File Copy Engine for Windows

> *When you need to move a single massive file across a network — fast, reliably, and restartably.*

**largecopy** copies single large files at maximum possible speed across any network condition — local NVMe, 10 GbE LAN, WiFi, VPN tunnels — while ensuring data integrity and the ability to resume from any interruption. It auto-detects your hardware and network, tunes itself, and gets out of your way.

Built from scratch in C++ with the Win32 API. No frameworks, no runtime dependencies, no external DLLs. A single ~200 KB executable.


---

## What problem does this solve?

You have a 100 GB database backup, a VM snapshot, or a disk image. You need to move it to a file server, a NAS, or a remote site. You use robocopy. It works, but it's slow — single-threaded buffered I/O can't saturate your 10 GbE link. You add `/Z` for restartability, and now it's even slower. If it does get interrupted, the resume trusts whatever bytes are already on disk without verifying them.

**largecopy** was built for this exact scenario:

- Copy a **single large file** at the highest throughput your hardware allows
- **Resume automatically** if interrupted — power failure, network drop, Ctrl+C, doesn't matter
- **Verify every byte** with xxHash3-128 checksums, pipelined so hashing doesn't slow you down
- **Work across any network** — auto-tunes for local SSDs, 10 GbE, WiFi, VPN, anything
- **Zero configuration required** — detects your storage, network, and system, then configures itself

---

## Quick Start

```cmd
:: Copy a file (you must specify the full destination filename)
largecopy D:\backup.vhdx \\server\share\backup.vhdx

:: If interrupted, just run the same command again — it resumes automatically
largecopy D:\backup.vhdx \\server\share\backup.vhdx

:: Compare two files without downloading
largecopy compare D:\backup.vhdx \\server\share\backup.vhdx

:: Hash a local file (xxHash3-128)
largecopy hash D:\backup.vhdx
```

### Important: you must specify the destination filename

Unlike robocopy, which accepts a destination *directory*, largecopy requires the **full destination path including the filename**. This is intentional — largecopy operates on exactly one file at a time and needs to know the precise target path for its ledger, pre-allocation, and resume logic.

```cmd
:: Correct — full path with filename
largecopy D:\data.bak \\server\share\data.bak

:: Wrong — this won't work
largecopy D:\data.bak \\server\share\
```

---

## What it does automatically

You don't need to understand any of the internals. Just run `largecopy <source> <dest>` and it handles everything:

**Auto-detection:**
- Identifies source and destination storage type (NVMe, SSD, HDD, network share)
- Detects filesystem (NTFS, ReFS, etc.), cluster size, sector size
- Probes network link speed, NIC count, MTU, SMB multichannel support
- Checks available RAM, CPU count, admin privileges

**Auto-tuning:**
- Sets optimal chunk size (larger for fast storage, smaller for slow links)
- Configures pipeline depth (more parallelism for high-latency networks)
- Opens multiple SMB connections scaled to your link speed and NIC count
- Enables SMB compression on Windows 11+ when beneficial
- Enables sparse file detection for NTFS/ReFS
- Enables adaptive throughput monitoring for network destinations
- Measures round-trip time and calculates bandwidth-delay product for WAN paths
- Bounds memory usage to 75% of available RAM

**Auto-resume:**
- If the destination file and ledger already exist from a previous interrupted copy, largecopy detects this and resumes automatically — no special flags needed, just run the same `copy` command again
- Already-verified chunks are skipped with zero I/O (no re-reading, no re-hashing, no re-writing)

**You can override any auto-detected setting** with command-line flags if you know your environment better than the detector does. But in most cases, the defaults are optimal.

---

## The restart mechanism

This is the core feature that makes largecopy different from every other copy tool.

### How it works

When largecopy starts a copy, it creates a **ledger file** — a small binary file (`.lcledger`) that tracks the state of every chunk:

```
Source file:  [chunk 0][chunk 1][chunk 2][chunk 3]...[chunk N]
Ledger:       [  OK  ][  OK  ][ FAIL  ][pending]...[pending]
```

Each chunk (default 50 MB) goes through a pipeline:

1. **Read** from source (async, overlapped)
2. **Hash** with xxHash3-128 (on a dedicated thread pool)
3. **Write** to destination (async or sync depending on target)
4. **Record** hash + state in the ledger

The ledger is memory-mapped, so state changes are instant and survive crashes. If the process is killed at any point — power failure, Ctrl+C, network disconnect — the ledger on disk reflects exactly which chunks are verified and which aren't.

### Resuming

When you run `largecopy` and the destination already exists with a valid ledger:

1. largecopy finds the ledger (stored locally in `%TEMP%\largecopy\` for network destinations, or next to the file for local copies)
2. Validates it matches the same source file (size + path hash)
3. Skips all chunks already marked as `Verified` — zero I/O for those
4. Re-queues everything else
5. Continues at full speed

**You don't need `resume` command** for this — just re-run the same `copy` command. The explicit `resume` command exists for when you want to point at a specific ledger file.

### How this compares to robocopy /Z

Robocopy **does** have a restartable mode (`/Z`). It works: if a copy is interrupted, robocopy can pick up from where it left off. However, it comes with significant trade-offs that make it a different kind of tool.

**How robocopy `/Z` works:** Robocopy writes restart information into the destination file (via an NTFS alternate data stream). It copies in small synchronous buffered blocks. On interruption, the restart marker tells robocopy where to continue writing. On successful completion, the restart data is removed.

**The trade-offs:**

| | robocopy /Z | largecopy |
|---|---|---|
| **Must opt in?** | Yes — you must use `/Z` from the start of the copy | No — the ledger is always maintained automatically |
| **Speed penalty** | Significant — `/Z` is noticeably slower than normal robocopy due to per-block restart marker updates | None — the ledger is updated as part of the normal pipeline at no extra cost |
| **Resume behavior** | Continues writing from the last marker position | Skips verified chunks entirely (zero I/O for already-done work) |
| **Data verification** | None — trusts that bytes already on disk are correct | xxHash3-128 on every chunk, verified before marking complete |
| **I/O model** | Synchronous, buffered, small blocks | Async IOCP pipeline, unbuffered, 50 MB chunks |
| **Resume after corruption** | Silently continues past corrupted data | Detects corruption via hash mismatch, re-transfers affected chunks |

**The key difference:** With robocopy `/Z`, you choose between speed (no `/Z`) and restartability (`/Z` with a speed penalty). With largecopy, you get both — the ledger is maintained as a natural byproduct of the hash verification pipeline, so restartability is always on at full speed.

**What robocopy `/Z` does well:** It's built into Windows, handles directory trees, preserves ACLs and timestamps, and works reliably for moderate-sized files. If you're copying a folder of mixed files and want basic restartability, `/Z` is convenient.

**Where largecopy is better:** For a single very large file where throughput matters, largecopy's IOCP pipeline, unbuffered I/O, connection striping, and verified resume give it a substantial advantage. The combination of maximum speed *and* verified restartability *and* zero configuration overhead is what makes the difference.

---

## How it achieves maximum throughput

### The pipeline

Traditional copy tools work like this:
```
Read 50MB → Wait → Hash → Wait → Write 50MB → Wait → Read next...
```

largecopy pipelines everything:
```
Read chunk 5   ─┐
Hash chunk 4   ─┤ all happening simultaneously
Write chunk 3  ─┘
```

Multiple chunks are in flight at once (default 8, auto-tuned up to 128 for high-latency networks). While one chunk is being written to the destination, the next is being hashed, and several more are being read from source. The pipeline never stalls.

### IOCP (I/O Completion Ports)

All I/O goes through Windows IOCP — the most efficient async I/O mechanism available on Windows. Multiple worker threads service completions from a single port. There is zero synchronous I/O during transfer.

### Unbuffered direct I/O

Files are opened with `FILE_FLAG_NO_BUFFERING`, bypassing the Windows file cache entirely. Data moves directly between the disk and the application's sector-aligned buffers — no double-copy through kernel buffers. This is critical for large files: a 100 GB file would thrash the file cache and evict everything else.

### Connection striping

For network destinations, largecopy opens multiple independent file handles to the same path. Each handle becomes a separate SMB session over a separate TCP connection. With SMB Multichannel, these connections are distributed across multiple NICs automatically. This multiplies throughput proportionally — 4 connections on a 10 GbE link approaches 4x the single-connection speed.

### Pre-allocation

With admin privileges, largecopy uses `SetFileValidData` to pre-allocate the destination file without zero-filling. For a 100 GB file, this takes milliseconds instead of minutes. Without admin, it falls back to `SetEndOfFile` (which zero-fills but still avoids fragmentation).

---

## Resilience

largecopy is designed for zero data loss in hostile network environments:

**Transient errors** (network timeout, SMB disconnect, share temporarily unavailable):
- Automatic retry with exponential backoff (100ms → 200ms → 400ms → ... → 10s)
- File handles are automatically re-opened on `ERROR_NETNAME_DELETED`
- Up to 5 retries per chunk (configurable)

**Stall detection:**
- If zero progress for 15 seconds with chunks in flight, the engine cancels stuck I/O and re-queues the affected chunks
- Adaptive controller reduces pipeline depth to prevent overwhelming the target

**Ctrl+C (graceful shutdown):**
- First Ctrl+C: cancels pending I/O, flushes the ledger, shows accurate summary of what was transferred
- Second Ctrl+C: force exit
- Ledger is always consistent — just run the copy command again to resume

**Hard kill / power failure:**
- The ledger is memory-mapped. Any chunk not atomically marked as `Verified` will be re-transferred on resume
- No partial/corrupt chunks can survive — the hash is written before the verified state, and the state byte write is atomic on x86/x64

**Permanent errors** (disk full, access denied):
- Chunk is marked `Failed` in the ledger
- Other chunks continue transferring
- Summary shows exactly which chunks failed
- `largecopy resume` retries only failed/pending chunks

**Source modification detection:**
- largecopy records the source file's last-write timestamp before starting
- After the transfer, it checks whether the timestamp changed
- If the source was modified during the copy, a warning is printed — the destination may contain a mix of old and new data

---

## Data integrity

### How safe is my copy?

The safety of a largecopy transfer depends on the **direction** and **storage type**. Understanding the differences helps you choose the right approach for critical data.

### Local to local / Remote to local

**This is the safest direction.** Writes to local storage use `FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH`, which bypasses the filesystem cache and commits data directly to stable storage. When a chunk is marked as transferred, the data is physically on disk.

### Local to remote (network share)

Writing to a remote SMB server is inherently less safe than writing locally. When Windows reports a write as "complete" for a network destination, it means the SMB client has sent the data — not that the server has committed it to disk. If the remote server crashes or loses power before flushing its cache, data that the ledger considers transferred may be lost.

largecopy mitigates this by flushing the destination at the end of the transfer, but individual chunk writes during the copy rely on the server's own caching policy.

**For critical transfers to a remote destination, use `--verify-after`:**

```cmd
largecopy D:\important.bak \\server\share\important.bak --verify-after
```

This re-reads the entire destination file after the copy and verifies every chunk's hash. It roughly doubles the transfer time but ensures end-to-end correctness.

### Recommendation

When possible, **prefer pulling files (remote to local) over pushing (local to remote)**. The receiving machine has full control over write durability when the destination is a local disk. If you must push to a remote target and the data is critical, always use `--verify-after`.

### What the checksums verify

During the copy, each chunk is hashed (xxHash3-128) from the source data in memory before being written. These hashes are stored in the ledger and enable:

- **Resume safety** — on restart, only chunks not yet marked as transferred are re-sent
- **Post-copy verification** — `--verify-after` or `largecopy verify` re-reads the destination and compares against stored hashes
- **Corruption detection** — any bit flip, truncation, or I/O error is caught by hash mismatch

The checksums do **not** protect against source file modification during the copy. If the source changes mid-transfer, largecopy detects the timestamp change and warns you, but individual chunks may contain a mix of old and new data.

---

## Commands

| Command | Description |
|---|---|
| `copy` | Copy a file. Resumes automatically if a valid ledger exists. |
| `resume` | Resume from a specific ledger file or destination path. |
| `verify` | Re-read the destination and verify every chunk against stored hashes. |
| `status` | Show progress of a transfer ledger (verified/failed/pending). |
| `compare` | Compare two files by reading chunks in random order. Ctrl+C to stop when satisfied. |
| `hash` | Compute xxHash3-128 of a file. Prints hash to stdout. |
| `bench` | Benchmark sequential read/write I/O to a path. |
| `help` | Show usage information. |

---

## Usage examples

### Copy a database backup to a file server

```cmd
largecopy D:\backups\prod.bak \\fileserver\backups\prod.bak
```

largecopy auto-detects NVMe source → 10 GbE network destination, opens multiple SMB connections, enables adaptive pipeline tuning, and transfers at full link speed. If the network drops mid-transfer, Ctrl+C or just wait — the ledger persists. Re-run the same command to resume.

### Copy a VM snapshot over VPN

```cmd
largecopy E:\VMs\server.vhdx \\remote-dc\share\server.vhdx
```

High-latency link? largecopy measures RTT, calculates bandwidth-delay product, and sets pipeline depth and connection count accordingly. Adaptive tuning adjusts in real-time as conditions change.

### Resume after interruption

```cmd
:: Just run the same command again
largecopy D:\backups\prod.bak \\fileserver\backups\prod.bak
```

Output:
```
Found valid ledger for interrupted transfer — resuming automatically.
Resuming transfer: 487 chunks remaining
```

### Verify a completed copy

```cmd
largecopy verify D:\backups\prod.bak \\fileserver\backups\prod.bak
```

Re-reads every chunk from the destination, recomputes xxHash3-128, and compares against the stored hashes in the ledger. Reports any mismatches.

### Compare two files across the network

```cmd
largecopy compare D:\local\backup.vhdx \\server\share\backup.vhdx
```

Reads chunks from both files in **random order**, hashes them, and compares. A visual grid shows progress — green squares for matching chunks, red for mismatches. Coverage increases over time. Press **Ctrl+C** when you're satisfied with the confidence level, or let it run to 100%.

```
  ■■■·■■■■
  ■■■■■■·■
  ■■·■■■■■
  ■■■■■■■■

  Checked: 28/32 | Match: 28 | 156 chunks/s | 87.5% coverage
```

Why random order? If the files differ, a random scan finds the difference quickly without reading the entire file. If they match, you get increasing statistical confidence and can stop early.

### Hash a file

```cmd
largecopy hash D:\backups\prod.bak
```

Computes xxHash3-128 with a progress bar and speed display. The hash is printed to **stdout** in a format compatible with `xxh128sum`:

```
A1B2C3D4E5F6A7B8C9D0E1F2A3B4C5D6  D:\backups\prod.bak
```

On macOS/Linux, verify with: `xxh128sum prod.bak` (install via `brew install xxhash` or `apt install xxhash`).

### Benchmark a storage path

```cmd
largecopy bench \\server\share\
```

Writes and reads 1 GB to measure sequential throughput. Useful for validating network/storage performance before a large transfer.

### Dry run (see what would happen)

```cmd
largecopy --dry-run D:\big.vhdx \\server\share\big.vhdx
```

Shows the auto-detected environment, auto-configured settings, and exits without copying. Useful for understanding what largecopy will do.

---

## Options

| Option | Default | Description |
|---|---|---|
| `--chunk-size <MB>` | 50 | Chunk size in megabytes. Auto-tuned by detection engine. |
| `--threads <n>` | auto | I/O thread count. Auto-set to CPU count (max 16). |
| `--inflight <n>` | 8 | Max chunks in the pipeline simultaneously. Auto-tuned. |
| `--retries <n>` | 5 | Retry count per chunk before marking failed. |
| `--connections <n>` | auto | Parallel SMB sessions. Auto-scaled by link speed. |
| `--no-checksum` | off | Skip xxHash3 checksumming. Faster but no verification. |
| `--compress` | off | Request SMB compression (auto-enabled on Win11+). |
| `--verify-after` | off | Full verification pass after copy completes. |
| `--force` | off | Overwrite existing destination, ignoring any ledger. |
| `--wan` | auto | WAN mode. Auto-enabled for network destinations. |
| `--adaptive` | auto | Auto-tune inflight depth in real-time. |
| `--sparse` | auto | Skip zero-filled regions in sparse files. |
| `--delta` | off | Skip chunks destination already has (hash compare). |
| `--verbose` | off | Print per-chunk completion details. |
| `--dry-run` | off | Show config and exit without copying. |
| `--quiet` | off | Suppress progress display. |

---

## Building from source

### Prerequisites

- **Visual Studio Build Tools 2019+** with C++ desktop workload
- Windows 10 / Windows Server 2016 or later
- x64 only

### Compile

```cmd
build-local.bat
```

Output: `build\largecopy.exe` (~200 KB, fully static, no external dependencies).

The build script calls `cl.exe` with `/std:c++20 /O2 /W4` and links against `ws2_32.lib`, `iphlpapi.lib`, `advapi32.lib`, and `mpr.lib` (all standard Windows system libraries).

---

## Architecture

### Pipeline flow (per chunk)

```
1. ReadFile(source, offset)        [async, overlapped, IOCP]
2. xxHash3-128(buffer)             [dedicated hash thread pool]
3. WriteFile(dest, offset)         [async IOCP or sync for network]
4. Ledger update: hash + Verified  [memory-mapped, atomic]
5. Buffer returned to pool         [lock-free SList]
6. Next pending chunk submitted    [pump_reads]
```

### Thread model

```
Main Thread ─── CLI parse → detect environment → auto-configure
             → create ledger → pre-allocate → submit initial reads
             → progress display loop (250ms) + adaptive tuning

IOCP Threads (N = CPU count, max 16) ─── GetQueuedCompletionStatus
             → dispatch read/write completions → error handling

Hash Threads (N = CPU count / 2, min 2) ─── lock-free SList queue
             → xxHash3-128 → post write or sync write to network
```

### Binary ledger format

```
[LedgerHeader   ~2200 bytes]  magic, source/dest paths, sizes, path hash
[ChunkRecord[0]   48 bytes]   offset, length, state, retry_count, hash
[ChunkRecord[1]   48 bytes]
...
[ChunkRecord[N-1] 48 bytes]
```

The ledger is memory-mapped via `CreateFileMapping` / `MapViewOfFile`. State updates are direct memory writes. For network destinations, the ledger is stored locally in `%TEMP%\largecopy\` to avoid SMB I/O interference.

### Chunk state machine

```
Pending → Reading → Hashing → Writing → Verified
   ↑         |                              |
   |         +── error → retry ────────────→+
   |                                        |
   +── DeltaMatch (already matches)         |
   +── Sparse (zero-filled region)          |
   +── Failed (max retries exceeded)        |
                                            |
all_done() = all chunks Verified/Sparse/DeltaMatch
```

### Key design decisions

**Sync writes for non-Windows SMB servers:** macOS smbd and Samba struggle with async overlapped writes from multiple threads. largecopy detects non-NTFS/ReFS network destinations and switches to synchronous writes from hash threads, gated by a semaphore. This prevents overwhelming the server while maintaining read/hash pipelining.

**Adaptive inflight (AIMD):** For network transfers, the pipeline depth is adjusted in real-time. Throughput increases → ramp up 25%. Throughput drops → back off 12.5%. This converges to optimal depth automatically without manual tuning.

**Local ledger for remote destinations:** Memory-mapping a file over SMB causes background flushes that interfere with data writes. The ledger is stored in `%TEMP%\largecopy\` (keyed by a hash of the destination path) for network targets.

---

## File structure

```
largecopy/
├── build-local.bat         Build script
├── LICENSE                  MIT License
├── vendor/
│   └── xxhash.h            xxHash (header-only, vendored)
└── src/
    ├── main.cpp             Entry point, Ctrl+C handler
    ├── common.h             Shared types, constants, utilities
    ├── args.h / args.cpp    CLI argument parser
    ├── engine.h / engine.cpp IOCP pipeline engine
    ├── ledger.h / ledger.cpp Memory-mapped binary ledger
    ├── hasher.h / hasher.cpp Async hash thread pool
    ├── buffer_pool.h / .cpp Lock-free aligned buffer pool
    ├── console.h / .cpp     VT100 progress display
    ├── detect.h / detect.cpp Storage/system/network detection
    ├── wan.h / wan.cpp       Connection pool, adaptive, sparse, delta
    ├── smb.h / smb.cpp       SMB compression
    └── privilege.h / .cpp    SetFileValidData privilege
```

---

## FAQ

**Q: Does it copy directories?**
No. largecopy is purpose-built for single large files. For directory trees, use robocopy. A common pattern: robocopy for the small files, largecopy for the big ones.

**Q: What's the minimum file size where this is faster than robocopy?**
Roughly 1 GB. Below that, the setup overhead (IOCP, pre-allocation, ledger creation) isn't worth it.

**Q: Can I use it without admin privileges?**
Yes. Without admin, pre-allocation falls back to `SetEndOfFile` (slower for very large files because it zero-fills). Everything else works normally.

**Q: Does it work with NFS, iSCSI, USB drives?**
The core engine works with any Windows filesystem. SMB-specific optimizations only apply to SMB shares. iSCSI volumes appear as local disks and get the local optimization path. USB drives are detected as HDD and get conservative settings.

**Q: Is xxHash3-128 cryptographically secure?**
No. It's a non-cryptographic hash optimized for speed. It detects accidental corruption and bit rot with extremely high probability (2^-128 collision chance) but is not suitable for security verification.

**Q: What happens if the destination runs out of disk space?**
Affected chunks are marked `Failed`. Other chunks continue. Free up space and run the copy again to retry only the failed chunks.

**Q: Can I copy to/from cloud storage (OneDrive, SharePoint)?**
If it's mounted as a drive letter or UNC path, yes. Performance depends on the cloud provider's SMB implementation.

**Q: Why does the hash output go to stdout while everything else goes to stderr?**
So you can pipe the hash into a file or another tool: `largecopy hash file.bin > hash.txt` captures just the hash line while still showing progress on the terminal.

**Q: Can I use this in a professional environment?**
Yes, no problem!

---

## License

MIT License. See [LICENSE](LICENSE) for full text.
