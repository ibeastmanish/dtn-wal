# dtn-wal

A crash-safe, append-only write-ahead log and store-and-forward bundle queue for Delay-Tolerant Networks (DTN), intermittent radio links and off-grid sensor meshes.

[![Crates.io](https://img.shields.io/crates/v/dtn-wal.svg)](https://crates.io/crates/dtn-wal)
[![Docs.rs](https://docs.rs/dtn-wal/badge.svg)](https://docs.rs/dtn-wal)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

## Why

Relay nodes and field gateways in a DTN hold bundles for hours or days until the next contact, and they lose power without warning. After any crash, the queue must come back with exactly what was committed: nothing committed lost, nothing half-written replayed, and damaged data reported rather than quietly dropped.

Since 0.2, `dtn-wal` is its own storage engine (0.1 was a wrapper around SQLite's WAL mode).

## Design

**Frames with fixed-size headers.** Every record is a frame: a 24-byte header followed by the payload.

```text
offset  size  field
     0     4  magic     "DTNW"
     4     1  version   1
     5     1  kind      enqueue | delivered | attempt | purge | commit | snapshot
     6     2  reserved  0
     8     8  seq       +1 for every frame in the log, across segments
    16     4  len       payload length
    20     4  crc       CRC32C of header bytes 0..20 + payload
    24   len  payload
```

**Hardware CRC32C.** The checksum covers the header and the payload, so a flipped bit in a length or sequence number is caught like one in the data. It is computed with the CPU's CRC32C instructions (SSE4.2 on x86-64, ARMv8 CRC on AArch64) through the [`crc32c`](https://crates.io/crates/crc32c) crate.

**Transactions.** Each call writes its data frames, then a commit frame, then `fsync`s (`F_FULLFSYNC` on macOS). `enqueue_batch` commits many bundles atomically. Recovery replays a transaction only if its commit frame is intact.

**Crash-safe log rotation.** The log is split into segment files named by their first sequence number. A new segment is written in full to a `.tmp` file, `fsync`ed, renamed into place, and the directory is `fsync`ed. A crash leaves either no new segment or a complete one, and leftover `.tmp` files are removed on open.

**Checkpoints.** After `compact_after_segments` segments, a checkpoint writes the whole queue into a new segment and deletes the older ones. Replay starts at the newest checkpoint, so a crash between the rename and the deletes is finished on the next open.

**Recovery.** On open, frames are replayed until the first damaged or unfinished one:

- If it is in the newest segment and no later commit follows, it is an unfinished transaction (a torn write, or a kill before the commit frame). The tail is truncated at the end of the last commit.
- If a valid commit of a *later* transaction follows the damage, or the damage is in an older segment or a checkpoint snapshot, committed data is damaged and `open` returns `DtnError::Corrupt` instead of dropping it.

`recovery_report()` says what happened: transactions replayed, records discarded, bytes truncated, temp files and old segments removed.

## Verification

```bash
cargo test                                    # unit + on-disk recovery tests + 300-kill campaign
DTN_WAL_KILL_TRIALS=10000 cargo test --release --test process_kill -- --nocapture
```

- **Torn writes at every byte offset.** A log of 7 transactions is cut at every possible length. Each cut must reopen at exactly the last commit before it, accept new writes, and reopen again cleanly.
- **Bit flips at every byte.** One bit is flipped at every byte of the log. Damage before the newest transaction must give `Corrupt`; damage inside the newest transaction must be truncated and reported.
- **Every single-bit flip in a frame is detected** by the CRC (exhaustive over all bits).
- **Rotation, checkpoints, and crashes during both:** leftover temp segments, pre-checkpoint segments put back after a checkpoint, a damaged checkpoint, a missing segment in the middle.
- **Real process kills** with [`crash-sim`](https://crates.io/crates/crash-sim)'s `KillCampaign`. Each trial starts a real writer process (fsync on, 1 KiB segments so rotations and checkpoints happen every few operations), `SIGKILL`s it after 2–80 ms, recovers a copy of the directory, and compares it with an independent reference model. Every acknowledged operation must be present, and the operation in flight must be all there or not there at all. The same directory carries on to the next trial, so each writer also recovers the previous crash.

Result of the full run (10,000 trials, Apple Silicon Mac, macOS, release build, 510 s):

```text
=== dtn-wal process-kill campaign (crash-sim) ===
10000 trials: 10000 SIGKILLed mid-run, 0 exited before the kill, 112091 stdout lines collected, 510.0s
state checked after every kill: 10000 trials, 0 mismatches
acknowledged operations verified present: 92491
operation in flight at the kill: committed 5063 times, absent 4937 times, never partial
recoveries that cut off an uncommitted transaction: 139 (182 records, 26608 bytes)
kills during rotation/checkpoint: 1875 temp segments removed, 2188 pre-checkpoint segments removed
recoveries that started from a checkpoint: 9999
```

A process kill leaves the operating system running, so it tests crash consistency, not power loss; the torn-write and bit-flip tests above cover what a power cut can leave on disk.

## Quickstart

```toml
[dependencies]
dtn-wal = "0.2"
```

```rust
use dtn_wal::{Bundle, BundleQueue, Priority};

fn main() -> Result<(), dtn_wal::DtnError> {
    // A directory of log segments; created if missing, recovered if a crash left it mid-write.
    let queue = BundleQueue::open("telemetry_queue")?;

    queue.enqueue(&Bundle::new(
        "pkt_001",
        "rover_alpha",
        "base_habitat",
        Priority::LifeCritical,
        b"EMERGENCY_DECOMPRESSION_DETECTED".to_vec(),
        1_730_332_800, // created at (Unix seconds)
        3600,          // TTL
    ))?;

    for b in queue.dequeue_pending(10)? {
        // ... transmit over the radio link ...
        queue.mark_delivered(&b.id)?;
    }
    Ok(())
}
```

Tuning:

```rust
use dtn_wal::{BundleQueue, Options};

let queue = BundleQueue::open_with(
    "telemetry_queue",
    Options::new()
        .max_segment_bytes(1 << 20)  // rotate at 1 MiB (default 4 MiB)
        .compact_after_segments(8),  // checkpoint when 8 segments exist (default)
)?;
```

## API

| Method | |
|---|---|
| `open(dir)`, `open_with(dir, options)`, `open_in_memory()` | open or create; recovery runs on open |
| `enqueue(&bundle)` | insert, or update priority, payload and TTL of an existing id |
| `enqueue_batch(&[bundle])` | one atomic transaction |
| `dequeue_pending(limit)` | undelivered bundles, highest priority first, then oldest |
| `mark_delivered(id)`, `record_attempt(id, ts)` | delivery bookkeeping |
| `purge_expired(now)` | drop bundles past their TTL |
| `get(id)`, `entries()`, `count_pending()` | inspect the queue |
| `checkpoint()` | write a checkpoint now |
| `recovery_report()`, `segment_count()` | what recovery did; segments in use |

## Migrating from 0.1

0.1 stored the queue in one SQLite file; 0.2 stores a directory of segments, and the `rusqlite` and `serde_json` dependencies are gone. Pass a directory path to `open`; passing an old SQLite file gives `DtnError::NotADirectory`. The queue methods are unchanged. `DtnError::Database` and `DtnError::Serialization` were replaced by `Io`, `Corrupt`, `RecordTooLarge` and `Poisoned`.

## Limits

- One process per queue directory; there is no cross-process lock.
- A damaged frame inside the newest transaction cannot be told apart from a torn write, so it is truncated (and reported in `recovery_report`), not returned as an error.
- `Options::fsync(false)` keeps data safe across a process crash, not across a power cut or OS crash.

## License

Dual-licensed under either:
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
