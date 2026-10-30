# dtn-wal

A crash-resilient, append-only SQLite Write-Ahead Log (WAL) store-and-forward bundle queue designed for Delay-Tolerant Networks (DTN), intermittent radio links, and off-grid sensor meshes.

[![Crates.io](https://img.shields.io/crates/v/dtn-wal.svg)](https://crates.io/crates/dtn-wal)
[![Docs.rs](https://docs.rs/dtn-wal/badge.svg)](https://docs.rs/dtn-wal)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)

## Overview

In disrupted environments—such as satellite passes, deep-space telemetry, disaster-zone networks, or physical sub-GHz LoRa meshes—uninterrupted end-to-end TCP/IP connections are impossible. 

`dtn-wal` provides an embedded, transactional persistence layer that guarantees zero message loss during power interruptions, abrupt shutdowns, and network link drops. Every bundle is written to disk via SQLite's Write-Ahead Log (WAL) before opportunistic transmission.

## Key Features

- **Crash-Resilient Persistence:** Uses SQLite in WAL mode with robust busy timeouts to prevent database corruption during sudden power cuts.
- **Priority-Ordered Queuing:** Four deterministic priority tiers (`Bulk`, `Normal`, `Expedited`, `LifeCritical`). High-priority emergency alerts are always dispatched first upon peer reconnection.
- **Store-and-Forward Lifecycle:** Decouples message creation from transmission; buffers bundles indefinitely until peer contact is established.
- **Automatic TTL Expiration:** Purges stale packets according to configurable time-to-live timestamps.
- **Zero-Unsafe Core:** 100% memory-safe Rust with minimal dependency overhead.

## Quickstart

Add `dtn-wal` to your `Cargo.toml`:

```toml
[dependencies]
dtn-wal = "0.1.0"
```

### Basic Example

```rust
use dtn_wal::{Bundle, BundleQueue, Priority};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open or create a persistent queue file
    let queue = BundleQueue::open("telemetry_queue.db")?;

    // Create a high-priority life-critical bundle
    let bundle = Bundle::new(
        "pkt_001",
        "rover_alpha",
        "base_habitat",
        Priority::LifeCritical,
        b"EMERGENCY_DECOMPRESSION_DETECTED".to_vec(),
        1730332800, // Unix timestamp
        3600,       // 1 hour TTL
    );

    // Enqueue bundle safely to disk
    queue.enqueue(&bundle)?;

    // Dequeue pending bundles ordered by priority
    let pending = queue.dequeue_pending(10)?;
    for b in pending {
        println!("Dispatching bundle: {} (Priority: {:?})", b.id, b.priority);
        
        // Once successfully transmitted over radio:
        queue.mark_delivered(&b.id)?;
    }

    Ok(())
}
```

## Architecture & Guarantees

`dtn-wal` executes the following pragmas on every connection:
- `PRAGMA journal_mode = WAL;` (Concurrent readers, sequential zero-overhead appends)
- `PRAGMA synchronous = NORMAL;` (Protects against corruption on system crashes while maximizing flash memory lifespan)
- `PRAGMA busy_timeout = 5000;` (Handles concurrent thread access without throwing lock errors)

## License

Dual-licensed under either:
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
