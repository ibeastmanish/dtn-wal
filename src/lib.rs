//! # dtn-wal
//!
//! A crash-safe, append-only write-ahead log and store-and-forward bundle queue for
//! Delay-Tolerant Networks (DTN), intermittent radio links and off-grid sensor meshes.
//!
//! Relay nodes and field gateways lose power without warning. `dtn-wal` keeps every bundle in its
//! own log so that, after any crash, the queue reopens with exactly the transactions that were
//! committed: nothing committed is lost, nothing half-written is replayed, and damaged committed
//! data is reported instead of silently dropped.
//!
//! ## How it works
//!
//! * **Frames.** Every record is a frame with a fixed-size 24-byte header (magic, version, record
//!   kind, sequence number, payload length, CRC32C) followed by the payload.
//! * **Checksums.** CRC32C covers the header and the payload. It is computed with hardware CRC
//!   instructions (SSE4.2 on x86-64, ARMv8 CRC32C on AArch64) through the `crc32c` crate.
//! * **Transactions.** Each call writes its data frames and then a commit frame, followed by
//!   `fsync` (`F_FULLFSYNC` on macOS). [`BundleQueue::enqueue_batch`] commits many bundles atomically.
//! * **Rotation and checkpoints.** The log is split into segment files. Full segments are rotated
//!   through a temp file, `fsync` and rename; after enough segments a checkpoint writes the whole
//!   queue to a new segment and the old ones are deleted.
//! * **Recovery.** On open, replay starts at the newest checkpoint, stops at the first damaged or
//!   unfinished frame, and truncates the uncommitted tail. See [`RecoveryReport`].
//!
//! ```no_run
//! use dtn_wal::{Bundle, BundleQueue, Priority};
//!
//! # fn main() -> Result<(), dtn_wal::DtnError> {
//! let queue = BundleQueue::open("telemetry_queue")?;
//! queue.enqueue(&Bundle::new(
//!     "pkt_001", "rover_alpha", "base_habitat",
//!     Priority::LifeCritical, b"EMERGENCY".to_vec(), 1_730_332_800, 3600,
//! ))?;
//! for b in queue.dequeue_pending(10)? {
//!     // ... transmit ...
//!     queue.mark_delivered(&b.id)?;
//! }
//! # Ok(())
//! # }
//! ```

mod frame;
mod log;
mod record;
mod state;

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::log::Log;
use crate::record::Record;
use crate::state::State;

pub use crate::state::Entry;

#[derive(Error, Debug)]
pub enum DtnError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Committed data is damaged. The queue refuses to open rather than drop it silently.
    #[error("log is corrupt in {path} at byte {offset}: {reason}")]
    Corrupt {
        path: PathBuf,
        offset: u64,
        reason: String,
    },
    #[error(
        "{0} exists and is not a directory (dtn-wal 0.1 used a single SQLite file; 0.2 stores a directory of log segments)"
    )]
    NotADirectory(PathBuf),
    #[error("record of {0} bytes exceeds the 64 MiB frame limit")]
    RecordTooLarge(usize),
    #[error("an earlier write failed and could not be rolled back; reopen the queue")]
    Poisoned,
    #[error("Bundle not found: {0}")]
    NotFound(String),
}

/// Priority tier for bundle dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum Priority {
    Bulk = 0,
    Normal = 1,
    Expedited = 2,
    LifeCritical = 3,
}

impl Priority {
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn from_u8(val: u8) -> Self {
        match val {
            3 => Priority::LifeCritical,
            2 => Priority::Expedited,
            1 => Priority::Normal,
            _ => Priority::Bulk,
        }
    }
}

/// A self-contained Delay-Tolerant Networking bundle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Bundle {
    pub id: String,
    pub source: String,
    pub destination: String,
    pub priority: Priority,
    pub payload: Vec<u8>,
    pub created_at: u64,
    pub ttl_seconds: u64,
}

impl Bundle {
    pub fn new(
        id: impl Into<String>,
        source: impl Into<String>,
        destination: impl Into<String>,
        priority: Priority,
        payload: Vec<u8>,
        created_at: u64,
        ttl_seconds: u64,
    ) -> Self {
        Self {
            id: id.into(),
            source: source.into(),
            destination: destination.into(),
            priority,
            payload,
            created_at,
            ttl_seconds,
        }
    }

    pub fn is_expired(&self, current_time: u64) -> bool {
        current_time > self.created_at.saturating_add(self.ttl_seconds)
    }
}

/// Tuning for an on-disk queue.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub(crate) max_segment_bytes: u64,
    pub(crate) compact_after_segments: usize,
    pub(crate) fsync: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_segment_bytes: 4 * 1024 * 1024,
            compact_after_segments: 8,
            fsync: true,
        }
    }
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rotate to a new segment once the active one reaches this size (minimum 256 bytes).
    pub fn max_segment_bytes(mut self, bytes: u64) -> Self {
        self.max_segment_bytes = bytes.max(256);
        self
    }

    /// Write a checkpoint instead of rotating once this many segments exist (minimum 2).
    pub fn compact_after_segments(mut self, segments: usize) -> Self {
        self.compact_after_segments = segments.max(2);
        self
    }

    /// `fsync` every commit, rotation and checkpoint (default `true`). Turning it off keeps data
    /// safe across a process crash but not across a power cut or OS crash.
    pub fn fsync(mut self, on: bool) -> Self {
        self.fsync = on;
        self
    }
}

/// What recovery found and did when the queue was opened.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Replay started from a checkpoint segment.
    pub checkpoint_used: bool,
    pub segments_replayed: usize,
    pub transactions_replayed: u64,
    pub records_replayed: u64,
    /// Whole records of an unfinished transaction that were dropped.
    pub discarded_records: u64,
    /// Bytes cut off the end of the newest segment.
    pub truncated_bytes: u64,
    /// `*.tmp` files left by a crash during rotation or a checkpoint.
    pub removed_temp_files: usize,
    /// Segments older than the newest checkpoint, left by a crash during a checkpoint.
    pub removed_old_segments: usize,
}

struct Inner {
    state: State,
    log: Option<Log>,
    report: RecoveryReport,
}

/// A crash-safe store-and-forward queue backed by a segmented write-ahead log.
pub struct BundleQueue {
    inner: Mutex<Inner>,
}

impl BundleQueue {
    /// Opens or creates a queue in the directory `path`, recovering it if needed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DtnError> {
        Self::open_with(path, Options::default())
    }

    pub fn open_with(path: impl AsRef<Path>, options: Options) -> Result<Self, DtnError> {
        let (log, state, report) = Log::open(path.as_ref(), options)?;
        Ok(Self::from_parts(state, Some(log), report))
    }

    /// A queue that lives only in memory, for tests and transient caching.
    pub fn open_in_memory() -> Result<Self, DtnError> {
        Ok(Self::from_parts(State::default(), None, RecoveryReport::default()))
    }

    fn from_parts(state: State, log: Option<Log>, report: RecoveryReport) -> Self {
        Self {
            inner: Mutex::new(Inner { state, log, report }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Commits `records` as one transaction, then applies them in memory.
    fn write(inner: &mut Inner, records: &[Record]) -> Result<(), DtnError> {
        if let Some(log) = inner.log.as_mut() {
            log.prepare(&inner.state)?;
            log.commit(records)?;
        }
        for r in records {
            inner.state.apply(r);
        }
        Ok(())
    }

    /// Enqueues a bundle. If the id already exists, its priority, payload and TTL are updated.
    pub fn enqueue(&self, bundle: &Bundle) -> Result<(), DtnError> {
        Self::write(&mut self.lock(), &[Record::Enqueue(bundle.clone())])
    }

    /// Enqueues several bundles in one transaction: after a crash, either all are present or none.
    pub fn enqueue_batch(&self, bundles: &[Bundle]) -> Result<(), DtnError> {
        if bundles.is_empty() {
            return Ok(());
        }
        let records: Vec<Record> = bundles.iter().cloned().map(Record::Enqueue).collect();
        Self::write(&mut self.lock(), &records)
    }

    /// Pending (undelivered) bundles, highest priority first, then oldest first.
    pub fn dequeue_pending(&self, limit: usize) -> Result<Vec<Bundle>, DtnError> {
        let inner = self.lock();
        let mut pending: Vec<&Entry> = inner.state.entries.values().filter(|e| !e.delivered).collect();
        pending.sort_by(|a, b| {
            b.bundle
                .priority
                .cmp(&a.bundle.priority)
                .then(a.bundle.created_at.cmp(&b.bundle.created_at))
                .then_with(|| a.bundle.id.cmp(&b.bundle.id))
        });
        Ok(pending.into_iter().take(limit).map(|e| e.bundle.clone()).collect())
    }

    /// Marks a bundle as delivered. Returns `false` if no bundle has this id.
    pub fn mark_delivered(&self, id: &str) -> Result<bool, DtnError> {
        let mut inner = self.lock();
        if !inner.state.entries.contains_key(id) {
            return Ok(false);
        }
        Self::write(&mut inner, &[Record::Delivered(id.to_string())])?;
        Ok(true)
    }

    /// Counts a transmission attempt. Does nothing if no bundle has this id.
    pub fn record_attempt(&self, id: &str, timestamp: u64) -> Result<(), DtnError> {
        let mut inner = self.lock();
        if !inner.state.entries.contains_key(id) {
            return Ok(());
        }
        Self::write(
            &mut inner,
            &[Record::Attempt {
                id: id.to_string(),
                timestamp,
            }],
        )
    }

    /// Number of pending (undelivered) bundles.
    pub fn count_pending(&self) -> Result<usize, DtnError> {
        Ok(self.lock().state.entries.values().filter(|e| !e.delivered).count())
    }

    /// Removes every bundle whose TTL has passed at `current_time`; returns how many.
    pub fn purge_expired(&self, current_time: u64) -> Result<usize, DtnError> {
        let mut inner = self.lock();
        let n = inner.state.expired_count(current_time);
        if n > 0 {
            Self::write(&mut inner, &[Record::Purge { now: current_time }])?;
        }
        Ok(n)
    }

    /// One bundle and its delivery bookkeeping.
    pub fn get(&self, id: &str) -> Option<Entry> {
        self.lock().state.entries.get(id).cloned()
    }

    /// Every entry, delivered or not, sorted by id.
    pub fn entries(&self) -> Vec<Entry> {
        self.lock().state.sorted()
    }

    /// Writes a checkpoint now: the whole queue goes into a new segment and older segments are
    /// deleted. Checkpoints also happen automatically (see [`Options::compact_after_segments`]).
    pub fn checkpoint(&self) -> Result<(), DtnError> {
        let mut inner = self.lock();
        let Inner { state, log, .. } = &mut *inner;
        match log {
            Some(log) => log.checkpoint(state),
            None => Ok(()),
        }
    }

    /// What recovery did when this queue was opened.
    pub fn recovery_report(&self) -> RecoveryReport {
        self.lock().report.clone()
    }

    /// Number of segment files in use (0 for an in-memory queue).
    pub fn segment_count(&self) -> usize {
        self.lock().log.as_ref().map_or(0, Log::segment_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enqueue_and_priority_order() {
        let queue = BundleQueue::open_in_memory().unwrap();

        let low_pri = Bundle::new("b1", "nodeA", "nodeB", Priority::Bulk, b"low".to_vec(), 100, 3600);
        let crit_pri = Bundle::new("b2", "nodeA", "nodeB", Priority::LifeCritical, b"crit".to_vec(), 105, 3600);
        let med_pri = Bundle::new("b3", "nodeA", "nodeB", Priority::Normal, b"normal".to_vec(), 90, 3600);

        queue.enqueue(&low_pri).unwrap();
        queue.enqueue(&crit_pri).unwrap();
        queue.enqueue(&med_pri).unwrap();

        assert_eq!(queue.count_pending().unwrap(), 3);

        let pending = queue.dequeue_pending(10).unwrap();
        assert_eq!(pending.len(), 3);
        assert_eq!(pending[0].id, "b2"); // LifeCritical first
        assert_eq!(pending[1].id, "b3"); // Normal second
        assert_eq!(pending[2].id, "b1"); // Bulk third
    }

    #[test]
    fn test_mark_delivered() {
        let queue = BundleQueue::open_in_memory().unwrap();
        let b = Bundle::new("b1", "nodeA", "nodeB", Priority::Normal, b"data".to_vec(), 100, 3600);
        queue.enqueue(&b).unwrap();

        assert_eq!(queue.count_pending().unwrap(), 1);
        assert!(queue.mark_delivered("b1").unwrap());
        assert!(!queue.mark_delivered("missing").unwrap());
        assert_eq!(queue.count_pending().unwrap(), 0);
    }

    #[test]
    fn test_purge_expired() {
        let queue = BundleQueue::open_in_memory().unwrap();
        let expired = Bundle::new("exp", "nodeA", "nodeB", Priority::Bulk, b"old".to_vec(), 100, 50); // expires at 150
        let fresh = Bundle::new("fresh", "nodeA", "nodeB", Priority::Bulk, b"new".to_vec(), 100, 500); // expires at 600

        queue.enqueue(&expired).unwrap();
        queue.enqueue(&fresh).unwrap();

        assert_eq!(queue.purge_expired(200).unwrap(), 1);

        let pending = queue.dequeue_pending(10).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "fresh");
    }

    #[test]
    fn upsert_keeps_source_and_delivery_state() {
        let queue = BundleQueue::open_in_memory().unwrap();
        queue.enqueue(&Bundle::new("x", "a", "b", Priority::Bulk, b"1".to_vec(), 10, 100)).unwrap();
        queue.mark_delivered("x").unwrap();
        queue.enqueue(&Bundle::new("x", "c", "d", Priority::LifeCritical, b"2".to_vec(), 99, 5)).unwrap();
        let e = queue.get("x").unwrap();
        assert_eq!((e.bundle.source.as_str(), e.bundle.created_at), ("a", 10));
        assert_eq!((e.bundle.priority, e.bundle.payload.as_slice(), e.bundle.ttl_seconds), (Priority::LifeCritical, &b"2"[..], 5));
        assert!(e.delivered);
    }
}
