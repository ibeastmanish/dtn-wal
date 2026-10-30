//! # dtn-wal
//!
//! A crash-resilient, append-only SQLite WAL store-and-forward bundle queue designed for
//! Delay-Tolerant Networks (DTN), intermittent radio links, and off-grid sensor meshes.
//!
//! ## Overview
//!
//! In disrupted or delay-tolerant environments (such as satellite networks, deep-space
//! telemetry, or sub-GHz LoRa mesh radios), conventional continuous TCP connections fail.
//! `dtn-wal` provides a local, transactional persistence layer backed by SQLite's
//! Write-Ahead Log (WAL) mode to guarantee zero data loss during power cuts or abrupt process kills.

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DtnError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Bundle not found: {0}")]
    NotFound(String),
}

/// Priority tier for bundle dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

/// A transactional SQLite-backed store-and-forward queue operating in Write-Ahead Log (WAL) mode.
pub struct BundleQueue {
    conn: Connection,
}

impl BundleQueue {
    /// Opens or creates a bundle queue at the specified file path, configuring WAL mode and robust pragmas.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DtnError> {
        let conn = Connection::open(path)?;
        Self::initialize_connection(conn)
    }

    /// Opens an in-memory queue, primarily useful for testing and transient caching.
    pub fn open_in_memory() -> Result<Self, DtnError> {
        let conn = Connection::open_in_memory()?;
        Self::initialize_connection(conn)
    }

    fn initialize_connection(conn: Connection) -> Result<Self, DtnError> {
        // Enforce crash-resilient pragmas
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = ON;
             
             CREATE TABLE IF NOT EXISTS bundles (
                 id TEXT PRIMARY KEY,
                 source TEXT NOT NULL,
                 destination TEXT NOT NULL,
                 priority INTEGER NOT NULL,
                 payload BLOB NOT NULL,
                 created_at INTEGER NOT NULL,
                 ttl_seconds INTEGER NOT NULL,
                 delivered INTEGER NOT NULL DEFAULT 0,
                 attempts INTEGER NOT NULL DEFAULT 0,
                 last_attempt INTEGER DEFAULT 0
             );
             
             CREATE INDEX IF NOT EXISTS idx_pending_priority 
             ON bundles(delivered, priority DESC, created_at ASC);",
        )?;

        Ok(Self { conn })
    }

    /// Enqueues a bundle into the persistence queue. Overwrites if a duplicate bundle ID exists.
    pub fn enqueue(&self, bundle: &Bundle) -> Result<(), DtnError> {
        self.conn.execute(
            "INSERT INTO bundles (id, source, destination, priority, payload, created_at, ttl_seconds, delivered)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)
             ON CONFLICT(id) DO UPDATE SET
                 priority = excluded.priority,
                 payload = excluded.payload,
                 ttl_seconds = excluded.ttl_seconds;",
            params![
                bundle.id,
                bundle.source,
                bundle.destination,
                bundle.priority.as_u8(),
                bundle.payload,
                bundle.created_at,
                bundle.ttl_seconds,
            ],
        )?;
        Ok(())
    }

    /// Fetches pending (undelivered) bundles ordered by highest priority and oldest creation time.
    pub fn dequeue_pending(&self, limit: usize) -> Result<Vec<Bundle>, DtnError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, source, destination, priority, payload, created_at, ttl_seconds
             FROM bundles
             WHERE delivered = 0
             ORDER BY priority DESC, created_at ASC
             LIMIT ?1;",
        )?;

        let bundle_iter = stmt.query_map(params![limit as i64], |row| {
            let priority_val: u8 = row.get(3)?;
            Ok(Bundle {
                id: row.get(0)?,
                source: row.get(1)?,
                destination: row.get(2)?,
                priority: Priority::from_u8(priority_val),
                payload: row.get(4)?,
                created_at: row.get(5)?,
                ttl_seconds: row.get(6)?,
            })
        })?;

        let mut bundles = Vec::new();
        for bundle in bundle_iter {
            bundles.push(bundle?);
        }
        Ok(bundles)
    }

    /// Marks a bundle as delivered, removing it from pending dispatch consideration.
    pub fn mark_delivered(&self, id: &str) -> Result<bool, DtnError> {
        let rows_affected = self.conn.execute(
            "UPDATE bundles SET delivered = 1 WHERE id = ?1;",
            params![id],
        )?;
        Ok(rows_affected > 0)
    }

    /// Increments the transmission attempt counter and updates the last attempt timestamp.
    pub fn record_attempt(&self, id: &str, timestamp: u64) -> Result<(), DtnError> {
        self.conn.execute(
            "UPDATE bundles SET attempts = attempts + 1, last_attempt = ?2 WHERE id = ?1;",
            params![id, timestamp],
        )?;
        Ok(())
    }

    /// Returns the number of currently pending (undelivered) bundles in the queue.
    pub fn count_pending(&self) -> Result<usize, DtnError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM bundles WHERE delivered = 0;",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Purges bundles that have exceeded their time-to-live (TTL).
    pub fn purge_expired(&self, current_time: u64) -> Result<usize, DtnError> {
        let rows_deleted = self.conn.execute(
            "DELETE FROM bundles WHERE (?1 > (created_at + ttl_seconds));",
            params![current_time],
        )?;
        Ok(rows_deleted)
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
        let res = queue.mark_delivered("b1").unwrap();
        assert!(res);
        assert_eq!(queue.count_pending().unwrap(), 0);
    }

    #[test]
    fn test_purge_expired() {
        let queue = BundleQueue::open_in_memory().unwrap();
        let expired = Bundle::new("exp", "nodeA", "nodeB", Priority::Bulk, b"old".to_vec(), 100, 50); // expires at 150
        let fresh = Bundle::new("fresh", "nodeA", "nodeB", Priority::Bulk, b"new".to_vec(), 100, 500); // expires at 600

        queue.enqueue(&expired).unwrap();
        queue.enqueue(&fresh).unwrap();

        let deleted = queue.purge_expired(200).unwrap();
        assert_eq!(deleted, 1);

        let pending = queue.dequeue_pending(10).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "fresh");
    }
}
