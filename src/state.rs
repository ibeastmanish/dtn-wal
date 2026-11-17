//! In-memory queue state, rebuilt on open by replaying committed records.

use std::collections::HashMap;

use crate::record::Record;
use crate::Bundle;

/// One bundle in the queue together with its delivery bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub bundle: Bundle,
    pub delivered: bool,
    pub attempts: u32,
    pub last_attempt: u64,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct State {
    pub(crate) entries: HashMap<String, Entry>,
}

impl State {
    /// Applies one data record. `Commit` records are a no-op here; the log handles them.
    pub(crate) fn apply(&mut self, record: &Record) {
        match record {
            Record::Enqueue(b) => match self.entries.get_mut(&b.id) {
                // Same rule as 0.1's `ON CONFLICT DO UPDATE`: source, destination,
                // creation time and delivery bookkeeping are kept.
                Some(e) => {
                    e.bundle.priority = b.priority;
                    e.bundle.payload = b.payload.clone();
                    e.bundle.ttl_seconds = b.ttl_seconds;
                }
                None => {
                    self.entries.insert(
                        b.id.clone(),
                        Entry {
                            bundle: b.clone(),
                            delivered: false,
                            attempts: 0,
                            last_attempt: 0,
                        },
                    );
                }
            },
            Record::Delivered(id) => {
                if let Some(e) = self.entries.get_mut(id) {
                    e.delivered = true;
                }
            }
            Record::Attempt { id, timestamp } => {
                if let Some(e) = self.entries.get_mut(id) {
                    e.attempts = e.attempts.saturating_add(1);
                    e.last_attempt = *timestamp;
                }
            }
            Record::Purge { now } => {
                self.entries.retain(|_, e| !e.bundle.is_expired(*now));
            }
            Record::Snapshot(e) => {
                self.entries.insert(e.bundle.id.clone(), e.clone());
            }
            Record::Commit { .. } => {}
        }
    }

    pub(crate) fn expired_count(&self, now: u64) -> usize {
        self.entries.values().filter(|e| e.bundle.is_expired(now)).count()
    }

    /// All entries sorted by id, for checkpoints and comparisons.
    pub(crate) fn sorted(&self) -> Vec<Entry> {
        let mut v: Vec<Entry> = self.entries.values().cloned().collect();
        v.sort_by(|a, b| a.bundle.id.cmp(&b.bundle.id));
        v
    }
}
