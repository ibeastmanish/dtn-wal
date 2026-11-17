//! Log records and their binary encoding (the payload of a frame).
//!
//! Integers are little-endian; strings and byte arrays are a `u32` length followed by the bytes.
//! A transaction is any number of data records followed by one `Commit` record; recovery applies
//! a transaction only when its `Commit` frame is present and valid.

use crate::state::Entry;
use crate::{Bundle, Priority};

pub(crate) const KIND_ENQUEUE: u8 = 1;
pub(crate) const KIND_DELIVERED: u8 = 2;
pub(crate) const KIND_ATTEMPT: u8 = 3;
pub(crate) const KIND_PURGE: u8 = 4;
pub(crate) const KIND_COMMIT: u8 = 5;
pub(crate) const KIND_SNAPSHOT: u8 = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Record {
    /// Insert a bundle, or update priority, payload and TTL of an existing one.
    Enqueue(Bundle),
    Delivered(String),
    Attempt { id: String, timestamp: u64 },
    /// Remove every bundle with `now > created_at + ttl_seconds`.
    Purge { now: u64 },
    /// Ends a transaction of `records` data records.
    Commit { txn: u64, records: u32 },
    /// One full queue entry, written by a checkpoint.
    Snapshot(Entry),
}

impl Record {
    pub(crate) fn kind(&self) -> u8 {
        match self {
            Record::Enqueue(_) => KIND_ENQUEUE,
            Record::Delivered(_) => KIND_DELIVERED,
            Record::Attempt { .. } => KIND_ATTEMPT,
            Record::Purge { .. } => KIND_PURGE,
            Record::Commit { .. } => KIND_COMMIT,
            Record::Snapshot(_) => KIND_SNAPSHOT,
        }
    }

    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Record::Enqueue(b) => put_bundle(out, b),
            Record::Delivered(id) => put_bytes(out, id.as_bytes()),
            Record::Attempt { id, timestamp } => {
                put_bytes(out, id.as_bytes());
                out.extend_from_slice(&timestamp.to_le_bytes());
            }
            Record::Purge { now } => out.extend_from_slice(&now.to_le_bytes()),
            Record::Commit { txn, records } => {
                out.extend_from_slice(&txn.to_le_bytes());
                out.extend_from_slice(&records.to_le_bytes());
            }
            Record::Snapshot(e) => {
                put_bundle(out, &e.bundle);
                out.push(e.delivered as u8);
                out.extend_from_slice(&e.attempts.to_le_bytes());
                out.extend_from_slice(&e.last_attempt.to_le_bytes());
            }
        }
    }

    pub(crate) fn decode(kind: u8, payload: &[u8]) -> Result<Record, &'static str> {
        let mut r = Reader { buf: payload, pos: 0 };
        let record = match kind {
            KIND_ENQUEUE => Record::Enqueue(r.bundle()?),
            KIND_DELIVERED => Record::Delivered(r.string()?),
            KIND_ATTEMPT => Record::Attempt {
                id: r.string()?,
                timestamp: r.u64()?,
            },
            KIND_PURGE => Record::Purge { now: r.u64()? },
            KIND_COMMIT => Record::Commit {
                txn: r.u64()?,
                records: r.u32()?,
            },
            KIND_SNAPSHOT => {
                let bundle = r.bundle()?;
                let delivered = match r.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err("bad delivered flag"),
                };
                Record::Snapshot(Entry {
                    bundle,
                    delivered,
                    attempts: r.u32()?,
                    last_attempt: r.u64()?,
                })
            }
            _ => return Err("unknown record kind"),
        };
        if r.pos != payload.len() {
            return Err("trailing bytes in record");
        }
        Ok(record)
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn put_bundle(out: &mut Vec<u8>, b: &Bundle) {
    put_bytes(out, b.id.as_bytes());
    put_bytes(out, b.source.as_bytes());
    put_bytes(out, b.destination.as_bytes());
    out.push(b.priority.as_u8());
    put_bytes(out, &b.payload);
    out.extend_from_slice(&b.created_at.to_le_bytes());
    out.extend_from_slice(&b.ttl_seconds.to_le_bytes());
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        let end = self.pos.checked_add(n).ok_or("length overflow")?;
        let s = self.buf.get(self.pos..end).ok_or("record truncated")?;
        self.pos = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, &'static str> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, &'static str> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, &'static str> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, &'static str> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    fn string(&mut self) -> Result<String, &'static str> {
        String::from_utf8(self.bytes()?).map_err(|_| "string is not UTF-8")
    }
    fn bundle(&mut self) -> Result<Bundle, &'static str> {
        let id = self.string()?;
        let source = self.string()?;
        let destination = self.string()?;
        let priority = match self.u8()? {
            p @ 0..=3 => Priority::from_u8(p),
            _ => return Err("bad priority"),
        };
        Ok(Bundle {
            id,
            source,
            destination,
            priority,
            payload: self.bytes()?,
            created_at: self.u64()?,
            ttl_seconds: self.u64()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Bundle {
        Bundle::new("id-1", "rover", "base", Priority::Expedited, vec![0, 1, 2, 255], 1_730_000_000, 600)
    }

    #[test]
    fn every_record_round_trips() {
        let records = [
            Record::Enqueue(sample()),
            Record::Delivered("id-1".into()),
            Record::Attempt { id: "id-1".into(), timestamp: 77 },
            Record::Purge { now: u64::MAX },
            Record::Commit { txn: 9, records: 3 },
            Record::Snapshot(Entry { bundle: sample(), delivered: true, attempts: 4, last_attempt: 12 }),
        ];
        for rec in records {
            let mut buf = Vec::new();
            rec.encode(&mut buf);
            assert_eq!(Record::decode(rec.kind(), &buf), Ok(rec.clone()));
            // Any shortened payload must be rejected, never misread.
            for cut in 0..buf.len() {
                assert!(Record::decode(rec.kind(), &buf[..cut]).is_err());
            }
            buf.push(0);
            assert!(Record::decode(rec.kind(), &buf).is_err());
        }
    }
}
