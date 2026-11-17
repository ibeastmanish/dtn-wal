//! Segmented append-only log: commit, rotation, checkpoints and crash recovery.
//!
//! ## Layout
//!
//! A queue is a directory of segment files named after the sequence number of their first
//! frame, e.g. `00000000000000000001.seg`. Each segment starts with a 16-byte header
//!
//! ```text
//! b"DTNWSEG1" | flags (u8, bit 0 = checkpoint) | 3 reserved zero bytes | CRC32C of bytes 0..12
//! ```
//!
//! followed by frames (see `frame.rs`). Frame sequence numbers run on without gaps across
//! segments, and a transaction never spans two segments.
//!
//! ## Crash-safety rules
//!
//! * A transaction is its data frames followed by a commit frame, then `fsync`. Only
//!   transactions whose commit frame is intact are replayed.
//! * New segments (rotation and checkpoints) are written in full to `*.tmp`, `fsync`ed, renamed
//!   into place, and then the directory is `fsync`ed. A crash leaves either no new segment or a
//!   complete one; leftover `*.tmp` files are deleted on open.
//! * A checkpoint segment holds a snapshot of the whole queue. On open, replay starts at the newest
//!   checkpoint and every older segment is deleted, so a crash after the rename but before the old
//!   segments were removed is finished on the next open.
//! * Only the newest segment may end in an unfinished transaction (a torn write or a kill before
//!   the commit frame). Recovery cuts it off at the end of the last commit. If a valid commit of a
//!   *later* transaction is found after a damaged frame, the damage is in committed data and
//!   `open` fails with [`DtnError::Corrupt`] instead of silently dropping it.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::frame::{self, Decoded, MAX_PAYLOAD};
use crate::record::{Record, KIND_COMMIT};
use crate::state::State;
use crate::{DtnError, Options, RecoveryReport};

const SEG_MAGIC: [u8; 8] = *b"DTNWSEG1";
pub(crate) const SEG_HEADER_LEN: usize = 16;
const FLAG_CHECKPOINT: u8 = 1;

pub(crate) fn segment_name(base: u64) -> String {
    format!("{base:020}.seg")
}

fn parse_segment_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".seg")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn segment_header(flags: u8) -> [u8; SEG_HEADER_LEN] {
    let mut h = [0u8; SEG_HEADER_LEN];
    h[..8].copy_from_slice(&SEG_MAGIC);
    h[8] = flags;
    let crc = crc32c::crc32c(&h[..12]);
    h[12..].copy_from_slice(&crc.to_le_bytes());
    h
}

fn parse_segment_header(buf: &[u8]) -> Option<u8> {
    let h = buf.get(..SEG_HEADER_LEN)?;
    let crc = u32::from_le_bytes(h[12..16].try_into().unwrap());
    let flags = h[8];
    (h[..8] == SEG_MAGIC
        && h[9..12] == [0, 0, 0]
        && flags & !FLAG_CHECKPOINT == 0
        && crc32c::crc32c(&h[..12]) == crc)
        .then_some(flags)
}

fn corrupt(path: &Path, offset: usize, reason: impl Into<String>) -> DtnError {
    DtnError::Corrupt {
        path: path.to_path_buf(),
        offset: offset as u64,
        reason: reason.into(),
    }
}

fn sync_dir(dir: &Path, opts: &Options) -> io::Result<()> {
    if opts.fsync {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

fn encode_frame(record: &Record, seq: u64, out: &mut Vec<u8>) -> Result<(), DtnError> {
    let mut payload = Vec::new();
    record.encode(&mut payload);
    if payload.len() > MAX_PAYLOAD as usize {
        return Err(DtnError::RecordTooLarge(payload.len()));
    }
    frame::encode(record.kind(), seq, &payload, out);
    Ok(())
}

/// Writes a complete segment through a temp file and renames it into place.
fn create_segment(dir: &Path, base: u64, flags: u8, body: &[u8], opts: &Options) -> io::Result<File> {
    let fin = dir.join(segment_name(base));
    let tmp = dir.join(format!("{}.tmp", segment_name(base)));
    {
        let mut f = OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;
        f.write_all(&segment_header(flags))?;
        f.write_all(body)?;
        if opts.fsync {
            f.sync_all()?;
        }
    }
    fs::rename(&tmp, &fin)?;
    sync_dir(dir, opts)?;
    OpenOptions::new().append(true).open(&fin)
}

/// After a damaged frame at `from`, looks for a valid commit of a transaction that started after
/// `first_uncommitted`. Finding one means committed data was damaged, not just an unfinished tail.
fn later_commit(buf: &[u8], from: usize, first_uncommitted: u64) -> Option<u64> {
    let last_start = buf.len().checked_sub(frame::HEADER_LEN)?;
    (from + 1..=last_start)
        .filter(|&p| buf[p..p + 4] == frame::MAGIC)
        .find_map(|p| match frame::decode(buf, p) {
            Decoded::Frame { header, payload, .. } if header.kind == KIND_COMMIT => {
                match Record::decode(KIND_COMMIT, payload) {
                    Ok(Record::Commit { records, .. })
                        if header.seq.saturating_sub(records as u64) > first_uncommitted =>
                    {
                        Some(header.seq)
                    }
                    _ => None,
                }
            }
            _ => None,
        })
}

pub(crate) struct Log {
    dir: PathBuf,
    opts: Options,
    file: File,
    active_base: u64,
    active_len: u64,
    /// Base sequence numbers of live segments, oldest first; the last one is active.
    segments: Vec<u64>,
    next_seq: u64,
    next_txn: u64,
    poisoned: bool,
}

impl Log {
    pub(crate) fn open(dir: &Path, opts: Options) -> Result<(Log, State, RecoveryReport), DtnError> {
        if dir.exists() && !dir.is_dir() {
            return Err(DtnError::NotADirectory(dir.to_path_buf()));
        }
        fs::create_dir_all(dir)?;
        let mut report = RecoveryReport::default();

        let mut bases = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.ends_with(".seg.tmp") {
                fs::remove_file(entry.path())?;
                report.removed_temp_files += 1;
            } else if let Some(base) = parse_segment_name(name) {
                bases.push(base);
            }
        }
        bases.sort_unstable();
        if report.removed_temp_files > 0 {
            sync_dir(dir, &opts)?;
        }

        if bases.is_empty() {
            let file = create_segment(dir, 1, 0, &[], &opts)?;
            let log = Log {
                dir: dir.to_path_buf(),
                opts,
                file,
                active_base: 1,
                active_len: SEG_HEADER_LEN as u64,
                segments: vec![1],
                next_seq: 1,
                next_txn: 1,
                poisoned: false,
            };
            return Ok((log, State::default(), report));
        }

        // Replay starts at the newest checkpoint; older segments are superseded by it.
        let mut start = 0;
        for i in (0..bases.len()).rev() {
            let path = dir.join(segment_name(bases[i]));
            let mut head = Vec::with_capacity(SEG_HEADER_LEN);
            File::open(&path)?.take(SEG_HEADER_LEN as u64).read_to_end(&mut head)?;
            let flags = parse_segment_header(&head).ok_or_else(|| corrupt(&path, 0, "bad segment header"))?;
            if flags & FLAG_CHECKPOINT != 0 {
                start = i;
                report.checkpoint_used = true;
                break;
            }
        }

        let mut state = State::default();
        let mut expected_seq = bases[start];
        let mut next_txn = 1;
        let mut pending: Vec<Record> = Vec::new();
        let mut active_len = 0u64;
        let last = bases.len() - 1;

        for (i, &base) in bases.iter().enumerate().skip(start) {
            let path = dir.join(segment_name(base));
            let buf = fs::read(&path)?;
            parse_segment_header(&buf).ok_or_else(|| corrupt(&path, 0, "bad segment header"))?;
            if base != expected_seq {
                return Err(corrupt(&path, 0, format!("segment starts at seq {base}, expected {expected_seq}")));
            }
            report.segments_replayed += 1;

            let mut off = SEG_HEADER_LEN;
            let mut commit_end = SEG_HEADER_LEN;
            let mut seq_after_commit = expected_seq;
            let damage: Option<(usize, &'static str)> = loop {
                match frame::decode(&buf, off) {
                    Decoded::Frame { header, payload, end } => {
                        if header.seq != expected_seq {
                            break Some((off, "sequence number out of order"));
                        }
                        let record = match Record::decode(header.kind, payload) {
                            Ok(r) => r,
                            Err(why) => break Some((off, why)),
                        };
                        if let Record::Commit { txn, records } = record {
                            if records as usize != pending.len() {
                                break Some((off, "commit record count mismatch"));
                            }
                            for r in pending.drain(..) {
                                state.apply(&r);
                            }
                            report.transactions_replayed += 1;
                            report.records_replayed += records as u64;
                            next_txn = txn + 1;
                            commit_end = end;
                            seq_after_commit = expected_seq + 1;
                        } else {
                            pending.push(record);
                        }
                        expected_seq += 1;
                        off = end;
                    }
                    Decoded::Incomplete if off == buf.len() => break None,
                    Decoded::Incomplete => break Some((off, "incomplete frame")),
                    Decoded::Invalid(why) => break Some((off, why)),
                }
            };

            if damage.is_none() && pending.is_empty() {
                active_len = buf.len() as u64;
                continue;
            }
            if i != last {
                let (at, why) = damage.unwrap_or((off, "segment ends inside a transaction"));
                return Err(corrupt(&path, at, why));
            }
            if report.checkpoint_used && i == start && commit_end == SEG_HEADER_LEN {
                // The snapshot itself is not intact; truncating would silently empty the queue.
                let (at, why) = damage.unwrap_or((off, "checkpoint ends inside its snapshot"));
                return Err(corrupt(&path, at, format!("checkpoint snapshot damaged: {why}")));
            }
            if let Some((at, why)) = damage {
                if let Some(seq) = later_commit(&buf, at, seq_after_commit) {
                    return Err(corrupt(
                        &path,
                        at,
                        format!("{why}; a later committed transaction (commit seq {seq}) follows the damage"),
                    ));
                }
            }

            // Unfinished tail: cut the segment back to the end of the last commit.
            report.discarded_records += pending.len() as u64;
            report.truncated_bytes += (buf.len() - commit_end) as u64;
            pending.clear();
            expected_seq = seq_after_commit;
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(commit_end as u64)?;
            if opts.fsync {
                f.sync_all()?;
            }
            active_len = commit_end as u64;
        }

        for &old in &bases[..start] {
            fs::remove_file(dir.join(segment_name(old)))?;
            report.removed_old_segments += 1;
        }
        if start > 0 {
            sync_dir(dir, &opts)?;
        }

        let active_base = bases[last];
        let file = OpenOptions::new().append(true).open(dir.join(segment_name(active_base)))?;
        let log = Log {
            dir: dir.to_path_buf(),
            opts,
            file,
            active_base,
            active_len,
            segments: bases[start..].to_vec(),
            next_seq: expected_seq,
            next_txn,
            poisoned: false,
        };
        Ok((log, state, report))
    }

    pub(crate) fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Rotates or checkpoints before the next write if the active segment is full.
    ///
    /// Doing this before a transaction (not after) means a failure here never leaves a committed
    /// transaction reported as failed.
    pub(crate) fn prepare(&mut self, state: &State) -> Result<(), DtnError> {
        if self.poisoned {
            return Err(DtnError::Poisoned);
        }
        if self.active_len < self.opts.max_segment_bytes {
            return Ok(());
        }
        if self.segments.len() >= self.opts.compact_after_segments {
            self.checkpoint(state)
        } else {
            self.rotate()
        }
    }

    /// Appends one transaction (data frames + commit frame) and syncs it.
    pub(crate) fn commit(&mut self, records: &[Record]) -> Result<(), DtnError> {
        if self.poisoned {
            return Err(DtnError::Poisoned);
        }
        // Encode everything first so an oversized record fails before anything is written.
        let mut frames = Vec::with_capacity(records.len() + 1);
        let mut seq = self.next_seq;
        for r in records {
            let mut buf = Vec::new();
            encode_frame(r, seq, &mut buf)?;
            frames.push(buf);
            seq += 1;
        }
        let mut buf = Vec::new();
        let commit = Record::Commit {
            txn: self.next_txn,
            records: records.len() as u32,
        };
        encode_frame(&commit, seq, &mut buf)?;
        frames.push(buf);

        let start_len = self.active_len;
        if let Err(e) = self.write_frames(&frames) {
            self.rollback(start_len);
            return Err(e.into());
        }
        self.next_seq = seq + 1;
        self.next_txn += 1;
        Ok(())
    }

    /// One `write` per frame, so a process killed mid-transaction leaves whole frames without a
    /// commit, which is exactly the case recovery discards.
    fn write_frames(&mut self, frames: &[Vec<u8>]) -> io::Result<()> {
        for f in frames {
            self.file.write_all(f)?;
            self.active_len += f.len() as u64;
        }
        if self.opts.fsync {
            self.file.sync_data()?;
        }
        Ok(())
    }

    fn rollback(&mut self, len: u64) {
        let ok = self.file.set_len(len).is_ok() && (!self.opts.fsync || self.file.sync_data().is_ok());
        self.active_len = len;
        if !ok {
            self.poisoned = true;
        }
    }

    fn rotate(&mut self) -> Result<(), DtnError> {
        if self.active_len <= SEG_HEADER_LEN as u64 {
            return Ok(()); // nothing in the active segment yet
        }
        let base = self.next_seq;
        self.file = create_segment(&self.dir, base, 0, &[], &self.opts)?;
        self.active_base = base;
        self.active_len = SEG_HEADER_LEN as u64;
        self.segments.push(base);
        Ok(())
    }

    /// Writes a checkpoint segment holding the whole queue and deletes the older segments.
    pub(crate) fn checkpoint(&mut self, state: &State) -> Result<(), DtnError> {
        if self.poisoned {
            return Err(DtnError::Poisoned);
        }
        let base = self.next_seq;
        let entries = state.sorted();
        let mut body = Vec::new();
        let mut seq = base;
        for e in entries {
            encode_frame(&Record::Snapshot(e), seq, &mut body)?;
            seq += 1;
        }
        let commit = Record::Commit {
            txn: self.next_txn,
            records: (seq - base) as u32,
        };
        encode_frame(&commit, seq, &mut body)?;

        // If the active segment is still empty it has this same base; the rename replaces it.
        self.file = create_segment(&self.dir, base, FLAG_CHECKPOINT, &body, &self.opts)?;
        self.active_base = base;
        self.active_len = (SEG_HEADER_LEN + body.len()) as u64;
        self.next_seq = seq + 1;
        self.next_txn += 1;

        let old: Vec<u64> = self.segments.drain(..).filter(|&b| b != base).collect();
        self.segments.push(base);
        // Best effort: anything left behind is older than the checkpoint and is removed on open.
        for b in old {
            let _ = fs::remove_file(self.dir.join(segment_name(b)));
        }
        let _ = sync_dir(&self.dir, &self.opts);
        Ok(())
    }
}
