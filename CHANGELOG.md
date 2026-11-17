# Changelog

## 0.2.0 (2026-11)

The SQLite backend is replaced by dtn-wal's own append-only log engine.

- Frames with a fixed-size 24-byte header (magic, version, kind, sequence number, length, CRC32C).
- Hardware-accelerated CRC32C over header and payload (SSE4.2 / ARMv8 CRC via `crc32c`).
- Transactions end in a commit frame; `enqueue_batch` commits many bundles atomically.
- Segment files with crash-safe rotation (temp file, `fsync`, rename, directory `fsync`).
- Checkpoints that write the whole queue to a new segment and delete older segments.
- Recovery truncates unfinished transactions and returns `DtnError::Corrupt` when committed data
  is damaged; `recovery_report()` describes what was done.
- New: `open_with`, `Options`, `enqueue_batch`, `get`, `entries`, `checkpoint`,
  `recovery_report`, `segment_count`, `Entry`, `RecoveryReport`.
- Tests: torn writes at every byte offset, bit flips at every byte, crashes during rotation and
  checkpoints, and a real process-kill campaign using `crash-sim` 0.2's `KillCampaign`.
- Breaking: `open` takes a directory instead of a SQLite file; `rusqlite` and `serde_json` are
  no longer dependencies; `DtnError::Database` and `DtnError::Serialization` are removed.

## 0.1.0 (2026-10-30)

- First release: a priority store-and-forward bundle queue on SQLite in WAL mode.
