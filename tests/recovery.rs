//! On-disk crash-recovery tests: torn writes at every byte offset, bit flips at every byte,
//! rotation, checkpoints, and crashes in the middle of rotation and checkpointing.

use std::fs;
use std::path::{Path, PathBuf};

use dtn_wal::{Bundle, BundleQueue, DtnError, Entry, Options, Priority};
use tempfile::TempDir;

const FIRST_SEGMENT: &str = "00000000000000000001.seg";

fn bundle(i: u64) -> Bundle {
    let priority = Priority::from_u8((i % 4) as u8);
    let payload: Vec<u8> = (0..(i * 7 % 40)).map(|k| (k * 31 + i) as u8).collect();
    Bundle::new(format!("b{i}"), "relay-7", "ground", priority, payload, 1_000 + i, 500)
}

fn fast() -> Options {
    Options::new().fsync(false)
}

fn segment_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "seg"))
        .collect();
    v.sort();
    v
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

/// Runs a fixed mix of operations, recording (segment length, queue contents) after each commit.
fn build_single_segment_log(dir: &Path) -> Vec<(u64, Vec<Entry>)> {
    let q = BundleQueue::open_with(dir, fast()).unwrap();
    let seg = dir.join(FIRST_SEGMENT);
    let mut points = vec![(fs::metadata(&seg).unwrap().len(), q.entries())];
    let mut snap = |q: &BundleQueue| points.push((fs::metadata(&seg).unwrap().len(), q.entries()));

    q.enqueue(&bundle(1)).unwrap();
    snap(&q);
    q.enqueue_batch(&[bundle(2), bundle(3), bundle(4)]).unwrap();
    snap(&q);
    q.mark_delivered("b2").unwrap();
    snap(&q);
    q.record_attempt("b3", 77).unwrap();
    snap(&q);
    q.enqueue(&Bundle::new("b1", "x", "y", Priority::LifeCritical, b"updated".to_vec(), 0, 9_000)).unwrap();
    snap(&q);
    q.purge_expired(1_502).unwrap(); // removes b2 (1002+500) only
    snap(&q);
    q.enqueue_batch(&[bundle(5), bundle(6)]).unwrap();
    snap(&q);
    drop(q);
    points
}

#[test]
fn data_survives_reopen() {
    let tmp = TempDir::new().unwrap();
    let q = BundleQueue::open(tmp.path()).unwrap();
    q.enqueue_batch(&[bundle(1), bundle(2)]).unwrap();
    q.mark_delivered("b1").unwrap();
    q.record_attempt("b2", 5).unwrap();
    let before = q.entries();
    drop(q);

    let q = BundleQueue::open(tmp.path()).unwrap();
    assert_eq!(q.entries(), before);
    let r = q.recovery_report();
    assert_eq!((r.transactions_replayed, r.records_replayed, r.truncated_bytes), (3, 4, 0));
    assert_eq!(q.dequeue_pending(10).unwrap()[0].id, "b2");
}

#[test]
fn torn_write_at_every_byte_offset_recovers_the_last_commit() {
    let src = TempDir::new().unwrap();
    let points = build_single_segment_log(src.path());
    let full = fs::read(src.path().join(FIRST_SEGMENT)).unwrap();
    assert_eq!(full.len() as u64, points.last().unwrap().0);

    for cut in 0..=full.len() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join(FIRST_SEGMENT), &full[..cut]).unwrap();
        let result = BundleQueue::open_with(tmp.path(), fast());

        if cut < 16 {
            assert!(matches!(result, Err(DtnError::Corrupt { .. })), "cut {cut}: header must not be guessed");
            continue;
        }
        let q = result.unwrap_or_else(|e| panic!("cut {cut}: {e}"));
        let (len, expected) = points.iter().rev().find(|(len, _)| *len <= cut as u64).unwrap();
        assert_eq!(&q.entries(), expected, "cut at byte {cut}");
        assert_eq!(q.recovery_report().truncated_bytes, cut as u64 - len, "cut at byte {cut}");

        // New writes after recovery must land on a clean log and survive another reopen.
        q.enqueue(&bundle(99)).unwrap();
        let after = q.entries();
        drop(q);
        let q = BundleQueue::open_with(tmp.path(), fast()).unwrap();
        assert_eq!(q.entries(), after, "reopen after cut {cut}");
        assert_eq!(q.recovery_report().truncated_bytes, 0);
    }
}

#[test]
fn garbage_after_the_last_commit_is_truncated() {
    let tmp = TempDir::new().unwrap();
    let points = build_single_segment_log(tmp.path());
    let seg = tmp.path().join(FIRST_SEGMENT);
    for garbage in [vec![0u8; 4096], b"DTNW\x01\x01\0\0garbage".to_vec(), (0..=255).collect()] {
        let mut bytes = fs::read(&seg).unwrap();
        let committed = bytes.len();
        bytes.extend_from_slice(&garbage);
        fs::write(&seg, &bytes).unwrap();

        let q = BundleQueue::open_with(tmp.path(), fast()).unwrap();
        assert_eq!(q.entries(), points.last().unwrap().1);
        assert_eq!(q.recovery_report().truncated_bytes, garbage.len() as u64);
        drop(q);
        assert_eq!(fs::metadata(&seg).unwrap().len() as usize, committed);
    }
}

#[test]
fn a_bit_flip_in_committed_data_is_reported_never_silently_dropped() {
    let src = TempDir::new().unwrap();
    let points = build_single_segment_log(src.path());
    let full = fs::read(src.path().join(FIRST_SEGMENT)).unwrap();
    let last_txn_start = points[points.len() - 2].0 as usize;

    for pos in 0..full.len() {
        for bit in [0u8, 3, 7] {
            let mut bytes = full.clone();
            bytes[pos] ^= 1 << bit;
            let tmp = TempDir::new().unwrap();
            fs::write(tmp.path().join(FIRST_SEGMENT), &bytes).unwrap();
            let result = BundleQueue::open_with(tmp.path(), fast());

            if pos < last_txn_start {
                // An earlier transaction is damaged and later commits exist: refuse to open.
                assert!(
                    matches!(result, Err(DtnError::Corrupt { .. })),
                    "flip at byte {pos} bit {bit} was not reported"
                );
            } else {
                // Damage inside the newest transaction looks exactly like a torn write: it is
                // cut off, the queue opens at the previous commit, and the report says so.
                let q = result.unwrap_or_else(|e| panic!("flip at byte {pos}: {e}"));
                assert_eq!(q.entries(), points[points.len() - 2].1, "flip at byte {pos}");
                assert!(q.recovery_report().truncated_bytes > 0);
            }
        }
    }
}

#[test]
fn rotation_splits_the_log_and_replays_across_segments() {
    let tmp = TempDir::new().unwrap();
    let opts = fast().max_segment_bytes(256).compact_after_segments(1_000);
    let q = BundleQueue::open_with(tmp.path(), opts).unwrap();
    for i in 0..120 {
        q.enqueue(&bundle(i)).unwrap();
        if i % 3 == 0 {
            q.mark_delivered(&format!("b{}", i / 2)).unwrap();
        }
    }
    let before = q.entries();
    let segments = q.segment_count();
    drop(q);

    assert!(segments > 10, "expected many segments, got {segments}");
    assert_eq!(segment_files(tmp.path()).len(), segments);
    let q = BundleQueue::open_with(tmp.path(), opts).unwrap();
    assert_eq!(q.entries(), before);
    assert_eq!(q.recovery_report().segments_replayed, segments);
}

#[test]
fn checkpoints_bound_the_number_of_segments() {
    let tmp = TempDir::new().unwrap();
    let opts = fast().max_segment_bytes(256).compact_after_segments(3);
    let q = BundleQueue::open_with(tmp.path(), opts).unwrap();
    for i in 0..300 {
        q.enqueue(&bundle(i % 40)).unwrap();
        q.record_attempt(&format!("b{}", i % 17), i).unwrap();
        assert!(q.segment_count() <= 3);
    }
    let before = q.entries();
    drop(q);

    let q = BundleQueue::open_with(tmp.path(), opts).unwrap();
    assert_eq!(q.entries(), before);
    assert!(q.recovery_report().checkpoint_used);

    q.checkpoint().unwrap();
    assert_eq!(q.segment_count(), 1);
    assert_eq!(segment_files(tmp.path()).len(), 1);
    drop(q);
    assert_eq!(BundleQueue::open_with(tmp.path(), opts).unwrap().entries(), before);
}

#[test]
fn crash_during_rotation_leaves_a_temp_file_that_is_removed() {
    let tmp = TempDir::new().unwrap();
    let q = BundleQueue::open_with(tmp.path(), fast()).unwrap();
    q.enqueue(&bundle(1)).unwrap();
    let before = q.entries();
    drop(q);
    // A rotation that died before its rename leaves a partial temp segment behind.
    fs::write(tmp.path().join("00000000000000000003.seg.tmp"), b"DTNWSEG1\0\0").unwrap();

    let q = BundleQueue::open_with(tmp.path(), fast()).unwrap();
    assert_eq!(q.entries(), before);
    assert_eq!(q.recovery_report().removed_temp_files, 1);
    assert!(!tmp.path().join("00000000000000000003.seg.tmp").exists());
}

#[test]
fn crash_after_checkpoint_rename_finishes_the_cleanup() {
    let tmp = TempDir::new().unwrap();
    let saved = TempDir::new().unwrap();
    let opts = fast().max_segment_bytes(256).compact_after_segments(1_000);
    let q = BundleQueue::open_with(tmp.path(), opts).unwrap();
    for i in 0..40 {
        q.enqueue(&bundle(i)).unwrap();
    }
    copy_dir(tmp.path(), saved.path());
    let old = segment_files(tmp.path()).len();
    q.checkpoint().unwrap();
    q.mark_delivered("b5").unwrap();
    let before = q.entries();
    drop(q);
    let live = segment_files(tmp.path()).len(); // the checkpoint, plus a rotation after it

    // Put the pre-checkpoint segments back, as if the process died before deleting them.
    copy_dir(saved.path(), tmp.path());
    assert_eq!(segment_files(tmp.path()).len(), old + live);

    let q = BundleQueue::open_with(tmp.path(), opts).unwrap();
    assert_eq!(q.entries(), before);
    let r = q.recovery_report();
    assert!(r.checkpoint_used);
    assert_eq!(r.removed_old_segments, old);
    assert_eq!(segment_files(tmp.path()).len(), live);
}

#[test]
fn a_damaged_checkpoint_is_reported() {
    let tmp = TempDir::new().unwrap();
    let q = BundleQueue::open_with(tmp.path(), fast()).unwrap();
    q.enqueue_batch(&[bundle(1), bundle(2)]).unwrap();
    q.checkpoint().unwrap();
    drop(q);
    let seg = segment_files(tmp.path()).pop().unwrap();
    let mut bytes = fs::read(&seg).unwrap();
    bytes[40] ^= 0x10; // inside the first snapshot frame
    fs::write(&seg, bytes).unwrap();
    assert!(matches!(BundleQueue::open_with(tmp.path(), fast()), Err(DtnError::Corrupt { .. })));
}

#[test]
fn damage_or_a_gap_in_an_older_segment_is_reported() {
    let opts = fast().max_segment_bytes(256).compact_after_segments(1_000);
    let make = || {
        let tmp = TempDir::new().unwrap();
        let q = BundleQueue::open_with(tmp.path(), opts).unwrap();
        for i in 0..30 {
            q.enqueue(&bundle(i)).unwrap();
        }
        drop(q);
        tmp
    };

    let tmp = make();
    let first = segment_files(tmp.path())[0].clone();
    let mut bytes = fs::read(&first).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    fs::write(&first, bytes).unwrap();
    assert!(matches!(BundleQueue::open_with(tmp.path(), opts), Err(DtnError::Corrupt { .. })));

    let tmp = make();
    fs::remove_file(&segment_files(tmp.path())[2]).unwrap();
    assert!(matches!(BundleQueue::open_with(tmp.path(), opts), Err(DtnError::Corrupt { .. })));
}

#[test]
fn an_old_sqlite_file_path_gives_a_clear_error() {
    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("telemetry_queue.db");
    fs::write(&file, b"SQLite format 3\0").unwrap();
    assert!(matches!(BundleQueue::open(&file), Err(DtnError::NotADirectory(_))));
}
