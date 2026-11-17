//! Real process-kill campaign, driven by `crash_sim::process::KillCampaign`.
//!
//! Each trial re-runs this test binary as a writer process. The writer opens the queue, which
//! recovers whatever the previous killed writer left behind, and then performs a seeded list of
//! operations, printing `ACK <n>` after operation `n` returns. Small segments make rotation and
//! checkpoints happen every few operations, so kills land in the middle of them too.
//!
//! After the `SIGKILL`, the parent copies the queue directory, opens the copy (running recovery)
//! and compares it with an independent reference model:
//!
//! * every acknowledged operation must be present, and
//! * the operation in flight must be either fully applied or not applied at all.
//!
//! The same directory carries on to the next trial, so each writer also recovers from the
//! previous crash. Default: 300 trials. Full run:
//!
//! ```text
//! DTN_WAL_KILL_TRIALS=10000 cargo test --release --test process_kill -- --nocapture
//! ```

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use crash_sim::process::KillCampaign;
use crash_sim::SimpleRng;
use dtn_wal::{Bundle, BundleQueue, Entry, Options, Priority};
use tempfile::TempDir;

const DIR_ENV: &str = "DTN_WAL_KILL_DIR";
const SEED_ENV: &str = "DTN_WAL_KILL_SEED";
const OPS_PER_TRIAL: usize = 400;

fn options() -> Options {
    // fsync stays on (the default) so the writer follows the real durability path.
    Options::new().max_segment_bytes(1024).compact_after_segments(4)
}

#[derive(Debug, Clone)]
enum Op {
    Enqueue(Bundle),
    Batch(Vec<Bundle>),
    Deliver(String),
    Attempt(String, u64),
    Purge(u64),
    Checkpoint,
}

fn random_bundle(rng: &mut SimpleRng) -> Bundle {
    let id = format!("b{}", rng.gen_range(0, 48));
    let len = rng.gen_range(0, 200);
    let payload = (0..len).map(|_| rng.next_u8()).collect();
    Bundle::new(
        id,
        format!("node{}", rng.gen_range(0, 5)),
        "ground",
        Priority::from_u8(rng.gen_range(0, 4) as u8),
        payload,
        rng.gen_range(0, 1_000) as u64,
        rng.gen_range(0, 1_000) as u64,
    )
}

fn ops_for(seed: u64) -> Vec<Op> {
    let mut rng = SimpleRng::new(seed);
    (0..OPS_PER_TRIAL)
        .map(|_| match rng.gen_range(0, 1_000) {
            0..=449 => Op::Enqueue(random_bundle(&mut rng)),
            450..=599 => {
                let n = rng.gen_range(2, 7);
                Op::Batch((0..n).map(|_| random_bundle(&mut rng)).collect())
            }
            600..=799 => Op::Deliver(format!("b{}", rng.gen_range(0, 48))),
            800..=969 => Op::Attempt(format!("b{}", rng.gen_range(0, 48)), rng.next_u64() % 10_000),
            970..=984 => Op::Purge(rng.gen_range(800, 2_200) as u64),
            _ => Op::Checkpoint,
        })
        .collect()
}

fn run_op(q: &BundleQueue, op: &Op) {
    match op {
        Op::Enqueue(b) => q.enqueue(b).unwrap(),
        Op::Batch(bs) => q.enqueue_batch(bs).unwrap(),
        Op::Deliver(id) => {
            q.mark_delivered(id).unwrap();
        }
        Op::Attempt(id, ts) => q.record_attempt(id, *ts).unwrap(),
        Op::Purge(now) => {
            q.purge_expired(*now).unwrap();
        }
        Op::Checkpoint => q.checkpoint().unwrap(),
    }
}

/// Reference model, written independently of the engine's own state code.
#[derive(Clone, Default, PartialEq)]
struct Model(BTreeMap<String, Entry>);

impl Model {
    fn apply(&mut self, op: &Op) {
        match op {
            Op::Enqueue(b) => self.upsert(b),
            Op::Batch(bs) => bs.iter().for_each(|b| self.upsert(b)),
            Op::Deliver(id) => {
                if let Some(e) = self.0.get_mut(id) {
                    e.delivered = true;
                }
            }
            Op::Attempt(id, ts) => {
                if let Some(e) = self.0.get_mut(id) {
                    e.attempts += 1;
                    e.last_attempt = *ts;
                }
            }
            Op::Purge(now) => self.0.retain(|_, e| *now <= e.bundle.created_at + e.bundle.ttl_seconds),
            Op::Checkpoint => {}
        }
    }

    fn upsert(&mut self, b: &Bundle) {
        let e = self.0.entry(b.id.clone()).or_insert_with(|| Entry {
            bundle: b.clone(),
            delivered: false,
            attempts: 0,
            last_attempt: 0,
        });
        e.bundle.priority = b.priority;
        e.bundle.payload = b.payload.clone();
        e.bundle.ttl_seconds = b.ttl_seconds;
    }

    fn entries(&self) -> Vec<Entry> {
        self.0.values().cloned().collect()
    }
}

/// The writer side. Only does anything when started by the campaign below.
#[test]
#[ignore = "child process entry point for the kill campaign"]
fn kill_target() {
    let (Ok(dir), Ok(seed)) = (std::env::var(DIR_ENV), std::env::var(SEED_ENV)) else {
        return;
    };
    let q = BundleQueue::open_with(&dir, options()).unwrap();
    let mut out = std::io::stdout().lock();
    for (i, op) in ops_for(seed.parse().unwrap()).iter().enumerate() {
        run_op(&q, op);
        writeln!(out, "ACK {i}").unwrap();
        out.flush().unwrap();
    }
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

#[derive(Default, Debug)]
struct Tally {
    acked_ops: u64,
    in_flight_committed: u64,
    in_flight_dropped: u64,
    recoveries_discarding_uncommitted: u64,
    discarded_records: u64,
    truncated_bytes: u64,
    temp_files_removed: u64,
    old_segments_removed: u64,
    checkpoint_recoveries: u64,
}

fn campaign(trials: u64, seed: u64) {
    let work = TempDir::new().unwrap();
    let dir = work.path().join("queue");
    let exe = std::env::current_exe().unwrap();
    let mut model = Model::default();
    let mut tally = Tally::default();

    let report = KillCampaign::new(trials)
        .seed(seed)
        .kill_window(Duration::from_millis(2), Duration::from_millis(80))
        .run(
            |trial| {
                let mut cmd = Command::new(&exe);
                cmd.args(["kill_target", "--exact", "--ignored", "--nocapture", "--test-threads=1", "-q"])
                    .env(DIR_ENV, &dir)
                    .env(SEED_ENV, trial.seed.to_string());
                cmd
            },
            |trial, outcome| {
                let ops = ops_for(trial.seed);
                let acks: Vec<&String> = outcome.acks.iter().filter(|l| l.starts_with("ACK ")).collect();
                for (i, line) in acks.iter().enumerate() {
                    if **line != format!("ACK {i}") {
                        return Err(format!("acknowledgements out of order: {line:?} at {i}"));
                    }
                }
                let k = acks.len();

                // Recover a copy, so the next writer still has to recover the original itself.
                let check = work.path().join(format!("check-{}", trial.index));
                if dir.exists() {
                    copy_dir(&dir, &check);
                }
                let recovered = BundleQueue::open_with(&check, options())
                    .map_err(|e| format!("recovery failed after {k} acks: {e}"))?;
                let got = recovered.entries();
                let r = recovered.recovery_report();
                drop(recovered);
                fs::remove_dir_all(&check).unwrap();

                let mut acked = model.clone();
                ops[..k].iter().for_each(|op| acked.apply(op));
                let matched = if got == acked.entries() {
                    tally.in_flight_dropped += (k < ops.len()) as u64;
                    acked
                } else if k < ops.len() && {
                    let mut with_next = acked.clone();
                    with_next.apply(&ops[k]);
                    got == with_next.entries()
                } {
                    tally.in_flight_committed += 1;
                    let mut with_next = acked;
                    with_next.apply(&ops[k]);
                    with_next
                } else {
                    return Err(format!(
                        "recovered state matches neither {k} nor {} acknowledged operations (next op {:?})",
                        k + 1,
                        ops.get(k)
                    ));
                };
                model = matched;

                tally.acked_ops += k as u64;
                tally.recoveries_discarding_uncommitted += (r.discarded_records > 0) as u64;
                tally.discarded_records += r.discarded_records;
                tally.truncated_bytes += r.truncated_bytes;
                tally.temp_files_removed += r.removed_temp_files as u64;
                tally.old_segments_removed += r.removed_old_segments as u64;
                tally.checkpoint_recoveries += r.checkpoint_used as u64;
                Ok(())
            },
        )
        .unwrap_or_else(|f| panic!("{f}"));

    println!("\n=== dtn-wal process-kill campaign (crash-sim) ===");
    println!("{report}");
    println!("state checked after every kill: {} trials, 0 mismatches", report.trials);
    println!("acknowledged operations verified present: {}", tally.acked_ops);
    println!(
        "operation in flight at the kill: committed {} times, absent {} times, never partial",
        tally.in_flight_committed, tally.in_flight_dropped
    );
    println!(
        "recoveries that cut off an uncommitted transaction: {} ({} records, {} bytes)",
        tally.recoveries_discarding_uncommitted, tally.discarded_records, tally.truncated_bytes
    );
    println!(
        "kills during rotation/checkpoint: {} temp segments removed, {} pre-checkpoint segments removed",
        tally.temp_files_removed, tally.old_segments_removed
    );
    println!("recoveries that started from a checkpoint: {}", tally.checkpoint_recoveries);
    assert!(report.killed > trials / 2, "most children should be killed mid-run");
}

#[test]
fn process_kill_campaign() {
    let trials = std::env::var("DTN_WAL_KILL_TRIALS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    campaign(trials, 0xD7_4A11);
}
