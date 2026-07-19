//! Live progress reporting.
//!
//! We keep this decoupled from the crawler by exposing a single `Progress`
//! struct held behind a `OnceLock`. Any code path that previously printed to
//! stderr routes through [`println`] so the spinner isn't shredded by
//! concurrent log lines. When progress is disabled (piped output, `--no-progress`,
//! tests) the fallback is a plain `eprintln!` — behaviour identical to before.

use indicatif::{ProgressBar, ProgressStyle};
use std::io::IsTerminal;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// Global handle. Set once from `main` before the crawler starts; readers get
/// `None` in tests / library consumers that never installed a progress bar.
static ACTIVE: OnceLock<Arc<Progress>> = OnceLock::new();

pub struct Progress {
    bar: ProgressBar,
    enabled: bool,
    dirs_pending: AtomicUsize,
    dirs_done: AtomicUsize,
    files_ok: AtomicUsize,
    files_skipped: AtomicUsize,
    files_failed: AtomicUsize,
    in_flight: AtomicUsize,
    bytes: AtomicU64,
}

impl Progress {
    /// Build a progress bar. `force` bypasses the TTY check (mainly for tests
    /// that want to exercise the render path); pass `None` for the normal
    /// "on-if-tty" heuristic.
    pub fn new(force_enabled: Option<bool>) -> Arc<Self> {
        let enabled = force_enabled.unwrap_or_else(|| std::io::stderr().is_terminal());

        let bar = if enabled {
            let pb = ProgressBar::new_spinner();
            // `elapsed_precise` gives HH:MM:SS which is the anti-hang signal we
            // want front-and-centre. The trailing {msg} is where our counters
            // land; the spinner keeps ticking even when counters are frozen so
            // the user can tell "hung" apart from "slow".
            let style = ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg}")
                .expect("static progress template compiles");
            pb.set_style(style);
            pb.enable_steady_tick(Duration::from_millis(120));
            pb
        } else {
            ProgressBar::hidden()
        };

        let p = Arc::new(Self {
            bar,
            enabled,
            dirs_pending: AtomicUsize::new(0),
            dirs_done: AtomicUsize::new(0),
            files_ok: AtomicUsize::new(0),
            files_skipped: AtomicUsize::new(0),
            files_failed: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
        });
        p.refresh();
        p
    }

    /// Install as the process-wide active progress. First call wins.
    pub fn install(self: &Arc<Self>) {
        let _ = ACTIVE.set(self.clone());
    }

    pub fn dir_enqueued(&self, n: usize) {
        self.dirs_pending.fetch_add(n, Ordering::Relaxed);
        self.refresh();
    }

    pub fn dir_completed(&self) {
        self.dirs_done.fetch_add(1, Ordering::Relaxed);
        self.refresh();
    }

    pub fn task_started(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        self.refresh();
    }

    pub fn file_downloaded(&self, bytes: u64) {
        self.files_ok.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.refresh();
    }

    /// Complete a file whose bytes were already accounted for via `bytes_delta`
    /// during the streamed download. Increments the file count without
    /// double-counting bytes.
    pub fn file_completed_streamed(&self) {
        self.files_ok.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.refresh();
    }

    /// Bump the bytes counter mid-download so the spinner reflects live
    /// throughput of an in-flight streaming body. Does NOT touch file counts;
    /// call `file_downloaded(0)` on completion so the file count still advances.
    pub fn bytes_delta(&self, delta: u64) {
        self.bytes.fetch_add(delta, Ordering::Relaxed);
        self.refresh();
    }

    pub fn file_skipped(&self) {
        self.files_skipped.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.refresh();
    }

    pub fn file_failed(&self) {
        self.files_failed.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.refresh();
    }

    /// Balance the `task_started` bump for a listing fetch without touching
    /// the file counters (those are for actual files).
    pub fn listing_finished(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.refresh();
    }

    pub fn println(&self, message: &str) {
        if self.enabled {
            // `bar.println` clears the current line, prints, and redraws the
            // spinner — safe to interleave with steady-tick.
            self.bar.println(message);
        } else {
            eprintln!("{message}");
        }
    }

    /// Called at the end of a run. Clears the bar so it doesn't linger under
    /// the final summary line.
    pub fn finish(&self) {
        self.refresh();
        if self.enabled {
            self.bar.finish_and_clear();
        }
    }

    fn refresh(&self) {
        if !self.enabled {
            return;
        }
        let dp = self.dirs_pending.load(Ordering::Relaxed);
        let dd = self.dirs_done.load(Ordering::Relaxed);
        let fo = self.files_ok.load(Ordering::Relaxed);
        let fs = self.files_skipped.load(Ordering::Relaxed);
        let ff = self.files_failed.load(Ordering::Relaxed);
        let inflight = self.in_flight.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        // Kept intentionally single-line — indicatif redraws a line, not a
        // multi-line block. Wide terminals show it all; narrow terminals get
        // it truncated but the leading counters (which change most) survive.
        let msg = format!(
            "dirs {dd}/{dp} • files {fo} (skip {fs}, fail {ff}) • {} • in-flight {inflight}",
            human_bytes(bytes),
        );
        self.bar.set_message(msg);
    }
}

/// Get the active progress bar, or a headless fallback if none was installed
/// (tests, library consumers, `--no-progress`). The fallback is a shared
/// singleton so we don't allocate per call.
pub fn active() -> Arc<Progress> {
    if let Some(p) = ACTIVE.get() {
        return p.clone();
    }
    static HEADLESS: OnceLock<Arc<Progress>> = OnceLock::new();
    HEADLESS
        .get_or_init(|| Progress::new(Some(false)))
        .clone()
}

/// Emit a log line without clobbering the spinner. Falls back to `eprintln!`
/// when no progress bar has been installed (tests, `--no-progress`, non-tty).
pub fn println(message: &str) {
    if let Some(p) = ACTIVE.get() {
        p.println(message);
    } else {
        eprintln!("{message}");
    }
}

fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut value = n as f64;
    let mut idx = 0;
    while value >= 1024.0 && idx < UNITS.len() - 1 {
        value /= 1024.0;
        idx += 1;
    }
    format!("{value:.1} {}", UNITS[idx])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_bytes_boundaries() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(1024), "1.0 KB");
        assert_eq!(human_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(human_bytes(1024_u64.pow(3)), "1.0 GB");
    }

    #[test]
    fn disabled_progress_counters_advance() {
        // Contract: every `task_started` is paired with exactly one completion
        // method. Under that contract, counters are consistent.
        let p = Progress::new(Some(false));
        p.dir_enqueued(3);

        p.task_started();
        p.bytes_delta(1500);
        p.file_completed_streamed();

        p.task_started();
        p.file_skipped();

        p.task_started();
        p.file_failed();

        p.dir_completed();

        assert_eq!(p.dirs_pending.load(Ordering::Relaxed), 3);
        assert_eq!(p.dirs_done.load(Ordering::Relaxed), 1);
        assert_eq!(p.files_ok.load(Ordering::Relaxed), 1);
        assert_eq!(p.files_skipped.load(Ordering::Relaxed), 1);
        assert_eq!(p.files_failed.load(Ordering::Relaxed), 1);
        assert_eq!(p.bytes.load(Ordering::Relaxed), 1500);
        assert_eq!(p.in_flight.load(Ordering::Relaxed), 0);
    }
}
