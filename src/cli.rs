use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use std::path::PathBuf;
use url::Url;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Clone recursive web directory listings to local disk"
)]
pub struct Cli {
    /// URL root directory listing (trailing '/' auto-appended if missing)
    pub url: String,

    /// Output directory on local machine. If omitted, derived from the last
    /// path segment of the URL (e.g. `.../.hermes/` → `./.hermes`), falling
    /// back to the host when the URL points at the root.
    pub output: Option<PathBuf>,

    /// Overall request timeout in seconds. Wall-clock cap for a single
    /// request (connect + all reads). Set 0 to disable — the per-read
    /// --read-timeout will still catch stalled connections. Default is 0
    /// (disabled) because a healthy multi-hundred-MB download at slow-server
    /// speeds legitimately takes many minutes, and prematurely killing it
    /// causes the "Stream error / error decoding response body" symptom
    /// on files like state.db or bin/tirith.
    #[arg(long, default_value_t = 0)]
    pub timeout_seconds: u64,

    /// Per-read idle timeout in seconds. Resets every time bytes arrive, so
    /// a slow-but-alive body doesn't get killed — only truly stalled
    /// connections do. This is the anti-hang guarantee: if the server stops
    /// sending for `--read-timeout` seconds, the request is retried.
    #[arg(long, default_value_t = 30)]
    pub read_timeout: u64,

    /// Connect-phase timeout in seconds. Fail-fast on TCP-unreachable hosts
    /// without eating into the read budget of healthy connections.
    #[arg(long, default_value_t = 8)]
    pub connect_timeout: u64,

    /// Custom User-Agent header
    #[arg(long, default_value = "dirclone/0.2")]
    pub user_agent: String,

    /// Maximum retries for transient errors (connect refuses, mid-stream body
    /// drops, 429/5xx). Default 4 = up to 5 total attempts per URL. Slow
    /// HTTP/1.0 targets like Python SimpleHTTP tend to drop ~5% of large-file
    /// connections mid-body under load — retries recover the vast majority.
    #[arg(long, default_value_t = 4)]
    pub retries: u32,

    /// Base backoff between retries in milliseconds
    #[arg(long, default_value_t = 300)]
    pub retry_backoff_ms: u64,

    /// Redirect limit
    #[arg(long, default_value_t = 10)]
    pub max_redirects: usize,

    /// Include glob pattern (repeatable)
    #[arg(long)]
    pub include: Vec<String>,

    /// Exclude glob pattern (repeatable)
    #[arg(long)]
    pub exclude: Vec<String>,

    /// Run without writing files
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,

    /// Maximum directory depth to recurse into, relative to the root URL.
    /// Root listing itself is depth 0, its immediate subdirs are depth 1, etc.
    /// Omit or set to 0-with-no-value for unlimited (the default). Examples:
    ///   --depth 0   → download only files at the root listing
    ///   --depth 2   → root + two subdir levels
    #[arg(long)]
    pub depth: Option<u32>,

    /// Concurrent file download workers
    #[arg(long, default_value_t = 100)]
    pub concurrency: usize,

    /// Force overwrite existing files and ignore resume cache
    #[arg(long, default_value_t = false)]
    pub force: bool,

    /// Manifest filename/path for resume state
    #[arg(long, default_value = ".dirclone-manifest.json")]
    pub manifest: PathBuf,

    /// Logging level
    #[arg(long, value_enum, default_value_t = LogLevel::Info)]
    pub log_level: LogLevel,

    /// Disable the live progress bar (auto-off when stderr isn't a TTY).
    #[arg(long, default_value_t = false)]
    pub no_progress: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Quiet,
    Info,
    Debug,
}

pub fn log_info(log_level: LogLevel, message: &str) {
    if log_level >= LogLevel::Info {
        crate::progress::println(message);
    }
}

pub fn log_debug(log_level: LogLevel, message: &str) {
    if log_level >= LogLevel::Debug {
        crate::progress::println(&format!("[debug] {message}"));
    }
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub root_url: Url,
    pub output: PathBuf,
    pub timeout_seconds: u64,
    pub read_timeout: u64,
    pub connect_timeout: u64,
    pub user_agent: String,
    pub retries: u32,
    pub retry_backoff_ms: u64,
    pub max_redirects: usize,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub dry_run: bool,
    pub concurrency: usize,
    /// None = unlimited depth.
    pub depth: Option<u32>,
    pub force: bool,
    pub manifest: PathBuf,
    pub log_level: LogLevel,
    pub no_progress: bool,
}

impl TryFrom<Cli> for AppConfig {
    type Error = anyhow::Error;

    fn try_from(value: Cli) -> Result<Self> {
        let mut url_str = value.url;
        // Be lenient: users often type `http://x/.hermes` without a trailing
        // slash. Directory listings only make sense with one, so we append it.
        // We only touch the path — never the query/fragment (they wouldn't be
        // meaningful for a listing root anyway, but stay safe).
        if !url_str.contains('?') && !url_str.contains('#') && !url_str.ends_with('/') {
            url_str.push('/');
        }
        let root_url = Url::parse(&url_str).context("failed to parse root URL")?;
        if !root_url.path().ends_with('/') {
            bail!("root URL must end with '/': {root_url}");
        }
        if value.concurrency == 0 {
            bail!("--concurrency must be at least 1");
        }

        let output = match value.output {
            Some(path) => path,
            None => derive_output_from_url(&root_url)?,
        };

        Ok(Self {
            root_url,
            output,
            timeout_seconds: value.timeout_seconds,
            read_timeout: value.read_timeout,
            connect_timeout: value.connect_timeout,
            user_agent: value.user_agent,
            retries: value.retries,
            retry_backoff_ms: value.retry_backoff_ms,
            max_redirects: value.max_redirects,
            includes: value.include,
            excludes: value.exclude,
            dry_run: value.dry_run,
            concurrency: value.concurrency,
            depth: value.depth,
            force: value.force,
            manifest: value.manifest,
            log_level: value.log_level,
            no_progress: value.no_progress,
        })
    }
}

/// Pick a sensible local output directory when the user doesn't supply one.
/// Rules:
///   - Use the last non-empty path segment (e.g. `/foo/.hermes/` → `.hermes`).
///   - If the URL points at the site root (`/`), fall back to the host, with
///     the port appended so `example.com:8080` and `example.com:9090` don't
///     collide.
///   - Reject segments that would escape CWD (`..`, `.`, or contain a path
///     separator). This can only happen with a hand-crafted URL, but refusing
///     is cheaper than sanitizing.
fn derive_output_from_url(url: &Url) -> Result<PathBuf> {
    let last_segment = url
        .path_segments()
        .and_then(|segs| segs.rev().find(|s| !s.is_empty()));

    let name = match last_segment {
        Some(seg) => seg.to_string(),
        None => match url.host_str() {
            Some(host) => match url.port() {
                Some(port) => format!("{host}_{port}"),
                None => host.to_string(),
            },
            None => bail!("cannot derive output directory: URL has no path segment or host"),
        },
    };

    if name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        bail!(
            "cannot derive output directory from URL segment {name:?}; \
             pass an explicit output path"
        );
    }

    Ok(PathBuf::from(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> Url {
        // Mirror the CLI's leniency for the happy path.
        let mut s = input.to_string();
        if !s.ends_with('/') {
            s.push('/');
        }
        Url::parse(&s).unwrap()
    }

    #[test]
    fn derives_last_segment_for_dotted_dir() {
        let url = parse("http://1.2.3.4:8080/.hermes");
        assert_eq!(derive_output_from_url(&url).unwrap(), PathBuf::from(".hermes"));
    }

    #[test]
    fn derives_last_segment_when_nested() {
        let url = parse("http://host/a/b/c/");
        assert_eq!(derive_output_from_url(&url).unwrap(), PathBuf::from("c"));
    }

    #[test]
    fn falls_back_to_host_at_root() {
        let url = parse("http://example.com/");
        assert_eq!(
            derive_output_from_url(&url).unwrap(),
            PathBuf::from("example.com")
        );
    }

    #[test]
    fn falls_back_to_host_and_port_at_root() {
        let url = parse("http://example.com:8080/");
        assert_eq!(
            derive_output_from_url(&url).unwrap(),
            PathBuf::from("example.com_8080")
        );
    }

    #[test]
    fn cli_appends_trailing_slash_and_derives_output() {
        let cli = Cli {
            url: "http://1.2.3.4:8080/.hermes".to_string(),
            output: None,
            timeout_seconds: 20,
            read_timeout: 30,
            connect_timeout: 8,
            user_agent: "x".into(),
            retries: 0,
            retry_backoff_ms: 0,
            max_redirects: 5,
            include: vec![],
            exclude: vec![],
            dry_run: false,
            concurrency: 1,
            depth: None,
            force: false,
            manifest: ".dirclone-manifest.json".into(),
            log_level: LogLevel::Quiet,
            no_progress: true,
        };
        let cfg = AppConfig::try_from(cli).unwrap();
        assert_eq!(cfg.root_url.as_str(), "http://1.2.3.4:8080/.hermes/");
        assert_eq!(cfg.output, PathBuf::from(".hermes"));
    }
}
