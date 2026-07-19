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
    /// URL root directory listing (must end with /)
    pub url: String,

    /// Output directory on local machine
    pub output: PathBuf,

    /// Timeout in seconds for HTTP requests
    #[arg(long, default_value_t = 20)]
    pub timeout_seconds: u64,

    /// Custom User-Agent header
    #[arg(long, default_value = "dirclone/0.2")]
    pub user_agent: String,

    /// Maximum retries for transient errors
    #[arg(long, default_value_t = 2)]
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

    /// Concurrent file download workers
    #[arg(long, default_value_t = 4)]
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
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Quiet,
    Info,
    Debug,
}

pub fn log_info(log_level: LogLevel, message: &str) {
    if log_level >= LogLevel::Info {
        eprintln!("{message}");
    }
}

pub fn log_debug(log_level: LogLevel, message: &str) {
    if log_level >= LogLevel::Debug {
        eprintln!("[debug] {message}");
    }
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub root_url: Url,
    pub output: PathBuf,
    pub timeout_seconds: u64,
    pub user_agent: String,
    pub retries: u32,
    pub retry_backoff_ms: u64,
    pub max_redirects: usize,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub dry_run: bool,
    pub concurrency: usize,
    pub force: bool,
    pub manifest: PathBuf,
    pub log_level: LogLevel,
}

impl TryFrom<Cli> for AppConfig {
    type Error = anyhow::Error;

    fn try_from(value: Cli) -> Result<Self> {
        let root_url = Url::parse(&value.url).context("failed to parse root URL")?;
        if !root_url.path().ends_with('/') {
            bail!("root URL must end with '/': {root_url}");
        }
        if value.concurrency == 0 {
            bail!("--concurrency must be at least 1");
        }

        Ok(Self {
            root_url,
            output: value.output,
            timeout_seconds: value.timeout_seconds,
            user_agent: value.user_agent,
            retries: value.retries,
            retry_backoff_ms: value.retry_backoff_ms,
            max_redirects: value.max_redirects,
            includes: value.include,
            excludes: value.exclude,
            dry_run: value.dry_run,
            concurrency: value.concurrency,
            force: value.force,
            manifest: value.manifest,
            log_level: value.log_level,
        })
    }
}
