pub mod cli;
pub mod crawler;
pub mod errors;
pub mod fetcher;
pub mod manifest;
pub mod models;
pub mod parser;
pub mod progress;

use anyhow::Result;
use cli::{AppConfig, Cli};
use errors::FinalStatus;
use progress::Progress;

pub async fn execute(cli: Cli) -> Result<FinalStatus> {
    let config = AppConfig::try_from(cli)?;
    // Install a process-wide progress bar so log calls route through it. The
    // library entry point owns the lifetime; tests that call `crawler::run`
    // directly get the plain-stderr fallback because `ACTIVE` stays unset.
    let progress = Progress::new(config.no_progress.then_some(false));
    progress.install();
    let status = crawler::run(&config).await;
    progress.finish();
    status
}
