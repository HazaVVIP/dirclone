pub mod cli;
pub mod crawler;
pub mod downloader;
pub mod errors;
pub mod models;
pub mod parser;

use anyhow::Result;
use cli::{AppConfig, Cli};
use errors::FinalStatus;

pub fn execute(cli: Cli) -> Result<FinalStatus> {
    let config = AppConfig::try_from(cli)?;
    crawler::run(&config)
}
