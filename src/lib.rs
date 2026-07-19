pub mod cli;
pub mod crawler;
pub mod errors;
pub mod fetcher;
pub mod models;
pub mod parser;
pub mod store;

use anyhow::Result;
use cli::{AppConfig, Cli};
use errors::FinalStatus;

pub async fn execute(cli: Cli) -> Result<FinalStatus> {
    let config = AppConfig::try_from(cli)?;
    crawler::run(&config).await
}
