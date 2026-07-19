use clap::Parser;
use dirclone::cli::Cli;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match dirclone::execute(cli).await {
        Ok(status) => status.exit_code(),
        Err(err) => {
            eprintln!("Fatal error: {err:#}");
            ExitCode::from(1)
        }
    }
}
