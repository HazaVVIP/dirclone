use clap::Parser;
use dirclone::cli::Cli;
use std::process::ExitCode;

fn main() -> ExitCode {
    let cli = Cli::parse();
    match dirclone::execute(cli) {
        Ok(status) => status.exit_code(),
        Err(err) => {
            eprintln!("Fatal error: {err:#}");
            ExitCode::from(1)
        }
    }
}
