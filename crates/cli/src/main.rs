mod client;
mod format;
mod tokenize;

use clap::Parser;
use client::Client;
use format::format_value;
use kvforge_core::{Command, Value};
use std::io::{self, BufRead, Write};
use tokenize::tokenize;

#[derive(Parser)]
#[command(name = "kvforge-cli", about = "Command-line client for kvforge")]
struct Args {
    #[arg(long, env = "KVFORGE_ADDR", default_value = "127.0.0.1:6390")]
    addr: String,

    /// A single command to run and exit, e.g. `kvforge-cli GET name`.
    /// Omit to start an interactive session instead.
    command: Vec<String>,
}

/// Builds a `Command` from raw string tokens by routing them through the
/// same request parser the server uses, so the CLI and server can never
/// disagree about what a command means.
fn command_from_args(args: &[String]) -> Result<Command, String> {
    if args.is_empty() {
        return Err("empty command".to_string());
    }
    let request = Value::array(
        args.iter()
            .map(|a| Value::bulk(a.as_bytes().to_vec()))
            .collect(),
    );
    Command::from_request(&request).map_err(|e| e.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut client = Client::connect(&args.addr).await?;

    if !args.command.is_empty() {
        match command_from_args(&args.command) {
            Ok(command) => {
                let response = client.call(&command).await?;
                println!("{}", format_value(&response));
            }
            Err(err) => eprintln!("(error) {err}"),
        }
        return Ok(());
    }

    run_repl(&mut client, &args.addr).await
}

async fn run_repl(client: &mut Client, addr: &str) -> anyhow::Result<()> {
    println!("kvforge-cli connected to {addr}");
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    loop {
        print!("kvforge> ");
        stdout.flush()?;

        let mut line = String::new();
        if stdin.lock().read_line(&mut line)? == 0 {
            println!();
            return Ok(());
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if matches!(line.to_ascii_lowercase().as_str(), "quit" | "exit") {
            return Ok(());
        }

        let tokens = match tokenize(line) {
            Ok(tokens) => tokens,
            Err(err) => {
                eprintln!("(error) {err}");
                continue;
            }
        };
        let command = match command_from_args(&tokens) {
            Ok(command) => command,
            Err(err) => {
                eprintln!("(error) {err}");
                continue;
            }
        };
        match client.call(&command).await {
            Ok(response) => println!("{}", format_value(&response)),
            Err(err) => {
                eprintln!("(error) connection lost: {err}");
                return Ok(());
            }
        }
    }
}
