use anyhow::Result;
use clap::{Parser, Subcommand};
use qwerk::{Database, Pessimistic};
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    #[clap(short, long)]
    path: PathBuf,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Get { key: String },
    Insert { key: String, value: String },
    Remove { key: String },
    Epoch,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let db = Database::<Pessimistic>::open(&cli.path)?;
    match cli.command {
        Command::Get { key } => {
            let mut worker = db.spawn_worker();
            let mut txn = worker.begin_transaction();
            if let Some(value) = txn.get(&key)? {
                println!("{}", String::from_utf8_lossy(value));
            }
            txn.commit()?;
        }
        Command::Insert { key, value } => {
            let mut worker = db.spawn_worker();
            let mut txn = worker.begin_transaction();
            if let Some(value) = txn.get(&key)? {
                println!("{}", String::from_utf8_lossy(value));
            }
            txn.insert(&key, &value)?;
            txn.commit()?;
            db.flush()?;
        }
        Command::Remove { key } => {
            let mut worker = db.spawn_worker();
            let mut txn = worker.begin_transaction();
            if let Some(value) = txn.get(&key)? {
                println!("{}", String::from_utf8_lossy(value));
            }
            txn.remove(&key)?;
            txn.commit()?;
            db.flush()?;
        }
        Command::Epoch => println!("{}", db.durable_epoch().0),
    }
    Ok(())
}
