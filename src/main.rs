pub mod executor;
pub mod query;
pub mod storage;
pub mod var_char;

use clap::Parser;
use std::io::{self, Write};
use tokio::fs;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(value_name = "DATABASE NAME")]
    database: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    // 데이터베이스 유무 체크
    if !fs::try_exists(&args.database).await.unwrap_or(false) {
        panic!("Database '{}' does not exist.", args.database);
    }
    println!("SQuirreL REPL (type '.exit' to stop)");
    println!("DATABASE: {}!", args.database);
}
