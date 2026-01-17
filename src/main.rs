pub mod executor;
pub mod query;
pub mod storage;
pub mod var_char;

use clap::Parser;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(value_name = "DATABASE NAME")]
    database: Option<PathBuf>,
}

fn main() {
    let args = Args::parse();
    // 데이터베이스 유무 체크
    if let None = args.database {
        eprintln!("No database specified. \
                   use '--help' for more information.");
        return
    } else if let Some(ref path) = args.database && !path.exists() {
        eprintln!("Database file not found: '{}'", path.display());
        return
    }
    // ! 위 조건문에서 확인했으므로 안전
    let path = args.database.unwrap();
    // TODO: Executor에서 데이터베이스를 열기
    println!("SQuirreL REPL (type '.exit' or '.quit' to stop)");
    let mut buffer = String::new();
    loop {
        if buffer.is_empty() {
            print!("sql> ");
        } else {
            print!("...  ");
        }
        io::stdout().flush().unwrap();
        let line = io::stdin().lock().lines().next();
        if let Some(Ok(input)) = line {
            // 종료 명령어 처리
            if input.trim() == ".exit" || input.trim() == ".quit" {
                break;
            } else if !input.trim().ends_with(";") {
                buffer.push_str(&input);
                buffer.push('\n');
            } else {
                buffer.push_str(&input);
                let src = std::mem::take(&mut buffer);
                println!("{}", src);
                // TODO: Executor에서 명령을 실행하기
            }
        } else {
            println!("Failed to read line.");
        }
    }
}
