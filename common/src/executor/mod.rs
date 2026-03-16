use crate::query::{Expr, Lexer, Parser, Stmt};

pub enum QueryResult {
    Rows(Vec<Vec<String>>),
    Count(usize),
    Success,
    Error(String),
}

pub struct TableView {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

pub struct Executor;

impl Executor {
    pub fn new() -> Self {
        Self
    }

    pub async fn run(&mut self, src: String) -> QueryResult {
        QueryResult::Success
    }
}
