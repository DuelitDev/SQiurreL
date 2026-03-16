pub mod error;
pub mod lexer;
pub mod parser;

pub use lexer::Lexer;
pub use parser::{Expr, Parser, Stmt};

#[derive(Debug, Clone, PartialEq)]
pub enum ValType {
    Int,
    Bool,
    Float,
    String,
}
