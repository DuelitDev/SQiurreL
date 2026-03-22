use thiserror::Error;

pub type Result<T> = std::result::Result<T, QueryErr>;

#[derive(Debug, Clone, PartialEq, Error)]
pub enum QueryErr {
    #[error("Unexpected end of file while parsing")]
    UnexpectedEof,
    #[error("Invalid number format: '{0}'")]
    InvalidNum(String),
    #[error("Unterminated text literal")]
    UnterminatedText,
    #[error("Invalid character: '{0}'")]
    InvalidToken(char),
    #[error("Invalid identifier: '{0}'")]
    InvalidIdent(String),
    #[error("Invalid expression: {0}")]
    InvalidExpr(String),
    #[error("Expected {expected}, but found {found}")]
    UnexpectedToken { expected: String, found: String },
}
