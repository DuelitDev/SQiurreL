use std::fmt;

pub type Result<T> = std::result::Result<T, QueryErr>;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryErr {
    ReservedKeyword(String),
    UnexpectedEof,
    InvalidNum(String),
    UnterminatedText,
    InvalidIdent(String),
    InvalidToken(char),
    UnexpectedToken { expected: String, found: String },
    InvalidExpr(String),
}

impl fmt::Display for QueryErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservedKeyword(k) => write!(
                f,
                "Reserved keyword '{}' cannot be used as an identifier",
                k
            ),
            Self::UnexpectedEof => write!(f, "Unexpected end of file while parsing"),
            Self::InvalidNum(s) => write!(f, "Invalid number format: '{}'", s),
            Self::UnterminatedText => write!(f, "Unterminated text literal"),
            Self::InvalidIdent(i) => write!(f, "Invalid identifier: '{}'", i),
            Self::InvalidToken(c) => write!(f, "Invalid character: '{}'", c),
            Self::UnexpectedToken { expected, found } => {
                write!(f, "Expected {}, but found {}", expected, found)
            }
            Self::InvalidExpr(e) => write!(f, "Invalid expression: {}", e),
        }
    }
}

impl std::error::Error for QueryErr {}
