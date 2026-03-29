use super::error::{QueryErr, QueryErrKind, Result};
use super::span::Span;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // 특수
    Eof,
    // 리터럴
    Nil,
    Int(i64),
    Real(f64),
    Bool(bool),
    Text(String),
    // 타입
    IntType,  // INT, INTEGER
    RealType, // FLOAT, DOUBLE
    BoolType, // BOOL, BOOLEAN
    TextType, // TEXT, STRING, VARCHAR
    // 식별자
    Ident(String),
    // 키워드
    Create,   // CREATE
    Table,    // TABLE
    If,       // IF
    Exists,   // EXISTS
    Insert,   // INSERT
    Into,     // INTO
    Values,   // VALUES
    Select,   // SELECT
    Distinct, // DISTINCT
    From,     // FROM
    Where,    // WHERE
    Group,    // GROUP
    By,       // BY
    Having,   // HAVING
    Order,    // ORDER
    Asc,      // ASC
    Desc,     // DESC
    Limit,    // LIMIT
    Update,   // UPDATE
    Set,      // SET
    Alter,    // ALTER
    Add,      // ADD
    Column,   // COLUMN
    Rename,   // RENAME
    To,       // TO
    Delete,   // DELETE
    Truncate, // TRUNCATE
    Drop,     // DROP
    Restrict, // RESTRICT
    Cascade,  // CASCADE
    Union,    // UNION
    // 구분자
    Dot,       // .
    Comma,     // ,
    Semicolon, // ;
    LParen,    // (
    RParen,    // )
    // 연산자
    Not,     // NOT
    And,     // AND
    Or,      // OR
    In,      // IN
    Like,    // LIKE
    Between, // BETWEEN
    Is,      // I
    OpEq,    // =
    OpGt,    // >
    OpLt,    // <
    OpGe,    // >=
    OpLe,    // <=
    OpAdd,   // +
    OpSub,   // -
    OpMul,   // *
    OpDiv,   // /
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpannedToken {
    pub token: Token,
    pub span: Span,
}

pub struct Lexer {
    src: VecDeque<char>,
    span: Span,
}

impl Lexer {
    pub fn new(src: &str) -> Self {
        Self { src: src.chars().collect(), span: Span::default() }
    }

    fn is_letter(ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_digit(ch: char) -> bool {
        ch.is_ascii_digit()
    }

    fn finished(&self) -> bool {
        self.src.is_empty()
    }

    fn curr(&self) -> Option<char> {
        self.src.front().copied()
    }

    fn peek(&self, step: usize) -> String {
        self.src.iter().take(step).collect()
    }

    fn walk(&mut self) -> Option<char> {
        let ch = self.src.pop_front()?;
        self.span.len += 1;
        Some(ch)
    }

    fn skip(&mut self) -> Option<char> {
        let ch = self.src.pop_front()?;
        self.span.pos += 1;
        if ch == '\n' {
            self.span.line += 1;
            self.span.col = 1;
        } else {
            self.span.col += 1;
        }
        Some(ch)
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.curr()
            && ch.is_whitespace()
        {
            self.skip();
        }
    }

    fn begin_span(&mut self) {
        self.span.pos += self.span.len;
        self.span.col += self.span.len;
        self.span.len = 0;
    }

    fn err(&self, kind: QueryErrKind) -> QueryErr {
        QueryErr { kind, span: self.span }
    }

    pub fn next(&mut self) -> Result<SpannedToken> {
        self.begin_span();
        loop {
            self.skip_ws();
            if self.peek(2) == "--" {
                self.skip();
                self.skip();
                while let Some(ch) = self.skip()
                    && ch != '\n'
                {}
                continue;
            }
            break;
        }
        // 렉싱이 성공적으로 끝난 경우
        if self.finished() {
            return Ok(SpannedToken { token: Token::Eof, span: self.span });
        }
        let ch = self.walk().ok_or_else(|| self.err(QueryErrKind::UnexpectedEof))?;
        let token = match ch {
            '.' => Token::Dot,
            ',' => Token::Comma,
            ';' => Token::Semicolon,
            '(' => Token::LParen,
            ')' => Token::RParen,
            '=' => Token::OpEq,
            '>' => {
                if self.curr() == Some('=') {
                    self.walk();
                    Token::OpGe
                } else {
                    Token::OpGt
                }
            }
            '<' => {
                if self.curr() == Some('=') {
                    self.walk();
                    Token::OpLe
                } else {
                    Token::OpLt
                }
            }
            '+' => Token::OpAdd,
            '-' => Token::OpSub,
            '*' => Token::OpMul,
            '/' => Token::OpDiv,
            '\'' | '"' => self.lex_text(ch)?,
            _ if Self::is_digit(ch) => self.lex_num(ch)?,
            _ if Self::is_letter(ch) => self.lex_keyword(ch)?,
            _ => return Err(self.err(QueryErrKind::InvalidToken(ch))),
        };
        Ok(SpannedToken { token, span: self.span })
    }

    fn lex_text(&mut self, quote: char) -> Result<Token> {
        let mut out = String::new();
        while let Some(ch) = self.walk() {
            if ch == quote {
                return Ok(Token::Text(out));
            } else if ch == '\n' {
                return Err(self.err(QueryErrKind::UnterminatedText));
            } else if ch == '\\' {
                let esc = self
                    .walk()
                    .ok_or_else(|| self.err(QueryErrKind::UnterminatedText))?;
                match esc {
                    '\\' => out.push('\\'),
                    '\'' => out.push('\''),
                    '"' => out.push('"'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    _ => {
                        out.push(ch);
                        out.push(esc);
                    }
                }
            } else {
                out.push(ch);
            }
        }
        Err(self.err(QueryErrKind::UnterminatedText))
    }

    fn lex_num(&mut self, start: char) -> Result<Token> {
        let mut float = false;
        let mut out = String::from(start);
        while let Some(ch) = self.curr() {
            // ! `curr()`의 반환값이 `Some`이므로 안전함
            if Self::is_digit(ch) {
                out.push(self.walk().unwrap());
            } else if ch == '.' && !float {
                float = true;
                out.push(self.walk().unwrap());
            } else {
                break;
            }
        }
        if out.is_empty() {
            Err(self.err(QueryErrKind::InvalidNum(out)))
        } else if float {
            if out.ends_with('.') {
                out.push('0');
            }
            Ok(Token::Real(
                out.parse::<f64>()
                    .map_err(|_| self.err(QueryErrKind::InvalidNum(out)))?,
            ))
        } else {
            Ok(Token::Int(
                out.parse::<i64>()
                    .map_err(|_| self.err(QueryErrKind::InvalidNum(out)))?,
            ))
        }
    }

    fn lex_keyword(&mut self, start: char) -> Result<Token> {
        let mut out = String::from(start);
        while let Some(ch) = self.curr()
            && (Self::is_letter(ch) || Self::is_digit(ch))
        {
            // ! `curr()`의 반환값이 `Some`이므로 안전함
            out.push(self.walk().unwrap());
        }
        // 키워드 매칭
        Ok(match out.to_uppercase().as_str() {
            // 리터럴
            "NULL" => Token::Nil,
            "TRUE" => Token::Bool(true),
            "FALSE" => Token::Bool(false),
            // 타입
            "INT" | "INTEGER" => Token::IntType,
            "FLOAT" | "DOUBLE" => Token::RealType,
            "BOOL" | "BOOLEAN" => Token::BoolType,
            "TEXT" | "STRING" | "VARCHAR" => Token::TextType,
            // 키워드
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "IF" => Token::If,
            "EXISTS" => Token::Exists,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "SELECT" => Token::Select,
            "DISTINCT" => Token::Distinct,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "GROUP" => Token::Group,
            "BY" => Token::By,
            "HAVING" => Token::Having,
            "ORDER" => Token::Order,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "LIMIT" => Token::Limit,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "ALTER" => Token::Alter,
            "ADD" => Token::Add,
            "COLUMN" => Token::Column,
            "RENAME" => Token::Rename,
            "TO" => Token::To,
            "DELETE" => Token::Delete,
            "TRUNCATE" => Token::Truncate,
            "DROP" => Token::Drop,
            "RESTRICT" => Token::Restrict,
            "CASCADE" => Token::Cascade,
            "UNION" => Token::Union,
            // 연산자
            "NOT" => Token::Not,
            "AND" => Token::And,
            "OR" => Token::Or,
            "IN" => Token::In,
            "LIKE" => Token::Like,
            "BETWEEN" => Token::Between,
            "IS" => Token::Is,
            _ => Token::Ident(out),
        })
    }
}
