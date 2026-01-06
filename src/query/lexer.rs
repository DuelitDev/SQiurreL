use super::error::{QueryErr, Result};
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // 특수
    Eof,
    // 리터럴
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    // 타입
    BoolType,  // BOOL, BOOLEAN
    IntType,   // INT, INTEGER
    FloatType, // FLOAT, DOUBLE
    TextType,  // TEXT, STRING, VARCHAR
    // 식별자
    Ident(String),
    // 키워드
    Create, // CREATE
    Table,  // TABLE
    Insert, // INSERT
    Into,   // INTO
    Values, // VALUES
    Select, // SELECT
    From,   // FROM
    Update, // UPDATE
    Set,    // SET
    Alter,  // ALTER
    Delete, // DELETE
    Drop,   // DROP
    Union,  // UNION
    Where,  // WHERE
    Order,  // ORDER
    By,     // BY
    Asc,    // ASC
    Desc,   // DESC
    Limit,  // LIMIT
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
    Is,      // IS
    Eq,      // =
    Gt,      // >
    Lt,      // <
    Ge,      // >=
    Le,      // <=
    Add,     // +
    Sub,     // -
    Mul,     // *
    Div,     // /
}

pub struct Lexer {
    src: VecDeque<char>,
}

impl Lexer {
    pub fn new(src: &str) -> Self {
        Self {
            src: src.chars().collect(),
        }
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
        self.src.pop_front()
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.curr()
            && ch.is_whitespace()
        {
            self.walk();
        }
    }

    pub fn next(&mut self) -> Result<Token> {
        self.skip_ws();
        // 렉싱이 성공적으로 끝난 경우
        if self.finished() {
            return Ok(Token::Eof);
        }
        // 주석 파싱
        if self.peek(2) == "--" {
            self.walk();
            self.walk();
            while let Some(ch) = self.walk()
                && ch != '\n'
            {}
            self.skip_ws();
            return self.next();
        }
        let ch = self.walk().ok_or(QueryErr::UnexpectedEof)?;
        Ok(match ch {
            '.' => Token::Dot,
            ',' => Token::Comma,
            ';' => Token::Semicolon,
            '(' => Token::LParen,
            ')' => Token::RParen,
            '=' => Token::Eq,
            '>' => {
                if self.curr() == Some('=') {
                    self.walk();
                    Token::Ge
                } else {
                    Token::Gt
                }
            }
            '<' => {
                if self.curr() == Some('=') {
                    self.walk();
                    Token::Le
                } else {
                    Token::Lt
                }
            }
            '+' => Token::Add,
            '-' => Token::Sub,
            '*' => Token::Mul,
            '/' => Token::Div,
            '\'' | '"' => self.lex_text(ch)?,
            _ if Self::is_digit(ch) => self.lex_num(ch)?,
            _ if Self::is_letter(ch) => self.lex_keyword(ch)?,
            _ => return Err(QueryErr::InvalidToken(ch)),
        })
    }

    fn lex_text(&mut self, quote: char) -> Result<Token> {
        let mut out = String::new();
        while let Some(ch) = self.walk() {
            if ch == quote {
                return Ok(Token::Text(out));
            } else if ch == '\\' {
                let esc = self.walk().ok_or(QueryErr::UnterminatedText)?;
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
        Err(QueryErr::UnterminatedText)
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
            Err(QueryErr::InvalidNum(out))
        } else if float {
            if out.ends_with('.') {
                out.push('0');
            }
            Ok(Token::Float(
                out.parse::<f64>().map_err(|_| QueryErr::InvalidNum(out))?,
            ))
        } else {
            Ok(Token::Int(
                out.parse::<i64>().map_err(|_| QueryErr::InvalidNum(out))?,
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
            "NULL" => Token::Null,
            "TRUE" => Token::Bool(true),
            "FALSE" => Token::Bool(false),
            // 타입
            "BOOL" | "BOOLEAN" => Token::BoolType,
            "INT" | "INTEGER" => Token::IntType,
            "FLOAT" | "DOUBLE" => Token::FloatType,
            "TEXT" | "STRING" | "VARCHAR" => Token::TextType,
            // 키워드
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "ALTER" => Token::Alter,
            "DELETE" => Token::Delete,
            "DROP" => Token::Drop,
            "UNION" => Token::Union,
            "WHERE" => Token::Where,
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "LIMIT" => Token::Limit,
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_keywords() {
        let mut lexer = Lexer::new("SELECT FROM WHERE CREATE TABLE");
        assert_eq!(lexer.next().unwrap(), Token::Select);
        assert_eq!(lexer.next().unwrap(), Token::From);
        assert_eq!(lexer.next().unwrap(), Token::Where);
        assert_eq!(lexer.next().unwrap(), Token::Create);
        assert_eq!(lexer.next().unwrap(), Token::Table);
    }

    #[test]
    fn test_identifiers() {
        let mut lexer = Lexer::new("my_table user_id123");
        assert_eq!(lexer.next().unwrap(), Token::Ident("my_table".to_string()));
        assert_eq!(
            lexer.next().unwrap(),
            Token::Ident("user_id123".to_string())
        );
    }

    #[test]
    fn test_numbers() {
        let mut lexer = Lexer::new("123 45.67");
        assert_eq!(lexer.next().unwrap(), Token::Int(123i64));
        assert_eq!(lexer.next().unwrap(), Token::Float(45.67f64));
    }

    #[test]
    fn test_strings() {
        let mut lexer = Lexer::new("'hello' \"world\" 'it\\'s me'");
        assert_eq!(lexer.next().unwrap(), Token::Text("hello".to_string()));
        assert_eq!(lexer.next().unwrap(), Token::Text("world".to_string()));
        assert_eq!(lexer.next().unwrap(), Token::Text("it's me".to_string()));
    }

    #[test]
    fn test_operators() {
        let mut lexer = Lexer::new("= > < >= <= + - * /");
        assert_eq!(lexer.next().unwrap(), Token::Eq);
        assert_eq!(lexer.next().unwrap(), Token::Gt);
        assert_eq!(lexer.next().unwrap(), Token::Lt);
        assert_eq!(lexer.next().unwrap(), Token::Ge);
        assert_eq!(lexer.next().unwrap(), Token::Le);
        assert_eq!(lexer.next().unwrap(), Token::Add);
        assert_eq!(lexer.next().unwrap(), Token::Sub);
        assert_eq!(lexer.next().unwrap(), Token::Mul);
        assert_eq!(lexer.next().unwrap(), Token::Div);
    }

    #[test]
    fn test_delimiters() {
        let mut lexer = Lexer::new(". , ; ( )");
        assert_eq!(lexer.next().unwrap(), Token::Dot);
        assert_eq!(lexer.next().unwrap(), Token::Comma);
        assert_eq!(lexer.next().unwrap(), Token::Semicolon);
        assert_eq!(lexer.next().unwrap(), Token::LParen);
        assert_eq!(lexer.next().unwrap(), Token::RParen);
    }

    #[test]
    fn test_complex_query() {
        let mut lexer = Lexer::new("SELECT name FROM users WHERE id = 1;");
        assert_eq!(lexer.next().unwrap(), Token::Select);
        assert_eq!(lexer.next().unwrap(), Token::Ident("name".to_string()));
        assert_eq!(lexer.next().unwrap(), Token::From);
        assert_eq!(lexer.next().unwrap(), Token::Ident("users".to_string()));
        assert_eq!(lexer.next().unwrap(), Token::Where);
        assert_eq!(lexer.next().unwrap(), Token::Ident("id".to_string()));
        assert_eq!(lexer.next().unwrap(), Token::Eq);
        assert_eq!(lexer.next().unwrap(), Token::Int(1i64));
        assert_eq!(lexer.next().unwrap(), Token::Semicolon);
    }

    #[test]
    fn test_case_insensitivity() {
        let mut lexer = Lexer::new("select From WhErE");
        assert_eq!(lexer.next().unwrap(), Token::Select);
        assert_eq!(lexer.next().unwrap(), Token::From);
        assert_eq!(lexer.next().unwrap(), Token::Where);
    }

    #[test]
    fn test_unterminated_string() {
        let mut lexer = Lexer::new("'unfinished");
        match lexer.next() {
            Err(QueryErr::UnterminatedText) => (),
            _ => panic!("Expected UnterminatedText error"),
        }
    }

    #[test]
    fn test_boolean_and_null() {
        let mut lexer = Lexer::new("TRUE FALSE NULL");
        assert_eq!(lexer.next().unwrap(), Token::Bool(true));
        assert_eq!(lexer.next().unwrap(), Token::Bool(false));
        assert_eq!(lexer.next().unwrap(), Token::Null);
    }

    #[test]
    fn test_logical_operators() {
        let mut lexer = Lexer::new("NOT AND OR");
        assert_eq!(lexer.next().unwrap(), Token::Not);
        assert_eq!(lexer.next().unwrap(), Token::And);
        assert_eq!(lexer.next().unwrap(), Token::Or);
    }

    #[test]
    fn test_invalid_numbers() {
        let mut lexer = Lexer::new("1.2.3");
        assert_eq!(lexer.next().unwrap(), Token::Float(1.2f64));
        assert_eq!(lexer.next().unwrap(), Token::Dot);
        assert_eq!(lexer.next().unwrap(), Token::Int(3i64));
    }

    #[test]
    fn test_comments() {
        let mut lexer = Lexer::new("SELECT -- comment\nFROM users");
        assert_eq!(lexer.next().unwrap(), Token::Select);
        assert_eq!(lexer.next().unwrap(), Token::From);
        assert_eq!(lexer.next().unwrap(), Token::Ident("users".to_string()));
    }

    #[test]
    fn test_hex_not_supported() {
        let mut lexer = Lexer::new("0x123");
        assert_eq!(lexer.next().unwrap(), Token::Int(0i64));
        assert_eq!(lexer.next().unwrap(), Token::Ident("x123".to_string()));
    }
}
