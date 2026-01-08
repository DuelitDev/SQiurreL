use super::error::{QueryErr, Result};
use super::lexer::{Lexer, Token};
use std::mem::{discriminant, replace};

#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Create {
        table: Box<str>,
        clauses: Vec<Clause>,
    },
    Insert {
        table: Box<str>,
        clauses: Vec<Clause>,
    },
    Select {
        table: Box<str>,
        clauses: Vec<Clause>,
    },
    Update {
        table: Box<str>,
        clauses: Vec<Clause>,
    },
    Delete {
        table: Box<str>,
        clauses: Vec<Clause>,
    },
    Drop {
        table: Box<str>,
    },
    Union {
        left: Box<Stmt>,
        right: Box<Stmt>,
        all: bool,
    },
}

impl Stmt {
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    Values(Vec<Expr>),               // expr
    Columns(Vec<Box<str>>),          // col name
    Assigns(Vec<(Box<str>, Expr)>),  // col name, expr
    Defs(Vec<(Box<str>, Box<str>)>), // col name, col type
    OrderBy(Vec<(Box<Expr>, bool)>), // bool: true=ASC, false=DESC
    Where(Box<Expr>),
    Limit(u64),
}

macro_rules! as_clause {
    ($name:ident, $variant:ident, $ret:ty) => {
        pub fn $name(&self) -> Option<&$ret> {
            if let Clause::$variant(inner) = self {
                Some(inner)
            } else {
                None
            }
        }
    };
}

impl Clause {
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
    as_clause!(as_values, Values, Vec<Expr>);
    as_clause!(as_columns, Columns, Vec<Box<str>>);
    as_clause!(as_assigns, Assigns, Vec<(Box<str>, Expr)>);
    as_clause!(as_defs, Defs, Vec<(Box<str>, Box<str>)>);
    as_clause!(as_order_by, OrderBy, Vec<(Box<Expr>, bool)>);
    as_clause!(as_where, Where, Expr);
    as_clause!(as_limit, Limit, u64);
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(Box<str>),
    Ident(Box<str>),
    Unary {
        op: Token,
        right: Box<Expr>,
    },
    Binary {
        op: Token,
        left: Box<Expr>,
        right: Box<Expr>,
    },
}

impl Expr {
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

pub struct Parser {
    lexer: Lexer,
    curr: Token,
    peek: Token,
}

impl Parser {
    pub fn new(mut lexer: Lexer) -> Result<Self> {
        let curr = lexer.next()?;
        let peek = lexer.next()?;
        Ok(Self { lexer, curr, peek })
    }

    fn precedence(token: &Token) -> u8 {
        match token {
            Token::Or => 1,
            Token::And => 2,
            Token::Eq => 3,
            Token::Gt | Token::Lt | Token::Ge | Token::Le => 4,
            Token::Add | Token::Sub => 5,
            Token::Mul | Token::Div => 6,
            Token::LParen => 7,
            _ => 0,
        }
    }

    fn next(&mut self) -> Result<Token> {
        Ok(replace(
            &mut self.curr,
            replace(&mut self.peek, self.lexer.next()?),
        ))
    }

    fn expect(&mut self, token: &Token) -> Result<()> {
        if discriminant(&self.curr) == discriminant(token) {
            self.next()?;
            Ok(())
        } else {
            Err(QueryErr::UnexpectedToken {
                expected: format!("{:?}", token),
                found: format!("{:?}", self.curr),
            })
        }
    }

    fn maybe(&mut self, token: &Token) -> Result<bool> {
        if discriminant(&self.curr) == discriminant(token) {
            self.next()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn parse(&mut self) -> Result<Vec<Stmt>> {
        self.parse_block(&[Token::Eof])
    }

    fn parse_block(&mut self, terms: &[Token]) -> Result<Vec<Stmt>> {
        let mut stmts = Vec::new();
        while !terms
            .iter()
            .any(|t| discriminant(t) == discriminant(&self.curr))
        {
            if self.curr == Token::Semicolon {
                self.next()?;
                continue;
            }
            let stmt = self.parse_stmt()?;
            stmts.push(stmt);
        }
        Ok(stmts)
    }

    pub fn parse_stmt(&mut self) -> Result<Stmt> {
        match &self.curr {
            Token::Create => self.parse_create(),
            Token::Insert => self.parse_insert(),
            Token::Select => self.parse_select(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Drop => self.parse_drop(),
            tok => Err(QueryErr::UnexpectedToken {
                expected: "SELECT, INSERT, UPDATE, DELETE, CREATE, DROP".into(),
                found: format!("{:?}", tok),
            }),
        }
    }

    fn parse_create(&mut self) -> Result<Stmt> {
        // CREATE TABLE <table> (<col1> <type>, <col2> <type>, ...)
        self.expect(&Token::Create)?;
        self.expect(&Token::Table)?;
        let table = self.consume_ident()?;
        let mut clauses = vec![];
        clauses.push(self.parse_defs_clause()?);
        clauses.extend(self.parse_optional_clauses()?);
        Ok(Stmt::Create { table, clauses })
    }

    fn parse_insert(&mut self) -> Result<Stmt> {
        // INSERT INTO <table> [(<col1>, <col2>, ...)] VALUES (<val1>, <val2>, ...)
        self.expect(&Token::Insert)?;
        self.expect(&Token::Into)?;
        let table = self.consume_ident()?;
        let mut clauses = vec![];
        // 부분 컬럼 선택 '(<col1>, <col2>, ...)' 처리
        if &self.curr == &Token::LParen {
            clauses.push(self.parse_columns_clause()?);
        }
        self.expect(&Token::Values)?;
        clauses.push(self.parse_defs_clause()?);
        // INSERT는 부가 절이 없음
        Ok(Stmt::Insert { table, clauses })
    }

    fn parse_select(&mut self) -> Result<Stmt> {
        // SELECT <col1>, <col2>, ... FROM <table> [WHERE ...] [ORDER BY ...] [LIMIT ...]
        self.expect(&Token::Select)?;
        let mut clauses = vec![];
        // 전체 컬럼 선택 '*' 처리
        if !self.maybe(&Token::Mul)? {
            clauses.push(self.parse_columns_clause()?);
        }
        self.expect(&Token::From)?;
        let table = self.consume_ident()?;
        clauses.extend(self.parse_optional_clauses()?);
        Ok(Stmt::Select { table, clauses })
    }

    fn parse_update(&mut self) -> Result<Stmt> {
        // UPDATE <table> SET <col1> = <val1>, <col2> = <val2>, ... [WHERE ...]
        self.expect(&Token::Update)?;
        let table = self.consume_ident()?;
        self.expect(&Token::Set)?;
        let mut clauses = vec![];
        clauses.push(self.parse_assigns_clause()?);
        clauses.extend(self.parse_optional_clauses()?);
        Ok(Stmt::Update { table, clauses })
    }

    fn parse_delete(&mut self) -> Result<Stmt> {
        // DELETE FROM <table> [WHERE ...]
        self.expect(&Token::Delete)?;
        self.expect(&Token::From)?;
        let table = self.consume_ident()?;
        let clauses = self.parse_optional_clauses()?;
        Ok(Stmt::Delete { table, clauses })
    }

    fn parse_drop(&mut self) -> Result<Stmt> {
        // DROP TABLE <table>
        self.expect(&Token::Drop)?;
        self.expect(&Token::Table)?;
        let table = self.consume_ident()?;
        Ok(Stmt::Drop { table })
    }

    fn parse_values_clause(&mut self) -> Result<Clause> {
        self.expect(&Token::LParen)?;
        let mut values = Vec::new();
        loop {
            values.push(self.parse_expr(0)?);
            match self.next()? {
                Token::Comma => continue,
                Token::RParen => break,
                tok => {
                    return Err(QueryErr::UnexpectedToken {
                        expected: "',' or ')'".into(),
                        found: format!("{:?}", tok),
                    });
                }
            }
        }
        Ok(Clause::Values(values))
    }

    fn parse_columns_clause(&mut self) -> Result<Clause> {
        let mut columns = Vec::new();
        if self.maybe(&Token::LParen)? {
            loop {
                columns.push(self.consume_ident()?);
                match self.next()? {
                    Token::Comma => continue,
                    Token::RParen => break,
                    tok => {
                        return Err(QueryErr::UnexpectedToken {
                            expected: "',' or ')'".into(),
                            found: format!("{:?}", tok),
                        });
                    }
                }
            }
        } else if self.curr == Token::Mul {
            self.next()?;
            columns.push("*".into());
        } else {
            loop {
                columns.push(self.consume_ident()?);
                if !self.maybe(&Token::Comma)? {
                    break;
                }
            }
        }
        Ok(Clause::Columns(columns))
    }

    fn parse_assigns_clause(&mut self) -> Result<Clause> {
        let mut assigns = Vec::new();
        loop {
            let col = self.consume_ident()?;
            self.expect(&Token::Eq)?;
            let val = self.parse_expr(0)?;
            assigns.push((col, val));
            if !self.maybe(&Token::Comma)? {
                break;
            }
        }
        Ok(Clause::Assigns(assigns))
    }

    fn parse_defs_clause(&mut self) -> Result<Clause> {
        self.expect(&Token::LParen)?;
        let mut defs = Vec::new();
        loop {
            let col_name = self.consume_ident()?;
            let col_type = self.consume_ident()?;
            defs.push((col_name, col_type));
            match self.next()? {
                Token::Comma => continue,
                Token::RParen => break,
                tok => {
                    return Err(QueryErr::UnexpectedToken {
                        expected: "',' or ')'".into(),
                        found: format!("{:?}", tok),
                    });
                }
            }
        }
        Ok(Clause::Defs(defs))
    }

    fn parse_optional_clauses(&mut self) -> Result<Vec<Clause>> {
        let mut clauses = Vec::new();
        loop {
            match &self.curr {
                Token::Where => {
                    self.next()?;
                    let expr = self.parse_expr(0)?;
                    clauses.push(Clause::Where(expr.boxed()));
                }
                _ => break,
            }
        }
        Ok(clauses)
    }

    fn consume_ident(&mut self) -> Result<Box<str>> {
        match self.next()? {
            Token::Ident(name) => Ok(name.into_boxed_str()),
            tok => Err(QueryErr::UnexpectedToken {
                expected: "identifier".into(),
                found: format!("{:?}", tok),
            }),
        }
    }

    fn parse_expr(&mut self, prec: u8) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        while prec < Self::precedence(&self.curr) {
            left = self.parse_binary(left)?;
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        match self.next()? {
            Token::Null => Ok(Expr::Null),
            Token::Bool(b) => Ok(Expr::Bool(b)),
            Token::Int(n) => Ok(Expr::Int(n)),
            Token::Float(f) => Ok(Expr::Float(f)),
            Token::Text(t) => Ok(Expr::Text(t.into_boxed_str())),
            Token::Ident(i) => Ok(Expr::Ident(i.into_boxed_str())),
            op @ (Token::Not | Token::Sub) => {
                let right = self.parse_expr(7)?.boxed();
                Ok(Expr::Unary { op, right })
            }
            Token::LParen => self.parse_group(),
            tok => Err(QueryErr::UnexpectedToken {
                expected: "expression (literal, identifier, or '(')".into(),
                found: format!("{:?}", tok),
            }),
        }
    }

    fn parse_group(&mut self) -> Result<Expr> {
        let expr = self.parse_expr(0)?;
        self.expect(&Token::RParen)?;
        Ok(expr)
    }

    fn parse_binary(&mut self, left: Expr) -> Result<Expr> {
        let token = self.next()?;
        let prec = Self::precedence(&token);
        match token {
            op if prec > 0 => {
                let left = left.boxed();
                let right = self.parse_expr(prec)?.boxed();
                Ok(Expr::Binary { op, left, right })
            }
            _ => Err(QueryErr::UnexpectedToken {
                expected: "binary operator".to_string(),
                found: format!("{:?}", token),
            }),
        }
    }
}
