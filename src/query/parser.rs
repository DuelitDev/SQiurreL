use super::error::{QueryErr, Result};
use super::lexer::{Lexer, Token};
use std::mem::{discriminant, replace};

#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    // CREATE TABLE [IF NOT EXISTS] <table> (<col1> <type>, <col2> <type>, ...)
    Create {
        table: Box<str>,                    // table name
        columns: Vec<(Box<str>, Box<str>)>, // col name, col type
        if_not_exists: bool,                // run if not exists
    },
    // INSERT INTO <table> [(<col1>, <col2>, ...)] VALUES (<val1>, <val2>, ...)
    Insert {
        table: Box<str>,        // table name
        columns: Vec<Box<str>>, // col name
        values: Vec<Vec<Expr>>, // row [val expr]
    },
    // SELECT [DISTINCT] <col1>, <col2>, ... FROM <table>
    //     [WHERE] [GROUP BY] [HAVING] [ORDER BY] [LIMIT]
    Select {
        table: Box<str>,                     // table name
        columns: Vec<Expr>,                  // col name (or expr)
        distinct: bool,                      // distinct flag
        where_clause: Option<Expr>,          // condition expr
        group_by: Option<Vec<Expr>>,         // col name (or expr)
        having: Option<Expr>,                // condition expr
        order_by: Option<Vec<(Expr, bool)>>, // col name, ASC/DESC
        limit: Option<u64>,                  // limit count
    },
    // UPDATE <table> SET <col1> = <val1>, <col2> = <val2>, ... [WHERE]
    Update {
        table: Box<str>,                // table name
        assigns: Vec<(Box<str>, Expr)>, // col name, val expr
        where_clause: Option<Expr>,     // condition expr
    },
    // DELETE FROM <table> [WHERE]
    Delete {
        table: Box<str>,            // table name
        where_clause: Option<Expr>, // condition expr
    },
    // DROP TABLE [IF EXISTS] <table> [RESTRICT|CASCADE]
    Drop {
        table: Box<str>, // table name
        if_exists: bool, // run if exists
        cascade: bool,   // run despite dependent
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

    fn expect(&mut self, tokens: &[Token]) -> Result<()> {
        for token in tokens {
            if discriminant(&self.curr) == discriminant(token) {
                self.next()?;
            } else {
                return Err(QueryErr::UnexpectedToken {
                    expected: format!("{:?}", token),
                    found: format!("{:?}", self.curr),
                });
            }
        }
        Ok(())
    }

    fn maybe(&mut self, tokens: &[Token]) -> bool {
        match self.expect(tokens) {
            Ok(_) => true,
            Err(_) => false,
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
        // CREATE TABLE [IF NOT EXISTS] <table> (<col1> <type>, <col2> <type>, ...)
        self.expect(&[Token::Create, Token::Table])?;
        let if_not_exists = self.maybe(&[Token::If, Token::Not, Token::Exists]);
        let table = self.consume_ident()?;
        self.expect(&[Token::LParen])?;
        let columns = self.parse_list_clause(true, |p| {
            let col_name = p.consume_ident()?;
            let col_type = p.consume_ident()?;
            Ok((col_name, col_type))
        })?;
        Ok(Stmt::Create {
            table,
            columns,
            if_not_exists,
        })
    }

    fn parse_insert(&mut self) -> Result<Stmt> {
        // INSERT INTO <table> [(<col1>, <col2>, ...)] VALUES (<val1>, <val2>, ...)
        self.expect(&[Token::Insert, Token::Into])?;
        let table = self.consume_ident()?;
        let columns = if &self.curr == &Token::LParen {
            self.parse_list_clause(true, |p| p.consume_ident())?
        } else {
            vec![]
        };
        self.expect(&[Token::Values])?;
        let values = self.parse_list_clause(false, |p| {
            p.parse_list_clause(true, |p| p.parse_expr(0))
        })?;
        Ok(Stmt::Insert {
            table,
            columns,
            values,
        })
    }

    fn parse_select(&mut self) -> Result<Stmt> {
        // SELECT [DISTINCT] <col1>, <col2>, ... FROM <table>
        //     [WHERE] [GROUP BY] [HAVING] [ORDER BY] [LIMIT]
        self.expect(&[Token::Select])?;
        let distinct = self.maybe(&[Token::Distinct]);
        // 전체 컬럼 선택 '*' 처리
        let columns = if !self.maybe(&[Token::Mul]) {
            self.parse_list_clause(false, |p| p.parse_expr(0))?
        } else {
            vec![]
        };
        self.expect(&[Token::From])?;
        let table = self.consume_ident()?;
        // TODO: 최소 구현 우선
        let where_clause = None;
        let group_by = None;
        let having = None;
        let order_by = None;
        let limit = None;
        Ok(Stmt::Select {
            table,
            distinct,
            columns,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
        })
    }

    fn parse_update(&mut self) -> Result<Stmt> {
        // UPDATE <table> SET <col1> = <val1>, <col2> = <val2>, ... [WHERE]
        self.expect(&[Token::Update])?;
        let table = self.consume_ident()?;
        self.expect(&[Token::Set])?;
        let assigns = self.parse_list_clause(false, |p| {
            let col_name = p.consume_ident()?;
            p.expect(&[Token::Eq])?;
            let val_expr = p.parse_expr(0)?;
            Ok((col_name, val_expr))
        })?;
        // TODO: 최소 구현 우선
        let where_clause = None;
        Ok(Stmt::Update {
            table,
            assigns,
            where_clause,
        })
    }

    fn parse_delete(&mut self) -> Result<Stmt> {
        // DELETE FROM <table> [WHERE]
        self.expect(&[Token::Delete, Token::From])?;
        let table = self.consume_ident()?;
        // TODO: 최소 구현 우선
        let where_clause = None;
        Ok(Stmt::Delete {
            table,
            where_clause,
        })
    }

    fn parse_drop(&mut self) -> Result<Stmt> {
        // DROP TABLE [IF EXISTS] <table> [RESTRICT|CASCADE]
        self.expect(&[Token::Drop, Token::Table])?;
        let table = self.consume_ident()?;
        let if_exists = self.maybe(&[Token::If, Token::Exists]);
        let cascade = !self.maybe(&[Token::Restrict]) && self.maybe(&[Token::Cascade]);
        Ok(Stmt::Drop {
            table,
            if_exists,
            cascade,
        })
    }

    fn parse_list_clause<T, F>(
        &mut self,
        with_parens: bool,
        mut parse_fn: F,
    ) -> Result<Vec<T>>
    where
        F: FnMut(&mut Self) -> Result<T>,
    {
        if with_parens {
            self.expect(&[Token::LParen])?;
        }
        let mut items = Vec::new();
        loop {
            items.push(parse_fn(self)?);
            if !self.maybe(&[Token::Comma]) {
                break;
            }
        }
        if with_parens {
            self.expect(&[Token::RParen])?;
        }
        Ok(items)
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
        self.expect(&[Token::RParen])?;
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
