use super::error::{QueryErr, QueryErrKind, Result};
use super::lexer::{Lexer, SpannedToken, Token};
use super::span::Span;
use crate::schema::DataType;
use std::mem::{discriminant, replace};

#[derive(Debug, Clone, PartialEq)]
pub struct SpannedStmt {
    pub stmt: Stmt,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Stmt {
    // CREATE TABLE [IF NOT EXISTS] <table> (<col1> <type>, <col2> <type>, ...)
    Create {
        table_name: Box<str>,               // table name
        defines: Vec<(Box<str>, DataType)>, // col name, col type
        if_not_exists: bool,                // run if not exists
    },
    // INSERT INTO <table> [(<col1>, <col2>, ...)] VALUES (<val1>, <val2>, ...)
    InsertValues {
        table_name: Box<str>,   // table name
        columns: Vec<Box<str>>, // col name
        values: Vec<Expr>,   // val expr
    },
    // SELECT [DISTINCT] <col1>, <col2>, ... FROM <table>
    //     [WHERE] [GROUP BY] [HAVING] [ORDER BY] [LIMIT]
    Select {
        table_name: Box<str>,                // table name
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
        table_name: Box<str>,           // table name
        assigns: Vec<(Box<str>, Expr)>, // col name, val expr
        where_clause: Option<Expr>,     // condition expr
    },
    AlterAdd {
        table_name: Box<str>,         // table name
        define: (Box<str>, DataType), // col name, col type
    },
    AlterDrop {
        table_name: Box<str>, // table name
        column: Box<str>,     // col name
    },
    AlterRename {
        table_name: Box<str>, // table name
        new_name: Box<str>,   // new table name
    },
    // DELETE FROM <table> [WHERE]
    Delete {
        table_name: Box<str>,       // table name
        where_clause: Option<Expr>, // condition expr
    },
    // TRUNCATE TABLE <table>
    Truncate {
        table_name: Box<str>, // table name
    },
    // DROP TABLE [IF EXISTS] <table> [RESTRICT|CASCADE]
    Drop {
        table_name: Box<str>, // table name
        if_exists: bool,      // run if exists
        cascade: bool,        // run despite dependent
    },
}

impl Stmt {
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Expr {
    Nil,
    Int(i64),
    Real(f64),
    Bool(bool),
    Text(Box<str>),
    Ident(Box<str>),
    Unary { op: Token, right: Box<Expr> },
    Binary { op: Token, left: Box<Expr>, right: Box<Expr> },
}

impl Expr {
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

pub struct Parser {
    lexer: Lexer,
    curr: SpannedToken,
    peek: SpannedToken,
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
            Token::OpEq => 3,
            Token::OpGt | Token::OpLt | Token::OpGe | Token::OpLe => 4,
            Token::OpAdd | Token::OpSub => 5,
            Token::OpMul | Token::OpDiv => 6,
            Token::LParen => 7,
            _ => 0,
        }
    }

    fn next(&mut self) -> Result<SpannedToken> {
        Ok(replace(&mut self.curr, replace(&mut self.peek, self.lexer.next()?)))
    }

    fn expect(&mut self, tokens: &[Token]) -> Result<()> {
        for token in tokens {
            if discriminant(&self.curr.token) == discriminant(token) {
                self.next()?;
            } else {
                return Err(QueryErr {
                    kind: QueryErrKind::UnexpectedToken {
                        expected: format!("{:?}", token),
                        found: format!("{:?}", self.curr.token),
                    },
                    span: self.curr.span,
                });
            }
        }
        Ok(())
    }

    fn maybe(&mut self, tokens: &[Token]) -> Result<bool> {
        if tokens.is_empty() {
            Ok(true)
        } else if discriminant(&self.curr.token) != discriminant(&tokens[0]) {
            Ok(false)
        } else {
            self.expect(&tokens).map(|_| true)
        }
    }

    pub fn parse(&mut self) -> Result<Vec<SpannedStmt>> {
        let mut stmts = Vec::new();
        while discriminant(&Token::Eof) != discriminant(&self.curr.token) {
            if self.curr.token == Token::Semicolon {
                self.next()?;
                continue;
            }
            let span = self.curr.span;
            let stmt = self.parse_stmt()?;
            stmts.push(SpannedStmt { stmt, span });
        }
        Ok(stmts)
    }

    pub fn parse_stmt(&mut self) -> Result<Stmt> {
        match &self.curr.token {
            Token::Create => self.parse_create(),
            Token::Insert => self.parse_insert(),
            Token::Select => self.parse_select(),
            Token::Update => self.parse_update(),
            Token::Alter => self.parse_alter(),
            Token::Delete => self.parse_delete(),
            Token::Truncate => self.parse_truncate(),
            Token::Drop => self.parse_drop(),
            tok => Err(QueryErr {
                kind: QueryErrKind::UnexpectedToken {
                    expected: "SELECT, INSERT, UPDATE, DELETE, CREATE, DROP".into(),
                    found: format!("{:?}", tok),
                },
                span: self.curr.span,
            }),
        }
    }

    fn parse_create(&mut self) -> Result<Stmt> {
        // CREATE TABLE [IF NOT EXISTS] <table> (<col1> <type>, <col2> <type>, ...)
        self.expect(&[Token::Create, Token::Table])?;
        let if_not_exists = self.maybe(&[Token::If, Token::Not, Token::Exists])?;
        let table = self.consume_ident()?;
        let columns = self.parse_list_clause(true, |p| {
            let col_name = p.consume_ident()?;
            let col_type = p.consume_type()?;
            Ok((col_name, col_type))
        })?;
        Ok(Stmt::Create { table_name: table, defines: columns, if_not_exists })
    }

    fn parse_insert(&mut self) -> Result<Stmt> {
        // INSERT INTO <table> [(<col1>, <col2>, ...)] ...
        self.expect(&[Token::Insert, Token::Into])?;
        let table = self.consume_ident()?;
        let columns = if self.curr.token == Token::LParen {
            self.parse_list_clause(true, |p| p.consume_ident())?
        } else {
            vec![]
        };
        if self.maybe(&[Token::Values])? {
            self.parse_insert_values(table, columns)
        } else if self.maybe(&[Token::Select])? {
            unimplemented!("최소 구현 우선 (INSERT ... SELECT 지원 보류)")
        } else {
            Err(QueryErr {
                kind: QueryErrKind::UnexpectedToken {
                    expected: "VALUES or SELECT".into(),
                    found: format!("{:?}", self.curr.token),
                },
                span: self.curr.span,
            })
        }
    }

    fn parse_insert_values(
        &mut self,
        table: Box<str>,
        columns: Vec<Box<str>>,
    ) -> Result<Stmt> {
        // ... VALUES (<val1>, <val2>, ...)
        let values = self.parse_list_clause(true, |p| p.parse_expr(0))?;
        Ok(Stmt::InsertValues { table_name: table, columns, values })
    }

    fn parse_select(&mut self) -> Result<Stmt> {
        // SELECT [DISTINCT] <col1>, <col2>, ... FROM <table>
        //     [WHERE] [GROUP BY] [HAVING] [ORDER BY] [LIMIT]
        self.expect(&[Token::Select])?;
        let distinct = self.maybe(&[Token::Distinct])?;
        // 전체 컬럼 선택 '*' 처리
        let columns = if !self.maybe(&[Token::OpMul])? {
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
            table_name: table,
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
            p.expect(&[Token::OpEq])?;
            let val_expr = p.parse_expr(0)?;
            Ok((col_name, val_expr))
        })?;
        // TODO: 최소 구현 우선
        let where_clause = None;
        Ok(Stmt::Update { table_name: table, assigns, where_clause })
    }

    fn parse_alter(&mut self) -> Result<Stmt> {
        // ALTER TABLE <table> ...
        self.expect(&[Token::Alter, Token::Table])?;
        let table = self.consume_ident()?;
        if self.maybe(&[Token::Add, Token::Column])? {
            self.parse_alter_add(table)
        } else if self.maybe(&[Token::Drop, Token::Column])? {
            self.parse_alter_drop(table)
        } else if self.maybe(&[Token::Rename, Token::To])? {
            self.parse_alter_rename(table)
        } else {
            Err(QueryErr {
                kind: QueryErrKind::UnexpectedToken {
                    expected: "ADD, DROP, or RENAME".into(),
                    found: format!("{:?}", self.curr.token),
                },
                span: self.curr.span,
            })
        }
    }
    fn parse_alter_add(&mut self, table: Box<str>) -> Result<Stmt> {
        // ... ADD COLUMN <col_name> <col_type>
        let col_name = self.consume_ident()?;
        let col_type = self.consume_type()?;
        let column = (col_name, col_type);
        Ok(Stmt::AlterAdd { table_name: table, define: column })
    }

    fn parse_alter_drop(&mut self, table: Box<str>) -> Result<Stmt> {
        // ... DROP COLUMN <col_name>
        let column = self.consume_ident()?;
        Ok(Stmt::AlterDrop { table_name: table, column })
    }

    fn parse_alter_rename(&mut self, table: Box<str>) -> Result<Stmt> {
        // ... RENAME TO <new_table_name>
        let new_name = self.consume_ident()?;
        Ok(Stmt::AlterRename { table_name: table, new_name })
    }

    fn parse_delete(&mut self) -> Result<Stmt> {
        // DELETE FROM <table> [WHERE]
        self.expect(&[Token::Delete, Token::From])?;
        let table = self.consume_ident()?;
        // TODO: 최소 구현 우선
        let where_clause = None;
        Ok(Stmt::Delete { table_name: table, where_clause })
    }

    fn parse_truncate(&mut self) -> Result<Stmt> {
        self.expect(&[Token::Truncate, Token::Table])?;
        let table = self.consume_ident()?;
        Ok(Stmt::Truncate { table_name: table })
    }

    fn parse_drop(&mut self) -> Result<Stmt> {
        // DROP TABLE [IF EXISTS] <table> [RESTRICT|CASCADE]
        self.expect(&[Token::Drop, Token::Table])?;
        let if_exists = self.maybe(&[Token::If, Token::Exists])?;
        let table = self.consume_ident()?;
        let cascade =
            !self.maybe(&[Token::Restrict])? && self.maybe(&[Token::Cascade])?;
        Ok(Stmt::Drop { table_name: table, if_exists, cascade })
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
            if !self.maybe(&[Token::Comma])? {
                break;
            }
        }
        if with_parens {
            self.expect(&[Token::RParen])?;
        }
        Ok(items)
    }

    fn consume_ident(&mut self) -> Result<Box<str>> {
        let spanned = self.next()?;
        match spanned.token {
            Token::Ident(name) => Ok(name.into_boxed_str()),
            tok => Err(QueryErr {
                kind: QueryErrKind::UnexpectedToken {
                    expected: "identifier".into(),
                    found: format!("{:?}", tok),
                },
                span: spanned.span,
            }),
        }
    }

    fn consume_type(&mut self) -> Result<DataType> {
        let spanned = self.next()?;
        match spanned.token {
            Token::IntType => Ok(DataType::Int),
            Token::RealType => Ok(DataType::Real),
            Token::BoolType => Ok(DataType::Bool),
            Token::TextType => Ok(DataType::Text),
            tok => Err(QueryErr {
                kind: QueryErrKind::UnexpectedToken {
                    expected: "type".into(),
                    found: format!("{:?}", tok),
                },
                span: spanned.span,
            }),
        }
    }

    fn parse_expr(&mut self, prec: u8) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        while prec < Self::precedence(&self.curr.token) {
            left = self.parse_binary(left)?;
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        let spanned = self.next()?;
        match spanned.token {
            Token::Nil => Ok(Expr::Nil),
            Token::Int(n) => Ok(Expr::Int(n)),
            Token::Real(f) => Ok(Expr::Real(f)),
            Token::Bool(b) => Ok(Expr::Bool(b)),
            Token::Text(t) => Ok(Expr::Text(t.into_boxed_str())),
            Token::Ident(i) => Ok(Expr::Ident(i.into_boxed_str())),
            op @ (Token::Not | Token::OpSub) => {
                let right = self.parse_expr(7)?.boxed();
                Ok(Expr::Unary { op, right })
            }
            Token::LParen => self.parse_group(),
            tok => Err(QueryErr {
                kind: QueryErrKind::UnexpectedToken {
                    expected: "expression (literal, identifier, or '(')".into(),
                    found: format!("{:?}", tok),
                },
                span: spanned.span,
            }),
        }
    }

    fn parse_group(&mut self) -> Result<Expr> {
        let expr = self.parse_expr(0)?;
        self.expect(&[Token::RParen])?;
        Ok(expr)
    }

    fn parse_binary(&mut self, left: Expr) -> Result<Expr> {
        let spanned = self.next()?;
        let prec = Self::precedence(&spanned.token);
        match spanned.token {
            op if prec > 0 => {
                let left = left.boxed();
                let right = self.parse_expr(prec)?.boxed();
                Ok(Expr::Binary { op, left, right })
            }
            tok => Err(QueryErr {
                kind: QueryErrKind::UnexpectedToken {
                    expected: "binary operator".to_string(),
                    found: format!("{:?}", tok),
                },
                span: spanned.span,
            }),
        }
    }
}
