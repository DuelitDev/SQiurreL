use super::error::{QueryErr, Result};
use super::lexer::{Lexer, Token};
use std::mem::{discriminant, replace};

#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Create {
        table: Box<str>,
        defs: Box<Clause>,
        clauses: Vec<Clause>,
    },
    Insert {
        table: Box<str>,
        columns: Box<Clause>,
        values: Box<Clause>,
        clauses: Vec<Clause>,
    },
    Select {
        table: Box<str>,
        columns: Box<Clause>,
        clauses: Vec<Clause>,
    },
    Update {
        table: Box<str>,
        assigns: Box<Clause>,
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
    OrderBy(Vec<(Box<str>, bool)>),  // bool: true=ASC, false=DESC
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
    as_clause!(as_order_by, OrderBy, Vec<(Box<str>, bool)>);
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
        op: Box<str>,
        right: Box<Expr>,
    },
    Binary {
        op: Box<str>,
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

    fn consume_ident(&mut self) -> Result<Box<str>> {
        match self.next()? {
            Token::Ident(name) => Ok(name.into_boxed_str()),
            tok => Err(QueryErr::UnexpectedToken {
                expected: "<ident>".into(),
                found: format!("{:?}", tok),
            }),
        }
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
                expected: "<stmt>".into(),
                found: format!("{:?}", tok),
            }),
        }
    }

    fn parse_create(&mut self) -> Result<Stmt> {
        // CREATE TABLE <table> (<col1> <type>, <col2> <type>, ...)
        // CREATE TABLE <table> 파싱
        self.expect(&Token::Create)?;
        self.expect(&Token::Table)?;
        let table = self.consume_ident()?;
        // (<col1> <type>, <col2> <type>, ...) 파싱
        self.expect(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col_name = self.consume_ident()?;
            let col_type = self.consume_ident()?;
            columns.push((col_name, col_type));
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
        Ok(Stmt::Create {
            table,
            defs: Clause::Defs(columns).boxed(),
            clauses: vec![],
        })
    }

    fn parse_insert(&mut self) -> Result<Stmt> {
        // INSERT INTO <table> [(<col1>, <col2>, ...)] VALUES (<val1>, <val2>, ...)
        // INSERT INTO <table> 파싱
        self.expect(&Token::Insert)?;
        self.expect(&Token::Into)?;
        let table = self.consume_ident()?;
        // [(<col1>, <col2>, ...)] 파싱
        let mut columns = Vec::new();
        if self.maybe(&Token::LParen)? {
            // 괄호가 있는 경우, 부분 칼럼 파싱
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
        }
        // VALUES (<val1>, <val2>, ...) 파싱
        self.expect(&Token::Values)?;
        self.expect(&Token::LParen)?;
        let mut values = Vec::new();
        loop {
            values.push(self.parse_expr()?);
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
        Ok(Stmt::Insert {
            table,
            columns: Clause::Columns(columns).boxed(),
            values: Clause::Values(values).boxed(),
            clauses: vec![],
        })
    }

    fn parse_select(&mut self) -> Result<Stmt> {
        // SELECT <col1>, <col2>, ... FROM <table> [WHERE ...] [ORDER BY ...] [LIMIT ...]
        // SELECT <col1>, <col2>, ... 파싱
        self.expect(&Token::Select)?;
        let mut columns = Vec::new();
        if &self.curr == &Token::Mul {
            // 전체 선택 `*` 처리
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
        // FROM <table> 파싱
        self.expect(&Token::From)?;
        let table = self.consume_ident()?;

        let mut clauses = Vec::new();
        // WHERE ...
        if self.maybe(&Token::Where)? {
            clauses.push(Clause::Where(self.parse_expr()?.boxed()));
        }

        // TODO: ORDER BY, LIMIT 파싱

        Ok(Stmt::Select {
            table,
            columns: Clause::Columns(columns).boxed(),
            clauses,
        })
    }

    fn parse_update(&mut self) -> Result<Stmt> {
        // UPDATE <table> SET <col1> = <val1>, <col2> = <val2>, ... [WHERE ...]
        // UPDATE <table> SET 파싱
        self.expect(&Token::Update)?;
        let table = self.consume_ident()?;
        self.expect(&Token::Set)?;
        // <col1> = <val1>, <col2> = <val2>, ... 파싱
        let mut assigns = Vec::new();
        loop {
            let col = self.consume_ident()?;
            self.expect(&Token::Eq)?;
            let val = self.parse_expr()?;
            assigns.push((col, val));
            if !self.maybe(&Token::Comma)? {
                break;
            }
        }

        let mut clauses = Vec::new();
        if self.maybe(&Token::Where)? {
            clauses.push(Clause::Where(self.parse_expr()?.boxed()));
        }

        Ok(Stmt::Update {
            table,
            assigns: Clause::Assigns(assigns).boxed(),
            clauses,
        })
    }

    fn parse_delete(&mut self) -> Result<Stmt> {
        // DELETE FROM <table> [WHERE ...]
        // DELETE FROM <table> 파싱
        self.expect(&Token::Delete)?;
        self.expect(&Token::From)?;
        let table = self.consume_ident()?;

        let mut clauses = Vec::new();
        if self.maybe(&Token::Where)? {
            clauses.push(Clause::Where(self.parse_expr()?.boxed()));
        }

        Ok(Stmt::Delete { table, clauses })
    }

    fn parse_drop(&mut self) -> Result<Stmt> {
        // DROP TABLE <table>
        self.expect(&Token::Drop)?;
        self.expect(&Token::Table)?;
        let table = self.consume_ident()?;
        Ok(Stmt::Drop { table })
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_logical_or()
    }

    fn parse_logical_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_logical_and()?;
        while self.maybe(&Token::Or)? {
            let right = self.parse_logical_and()?;
            left = Expr::Binary {
                op: "OR".into(),
                left: left.boxed(),
                right: right.boxed(),
            };
        }
        Ok(left)
    }

    fn parse_logical_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_comparison()?;
        while self.maybe(&Token::And)? {
            let right = self.parse_comparison()?;
            left = Expr::Binary {
                op: "AND".into(),
                left: left.boxed(),
                right: right.boxed(),
            };
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_primary()?;
        let op = match &self.curr {
            Token::Eq => "=",
            Token::Gt => ">",
            Token::Lt => "<",
            Token::Ge => ">=",
            Token::Le => "<=",
            _ => return Ok(left),
        };
        self.next()?;
        let right = self.parse_primary()?;
        Ok(Expr::Binary {
            op: op.into(),
            left: left.boxed(),
            right: right.boxed(),
        })
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.next()? {
            Token::Null => Ok(Expr::Null),
            Token::Bool(b) => Ok(Expr::Bool(b)),
            Token::Num(n) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Expr::Int(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Expr::Float(f))
                } else {
                    Err(QueryErr::InvalidExpr(format!("Invalid number: {}", n)))
                }
            }
            Token::Text(t) => Ok(Expr::Text(t.into_boxed_str())),
            Token::Ident(i) => Ok(Expr::Ident(i.into_boxed_str())),
            Token::LParen => {
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            tok => Err(QueryErr::UnexpectedToken {
                expected: "<expr>".into(),
                found: format!("{:?}", tok),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::lexer::Lexer;

    #[test]
    fn test_parse_drop_table() {
        let lexer = Lexer::new("DROP TABLE users");
        let mut parser = Parser::new(lexer).unwrap();
        let stmt = parser.parse_stmt().unwrap();
        assert_eq!(
            stmt,
            Stmt::Drop {
                table: "users".into()
            }
        );
    }

    #[test]
    fn test_parse_select_star() {
        let lexer = Lexer::new("SELECT * FROM users");
        let mut parser = Parser::new(lexer).unwrap();
        let stmt = parser.parse_stmt().unwrap();
        assert_eq!(
            stmt,
            Stmt::Select {
                table: "users".into(),
                columns: Clause::Columns(vec!["*".into()]).boxed(),
                clauses: vec![],
            }
        );
    }

    #[test]
    fn test_parse_select_cols() {
        let lexer = Lexer::new("SELECT id, name FROM users");
        let mut parser = Parser::new(lexer).unwrap();
        let stmt = parser.parse_stmt().unwrap();
        assert_eq!(
            stmt,
            Stmt::Select {
                table: "users".into(),
                columns: Clause::Columns(vec!["id".into(), "name".into()]).boxed(),
                clauses: vec![],
            }
        );
    }

    #[test]
    fn test_parse_insert() {
        let lexer = Lexer::new("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        let mut parser = Parser::new(lexer).unwrap();
        let stmt = parser.parse_stmt().unwrap();
        assert_eq!(
            stmt,
            Stmt::Insert {
                table: "users".into(),
                columns: Clause::Columns(vec!["id".into(), "name".into()]).boxed(),
                values: Clause::Values(vec![Expr::Int(1), Expr::Text("Alice".into())]).boxed(),
                clauses: vec![],
            }
        );
    }

    #[test]
    fn test_parse_select_where() {
        let lexer = Lexer::new("SELECT * FROM users WHERE id = 1 AND name = 'Alice'");
        let mut parser = Parser::new(lexer).unwrap();
        let stmt = parser.parse_stmt().unwrap();

        let expected_where = Expr::Binary {
            op: "AND".into(),
            left: Expr::Binary {
                op: "=".into(),
                left: Expr::Ident("id".into()).boxed(),
                right: Expr::Int(1).boxed(),
            }
            .boxed(),
            right: Expr::Binary {
                op: "=".into(),
                left: Expr::Ident("name".into()).boxed(),
                right: Expr::Text("Alice".into()).boxed(),
            }
            .boxed(),
        };

        assert_eq!(
            stmt,
            Stmt::Select {
                table: "users".into(),
                columns: Clause::Columns(vec!["*".into()]).boxed(),
                clauses: vec![Clause::Where(expected_where.boxed())],
            }
        );
    }
}
