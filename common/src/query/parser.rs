use super::error::{QueryErr, Result};
use super::lexer::{Lexer, Token};
use crate::storage::DataType;
use std::mem::{discriminant, replace};

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Stmt {
    // CREATE TABLE [IF NOT EXISTS] <table> (<col1> <type>, <col2> <type>, ...)
    Create {
        table: Box<str>,                    // table name
        columns: Vec<(Box<str>, DataType)>, // col name, col type
        if_not_exists: bool,                // run if not exists
    },
    // INSERT INTO <table> [(<col1>, <col2>, ...)] VALUES (<val1>, <val2>, ...)
    InsertValues {
        table: Box<str>,        // table name
        columns: Vec<Box<str>>, // col name
        rows: Vec<Vec<Expr>>, // row [val expr]
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
    AlterAdd {
        table: Box<str>,              // table name
        column: (Box<str>, DataType), // col name, col type
    },
    AlterDrop {
        table: Box<str>,  // table name
        column: Box<str>, // col name
    },
    AlterRename {
        table: Box<str>,    // table name
        new_name: Box<str>, // new table name
    },
    // DELETE FROM <table> [WHERE]
    Delete {
        table: Box<str>,            // table name
        where_clause: Option<Expr>, // condition expr
    },
    // TRUNCATE TABLE <table>
    Truncate {
        table: Box<str>, // table name
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
    Defs(Vec<(Box<str>, DataType)>), // col name, col type
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
    as_clause!(as_defs, Defs, Vec<(Box<str>, DataType)>);
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
            Token::OpEq => 3,
            Token::OpGt | Token::OpLt | Token::OpGe | Token::OpLe => 4,
            Token::OpAdd | Token::OpSub => 5,
            Token::OpMul | Token::OpDiv => 6,
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

    fn maybe(&mut self, tokens: &[Token]) -> Result<bool> {
        if tokens.is_empty() {
            Ok(true)
        } else if discriminant(&self.curr) != discriminant(&tokens[0]) {
            Ok(false)
        } else {
            self.expect(&tokens).map(|_| true)
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
            Token::Alter => self.parse_alter(),
            Token::Delete => self.parse_delete(),
            Token::Truncate => self.parse_truncate(),
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
        let if_not_exists = self.maybe(&[Token::If, Token::Not, Token::Exists])?;
        let table = self.consume_ident()?;
        let columns = self.parse_list_clause(true, |p| {
            let col_name = p.consume_ident()?;
            let col_type = p.consume_type()?;
            Ok((col_name, col_type))
        })?;
        Ok(Stmt::Create {
            table,
            columns,
            if_not_exists,
        })
    }

    fn parse_insert(&mut self) -> Result<Stmt> {
        // INSERT INTO <table> [(<col1>, <col2>, ...)] ...
        self.expect(&[Token::Insert, Token::Into])?;
        let table = self.consume_ident()?;
        let columns = if &self.curr == &Token::LParen {
            self.parse_list_clause(true, |p| p.consume_ident())?
        } else {
            vec![]
        };
        if self.maybe(&[Token::Values])? {
            self.parse_insert_values(table, columns)
        } else if self.maybe(&[Token::Select])? {
            unimplemented!("최소 구현 우선 (INSERT ... SELECT 지원 보류)")
        } else {
            Err(QueryErr::UnexpectedToken {
                expected: "VALUES or SELECT".into(),
                found: format!("{:?}", self.curr),
            })
        }
    }

    fn parse_insert_values(&mut self, table: Box<str>, columns: Vec<Box<str>>) -> Result<Stmt> {
        // ... VALUES (<val1>, <val2>, ...)
        let rows =
            self.parse_list_clause(false, |p| p.parse_list_clause(true, |p| p.parse_expr(0)))?;
        Ok(Stmt::InsertValues {
            table,
            columns,
            rows,
        })
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
            p.expect(&[Token::OpEq])?;
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
            Err(QueryErr::UnexpectedToken {
                expected: "ADD, DROP, or RENAME".into(),
                found: format!("{:?}", self.curr),
            })
        }
    }
    fn parse_alter_add(&mut self, table: Box<str>) -> Result<Stmt> {
        // ... ADD COLUMN <col_name> <col_type>
        let col_name = self.consume_ident()?;
        let col_type = self.consume_type()?;
        let column = (col_name, col_type);
        Ok(Stmt::AlterAdd { table, column })
    }

    fn parse_alter_drop(&mut self, table: Box<str>) -> Result<Stmt> {
        // ... DROP COLUMN <col_name>
        let column = self.consume_ident()?;
        Ok(Stmt::AlterDrop { table, column })
    }

    fn parse_alter_rename(&mut self, table: Box<str>) -> Result<Stmt> {
        // ... RENAME TO <new_table_name>
        let new_name = self.consume_ident()?;
        Ok(Stmt::AlterRename { table, new_name })
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

    fn parse_truncate(&mut self) -> Result<Stmt> {
        self.expect(&[Token::Truncate, Token::Table])?;
        let table = self.consume_ident()?;
        Ok(Stmt::Truncate { table })
    }

    fn parse_drop(&mut self) -> Result<Stmt> {
        // DROP TABLE [IF EXISTS] <table> [RESTRICT|CASCADE]
        self.expect(&[Token::Drop, Token::Table])?;
        let if_exists = self.maybe(&[Token::If, Token::Exists])?;
        let table = self.consume_ident()?;
        let cascade = !self.maybe(&[Token::Restrict])? && self.maybe(&[Token::Cascade])?;
        Ok(Stmt::Drop {
            table,
            if_exists,
            cascade,
        })
    }

    fn parse_list_clause<T, F>(&mut self, with_parens: bool, mut parse_fn: F) -> Result<Vec<T>>
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
        match self.next()? {
            Token::Ident(name) => Ok(name.into_boxed_str()),
            tok => Err(QueryErr::UnexpectedToken {
                expected: "identifier".into(),
                found: format!("{:?}", tok),
            }),
        }
    }

    fn consume_type(&mut self) -> Result<DataType> {
        match self.next()? {
            Token::BoolType => Ok(DataType::Bool),
            Token::IntType => Ok(DataType::Int),
            Token::FloatType => Ok(DataType::Float),
            Token::TextType => Ok(DataType::VChar),
            tok => Err(QueryErr::UnexpectedToken {
                expected: "type".into(),
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
            op @ (Token::Not | Token::OpSub) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> Stmt {
        let lexer = Lexer::new(input);
        let mut parser = Parser::new(lexer).unwrap();
        let mut stmts = parser.parse().unwrap();
        assert_eq!(stmts.len(), 1);
        stmts.pop().unwrap()
    }

    #[test]
    fn test_create_table() {
        let input = "CREATE TABLE users (id INT, name TEXT);";
        let stmt = parse(input);
        match stmt {
            Stmt::Create {
                table,
                columns,
                if_not_exists,
            } => {
                assert_eq!(table.as_ref(), "users");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0], ("id".into(), DataType::Int));
                assert_eq!(columns[1], ("name".into(), DataType::VChar));
                assert!(!if_not_exists);
            }
            _ => panic!("Expected Create stmt"),
        }

        let input_if_not_exists = "CREATE TABLE IF NOT EXISTS items (price FLOAT);";
        let stmt = parse(input_if_not_exists);
        match stmt {
            Stmt::Create {
                table,
                columns,
                if_not_exists,
            } => {
                assert_eq!(table.as_ref(), "items");
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0], ("price".into(), DataType::Float));
                assert!(if_not_exists);
            }
            _ => panic!("Expected Create stmt"),
        }
    }

    #[test]
    fn test_insert() {
        let input = "INSERT INTO users VALUES (1, 'Alice');";
        let stmt = parse(input);
        match stmt {
            Stmt::InsertValues {
                table,
                columns,
                rows,
            } => {
                assert_eq!(table.as_ref(), "users");
                assert!(columns.is_empty()); // 컬럼 명시 안함
                assert_eq!(rows.len(), 1); // 1 row
                assert_eq!(rows[0].len(), 2);
                assert_eq!(rows[0][0], Expr::Int(1));
                assert_eq!(rows[0][1], Expr::Text("Alice".into()));
            }
            _ => panic!("Expected InsertValues stmt"),
        }

        // 컬럼 명시
        let input_cols = "INSERT INTO users (id, name) VALUES (2, 'Bob');";
        let stmt = parse(input_cols);
        match stmt {
            Stmt::InsertValues {
                table,
                columns,
                rows,
            } => {
                assert_eq!(table.as_ref(), "users");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].as_ref(), "id");
                assert_eq!(columns[1].as_ref(), "name");
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected InsertValues stmt"),
        }
    }

    #[test]
    fn test_select() {
        let input = "SELECT id, name FROM users;";
        let stmt = parse(input);
        match stmt {
            Stmt::Select {
                table,
                columns,
                distinct,
                ..
            } => {
                assert_eq!(table.as_ref(), "users");
                assert_eq!(columns.len(), 2);
                assert!(!distinct);
            }
            _ => panic!("Expected Select stmt"),
        }

        let input_all = "SELECT * FROM users;";
        let stmt = parse(input_all);
        match stmt {
            Stmt::Select { columns, .. } => {
                assert!(columns.is_empty()); // * is parsed as empty columns list
            }
            _ => panic!("Expected Select stmt"),
        }

        let input_distinct = "SELECT DISTINCT id FROM users;";
        let stmt = parse(input_distinct);
        match stmt {
            Stmt::Select { distinct, .. } => {
                assert!(distinct);
            }
            _ => panic!("Expected Select stmt"),
        }
    }

    #[test]
    fn test_update() {
        let input = "UPDATE users SET name = 'Charlie', score = score + 1;";
        let stmt = parse(input);
        match stmt {
            Stmt::Update { table, assigns, .. } => {
                assert_eq!(table.as_ref(), "users");
                assert_eq!(assigns.len(), 2);
                assert_eq!(assigns[0].0.as_ref(), "name");
                assert_eq!(assigns[1].0.as_ref(), "score");
            }
            _ => panic!("Expected Update stmt"),
        }
    }

    #[test]
    fn test_alter() {
        let input_add = "ALTER TABLE users ADD COLUMN age INT;";
        let stmt = parse(input_add);
        match stmt {
            Stmt::AlterAdd { table, column } => {
                assert_eq!(table.as_ref(), "users");
                assert_eq!(column, ("age".into(), DataType::Int));
            }
            _ => panic!("Expected AlterAdd stmt"),
        }

        let input_drop = "ALTER TABLE users DROP COLUMN age;";
        let stmt = parse(input_drop);
        match stmt {
            Stmt::AlterDrop { table, column } => {
                assert_eq!(table.as_ref(), "users");
                assert_eq!(column.as_ref(), "age");
            }
            _ => panic!("Expected AlterDrop stmt"),
        }

        let input_rename = "ALTER TABLE users RENAME TO super_users;";
        let stmt = parse(input_rename);
        match stmt {
            Stmt::AlterRename { table, new_name } => {
                assert_eq!(table.as_ref(), "users");
                assert_eq!(new_name.as_ref(), "super_users");
            }
            _ => panic!("Expected AlterRename stmt"),
        }
    }

    #[test]
    fn test_delete() {
        let input = "DELETE FROM users;";
        let stmt = parse(input);
        match stmt {
            Stmt::Delete { table, .. } => {
                assert_eq!(table.as_ref(), "users");
            }
            _ => panic!("Expected Delete stmt"),
        }
    }

    #[test]
    fn test_truncate() {
        let input = "TRUNCATE TABLE users;";
        let stmt = parse(input);
        match stmt {
            Stmt::Truncate { table } => {
                assert_eq!(table.as_ref(), "users");
            }
            _ => panic!("Expected Truncate stmt"),
        }
    }

    #[test]
    fn test_drop() {
        let input = "DROP TABLE items;";
        let stmt = parse(input);
        match stmt {
            Stmt::Drop {
                table,
                if_exists,
                cascade,
            } => {
                assert_eq!(table.as_ref(), "items");
                assert!(!if_exists);
                assert!(!cascade);
            }
            _ => panic!("Expected Drop stmt"),
        }

        let input_opts = "DROP TABLE IF EXISTS items CASCADE;";
        let stmt = parse(input_opts);
        match stmt {
            Stmt::Drop {
                if_exists, cascade, ..
            } => {
                assert!(if_exists);
                assert!(cascade);
            }
            _ => panic!("Expected Drop stmt"),
        }
    }
}
