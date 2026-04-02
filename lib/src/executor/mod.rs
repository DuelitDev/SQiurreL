use crate::query::lexer::Token;
use crate::query::{Expr, QueryErr, Stmt};
use crate::schema::{DataType, DataValue};
use crate::storage::{ColId, RowId, RowState, Storage, StorageErr, TableState};
use std::cmp::Ordering;
use std::collections::HashSet;

#[derive(serde::Serialize)]
#[serde(tag = "type", content = "data")]
pub enum QueryResult {
    Rows { columns: Vec<String>, rows: Vec<Vec<String>> },
    Count(usize),
    Success,
    Err(String),
}

#[derive(Debug, thiserror::Error)]
pub enum SQRLErr {
    #[error("{0}")]
    StorageErr(#[from] StorageErr),

    #[error("{0}")]
    QueryErr(#[from] QueryErr),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("duplicate column: {0}")]
    DuplicateColumn(String),

    #[error("column count mismatch: expected {expected}, got {got}")]
    ColumnCountMismatch { expected: usize, got: usize },

    #[error("type mismatch for column '{column}': expected {expected:?}, got {got:?}")]
    TypeMismatch { column: String, expected: DataType, got: DataType },

    #[error("cannot resolve identifier: {0}")]
    CannotResolveIdentifier(String),

    #[error("invalid unary operation: {0}")]
    InvalidUnaryOp(String),

    #[error("invalid binary operation: {0}")]
    InvalidBinaryOp(String),

    #[error("predicate must evaluate to BOOL, got {0:?}")]
    InvalidPredicate(DataType),

    #[error("unsupported feature: {0}")]
    UnsupportedFeature(String),

    #[error("invalid function call: {0}")]
    InvalidFunction(String),
}

pub type Result<T> = std::result::Result<T, SQRLErr>;

pub struct Executor {
    storage: Storage,
}

impl Executor {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    pub fn schema_ddl(&self) -> String {
        let mut tables = self
            .storage
            .state
            .tables
            .values()
            .filter(|table| table.alive)
            .collect::<Vec<_>>();
        tables.sort_by(|left, right| left.name.cmp(&right.name));

        if tables.is_empty() {
            return "-- No tables defined.".to_string();
        }

        tables.into_iter().map(Self::format_table_ddl).collect::<Vec<_>>().join("\n\n")
    }
}

impl Executor {
    fn format_table_ddl(table: &TableState) -> String {
        let columns = table
            .live_cols()
            .map(|column| {
                format!(
                    "  {} {}",
                    column.name,
                    Self::format_data_type(column.data_type)
                )
            })
            .collect::<Vec<_>>()
            .join(",\n");

        format!("CREATE TABLE {} (\n{}\n);", table.name, columns)
    }

    fn format_data_type(data_type: DataType) -> &'static str {
        match data_type {
            DataType::Nil => "NIL",
            DataType::Int => "INT",
            DataType::Real => "REAL",
            DataType::Bool => "BOOL",
            DataType::Text => "TEXT",
        }
    }

    fn format_value(value: &DataValue) -> String {
        match value {
            DataValue::Nil => "nil".to_string(),
            DataValue::Int(value) => value.to_string(),
            DataValue::Real(value) => value.to_string(),
            DataValue::Bool(value) => value.to_string(),
            DataValue::Text(value) => value.to_string(),
        }
    }

    fn expr_label(expr: &Expr) -> String {
        match expr {
            Expr::Ident(name) => name.to_string(),
            Expr::Wildcard => "*".to_string(),
            Expr::List(values) => format!(
                "({})",
                values.iter().map(Self::expr_label).collect::<Vec<_>>().join(", ")
            ),
            Expr::Call { name, args } => format!(
                "{}({})",
                name,
                args.iter().map(Self::expr_label).collect::<Vec<_>>().join(", ")
            ),
            _ => format!("{expr:?}"),
        }
    }

    fn is_aggregate(expr: &Expr) -> bool {
        matches!(expr, Expr::Call { name, .. } if matches!(name.to_ascii_uppercase().as_str(), "MAX" | "COUNT"))
    }

    fn compare_values(left: &DataValue, right: &DataValue) -> Result<Ordering> {
        let ord = match (left, right) {
            (DataValue::Int(left), DataValue::Int(right)) => Some(left.cmp(right)),
            (DataValue::Int(left), DataValue::Real(right)) => {
                (*left as f64).partial_cmp(right)
            }
            (DataValue::Real(left), DataValue::Int(right)) => {
                left.partial_cmp(&(*right as f64))
            }
            (DataValue::Real(left), DataValue::Real(right)) => left.partial_cmp(right),
            (DataValue::Text(left), DataValue::Text(right)) => Some(left.cmp(right)),
            (DataValue::Bool(left), DataValue::Bool(right)) => Some(left.cmp(right)),
            (DataValue::Nil, DataValue::Nil) => Some(Ordering::Equal),
            _ => None,
        };
        ord.ok_or_else(|| {
            SQRLErr::InvalidFunction(format!(
                "cannot compare {:?} and {:?}",
                left.data_type(),
                right.data_type()
            ))
        })
    }

    fn eval_aggregate(
        &self,
        expr: &Expr,
        table: &TableState,
        rows: &[&RowState],
    ) -> Result<DataValue> {
        let Expr::Call { name, args } = expr else {
            return Err(SQRLErr::InvalidFunction(
                "expected aggregate call".to_string(),
            ));
        };

        match name.to_ascii_uppercase().as_str() {
            "COUNT" => {
                if args.len() != 1 {
                    return Err(SQRLErr::InvalidFunction(
                        "COUNT() expects exactly one argument".to_string(),
                    ));
                }
                let count = match &args[0] {
                    Expr::Wildcard => rows.len() as i64,
                    arg => rows
                        .iter()
                        .map(|row| self.eval_in_row(arg, Some(table), Some(row)))
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .filter(|value| *value != DataValue::Nil)
                        .count() as i64,
                };
                Ok(DataValue::Int(count))
            }
            "MAX" => {
                if args.len() != 1 {
                    return Err(SQRLErr::InvalidFunction(
                        "MAX() expects exactly one argument".to_string(),
                    ));
                }
                if matches!(args[0], Expr::Wildcard) {
                    return Err(SQRLErr::InvalidFunction(
                        "MAX(*) is not supported".to_string(),
                    ));
                }
                let mut max_value: Option<DataValue> = None;
                for row in rows {
                    let value = self.eval_in_row(&args[0], Some(table), Some(row))?;
                    if value == DataValue::Nil {
                        continue;
                    }
                    match &max_value {
                        Some(current)
                            if Self::compare_values(&value, current)?
                                != Ordering::Greater => {}
                        _ => max_value = Some(value),
                    }
                }
                Ok(max_value.unwrap_or(DataValue::Nil))
            }
            _ => Err(SQRLErr::UnsupportedFeature(format!("function {name}"))),
        }
    }

    fn eval(&self, expr: &Expr) -> Result<DataValue> {
        self.eval_in_row(expr, None, None)
    }

    fn eval_in_row(
        &self,
        expr: &Expr,
        table: Option<&TableState>,
        row: Option<&RowState>,
    ) -> Result<DataValue> {
        match expr {
            Expr::Nil => Ok(DataValue::Nil),
            Expr::Int(i) => Ok(DataValue::Int(*i)),
            Expr::Real(r) => Ok(DataValue::Real(*r)),
            Expr::Bool(b) => Ok(DataValue::Bool(*b)),
            Expr::Text(s) => Ok(DataValue::Text(s.clone())),
            Expr::Wildcard => {
                Err(SQRLErr::UnsupportedFeature("wildcard expression".to_string()))
            }
            Expr::List(_) => {
                Err(SQRLErr::UnsupportedFeature("list expression".to_string()))
            }
            Expr::Call { name, .. } => Err(SQRLErr::UnsupportedFeature(format!(
                "function {name} outside aggregate SELECT"
            ))),
            Expr::Ident(name) => {
                let (table, row) = match (table, row) {
                    (Some(table), Some(row)) => (table, row),
                    _ => {
                        return Err(SQRLErr::CannotResolveIdentifier(name.to_string()));
                    }
                };
                let col = table.get_col_by_name(name).ok_or_else(|| {
                    SQRLErr::CannotResolveIdentifier(name.to_string())
                })?;
                row.values
                    .get(&col.id)
                    .cloned()
                    .ok_or_else(|| SQRLErr::CannotResolveIdentifier(name.to_string()))
            }
            Expr::Unary { op, right } => {
                let value = self.eval_in_row(right, table, row)?;
                match op {
                    Token::Not => match value {
                        DataValue::Bool(value) => Ok(DataValue::Bool(!value)),
                        other => Err(SQRLErr::InvalidUnaryOp(format!(
                            "NOT {:?}",
                            other.data_type()
                        ))),
                    },
                    Token::OpSub => match value {
                        DataValue::Int(value) => Ok(DataValue::Int(-value)),
                        DataValue::Real(value) => Ok(DataValue::Real(-value)),
                        other => Err(SQRLErr::InvalidUnaryOp(format!(
                            "- {:?}",
                            other.data_type()
                        ))),
                    },
                    _ => Err(SQRLErr::InvalidUnaryOp(format!("{op:?}"))),
                }
            }
            Expr::Binary { op, left, right } => {
                if *op == Token::In {
                    let left = self.eval_in_row(left, table, row)?;
                    let Expr::List(values) = right.as_ref() else {
                        return Err(SQRLErr::InvalidBinaryOp(
                            "IN requires a parenthesized value list".to_string(),
                        ));
                    };
                    let is_match = values
                        .iter()
                        .map(|expr| self.eval_in_row(expr, table, row))
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .any(|value| value == left);
                    return Ok(DataValue::Bool(is_match));
                }
                let left = self.eval_in_row(left, table, row)?;
                let right = self.eval_in_row(right, table, row)?;
                self.eval_binary(op, left, right)
            }
        }
    }

    fn eval_binary(
        &self,
        op: &Token,
        left: DataValue,
        right: DataValue,
    ) -> Result<DataValue> {
        match op {
            Token::OpEq => Ok(DataValue::Bool(left == right)),
            Token::And => match (left, right) {
                (DataValue::Bool(left), DataValue::Bool(right)) => {
                    Ok(DataValue::Bool(left && right))
                }
                (left, right) => Err(SQRLErr::InvalidBinaryOp(format!(
                    "{:?} AND {:?}",
                    left.data_type(),
                    right.data_type()
                ))),
            },
            Token::Or => match (left, right) {
                (DataValue::Bool(left), DataValue::Bool(right)) => {
                    Ok(DataValue::Bool(left || right))
                }
                (left, right) => Err(SQRLErr::InvalidBinaryOp(format!(
                    "{:?} OR {:?}",
                    left.data_type(),
                    right.data_type()
                ))),
            },
            Token::OpAdd => match (left, right) {
                (DataValue::Int(left), DataValue::Int(right)) => {
                    Ok(DataValue::Int(left + right))
                }
                (DataValue::Int(left), DataValue::Real(right)) => {
                    Ok(DataValue::Real(left as f64 + right))
                }
                (DataValue::Real(left), DataValue::Int(right)) => {
                    Ok(DataValue::Real(left + right as f64))
                }
                (DataValue::Real(left), DataValue::Real(right)) => {
                    Ok(DataValue::Real(left + right))
                }
                (DataValue::Text(left), DataValue::Text(right)) => {
                    Ok(DataValue::Text(format!("{left}{right}").into_boxed_str()))
                }
                (left, right) => Err(SQRLErr::InvalidBinaryOp(format!(
                    "{:?} + {:?}",
                    left.data_type(),
                    right.data_type()
                ))),
            },
            Token::OpConcat => Ok(DataValue::Text(
                format!("{}{}", Self::format_value(&left), Self::format_value(&right))
                    .into_boxed_str(),
            )),
            Token::OpSub => match (left, right) {
                (DataValue::Int(left), DataValue::Int(right)) => {
                    Ok(DataValue::Int(left - right))
                }
                (DataValue::Int(left), DataValue::Real(right)) => {
                    Ok(DataValue::Real(left as f64 - right))
                }
                (DataValue::Real(left), DataValue::Int(right)) => {
                    Ok(DataValue::Real(left - right as f64))
                }
                (DataValue::Real(left), DataValue::Real(right)) => {
                    Ok(DataValue::Real(left - right))
                }
                (left, right) => Err(SQRLErr::InvalidBinaryOp(format!(
                    "{:?} - {:?}",
                    left.data_type(),
                    right.data_type()
                ))),
            },
            Token::OpMul => match (left, right) {
                (DataValue::Int(left), DataValue::Int(right)) => {
                    Ok(DataValue::Int(left * right))
                }
                (DataValue::Int(left), DataValue::Real(right)) => {
                    Ok(DataValue::Real(left as f64 * right))
                }
                (DataValue::Real(left), DataValue::Int(right)) => {
                    Ok(DataValue::Real(left * right as f64))
                }
                (DataValue::Real(left), DataValue::Real(right)) => {
                    Ok(DataValue::Real(left * right))
                }
                (left, right) => Err(SQRLErr::InvalidBinaryOp(format!(
                    "{:?} * {:?}",
                    left.data_type(),
                    right.data_type()
                ))),
            },
            Token::OpDiv => match (left, right) {
                (DataValue::Int(_), DataValue::Int(0)) => {
                    Err(SQRLErr::InvalidBinaryOp("division by zero".to_string()))
                }
                (DataValue::Real(_), DataValue::Real(0.0)) => {
                    Err(SQRLErr::InvalidBinaryOp("division by zero".to_string()))
                }
                (DataValue::Int(_), DataValue::Real(0.0)) => {
                    Err(SQRLErr::InvalidBinaryOp("division by zero".to_string()))
                }
                (DataValue::Real(_), DataValue::Int(0)) => {
                    Err(SQRLErr::InvalidBinaryOp("division by zero".to_string()))
                }
                (DataValue::Int(left), DataValue::Int(right)) => {
                    Ok(DataValue::Int(left / right))
                }
                (DataValue::Int(left), DataValue::Real(right)) => {
                    Ok(DataValue::Real(left as f64 / right))
                }
                (DataValue::Real(left), DataValue::Int(right)) => {
                    Ok(DataValue::Real(left / right as f64))
                }
                (DataValue::Real(left), DataValue::Real(right)) => {
                    Ok(DataValue::Real(left / right))
                }
                (left, right) => Err(SQRLErr::InvalidBinaryOp(format!(
                    "{:?} / {:?}",
                    left.data_type(),
                    right.data_type()
                ))),
            },
            Token::OpGt | Token::OpLt | Token::OpGe | Token::OpLe => {
                let result = match (&left, &right) {
                    (DataValue::Int(left), DataValue::Int(right)) => match op {
                        Token::OpGt => left > right,
                        Token::OpLt => left < right,
                        Token::OpGe => left >= right,
                        Token::OpLe => left <= right,
                        _ => unreachable!(),
                    },
                    (DataValue::Int(left), DataValue::Real(right)) => match op {
                        Token::OpGt => (*left as f64) > *right,
                        Token::OpLt => (*left as f64) < *right,
                        Token::OpGe => (*left as f64) >= *right,
                        Token::OpLe => (*left as f64) <= *right,
                        _ => unreachable!(),
                    },
                    (DataValue::Real(left), DataValue::Int(right)) => match op {
                        Token::OpGt => *left > (*right as f64),
                        Token::OpLt => *left < (*right as f64),
                        Token::OpGe => *left >= (*right as f64),
                        Token::OpLe => *left <= (*right as f64),
                        _ => unreachable!(),
                    },
                    (DataValue::Real(left), DataValue::Real(right)) => match op {
                        Token::OpGt => left > right,
                        Token::OpLt => left < right,
                        Token::OpGe => left >= right,
                        Token::OpLe => left <= right,
                        _ => unreachable!(),
                    },
                    (DataValue::Text(left), DataValue::Text(right)) => match op {
                        Token::OpGt => left > right,
                        Token::OpLt => left < right,
                        Token::OpGe => left >= right,
                        Token::OpLe => left <= right,
                        _ => unreachable!(),
                    },
                    _ => {
                        return Err(SQRLErr::InvalidBinaryOp(format!(
                            "{:?} {op:?} {:?}",
                            left.data_type(),
                            right.data_type()
                        )));
                    }
                };
                Ok(DataValue::Bool(result))
            }
            _ => Err(SQRLErr::InvalidBinaryOp(format!("{op:?}"))),
        }
    }

    fn matches_where(
        &self,
        table: &TableState,
        row: &RowState,
        where_clause: Option<&Expr>,
    ) -> Result<bool> {
        let Some(expr) = where_clause else {
            return Ok(true);
        };
        match self.eval_in_row(expr, Some(table), Some(row))? {
            DataValue::Bool(value) => Ok(value),
            other => Err(SQRLErr::InvalidPredicate(other.data_type())),
        }
    }
}

impl Executor {
    pub fn run(&mut self, stmt: Stmt) -> Result<QueryResult> {
        match stmt {
            Stmt::Create { table_name, defines, if_not_exists } => {
                self.run_create(&table_name, defines, if_not_exists)
            }
            Stmt::AlterAdd { table_name, define } => {
                self.run_alter_add(&table_name, define)
            }
            Stmt::AlterDrop { table_name, column } => {
                self.run_alter_drop(&table_name, &column)
            }
            Stmt::AlterRename { table_name, new_name } => {
                self.run_alter_rename(&table_name, &new_name)
            }
            Stmt::InsertValues { table_name, columns, values } => {
                self.run_insert_values(&table_name, columns, values)
            }
            Stmt::InsertSelect { table_name, columns, select } => {
                self.run_insert_select(&table_name, columns, *select)
            }
            Stmt::Select {
                table_name,
                columns,
                distinct,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
            } => self.run_select(
                &table_name,
                columns,
                distinct,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
            ),
            Stmt::Update { table_name, assigns, where_clause } => {
                self.run_update(&table_name, assigns, where_clause)
            }
            Stmt::Delete { table_name, where_clause } => {
                self.run_delete(&table_name, where_clause)
            }
            Stmt::Drop { table_name, if_exists, cascade } => {
                self.run_drop(&table_name, if_exists, cascade)
            }
            _ => todo!("unimplemented statement: {stmt:?}"),
        }
    }

    fn run_create(
        &mut self,
        table_name: &str,
        defines: Vec<(Box<str>, DataType)>,
        if_not_exists: bool,
    ) -> Result<QueryResult> {
        let table_id = match self.storage.create_table(table_name) {
            Err(_) if if_not_exists => return Ok(QueryResult::Success),
            Err(e) => return Err(e.into()),
            Ok(id) => id,
        };
        for (name, dt) in defines {
            self.storage.create_column(table_id, dt, &name)?;
        }
        Ok(QueryResult::Success)
    }

    fn run_alter_add(
        &mut self,
        table_name: &str,
        define: (Box<str>, DataType),
    ) -> Result<QueryResult> {
        let table_id = self.storage.get_table(table_name)?.id;
        let (name, data_type) = define;
        self.storage.create_column(table_id, data_type, &name)?;
        Ok(QueryResult::Success)
    }

    fn run_alter_drop(
        &mut self,
        table_name: &str,
        column: &str,
    ) -> Result<QueryResult> {
        let table = self.storage.get_table(table_name)?;
        let column_id = table
            .get_col_by_name(column)
            .ok_or_else(|| SQRLErr::ColumnNotFound(column.to_string()))?
            .id;
        self.storage.drop_column(table.id, column_id)?;
        Ok(QueryResult::Success)
    }

    fn run_alter_rename(
        &mut self,
        table_name: &str,
        new_name: &str,
    ) -> Result<QueryResult> {
        let table_id = self.storage.get_table(table_name)?.id;
        self.storage.rename_table(table_id, new_name)?;
        Ok(QueryResult::Success)
    }

    fn run_insert_values(
        &mut self,
        table_name: &str,
        columns: Vec<Box<str>>,
        values: Vec<Expr>,
    ) -> Result<QueryResult> {
        let (table_id, live_cols, source_indexes, expected) =
            self.resolve_insert_targets(table_name, &columns)?;

        let evaluated = values
            .into_iter()
            .map(|expr| self.eval(&expr))
            .collect::<Result<Vec<_>>>()?;

        if evaluated.len() != expected {
            return Err(SQRLErr::ColumnCountMismatch {
                expected,
                got: evaluated.len(),
            });
        }

        let row = self.build_insert_row(&live_cols, &source_indexes, &evaluated)?;

        self.storage.insert_row(table_id, row)?;
        Ok(QueryResult::Count(1))
    }

    fn run_insert_select(
        &mut self,
        table_name: &str,
        columns: Vec<Box<str>>,
        select: Stmt,
    ) -> Result<QueryResult> {
        let (table_id, live_cols, source_indexes, expected) =
            self.resolve_insert_targets(table_name, &columns)?;
        let Stmt::Select {
            table_name,
            columns,
            distinct,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
        } = select
        else {
            return Err(SQRLErr::UnsupportedFeature(
                "INSERT source must be SELECT".to_string(),
            ));
        };

        let (_, source_rows) = self.collect_select_rows(
            &table_name,
            columns,
            distinct,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
        )?;

        let count = source_rows.len();
        for source_row in source_rows {
            if source_row.len() != expected {
                return Err(SQRLErr::ColumnCountMismatch {
                    expected,
                    got: source_row.len(),
                });
            }
            let row =
                self.build_insert_row(&live_cols, &source_indexes, &source_row)?;
            self.storage.insert_row(table_id, row)?;
        }

        Ok(QueryResult::Count(count))
    }

    fn resolve_insert_targets(
        &self,
        table_name: &str,
        columns: &[Box<str>],
    ) -> Result<(
        crate::storage::TableId,
        Vec<(Box<str>, DataType)>,
        Vec<Option<usize>>,
        usize,
    )> {
        let table = self.storage.get_table(table_name)?;
        let live_cols = table
            .live_cols()
            .map(|col| (col.name.clone(), col.data_type))
            .collect::<Vec<_>>();
        let expected = if columns.is_empty() { live_cols.len() } else { columns.len() };
        let source_indexes = if columns.is_empty() {
            (0..live_cols.len()).map(Some).collect::<Vec<_>>()
        } else {
            let mut source_indexes = vec![None; live_cols.len()];
            for (value_index, column) in columns.iter().enumerate() {
                let Some(col_index) = live_cols
                    .iter()
                    .position(|(name, _)| name.as_ref() == column.as_ref())
                else {
                    return Err(SQRLErr::ColumnNotFound(column.to_string()));
                };
                if source_indexes[col_index].is_some() {
                    return Err(SQRLErr::DuplicateColumn(column.to_string()));
                }
                source_indexes[col_index] = Some(value_index);
            }
            source_indexes
        };
        Ok((table.id, live_cols, source_indexes, expected))
    }

    fn build_insert_row(
        &self,
        live_cols: &[(Box<str>, DataType)],
        source_indexes: &[Option<usize>],
        source_values: &[DataValue],
    ) -> Result<Vec<DataValue>> {
        source_indexes
            .iter()
            .enumerate()
            .map(|(col_index, source_index)| {
                let (col_name, col_type) = &live_cols[col_index];
                let value = match source_index {
                    Some(value_index) => source_values[*value_index].clone(),
                    None => col_type.default(),
                };
                let value_type = value.data_type();
                if value_type != *col_type {
                    return Err(SQRLErr::TypeMismatch {
                        column: col_name.to_string(),
                        expected: *col_type,
                        got: value_type,
                    });
                }
                Ok(value)
            })
            .collect::<Result<Vec<_>>>()
    }

    fn run_select(
        &mut self,
        table_name: &str,
        columns: Vec<Expr>,
        distinct: bool,
        where_clause: Option<Expr>,
        group_by: Option<Vec<Expr>>,
        having: Option<Expr>,
        order_by: Option<Vec<(Expr, bool)>>,
        limit: Option<u64>,
    ) -> Result<QueryResult> {
        let (result_columns, rows) = self.collect_select_rows(
            table_name,
            columns,
            distinct,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
        )?;
        let rows = rows
            .into_iter()
            .map(|row| row.iter().map(Self::format_value).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        Ok(QueryResult::Rows { columns: result_columns, rows })
    }

    fn collect_select_rows(
        &self,
        table_name: &str,
        columns: Vec<Expr>,
        distinct: bool,
        where_clause: Option<Expr>,
        group_by: Option<Vec<Expr>>,
        having: Option<Expr>,
        order_by: Option<Vec<(Expr, bool)>>,
        limit: Option<u64>,
    ) -> Result<(Vec<String>, Vec<Vec<DataValue>>)> {
        if group_by.is_some() {
            return Err(SQRLErr::UnsupportedFeature("GROUP BY".to_string()));
        }
        if having.is_some() {
            return Err(SQRLErr::UnsupportedFeature("HAVING".to_string()));
        }
        if order_by.is_some() {
            return Err(SQRLErr::UnsupportedFeature("ORDER BY".to_string()));
        }

        let table = self.storage.get_table(table_name)?;
        let live_cols: Vec<_> = table.live_cols().collect();
        let projections = if columns.is_empty() {
            live_cols
                .iter()
                .map(|col| Expr::Ident(col.name.clone()))
                .collect::<Vec<_>>()
        } else {
            columns
        };
        let result_columns =
            projections.iter().map(Self::expr_label).collect::<Vec<_>>();

        let mut live_rows =
            table.rows.values().filter(|row| row.alive).collect::<Vec<_>>();
        live_rows.sort_by_key(|row| row.id.0);

        let mut filtered_rows = Vec::new();
        for row in live_rows {
            if self.matches_where(table, row, where_clause.as_ref())? {
                filtered_rows.push(row);
            }
        }

        let has_aggregate = projections.iter().any(Self::is_aggregate);
        if has_aggregate {
            if projections.iter().any(|expr| !Self::is_aggregate(expr)) {
                return Err(SQRLErr::UnsupportedFeature(
                    "mixing aggregate and non-aggregate projections".to_string(),
                ));
            }
            let values = projections
                .iter()
                .map(|expr| self.eval_aggregate(expr, table, &filtered_rows))
                .collect::<Result<Vec<_>>>()?;
            return Ok((result_columns, vec![values]));
        }

        let mut result_rows = Vec::new();
        for row in filtered_rows {
            let values = projections
                .iter()
                .map(|expr| self.eval_in_row(expr, Some(table), Some(row)))
                .collect::<Result<Vec<_>>>()?;

            if distinct && result_rows.contains(&values) {
                continue;
            }

            result_rows.push(values);
            if let Some(limit) = limit {
                if result_rows.len() as u64 >= limit {
                    break;
                }
            }
        }

        Ok((result_columns, result_rows))
    }

    fn run_update(
        &mut self,
        table_name: &str,
        assigns: Vec<(Box<str>, Expr)>,
        where_clause: Option<Expr>,
    ) -> Result<QueryResult> {
        let plans = {
            let table = self.storage.get_table(table_name)?;
            let mut seen = HashSet::new();
            let targets = assigns
                .iter()
                .map(|(name, _)| {
                    if !seen.insert(name.clone()) {
                        return Err(SQRLErr::DuplicateColumn(name.to_string()));
                    }
                    let col = table
                        .get_col_by_name(name)
                        .ok_or_else(|| SQRLErr::ColumnNotFound(name.to_string()))?;
                    Ok((col.id, col.name.clone(), col.data_type))
                })
                .collect::<Result<Vec<(ColId, Box<str>, DataType)>>>()?;

            let mut rows =
                table.rows.values().filter(|row| row.alive).collect::<Vec<_>>();
            rows.sort_by_key(|row| row.id.0);

            let mut plans = Vec::new();
            for row in rows {
                if !self.matches_where(table, row, where_clause.as_ref())? {
                    continue;
                }
                let mut patches = Vec::with_capacity(assigns.len());
                for ((_, expr), (col_id, col_name, col_type)) in
                    assigns.iter().zip(targets.iter())
                {
                    let value = self.eval_in_row(expr, Some(table), Some(row))?;
                    if value.data_type() != *col_type {
                        return Err(SQRLErr::TypeMismatch {
                            column: col_name.to_string(),
                            expected: *col_type,
                            got: value.data_type(),
                        });
                    }
                    patches.push((*col_id, value));
                }
                plans.push((row.id, patches));
            }
            plans
        };

        let count = plans.len();
        for (row_id, patches) in plans {
            self.storage.update_row(
                self.storage.get_table(table_name)?.id,
                row_id,
                patches,
            )?;
        }
        Ok(QueryResult::Count(count))
    }

    fn run_delete(
        &mut self,
        table_name: &str,
        where_clause: Option<Expr>,
    ) -> Result<QueryResult> {
        let (table_id, row_ids) = {
            let table = self.storage.get_table(table_name)?;
            let mut rows =
                table.rows.values().filter(|row| row.alive).collect::<Vec<_>>();
            rows.sort_by_key(|row| row.id.0);
            let row_ids = rows
                .into_iter()
                .filter_map(|row| {
                    match self.matches_where(table, row, where_clause.as_ref()) {
                        Ok(true) => Some(Ok(row.id)),
                        Ok(false) => None,
                        Err(err) => Some(Err(err)),
                    }
                })
                .collect::<Result<Vec<RowId>>>()?;
            (table.id, row_ids)
        };

        let count = row_ids.len();
        for row_id in row_ids {
            self.storage.delete_row(table_id, row_id)?;
        }
        Ok(QueryResult::Count(count))
    }

    fn run_drop(
        &mut self,
        table_name: &str,
        if_exists: bool,
        _cascade: bool,
    ) -> Result<QueryResult> {
        let table_id = match self.storage.get_table(table_name) {
            Err(_) if if_exists => return Ok(QueryResult::Success),
            Err(e) => return Err(e.into()),
            Ok(table) => table.id,
        };
        self.storage.drop_table(table_id)?;
        Ok(QueryResult::Success)
    }
}
