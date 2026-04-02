use crate::query::lexer::Token;
use crate::query::{Expr, QueryErr, Stmt};
use crate::schema::{DataType, DataValue};
use crate::storage::{ColId, RowId, RowState, Storage, StorageErr, TableState};
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
}

pub type Result<T> = std::result::Result<T, SQRLErr>;

pub struct Executor {
    storage: Storage,
}

impl Executor {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl Executor {
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
            _ => format!("{expr:?}"),
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
        let (table_id, live_cols) = {
            let table = self.storage.get_table(table_name)?;
            let live_cols = table
                .live_cols()
                .map(|col| (col.name.clone(), col.data_type))
                .collect::<Vec<_>>();
            (table.id, live_cols)
        };

        let expected = if columns.is_empty() { live_cols.len() } else { columns.len() };
        if values.len() != expected {
            return Err(SQRLErr::ColumnCountMismatch { expected, got: values.len() });
        }

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

        let evaluated = values
            .into_iter()
            .map(|expr| self.eval(&expr))
            .collect::<Result<Vec<_>>>()?;

        let row = source_indexes
            .into_iter()
            .enumerate()
            .map(|(col_index, source_index)| {
                let (col_name, col_type) = &live_cols[col_index];
                let value = match source_index {
                    Some(value_index) => evaluated[value_index].clone(),
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
            .collect::<Result<Vec<_>>>()?;

        self.storage.insert_row(table_id, row)?;
        Ok(QueryResult::Count(1))
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

        let mut result_rows = Vec::new();
        let mut seen = HashSet::new();
        for row in live_rows {
            if !self.matches_where(table, row, where_clause.as_ref())? {
                continue;
            }

            let values = projections
                .iter()
                .map(|expr| self.eval_in_row(expr, Some(table), Some(row)))
                .collect::<Result<Vec<_>>>()?;
            let rendered = values.iter().map(Self::format_value).collect::<Vec<_>>();

            if distinct && !seen.insert(rendered.clone()) {
                continue;
            }

            result_rows.push(rendered);
            if let Some(limit) = limit {
                if result_rows.len() as u64 >= limit {
                    break;
                }
            }
        }

        Ok(QueryResult::Rows { columns: result_columns, rows: result_rows })
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
