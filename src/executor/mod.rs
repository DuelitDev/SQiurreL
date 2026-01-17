use crate::query::parser::Stmt;

pub struct TxId(pub u64);
pub struct TableId(pub u64);
pub struct ColumnId(pub u64);
pub struct RowId(pub u64);

pub enum Emit {
    Rows,
    Count,
    Success,
}

pub struct Executor;

impl Executor {
    pub fn execute_simple(&self, stmt: Stmt) {
        if let Stmt::Create { table, columns, .. } = stmt {
            println!("Creating table: {}", table);
        } else if let Stmt::InsertValues { table, values, .. } = stmt {
            println!("Inserting data into: {}", table);
        } else {
            println!("Unsupported statement");
        }
    }
}
