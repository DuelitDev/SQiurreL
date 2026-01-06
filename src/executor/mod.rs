mod planner;

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
