use super::record::*;
use super::{ColId, RowId, SeqNo, TableId};
use crate::schema::{DataType, DataValue};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RowState {
    pub id: RowId,
    pub alive: bool,
    pub values: HashMap<ColId, DataValue>,
}

#[derive(Debug, Clone)]
pub struct ColState {
    pub id: ColId,
    pub name: Box<str>,
    pub alive: bool,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub struct TableState {
    pub id: TableId,
    pub name: Box<str>,
    pub alive: bool,
    pub cols: Vec<ColState>,
    pub rows: HashMap<RowId, RowState>,
}

impl TableState {
    pub fn get_col(&self, id: &ColId) -> Option<&ColState> {
        self.cols.iter().find(|c| &c.id == id)
    }

    pub fn get_col_mut(&mut self, id: &ColId) -> Option<&mut ColState> {
        self.cols.iter_mut().find(|c| &c.id == id)
    }

    pub fn get_col_by_name(&self, name: &str) -> Option<&ColState> {
        self.cols.iter().find(|c| c.alive && &*c.name == name)
    }

    pub fn get_col_by_name_mut(&mut self, name: &str) -> Option<&mut ColState> {
        self.cols.iter_mut().find(|c| c.alive && &*c.name == name)
    }

    pub fn live_cols(&self) -> impl Iterator<Item = &ColState> {
        self.cols.iter().filter(|c| c.alive)
    }
}

#[derive(Debug)]
pub struct DbState {
    pub tables: HashMap<TableId, TableState>,
    next_table_id: TableId,
    next_col_id: ColId,
    next_row_id: RowId,
    next_seq_no: SeqNo,
}

impl Default for DbState {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
            next_table_id: 1.into(),
            next_col_id: 1.into(),
            next_row_id: 1.into(),
            next_seq_no: 1.into(),
        }
    }
}

impl DbState {
    pub fn get_table(&self, id: &TableId) -> Option<&TableState> {
        self.tables.get(id)
    }

    pub fn get_table_mut(&mut self, id: &TableId) -> Option<&mut TableState> {
        self.tables.get_mut(id)
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<&TableState> {
        self.tables.values().find(|t| t.alive && &*t.name == name)
    }

    pub fn get_table_by_name_mut(&mut self, name: &str) -> Option<&mut TableState> {
        self.tables.values_mut().find(|t| t.alive && &*t.name == name)
    }

    pub(super) fn alloc_table(&mut self) -> TableId {
        let id = self.next_table_id;
        self.next_table_id.0 += 1;
        id
    }

    pub(super) fn alloc_col(&mut self) -> ColId {
        let id = self.next_col_id;
        self.next_col_id.0 += 1;
        id
    }

    pub(super) fn alloc_row(&mut self) -> RowId {
        let id = self.next_row_id;
        self.next_row_id.0 += 1;
        id
    }

    pub(super) fn next_seq_no(&mut self) -> SeqNo {
        let id = self.next_seq_no;
        self.next_seq_no.0 += 1;
        id
    }

    pub fn commit(&mut self, record: Record) {
        match record {
            Record::TableCreate(rec) => self.commit_table_create(rec),
            Record::TableTruncate(rec) => self.commit_table_truncate(rec),
            Record::TableRename(rec) => self.commit_table_rename(rec),
            Record::TableDrop(rec) => self.commit_table_drop(rec),
            Record::ColumnCreate(rec) => self.commit_column_create(rec),
            Record::ColumnAlter(rec) => self.commit_column_alter(rec),
            Record::ColumnDrop(rec) => self.commit_column_drop(rec),
            Record::RowInsert(rec) => self.commit_row_insert(rec),
            Record::RowUpdate(rec) => self.commit_row_update(rec),
            Record::RowDelete(rec) => self.commit_row_delete(rec),
        }
    }

    pub fn commit_table_create(&mut self, rec: TableCreate) {
        self.next_table_id = self.next_table_id.max(TableId(rec.table_id.0 + 1));
        self.tables.insert(
            rec.table_id,
            TableState {
                id: rec.table_id,
                name: rec.table_name,
                alive: true,
                cols: Vec::new(),
                rows: HashMap::new(),
            },
        );
    }

    pub fn commit_table_truncate(&mut self, rec: TableTruncate) {
        let table = self
            .get_table_mut(&rec.table_id)
            .expect("corrupted: table not found during commit");
        table.rows.clear();
    }

    pub fn commit_table_rename(&mut self, rec: TableRename) {
        let table = self
            .get_table_mut(&rec.table_id)
            .expect("corrupted: table not found during commit");
        table.name = rec.new_table_name;
    }

    pub fn commit_table_drop(&mut self, rec: TableDrop) {
        let table = self
            .get_table_mut(&rec.table_id)
            .expect("corrupted: table not found during commit");
        table.alive = false;
    }

    pub fn commit_column_create(&mut self, rec: ColumnCreate) {
        self.next_col_id = self.next_col_id.max(ColId(rec.col_id.0 + 1));
        let table = self
            .get_table_mut(&rec.table_id)
            .expect("corrupted: table not found during commit");
        table.cols.push(ColState {
            id: rec.col_id,
            name: rec.col_name,
            alive: true,
            data_type: rec.col_type,
        });
    }

    pub fn commit_column_alter(&mut self, rec: ColumnAlter) {
        let table = self
            .get_table_mut(&rec.table_id)
            .expect("corrupted: table not found during commit");
        let col = table
            .get_col_mut(&rec.col_id)
            .expect("corrupted: column not found during commit");
        col.name = rec.new_col_name;
        col.data_type = rec.new_col_type;
    }

    pub fn commit_column_drop(&mut self, rec: ColumnDrop) {
        let table = self
            .get_table_mut(&rec.table_id)
            .expect("corrupted: table not found during commit");
        let col = table
            .get_col_mut(&rec.col_id)
            .expect("corrupted: column not found during commit");
        col.alive = false;
    }

    pub fn commit_row_insert(&mut self, rec: RowInsert) {
        self.next_row_id = self.next_row_id.max(RowId(rec.row_id.0 + 1));
        let table = self
            .get_table_mut(&rec.table_id)
            .expect("corrupted: table not found during commit");
        let live_cols: Vec<_> = table.live_cols().map(|c| c.id).collect();
        let values = live_cols.into_iter().zip(rec.values).collect();
        let row = RowState { id: rec.row_id, values, alive: true };
        table.rows.insert(rec.row_id, row);
    }

    pub fn commit_row_update(&mut self, rec: RowUpdate) {
        let table = self
            .get_table_mut(&rec.table_id)
            .expect("corrupted: table not found during commit");
        let row = table
            .rows
            .get_mut(&rec.row_id)
            .expect("corrupted: row not found during commit");
        for (col_id, value) in rec.patches {
            row.values.insert(col_id, value);
        }
    }

    pub fn commit_row_delete(&mut self, rec: RowDelete) {
        let table = self
            .get_table_mut(&rec.table_id)
            .expect("corrupted: table not found during commit");
        let row = table
            .rows
            .get_mut(&rec.row_id)
            .expect("corrupted: row not found during commit");
        row.alive = false;
    }
}
