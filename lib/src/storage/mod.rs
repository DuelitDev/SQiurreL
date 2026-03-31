mod codec;
mod header;
mod record;
mod state;

pub mod error;

use crate::schema::{DataType, DataValue};
use error::Result;
pub use error::StorageErr;
use header::FileHeader;
use record::*;
pub use state::{ColState, DbState, RowState, TableState};
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u64);

impl From<u64> for TableId {
    fn from(value: u64) -> Self {
        TableId(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColId(pub u64);

impl From<u64> for ColId {
    fn from(value: u64) -> Self {
        ColId(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(pub u64);

impl From<u64> for RowId {
    fn from(value: u64) -> Self {
        RowId(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeqNo(pub u32);

impl From<u32> for SeqNo {
    fn from(value: u32) -> Self {
        SeqNo(value)
    }
}

#[derive(Debug)]
pub struct Storage {
    pub path: PathBuf,
    pub state: DbState,
    header: FileHeader,
    file: File,
}

impl Storage {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        match File::options().read(true).write(true).open(&path) {
            Ok(mut file) => {
                let header = FileHeader::read_from(&mut file)?;
                let mut storage =
                    Self { path, file, header, state: DbState::default() };
                storage.replay()?;
                Ok(storage)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut file = File::options()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                let header = FileHeader::new();
                header.write_to(&mut file)?;
                Ok(Self { path, file, header, state: DbState::default() })
            }
            Err(e) => Err(e.into()),
        }
    }
}

impl Storage {
    fn replay(&mut self) -> Result<()> {
        loop {
            match read_rec(&mut self.file) {
                Ok(record) => {
                    self.state.next_seq_no();
                    self.state.commit(record);
                }
                Err(StorageErr::Io(e))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

impl Storage {
    pub fn create_table(&mut self, name: &str) -> Result<TableId> {
        // validate
        if self.state.get_table_by_name(name).is_some() {
            return Err(StorageErr::TableAlreadyExists {
                id: self.state.get_table_by_name(name).unwrap().id,
                name: name.into(),
            });
        }

        // build record
        let table_id = self.state.alloc_table();
        let seq = self.state.next_seq_no();
        let rec = TableCreate { table_id, table_name: name.into() };

        // write then commit
        write_rec(&mut self.file, &rec, seq)?;
        self.state.commit_table_create(rec);
        Ok(table_id)
    }

    pub fn drop_table(&mut self, table_id: TableId) -> Result<()> {
        // validate
        let table = self
            .state
            .get_table(&table_id)
            .ok_or(StorageErr::TableNotFound(table_id))?;
        if !table.alive {
            return Err(StorageErr::TableNotFound(table_id));
        }

        // build record
        let seq = self.state.next_seq_no();
        let rec = TableDrop { table_id };

        // write then commit
        write_rec(&mut self.file, &rec, seq)?;
        self.state.commit_table_drop(rec);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<&TableState> {
        self.state
            .get_table_by_name(name)
            .ok_or_else(|| StorageErr::CannotResolveTable(name.into()))
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.state.get_table_by_name(name).is_some()
    }

    pub fn create_column(
        &mut self,
        table_id: TableId,
        col_type: DataType,
        name: &str,
    ) -> Result<ColId> {
        // validate
        let table = self
            .state
            .get_table(&table_id)
            .ok_or(StorageErr::TableNotFound(table_id))?;
        for col in &table.cols {
            if col.alive && &*col.name == name {
                return Err(StorageErr::ColumnAlreadyExists {
                    id: col.id,
                    name: name.into(),
                });
            }
        }

        // build record
        let col_id = self.state.alloc_col();
        let seq = self.state.next_seq_no();
        let rec = ColumnCreate { table_id, col_id, col_type, col_name: name.into() };

        // write then commit
        write_rec(&mut self.file, &rec, seq)?;
        self.state.commit_column_create(rec);
        Ok(col_id)
    }

    pub fn insert_row(
        &mut self,
        table_id: TableId,
        values: Vec<DataValue>,
    ) -> Result<RowId> {
        // validate
        let table = self
            .state
            .get_table(&table_id)
            .ok_or(StorageErr::TableNotFound(table_id))?;
        let live_col_count = table.live_cols().count();
        if values.len() != live_col_count {
            return Err(StorageErr::InvalidRow("column count mismatch"));
        }

        // build record
        let count = values.len() as u64;
        let row_id = self.state.alloc_row();
        let seq = self.state.next_seq_no();
        let rec = RowInsert { table_id, row_id, count, values };

        // write then commit
        write_rec(&mut self.file, &rec, seq)?;
        self.state.commit_row_insert(rec);
        Ok(row_id)
    }
}
