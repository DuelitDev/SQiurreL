use super::codec::{Decoder, Encoder};
use super::error::Result;
use super::{ColId, RowId, TableId};
use crate::schema::{DataType, DataValue};

trait Recordable: Sized {
    fn encode(&self, enc: &mut Encoder);
    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Self>;
}

pub struct TableCreate {
    pub table_id: TableId,
    pub table_name: Box<str>,
}

impl Recordable for TableCreate {
    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.text(&self.table_name);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Self> {
        Ok(Self { table_id: TableId(dec.u64()?), table_name: dec.text()? })
    }
}

pub struct TableDrop {
    pub table_id: TableId,
}

impl Recordable for TableDrop {
    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Self> {
        Ok(Self { table_id: TableId(dec.u64()?) })
    }
}

pub struct ColumnCreate {
    pub table_id: TableId,
    pub col_id: ColId,
    pub col_type: DataType,
    pub col_name: Box<str>,
}

impl Recordable for ColumnCreate {
    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.col_id.0);
        enc.ty(self.col_type);
        enc.text(&self.col_name);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Self> {
        Ok(Self {
            table_id: TableId(dec.u64()?),
            col_id: ColId(dec.u64()?),
            col_type: dec.ty()?,
            col_name: dec.text()?,
        })
    }
}

pub struct ColumnAlter {
    pub table_id: TableId,
    pub col_id: ColId,
    pub new_col_type: DataType,
    pub new_col_name: Box<str>,
}

impl Recordable for ColumnAlter {
    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.col_id.0);
        enc.ty(self.new_col_type);
        enc.text(&self.new_col_name);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Self> {
        Ok(Self {
            table_id: TableId(dec.u64()?),
            col_id: ColId(dec.u64()?),
            new_col_type: dec.ty()?,
            new_col_name: dec.text()?,
        })
    }
}

pub struct ColumnDrop {
    pub table_id: TableId,
    pub col_id: ColId,
}

impl Recordable for ColumnDrop {
    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.col_id.0);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Self> {
        Ok(Self { table_id: TableId(dec.u64()?), col_id: ColId(dec.u64()?) })
    }
}

pub struct RowInsert {
    pub table_id: TableId,
    pub row_id: RowId,
    pub count: u64,
    pub values: Vec<DataValue>,
}

impl Recordable for RowInsert {
    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.row_id.0);
        enc.u64(self.count);
        for value in &self.values {
            enc.value(value);
        }
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Self> {
        let table_id = TableId(dec.u64()?);
        let row_id = RowId(dec.u64()?);
        let count = dec.u64()?;
        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let ty = dec.ty()?;
            let data = dec.value(ty)?;
            values.push(data);
        }
        Ok(Self { table_id, row_id, count, values })
    }
}

pub struct RowUpdate {
    pub table_id: TableId,
    pub row_id: RowId,
    pub count: u64,
    pub patches: Vec<(ColId, DataValue)>,
}

impl Recordable for RowUpdate {
    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.row_id.0);
        enc.u64(self.count);
        for (col_id, value) in &self.patches {
            enc.u64(col_id.0);
            enc.value(value);
        }
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Self> {
        let table_id = TableId(dec.u64()?);
        let row_id = RowId(dec.u64()?);
        let count = dec.u64()?;
        let mut patches = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let col_id = ColId(dec.u64()?);
            let ty = dec.ty()?;
            let data = dec.value(ty)?;
            patches.push((col_id, data));
        }
        Ok(Self { table_id, row_id, count, patches })
    }
}

pub struct RowDelete {
    pub table_id: TableId,
    pub row_id: RowId,
}

impl Recordable for RowDelete {
    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.row_id.0);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Self> {
        Ok(Self { table_id: TableId(dec.u64()?), row_id: RowId(dec.u64()?) })
    }
}
