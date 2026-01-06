use std::cmp::PartialEq;
use std::fmt::format;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::SeekFrom;
use tokio::{fs, io};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use crate::query::{ColumnId, RowId, TableId};

#[repr(u8)]
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy)]
pub enum DataType {

    Int = 11,
    Float = 12,
    Bool = 13,
    String = 14,
}

#[derive(PartialEq, Clone, Debug)]
pub enum DataValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
}

impl DataType {
    pub fn as_str(self) -> &'static str {
        match(self) {
            DataType::Int => "Int",
            DataType::Float => "Float",
            DataType::Bool => "Bool",
            DataType::String => "String"
        }
    }
}


impl DataValue {
    pub fn verify(self, data_type: DataType) -> bool {
        match self {
            DataValue::Int(_) => DataType::Int == data_type,
            DataValue::Float(_) => DataType::Float == data_type,
            DataValue::Bool(_) => DataType::Bool == data_type,
            DataValue::String(_) => DataType::String == data_type,
        }
    }
}

async fn create_table(name: String) -> tokio::io::Result<TableId> {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    let val = hasher.finish();
    fs::create_dir(val.to_string()).await?;
    let mut file = fs::File::create(format!("{}/schema", val)).await?;
    file.write_all(format!("NAME {}\n", name).as_bytes()).await?;
    file.write_all("LAST_ID 00000000\n".as_bytes()).await?;
    file.flush().await?;
    Ok(TableId(val))
}

async fn create_column(table_id: TableId, col_name: String, col_type: DataType) -> tokio::io::Result<ColumnId> {
    let mut hasher = DefaultHasher::new();
    col_name.hash(&mut hasher);
    let val = hasher.finish();
    let mut file = fs::File::options().append(true).open(format!("{}/schema", table_id.0)).await?;
    file.write_all(format!("COLUMN {} {} {col_name}\n", val, col_type.as_str()).as_bytes()).await?;
    file.flush().await?;
    Ok(ColumnId(val))
}

async fn create_row(table_id: TableId, values: Vec<DataValue>) -> tokio::io::Result<RowId> {
    let mut file = fs::File::options().write(true).open(format!("{}/schema", table_id.0)).await?;
    let mut matching_count = 0;
    let pattern = "\nLAST_ID ";
    let mut position = 0;
    let mut buffered = BufReader::new(file);
    loop {
        let value = buffered.read_u8().await?;
        position += 1;
        if pattern.as_bytes()[matching_count] != value {
            matching_count = 0;
            continue;
        }
        matching_count += 1;
        if matching_count == pattern.len() {
            break;
        }
    };
    if matching_count != pattern.len() {
        return Err(io::Error::new(io::ErrorKind::Other, "Schema file is corrupted"));
    }

    let mut hexadecimal = [0u8; 8];
    buffered.read_exact(&mut hexadecimal).await?;
    let hexadecimal = String::from_utf8_lossy(&hexadecimal);
    println!("string: {}", hexadecimal);
    let parsed = u32::from_str_radix(&hexadecimal, 16).unwrap();
    buffered.seek(SeekFrom::Start(position)).await?;
    buffered.write("00000001".as_bytes()).await?;

    todo!()
}