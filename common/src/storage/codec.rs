use super::error::{Result, StorageErr};
use crate::schema::{DataType, DataValue};
use std::io::Read;

pub fn read_u8(r: &mut impl Read) -> Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn read_u16(r: &mut impl Read) -> Result<u16> {
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

pub fn read_u32(r: &mut impl Read) -> Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

pub fn read_u64(r: &mut impl Read) -> Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

pub fn read_i64(r: &mut impl Read) -> Result<i64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(i64::from_le_bytes(buf))
}

pub fn read_f64(r: &mut impl Read) -> Result<f64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(f64::from_le_bytes(buf))
}

pub fn read_str(r: &mut impl Read) -> Result<Box<str>> {
    let len = read_u32(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    String::from_utf8(buf)
        .map(|s| s.into_boxed_str())
        .map_err(|e| StorageErr::Corrupted(format!("invalid UTF-8: {e}")))
}

pub fn read_type(r: &mut impl Read) -> Result<DataType> {
    let ty_id = read_u8(r)?;
    match ty_id {
        0 => Ok(DataType::Nil),
        1 => Ok(DataType::Int),
        2 => Ok(DataType::Real),
        3 => Ok(DataType::Bool),
        4 => Ok(DataType::Text),
        _ => Err(StorageErr::Corrupted(format!("invalid type id: {ty_id}"))),
    }
}

pub fn read_value(r: &mut impl Read, ty: DataType) -> Result<DataValue> {
    match ty {
        DataType::Nil => read_u8(r).map(|_| DataValue::Nil),
        DataType::Int => read_i64(r).map(DataValue::Int),
        DataType::Real => read_f64(r).map(DataValue::Real),
        DataType::Bool => read_u8(r).map(|b| DataValue::Bool(b != 0)),
        DataType::Text => read_str(r).map(DataValue::Text),
    }
}

pub fn encode_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

pub fn encode_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn encode_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn encode_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn encode_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn encode_f64(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn encode_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

pub fn encode_type(buf: &mut Vec<u8>, ty: DataType) {
    let ty_id = match ty {
        DataType::Nil => 0,
        DataType::Int => 1,
        DataType::Real => 2,
        DataType::Bool => 3,
        DataType::Text => 4,
    };
    encode_u8(buf, ty_id);
}

pub fn encode_value(buf: &mut Vec<u8>, val: &DataValue) {
    match val {
        DataValue::Nil => encode_u8(buf, 0),
        DataValue::Int(i) => encode_i64(buf, *i),
        DataValue::Real(r) => encode_f64(buf, *r),
        DataValue::Bool(b) => encode_u8(buf, if *b { 1 } else { 0 }),
        DataValue::Text(s) => encode_str(buf, s),
    }
}
