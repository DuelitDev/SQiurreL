#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Nil,
    Int,
    Real,
    Bool,
    Text,
}

impl DataType {
    pub fn default(&self) -> DataValue {
        match self {
            DataType::Nil => DataValue::Nil,
            DataType::Int => DataValue::Int(0),
            DataType::Real => DataValue::Real(0.0),
            DataType::Bool => DataValue::Bool(false),
            DataType::Text => DataValue::Text(Box::from("")),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataValue {
    Nil,
    Int(i64),
    Real(f64),
    Bool(bool),
    Text(Box<str>),
}
