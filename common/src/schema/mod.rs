#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Nil = 1,
    Int = 2,
    Real = 3,
    Bool = 4,
    Text = 5,
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
