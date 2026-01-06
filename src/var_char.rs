use std::fmt::Display;

pub const VAR_CHAR_CAPACITY: usize = 32;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VarChar{
    length: u8,
    data: [char; VAR_CHAR_CAPACITY]
}

pub struct StringTooLong;

impl VarChar {
    pub fn as_slice(&self) -> &[char] {
        &self.data[0..self.length as usize]
    }
}

impl Display for VarChar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from_iter(self.data.iter().take(self.length as usize)))
    }
}

impl TryFrom<String> for VarChar {
    type Error = StringTooLong;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() > VAR_CHAR_CAPACITY {
            return Err(StringTooLong);
        }
        let length = value.len() as u8;

        let mut data = [char::default(); VAR_CHAR_CAPACITY];
        for (data, dest) in value.chars().zip(data.iter_mut()) {
            *dest = data;
        }

        Ok(Self {
            length,
            data,
        })
    }
}