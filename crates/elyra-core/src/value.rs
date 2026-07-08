//! Runtime value model shared across all engines.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Vector(Vec<f32>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Render as a MySQL text-protocol column value.
    pub fn to_wire_string(&self) -> Option<String> {
        match self {
            Value::Null => None,
            Value::Bool(b) => Some(if *b { "1".into() } else { "0".into() }),
            Value::Int(i) => Some(i.to_string()),
            Value::Float(f) => Some(f.to_string()),
            Value::Text(s) => Some(s.clone()),
            Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
            Value::Vector(v) => {
                let inner = v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",");
                Some(format!("[{inner}]"))
            }
        }
    }
}
