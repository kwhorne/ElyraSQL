// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::io;

use crate::myc;
use crate::{StatementData, Value};

/// A `ParamParser` decodes query parameters included in a client's `EXECUTE` command given
/// type information for the expected parameters.
///
/// Users should invoke [`iter`](struct.ParamParser.html#method.iter) method to iterate over the
/// provided parameters.
pub struct ParamParser<'a> {
    pub(crate) params: u16,
    pub(crate) bytes: &'a [u8],
    pub(crate) long_data: &'a HashMap<u16, Vec<u8>>,
    pub(crate) bound_types: &'a mut Vec<(myc::constants::ColumnType, bool)>,
}

impl<'a> ParamParser<'a> {
    pub(crate) fn new(input: &'a [u8], stmt: &'a mut StatementData) -> Self {
        ParamParser {
            params: stmt.params,
            bytes: input,
            long_data: &stmt.long_data,
            bound_types: &mut stmt.bound_types,
        }
    }
}

impl<'a> IntoIterator for ParamParser<'a> {
    type IntoIter = Params<'a>;
    type Item = io::Result<ParamValue<'a>>;
    fn into_iter(self) -> Params<'a> {
        Params {
            params: self.params,
            input: self.bytes,
            nullmap: None,
            col: 0,
            long_data: self.long_data,
            bound_types: self.bound_types,
        }
    }
}

/// An iterator over parameters provided by a client in an `EXECUTE` command.
pub struct Params<'a> {
    params: u16,
    input: &'a [u8],
    nullmap: Option<&'a [u8]>,
    col: u16,
    long_data: &'a HashMap<u16, Vec<u8>>,
    bound_types: &'a mut Vec<(myc::constants::ColumnType, bool)>,
}

/// A single parameter value provided by a client when issuing an `EXECUTE` command.
pub struct ParamValue<'a> {
    /// The value provided for this parameter.
    pub value: Value<'a>,
    /// The column type assigned to this parameter.
    pub coltype: myc::constants::ColumnType,
}

impl<'a> Iterator for Params<'a> {
    type Item = io::Result<ParamValue<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.nullmap.is_none() {
            let nullmap_len = (self.params as usize).div_ceil(8);
            let Some((nullmap, rest)) = self.input.split_at_checked(nullmap_len) else {
                self.col = self.params;
                return Some(Err(invalid_params("truncated parameter null bitmap")));
            };
            self.nullmap = Some(nullmap);
            self.input = rest;

            if !rest.is_empty() && rest[0] != 0x00 {
                let type_bytes = 2 * self.params as usize;
                let Some((typmap, rest)) = rest[1..].split_at_checked(type_bytes) else {
                    self.col = self.params;
                    return Some(Err(invalid_params("truncated parameter type map")));
                };
                self.bound_types.clear();
                for i in 0..self.params as usize {
                    let Ok(column_type) = myc::constants::ColumnType::try_from(typmap[2 * i])
                    else {
                        self.col = self.params;
                        return Some(Err(invalid_params("invalid parameter column type")));
                    };
                    self.bound_types
                        .push((column_type, (typmap[2 * i + 1] & 128) != 0));
                }
                self.input = rest;
            }
        }

        if self.col >= self.params {
            return None;
        }
        let Some(pt) = self.bound_types.get(self.col as usize) else {
            self.col = self.params;
            return Some(Err(invalid_params("parameter types were not supplied")));
        };

        // https://web.archive.org/web/20170404144156/https://dev.mysql.com/doc/internals/en/null-bitmap.html
        // NULL-bitmap-byte = ((field-pos + offset) / 8)
        // NULL-bitmap-bit  = ((field-pos + offset) % 8)
        if let Some(nullmap) = self.nullmap {
            let byte = self.col as usize / 8;
            if byte >= nullmap.len() {
                return None;
            }
            if (nullmap[byte] & 1u8 << (self.col % 8)) != 0 {
                self.col += 1;
                return Some(Ok(ParamValue {
                    value: Value::null(),
                    coltype: pt.0,
                }));
            }
        } else {
            unreachable!();
        }

        let v = if let Some(data) = self.long_data.get(&self.col) {
            Value::bytes(&data[..])
        } else {
            let Ok(value) = Value::parse_from(&mut self.input, pt.0, pt.1) else {
                self.col = self.params;
                return Some(Err(invalid_params("truncated or invalid parameter value")));
            };
            value
        };
        self.col += 1;
        Some(Ok(ParamValue {
            value: v,
            coltype: pt.0,
        }))
    }
}

fn invalid_params(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
