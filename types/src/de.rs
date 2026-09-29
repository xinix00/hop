//! Hulpjes om getypte velden uit een [`Value`] te lezen, met de veldnaam in de fout.
//!
//! Gedeeld door de typen van deze crate en door `config`: elke lezer doet
//! hetzelfde (type toetsen, bereik toetsen, faalbaar kopiëren), en een fout
//! noemt altijd het veld.

use alloc::string::String;
use alloc::vec::Vec;

use crate::json::{Object, Value};
use crate::time::{DurationError, Nanos, parse_duration};
use crate::{Error, Map, Name, Result, try_string};

/// Het object in `v`, of een typefout voor `field`.
pub fn object<'a>(v: &'a Value, field: &str) -> Result<&'a Object> {
    v.as_object().ok_or(Error::WrongType {
        field: Name::new(field),
        want: "an object",
    })
}

/// De array in `v`, of een typefout voor `field`.
pub fn array<'a>(v: &'a Value, field: &str) -> Result<&'a [Value]> {
    v.as_array().ok_or(Error::WrongType {
        field: Name::new(field),
        want: "an array",
    })
}

/// Een faalbare kopie van de string in `v`.
pub fn string(v: &Value, field: &str) -> Result<String> {
    let s = v.as_str().ok_or(Error::WrongType {
        field: Name::new(field),
        want: "a string",
    })?;
    try_string(s)
}

/// De bool in `v`.
pub fn boolean(v: &Value, field: &str) -> Result<bool> {
    v.as_bool().ok_or(Error::WrongType {
        field: Name::new(field),
        want: "a bool",
    })
}

/// Het gehele getal in `v` als `i64`.
pub fn int(v: &Value, field: &str) -> Result<i64> {
    match v {
        Value::Number(n) => n.as_i64().ok_or(Error::OutOfRange {
            field: Name::new(field),
        }),
        _ => Err(Error::WrongType {
            field: Name::new(field),
            want: "an integer",
        }),
    }
}

/// Het niet-negatieve gehele getal in `v` als `u64`.
pub fn uint(v: &Value, field: &str) -> Result<u64> {
    match v {
        Value::Number(n) => n.as_u64().ok_or(Error::OutOfRange {
            field: Name::new(field),
        }),
        _ => Err(Error::WrongType {
            field: Name::new(field),
            want: "a non-negative integer",
        }),
    }
}

/// Het getal in `v` als `f64`.
pub fn float(v: &Value, field: &str) -> Result<f64> {
    match v {
        Value::Number(n) => Ok(n.as_f64()),
        _ => Err(Error::WrongType {
            field: Name::new(field),
            want: "a number",
        }),
    }
}

/// Een duur: een geheel getal nanoseconden (zoals Go's `time.Duration` op
/// de draad staat) of een Go-duurstring (`"5s"`, zoals de documentatie hem
/// schreef).
pub fn duration(v: &Value, field: &str) -> Result<Nanos> {
    match v {
        Value::String(s) => duration_str(s, field),
        _ => uint(v, field),
    }
}

/// Een duur die alleen als string mag (de config: een getal is daar een fout).
pub fn duration_str(s: &str, field: &str) -> Result<Nanos> {
    parse_duration(s).map_err(|e| Error::Invalid {
        field: Name::new(field),
        why: match e {
            DurationError::Invalid => "not a duration (try \"30s\")",
            DurationError::Negative => "negative duration",
            DurationError::Overflow => "duration too large",
        },
    })
}

/// Een map van string naar string, met hoogstens `max` sleutels.
pub fn str_map(v: &Value, field: &str, max: usize) -> Result<Map<String>> {
    let obj = object(v, field)?;
    if obj.len() > max {
        return Err(Error::TooMany {
            field: Name::new(field),
            max,
        });
    }
    let mut m = Map::new();
    for (k, item) in obj.iter() {
        m.insert(try_string(k)?, string(item, field)?)?;
    }
    Ok(m)
}

/// Een lijst van `T`, met hoogstens `max` elementen, gelezen met `each`.
pub fn list<T>(
    v: &Value,
    field: &str,
    max: usize,
    mut each: impl FnMut(&Value) -> Result<T>,
) -> Result<Vec<T>> {
    let arr = array(v, field)?;
    if arr.len() > max {
        return Err(Error::TooMany {
            field: Name::new(field),
            max,
        });
    }
    let mut out = Vec::new();
    out.try_reserve_exact(arr.len())
        .map_err(|_| Error::OutOfMemory)?;
    for item in arr {
        out.push(each(item)?);
    }
    Ok(out)
}

/// De fout voor een onbekende sleutel in strikte modus, of `Ok` in lakse.
pub fn unknown(key: &str, strict: bool) -> Result {
    if strict {
        Err(Error::UnknownField {
            field: Name::new(key),
        })
    } else {
        Ok(())
    }
}

/// Bouwt een object veld voor veld, voor de schrijfkant.
#[derive(Default)]
pub struct ObjectBuilder {
    obj: Object,
}

impl ObjectBuilder {
    /// Een leeg object.
    pub fn new() -> Self {
        Self::default()
    }

    /// Zet `key` op `v`.
    pub fn field(&mut self, key: &str, v: Value) -> Result<&mut Self> {
        self.obj.push(key, v)?;
        Ok(self)
    }

    /// Zet `key` op de string `s`.
    pub fn str(&mut self, key: &str, s: &str) -> Result<&mut Self> {
        self.field(key, Value::string(s)?)
    }

    /// Zet `key` op `s`, maar alleen als `s` niet leeg is (Go's `omitempty`).
    pub fn str_opt(&mut self, key: &str, s: &str) -> Result<&mut Self> {
        if s.is_empty() {
            return Ok(self);
        }
        self.str(key, s)
    }

    /// Zet `key` op het getal `n`, maar alleen als het niet 0 is.
    pub fn int_opt(&mut self, key: &str, n: i64) -> Result<&mut Self> {
        if n == 0 {
            return Ok(self);
        }
        self.field(key, Value::int(n))
    }

    /// Zet `key` op het getal `n`, maar alleen als het niet 0 is.
    pub fn uint_opt(&mut self, key: &str, n: u64) -> Result<&mut Self> {
        if n == 0 {
            return Ok(self);
        }
        self.field(key, Value::uint(n))
    }

    /// Zet `key` op de map `m` van strings, maar alleen als hij niet leeg is.
    pub fn map_opt(&mut self, key: &str, m: &Map<String>) -> Result<&mut Self> {
        if m.is_empty() {
            return Ok(self);
        }
        self.field(key, str_map_value(m)?)
    }

    /// Het gebouwde object als waarde.
    pub fn build(self) -> Value {
        Value::Object(self.obj)
    }
}

/// Een map van strings als JSON-object.
pub fn str_map_value(m: &Map<String>) -> Result<Value> {
    let mut obj = Object::new();
    for (k, v) in m.iter() {
        obj.push(k, Value::string(v)?)?;
    }
    Ok(Value::Object(obj))
}
