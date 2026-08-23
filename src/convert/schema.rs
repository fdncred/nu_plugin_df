use datafusion::arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use nu_protocol::{Record, Span, Value};

pub fn schema_to_nu(schema: SchemaRef, span: Span) -> Value {
    let rows: Vec<Value> = schema
        .fields()
        .iter()
        .map(|field| field_to_record(field, span))
        .collect();
    Value::list(rows, span)
}

fn field_to_record(field: &Field, span: Span) -> Value {
    let mut rec = Record::new();
    rec.push("name", Value::string(field.name().clone(), span));
    rec.push("type", Value::string(dtype_name(field.data_type()), span));
    rec.push("nullable", Value::bool(field.is_nullable(), span));
    Value::record(rec, span)
}

pub fn dtype_name(dt: &DataType) -> String {
    match dt {
        DataType::Null => "null".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "int8".into(),
        DataType::Int16 => "int16".into(),
        DataType::Int32 => "int32".into(),
        DataType::Int64 => "int64".into(),
        DataType::UInt8 => "uint8".into(),
        DataType::UInt16 => "uint16".into(),
        DataType::UInt32 => "uint32".into(),
        DataType::UInt64 => "uint64".into(),
        DataType::Float16 => "float16".into(),
        DataType::Float32 => "float32".into(),
        DataType::Float64 => "float64".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "utf8".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary".into(),
        DataType::Timestamp(unit, tz) => match tz {
            Some(tz) => format!("timestamp[{unit:?}, {tz}]"),
            None => format!("timestamp[{unit:?}]"),
        },
        DataType::Date32 => "date32".into(),
        DataType::Date64 => "date64".into(),
        DataType::Duration(unit) => format!("duration[{unit:?}]"),
        DataType::Time32(unit) => format!("time32[{unit:?}]"),
        DataType::Time64(unit) => format!("time64[{unit:?}]"),
        DataType::Decimal128(p, s) => format!("decimal128({p},{s})"),
        DataType::Decimal256(p, s) => format!("decimal256({p},{s})"),
        DataType::List(f) => format!("list<{}>", dtype_name(f.data_type())),
        DataType::LargeList(f) => format!("large_list<{}>", dtype_name(f.data_type())),
        DataType::FixedSizeList(f, n) => format!("fixed_list<{n}, {}>", dtype_name(f.data_type())),
        DataType::Struct(fields) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name(), dtype_name(f.data_type())))
                .collect();
            format!("struct<{}>", inner.join(", "))
        }
        DataType::Dictionary(_, value) => format!("dictionary<{}>", dtype_name(value)),
        other => format!("{other}"),
    }
}

pub fn parse_arrow_type(name: &str) -> Option<DataType> {
    match name.trim().to_ascii_lowercase().as_str() {
        "int8" => Some(DataType::Int8),
        "int16" => Some(DataType::Int16),
        "int32" => Some(DataType::Int32),
        "int64" | "int" => Some(DataType::Int64),
        "uint8" => Some(DataType::UInt8),
        "uint16" => Some(DataType::UInt16),
        "uint32" => Some(DataType::UInt32),
        "uint64" => Some(DataType::UInt64),
        "float32" | "float" => Some(DataType::Float32),
        "float64" | "double" => Some(DataType::Float64),
        "utf8" | "string" | "str" => Some(DataType::Utf8),
        "bool" | "boolean" => Some(DataType::Boolean),
        "binary" => Some(DataType::Binary),
        "date32" | "date" => Some(DataType::Date32),
        "date64" => Some(DataType::Date64),
        "timestamp" | "datetime" => Some(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        "duration" => Some(DataType::Duration(TimeUnit::Nanosecond)),
        _ => None,
    }
}
