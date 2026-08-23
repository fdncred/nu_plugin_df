use std::sync::Arc;

use chrono::{DateTime, Datelike, FixedOffset};
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    StructArray, TimestampNanosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{DataFrame, SessionContext};
use indexmap::IndexMap;
use nu_protocol::{Record, ShellError, Span, Value};

use crate::convert::dataframe_from_batches;
use crate::error::shell;

enum ColKind {
    Int,
    Float,
    Bool,
    String,
    Binary,
    Date,
    List(Box<ColKind>),
    Struct(Vec<(String, ColKind)>),
}

pub fn values_to_dataframe(
    ctx: &SessionContext,
    input: Value,
    span: Span,
) -> Result<DataFrame, ShellError> {
    let records = records_from_value(input, span)?;
    if records.is_empty() {
        return Err(shell(
            span,
            "empty input",
            "need at least one row to create a dataframe",
        ));
    }
    let batch = records_to_batch(&records, span)?;
    dataframe_from_batches(ctx, vec![batch], span)
}

fn records_from_value(input: Value, span: Span) -> Result<Vec<Record>, ShellError> {
    match input {
        Value::List { vals, .. } => {
            let vals = vals.into_owned();
            if vals.is_empty() {
                return Ok(vec![]);
            }
            if vals.iter().all(|v| matches!(v, Value::Record { .. })) {
                vals.into_iter()
                    .map(|v| {
                        v.into_record()
                            .map_err(|e| shell(span, "invalid table row", e.to_string()))
                    })
                    .collect()
            } else if vals
                .iter()
                .all(|v| !matches!(v, Value::Record { .. } | Value::List { .. }))
            {
                Ok(vals
                    .into_iter()
                    .map(|v| {
                        let mut rec = Record::new();
                        rec.push("value", v);
                        rec
                    })
                    .collect())
            } else {
                Err(shell(
                    span,
                    "unsupported list",
                    "expected a table (list of records) or a list of scalars",
                ))
            }
        }
        Value::Record { val, .. } => {
            let rec = val.into_owned();
            if is_columnar_record(&rec) {
                columnar_record_to_rows(&rec, span)
            } else {
                Ok(vec![rec])
            }
        }
        other => Err(shell(
            span,
            "cannot convert to dataframe",
            format!(
                "expected table, record, or list; found {}",
                other.get_type()
            ),
        )),
    }
}

fn is_columnar_record(rec: &Record) -> bool {
    !rec.is_empty() && rec.iter().all(|(_, v)| matches!(v, Value::List { .. }))
}

fn columnar_record_to_rows(rec: &Record, span: Span) -> Result<Vec<Record>, ShellError> {
    let mut lists: Vec<(String, Vec<Value>)> = Vec::new();
    for (k, v) in rec.iter() {
        match v {
            Value::List { vals, .. } => lists.push((k.clone(), vals.as_ref().to_vec())),
            _ => {
                return Err(shell(span, "columnar record", "all fields must be lists"));
            }
        }
    }
    let len = lists.first().map(|(_, v)| v.len()).unwrap_or(0);
    if lists.iter().any(|(_, v)| v.len() != len) {
        return Err(shell(
            span,
            "ragged columns",
            "all lists in a columnar record must have the same length",
        ));
    }
    let mut rows = Vec::with_capacity(len);
    for i in 0..len {
        let mut row = Record::new();
        for (name, vals) in &lists {
            row.push(name.clone(), vals[i].clone());
        }
        rows.push(row);
    }
    Ok(rows)
}

fn records_to_batch(records: &[Record], span: Span) -> Result<RecordBatch, ShellError> {
    let mut columns: IndexMap<String, Vec<Value>> = IndexMap::new();
    for rec in records {
        for (k, _) in rec.iter() {
            columns.entry(k.clone()).or_default();
        }
    }
    for rec in records {
        for (name, col) in columns.iter_mut() {
            match rec.get(name) {
                Some(v) => col.push(v.clone()),
                None => col.push(Value::nothing(span)),
            }
        }
    }

    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (name, values) in &columns {
        let kind = infer_kind(values, name, span)?;
        let field = Field::new(name, kind_to_dtype(&kind), true);
        let array = build_array(&kind, values, span)?;
        fields.push(field);
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| shell(span, "failed to build record batch", e.to_string()))
}

fn infer_kind(values: &[Value], name: &str, span: Span) -> Result<ColKind, ShellError> {
    let mut kind: Option<ColKind> = None;
    for v in values {
        if matches!(v, Value::Nothing { .. }) {
            continue;
        }
        let next = kind_of_value(v, name, span)?;
        match &kind {
            None => kind = Some(next),
            Some(prev) if kinds_compatible(prev, &next) => {
                kind = Some(widen(prev, &next));
            }
            Some(prev) => {
                return Err(shell(
                    span,
                    "mixed column types",
                    format!(
                        "column '{name}' has mixed types {} and {}",
                        kind_label(prev),
                        kind_label(&next)
                    ),
                ));
            }
        }
    }
    Ok(kind.unwrap_or(ColKind::String))
}

fn kind_of_value(v: &Value, name: &str, span: Span) -> Result<ColKind, ShellError> {
    match v {
        Value::Int { .. } | Value::Filesize { .. } => Ok(ColKind::Int),
        Value::Float { .. } => Ok(ColKind::Float),
        Value::Bool { .. } => Ok(ColKind::Bool),
        Value::String { .. } => Ok(ColKind::String),
        Value::Binary { .. } => Ok(ColKind::Binary),
        Value::Date { .. } | Value::Duration { .. } => Ok(ColKind::Date),
        Value::List { vals, .. } => {
            let inner = infer_kind(vals, name, span)?;
            Ok(ColKind::List(Box::new(inner)))
        }
        Value::Record { val, .. } => {
            let mut fields = Vec::new();
            for (k, inner) in val.iter() {
                fields.push((k.clone(), kind_of_value(inner, k, span)?));
            }
            Ok(ColKind::Struct(fields))
        }
        Value::Nothing { .. } => Ok(ColKind::String),
        other => Err(shell(
            span,
            "unsupported cell type",
            format!(
                "column '{name}' contains {} which cannot be stored in a dataframe",
                other.get_type()
            ),
        )),
    }
}

fn kinds_compatible(a: &ColKind, b: &ColKind) -> bool {
    match (a, b) {
        (ColKind::Int, ColKind::Int)
        | (ColKind::Int, ColKind::Float)
        | (ColKind::Float, ColKind::Int)
        | (ColKind::Float, ColKind::Float)
        | (ColKind::Bool, ColKind::Bool)
        | (ColKind::String, ColKind::String)
        | (ColKind::Binary, ColKind::Binary)
        | (ColKind::Date, ColKind::Date) => true,
        (ColKind::List(a), ColKind::List(b)) => kinds_compatible(a, b),
        (ColKind::Struct(a), ColKind::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|((na, ka), (nb, kb))| na == nb && kinds_compatible(ka, kb))
        }
        _ => false,
    }
}

fn widen(a: &ColKind, b: &ColKind) -> ColKind {
    match (a, b) {
        (ColKind::Float, _) | (_, ColKind::Float) if matches!(a, ColKind::Int | ColKind::Float) => {
            ColKind::Float
        }
        (ColKind::List(a), ColKind::List(b)) => ColKind::List(Box::new(widen(a, b))),
        _ => match a {
            ColKind::Int => ColKind::Int,
            ColKind::Float => ColKind::Float,
            ColKind::Bool => ColKind::Bool,
            ColKind::String => ColKind::String,
            ColKind::Binary => ColKind::Binary,
            ColKind::Date => ColKind::Date,
            ColKind::List(inner) => ColKind::List(inner.clone()),
            ColKind::Struct(fields) => ColKind::Struct(fields.clone()),
        },
    }
}

impl Clone for ColKind {
    fn clone(&self) -> Self {
        match self {
            ColKind::Int => ColKind::Int,
            ColKind::Float => ColKind::Float,
            ColKind::Bool => ColKind::Bool,
            ColKind::String => ColKind::String,
            ColKind::Binary => ColKind::Binary,
            ColKind::Date => ColKind::Date,
            ColKind::List(k) => ColKind::List(k.clone()),
            ColKind::Struct(f) => ColKind::Struct(f.clone()),
        }
    }
}

fn kind_label(k: &ColKind) -> &'static str {
    match k {
        ColKind::Int => "int",
        ColKind::Float => "float",
        ColKind::Bool => "bool",
        ColKind::String => "string",
        ColKind::Binary => "binary",
        ColKind::Date => "date",
        ColKind::List(_) => "list",
        ColKind::Struct(_) => "record",
    }
}

fn kind_to_dtype(kind: &ColKind) -> DataType {
    match kind {
        ColKind::Int => DataType::Int64,
        ColKind::Float => DataType::Float64,
        ColKind::Bool => DataType::Boolean,
        ColKind::String => DataType::Utf8,
        ColKind::Binary => DataType::Binary,
        ColKind::Date => DataType::Timestamp(TimeUnit::Nanosecond, None),
        ColKind::List(inner) => {
            DataType::List(Arc::new(Field::new("item", kind_to_dtype(inner), true)))
        }
        ColKind::Struct(fields) => {
            let arrow_fields: Fields = fields
                .iter()
                .map(|(n, k)| Field::new(n, kind_to_dtype(k), true))
                .collect();
            DataType::Struct(arrow_fields)
        }
    }
}

fn build_array(kind: &ColKind, values: &[Value], span: Span) -> Result<ArrayRef, ShellError> {
    match kind {
        ColKind::Int => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Nothing { .. } => b.append_null(),
                    Value::Int { val, .. } => b.append_value(*val),
                    Value::Filesize { val, .. } => b.append_value(val.get()),
                    _ => {
                        return Err(shell(span, "type mismatch", "expected int in int column"));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColKind::Float => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Nothing { .. } => b.append_null(),
                    Value::Float { val, .. } => b.append_value(*val),
                    Value::Int { val, .. } => b.append_value(*val as f64),
                    _ => {
                        return Err(shell(
                            span,
                            "type mismatch",
                            "expected float in float column",
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColKind::Bool => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Nothing { .. } => b.append_null(),
                    Value::Bool { val, .. } => b.append_value(*val),
                    _ => {
                        return Err(shell(span, "type mismatch", "expected bool in bool column"));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColKind::String => {
            let mut b = StringBuilder::new();
            for v in values {
                match v {
                    Value::Nothing { .. } => b.append_null(),
                    Value::String { val, .. } => b.append_value(val),
                    _ => b.append_value(v.to_expanded_string("", &nu_protocol::Config::default())),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColKind::Binary => {
            let mut b = BinaryBuilder::new();
            for v in values {
                match v {
                    Value::Nothing { .. } => b.append_null(),
                    Value::Binary { val, .. } => b.append_value(val),
                    _ => {
                        return Err(shell(
                            span,
                            "type mismatch",
                            "expected binary in binary column",
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColKind::Date => {
            let mut b = TimestampNanosecondBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Nothing { .. } => b.append_null(),
                    Value::Date { val, .. } => b.append_value(datetime_to_nanos(val)),
                    Value::Duration { val, .. } => b.append_value(*val),
                    _ => {
                        return Err(shell(span, "type mismatch", "expected date in date column"));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColKind::List(inner) => build_list_array(inner, values, span),
        ColKind::Struct(fields) => build_struct_array(fields, values, span),
    }
}

fn build_list_array(inner: &ColKind, values: &[Value], span: Span) -> Result<ArrayRef, ShellError> {
    let mut offsets: Vec<i32> = vec![0];
    let mut flat: Vec<Value> = Vec::new();
    let mut validity = Vec::new();
    for v in values {
        match v {
            Value::Nothing { .. } => {
                validity.push(false);
                offsets.push(flat.len() as i32);
            }
            Value::List { vals, .. } => {
                validity.push(true);
                flat.extend(vals.iter().cloned());
                offsets.push(flat.len() as i32);
            }
            _ => {
                return Err(shell(span, "type mismatch", "expected list in list column"));
            }
        }
    }
    let inner_array = if flat.is_empty() {
        build_array(inner, &[], span)?
    } else {
        build_array(inner, &flat, span)?
    };
    let field = Arc::new(Field::new("item", kind_to_dtype(inner), true));
    let offset_buffer = datafusion::arrow::buffer::OffsetBuffer::new(offsets.into());
    let nulls = datafusion::arrow::buffer::NullBuffer::from(validity);
    let list = datafusion::arrow::array::ListArray::try_new(
        field,
        offset_buffer,
        inner_array,
        Some(nulls),
    )
    .map_err(|e| shell(span, "failed to build list array", e.to_string()))?;
    Ok(Arc::new(list))
}

fn build_struct_array(
    fields: &[(String, ColKind)],
    values: &[Value],
    span: Span,
) -> Result<ArrayRef, ShellError> {
    let mut child_vals: Vec<Vec<Value>> = fields
        .iter()
        .map(|_| Vec::with_capacity(values.len()))
        .collect();
    let mut validity = Vec::with_capacity(values.len());
    for v in values {
        match v {
            Value::Nothing { .. } => {
                validity.push(false);
                for col in &mut child_vals {
                    col.push(Value::nothing(span));
                }
            }
            Value::Record { val, .. } => {
                validity.push(true);
                for (i, (name, _)) in fields.iter().enumerate() {
                    child_vals[i].push(
                        val.get(name)
                            .cloned()
                            .unwrap_or_else(|| Value::nothing(span)),
                    );
                }
            }
            _ => {
                return Err(shell(
                    span,
                    "type mismatch",
                    "expected record in struct column",
                ));
            }
        }
    }
    let mut arrays = Vec::new();
    let mut arrow_fields = Vec::new();
    for (i, (name, kind)) in fields.iter().enumerate() {
        arrow_fields.push(Field::new(name, kind_to_dtype(kind), true));
        arrays.push(build_array(kind, &child_vals[i], span)?);
    }
    let nulls = datafusion::arrow::buffer::NullBuffer::from(validity);
    let struct_arr = StructArray::try_new(arrow_fields.into(), arrays, Some(nulls))
        .map_err(|e| shell(span, "failed to build struct array", e.to_string()))?;
    Ok(Arc::new(struct_arr))
}

fn datetime_to_nanos(dt: &DateTime<FixedOffset>) -> i64 {
    dt.timestamp_nanos_opt()
        .unwrap_or_else(|| dt.timestamp() * 1_000_000_000 + i64::from(dt.timestamp_subsec_nanos()))
}

// silence unused Datelike import if timestamp_nanos_opt exists
#[allow(dead_code)]
fn _date_parts(dt: &DateTime<FixedOffset>) -> (i32, u32) {
    (dt.year(), dt.month())
}
