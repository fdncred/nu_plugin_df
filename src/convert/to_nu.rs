use chrono::{TimeZone, Utc};
use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    DurationNanosecondArray, Float32Array, Float64Array, GenericListArray, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, OffsetSizeTrait, RecordBatch,
    StringArray, StringViewArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::prelude::DataFrame;
use nu_protocol::{Record, ShellError, Span, Value};

use crate::error::{from_datafusion, shell};
use crate::plugin::DfPlugin;

pub fn collect_to_nu(
    plugin: &DfPlugin,
    df: DataFrame,
    span: Span,
    max_rows: Option<usize>,
) -> Result<(Value, bool), ShellError> {
    let limited = if let Some(n) = max_rows {
        df.limit(0, Some(n + 1))
            .map_err(|e| from_datafusion(e, span))?
    } else {
        df
    };
    let batches = plugin.block_on(limited.collect(), span)?;
    let mut rows = batches_to_records(&batches, span)?;
    let mut truncated = false;
    if let Some(n) = max_rows
        && rows.len() > n
    {
        rows.truncate(n);
        truncated = true;
    }
    let values: Vec<Value> = rows
        .into_iter()
        .map(|rec| Value::record(rec, span))
        .collect();
    Ok((Value::list(values, span), truncated))
}

#[cfg(test)]
pub(crate) fn batches_to_nu(batches: &[RecordBatch], span: Span) -> Result<Value, ShellError> {
    let rows = batches_to_records(batches, span)?;
    Ok(Value::list(
        rows.into_iter()
            .map(|rec| Value::record(rec, span))
            .collect(),
        span,
    ))
}

fn batches_to_records(batches: &[RecordBatch], span: Span) -> Result<Vec<Record>, ShellError> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut rec = Record::new();
            for (i, field) in batch.schema().fields().iter().enumerate() {
                let col = batch.column(i);
                rec.push(field.name().clone(), array_value_at(col, row, span)?);
            }
            rows.push(rec);
        }
    }
    Ok(rows)
}

fn array_value_at(array: &dyn Array, row: usize, span: Span) -> Result<Value, ShellError> {
    if array.is_null(row) {
        return Ok(Value::nothing(span));
    }
    match array.data_type() {
        DataType::Null => Ok(Value::nothing(span)),
        DataType::Boolean => Ok(Value::bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("bool")
                .value(row),
            span,
        )),
        DataType::Int8 => Ok(Value::int(
            i64::from(
                array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("i8")
                    .value(row),
            ),
            span,
        )),
        DataType::Int16 => Ok(Value::int(
            i64::from(
                array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .expect("i16")
                    .value(row),
            ),
            span,
        )),
        DataType::Int32 => Ok(Value::int(
            i64::from(
                array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("i32")
                    .value(row),
            ),
            span,
        )),
        DataType::Int64 => Ok(Value::int(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64")
                .value(row),
            span,
        )),
        DataType::UInt8 => Ok(Value::int(
            i64::from(
                array
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .expect("u8")
                    .value(row),
            ),
            span,
        )),
        DataType::UInt16 => Ok(Value::int(
            i64::from(
                array
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .expect("u16")
                    .value(row),
            ),
            span,
        )),
        DataType::UInt32 => Ok(Value::int(
            i64::from(
                array
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .expect("u32")
                    .value(row),
            ),
            span,
        )),
        DataType::UInt64 => {
            let v = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("u64")
                .value(row);
            i64::try_from(v).map(|n| Value::int(n, span)).map_err(|_| {
                shell(
                    span,
                    "integer overflow",
                    format!("uint64 value {v} does not fit in a Nushell int"),
                )
            })
        }
        DataType::Float32 => Ok(Value::float(
            f64::from(
                array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("f32")
                    .value(row),
            ),
            span,
        )),
        DataType::Float64 => Ok(Value::float(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("f64")
                .value(row),
            span,
        )),
        DataType::Utf8 => Ok(Value::string(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8")
                .value(row)
                .to_string(),
            span,
        )),
        DataType::LargeUtf8 => Ok(Value::string(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large utf8")
                .value(row)
                .to_string(),
            span,
        )),
        DataType::Utf8View => Ok(Value::string(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("utf8 view")
                .value(row)
                .to_string(),
            span,
        )),
        DataType::Binary => Ok(Value::binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary")
                .value(row)
                .to_vec(),
            span,
        )),
        DataType::LargeBinary => Ok(Value::binary(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("large binary")
                .value(row)
                .to_vec(),
            span,
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let ns = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts ns")
                .value(row);
            Ok(nanos_to_date(ns, span))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let us = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("ts us")
                .value(row);
            Ok(nanos_to_date(us.saturating_mul(1_000), span))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let ms = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("ts ms")
                .value(row);
            Ok(nanos_to_date(ms.saturating_mul(1_000_000), span))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let s = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("ts s")
                .value(row);
            Ok(nanos_to_date(s.saturating_mul(1_000_000_000), span))
        }
        DataType::Date32 => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("date32")
                .value(row);
            Ok(nanos_to_date(
                i64::from(days) * 86_400 * 1_000_000_000,
                span,
            ))
        }
        DataType::Date64 => {
            let ms = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .expect("date64")
                .value(row);
            Ok(nanos_to_date(ms.saturating_mul(1_000_000), span))
        }
        DataType::Duration(TimeUnit::Nanosecond) => Ok(Value::duration(
            array
                .as_any()
                .downcast_ref::<DurationNanosecondArray>()
                .expect("dur ns")
                .value(row),
            span,
        )),
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("dec128");
            let raw = arr.value(row);
            Ok(Value::string(format_decimal128(raw, *scale), span))
        }
        DataType::List(_) => list_to_value::<i32>(array, row, span),
        DataType::LargeList(_) => list_to_value::<i64>(array, row, span),
        DataType::Struct(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct");
            let mut rec = Record::new();
            for (i, field) in arr.fields().iter().enumerate() {
                rec.push(
                    field.name().clone(),
                    array_value_at(arr.column(i).as_ref(), row, span)?,
                );
            }
            Ok(Value::record(rec, span))
        }
        other => Ok(Value::string(format!("{other:?}"), span)),
    }
}

fn list_to_value<O: OffsetSizeTrait>(
    array: &dyn Array,
    row: usize,
    span: Span,
) -> Result<Value, ShellError> {
    let arr = array
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .ok_or_else(|| shell(span, "list downcast failed", "internal arrow error"))?;
    let values = arr.value(row);
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        out.push(array_value_at(values.as_ref(), i, span)?);
    }
    Ok(Value::list(out, span))
}

fn nanos_to_date(ns: i64, span: Span) -> Value {
    let secs = ns.div_euclid(1_000_000_000);
    let nsecs = ns.rem_euclid(1_000_000_000) as u32;
    match Utc.timestamp_opt(secs, nsecs).single() {
        Some(dt) => Value::date(dt.fixed_offset(), span),
        None => Value::int(ns, span),
    }
}

fn format_decimal128(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }
    let scale = scale as u32;
    let factor = 10i128.pow(scale);
    let (int, frac) = (value / factor, value.abs() % factor);
    let frac_str = format!("{frac:0width$}", width = scale as usize);
    if value < 0 && int == 0 {
        format!("-0.{frac_str}")
    } else {
        format!("{int}.{frac_str}")
    }
}
