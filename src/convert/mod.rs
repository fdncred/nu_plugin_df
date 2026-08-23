mod schema;
mod to_arrow;
mod to_nu;

pub use schema::{parse_arrow_type, schema_to_nu};
pub use to_arrow::values_to_dataframe;
pub use to_nu::collect_to_nu;

use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::{DataFrame, SessionContext};
use nu_protocol::{ShellError, Span};

use crate::error::from_datafusion;

pub fn dataframe_from_batches(
    ctx: &SessionContext,
    batches: Vec<RecordBatch>,
    span: Span,
) -> Result<DataFrame, ShellError> {
    if batches.is_empty() {
        return Err(crate::error::shell(
            span,
            "empty dataframe",
            "no record batches to load",
        ));
    }
    ctx.read_batches(batches)
        .map_err(|e| from_datafusion(e, span))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nu_protocol::{Span, Value, record};
    use tokio::runtime::Runtime;

    fn rt() -> Runtime {
        Runtime::new().expect("tokio")
    }

    fn collect_rows(df: DataFrame) -> Vec<RecordBatch> {
        rt().block_on(df.collect()).expect("collect")
    }

    #[test]
    fn table_to_dataframe_roundtrip() {
        let ctx = SessionContext::new();
        let span = Span::test_data();
        let input = Value::test_list(vec![
            Value::test_record(record! {
                "a" => Value::test_int(1),
                "b" => Value::test_int(2),
            }),
            Value::test_record(record! {
                "a" => Value::test_int(3),
                "b" => Value::test_int(4),
            }),
        ]);
        let df = values_to_dataframe(&ctx, input, span).expect("convert");
        let expr = df.parse_sql_expr("a > 1").expect("expr");
        let filtered = df.filter(expr).expect("filter");
        let batches = collect_rows(filtered);
        let nu = crate::convert::to_nu::batches_to_nu(&batches, span).expect("to nu");
        match nu {
            Value::List { vals, .. } => {
                assert_eq!(vals.as_ref().len(), 1);
                let rec = vals.as_ref()[0].as_record().expect("row");
                assert_eq!(rec.get("a").unwrap().as_int().unwrap(), 3);
            }
            other => panic!("expected list, got {other:?}"),
        }
    }

    #[test]
    fn parquet_roundtrip() {
        use datafusion::dataframe::DataFrameWriteOptions;
        use datafusion::prelude::ParquetReadOptions;

        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("t.parquet");
        let path_str = path.to_string_lossy().to_string();
        let ctx = SessionContext::new();
        let span = Span::test_data();
        let input = Value::test_list(vec![Value::test_record(record! {
            "a" => Value::test_int(1),
            "b" => Value::test_string("x"),
        })]);
        let df = values_to_dataframe(&ctx, input, span).expect("convert");
        rt().block_on(df.write_parquet(&path_str, DataFrameWriteOptions::new(), None))
            .expect("write");
        let opened = rt()
            .block_on(ctx.read_parquet(&path_str, ParquetReadOptions::default()))
            .expect("read");
        let n = rt().block_on(opened.count()).expect("count");
        assert_eq!(n, 1);
    }

    #[test]
    fn mixed_column_fails() {
        let ctx = SessionContext::new();
        let span = Span::test_data();
        let input = Value::test_list(vec![
            Value::test_record(record! { "a" => Value::test_int(1) }),
            Value::test_record(record! { "a" => Value::test_bool(true) }),
        ]);
        let err = values_to_dataframe(&ctx, input, span).expect_err("mixed");
        let msg = err.to_string();
        assert!(
            msg.contains("mixed") || msg.contains("type"),
            "unexpected: {msg}"
        );
    }
}
