use datafusion::logical_expr::Expr;
use datafusion::prelude::DataFrame;
use nu_protocol::{ShellError, Span};

use crate::error::from_datafusion;

pub fn parse_sql_expr(df: &DataFrame, fragment: &str, span: Span) -> Result<Expr, ShellError> {
    df.parse_sql_expr(fragment)
        .map_err(|e| from_datafusion(e, span))
}
