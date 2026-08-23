use datafusion::prelude::DataFrame;
use nu_protocol::{PipelineData, ShellError, Span, Value};

use crate::convert::values_to_dataframe;
use crate::error::shell;
use crate::plugin::DfPlugin;
use crate::values::dataframe_from_custom;

/// Resolve pipeline input to a DataFrame.
/// Custom dataframe handles are loaded from the cache.
/// Nu tables/lists/records are converted in memory.
pub fn resolve_df(
    plugin: &DfPlugin,
    input: PipelineData,
    span: Span,
) -> Result<DataFrame, ShellError> {
    match input {
        PipelineData::Value(value, _) => resolve_value(plugin, value, span),
        PipelineData::Empty => Err(shell(
            span,
            "pipeline empty",
            "expected a dataframe, table, record, or list",
        )),
        other => {
            let value = other.into_value(span)?;
            resolve_value(plugin, value, span)
        }
    }
}

pub fn resolve_value(plugin: &DfPlugin, value: Value, span: Span) -> Result<DataFrame, ShellError> {
    match &value {
        Value::Custom { .. } => dataframe_from_custom(&plugin.cache, &value, span),
        _ => values_to_dataframe(&plugin.ctx, value, span),
    }
}
