mod dataframe;

pub use dataframe::{NuDataFrameCustomValue, dataframe_type};

use datafusion::prelude::DataFrame;
use nu_plugin::EngineInterface;
use nu_protocol::{CustomValue, ShellError, Span, Value};
use uuid::Uuid;

use crate::cache::Cache;
use crate::error::shell;
use crate::plugin::DfPlugin;

pub const TYPE_NAME: &str = "datafusion_dataframe";
pub const PREVIEW_ROWS: usize = 20;
pub const COLLECT_DEFAULT: usize = 10_000;

pub fn cache_and_value(
    plugin: &DfPlugin,
    engine: &EngineInterface,
    df: DataFrame,
    span: Span,
) -> Result<Value, ShellError> {
    let id = Uuid::new_v4();
    plugin.cache.insert(engine, id, df, span)?;
    Ok(Value::custom(Box::new(NuDataFrameCustomValue { id }), span))
}

pub fn dataframe_from_custom(
    cache: &Cache,
    value: &Value,
    span: Span,
) -> Result<DataFrame, ShellError> {
    let custom = match value {
        Value::Custom { val, .. } => val,
        _ => {
            return Err(shell(
                span,
                "expected dataframe",
                format!("found {}", value.get_type()),
            ));
        }
    };
    let handle = custom
        .as_any()
        .downcast_ref::<NuDataFrameCustomValue>()
        .ok_or_else(|| {
            shell(
                span,
                "expected dataframe",
                format!("found custom value '{}'", custom.type_name()),
            )
        })?;
    cache.get(&handle.id)?.ok_or_else(|| {
        shell(
            span,
            "stale dataframe handle",
            "this dataframe is no longer in the plugin cache",
        )
    })
}

pub fn id_from_custom_box(custom: &dyn CustomValue) -> Option<Uuid> {
    custom
        .as_any()
        .downcast_ref::<NuDataFrameCustomValue>()
        .map(|v| v.id)
}
