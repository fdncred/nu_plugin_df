use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type, Value};

use crate::commands::category;
use crate::error::{from_datafusion, shell};
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_from_custom, dataframe_type};

pub struct Union;

impl PluginCommand for Union {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df union"
    }

    fn description(&self) -> &str {
        "Concatenate two dataframes. Default is UNION ALL; --distinct drops duplicates."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required(
                "other",
                SyntaxShape::Any,
                "Other dataframe handle or catalog table name",
            )
            .switch("distinct", "Drop duplicate rows (SQL UNION)", Some('d'))
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"let o = ([[a]; [3]] | df from-nu); [[a]; [1]] | df union $o | df collect"#,
            description: "Union two dataframes",
            result: None,
        }]
    }

    fn run(
        &self,
        plugin: &DfPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let span = call.head;
        let left = resolve_df(plugin, input, span)?;
        let other: Value = call.req(0)?;
        let right = match &other {
            Value::Custom { .. } => dataframe_from_custom(&plugin.cache, &other, span)?,
            Value::String { val, .. } => plugin.block_on(plugin.ctx.table(val.as_str()), span)?,
            _ => {
                return Err(shell(
                    span,
                    "invalid union input",
                    "expected a dataframe handle or catalog table name",
                )
                .into());
            }
        };
        let out = if call.has_flag("distinct")? {
            left.union_distinct(right)
        } else {
            left.union(right)
        }
        .map_err(|e| from_datafusion(e, span))?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, out, span)?,
            None,
        ))
    }
}
