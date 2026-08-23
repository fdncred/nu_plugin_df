use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type};

use crate::commands::category;
use crate::error::from_datafusion;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_type};

pub struct Limit;

impl PluginCommand for Limit {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df limit"
    }

    fn description(&self) -> &str {
        "Keep the first n rows of a dataframe."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required("n", SyntaxShape::Int, "Number of rows to keep")
            .named("skip", SyntaxShape::Int, "Rows to skip first", Some('s'))
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "[[a]; [1] [2] [3]] | df limit 2 | df collect",
            description: "Keep the first two rows",
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
        let n: i64 = call.req(0)?;
        let skip: i64 = call.get_flag("skip")?.unwrap_or(0);
        let df = resolve_df(plugin, input, span)?;
        let out = df
            .limit(skip as usize, Some(n as usize))
            .map_err(|e| from_datafusion(e, span))?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, out, span)?,
            None,
        ))
    }
}
