use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type};

use crate::commands::category;
use crate::error::from_datafusion;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_type};

pub struct DropCols;

impl PluginCommand for DropCols {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df drop"
    }

    fn description(&self) -> &str {
        "Drop columns from a dataframe."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .rest("columns", SyntaxShape::String, "Column names to drop")
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "[[a b]; [1 2]] | df drop b | df collect",
            description: "Drop a column",
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
        let cols: Vec<String> = call.rest(0)?;
        let df = resolve_df(plugin, input, span)?;
        let out = df
            .drop_columns(&cols)
            .map_err(|e| from_datafusion(e, span))?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, out, span)?,
            None,
        ))
    }
}
