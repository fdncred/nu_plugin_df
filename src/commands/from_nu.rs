use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, Type};

use crate::commands::category;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_type};

pub struct FromNu;

impl PluginCommand for FromNu {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df from-nu"
    }

    fn description(&self) -> &str {
        "Convert a Nu table, record, or list into a dataframe."
    }

    fn extra_description(&self) -> &str {
        "Most `df` commands already convert Nu tables automatically. Use this when you want an explicit conversion."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Any, dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "[[a b]; [1 2] [3 4]] | df from-nu | df collect",
            description: "Convert a table to a dataframe and back",
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
        let df = resolve_df(plugin, input, span)?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, df, span)?,
            None,
        ))
    }
}
