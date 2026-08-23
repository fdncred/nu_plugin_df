use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, Type};

use crate::commands::category;
use crate::convert::collect_to_nu;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::dataframe_type;

pub struct Describe;

impl PluginCommand for Describe {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df describe"
    }

    fn description(&self) -> &str {
        "Column statistics (count, nulls, min, max, mean, ...). This scans the data."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(dataframe_type(), Type::table())
            .input_output_type(Type::table(), Type::table())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "[[a]; [1] [2] [3]] | df describe",
            description: "Describe a numeric column",
            result: None,
        }]
    }

    fn run(
        &self,
        plugin: &DfPlugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let span = call.head;
        let df = resolve_df(plugin, input, span)?;
        let described = plugin.block_on(df.describe(), span)?;
        let (value, _) = collect_to_nu(plugin, described, span, None)?;
        Ok(PipelineData::value(value, None))
    }
}
