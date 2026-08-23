use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, Type, Value};

use crate::commands::category;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::dataframe_type;

pub struct Count;

impl PluginCommand for Count {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df count"
    }

    fn description(&self) -> &str {
        "Count rows in a dataframe. This runs the plan."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(dataframe_type(), Type::Int)
            .input_output_type(Type::table(), Type::Int)
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "[[a]; [1] [2] [3]] | df count",
            description: "Count rows",
            result: Some(Value::test_int(3)),
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
        let n = plugin.block_on(df.count(), span)?;
        Ok(PipelineData::value(Value::int(n as i64, span), None))
    }
}
