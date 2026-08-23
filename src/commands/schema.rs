use std::sync::Arc;

use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, Type};

use crate::commands::category;
use crate::convert::schema_to_nu;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::dataframe_type;

pub struct SchemaCmd;

impl PluginCommand for SchemaCmd {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df schema"
    }

    fn description(&self) -> &str {
        "Show column names and Arrow types without collecting rows."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(dataframe_type(), Type::table())
            .input_output_type(Type::table(), Type::table())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "[[a b]; [1 2]] | df schema",
            description: "Inspect schema of a Nu table via DataFusion",
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
        let schema = Arc::new(df.schema().as_arrow().clone());
        Ok(PipelineData::value(schema_to_nu(schema, span), None))
    }
}
