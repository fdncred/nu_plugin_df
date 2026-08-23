use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type, Value};

use crate::commands::category;
use crate::plugin::DfPlugin;
use crate::session::unregister_table;

pub struct Unregister;

impl PluginCommand for Unregister {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df unregister"
    }

    fn description(&self) -> &str {
        "Remove a named table from the session catalog."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required("name", SyntaxShape::String, "Table name")
            .input_output_type(Type::Nothing, Type::Nothing)
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "df unregister nums",
            description: "Drop a registered table",
            result: None,
        }]
    }

    fn run(
        &self,
        plugin: &DfPlugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let span = call.head;
        let name: String = call.req(0)?;
        unregister_table(&plugin.ctx, &plugin.catalog, &name, span)?;
        Ok(PipelineData::value(Value::nothing(span), None))
    }
}
