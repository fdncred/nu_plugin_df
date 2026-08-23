use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Record, Signature, Type, Value};

use crate::commands::category;
use crate::plugin::DfPlugin;

pub struct Tables;

impl PluginCommand for Tables {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df tables"
    }

    fn description(&self) -> &str {
        "List tables registered in this plugin session."
    }

    fn extra_description(&self) -> &str {
        "Does not list the ephemeral SQL table `this`."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::table())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "df tables",
            description: "Show registered catalog tables",
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
        let names = plugin.catalog.list()?;
        let rows: Vec<Value> = names
            .into_iter()
            .map(|(name, source)| {
                let mut rec = Record::new();
                rec.push("name", Value::string(name, span));
                rec.push(
                    "source",
                    match source {
                        Some(s) => Value::string(s, span),
                        None => Value::nothing(span),
                    },
                );
                Value::record(rec, span)
            })
            .collect();
        Ok(PipelineData::value(Value::list(rows, span), None))
    }
}
