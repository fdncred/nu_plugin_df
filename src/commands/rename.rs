use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type, Value};

use crate::commands::category;
use crate::error::{from_datafusion, shell};
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_type};

pub struct Rename;

impl PluginCommand for Rename {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df rename"
    }

    fn description(&self) -> &str {
        "Rename columns. Pass a record of {old: new} pairs."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required(
                "mapping",
                SyntaxShape::Record(vec![].into()),
                "Record of old names to new names",
            )
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "[[a b]; [1 2]] | df rename {a: x} | df collect",
            description: "Rename column a to x",
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
        let mapping: Value = call.req(0)?;
        let rec = mapping.as_record()?;
        let mut df = resolve_df(plugin, input, span)?;
        for (old, new_val) in rec.iter() {
            let new = new_val.as_str()?;
            df = df
                .with_column_renamed(old, new)
                .map_err(|e| from_datafusion(e, span))?;
        }
        if rec.is_empty() {
            return Err(shell(span, "empty mapping", "pass at least one old: new pair").into());
        }
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, df, span)?,
            None,
        ))
    }
}
