use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type};

use crate::commands::category;
use crate::error::from_datafusion;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_type};

pub struct Select;

impl PluginCommand for Select {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df select"
    }

    fn description(&self) -> &str {
        "Project columns or SQL expressions."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .rest(
                "columns",
                SyntaxShape::String,
                "Column names or SQL expressions (\"a + 1 as b\")",
            )
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[a b]; [1 2] [3 4]] | df select a "a + b as sum" | df collect"#,
            description: "Select a column and a SQL expression",
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
        let refs: Vec<&str> = cols.iter().map(String::as_str).collect();
        let df = resolve_df(plugin, input, span)?;
        let out = df
            .select_exprs(&refs)
            .map_err(|e| from_datafusion(e, span))?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, out, span)?,
            None,
        ))
    }
}
