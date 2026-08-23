use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type};

use crate::commands::category;
use crate::error::from_datafusion;
use crate::expr::parse_sql_expr;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_type};

pub struct Filter;

impl PluginCommand for Filter {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df filter"
    }

    fn description(&self) -> &str {
        "Keep rows matching a SQL predicate."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required(
                "predicate",
                SyntaxShape::String,
                "SQL boolean expression, e.g. \"amount > 1000\"",
            )
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[a b]; [1 2] [3 4]] | df filter "a > 1" | df collect"#,
            description: "Filter rows with a SQL predicate",
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
        let predicate: String = call.req(0)?;
        let df = resolve_df(plugin, input, span)?;
        let expr = parse_sql_expr(&df, &predicate, span)?;
        let out = df.filter(expr).map_err(|e| from_datafusion(e, span))?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, out, span)?,
            None,
        ))
    }
}
