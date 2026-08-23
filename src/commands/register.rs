use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type};

use crate::commands::category;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::session::register_table;
use crate::values::{cache_and_value, dataframe_type};

pub struct Register;

impl PluginCommand for Register {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df register"
    }

    fn description(&self) -> &str {
        "Register the piped dataframe as a named catalog table for SQL."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required("name", SyntaxShape::String, "Table name")
            .switch(
                "force",
                "Replace an existing table with this name",
                Some('f'),
            )
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[a]; [1]] | df register nums; df sql "select * from nums" | df collect"#,
            description: "Register a table and query it",
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
        let name: String = call.req(0)?;
        let force = call.has_flag("force")?;
        let df = resolve_df(plugin, input, span)?;
        register_table(
            &plugin.ctx,
            &plugin.catalog,
            &name,
            df.clone(),
            None,
            force,
            span,
        )?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, df, span)?,
            None,
        ))
    }
}
