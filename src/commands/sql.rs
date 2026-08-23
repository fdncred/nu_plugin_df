use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type};

use crate::commands::category;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::session::{ThisGuard, inject_from_this};
use crate::values::{cache_and_value, dataframe_type};

pub struct Sql;

impl PluginCommand for Sql {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df sql"
    }

    fn description(&self) -> &str {
        "Run SQL against the session catalog. A piped dataframe is table `this`."
    }

    fn extra_description(&self) -> &str {
        r#"If a dataframe is piped in, it is registered as `this` for this call only (unless you already registered that name). FROM-less queries get `FROM this` appended.

Registered tables (see `df tables`) are visible to every `df sql` call in this plugin process."#
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required("query", SyntaxShape::String, "SQL query")
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::Nothing, dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[a b]; [1 2] [3 4]] | df sql "select a, b from this where a > 1" | df collect"#,
            description: "SQL filter on a piped table",
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
        let query: String = call.req(0)?;
        let guard = match input {
            PipelineData::Empty => None,
            other => {
                let df = resolve_df(plugin, other, span)?;
                Some(ThisGuard::bind(&plugin.ctx, &plugin.catalog, df, span)?)
            }
        };
        let sql = if guard.is_some() {
            inject_from_this(&query)
        } else {
            query
        };
        let result = plugin.block_on(plugin.ctx.sql(&sql), span);
        if let Some(g) = guard {
            g.unbind(&plugin.ctx);
        }
        let out = result?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, out, span)?,
            None,
        ))
    }
}
