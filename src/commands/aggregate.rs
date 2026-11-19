use datafusion::prelude::col;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type};

use crate::commands::category;
use crate::error::from_datafusion;
use crate::expr::parse_sql_expr;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_type};

pub struct Aggregate;

impl PluginCommand for Aggregate {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df aggregate"
    }

    fn description(&self) -> &str {
        "Group by columns and compute SQL aggregate expressions."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .named(
                "group",
                SyntaxShape::List(Box::new(SyntaxShape::String)),
                "Group-by column names",
                Some('g'),
            )
            .named(
                "agg",
                SyntaxShape::List(Box::new(SyntaxShape::String)),
                "SQL aggregates, e.g. [\"sum(amount) as total\" \"count(*) as n\"]",
                Some('a'),
            )
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[g v]; [a 1] [a 2] [b 3]] | df aggregate --group [g] --agg ["sum(v) as total"] | df collect"#,
            description: "Sum v grouped by g",
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
        let groups: Vec<String> = call.get_flag("group")?.unwrap_or_default();
        let aggs: Vec<String> = call.get_flag("agg")?.unwrap_or_default();
        let df = resolve_df(plugin, input, span)?;
        let group_expr = groups.iter().map(col).collect();
        let mut agg_expr = Vec::new();
        for sql in &aggs {
            agg_expr.push(parse_sql_expr(&df, sql, span)?);
        }
        let out = df
            .aggregate(group_expr, agg_expr)
            .map_err(|e| from_datafusion(e, span))?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, out, span)?,
            None,
        ))
    }
}
