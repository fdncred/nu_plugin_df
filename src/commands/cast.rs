use datafusion::logical_expr::{Expr, cast, col};
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type};

use crate::commands::category;
use crate::convert::parse_arrow_type;
use crate::error::{from_datafusion, shell};
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_type};

pub struct Cast;

impl PluginCommand for Cast {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df cast"
    }

    fn description(&self) -> &str {
        "Cast a column to an Arrow type (int64, utf8, float64, boolean, timestamp, ...)."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required("column", SyntaxShape::String, "Column to cast")
            .required("type", SyntaxShape::String, "Arrow type name")
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[a]; [1]] | df cast a utf8 | df schema"#,
            description: "Cast a column to utf8",
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
        let column: String = call.req(0)?;
        let type_name: String = call.req(1)?;
        let dt = parse_arrow_type(&type_name).ok_or_else(|| {
            shell(
                span,
                "unknown type",
                format!("'{type_name}' is not a supported Arrow type name"),
            )
        })?;
        let df = resolve_df(plugin, input, span)?;
        let expr: Expr = cast(col(&column), dt);
        let out = df
            .with_column(&column, expr)
            .map_err(|e| from_datafusion(e, span))?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, out, span)?,
            None,
        ))
    }
}
