use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type};

use crate::commands::category;
use crate::error::{from_datafusion, shell};
use crate::expr::parse_sql_expr;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_type};

pub struct WithColumn;

impl PluginCommand for WithColumn {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df with-column"
    }

    fn description(&self) -> &str {
        "Add or replace a column using a SQL expression."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required(
                "expr",
                SyntaxShape::String,
                "SQL expression, optionally `expr as name`",
            )
            .named(
                "as",
                SyntaxShape::String,
                "Column name if the expression has no alias",
                Some('a'),
            )
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[a b]; [1 2]] | df with-column "a + b" --as sum | df collect"#,
            description: "Add a computed column",
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
        let expr_sql: String = call.req(0)?;
        let df = resolve_df(plugin, input, span)?;
        let (name, fragment) = split_as(&expr_sql, call.get_flag::<String>("as")?, span)?;
        let expr = parse_sql_expr(&df, &fragment, span)?;
        let out = df
            .with_column(&name, expr)
            .map_err(|e| from_datafusion(e, span))?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, out, span)?,
            None,
        ))
    }
}

fn split_as(
    expr_sql: &str,
    flag_name: Option<String>,
    span: nu_protocol::Span,
) -> Result<(String, String), nu_protocol::ShellError> {
    if let Some(name) = flag_name {
        return Ok((name, expr_sql.to_string()));
    }
    let lower = expr_sql.to_ascii_lowercase();
    if let Some(idx) = lower.rfind(" as ") {
        let fragment = expr_sql[..idx].trim().to_string();
        let name = expr_sql[idx + 4..].trim().to_string();
        if name.is_empty() || fragment.is_empty() {
            return Err(shell(
                span,
                "invalid with-column expression",
                "use `expr as name` or `--as name`",
            ));
        }
        return Ok((name, fragment));
    }
    Err(shell(
        span,
        "missing column name",
        "pass --as <name> or write `expr as name`",
    ))
}
