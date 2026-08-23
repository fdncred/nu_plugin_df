use datafusion::prelude::JoinType;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type, Value};

use crate::commands::category;
use crate::error::{from_datafusion, shell};
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_from_custom, dataframe_type};

pub struct Join;

impl PluginCommand for Join {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df join"
    }

    fn description(&self) -> &str {
        "Join the piped dataframe with another handle or catalog table."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required(
                "right",
                SyntaxShape::Any,
                "Right-hand dataframe handle or catalog table name",
            )
            .named(
                "on",
                SyntaxShape::List(Box::new(SyntaxShape::String)),
                "Join key(s) shared by both sides",
                Some('o'),
            )
            .named(
                "left-on",
                SyntaxShape::List(Box::new(SyntaxShape::String)),
                "Left join key(s)",
                None,
            )
            .named(
                "right-on",
                SyntaxShape::List(Box::new(SyntaxShape::String)),
                "Right join key(s)",
                None,
            )
            .named(
                "type",
                SyntaxShape::String,
                "inner, left, right, full, semi, anti (default inner)",
                Some('t'),
            )
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"let r = ([[id n]; [1 x]] | df from-nu); [[id v]; [1 10]] | df join $r --on [id] | df collect"#,
            description: "Inner join two dataframes on id",
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
        let left = resolve_df(plugin, input, span)?;
        let right_val: Value = call.req(0)?;
        let right = match &right_val {
            Value::Custom { .. } => dataframe_from_custom(&plugin.cache, &right_val, span)?,
            Value::String { val, .. } => plugin.block_on(plugin.ctx.table(val.as_str()), span)?,
            _ => {
                return Err(shell(
                    span,
                    "invalid right side",
                    "expected a dataframe handle or catalog table name",
                )
                .into());
            }
        };
        let join_type = parse_join_type(call.get_flag::<String>("type")?.as_deref(), span)?;
        let (left_on, right_on) = join_keys(call, span)?;
        let left_refs: Vec<&str> = left_on.iter().map(String::as_str).collect();
        let right_refs: Vec<&str> = right_on.iter().map(String::as_str).collect();
        let out = left
            .join(right, join_type, &left_refs, &right_refs, None)
            .map_err(|e| from_datafusion(e, span))?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, out, span)?,
            None,
        ))
    }
}

fn parse_join_type(
    name: Option<&str>,
    span: nu_protocol::Span,
) -> Result<JoinType, nu_protocol::ShellError> {
    Ok(
        match name.unwrap_or("inner").to_ascii_lowercase().as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" | "outer" => JoinType::Full,
            "semi" | "leftsemi" | "left_semi" => JoinType::LeftSemi,
            "anti" | "leftanti" | "left_anti" => JoinType::LeftAnti,
            other => {
                return Err(shell(
                    span,
                    "unknown join type",
                    format!("'{other}' is not inner/left/right/full/semi/anti"),
                ));
            }
        },
    )
}

fn join_keys(
    call: &EvaluatedCall,
    span: nu_protocol::Span,
) -> Result<(Vec<String>, Vec<String>), nu_protocol::ShellError> {
    let on: Option<Vec<String>> = call.get_flag("on")?;
    let left_on: Option<Vec<String>> = call.get_flag("left-on")?;
    let right_on: Option<Vec<String>> = call.get_flag("right-on")?;
    match (on, left_on, right_on) {
        (Some(on), None, None) => Ok((on.clone(), on)),
        (None, Some(l), Some(r)) if l.len() == r.len() => Ok((l, r)),
        (None, Some(_), Some(_)) => Err(shell(
            span,
            "join key mismatch",
            "--left-on and --right-on must have the same number of columns",
        )),
        _ => Err(shell(
            span,
            "missing join keys",
            "pass --on [col ...] or both --left-on and --right-on",
        )),
    }
}
