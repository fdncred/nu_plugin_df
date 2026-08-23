use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Example, LabeledError, PipelineData, PipelineMetadata, Record, Signature, SyntaxShape, Type,
    Value,
};

use crate::commands::category;
use crate::convert::collect_to_nu;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{COLLECT_DEFAULT, dataframe_type};

pub struct Collect;

impl PluginCommand for Collect {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df collect"
    }

    fn description(&self) -> &str {
        "Materialize a dataframe into a Nushell table."
    }

    fn extra_description(&self) -> &str {
        "Default cap is 10_000 rows. Use --n to set a cap, or --all to disable it. Printing a dataframe already shows a 20-row preview; collect only when you need native Nu values."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .named(
                "n",
                SyntaxShape::Int,
                "Maximum rows to collect (default 10000)",
                Some('n'),
            )
            .switch(
                "all",
                "Collect every row (can lock up the shell)",
                Some('a'),
            )
            .input_output_type(dataframe_type(), Type::table())
            .input_output_type(Type::table(), Type::table())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "[[a b]; [1 2] [3 4]] | df collect",
            description: "Collect a small table through DataFusion",
            result: None,
        }]
    }

    fn run(
        &self,
        plugin: &DfPlugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let span = call.head;
        let df = resolve_df(plugin, input, span)?;
        let cap = if call.has_flag("all")? {
            None
        } else {
            Some(call.get_flag::<i64>("n")?.unwrap_or(COLLECT_DEFAULT as i64) as usize)
        };
        let (value, truncated) = collect_to_nu(plugin, df, span, cap)?;
        let metadata = if truncated {
            let mut rec = Record::new();
            rec.push("truncated", Value::bool(true, span));
            rec.push("cap", Value::int(cap.unwrap_or(0) as i64, span));
            Some(PipelineMetadata {
                custom: rec,
                ..PipelineMetadata::default()
            })
        } else {
            None
        };
        Ok(PipelineData::value(value, metadata))
    }
}
