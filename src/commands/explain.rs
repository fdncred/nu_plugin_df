use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, Type};

use crate::commands::category;
use crate::convert::collect_to_nu;
use crate::error::from_datafusion;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::dataframe_type;

pub struct Explain;

impl PluginCommand for Explain {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df explain"
    }

    fn description(&self) -> &str {
        "Show the logical (and optionally physical) plan."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .switch("verbose", "Verbose plan", Some('v'))
            .switch("analyze", "Execute the plan and include metrics", Some('a'))
            .input_output_type(dataframe_type(), Type::table())
            .input_output_type(Type::table(), Type::table())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[a]; [1]] | df filter "a > 0" | df explain"#,
            description: "Explain a simple plan",
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
        let verbose = call.has_flag("verbose")?;
        let analyze = call.has_flag("analyze")?;
        let df = resolve_df(plugin, input, span)?;
        let explained = df
            .explain(verbose, analyze)
            .map_err(|e| from_datafusion(e, span))?;
        let (value, _) = collect_to_nu(plugin, explained, span, None)?;
        Ok(PipelineData::value(value, None))
    }
}
