use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Category, Example, LabeledError, PipelineData, Signature, Type, Value};

use crate::plugin::DfPlugin;

pub struct DfCmd;

impl PluginCommand for DfCmd {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df"
    }

    fn description(&self) -> &str {
        "Apache DataFusion dataframes and SQL for Nushell."
    }

    fn extra_description(&self) -> &str {
        r#"Use a subcommand. `df` by itself only prints this help.

Pipeline values are lazy DataFusion dataframes (custom type `dataframe`).
Printing a dataframe shows its schema plus the first 20 rows; it does not load the full result into Nushell.

Large files should be opened with `df open` (parquet/csv/ndjson/arrow). Small Nu tables are converted automatically when piped into a `df` command.

SQL: a piped dataframe is visible as table `this` for that `df sql` call. Register named tables with `df open --as name` or `df register`.

`df collect` copies rows into a native Nu table (default 10_000 rows; `--all` disables the cap). Prefer `df save` for large results.

Unix `df` (disk free) is `^df` while this plugin is loaded."#
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::String)
            .category(Category::Custom("dataframe".into()))
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "df --help",
            description: "Show DataFusion plugin help",
            result: None,
        }]
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["dataframe", "datafusion", "sql", "parquet"]
    }

    fn run(
        &self,
        _plugin: &DfPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        Ok(PipelineData::value(
            Value::string(engine.get_help()?, call.head),
            None,
        ))
    }
}
