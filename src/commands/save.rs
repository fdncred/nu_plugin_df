use std::fs::File;
use std::path::Path;

use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::DataFrame;
use futures::StreamExt;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type, Value};

use crate::commands::category;
use crate::error::shell;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::dataframe_type;

pub struct Save;

impl PluginCommand for Save {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df save"
    }

    fn description(&self) -> &str {
        "Write a dataframe to parquet, csv, ndjson, or arrow IPC. Streams; not capped."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required("path", SyntaxShape::String, "Output path")
            .named(
                "type",
                SyntaxShape::String,
                "parquet, csv, ndjson, arrow (default: from extension)",
                Some('t'),
            )
            .switch("force", "Overwrite an existing file", Some('f'))
            .input_output_type(dataframe_type(), Type::Nothing)
            .input_output_type(Type::table(), Type::Nothing)
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "[[a]; [1]] | df save out.parquet",
            description: "Save a dataframe as parquet",
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
        let path: String = call.req(0)?;
        let force = call.has_flag("force")?;
        if Path::new(&path).exists() && !force {
            return Err(shell(
                span,
                "file exists",
                format!("'{path}' already exists; pass --force to overwrite"),
            )
            .into());
        }
        let kind = infer_kind(&path, call.get_flag::<String>("type")?.as_deref(), span)?;
        let df = resolve_df(plugin, input, span)?;
        match kind {
            OutKind::Parquet => {
                plugin.block_on(
                    df.write_parquet(&path, DataFrameWriteOptions::new(), None),
                    span,
                )?;
            }
            OutKind::Csv => {
                plugin.block_on(
                    df.write_csv(&path, DataFrameWriteOptions::new(), None),
                    span,
                )?;
            }
            OutKind::Ndjson => {
                plugin.block_on(
                    df.write_json(&path, DataFrameWriteOptions::new(), None),
                    span,
                )?;
            }
            OutKind::Arrow => {
                plugin.block_on(write_arrow(df, &path), span)?;
            }
        }
        Ok(PipelineData::value(Value::nothing(span), None))
    }
}

enum OutKind {
    Parquet,
    Csv,
    Ndjson,
    Arrow,
}

fn infer_kind(
    path: &str,
    override_ty: Option<&str>,
    span: nu_protocol::Span,
) -> Result<OutKind, nu_protocol::ShellError> {
    let t = override_ty.map(|s| s.to_ascii_lowercase()).or_else(|| {
        Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_ascii_lowercase())
    });
    match t.as_deref() {
        Some("parquet" | "pq") => Ok(OutKind::Parquet),
        Some("csv" | "tsv") => Ok(OutKind::Csv),
        Some("ndjson" | "jsonl" | "json") => Ok(OutKind::Ndjson),
        Some("arrow" | "ipc" | "feather") => Ok(OutKind::Arrow),
        _ => Err(shell(
            span,
            "unknown file type",
            "pass --type parquet|csv|ndjson|arrow",
        )),
    }
}

async fn write_arrow(df: DataFrame, path: &str) -> Result<(), datafusion::error::DataFusionError> {
    let mut stream = df.execute_stream().await?;
    let mut writer: Option<FileWriter<File>> = None;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if writer.is_none() {
            let file = File::create(path)?;
            writer = Some(FileWriter::try_new(file, &batch.schema())?);
        }
        writer
            .as_mut()
            .expect("writer")
            .write(&batch)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    }
    if let Some(mut w) = writer {
        w.finish()
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    }
    Ok(())
}
