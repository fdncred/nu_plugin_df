use std::path::{Path, PathBuf};

use datafusion::prelude::{CsvReadOptions, DataFrame, JsonReadOptions, ParquetReadOptions};
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type};

use crate::commands::category;
use crate::error::shell;
use crate::plugin::DfPlugin;
use crate::session::register_table;
use crate::values::{cache_and_value, dataframe_type};

pub struct Open;

impl PluginCommand for Open {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df open"
    }

    fn description(&self) -> &str {
        "Lazily scan parquet, csv, ndjson, or arrow IPC. Directories and globs work for parquet."
    }

    fn extra_description(&self) -> &str {
        "Does not register a catalog table unless you pass --as. Does not collect rows."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .required("path", SyntaxShape::String, "File path, directory, or glob")
            .named(
                "type",
                SyntaxShape::String,
                "parquet, csv, tsv, ndjson, json, arrow (default: from extension)",
                Some('t'),
            )
            .named(
                "as",
                SyntaxShape::String,
                "Also register this scan as a catalog table",
                Some('a'),
            )
            .named("delimiter", SyntaxShape::String, "CSV delimiter", Some('d'))
            .switch("no-header", "CSV has no header row", None)
            .named(
                "infer-schema",
                SyntaxShape::Int,
                "CSV/JSON rows to infer schema (default 1000)",
                None,
            )
            .input_output_type(Type::Nothing, dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "df open sales.parquet | df schema",
            description: "Scan a parquet file",
            result: None,
        }]
    }

    fn run(
        &self,
        plugin: &DfPlugin,
        engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let span = call.head;
        let path: String = call.req(0)?;
        let kind = file_kind(&path, call.get_flag::<String>("type")?.as_deref(), span)?;
        let df = scan(plugin, &path, kind, call, span)?;
        if let Some(name) = call.get_flag::<String>("as")? {
            register_table(
                &plugin.ctx,
                &plugin.catalog,
                &name,
                df.clone(),
                Some(path.clone()),
                false,
                span,
            )?;
        }
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, df, span)?,
            None,
        ))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FileKind {
    Parquet,
    Csv,
    Tsv,
    Ndjson,
    Json,
    Arrow,
}

fn file_kind(
    path: &str,
    override_ty: Option<&str>,
    span: nu_protocol::Span,
) -> Result<FileKind, nu_protocol::ShellError> {
    if let Some(t) = override_ty {
        return parse_kind(t, span);
    }
    let p = Path::new(path);
    if p.is_dir() {
        return Ok(FileKind::Parquet);
    }
    let ext = p
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "parquet" | "pq" => Ok(FileKind::Parquet),
        "csv" => Ok(FileKind::Csv),
        "tsv" => Ok(FileKind::Tsv),
        "jsonl" | "ndjson" => Ok(FileKind::Ndjson),
        "json" => Ok(FileKind::Json),
        "arrow" | "ipc" | "feather" => Ok(FileKind::Arrow),
        _ if path.contains('*') => Ok(FileKind::Parquet),
        _ => Err(shell(
            span,
            "unknown file type",
            "pass --type parquet|csv|ndjson|json|arrow",
        )),
    }
}

fn parse_kind(t: &str, span: nu_protocol::Span) -> Result<FileKind, nu_protocol::ShellError> {
    match t.to_ascii_lowercase().as_str() {
        "parquet" | "pq" => Ok(FileKind::Parquet),
        "csv" => Ok(FileKind::Csv),
        "tsv" => Ok(FileKind::Tsv),
        "ndjson" | "jsonl" => Ok(FileKind::Ndjson),
        "json" => Ok(FileKind::Json),
        "arrow" | "ipc" | "feather" => Ok(FileKind::Arrow),
        other => Err(shell(
            span,
            "unknown file type",
            format!("'{other}' is not parquet/csv/tsv/ndjson/json/arrow"),
        )),
    }
}

fn scan(
    plugin: &DfPlugin,
    path: &str,
    kind: FileKind,
    call: &EvaluatedCall,
    span: nu_protocol::Span,
) -> Result<DataFrame, nu_protocol::ShellError> {
    match kind {
        FileKind::Parquet => plugin.block_on(
            plugin.ctx.read_parquet(path, ParquetReadOptions::default()),
            span,
        ),
        FileKind::Csv | FileKind::Tsv => {
            let mut opts = CsvReadOptions::new();
            if kind == FileKind::Tsv {
                opts = opts.delimiter(b'\t');
            }
            if let Some(d) = call.get_flag::<String>("delimiter")? {
                let b = d.as_bytes();
                if b.len() != 1 {
                    return Err(shell(
                        span,
                        "invalid delimiter",
                        "delimiter must be a single character",
                    ));
                }
                opts = opts.delimiter(b[0]);
            }
            if call.has_flag("no-header")? {
                opts = opts.has_header(false);
            }
            if let Some(n) = call.get_flag::<i64>("infer-schema")? {
                opts = opts.schema_infer_max_records(n.max(0) as usize);
            }
            plugin.block_on(plugin.ctx.read_csv(path, opts), span)
        }
        FileKind::Ndjson => {
            let mut opts = JsonReadOptions {
                file_extension: ".jsonl",
                ..Default::default()
            };

            if let Some(n) = call.get_flag::<i64>("infer-schema")? {
                opts.schema_infer_max_records = n.max(0) as usize;
            }
            plugin.block_on(plugin.ctx.read_json(path, opts), span)
        }
        FileKind::Json => {
            let mut opts = JsonReadOptions::default();
            if let Some(n) = call.get_flag::<i64>("infer-schema")? {
                opts.schema_infer_max_records = n.max(0) as usize;
            }
            plugin.block_on(plugin.ctx.read_json(path, opts), span)
        }
        FileKind::Arrow => {
            let path_buf = PathBuf::from(path);
            plugin.block_on(read_arrow(&plugin.ctx, &path_buf), span)
        }
    }
}

async fn read_arrow(
    ctx: &datafusion::prelude::SessionContext,
    path: &Path,
) -> Result<DataFrame, datafusion::error::DataFusionError> {
    use datafusion::arrow::ipc::reader::FileReader;
    use std::fs::File;
    let file = File::open(path)?;
    let reader = FileReader::try_new(file, None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    ctx.read_batches(batches)
}
