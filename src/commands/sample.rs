use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use futures::StreamExt;
use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{Example, LabeledError, PipelineData, Signature, SyntaxShape, Type};
use rand::{RngExt, seq::SliceRandom};

use crate::commands::category;
use crate::convert::dataframe_from_batches;
use crate::error::shell;
use crate::input::resolve_df;
use crate::plugin::DfPlugin;
use crate::values::{cache_and_value, dataframe_type};

pub struct Sample;

impl PluginCommand for Sample {
    type Plugin = DfPlugin;

    fn name(&self) -> &str {
        "df sample"
    }

    fn description(&self) -> &str {
        "Reservoir-sample n rows from a dataframe (one pass over the plan)."
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .named(
                "n",
                SyntaxShape::Int,
                "Number of rows to sample (default 10)",
                Some('n'),
            )
            .input_output_type(dataframe_type(), dataframe_type())
            .input_output_type(Type::table(), dataframe_type())
            .category(category())
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: "[[a]; [1] [2] [3] [4]] | df sample --n 2 | df count",
            description: "Sample two rows",
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
        let n: usize = call.get_flag::<i64>("n")?.unwrap_or(10).max(0) as usize;
        if n == 0 {
            return Err(shell(span, "invalid sample size", "--n must be > 0").into());
        }
        let df = resolve_df(plugin, input, span)?;
        let sampled = plugin.block_on(sample_stream(df, n), span)?;
        let out = dataframe_from_batches(&plugin.ctx, sampled, span)?;
        Ok(PipelineData::value(
            cache_and_value(plugin, engine, out, span)?,
            None,
        ))
    }
}

async fn sample_stream(
    df: datafusion::prelude::DataFrame,
    n: usize,
) -> Result<Vec<RecordBatch>, datafusion::error::DataFusionError> {
    let mut stream = df.execute_stream().await?;
    let mut picked: Vec<(RecordBatch, usize)> = Vec::new();
    let mut seen: usize = 0;
    let mut rng = rand::rng();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            seen += 1;
            if picked.len() < n {
                picked.push((batch.slice(row, 1), seen));
            } else {
                let j = rng.random_range(0..seen);
                if j < n {
                    picked[j] = (batch.slice(row, 1), seen);
                }
            }
        }
    }
    if picked.is_empty() {
        return Ok(vec![]);
    }
    picked.shuffle(&mut rng);
    let schema = picked[0].0.schema();
    let batches: Vec<RecordBatch> = picked.into_iter().map(|(b, _)| b).collect();
    let concatenated = concat_batches(&schema, &batches)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(vec![concatenated])
}
