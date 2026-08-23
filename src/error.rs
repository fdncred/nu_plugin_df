use datafusion::error::DataFusionError;
use nu_protocol::{ShellError, Span, shell_error::generic::GenericError};

pub fn shell(span: Span, error: impl Into<String>, msg: impl Into<String>) -> ShellError {
    ShellError::Generic(GenericError::new(error.into(), msg.into(), span))
}

pub fn from_datafusion(err: DataFusionError, span: Span) -> ShellError {
    shell(span, "DataFusion error", err.to_string())
}
