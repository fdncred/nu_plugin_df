use std::future::Future;

use datafusion::prelude::SessionContext;
use nu_plugin::{EngineInterface, Plugin, PluginCommand};
use nu_protocol::{
    CustomValue, LabeledError, ShellError, Span, Spanned, Value, shell_error::generic::GenericError,
};
use tokio::runtime::Runtime;

use crate::cache::Cache;
use crate::commands;
use crate::convert::collect_to_nu;
use crate::error::from_datafusion;
use crate::session::Catalog;
use crate::values::{PREVIEW_ROWS, id_from_custom_box};

pub trait EngineWrapper {
    fn set_gc_disabled(&self, disabled: bool) -> Result<(), ShellError>;
}

impl EngineWrapper for &EngineInterface {
    fn set_gc_disabled(&self, disabled: bool) -> Result<(), ShellError> {
        EngineInterface::set_gc_disabled(self, disabled)
    }
}

pub struct DfPlugin {
    pub ctx: SessionContext,
    pub runtime: Runtime,
    pub cache: Cache,
    pub catalog: Catalog,
    pub disable_cache_drop: bool,
}

impl DfPlugin {
    pub fn new() -> Result<Self, ShellError> {
        Ok(Self {
            ctx: SessionContext::new(),
            runtime: Runtime::new().map_err(|e| {
                ShellError::Generic(GenericError::new_internal(
                    format!("could not create tokio runtime: {e}"),
                    "",
                ))
            })?,
            cache: Cache::default(),
            catalog: Catalog::default(),
            disable_cache_drop: false,
        })
    }

    pub fn new_test_mode() -> Result<Self, ShellError> {
        Ok(Self {
            disable_cache_drop: true,
            ..Self::new()?
        })
    }

    pub fn block_on<T>(
        &self,
        fut: impl Future<Output = Result<T, datafusion::error::DataFusionError>>,
        span: Span,
    ) -> Result<T, ShellError> {
        self.runtime
            .block_on(fut)
            .map_err(|e| from_datafusion(e, span))
    }
}

impl Plugin for DfPlugin {
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").into()
    }

    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        commands::all()
    }

    fn custom_value_to_base_value(
        &self,
        _engine: &EngineInterface,
        custom_value: Spanned<Box<dyn CustomValue>>,
    ) -> Result<Value, LabeledError> {
        let span = custom_value.span;
        let id = id_from_custom_box(custom_value.item.as_ref()).ok_or_else(|| {
            LabeledError::from(crate::error::shell(
                span,
                "not a dataframe",
                custom_value.item.type_name(),
            ))
        })?;
        let df = self.cache.get(&id)?.ok_or_else(|| {
            LabeledError::from(crate::error::shell(
                span,
                "stale dataframe handle",
                "this dataframe is no longer in the plugin cache",
            ))
        })?;
        let preview = df
            .limit(0, Some(PREVIEW_ROWS))
            .map_err(|e| from_datafusion(e, span))?;
        let (value, _) = collect_to_nu(self, preview, span, Some(PREVIEW_ROWS))?;
        Ok(value)
    }

    fn custom_value_dropped(
        &self,
        engine: &EngineInterface,
        custom_value: Box<dyn CustomValue>,
    ) -> Result<(), LabeledError> {
        if self.disable_cache_drop {
            return Ok(());
        }
        if let Some(id) = id_from_custom_box(custom_value.as_ref()) {
            self.cache.remove(engine, &id, false)?;
        }
        Ok(())
    }
}
