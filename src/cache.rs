use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use datafusion::prelude::DataFrame;
use nu_protocol::{LabeledError, ShellError, Span, shell_error::generic::GenericError};
use uuid::Uuid;

use crate::plugin::EngineWrapper;

pub struct CacheEntry {
    pub df: DataFrame,
    pub reference_count: i16,
}

#[derive(Default)]
pub struct Cache {
    inner: Mutex<HashMap<Uuid, CacheEntry>>,
}

impl Cache {
    fn lock(&self) -> Result<MutexGuard<'_, HashMap<Uuid, CacheEntry>>, ShellError> {
        self.inner.lock().map_err(|e| {
            ShellError::Generic(GenericError::new_internal(
                format!("error acquiring cache lock: {e}"),
                "",
            ))
        })
    }

    pub fn insert(
        &self,
        engine: impl EngineWrapper,
        id: Uuid,
        df: DataFrame,
        _span: Span,
    ) -> Result<(), ShellError> {
        let mut lock = self.lock()?;
        engine.set_gc_disabled(true).map_err(LabeledError::from)?;
        lock.insert(
            id,
            CacheEntry {
                df,
                reference_count: 1,
            },
        );
        Ok(())
    }

    pub fn get(&self, id: &Uuid) -> Result<Option<DataFrame>, ShellError> {
        let lock = self.lock()?;
        Ok(lock.get(id).map(|entry| entry.df.clone()))
    }

    pub fn remove(
        &self,
        engine: impl EngineWrapper,
        id: &Uuid,
        force: bool,
    ) -> Result<(), ShellError> {
        let mut lock = self.lock()?;
        let remaining = lock.get_mut(id).map(|entry| {
            entry.reference_count -= 1;
            entry.reference_count
        });
        if force || remaining.unwrap_or_default() < 1 {
            lock.remove(id);
        }
        if lock.is_empty() {
            engine.set_gc_disabled(false).map_err(LabeledError::from)?;
        }
        Ok(())
    }
}
