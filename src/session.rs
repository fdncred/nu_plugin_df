use std::collections::BTreeMap;
use std::sync::Mutex;

use datafusion::prelude::{DataFrame, SessionContext};
use nu_protocol::{ShellError, Span, shell_error::generic::GenericError};

use crate::error::{from_datafusion, shell};

const THIS_TABLE: &str = "this";

#[derive(Default)]
pub struct Catalog {
    /// User-registered names (not ephemeral `this`).
    names: Mutex<BTreeMap<String, Option<String>>>,
}

impl Catalog {
    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, BTreeMap<String, Option<String>>>, ShellError> {
        self.names.lock().map_err(|e| {
            ShellError::Generic(GenericError::new_internal(
                format!("error acquiring catalog lock: {e}"),
                "",
            ))
        })
    }

    pub fn list(&self) -> Result<Vec<(String, Option<String>)>, ShellError> {
        let lock = self.lock()?;
        Ok(lock.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    pub fn contains(&self, name: &str) -> Result<bool, ShellError> {
        Ok(self.lock()?.contains_key(name))
    }

    pub fn insert(&self, name: String, source: Option<String>) -> Result<(), ShellError> {
        self.lock()?.insert(name, source);
        Ok(())
    }

    pub fn remove(&self, name: &str) -> Result<bool, ShellError> {
        Ok(self.lock()?.remove(name).is_some())
    }
}

pub fn register_table(
    ctx: &SessionContext,
    catalog: &Catalog,
    name: &str,
    df: DataFrame,
    source: Option<String>,
    force: bool,
    span: Span,
) -> Result<(), ShellError> {
    if !force && catalog.contains(name)? {
        return Err(shell(
            span,
            "table already registered",
            format!("a table named '{name}' is already in the catalog; use --force to replace it"),
        ));
    }
    if ctx.table_exist(name).unwrap_or(false) {
        let _ = ctx.deregister_table(name);
    }
    ctx.register_table(name, df.into_view())
        .map_err(|e| from_datafusion(e, span))?;
    catalog.insert(name.to_string(), source)?;
    Ok(())
}

pub fn unregister_table(
    ctx: &SessionContext,
    catalog: &Catalog,
    name: &str,
    span: Span,
) -> Result<(), ShellError> {
    if !catalog.contains(name)? {
        return Err(shell(
            span,
            "table not found",
            format!("no table named '{name}' is registered"),
        ));
    }
    ctx.deregister_table(name)
        .map_err(|e| from_datafusion(e, span))?;
    catalog.remove(name)?;
    Ok(())
}

/// Register `this` for one SQL call. Restores any previous user table named `this`.
pub struct ThisGuard {
    had_user_this: bool,
}

impl ThisGuard {
    pub fn bind(
        ctx: &SessionContext,
        catalog: &Catalog,
        df: DataFrame,
        span: Span,
    ) -> Result<Self, ShellError> {
        let had_user_this = catalog.contains(THIS_TABLE)?;
        if ctx.table_exist(THIS_TABLE).unwrap_or(false) {
            let _ = ctx.deregister_table(THIS_TABLE);
        }
        ctx.register_table(THIS_TABLE, df.into_view())
            .map_err(|e| from_datafusion(e, span))?;
        Ok(Self { had_user_this })
    }

    pub fn unbind(self, ctx: &SessionContext) {
        if !self.had_user_this {
            let _ = ctx.deregister_table(THIS_TABLE);
        }
    }
}

pub fn inject_from_this(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    if has_from_clause(&lower) {
        sql.to_string()
    } else {
        format!("{sql} FROM {THIS_TABLE}")
    }
}

fn has_from_clause(sql_lower: &str) -> bool {
    sql_lower.split_whitespace().any(|w| w == "from")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injects_from_this_when_missing() {
        assert_eq!(inject_from_this("select a, b"), "select a, b FROM this");
    }

    #[test]
    fn leaves_from_this_alone() {
        let q = "select a from this where a > 1";
        assert_eq!(inject_from_this(q), q);
    }
}
