use nu_protocol::{CustomValue, ShellError, Span, Type, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::TYPE_NAME;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NuDataFrameCustomValue {
    pub id: Uuid,
}

pub fn dataframe_type() -> Type {
    Type::Custom(TYPE_NAME.into())
}

#[typetag::serde]
impl CustomValue for NuDataFrameCustomValue {
    fn clone_value(&self, span: Span) -> Value {
        Value::custom(Box::new(self.clone()), span)
    }

    fn type_name(&self) -> String {
        TYPE_NAME.to_string()
    }

    fn to_base_value(&self, span: Span) -> Result<Value, ShellError> {
        Ok(Value::string(
            "dataframe: custom_value_to_base_value should have been called on the plugin",
            span,
        ))
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn notify_plugin_on_drop(&self) -> bool {
        true
    }
}
