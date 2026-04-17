use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::errors::{AppError, AppResult};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOperation {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub component_path: String,
    pub table_name: String,
    pub document_id: String,
    pub timestamp: i64,
    pub op: ChangeOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document: Option<Value>,
}

impl ChangeEvent {
    pub fn from_convex_value(value: Value, schema_fingerprint: Option<String>) -> AppResult<Self> {
        let mut object = match value {
            Value::Object(object) => object,
            _ => return Err(AppError::ExpectedJsonObject),
        };

        let component_path = take_string(&mut object, "_component")?;
        let table_name = take_string(&mut object, "_table")?;
        let document_id = take_string(&mut object, "_id")?;
        let timestamp = take_i64(&mut object, "_ts")?;
        let is_deleted = take_bool(&mut object, "_deleted").unwrap_or(false);
        strip_reserved_metadata_fields(&mut object);

        let op = if is_deleted {
            ChangeOperation::Delete
        } else {
            ChangeOperation::Upsert
        };

        let document = if matches!(op, ChangeOperation::Delete) {
            None
        } else {
            Some(Value::Object(object))
        };

        Ok(Self {
            component_path,
            table_name,
            document_id,
            timestamp,
            op,
            schema_fingerprint,
            document,
        })
    }
}

fn take_string(object: &mut Map<String, Value>, key: &'static str) -> AppResult<String> {
    let value = object.remove(key).ok_or(AppError::MissingMetadata(key))?;
    value
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or(AppError::InvalidFieldType(key))
}

fn take_i64(object: &mut Map<String, Value>, key: &'static str) -> AppResult<i64> {
    let value = object.remove(key).ok_or(AppError::MissingMetadata(key))?;
    value.as_i64().ok_or(AppError::InvalidFieldType(key))
}

fn take_bool(object: &mut Map<String, Value>, key: &'static str) -> AppResult<bool> {
    let value = object.remove(key).ok_or(AppError::MissingMetadata(key))?;
    value.as_bool().ok_or(AppError::InvalidFieldType(key))
}

fn strip_reserved_metadata_fields(object: &mut Map<String, Value>) {
    object.retain(|key, _| !key.starts_with('_') || key == "_creationTime");
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ChangeEvent, ChangeOperation};

    #[test]
    fn normalizes_upserts() {
        let event = ChangeEvent::from_convex_value(
            json!({
                "_component": "",
                "_id": "users:123",
                "_table": "users",
                "_ts": 42,
                "_index": 7,
                "_creationTime": 1700.5,
                "name": "Ada"
            }),
            Some("abc123".to_string()),
        )
        .unwrap();

        assert_eq!(event.component_path, "");
        assert_eq!(event.table_name, "users");
        assert_eq!(event.document_id, "users:123");
        assert_eq!(event.timestamp, 42);
        assert_eq!(event.op, ChangeOperation::Upsert);
        assert_eq!(event.schema_fingerprint.as_deref(), Some("abc123"));
        let document = event.document.as_ref().unwrap();
        assert_eq!(document["name"], "Ada");
        assert!(document.get("_index").is_none());
        assert_eq!(document["_creationTime"], 1700.5);
    }

    #[test]
    fn normalizes_deletes() {
        let event = ChangeEvent::from_convex_value(
            json!({
                "_component": "r2",
                "_id": "users:123",
                "_table": "users",
                "_ts": 42,
                "_deleted": true
            }),
            None,
        )
        .unwrap();

        assert_eq!(event.component_path, "r2");
        assert_eq!(event.op, ChangeOperation::Delete);
        assert!(event.document.is_none());
    }
}
