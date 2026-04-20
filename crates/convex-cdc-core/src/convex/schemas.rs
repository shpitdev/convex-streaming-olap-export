use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{convex::client::ConvexClient, errors::AppResult};

#[derive(Debug, Clone, Default)]
pub struct JsonSchemasQuery {
    pub delta_schema: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JsonSchemasResponse {
    pub payload: Value,
}

impl ConvexClient {
    pub async fn json_schemas(&self, query: &JsonSchemasQuery) -> AppResult<JsonSchemasResponse> {
        let mut params = vec![("format".to_string(), "json".to_string())];
        if query.delta_schema {
            params.push(("deltaSchema".to_string(), "true".to_string()));
        }

        self.get("api/json_schemas", &params).await
    }
}
