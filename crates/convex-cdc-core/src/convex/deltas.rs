use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{convex::client::ConvexClient, errors::AppResult};

#[derive(Debug, Clone)]
pub struct DocumentDeltasQuery {
    pub cursor: i64,
    pub table_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentDeltasResponse {
    pub values: Vec<Value>,
    #[serde(rename = "hasMore")]
    pub has_more: bool,
    pub cursor: i64,
}

impl ConvexClient {
    pub async fn document_deltas(
        &self,
        query: &DocumentDeltasQuery,
    ) -> AppResult<DocumentDeltasResponse> {
        let mut params = vec![
            ("format".to_string(), "json".to_string()),
            ("cursor".to_string(), query.cursor.to_string()),
        ];
        if let Some(table_name) = &query.table_name {
            params.push(("tableName".to_string(), table_name.clone()));
        }

        self.get("api/document_deltas", &params).await
    }
}
