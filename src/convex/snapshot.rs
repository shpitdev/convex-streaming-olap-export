use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{convex::client::ConvexClient, errors::AppResult};

#[derive(Debug, Clone, Default)]
pub struct ListSnapshotQuery {
    pub snapshot: Option<i64>,
    pub cursor: Option<String>,
    pub table_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListSnapshotResponse {
    pub values: Vec<Value>,
    #[serde(rename = "hasMore")]
    pub has_more: bool,
    pub snapshot: i64,
    pub cursor: Option<String>,
}

impl ConvexClient {
    pub async fn list_snapshot(
        &self,
        query: &ListSnapshotQuery,
    ) -> AppResult<ListSnapshotResponse> {
        let mut params = vec![("format".to_string(), "json".to_string())];
        if let Some(snapshot) = query.snapshot {
            params.push(("snapshot".to_string(), snapshot.to_string()));
        }
        if let Some(cursor) = &query.cursor {
            params.push(("cursor".to_string(), cursor.clone()));
        }
        if let Some(table_name) = &query.table_name {
            params.push(("tableName".to_string(), table_name.clone()));
        }

        self.get("api/list_snapshot", &params).await
    }
}
