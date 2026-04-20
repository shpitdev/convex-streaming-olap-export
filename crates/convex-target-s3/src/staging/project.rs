use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StagingProjection {
    pub component_path: String,
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StagingRow {
    pub component_path: String,
    pub table_name: String,
    pub document_id: String,
    pub timestamp: i64,
    pub schema_fingerprint: Option<String>,
    pub document: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StagingColumnKind {
    Boolean,
    Int64,
    Float64,
    Utf8,
    JsonUtf8,
}

#[derive(Debug, Clone)]
pub struct StagingColumnProjection {
    pub name: String,
    pub kind: StagingColumnKind,
}

impl StagingProjection {
    pub fn output_path(&self, root: &Path) -> PathBuf {
        let mut path = root.to_path_buf();
        if self.component_path.is_empty() {
            path.push("_root");
        } else {
            for segment in self.component_path.split('/') {
                path.push(segment);
            }
        }
        path.push(format!("{}.parquet", self.table_name));
        path
    }
}
