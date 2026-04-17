use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaCatalog {
    pub table_fingerprints: BTreeMap<String, String>,
}

impl SchemaCatalog {
    pub fn from_json_schemas(payload: &Value) -> Self {
        let Some(object) = payload.as_object() else {
            return Self::default();
        };

        let table_fingerprints = object
            .iter()
            .filter(|(table_name, _)| !table_name.starts_with('$'))
            .map(|(table_name, schema)| (table_name.clone(), fingerprint_json(schema)))
            .collect();

        Self { table_fingerprints }
    }

    pub fn fingerprint_for(&self, table_name: &str) -> Option<String> {
        self.table_fingerprints.get(table_name).cloned()
    }
}

fn fingerprint_json(value: &Value) -> String {
    let canonical = serde_json::to_vec(value).unwrap_or_default();
    let digest = Sha256::digest(canonical);
    hex::encode(digest)
}
