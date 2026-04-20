use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::Path,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::errors::AppResult;

const SCHEMA_SNAPSHOT_FILE: &str = "_schemas.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaCatalog {
    pub table_fingerprints: BTreeMap<String, String>,
    pub table_schemas: BTreeMap<String, Value>,
}

impl SchemaCatalog {
    pub fn from_json_schemas(payload: &Value) -> Self {
        let Some(object) = payload.as_object() else {
            return Self::default();
        };

        let table_schemas: BTreeMap<String, Value> = object
            .iter()
            .filter(|(table_name, _)| !table_name.starts_with('$'))
            .map(|(table_name, schema)| (table_name.clone(), schema.clone()))
            .collect();

        let table_fingerprints = table_schemas
            .iter()
            .map(|(table_name, schema)| (table_name.clone(), fingerprint_json(schema)))
            .collect();

        Self {
            table_fingerprints,
            table_schemas,
        }
    }

    pub fn fingerprint_for(&self, table_name: &str) -> Option<String> {
        self.table_fingerprints.get(table_name).cloned()
    }

    pub fn schema_for(&self, table_name: &str) -> Option<&Value> {
        self.table_schemas.get(table_name)
    }

    pub fn write_snapshot(&self, raw_change_log_dir: &Path) -> AppResult<()> {
        fs::create_dir_all(raw_change_log_dir)?;
        let path = raw_change_log_dir.join(SCHEMA_SNAPSHOT_FILE);
        let temp = path.with_extension("json.tmp");
        let mut file = File::create(&temp)?;
        serde_json::to_writer_pretty(&mut file, self)?;
        use std::io::Write;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, &path)?;
        sync_parent_directory(path.parent())?;
        Ok(())
    }

    pub fn read_snapshot(raw_change_log_dir: &Path) -> AppResult<Self> {
        let path = raw_change_log_dir.join(SCHEMA_SNAPSHOT_FILE);
        let file = File::open(path)?;
        Ok(serde_json::from_reader(file)?)
    }
}

fn fingerprint_json(value: &Value) -> String {
    let canonical = serde_json::to_vec(value).unwrap_or_default();
    let digest = Sha256::digest(canonical);
    hex::encode(digest)
}

#[cfg(unix)]
fn sync_parent_directory(parent: Option<&Path>) -> AppResult<()> {
    if let Some(parent) = parent {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_: Option<&Path>) -> AppResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use serde_json::json;

    use super::SchemaCatalog;

    #[test]
    fn roundtrips_schema_snapshot() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("schema-catalog-{nanos}"));
        let catalog = SchemaCatalog::from_json_schemas(&json!({
            "users": {
                "type": "object",
                "properties": {
                    "_creationTime": {"type": "number"},
                    "name": {"type": "string"},
                    "age": {"type": "number"}
                }
            }
        }));

        catalog.write_snapshot(&dir).unwrap();
        let loaded = SchemaCatalog::read_snapshot(&dir).unwrap();

        assert_eq!(
            loaded.fingerprint_for("users"),
            catalog.fingerprint_for("users")
        );
        assert!(loaded.schema_for("users").is_some());

        let _ = fs::remove_dir_all(dir);
    }
}
