use std::{
    collections::BTreeSet,
    ffi::OsString,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use convex_cdc_core::errors::{AppError, AppResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StagingState {
    pub version: i64,
    pub schema_snapshot_hash: Option<String>,
    pub processed_raw_files: BTreeSet<String>,
}

impl StagingState {
    pub const VERSION: i64 = 1;

    pub fn new(
        schema_snapshot_hash: Option<String>,
        processed_raw_files: BTreeSet<String>,
    ) -> Self {
        Self {
            version: Self::VERSION,
            schema_snapshot_hash,
            processed_raw_files,
        }
    }
}

pub struct FileStagingStateStore {
    path: PathBuf,
}

impl FileStagingStateStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> AppResult<Option<StagingState>> {
        if !self.path.exists() {
            return Ok(None);
        }

        let file = File::open(&self.path)?;
        let state: StagingState = serde_json::from_reader(file)?;
        if state.version != StagingState::VERSION {
            return Err(AppError::UnsupportedCheckpointVersion(state.version));
        }
        Ok(Some(state))
    }

    pub fn save(&self, state: &StagingState) -> AppResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp = temporary_state_path(&self.path);
        let mut file = File::create(&temp)?;
        serde_json::to_writer_pretty(&mut file, state)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);

        fs::rename(&temp, &self.path)?;
        sync_parent_directory(self.path.parent())?;
        Ok(())
    }
}

pub fn schema_snapshot_hash(raw_change_log_dir: &Path) -> AppResult<Option<String>> {
    let schema_path = raw_change_log_dir.join("_schemas.json");
    if !schema_path.exists() {
        return Ok(None);
    }

    let bytes = fs::read(schema_path)?;
    Ok(Some(hex::encode(Sha256::digest(bytes))))
}

fn temporary_state_path(path: &Path) -> PathBuf {
    let mut temp_name: OsString = path.as_os_str().to_owned();
    temp_name.push(".tmp");
    PathBuf::from(temp_name)
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
        collections::BTreeSet,
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{schema_snapshot_hash, FileStagingStateStore, StagingState};

    #[test]
    fn saves_and_loads_staging_state() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("staging-state-{nanos}.json"));
        let store = FileStagingStateStore::new(&path);
        let state = StagingState::new(
            Some("abc".to_string()),
            BTreeSet::from(["a.parquet".to_string(), "b.parquet".to_string()]),
        );

        store.save(&state).unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded, state);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn hashes_schema_snapshot_file() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("schema-hash-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("_schemas.json"), "{}\n").unwrap();

        let hash = schema_snapshot_hash(&dir).unwrap();
        assert!(hash.is_some());

        let _ = fs::remove_dir_all(dir);
    }
}
