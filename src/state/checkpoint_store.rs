use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use crate::{
    errors::{AppError, AppResult},
    model::checkpoint::Checkpoint,
};

pub trait CheckpointStore {
    fn load(&self) -> AppResult<Option<Checkpoint>>;
    fn save(&self, checkpoint: &Checkpoint) -> AppResult<()>;
}

#[derive(Debug, Clone)]
pub struct FileCheckpointStore {
    path: PathBuf,
}

impl FileCheckpointStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl CheckpointStore for FileCheckpointStore {
    fn load(&self) -> AppResult<Option<Checkpoint>> {
        if !self.path.exists() {
            return Ok(None);
        }

        let file = File::open(&self.path)?;
        let checkpoint: Checkpoint = serde_json::from_reader(file)?;
        if checkpoint.version != Checkpoint::VERSION {
            return Err(AppError::UnsupportedCheckpointVersion(checkpoint.version));
        }
        Ok(Some(checkpoint))
    }

    fn save(&self, checkpoint: &Checkpoint) -> AppResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(&self.path)?;
        serde_json::to_writer_pretty(file, checkpoint)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{CheckpointStore, FileCheckpointStore};
    use crate::model::checkpoint::Checkpoint;

    #[test]
    fn saves_and_loads_checkpoint() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("checkpoint-{nanos}.json"));

        let store = FileCheckpointStore::new(&path);
        let checkpoint = Checkpoint::initial_snapshot(456, "cursor-123".to_string());
        store.save(&checkpoint).unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded, checkpoint);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn rejects_unknown_checkpoint_version() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("checkpoint-version-{nanos}.json"));
        fs::write(
            &path,
            r#"{"version":999,"sync_state":{"phase":"delta_tail","cursor":1}}"#,
        )
        .unwrap();

        let store = FileCheckpointStore::new(&path);
        let err = store.load().unwrap_err();
        assert!(err.to_string().contains("unsupported checkpoint version"));

        let _ = fs::remove_file(path);
    }
}
