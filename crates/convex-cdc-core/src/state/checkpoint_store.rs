use std::{
    ffi::OsString,
    fs::{self, File},
    io::Write,
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

        let temp_path = temporary_checkpoint_path(&self.path);
        let mut file = File::create(&temp_path)?;
        serde_json::to_writer_pretty(&mut file, checkpoint)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);

        fs::rename(&temp_path, &self.path)?;
        sync_parent_directory(self.path.parent())?;
        Ok(())
    }
}

fn temporary_checkpoint_path(path: &Path) -> PathBuf {
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

    #[test]
    fn save_replaces_existing_checkpoint_atomically() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("checkpoint-overwrite-{nanos}.json"));

        let store = FileCheckpointStore::new(&path);
        store
            .save(&Checkpoint::initial_snapshot(1, "cursor-1".to_string()))
            .unwrap();
        store.save(&Checkpoint::delta_tail(2)).unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded, Checkpoint::delta_tail(2));
        assert!(!path.with_extension("json.tmp").exists());

        let _ = fs::remove_file(path);
    }
}
