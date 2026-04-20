use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub version: i64,
    pub sync_state: SyncState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "phase")]
pub enum SyncState {
    InitialSnapshot { snapshot: i64, cursor: String },
    DeltaTail { cursor: i64 },
}

impl Checkpoint {
    pub const VERSION: i64 = 1;

    pub fn initial_snapshot(snapshot: i64, cursor: String) -> Self {
        Self {
            version: Self::VERSION,
            sync_state: SyncState::InitialSnapshot { snapshot, cursor },
        }
    }

    pub fn delta_tail(cursor: i64) -> Self {
        Self {
            version: Self::VERSION,
            sync_state: SyncState::DeltaTail { cursor },
        }
    }

    pub fn phase_name(&self) -> &'static str {
        match self.sync_state {
            SyncState::InitialSnapshot { .. } => "initial_snapshot",
            SyncState::DeltaTail { .. } => "delta_tail",
        }
    }
}
