use std::path::PathBuf;

use serde::Serialize;

use crate::{
    convex::client::ConvexClient,
    errors::{AppError, AppResult},
    model::{
        checkpoint::{Checkpoint, SyncState},
        schema::SchemaCatalog,
    },
    sink::parquet::write_change_events_batch,
    state::checkpoint_store::CheckpointStore,
    sync::{
        delta_sync::{fetch_delta_events, DeltaSyncOptions},
        snapshot_sync::{fetch_snapshot_events, SnapshotSyncOptions},
    },
};

#[derive(Clone)]
pub struct ExportRunner {
    client: ConvexClient,
    schemas: SchemaCatalog,
}

impl ExportRunner {
    pub fn new(client: ConvexClient, schemas: SchemaCatalog) -> Self {
        Self { client, schemas }
    }

    pub fn client(&self) -> &ConvexClient {
        &self.client
    }

    pub fn schemas(&self) -> &SchemaCatalog {
        &self.schemas
    }
}

#[derive(Debug, Clone)]
pub struct SyncOnceOptions {
    pub raw_change_log_path: PathBuf,
    pub checkpoint_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncOnceSummary {
    pub raw_change_log_path: PathBuf,
    pub checkpoint_path: PathBuf,
    pub snapshot_pages_fetched: usize,
    pub delta_pages_fetched: usize,
    pub events_written: usize,
    pub final_checkpoint: Checkpoint,
}

impl ExportRunner {
    pub async fn sync_once(
        &self,
        checkpoint_store: &impl CheckpointStore,
        options: &SyncOnceOptions,
    ) -> AppResult<SyncOnceSummary> {
        let mut snapshot_pages_fetched = 0usize;
        let mut delta_pages_fetched = 0usize;
        let mut events_written = 0usize;

        let mut checkpoint = checkpoint_store.load()?;
        let delta_cursor = match checkpoint.as_ref().map(|checkpoint| &checkpoint.sync_state) {
            None => {
                self.run_snapshot_until_delta(
                    checkpoint_store,
                    options,
                    &mut snapshot_pages_fetched,
                    &mut events_written,
                    None,
                )
                .await?
            },
            Some(SyncState::InitialSnapshot { snapshot, cursor }) => {
                self.run_snapshot_until_delta(
                    checkpoint_store,
                    options,
                    &mut snapshot_pages_fetched,
                    &mut events_written,
                    Some((*snapshot, cursor.clone())),
                )
                .await?
            },
            Some(SyncState::DeltaTail { cursor }) => *cursor,
        };

        let final_checkpoint = self
            .run_delta_sync(
                checkpoint_store,
                options,
                &mut delta_pages_fetched,
                &mut events_written,
                delta_cursor,
            )
            .await?;
        checkpoint = Some(final_checkpoint);

        Ok(SyncOnceSummary {
            raw_change_log_path: options.raw_change_log_path.clone(),
            checkpoint_path: options.checkpoint_path.clone(),
            snapshot_pages_fetched,
            delta_pages_fetched,
            events_written,
            final_checkpoint: checkpoint.expect("sync_once always leaves a checkpoint"),
        })
    }

    async fn run_snapshot_until_delta(
        &self,
        checkpoint_store: &impl CheckpointStore,
        options: &SyncOnceOptions,
        snapshot_pages_fetched: &mut usize,
        events_written: &mut usize,
        initial_state: Option<(i64, String)>,
    ) -> AppResult<i64> {
        let mut snapshot = initial_state.as_ref().map(|(snapshot, _)| *snapshot);
        let mut cursor = initial_state.map(|(_, cursor)| cursor);

        loop {
            let result = fetch_snapshot_events(
                &self.client,
                &self.schemas,
                &SnapshotSyncOptions {
                    table_name: None,
                    snapshot,
                    cursor: cursor.clone(),
                    max_pages: 1,
                },
            )
            .await?;

            if result.has_more {
                let next_cursor = result.cursor.ok_or(AppError::MissingSnapshotCursor)?;
                let checkpoint = Checkpoint::initial_snapshot(result.snapshot, next_cursor.clone());
                write_change_events_batch(
                    &options.raw_change_log_path,
                    &checkpoint,
                    &result.events,
                )?;
                *events_written += result.events.len();
                *snapshot_pages_fetched += result.pages_fetched;
                checkpoint_store.save(&checkpoint)?;
                snapshot = Some(result.snapshot);
                cursor = Some(next_cursor);
                continue;
            }

            let checkpoint = Checkpoint::delta_tail(result.snapshot);
            write_change_events_batch(&options.raw_change_log_path, &checkpoint, &result.events)?;
            *events_written += result.events.len();
            *snapshot_pages_fetched += result.pages_fetched;
            checkpoint_store.save(&checkpoint)?;
            return Ok(result.snapshot);
        }
    }

    async fn run_delta_sync(
        &self,
        checkpoint_store: &impl CheckpointStore,
        options: &SyncOnceOptions,
        delta_pages_fetched: &mut usize,
        events_written: &mut usize,
        mut cursor: i64,
    ) -> AppResult<Checkpoint> {
        loop {
            let result = fetch_delta_events(
                &self.client,
                &self.schemas,
                &DeltaSyncOptions {
                    cursor,
                    table_name: None,
                    max_pages: 1,
                },
            )
            .await?;

            cursor = result.cursor;
            let checkpoint = Checkpoint::delta_tail(cursor);
            write_change_events_batch(&options.raw_change_log_path, &checkpoint, &result.events)?;
            *events_written += result.events.len();
            *delta_pages_fetched += result.pages_fetched;
            checkpoint_store.save(&checkpoint)?;

            if !result.has_more {
                return Ok(checkpoint);
            }
        }
    }
}
