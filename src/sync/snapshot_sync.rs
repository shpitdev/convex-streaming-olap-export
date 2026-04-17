use crate::{
    convex::{client::ConvexClient, snapshot::ListSnapshotQuery},
    errors::{AppError, AppResult},
    model::{event::ChangeEvent, schema::SchemaCatalog},
};

#[derive(Debug, Clone)]
pub struct SnapshotSyncOptions {
    pub table_name: Option<String>,
    pub snapshot: Option<i64>,
    pub cursor: Option<String>,
    pub max_pages: usize,
}

#[derive(Debug, Clone)]
pub struct SnapshotSyncResult {
    pub events: Vec<ChangeEvent>,
    pub snapshot: i64,
    pub cursor: Option<String>,
    pub has_more: bool,
    pub pages_fetched: usize,
}

pub async fn fetch_snapshot_events(
    client: &ConvexClient,
    schemas: &SchemaCatalog,
    options: &SnapshotSyncOptions,
) -> AppResult<SnapshotSyncResult> {
    if options.max_pages == 0 {
        return Err(AppError::InvalidPageLimit(options.max_pages));
    }

    let mut events = Vec::new();
    let mut next_cursor = options.cursor.clone();
    let mut next_snapshot = options.snapshot.unwrap_or_default();
    let mut has_more = false;

    for page_index in 0..options.max_pages {
        let response = client
            .list_snapshot(&ListSnapshotQuery {
                snapshot: if page_index == 0 {
                    options.snapshot
                } else {
                    Some(next_snapshot)
                },
                cursor: next_cursor.clone(),
                table_name: options.table_name.clone(),
            })
            .await?;

        next_snapshot = response.snapshot;
        next_cursor = response.cursor.clone();
        has_more = response.has_more;

        for value in response.values {
            let event = ChangeEvent::from_convex_value(value, None)?;
            let schema_fingerprint = schemas.fingerprint_for(&event.table_name);
            events.push(ChangeEvent {
                schema_fingerprint,
                ..event
            });
        }

        if !has_more {
            return Ok(SnapshotSyncResult {
                events,
                snapshot: next_snapshot,
                cursor: next_cursor,
                has_more,
                pages_fetched: page_index + 1,
            });
        }
    }

    Ok(SnapshotSyncResult {
        events,
        snapshot: next_snapshot,
        cursor: next_cursor,
        has_more,
        pages_fetched: options.max_pages,
    })
}
