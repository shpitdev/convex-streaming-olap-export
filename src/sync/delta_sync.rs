use crate::{
    convex::{client::ConvexClient, deltas::DocumentDeltasQuery},
    errors::{AppError, AppResult},
    model::{event::ChangeEvent, schema::SchemaCatalog},
};

#[derive(Debug, Clone)]
pub struct DeltaSyncOptions {
    pub cursor: i64,
    pub table_name: Option<String>,
    pub max_pages: usize,
}

#[derive(Debug, Clone)]
pub struct DeltaSyncResult {
    pub events: Vec<ChangeEvent>,
    pub cursor: i64,
    pub has_more: bool,
    pub pages_fetched: usize,
}

pub async fn fetch_delta_events(
    client: &ConvexClient,
    schemas: &SchemaCatalog,
    options: &DeltaSyncOptions,
) -> AppResult<DeltaSyncResult> {
    if options.max_pages == 0 {
        return Err(AppError::InvalidPageLimit(options.max_pages));
    }

    let mut events = Vec::new();
    let mut cursor = options.cursor;
    let mut has_more = false;

    for page_index in 0..options.max_pages {
        let response = client
            .document_deltas(&DocumentDeltasQuery {
                cursor,
                table_name: options.table_name.clone(),
            })
            .await?;

        cursor = response.cursor;
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
            return Ok(DeltaSyncResult {
                events,
                cursor,
                has_more,
                pages_fetched: page_index + 1,
            });
        }
    }

    Ok(DeltaSyncResult {
        events,
        cursor,
        has_more,
        pages_fetched: options.max_pages,
    })
}
