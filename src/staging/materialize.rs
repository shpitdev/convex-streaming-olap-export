use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::PathBuf,
};

use serde::Serialize;
use serde_json::Value;

use crate::{
    errors::AppResult,
    model::{
        event::{ChangeEvent, ChangeOperation},
        schema::SchemaCatalog,
    },
    sink::parquet::{list_change_event_batch_paths, read_change_events_files, write_staging_table},
    staging::project::{StagingColumnKind, StagingColumnProjection, StagingProjection, StagingRow},
    staging::state::{schema_snapshot_hash, FileStagingStateStore, StagingState},
};

#[derive(Debug, Clone)]
pub struct MaterializeStagingOptions {
    pub raw_change_log_dir: PathBuf,
    pub output_dir: PathBuf,
    pub incremental: bool,
    pub state_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MaterializeStagingSummary {
    pub mode: String,
    pub raw_change_log_dir: PathBuf,
    pub output_dir: PathBuf,
    pub files_read: usize,
    pub events_read: usize,
    pub new_raw_files: usize,
    pub affected_tables: usize,
    pub tables_materialized: usize,
    pub rows_materialized: usize,
}

#[derive(Debug, Default)]
pub struct StagingMaterializer;

impl StagingMaterializer {
    pub fn materialize(
        options: &MaterializeStagingOptions,
    ) -> AppResult<MaterializeStagingSummary> {
        let state_path = options
            .state_path
            .clone()
            .unwrap_or_else(|| options.output_dir.join("_state.json"));
        let state_store = FileStagingStateStore::new(&state_path);
        let schema_hash = schema_snapshot_hash(&options.raw_change_log_dir)?;
        if options.incremental {
            if let Some(summary) =
                Self::try_incremental(options, &state_store, schema_hash.clone())?
            {
                return Ok(summary);
            }
        }

        let all_paths = list_change_event_batch_paths(&options.raw_change_log_dir)?;
        let files_read = all_paths.len();
        let events = read_change_events_files(&all_paths)?;
        let events_read = events.len();
        let new_raw_files = files_read;
        let schema_catalog = SchemaCatalog::read_snapshot(&options.raw_change_log_dir).ok();

        let current_state = fold_current_state(events);

        if options.output_dir.exists() {
            fs::remove_dir_all(&options.output_dir)?;
        }
        fs::create_dir_all(&options.output_dir)?;

        let mut per_table: HashMap<StagingProjection, Vec<StagingRow>> = HashMap::new();
        for event in current_state.into_values() {
            if event.op == ChangeOperation::Delete {
                continue;
            }
            let Some(document) = event.document else {
                continue;
            };

            let projection = StagingProjection {
                component_path: event.component_path.clone(),
                table_name: event.table_name.clone(),
            };
            per_table.entry(projection).or_default().push(StagingRow {
                component_path: event.component_path,
                table_name: event.table_name,
                document_id: event.document_id,
                timestamp: event.timestamp,
                schema_fingerprint: event.schema_fingerprint,
                document,
            });
        }

        let mut rows_materialized = 0usize;
        let tables_materialized = per_table.len();
        let affected_tables = tables_materialized;

        let mut tables: Vec<_> = per_table.into_iter().collect();
        tables.sort_by(|(left, _), (right, _)| {
            (&left.component_path, &left.table_name)
                .cmp(&(&right.component_path, &right.table_name))
        });

        for (projection, rows) in tables {
            let mut rows = rows;
            rows.sort_by(|left, right| {
                left.document_id
                    .cmp(&right.document_id)
                    .then_with(|| left.timestamp.cmp(&right.timestamp))
            });
            rows_materialized += rows.len();
            let columns = infer_staging_columns(
                &rows,
                schema_catalog
                    .as_ref()
                    .and_then(|catalog| catalog.schema_for(&projection.table_name)),
            );
            write_staging_table(&options.output_dir, &projection, &rows, &columns)?;
        }

        let processed_raw_files = all_paths
            .into_iter()
            .filter_map(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().to_string())
            })
            .collect();
        state_store.save(&StagingState::new(schema_hash, processed_raw_files))?;

        Ok(MaterializeStagingSummary {
            mode: "full_rebuild".to_string(),
            raw_change_log_dir: options.raw_change_log_dir.clone(),
            output_dir: options.output_dir.clone(),
            files_read,
            events_read,
            new_raw_files,
            affected_tables,
            tables_materialized,
            rows_materialized,
        })
    }

    fn try_incremental(
        options: &MaterializeStagingOptions,
        state_store: &FileStagingStateStore,
        schema_hash: Option<String>,
    ) -> AppResult<Option<MaterializeStagingSummary>> {
        let Some(state) = state_store.load()? else {
            return Ok(None);
        };

        if state.schema_snapshot_hash != schema_hash {
            return Ok(None);
        }

        let all_paths = list_change_event_batch_paths(&options.raw_change_log_dir)?;
        let new_paths: Vec<_> = all_paths
            .iter()
            .filter(|path| {
                path.file_name()
                    .map(|name| {
                        !state
                            .processed_raw_files
                            .contains(&name.to_string_lossy().to_string())
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect();

        if new_paths.is_empty() {
            return Ok(Some(MaterializeStagingSummary {
                mode: "incremental".to_string(),
                raw_change_log_dir: options.raw_change_log_dir.clone(),
                output_dir: options.output_dir.clone(),
                files_read: 0,
                events_read: 0,
                new_raw_files: 0,
                affected_tables: 0,
                tables_materialized: 0,
                rows_materialized: 0,
            }));
        }

        let new_events = read_change_events_files(&new_paths)?;
        let affected: BTreeSet<StagingProjection> = new_events
            .iter()
            .map(|event| StagingProjection {
                component_path: event.component_path.clone(),
                table_name: event.table_name.clone(),
            })
            .collect();

        let all_events = read_change_events_files(&all_paths)?;
        let current_state = fold_current_state(all_events);
        let schema_catalog = SchemaCatalog::read_snapshot(&options.raw_change_log_dir).ok();

        let mut rows_materialized = 0usize;
        for projection in &affected {
            let mut rows: Vec<_> = current_state
                .values()
                .filter(|event| {
                    event.op != ChangeOperation::Delete
                        && event.component_path == projection.component_path
                        && event.table_name == projection.table_name
                })
                .filter_map(|event| {
                    event.document.as_ref().map(|document| StagingRow {
                        component_path: event.component_path.clone(),
                        table_name: event.table_name.clone(),
                        document_id: event.document_id.clone(),
                        timestamp: event.timestamp,
                        schema_fingerprint: event.schema_fingerprint.clone(),
                        document: document.clone(),
                    })
                })
                .collect();

            rows.sort_by(|left, right| {
                left.document_id
                    .cmp(&right.document_id)
                    .then_with(|| left.timestamp.cmp(&right.timestamp))
            });

            rows_materialized += rows.len();
            let columns = infer_staging_columns(
                &rows,
                schema_catalog
                    .as_ref()
                    .and_then(|catalog| catalog.schema_for(&projection.table_name)),
            );
            let path = projection.output_path(&options.output_dir);
            if rows.is_empty() {
                let _ = fs::remove_file(path);
            } else {
                write_staging_table(&options.output_dir, projection, &rows, &columns)?;
            }
        }

        let processed_raw_files = all_paths
            .into_iter()
            .filter_map(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().to_string())
            })
            .collect();
        state_store.save(&StagingState::new(schema_hash, processed_raw_files))?;

        Ok(Some(MaterializeStagingSummary {
            mode: "incremental".to_string(),
            raw_change_log_dir: options.raw_change_log_dir.clone(),
            output_dir: options.output_dir.clone(),
            files_read: new_paths.len(),
            events_read: new_events.len(),
            new_raw_files: new_paths.len(),
            affected_tables: affected.len(),
            tables_materialized: affected.len(),
            rows_materialized,
        }))
    }
}

fn fold_current_state(events: Vec<ChangeEvent>) -> HashMap<(String, String, String), ChangeEvent> {
    let mut latest: HashMap<(String, String, String), ChangeEvent> = HashMap::new();
    for event in events {
        let key = (
            event.component_path.clone(),
            event.table_name.clone(),
            event.document_id.clone(),
        );
        let should_replace = match latest.get(&key) {
            Some(existing) => event.timestamp >= existing.timestamp,
            None => true,
        };
        if should_replace {
            latest.insert(key, event);
        }
    }
    latest
}

fn infer_staging_columns(
    rows: &[StagingRow],
    schema: Option<&Value>,
) -> Vec<StagingColumnProjection> {
    let mut observed = std::collections::BTreeMap::<String, ObservedColumnKind>::new();

    if let Some(schema) = schema {
        seed_schema_columns(&mut observed, schema);
    }

    for row in rows {
        let Some(object) = row.document.as_object() else {
            continue;
        };
        for (field_name, value) in object {
            if is_reserved_staging_column(field_name) {
                continue;
            }
            observed
                .entry(field_name.clone())
                .or_default()
                .observe(value);
        }
    }

    observed
        .into_iter()
        .filter_map(|(name, observed)| {
            observed
                .final_kind()
                .map(|kind| StagingColumnProjection { name, kind })
        })
        .collect()
}

fn seed_schema_columns(
    observed: &mut std::collections::BTreeMap<String, ObservedColumnKind>,
    schema: &Value,
) {
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return;
    };

    for (field_name, field_schema) in properties {
        if is_reserved_schema_metadata(field_name) {
            continue;
        }

        let kind = if field_name == "_creationTime" {
            Some(StagingColumnKind::Float64)
        } else {
            classify_schema_kind(field_schema)
        };

        if let Some(kind) = kind {
            observed.entry(field_name.clone()).or_default().seed(kind);
        }
    }
}

fn classify_schema_kind(schema: &Value) -> Option<StagingColumnKind> {
    if let Some(types) = schema.get("type") {
        return classify_schema_type(types);
    }

    if let Some(branches) = schema.get("anyOf").and_then(Value::as_array) {
        return classify_schema_union(branches);
    }

    if let Some(branches) = schema.get("oneOf").and_then(Value::as_array) {
        return classify_schema_union(branches);
    }

    None
}

fn classify_schema_union(branches: &[Value]) -> Option<StagingColumnKind> {
    let mut kinds = branches
        .iter()
        .filter_map(classify_schema_kind)
        .collect::<Vec<_>>();
    kinds.sort();
    kinds.dedup();
    match kinds.as_slice() {
        [] => None,
        [kind] => Some(*kind),
        [StagingColumnKind::Int64, StagingColumnKind::Float64]
        | [StagingColumnKind::Float64, StagingColumnKind::Int64] => {
            Some(StagingColumnKind::Float64)
        },
        _ => Some(StagingColumnKind::JsonUtf8),
    }
}

fn classify_schema_type(schema_type: &Value) -> Option<StagingColumnKind> {
    match schema_type {
        Value::String(value) => Some(match value.as_str() {
            "boolean" => StagingColumnKind::Boolean,
            "integer" => StagingColumnKind::Int64,
            "number" => StagingColumnKind::Float64,
            "string" => StagingColumnKind::Utf8,
            "array" | "object" => StagingColumnKind::JsonUtf8,
            "null" => return None,
            _ => StagingColumnKind::JsonUtf8,
        }),
        Value::Array(values) => {
            let mut kinds = values
                .iter()
                .filter_map(classify_schema_type)
                .collect::<Vec<_>>();
            kinds.sort();
            kinds.dedup();
            match kinds.as_slice() {
                [] => None,
                [kind] => Some(*kind),
                [StagingColumnKind::Int64, StagingColumnKind::Float64]
                | [StagingColumnKind::Float64, StagingColumnKind::Int64] => {
                    Some(StagingColumnKind::Float64)
                },
                _ => Some(StagingColumnKind::JsonUtf8),
            }
        },
        _ => None,
    }
}

fn is_reserved_schema_metadata(field_name: &str) -> bool {
    matches!(
        field_name,
        "_component" | "_deleted" | "_id" | "_table" | "_ts"
    )
}

fn is_reserved_staging_column(field_name: &str) -> bool {
    matches!(
        field_name,
        "_component_path"
            | "_table_name"
            | "_document_id"
            | "_timestamp"
            | "_schema_fingerprint"
            | "_document_json"
    )
}

#[derive(Debug, Clone, Copy, Default)]
struct ObservedColumnKind {
    kind: Option<StagingColumnKind>,
}

impl ObservedColumnKind {
    fn seed(&mut self, kind: StagingColumnKind) {
        self.kind = Some(kind);
    }

    fn observe(&mut self, value: &Value) {
        if value.is_null() {
            return;
        }
        let next = classify_value(value);
        self.kind = Some(match self.kind {
            None => next,
            Some(StagingColumnKind::Int64) if next == StagingColumnKind::Float64 => {
                StagingColumnKind::Float64
            },
            Some(StagingColumnKind::Float64) if next == StagingColumnKind::Int64 => {
                StagingColumnKind::Float64
            },
            Some(existing) if existing == next => existing,
            _ => StagingColumnKind::JsonUtf8,
        });
    }

    fn final_kind(self) -> Option<StagingColumnKind> {
        self.kind
    }
}

fn classify_value(value: &Value) -> StagingColumnKind {
    match value {
        Value::Bool(_) => StagingColumnKind::Boolean,
        Value::Number(number) if number.is_i64() => StagingColumnKind::Int64,
        Value::Number(_) => StagingColumnKind::Float64,
        Value::String(_) => StagingColumnKind::Utf8,
        Value::Array(_) | Value::Object(_) => StagingColumnKind::JsonUtf8,
        Value::Null => StagingColumnKind::JsonUtf8,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow_array::{Array, Float64Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::json;

    use crate::{
        model::{
            checkpoint::Checkpoint,
            event::{ChangeEvent, ChangeOperation},
            schema::SchemaCatalog,
        },
        sink::parquet::write_change_events_batch,
    };

    use super::{MaterializeStagingOptions, StagingMaterializer};

    #[test]
    fn materializes_current_state_tables() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("staging-materialize-{nanos}"));
        let raw = root.join("raw");
        let output = root.join("staging");

        write_change_events_batch(
            &raw,
            &Checkpoint::initial_snapshot(100, "cursor-1".to_string()),
            &[
                ChangeEvent {
                    component_path: "".to_string(),
                    table_name: "users".to_string(),
                    document_id: "users:1".to_string(),
                    timestamp: 10,
                    op: ChangeOperation::Upsert,
                    schema_fingerprint: Some("abc".to_string()),
                    document: Some(json!({"name":"Ada"})),
                },
                ChangeEvent {
                    component_path: "".to_string(),
                    table_name: "users".to_string(),
                    document_id: "users:2".to_string(),
                    timestamp: 11,
                    op: ChangeOperation::Upsert,
                    schema_fingerprint: Some("abc".to_string()),
                    document: Some(json!({"name":"Ben","age":20})),
                },
                ChangeEvent {
                    component_path: "workflow".to_string(),
                    table_name: "events".to_string(),
                    document_id: "events:1".to_string(),
                    timestamp: 12,
                    op: ChangeOperation::Upsert,
                    schema_fingerprint: Some("def".to_string()),
                    document: Some(json!({"kind":"queued"})),
                },
            ],
        )
        .unwrap();

        write_change_events_batch(
            &raw,
            &Checkpoint::delta_tail(200),
            &[
                ChangeEvent {
                    component_path: "".to_string(),
                    table_name: "users".to_string(),
                    document_id: "users:1".to_string(),
                    timestamp: 20,
                    op: ChangeOperation::Upsert,
                    schema_fingerprint: Some("abc".to_string()),
                    document: Some(json!({"name":"Ada Lovelace","age":10.5,"meta":{"tier":"pro"}})),
                },
                ChangeEvent {
                    component_path: "".to_string(),
                    table_name: "users".to_string(),
                    document_id: "users:2".to_string(),
                    timestamp: 21,
                    op: ChangeOperation::Delete,
                    schema_fingerprint: Some("abc".to_string()),
                    document: None,
                },
            ],
        )
        .unwrap();

        let summary = StagingMaterializer::materialize(&MaterializeStagingOptions {
            raw_change_log_dir: raw.clone(),
            output_dir: output.clone(),
            incremental: false,
            state_path: None,
        })
        .unwrap();

        assert_eq!(summary.files_read, 2);
        assert_eq!(summary.events_read, 5);
        assert_eq!(summary.tables_materialized, 2);
        assert_eq!(summary.rows_materialized, 2);

        let users_path = output.join("_root").join("users.parquet");
        let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&users_path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|batch| batch.unwrap()).collect();
        assert_eq!(batches[0].num_rows(), 1);
        let doc_ids = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(doc_ids.value(0), "users:1");
        let age = batches[0]
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(age.value(0), 10.5);
        let meta = batches[0]
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(meta.value(0).contains("\"tier\":\"pro\""));
        let workflow_events = output.join("workflow").join("events.parquet");
        assert!(workflow_events.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn incrementally_updates_only_affected_tables() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("staging-incremental-{nanos}"));
        let raw = root.join("raw");
        let output = root.join("staging");
        let state_path = root.join("staging-state.json");

        let schema_catalog = SchemaCatalog::from_json_schemas(&json!({
            "users": {
                "type": "object",
                "properties": {
                    "_creationTime": {"type": "number"},
                    "name": {"type": "string"}
                }
            },
            "events": {
                "type": "object",
                "properties": {
                    "_creationTime": {"type": "number"},
                    "kind": {"type": "string"}
                }
            }
        }));
        schema_catalog.write_snapshot(&raw).unwrap();

        write_change_events_batch(
            &raw,
            &Checkpoint::initial_snapshot(100, "cursor-1".to_string()),
            &[ChangeEvent {
                component_path: "".to_string(),
                table_name: "users".to_string(),
                document_id: "users:1".to_string(),
                timestamp: 10,
                op: ChangeOperation::Upsert,
                schema_fingerprint: Some("abc".to_string()),
                document: Some(json!({"name":"Ada"})),
            }],
        )
        .unwrap();

        let full = StagingMaterializer::materialize(&MaterializeStagingOptions {
            raw_change_log_dir: raw.clone(),
            output_dir: output.clone(),
            incremental: false,
            state_path: Some(state_path.clone()),
        })
        .unwrap();
        assert_eq!(full.mode, "full_rebuild");
        assert_eq!(full.tables_materialized, 1);

        write_change_events_batch(
            &raw,
            &Checkpoint::delta_tail(200),
            &[ChangeEvent {
                component_path: "workflow".to_string(),
                table_name: "events".to_string(),
                document_id: "events:1".to_string(),
                timestamp: 20,
                op: ChangeOperation::Upsert,
                schema_fingerprint: Some("def".to_string()),
                document: Some(json!({"kind":"queued"})),
            }],
        )
        .unwrap();

        let incremental = StagingMaterializer::materialize(&MaterializeStagingOptions {
            raw_change_log_dir: raw.clone(),
            output_dir: output.clone(),
            incremental: true,
            state_path: Some(state_path),
        })
        .unwrap();

        assert_eq!(incremental.mode, "incremental");
        assert_eq!(incremental.new_raw_files, 1);
        assert_eq!(incremental.affected_tables, 1);
        assert_eq!(incremental.tables_materialized, 1);

        let users_path = output.join("_root").join("users.parquet");
        let workflow_events = output.join("workflow").join("events.parquet");
        assert!(users_path.exists());
        assert!(workflow_events.exists());

        let _ = fs::remove_dir_all(root);
    }
}
