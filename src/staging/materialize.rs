use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;
use serde_json::Value;

use crate::{
    errors::AppResult,
    model::{
        event::{ChangeEvent, ChangeOperation},
        schema::SchemaCatalog,
    },
    sink::parquet::{read_change_events_dir, write_staging_table},
    staging::project::{StagingColumnKind, StagingColumnProjection, StagingProjection, StagingRow},
};

#[derive(Debug, Clone)]
pub struct MaterializeStagingOptions {
    pub raw_change_log_dir: PathBuf,
    pub output_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct MaterializeStagingSummary {
    pub raw_change_log_dir: PathBuf,
    pub output_dir: PathBuf,
    pub files_read: usize,
    pub events_read: usize,
    pub tables_materialized: usize,
    pub rows_materialized: usize,
}

#[derive(Debug, Default)]
pub struct StagingMaterializer;

impl StagingMaterializer {
    pub fn materialize(
        options: &MaterializeStagingOptions,
    ) -> AppResult<MaterializeStagingSummary> {
        let events = read_change_events_dir(&options.raw_change_log_dir)?;
        let files_read = parquet_file_count(&options.raw_change_log_dir)?;
        let events_read = events.len();
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

        Ok(MaterializeStagingSummary {
            raw_change_log_dir: options.raw_change_log_dir.clone(),
            output_dir: options.output_dir.clone(),
            files_read,
            events_read,
            tables_materialized,
            rows_materialized,
        })
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

fn parquet_file_count(dir: &Path) -> AppResult<usize> {
    Ok(fs::read_dir(dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
        .count())
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

    use arrow_array::{Float64Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::json;

    use crate::{
        model::{
            checkpoint::Checkpoint,
            event::{ChangeEvent, ChangeOperation},
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
}
