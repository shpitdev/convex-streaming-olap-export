use std::{
    fmt::Display,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
    Array, ArrayRef, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    file::properties::WriterProperties,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

use convex_sync_core::{
    errors::{AppError, AppResult},
    model::{
        checkpoint::{Checkpoint, SyncState},
        event::ChangeEvent,
        schema::SchemaCatalog,
    },
    sync::runner::ChangeEventBatchWriter,
};

use crate::staging::project::{
    StagingColumnKind, StagingColumnProjection, StagingProjection, StagingRow,
};

fn arrow_error(err: impl Display) -> AppError {
    AppError::Arrow(err.to_string())
}

fn parquet_error(err: impl Display) -> AppError {
    AppError::Parquet(err.to_string())
}

#[derive(Debug, Clone)]
pub struct ParquetRawChangeLogWriter {
    output_dir: PathBuf,
}

impl ParquetRawChangeLogWriter {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            output_dir: path.into(),
        }
    }

    pub fn output_dir(&self) -> &Path {
        &self.output_dir
    }
}

impl ChangeEventBatchWriter for ParquetRawChangeLogWriter {
    fn write_schema_snapshot(&mut self, schemas: &SchemaCatalog) -> AppResult<()> {
        schemas.write_snapshot(&self.output_dir)
    }

    fn write_change_events(
        &mut self,
        checkpoint: &Checkpoint,
        events: &[ChangeEvent],
    ) -> AppResult<()> {
        let _ = write_change_events_batch(&self.output_dir, checkpoint, events)?;
        Ok(())
    }
}

pub fn write_change_events_batch(
    output_dir: &Path,
    checkpoint: &Checkpoint,
    events: &[ChangeEvent],
) -> AppResult<Option<PathBuf>> {
    if events.is_empty() {
        return Ok(None);
    }

    fs::create_dir_all(output_dir)?;
    let final_path = output_dir.join(batch_file_name(checkpoint));
    let temp_path = temporary_parquet_path(&final_path);

    let mut file = File::create(&temp_path)?;
    let schema = parquet_schema();
    let batch = build_record_batch(schema.clone(), events)?;
    let properties = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(&mut file, schema, Some(properties)).map_err(parquet_error)?;
    writer.write(&batch).map_err(parquet_error)?;
    writer.close().map_err(parquet_error)?;
    file.sync_all()?;
    drop(file);

    fs::rename(&temp_path, &final_path)?;
    sync_parent_directory(final_path.parent())?;
    Ok(Some(final_path))
}

pub fn read_change_events_dir(input_dir: &Path) -> AppResult<Vec<ChangeEvent>> {
    let paths = list_change_event_batch_paths(input_dir)?;
    read_change_events_files(&paths)
}

pub fn list_change_event_batch_paths(input_dir: &Path) -> AppResult<Vec<PathBuf>> {
    let mut paths: Vec<_> = fs::read_dir(input_dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    paths.sort();
    Ok(paths)
}

pub fn read_change_events_files(paths: &[PathBuf]) -> AppResult<Vec<ChangeEvent>> {
    let mut events = Vec::new();
    for path in paths {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
            .map_err(parquet_error)?
            .build()
            .map_err(parquet_error)?;
        for batch in reader {
            events.extend(change_events_from_batch(&batch.map_err(arrow_error)?)?);
        }
    }
    Ok(events)
}

pub fn write_staging_table(
    output_root: &Path,
    projection: &StagingProjection,
    rows: &[StagingRow],
    columns: &[StagingColumnProjection],
) -> AppResult<Option<PathBuf>> {
    if rows.is_empty() {
        return Ok(None);
    }

    let final_path = projection.output_path(output_root);
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp_path = temporary_parquet_path(&final_path);

    let mut file = File::create(&temp_path)?;
    let schema = staging_schema(columns);
    let batch = build_staging_record_batch(schema.clone(), rows, columns)?;
    let properties = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(&mut file, schema, Some(properties)).map_err(parquet_error)?;
    writer.write(&batch).map_err(parquet_error)?;
    writer.close().map_err(parquet_error)?;
    file.sync_all()?;
    drop(file);

    fs::rename(&temp_path, &final_path)?;
    sync_parent_directory(final_path.parent())?;
    Ok(Some(final_path))
}

fn build_record_batch(schema: Arc<Schema>, events: &[ChangeEvent]) -> AppResult<RecordBatch> {
    let mut component_path = StringBuilder::new();
    let mut table_name = StringBuilder::new();
    let mut document_id = StringBuilder::new();
    let mut timestamp = Int64Builder::new();
    let mut op = StringBuilder::new();
    let mut schema_fingerprint = StringBuilder::new();
    let mut document = StringBuilder::new();

    for event in events {
        component_path.append_value(&event.component_path);
        table_name.append_value(&event.table_name);
        document_id.append_value(&event.document_id);
        timestamp.append_value(event.timestamp);
        op.append_value(event.op.as_str());

        match event.schema_fingerprint.as_deref() {
            Some(value) => schema_fingerprint.append_value(value),
            None => schema_fingerprint.append_null(),
        }

        match event.document.as_ref() {
            Some(value) => document.append_value(document_json(value)?),
            None => document.append_null(),
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(component_path.finish()),
        Arc::new(table_name.finish()),
        Arc::new(document_id.finish()),
        Arc::new(timestamp.finish()),
        Arc::new(op.finish()),
        Arc::new(schema_fingerprint.finish()),
        Arc::new(document.finish()),
    ];

    RecordBatch::try_new(schema, arrays).map_err(arrow_error)
}

fn document_json(value: &Value) -> AppResult<String> {
    Ok(serde_json::to_string(value)?)
}

fn parquet_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("component_path", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("document_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
        Field::new("schema_fingerprint", DataType::Utf8, true),
        Field::new("document", DataType::Utf8, true),
    ]))
}

fn staging_schema(columns: &[StagingColumnProjection]) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("_component_path", DataType::Utf8, false),
        Field::new("_table_name", DataType::Utf8, false),
        Field::new("_document_id", DataType::Utf8, false),
        Field::new("_timestamp", DataType::Int64, false),
        Field::new("_schema_fingerprint", DataType::Utf8, true),
    ];
    fields.extend(columns.iter().map(|column| {
        Field::new(
            &column.name,
            match column.kind {
                StagingColumnKind::Boolean => DataType::Boolean,
                StagingColumnKind::Int64 => DataType::Int64,
                StagingColumnKind::Float64 => DataType::Float64,
                StagingColumnKind::Utf8 | StagingColumnKind::JsonUtf8 => DataType::Utf8,
            },
            true,
        )
    }));
    fields.push(Field::new("_document_json", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

fn build_staging_record_batch(
    schema: Arc<Schema>,
    rows: &[StagingRow],
    columns: &[StagingColumnProjection],
) -> AppResult<RecordBatch> {
    let mut component_path = StringBuilder::new();
    let mut table_name = StringBuilder::new();
    let mut document_id = StringBuilder::new();
    let mut timestamp = Int64Builder::new();
    let mut schema_fingerprint = StringBuilder::new();
    let mut full_document_json = StringBuilder::new();
    let mut dynamic_builders: Vec<DynamicBuilder> = columns
        .iter()
        .map(|column| DynamicBuilder::new(column.kind))
        .collect();

    for row in rows {
        component_path.append_value(&row.component_path);
        table_name.append_value(&row.table_name);
        document_id.append_value(&row.document_id);
        timestamp.append_value(row.timestamp);
        match row.schema_fingerprint.as_deref() {
            Some(value) => schema_fingerprint.append_value(value),
            None => schema_fingerprint.append_null(),
        }
        for (column, builder) in columns.iter().zip(dynamic_builders.iter_mut()) {
            let value = row
                .document
                .as_object()
                .and_then(|object| object.get(&column.name));
            builder.append(value)?;
        }
        full_document_json.append_value(document_json(&row.document)?);
    }

    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(component_path.finish()),
        Arc::new(table_name.finish()),
        Arc::new(document_id.finish()),
        Arc::new(timestamp.finish()),
        Arc::new(schema_fingerprint.finish()),
    ];
    arrays.extend(dynamic_builders.into_iter().map(DynamicBuilder::finish));
    arrays.push(Arc::new(full_document_json.finish()));

    RecordBatch::try_new(schema, arrays).map_err(arrow_error)
}

enum DynamicBuilder {
    Boolean(BooleanBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
}

impl DynamicBuilder {
    fn new(kind: StagingColumnKind) -> Self {
        match kind {
            StagingColumnKind::Boolean => Self::Boolean(BooleanBuilder::new()),
            StagingColumnKind::Int64 => Self::Int64(Int64Builder::new()),
            StagingColumnKind::Float64 => Self::Float64(Float64Builder::new()),
            StagingColumnKind::Utf8 | StagingColumnKind::JsonUtf8 => {
                Self::Utf8(StringBuilder::new())
            },
        }
    }

    fn append(&mut self, value: Option<&Value>) -> AppResult<()> {
        match self {
            Self::Boolean(builder) => match value.and_then(Value::as_bool) {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            },
            Self::Int64(builder) => match value.and_then(Value::as_i64) {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            },
            Self::Float64(builder) => match value
                .and_then(|value| value.as_f64().or_else(|| value.as_i64().map(|v| v as f64)))
            {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            },
            Self::Utf8(builder) => match value {
                Some(Value::Null) | None => builder.append_null(),
                Some(Value::String(value)) => builder.append_value(value),
                Some(other) => builder.append_value(document_json(other)?),
            },
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Boolean(mut builder) => Arc::new(builder.finish()),
            Self::Int64(mut builder) => Arc::new(builder.finish()),
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
        }
    }
}

fn batch_file_name(checkpoint: &Checkpoint) -> String {
    let digest = checkpoint_digest(checkpoint);
    match &checkpoint.sync_state {
        SyncState::InitialSnapshot { snapshot, .. } => {
            format!(
                "{}-{}-{}.parquet",
                checkpoint.phase_name(),
                snapshot,
                &digest[..16]
            )
        },
        SyncState::DeltaTail { cursor } => {
            format!(
                "{}-{}-{}.parquet",
                checkpoint.phase_name(),
                cursor,
                &digest[..16]
            )
        },
    }
}

fn checkpoint_digest(checkpoint: &Checkpoint) -> String {
    let bytes = serde_json::to_vec(checkpoint).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

fn change_events_from_batch(batch: &RecordBatch) -> AppResult<Vec<ChangeEvent>> {
    let component_path = string_column(batch, 0, "component_path")?;
    let table_name = string_column(batch, 1, "table_name")?;
    let document_id = string_column(batch, 2, "document_id")?;
    let timestamp = int64_column(batch, 3, "timestamp")?;
    let op = string_column(batch, 4, "op")?;
    let schema_fingerprint = string_column(batch, 5, "schema_fingerprint")?;
    let document = string_column(batch, 6, "document")?;

    let mut events = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let op = match op.value(row) {
            "upsert" => convex_sync_core::model::event::ChangeOperation::Upsert,
            "delete" => convex_sync_core::model::event::ChangeOperation::Delete,
            other => {
                return Err(AppError::InvalidParquetSchema(format!(
                    "unexpected op value `{other}`"
                )))
            },
        };

        let document = if document.is_null(row) {
            None
        } else {
            Some(serde_json::from_str::<Value>(document.value(row))?)
        };

        let schema_fingerprint = if schema_fingerprint.is_null(row) {
            None
        } else {
            Some(schema_fingerprint.value(row).to_string())
        };

        events.push(ChangeEvent {
            component_path: component_path.value(row).to_string(),
            table_name: table_name.value(row).to_string(),
            document_id: document_id.value(row).to_string(),
            timestamp: timestamp.value(row),
            op,
            schema_fingerprint,
            document,
        });
    }
    Ok(events)
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
) -> AppResult<&'a StringArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| AppError::InvalidParquetSchema(format!("column `{name}` was not utf8")))
}

fn int64_column<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> AppResult<&'a Int64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| AppError::InvalidParquetSchema(format!("column `{name}` was not int64")))
}

fn temporary_parquet_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    PathBuf::from(tmp)
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
        fs::{self, File},
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow_array::{Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::json;

    use crate::staging::project::{
        StagingColumnKind, StagingColumnProjection, StagingProjection, StagingRow,
    };
    use convex_sync_core::model::{
        checkpoint::Checkpoint,
        event::{ChangeEvent, ChangeOperation},
    };

    use super::{read_change_events_dir, write_change_events_batch, write_staging_table};

    #[test]
    fn writes_parquet_batch() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let output_dir = std::env::temp_dir().join(format!("parquet-batch-{nanos}"));
        let checkpoint = Checkpoint::delta_tail(42);
        let events = vec![
            ChangeEvent {
                component_path: "".to_string(),
                table_name: "users".to_string(),
                document_id: "users:1".to_string(),
                timestamp: 42,
                op: ChangeOperation::Upsert,
                schema_fingerprint: Some("abc".to_string()),
                document: Some(json!({"name":"Ada"})),
            },
            ChangeEvent {
                component_path: "r2".to_string(),
                table_name: "metadata".to_string(),
                document_id: "metadata:1".to_string(),
                timestamp: 43,
                op: ChangeOperation::Delete,
                schema_fingerprint: None,
                document: None,
            },
        ];

        let path = write_change_events_batch(&output_dir, &checkpoint, &events)
            .unwrap()
            .unwrap();

        assert!(path.exists());
        assert!(path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("delta_tail-42"));

        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|batch| batch.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        let table_name = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(table_name.value(0), "users");
        assert_eq!(table_name.value(1), "metadata");

        let _ = fs::remove_file(path);
        let _ = fs::remove_dir(output_dir);
    }

    #[test]
    fn reads_change_event_batches() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let output_dir = std::env::temp_dir().join(format!("parquet-read-{nanos}"));
        let checkpoint = Checkpoint::delta_tail(42);
        let events = vec![ChangeEvent {
            component_path: "".to_string(),
            table_name: "users".to_string(),
            document_id: "users:1".to_string(),
            timestamp: 42,
            op: ChangeOperation::Upsert,
            schema_fingerprint: Some("abc".to_string()),
            document: Some(json!({"name":"Ada"})),
        }];

        write_change_events_batch(&output_dir, &checkpoint, &events).unwrap();
        let roundtrip = read_change_events_dir(&output_dir).unwrap();
        assert_eq!(roundtrip.len(), 1);
        assert_eq!(roundtrip[0].document_id, "users:1");

        let _ = fs::remove_dir_all(output_dir);
    }

    #[test]
    fn writes_staging_table() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let output_dir = std::env::temp_dir().join(format!("staging-write-{nanos}"));
        let projection = StagingProjection {
            component_path: "".to_string(),
            table_name: "users".to_string(),
        };
        let columns = vec![
            StagingColumnProjection {
                name: "name".to_string(),
                kind: StagingColumnKind::Utf8,
            },
            StagingColumnProjection {
                name: "nickname".to_string(),
                kind: StagingColumnKind::Utf8,
            },
        ];
        let rows = vec![StagingRow {
            component_path: "".to_string(),
            table_name: "users".to_string(),
            document_id: "users:1".to_string(),
            timestamp: 42,
            schema_fingerprint: Some("abc".to_string()),
            document: json!({"name":"Ada","nickname":null}),
        }];

        let path = write_staging_table(&output_dir, &projection, &rows, &columns)
            .unwrap()
            .unwrap();
        assert!(path.exists());

        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|batch| batch.unwrap()).collect();
        assert_eq!(batches[0].num_rows(), 1);
        let nickname_index = batches[0].schema().index_of("nickname").unwrap();
        let nickname = batches[0]
            .column(nickname_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(nickname.is_null(0));

        let _ = fs::remove_file(path);
        let _ = fs::remove_dir(output_dir.join("_root"));
        let _ = fs::remove_dir(output_dir);
    }
}
