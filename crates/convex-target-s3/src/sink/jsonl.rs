use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
};

use serde::Serialize;

use convex_cdc_core::{config::OutputFormat, errors::AppResult};

pub fn write_value<W, T>(writer: &mut W, value: &T, format: OutputFormat) -> AppResult<()>
where
    W: Write,
    T: Serialize,
{
    match format {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut *writer, value)?;
            writer.write_all(b"\n")?;
        },
        OutputFormat::Jsonl => {
            serde_json::to_writer(&mut *writer, value)?;
            writer.write_all(b"\n")?;
        },
    }

    Ok(())
}

pub fn write_jsonl_stream<W, T>(writer: &mut W, values: &[T]) -> AppResult<()>
where
    W: Write,
    T: Serialize,
{
    for value in values {
        serde_json::to_writer(&mut *writer, value)?;
        writer.write_all(b"\n")?;
    }

    Ok(())
}

pub fn append_jsonl_to_path<T>(path: &Path, values: &[T]) -> AppResult<()>
where
    T: Serialize,
{
    if values.is_empty() {
        return Ok(());
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::new(file);
    write_jsonl_stream(&mut writer, values)?;
    let file = writer.into_inner().map_err(|err| err.into_error())?;
    file.sync_all()?;
    sync_parent_directory(path.parent())?;
    Ok(())
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

    use serde::Serialize;

    use super::append_jsonl_to_path;

    #[derive(Serialize)]
    struct Row {
        value: i32,
    }

    #[test]
    fn appends_jsonl_rows() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("jsonl-append-{nanos}.jsonl"));

        append_jsonl_to_path(&path, &[Row { value: 1 }, Row { value: 2 }]).unwrap();

        let contents = fs::read_to_string(&path).unwrap();
        assert_eq!(contents.lines().count(), 2);
        assert!(contents.contains(r#""value":1"#));
        assert!(contents.contains(r#""value":2"#));

        let _ = fs::remove_file(path);
    }
}
