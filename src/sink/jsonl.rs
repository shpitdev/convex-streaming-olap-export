use std::{
    fs::{self, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
};

use serde::Serialize;

use crate::{config::OutputFormat, errors::AppResult};

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
    writer.flush()?;
    Ok(())
}
