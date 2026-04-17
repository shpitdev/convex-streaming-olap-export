#[derive(Debug, Default)]
pub struct ParquetSink;

impl ParquetSink {
    pub fn milestone_note() -> &'static str {
        "Parquet writing lands once the event envelope and checkpoint flow are validated."
    }
}
