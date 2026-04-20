use thiserror::Error;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("arrow error: {0}")]
    Arrow(String),
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("i/o failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("parquet error: {0}")]
    Parquet(String),
    #[error("url error: {0}")]
    Url(#[from] url::ParseError),
    #[error("invalid deploy URL: {0}")]
    InvalidDeployUrl(String),
    #[error("deploy key cannot be empty")]
    EmptyDeployKey,
    #[error("invalid deploy key for authorization header: {0}")]
    InvalidDeployKey(String),
    #[error("expected a JSON object document from Convex")]
    ExpectedJsonObject,
    #[error("missing required Convex metadata field `{0}`")]
    MissingMetadata(&'static str),
    #[error("field `{0}` had the wrong type")]
    InvalidFieldType(&'static str),
    #[error("invalid page limit {0}; expected at least 1")]
    InvalidPageLimit(usize),
    #[error("invalid run poll interval {0}; use a positive interval for long-running mode")]
    InvalidRunPollInterval(u64),
    #[error("snapshot page indicated more data but did not return a cursor")]
    MissingSnapshotCursor,
    #[error("unsupported checkpoint version {0}")]
    UnsupportedCheckpointVersion(i64),
    #[error("invalid parquet schema: {0}")]
    InvalidParquetSchema(String),
    #[error("missing required configuration `{0}`")]
    MissingRequiredConfig(&'static str),
    #[error("invalid staging path: {0}")]
    InvalidStagingPath(String),
    #[error("aws s3 operation failed: {0}")]
    S3(String),
    #[error("failed to initialize telemetry: {0}")]
    TelemetryInit(String),
}
