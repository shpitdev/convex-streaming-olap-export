use tracing_subscriber::EnvFilter;

use crate::errors::{AppError, AppResult};

pub fn install() -> AppResult<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .try_init()
        .map_err(|err| AppError::TelemetryInit(err.to_string()))
}
