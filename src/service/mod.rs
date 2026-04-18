use std::{path::PathBuf, time::Duration};

use serde::Serialize;
use tokio::time::sleep;
use tracing::info;

use crate::{
    convex::{client::ConvexClient, schemas::JsonSchemasQuery},
    errors::{AppError, AppResult},
    model::schema::SchemaCatalog,
    publish::{publish_staging_to_s3, PublishS3Options, PublishS3Summary},
    staging::materialize::{
        MaterializeStagingOptions, MaterializeStagingSummary, StagingMaterializer,
    },
    state::checkpoint_store::FileCheckpointStore,
    sync::runner::{ExportRunner, SyncOnceOptions, SyncOnceSummary},
};

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub raw_change_log_path: PathBuf,
    pub checkpoint_path: PathBuf,
    pub staging_output_dir: PathBuf,
    pub staging_state_path: Option<PathBuf>,
    pub publish_bucket: String,
    pub publish_prefix: Option<String>,
    pub publish_region: Option<String>,
    pub poll_interval: Duration,
    pub max_iterations: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunIterationSummary {
    pub iteration: usize,
    pub sync: SyncOnceSummary,
    pub staging: MaterializeStagingSummary,
    pub publish: Option<PublishS3Summary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub iterations_completed: usize,
    pub stop_reason: String,
    pub last_iteration: Option<RunIterationSummary>,
}

pub async fn run_service(client: &ConvexClient, options: &RunOptions) -> AppResult<RunSummary> {
    validate_run_options(options)?;

    let mut iterations_completed = 0usize;
    let mut last_iteration: Option<RunIterationSummary>;

    loop {
        let iteration = iterations_completed + 1;
        let iteration_summary = run_iteration(client, options, iteration).await?;
        log_iteration(&iteration_summary);
        last_iteration = Some(iteration_summary);
        iterations_completed = iteration;

        if options
            .max_iterations
            .is_some_and(|max_iterations| iterations_completed >= max_iterations)
        {
            return Ok(RunSummary {
                iterations_completed,
                stop_reason: "max_iterations_reached".to_string(),
                last_iteration,
            });
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                return Ok(RunSummary {
                    iterations_completed,
                    stop_reason: "ctrl_c".to_string(),
                    last_iteration,
                });
            }
            _ = sleep(options.poll_interval) => {}
        }
    }
}

async fn run_iteration(
    client: &ConvexClient,
    options: &RunOptions,
    iteration: usize,
) -> AppResult<RunIterationSummary> {
    let schemas = load_schema_catalog(client).await?;
    let runner = ExportRunner::new(client.clone(), schemas);
    let checkpoint_store = FileCheckpointStore::new(&options.checkpoint_path);
    let sync = runner
        .sync_once(
            &checkpoint_store,
            &SyncOnceOptions {
                raw_change_log_path: options.raw_change_log_path.clone(),
                checkpoint_path: options.checkpoint_path.clone(),
            },
        )
        .await?;
    let staging = StagingMaterializer::materialize(&MaterializeStagingOptions {
        raw_change_log_dir: options.raw_change_log_path.clone(),
        output_dir: options.staging_output_dir.clone(),
        incremental: true,
        state_path: options.staging_state_path.clone(),
    })?;
    let publish = if should_publish(&staging) {
        Some(
            publish_staging_to_s3(&PublishS3Options {
                staging_dir: options.staging_output_dir.clone(),
                bucket: options.publish_bucket.clone(),
                prefix: options.publish_prefix.clone(),
                region: options.publish_region.clone(),
            })
            .await?,
        )
    } else {
        None
    };

    Ok(RunIterationSummary {
        iteration,
        sync,
        staging,
        publish,
    })
}

async fn load_schema_catalog(client: &ConvexClient) -> AppResult<SchemaCatalog> {
    let response = client
        .json_schemas(&JsonSchemasQuery { delta_schema: true })
        .await?;
    Ok(SchemaCatalog::from_json_schemas(&response.payload))
}

fn validate_run_options(options: &RunOptions) -> AppResult<()> {
    if options.poll_interval.is_zero() && options.max_iterations.is_none() {
        return Err(AppError::InvalidRunPollInterval(0));
    }
    Ok(())
}

fn should_publish(staging: &MaterializeStagingSummary) -> bool {
    staging.new_raw_files > 0 || staging.affected_tables > 0 || staging.tables_materialized > 0
}

fn log_iteration(summary: &RunIterationSummary) {
    match &summary.publish {
        Some(publish) => info!(
            iteration = summary.iteration,
            events_written = summary.sync.events_written,
            staging_mode = summary.staging.mode,
            affected_tables = summary.staging.affected_tables,
            tables_uploaded = publish.tables_uploaded,
            tables_deleted = publish.tables_deleted,
            tables_unchanged = publish.tables_unchanged,
            publish_id = publish.publish_id,
            "completed run iteration"
        ),
        None => info!(
            iteration = summary.iteration,
            events_written = summary.sync.events_written,
            staging_mode = summary.staging.mode,
            affected_tables = summary.staging.affected_tables,
            "completed run iteration without S3 publish"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use super::{should_publish, validate_run_options, RunOptions};
    use crate::staging::materialize::MaterializeStagingSummary;

    fn sample_options() -> RunOptions {
        RunOptions {
            raw_change_log_path: PathBuf::from(".memory/raw_change_log"),
            checkpoint_path: PathBuf::from(".memory/raw_change_log.checkpoint.json"),
            staging_output_dir: PathBuf::from(".memory/staging"),
            staging_state_path: None,
            publish_bucket: "bucket".to_string(),
            publish_prefix: Some("prefix".to_string()),
            publish_region: Some("us-east-1".to_string()),
            poll_interval: Duration::from_secs(30),
            max_iterations: None,
        }
    }

    fn sample_staging_summary() -> MaterializeStagingSummary {
        MaterializeStagingSummary {
            mode: "incremental".to_string(),
            raw_change_log_dir: PathBuf::from(".memory/raw_change_log"),
            output_dir: PathBuf::from(".memory/staging"),
            files_read: 0,
            events_read: 0,
            new_raw_files: 0,
            affected_tables: 0,
            tables_materialized: 0,
            rows_materialized: 0,
        }
    }

    #[test]
    fn rejects_zero_interval_for_unbounded_run() {
        let mut options = sample_options();
        options.poll_interval = Duration::from_secs(0);

        let err = validate_run_options(&options).unwrap_err();
        assert!(err.to_string().contains("invalid run poll interval"));
    }

    #[test]
    fn allows_zero_interval_for_bounded_run() {
        let mut options = sample_options();
        options.poll_interval = Duration::from_secs(0);
        options.max_iterations = Some(1);

        validate_run_options(&options).unwrap();
    }

    #[test]
    fn skips_publish_for_incremental_no_op() {
        let summary = sample_staging_summary();
        assert!(!should_publish(&summary));
    }

    #[test]
    fn publishes_when_tables_changed() {
        let mut summary = sample_staging_summary();
        summary.affected_tables = 1;
        assert!(should_publish(&summary));
    }
}
