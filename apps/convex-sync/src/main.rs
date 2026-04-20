use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::PathBuf,
};

use clap::{Args, Parser, Subcommand};
use convex_cdc_core::{
    config::{ConvexConnectionConfig, OutputConfig, OutputFormat},
    convex::{client::ConvexClient, schemas::JsonSchemasQuery},
    errors::AppResult,
    model::schema::SchemaCatalog,
    state::checkpoint_store::FileCheckpointStore,
    sync::{
        delta_sync::{fetch_delta_events, DeltaSyncOptions},
        runner::ExportRunner,
        snapshot_sync::{fetch_snapshot_events, SnapshotSyncOptions},
    },
    telemetry::{logging, metrics},
};
use convex_target_s3::{
    publish::{publish_staging_to_s3, PublishS3Options},
    service::{run_service, RunOptions},
    sink::{
        jsonl::{write_jsonl_stream, write_value},
        parquet::ParquetRawChangeLogWriter,
    },
    staging::materialize::{MaterializeStagingOptions, StagingMaterializer},
};
use url::Url;

const CLI_VERSION: &str = match option_env!("CONVEX_SYNC_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};

#[derive(Debug, Parser)]
#[command(author, version = CLI_VERSION, about = "Convex CDC sync CLI")]
struct Cli {
    #[command(flatten)]
    connection: ConnectionArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Args)]
struct ConnectionArgs {
    #[arg(long, env = "CONVEX_DEPLOYMENT_URL", hide_env_values = true)]
    deployment_url: Option<Url>,

    #[arg(long, env = "CONVEX_DEPLOY_KEY", hide_env_values = true)]
    deploy_key: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Schemas(SchemasArgs),
    Snapshot(SnapshotArgs),
    Deltas(DeltasArgs),
    SyncOnce(SyncOnceArgs),
    MaterializeStaging(MaterializeStagingArgs),
    PublishS3(PublishS3Args),
    Run(RunArgs),
}

#[derive(Debug, Args)]
struct OutputArgs {
    #[arg(long)]
    output: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    output_format: OutputFormat,
}

#[derive(Debug, Args)]
struct PaginationArgs {
    #[arg(long, default_value_t = 1)]
    max_pages: usize,

    #[arg(long)]
    all_pages: bool,
}

#[derive(Debug, Args)]
struct SchemasArgs {
    #[arg(long)]
    delta_schema: bool,

    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct SnapshotArgs {
    #[arg(long)]
    table_name: Option<String>,

    #[arg(long)]
    snapshot: Option<i64>,

    #[arg(long)]
    cursor: Option<String>,

    #[arg(long)]
    raw: bool,

    #[command(flatten)]
    pagination: PaginationArgs,

    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct DeltasArgs {
    #[arg(long)]
    cursor: i64,

    #[arg(long)]
    table_name: Option<String>,

    #[arg(long)]
    raw: bool,

    #[command(flatten)]
    pagination: PaginationArgs,

    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Debug, Args)]
struct SyncOnceArgs {
    #[arg(long, default_value = ".memory/raw_change_log")]
    output: PathBuf,

    #[arg(long, default_value = ".memory/raw_change_log.checkpoint.json")]
    checkpoint_path: PathBuf,
}

#[derive(Debug, Args)]
struct MaterializeStagingArgs {
    #[arg(long, default_value = ".memory/raw_change_log")]
    raw_change_log: PathBuf,

    #[arg(long, default_value = ".memory/staging")]
    output: PathBuf,

    #[arg(long)]
    incremental: bool,

    #[arg(long)]
    state_path: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct PublishS3Args {
    #[arg(long, default_value = ".memory/staging")]
    staging_dir: PathBuf,

    #[arg(long)]
    bucket: String,

    #[arg(long)]
    prefix: Option<String>,

    #[arg(long, env = "AWS_REGION")]
    region: Option<String>,
}

#[derive(Debug, Args)]
struct RunArgs {
    #[arg(long, default_value = ".memory/raw_change_log")]
    output: PathBuf,

    #[arg(long, default_value = ".memory/raw_change_log.checkpoint.json")]
    checkpoint_path: PathBuf,

    #[arg(long, default_value = ".memory/staging")]
    staging_dir: PathBuf,

    #[arg(long)]
    staging_state_path: Option<PathBuf>,

    #[arg(long)]
    bucket: String,

    #[arg(long)]
    prefix: Option<String>,

    #[arg(long, env = "AWS_REGION")]
    region: Option<String>,

    #[arg(long, default_value_t = 30)]
    poll_interval_secs: u64,

    #[arg(long)]
    max_iterations: Option<usize>,
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let _ = dotenvy::dotenv();
    logging::install()?;
    metrics::install_noop();

    let cli = Cli::parse();

    match cli.command {
        Command::Schemas(args) => handle_schemas(&build_client(&cli.connection)?, args).await?,
        Command::Snapshot(args) => handle_snapshot(&build_client(&cli.connection)?, args).await?,
        Command::Deltas(args) => handle_deltas(&build_client(&cli.connection)?, args).await?,
        Command::SyncOnce(args) => handle_sync_once(&build_client(&cli.connection)?, args).await?,
        Command::MaterializeStaging(args) => handle_materialize_staging(args).await?,
        Command::PublishS3(args) => handle_publish_s3(args).await?,
        Command::Run(args) => handle_run(&build_client(&cli.connection)?, args).await?,
    }

    Ok(())
}

async fn handle_schemas(client: &ConvexClient, args: SchemasArgs) -> AppResult<()> {
    let response = client
        .json_schemas(&JsonSchemasQuery {
            delta_schema: args.delta_schema,
        })
        .await?;

    let mut writer = open_writer(&OutputConfig {
        output_path: args.output.output,
        format: args.output.output_format,
    })?;
    write_value(&mut writer, &response.payload, args.output.output_format)?;
    writer.flush()?;
    Ok(())
}

async fn handle_snapshot(client: &ConvexClient, args: SnapshotArgs) -> AppResult<()> {
    let max_pages = resolve_page_limit(&args.pagination);
    let output = OutputConfig {
        output_path: args.output.output,
        format: args.output.output_format,
    };

    let mut writer = open_writer(&output)?;
    if args.raw {
        let response = client
            .list_snapshot(&convex_cdc_core::convex::snapshot::ListSnapshotQuery {
                snapshot: args.snapshot,
                cursor: args.cursor,
                table_name: args.table_name,
            })
            .await?;
        write_value(&mut writer, &response, output.format)?;
    } else {
        let schemas = load_schema_catalog(client).await?;
        let result = fetch_snapshot_events(
            client,
            &schemas,
            &SnapshotSyncOptions {
                table_name: args.table_name,
                snapshot: args.snapshot,
                cursor: args.cursor,
                max_pages,
            },
        )
        .await?;

        match output.format {
            OutputFormat::Json => write_value(&mut writer, &result.events, output.format)?,
            OutputFormat::Jsonl => write_jsonl_stream(&mut writer, &result.events)?,
        }
    }

    writer.flush()?;
    Ok(())
}

async fn handle_deltas(client: &ConvexClient, args: DeltasArgs) -> AppResult<()> {
    let max_pages = resolve_page_limit(&args.pagination);
    let output = OutputConfig {
        output_path: args.output.output,
        format: args.output.output_format,
    };

    let mut writer = open_writer(&output)?;
    if args.raw {
        let response = client
            .document_deltas(&convex_cdc_core::convex::deltas::DocumentDeltasQuery {
                cursor: args.cursor,
                table_name: args.table_name,
            })
            .await?;
        write_value(&mut writer, &response, output.format)?;
    } else {
        let schemas = load_schema_catalog(client).await?;
        let result = fetch_delta_events(
            client,
            &schemas,
            &DeltaSyncOptions {
                cursor: args.cursor,
                table_name: args.table_name,
                max_pages,
            },
        )
        .await?;

        match output.format {
            OutputFormat::Json => write_value(&mut writer, &result.events, output.format)?,
            OutputFormat::Jsonl => write_jsonl_stream(&mut writer, &result.events)?,
        }
    }

    writer.flush()?;
    Ok(())
}

async fn handle_sync_once(client: &ConvexClient, args: SyncOnceArgs) -> AppResult<()> {
    let schemas = load_schema_catalog(client).await?;
    let runner = ExportRunner::new(client.clone(), schemas);
    let checkpoint_store = FileCheckpointStore::new(&args.checkpoint_path);
    let mut writer = ParquetRawChangeLogWriter::new(args.output);
    let summary = runner.sync_once(&checkpoint_store, &mut writer).await?;

    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    write_value(&mut writer, &summary, OutputFormat::Json)?;
    writer.flush()?;
    Ok(())
}

async fn handle_materialize_staging(args: MaterializeStagingArgs) -> AppResult<()> {
    let summary = StagingMaterializer::materialize(&MaterializeStagingOptions {
        raw_change_log_dir: args.raw_change_log,
        output_dir: args.output,
        incremental: args.incremental,
        state_path: args.state_path,
    })?;

    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    write_value(&mut writer, &summary, OutputFormat::Json)?;
    writer.flush()?;
    Ok(())
}

async fn handle_publish_s3(args: PublishS3Args) -> AppResult<()> {
    let summary = publish_staging_to_s3(&PublishS3Options {
        staging_dir: args.staging_dir,
        bucket: args.bucket,
        prefix: args.prefix,
        region: args.region,
    })
    .await?;

    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    write_value(&mut writer, &summary, OutputFormat::Json)?;
    writer.flush()?;
    Ok(())
}

async fn handle_run(client: &ConvexClient, args: RunArgs) -> AppResult<()> {
    let summary = run_service(
        client,
        &RunOptions {
            raw_change_log_path: args.output,
            checkpoint_path: args.checkpoint_path,
            staging_output_dir: args.staging_dir,
            staging_state_path: args.staging_state_path,
            publish_bucket: args.bucket,
            publish_prefix: args.prefix,
            publish_region: args.region,
            poll_interval: std::time::Duration::from_secs(args.poll_interval_secs),
            max_iterations: args.max_iterations,
        },
    )
    .await?;

    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    write_value(&mut writer, &summary, OutputFormat::Json)?;
    writer.flush()?;
    Ok(())
}

async fn load_schema_catalog(client: &ConvexClient) -> AppResult<SchemaCatalog> {
    let response = client
        .json_schemas(&JsonSchemasQuery { delta_schema: true })
        .await?;
    Ok(SchemaCatalog::from_json_schemas(&response.payload))
}

fn build_client(connection: &ConnectionArgs) -> AppResult<ConvexClient> {
    let deployment_url = connection.deployment_url.clone().ok_or(
        convex_cdc_core::errors::AppError::MissingRequiredConfig("CONVEX_DEPLOYMENT_URL"),
    )?;
    let deploy_key = connection.deploy_key.clone().ok_or(
        convex_cdc_core::errors::AppError::MissingRequiredConfig("CONVEX_DEPLOY_KEY"),
    )?;
    ConvexClient::new(ConvexConnectionConfig::new(deployment_url, deploy_key)?)
}

fn resolve_page_limit(pagination: &PaginationArgs) -> usize {
    if pagination.all_pages {
        usize::MAX
    } else {
        pagination.max_pages
    }
}

fn open_writer(output: &OutputConfig) -> AppResult<Box<dyn Write>> {
    match &output.output_path {
        Some(path) => {
            let file = File::create(path)?;
            Ok(Box::new(BufWriter::new(file)))
        },
        None => Ok(Box::new(BufWriter::new(io::stdout()))),
    }
}
