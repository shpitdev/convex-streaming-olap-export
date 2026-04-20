use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::PathBuf,
};

use clap::{Args, Parser, Subcommand};
use convex_sync_core::{
    config::{ConvexConnectionConfig, OutputConfig, OutputFormat},
    convex::{client::ConvexClient, schemas::JsonSchemasQuery},
    errors::{AppError, AppResult},
    model::schema::SchemaCatalog,
    output::{write_jsonl_stream, write_value},
    sync::{
        delta_sync::{fetch_delta_events, DeltaSyncOptions},
        snapshot_sync::{fetch_snapshot_events, SnapshotSyncOptions},
    },
    telemetry::{logging, metrics},
};
use url::Url;

#[derive(Debug, Parser)]
#[command(author, version, about = "Convex source inspection CLI")]
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

#[tokio::main]
async fn main() -> AppResult<()> {
    let _ = dotenvy::dotenv();
    logging::install()?;
    metrics::install_noop();

    let cli = Cli::parse();
    let client = build_client(&cli.connection)?;

    match cli.command {
        Command::Schemas(args) => handle_schemas(&client, args).await?,
        Command::Snapshot(args) => handle_snapshot(&client, args).await?,
        Command::Deltas(args) => handle_deltas(&client, args).await?,
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
            .list_snapshot(&convex_sync_core::convex::snapshot::ListSnapshotQuery {
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
            .document_deltas(&convex_sync_core::convex::deltas::DocumentDeltasQuery {
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

async fn load_schema_catalog(client: &ConvexClient) -> AppResult<SchemaCatalog> {
    let response = client
        .json_schemas(&JsonSchemasQuery { delta_schema: true })
        .await?;
    Ok(SchemaCatalog::from_json_schemas(&response.payload))
}

fn build_client(connection: &ConnectionArgs) -> AppResult<ConvexClient> {
    let deployment_url = connection
        .deployment_url
        .clone()
        .ok_or(AppError::MissingRequiredConfig("CONVEX_DEPLOYMENT_URL"))?;
    let deploy_key = connection
        .deploy_key
        .clone()
        .ok_or(AppError::MissingRequiredConfig("CONVEX_DEPLOY_KEY"))?;
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
