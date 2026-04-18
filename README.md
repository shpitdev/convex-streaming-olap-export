# convex-streaming-olap-export

Language: ![Rust](https://img.shields.io/badge/Rust-000000?logo=rust&logoColor=white)
Source/API: ![Convex](https://img.shields.io/badge/Convex-EE342F?logo=convex&logoColor=white)
Data/Storage: ![Apache Arrow](https://img.shields.io/badge/Apache%20Arrow-4B5563) ![Parquet](https://img.shields.io/badge/Parquet-1F2937) ![Amazon S3](https://img.shields.io/badge/Amazon%20S3-569A31?logo=amazons3&logoColor=white)
Platforms/IaC: ![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white) ![Terraform](https://img.shields.io/badge/Terraform-844FBA?logo=terraform&logoColor=white)

Independent Convex-to-OLAP export with a Parquet-first sink.

The intent is to stay behaviorally close to Convex's existing streaming export model and the upstream `fivetran_source` connector, while remaining fully independent of Fivetran as a runtime and product dependency.

## Goal

Build a small Rust service that:

- reads Convex schemas and data through the streaming export APIs
- performs an initial snapshot sync
- continuously applies document deltas
- writes durable Parquet outputs plus checkpoints

If this is reliable, we can add destination-specific packaging later for systems like Databricks and Palantir.

## Non-goals for v0

- implementing the Fivetran gRPC server
- building a Palantir compute module
- writing directly into Databricks tables
- supporting every possible sink up front

## Design Principles

- Match Convex/Fivetran sync semantics where practical.
- Keep the core exporter independent from any host platform.
- Prefer an append-only Parquet replication layer before destination-specific loaders.
- Checkpoint only after successful durable writes.
- Delay abstractions until there is a second real sink or runtime.

## Named Layers

- `raw_change_log`: append-only replication history written by this repo
- `staging`: source-conformed current-state tables materialized from `raw_change_log`
- `marts`: business-centric downstream models, usually built outside this repo

`intermediate` is optional and should only appear if we need helper models between `staging` and `marts`.

## Architecture

Core:
- Convex `json_schemas`
- Convex `list_snapshot`
- Convex `document_deltas`
- schema fetch
- snapshot walker
- delta poller
- checkpoint manager
- Parquet writer
- staging materializer

Outputs:
- `raw_change_log`
- `staging`
- checkpoint state
- sync logs / metrics

More detail: [docs/architecture.md](docs/architecture.md)

## First Chunk

Milestone 01 is intentionally narrow. It ends at `raw_change_log`:

1. Fetch Convex schemas.
2. Run a full snapshot sync into the `raw_change_log`.
3. Persist the returned snapshot cursor/timestamp as the initial delta checkpoint.
4. Run delta sync from that checkpoint.
5. Write inserts, updates, and deletes into an append-only Parquet replication dataset.
6. Restart cleanly from checkpoint without duplicating committed output.

The next milestone materializes `staging` from `raw_change_log`.

Detailed breakdown:

- [docs/milestone-01.md](docs/milestone-01.md)
- [docs/roadmap.md](docs/roadmap.md)

## Why This Shape

Convex already exposes the core primitives we need for export. The upstream connector model is:

- bootstrap from `list_snapshot`
- continue from `document_deltas`

That is the part worth copying first. The Fivetran gRPC server is an integration wrapper, not the core export logic. Fivetran-style downstream modeling maps more closely to `staging` and `marts` than to a single monolithic export job.

## Tentative Rust Stack

- `tokio`
- `reqwest`
- `serde`
- `clap`
- `tracing`
- `arrow`
- `parquet`

## Local Env

The CLI reads a local `.env` file automatically.

Fill in:

- `CONVEX_DEPLOYMENT_URL`
- `CONVEX_DEPLOY_KEY`
- optional `RUST_LOG`

Then run commands like:

- `cargo run --bin convex-export -- schemas`
- `cargo run --bin convex-export -- snapshot --table-name users`
- `cargo run --bin convex-export -- deltas --cursor 0`
- `cargo run --bin convex-export -- sync-once`
- `cargo run --bin convex-export -- materialize-staging`
- `cargo run --bin convex-export -- publish-s3 --bucket your-bucket`
- `cargo run --bin convex-export -- run --bucket your-bucket`
- `just verify`
- `just install-hooks`
- `just sync-once`
- `just materialize-staging`
- `just publish-s3 --bucket your-bucket`
- `just run --bucket your-bucket`
- `just depot-ci --job fmt`

`sync-once` writes deterministic Parquet batch files under `.memory/raw_change_log/`
and stores checkpoint state in `.memory/raw_change_log.checkpoint.json` by default.

`materialize-staging` rebuilds source-conformed current-state Parquet tables under
`.memory/staging/` from the Parquet `raw_change_log` dataset.

Use `materialize-staging --incremental` to update only the tables affected by
new raw Parquet batches, while keeping the full rebuild path available as the
correctness fallback.

`publish-s3` uploads those local `staging` parquet files to S3 with:

- stable `staging/current/...` table paths
- versioned `staging/versions/<publish_id>/...` snapshots
- `staging/manifests/latest.json` as the publish pointer

`run` is the maintained long-running service mode. Each loop does:

1. `sync-once`
2. `materialize-staging --incremental`
3. `publish-s3` if the staging step produced a real change

Use `--poll-interval-secs` to control the sleep between loops. For bounded smoke
tests, `--max-iterations 1` runs one full cycle and exits.

AWS bootstrap templates live under `ops/aws/`. Snapshot them into `.memory/`
before running Terraform or emitting access keys:

- `just aws-template-snapshot`

Databricks bootstrap templates live under `ops/databricks/`. Snapshot them into
`.memory/` before running Terraform:

- `just databricks-template-snapshot`

Databricks landing sync lives in `ops/databricks/sql/` plus `scripts/` and is
intentionally kept outside Terraform. It uses the published S3 manifest to
create a schema of stable views over `staging/current/...` parquet files:

- `just databricks-sync-staging-views --warehouse-id <warehouse-id> --bucket <bucket> --prefix <prefix>`

Defaults:

- catalog: `workspace`
- schema: `convex_streaming_olap_export`
- profile: `DEFAULT`

The sync renders SQL and statement results into `.memory/databricks-view-sync/`
before applying them.

VS Code recommendations live in `.vscode/extensions.json`, including Rust,
Terraform, Databricks, and Data Wrangler.

## Quality Gates

Local:

- `just install-hooks` configures a repo-local pre-commit hook
- the hook runs `just verify`

Remote:

- `.depot/workflows/ci.yml` is the single fmt/clippy/test CI workflow
- `.github/workflows/semgrep.yml` is the lightweight security scan workflow

## References

- [Convex streaming export docs](https://docs.convex.dev/production/integrations/streaming-import-export)
- [Convex streaming export API](https://docs.convex.dev/streaming-export-api)
- [Upstream Convex `fivetran_source` crate](https://github.com/get-convex/convex-backend/tree/main/crates/fivetran_source)
- [Palantir compute modules overview](https://www.palantir.com/docs/foundry/compute-modules/overview)
