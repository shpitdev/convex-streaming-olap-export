# convex-streaming-olap-export

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
- `just verify`
- `just install-hooks`
- `just sync-once`
- `just materialize-staging`
- `just depot-ci --job fmt`

`sync-once` writes deterministic Parquet batch files under `.memory/raw_change_log/`
and stores checkpoint state in `.memory/raw_change_log.checkpoint.json` by default.

`materialize-staging` rebuilds source-conformed current-state Parquet tables under
`.memory/staging/` from the Parquet `raw_change_log` dataset.

## Quality Gates

Local:

- `just install-hooks` configures a repo-local pre-commit hook
- the hook runs `just verify`

Remote:

- `.github/workflows/ci.yml` is the baseline GitHub workflow
- `.depot/workflows/ci.yml` is the preferred Depot CI workflow

During rollout, keep both until the Depot path is fully trusted.

## References

- [Convex streaming export docs](https://docs.convex.dev/production/integrations/streaming-import-export)
- [Convex streaming export API](https://docs.convex.dev/streaming-export-api)
- [Upstream Convex `fivetran_source` crate](https://github.com/get-convex/convex-backend/tree/main/crates/fivetran_source)
- [Palantir compute modules overview](https://www.palantir.com/docs/foundry/compute-modules/overview)
