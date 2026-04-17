# Milestone 01

Ship a local, single-process exporter that proves Convex snapshot + delta sync can be landed reliably into `raw_change_log`.

## Deliverables

- CLI that accepts Convex URL, deploy key, output path, and polling settings
- schema fetch from Convex
- full snapshot sync
- persisted checkpoint
- incremental delta polling
- append-only Parquet `raw_change_log`
- restart recovery from checkpoint

## Scope

### In

- one process
- one Convex deployment
- local filesystem output
- one append-only Parquet replication dataset
- structured logs

### Out

- Databricks-specific loading
- Palantir packaging
- Fivetran gRPC server
- `staging` materialization
- multi-tenant orchestration
- downstream `marts`

## Work Breakdown

1. Build a thin Convex client.
   - `json_schemas`
   - `list_snapshot`
   - `document_deltas`

2. Define the internal event model.
   - schema metadata
   - upsert event
   - delete event

3. Implement snapshot sync.
   - page through `list_snapshot`
   - emit each document as an upsert event
   - capture final snapshot timestamp

4. Implement checkpoint persistence.
   - plain JSON file is enough for now
   - save snapshot timestamp after successful snapshot write
   - save delta cursor after successful delta batch write

5. Implement delta sync.
   - poll `document_deltas`
   - emit upsert and delete events
   - checkpoint after durable write

6. Write `raw_change_log`.
   - append-only records
   - include `table_name`, `_id`, `_ts`, `op`, `schema_fingerprint`, `document`

7. Verify restart behavior.
   - stop after snapshot
   - restart and continue deltas
   - stop mid-run
   - restart from last checkpoint

## Suggested Crates

- `tokio` for async runtime
- `reqwest` for HTTP
- `serde` and `serde_json` for API payloads and checkpoints
- `clap` for CLI
- `tracing` and `tracing-subscriber` for logs
- `parquet` and `arrow` for file output

## Acceptance Criteria

- snapshot completes against a real Convex deployment
- deltas continue from the snapshot timestamp
- deletes are preserved in output
- restart resumes from checkpoint
- no manual intervention is needed between runs

## Immediately After This Milestone

Milestone 02 materializes `staging` from `raw_change_log`:

- one current-state table per Convex table
- delete application
- schema projection into typed columns

That is where the repo starts looking like a Fivetran-style managed base-table system.

## Rust Concepts You’ll Hit

- `struct`: named data shape
- `enum`: one value that can be one of several variants, useful for `Upsert` vs `Delete`
- `Result<T, E>`: explicit success/error return type
- `async fn`: function that can wait on IO without blocking the process

That is enough Rust to get through this milestone. Traits and heavier abstractions can wait.
