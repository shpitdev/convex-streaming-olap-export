# Architecture

## Project Shape

This repo should start as an independent exporter with one concrete source and one concrete sink:

- source: Convex streaming export API
- sink: Parquet files

But the output contract has named layers:

- `raw_change_log`
- `staging`
- `marts`

Only the first two are owned here. `marts` are usually downstream.

## Behavioral Target

We want to stay close to the upstream Convex/Fivetran export behavior:

1. Fetch schemas.
2. Walk a full consistent snapshot with `list_snapshot`.
3. Save the returned snapshot timestamp.
4. Poll `document_deltas` from that timestamp forward.
5. Treat deletes as first-class events.
6. Only advance checkpoint after output is durably written.

That gives us compatibility at the sync-model level without inheriting the Fivetran runtime model.

## Named Layers

### `raw_change_log`

Durable append-only replication history.

Properties:

- one row per source change event
- restart-safe and replayable
- flexible enough to tolerate schema drift
- not meant to be the final analyst-facing shape

### `staging`

Source-conformed current-state tables materialized from `raw_change_log`.

Properties:

- one current row per source `_id`
- deletes have already been applied
- columns are projected into a cleaner table shape
- still source-centric, not business-centric

This is the closest analog to Fivetran-managed base tables.

### `marts`

Business-centric downstream models built from `staging`.

Properties:

- joins across entities
- denormalized reporting tables
- metrics-oriented facts and dimensions

These usually belong in dbt or the target warehouse, not in the exporter runtime.

### `intermediate`

Optional helper layer between `staging` and `marts`.

Use it only if a transformation is reusable but not yet a final business model.

## Boundaries

### Source Layer

Responsible for:

- calling Convex HTTP endpoints
- authenticating with a deploy key
- decoding schemas
- decoding snapshot pages
- decoding delta pages

It should not know anything about Databricks or Palantir.

### Sync Engine

Responsible for:

- initial sync orchestration
- incremental sync orchestration
- retry behavior
- checkpoint rules
- idempotency rules

This is the core of the repo.

### Sink Layer

Responsible for:

- translating change events into Parquet rows
- file rotation / partitioning
- durable flush behavior

For now, keep this sink concrete. Do not build a plugin system yet.

### Materializer

Responsible for:

- replaying `raw_change_log`
- resolving the latest state per `_id`
- applying deletes
- projecting rows into `staging`

This layer still belongs in this repo. It is destination-agnostic data shaping, not business modeling.

## Ownership Boundary

This repo should own:

- Convex extraction
- checkpointing
- `raw_change_log`
- `staging`

Downstream systems should usually own:

- `intermediate`
- `marts`
- BI-specific denormalization
- metrics and semantic modeling

## `raw_change_log` Shape

Start with an append-only replication log instead of trying to materialize final destination tables immediately.

Suggested records:

- `table_name`
- `_id`
- `_ts`
- `op` where `op in {upsert, delete}`
- `schema_fingerprint`
- `document` as JSON payload for upserts

Why:

- simple to write
- simple to recover
- easy to replay into another sink later
- preserves full change history

## Materialization Behavior

Once data lands in `raw_change_log`, the flow is:

1. Read events for a table in event order.
2. For each `_id`, keep the latest surviving event.
3. If the latest event is `delete`, remove that row from `staging`.
4. If the latest event is `upsert`, write the projected row into `staging`.
5. Let downstream systems build `marts` from `staging`.

`staging` can be rebuilt from scratch from `raw_change_log`, or updated incrementally once the mechanics are reliable.

## Checkpoint Strategy

Single checkpoint is enough for v0:

- last fully committed delta cursor

Rules:

- after snapshot completes, save the snapshot timestamp as the initial delta cursor
- after each successful delta batch write, save the returned cursor
- never advance checkpoint before data is flushed successfully

If the process crashes after writing but before checkpointing, reprocessing a small delta window is acceptable as long as downstream replay can tolerate duplicates or we make file commit atomic.

## Failure Model

We should design for:

- process restart
- transient Convex API failures
- partial page write failure
- schema drift between runs

We do not need distributed coordination in v0. A single worker process is enough.

## Schema Evolution

The safest split is:

- keep `raw_change_log` schema stable
- let `staging` absorb most source schema projection rules

### Additive changes

If Convex adds a field:

- `raw_change_log`: new payload includes the field, no envelope change required
- `staging`: add the new column, backfill old rows as `NULL`

### Field removal

If Convex removes a field:

- `raw_change_log`: old events still contain it, new ones do not
- `staging`: keep the column nullable for a compatibility window, then remove it deliberately

### Field rename

Treat rename as:

- add new field
- deprecate old field

Do not try to infer renames automatically.

### Type change

Widening changes are usually manageable.

Examples:

- `int -> long`
- narrower string constraint -> wider string constraint

Narrowing or incompatible changes should require an explicit rule in the materializer and may require a `staging` rebuild.

### Primary key identity

For v0, assume Convex `_id` is the stable identity. If identity semantics change, that should trigger a deliberate rebuild rather than an automatic migration.

## Future Layers

These should stay out of the first slice:

- `marts`
- S3-backed warehouse-specific loaders
- Databricks loader
- object-store sink manager
- Palantir compute-module packaging
- metrics backend integration

Those are adapters around the core exporter, not the exporter itself.

## Rust Guidance

Keep the first version boring:

- concrete structs over traits
- one binary crate
- async HTTP client
- plain JSON checkpoint file
- local filesystem output first

That will make the code easier to learn and easier to change.
