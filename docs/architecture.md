# Architecture

## Shape

This repo has three explicit layers:

- `core`: Convex extraction and checkpoint semantics
- `target-s3`: raw parquet, staging parquet, and S3 publish
- `platform/databricks`: Databricks assets for both S3-backed consumption and Databricks-native landing

The shared extraction logic stays target-agnostic. Targets decide how event
batches are durably written and what downstream shape they expose.

## Extraction Model

The repo stays aligned with the public Convex/Fivetran extraction contract:

1. fetch schemas
2. walk a consistent snapshot with `list_snapshot`
3. persist `snapshot + cursor` while the snapshot is incomplete
4. hand off from the final snapshot timestamp to `document_deltas`
5. treat deletes as first-class events
6. advance checkpoints only after durable writes succeed

That is the stable core. Scheduling, warehouse maintenance, and consumer
integration differ per target family.

## Shared Core

The core layer owns:

- Convex HTTP calls
- schema fetch + fingerprinting
- `ChangeEvent` normalization
- checkpoint state transitions
- snapshot orchestration
- delta orchestration

It should not know anything about:

- S3
- Databricks
- Foundry
- Unity Catalog
- Terraform

## `S3/export`

```text
Convex
  -> raw_change_log parquet
  -> staging parquet
  -> S3 publish
```

Owned pieces:

- append-only raw parquet sink
- checkpoint file
- staging materializer
- staging state
- S3 publish manifest

Use it when you want:

- replayable local artifacts
- a target-agnostic export contract
- another platform to consume S3 directly

## `Databricks/native`

```text
Convex
  -> bronze CDC Delta tables
  -> checkpoint Delta table
  -> Lakeflow AUTO CDC
  -> silver current-state Delta tables
```

Owned pieces:

- extractor job entrypoint
- control/checkpoint schema
- bronze CDC tables
- Lakeflow `AUTO CDC` templates

Use it when:

- Databricks is the primary serving layer
- you want Databricks-native CDC reconstruction
- downstream consumers can read Unity Catalog tables directly

## Data Shapes

### `raw_change_log`

Append-only replication history.

- one row per source change event
- restart-safe and replayable
- preserves multiple updates to the same document

### `staging`

Source-conformed current-state tables derived from `raw_change_log`.

- one current row per source `_id`
- deletes applied
- source-centric shape, not business-centric

### `bronze CDC`

Append-only CDC landing in Delta.

- one row per source change event
- explicit key, sequence, and delete columns
- intended for Lakeflow `AUTO CDC`

### `silver`

Current-state Delta tables derived from bronze CDC.

- one current row per source key
- resolved with Databricks-native CDC semantics

## Checkpoints

One logical checkpoint per source is still enough:

- during snapshot: `InitialSnapshot { snapshot, cursor }`
- during delta tail: `DeltaTail { cursor }`

Rules:

- after partial snapshot pages, save `snapshot + cursor`
- after the final snapshot page, save the snapshot timestamp as the initial delta cursor
- after each successful delta batch write, save the returned delta cursor
- never advance before the target write succeeds

Target storage differs:

- `S3/export`: file-backed JSON
- `Databricks/native`: Delta control table

## Boundary

This repo should own:

- Convex extraction
- checkpoint semantics
- S3/export target code
- Databricks-native landing assets

Downstream systems should own:

- business marts
- semantic models
- BI-facing denormalization
- app-specific joins
