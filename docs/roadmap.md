# Roadmap

## Layer Contract

- `raw_change_log`: durable append-only replication history
- `staging`: source-conformed current-state tables rebuilt or incrementally maintained from `raw_change_log`
- `marts`: business-centric downstream models, typically owned by the warehouse or dbt project

`intermediate` is optional and only exists when `staging -> marts` needs helper steps.

## Milestone 01

Land Convex snapshot + delta replication into `raw_change_log`.

Outcome:

- reliable extraction
- durable checkpoints
- restart-safe replication history

Reference: [milestone-01.md](milestone-01.md)

## Milestone 02

Materialize `staging` from `raw_change_log`.

Outcome:

- one current-state table per Convex table
- delete handling applied
- stable source-conformed contract for downstream systems
- explicit schema projection rules

## Milestone 03

Publish real `staging` table files to S3.

Outcome:

- versioned S3 publishes plus a latest manifest
- stable S3 parquet table files for downstream readers
- incremental publish by changed table file, with full rebuild still allowed

## Milestone 04

Add destination-specific readers and loaders on top of the S3 contract.

Examples:

- Databricks ingestion from `staging`
- Palantir compute-module packaging

The intent is to keep these as adapters around the S3-backed exporter contract, not as the exporter itself.
