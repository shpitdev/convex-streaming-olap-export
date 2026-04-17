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

Add destination-specific loading and packaging.

Examples:

- Databricks ingestion from `staging`
- Palantir compute-module packaging
- optional Fivetran-compatible runtime wrapper

The intent is to keep these as adapters around the core exporter, not as the exporter itself.
