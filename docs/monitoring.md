# Monitoring

## Databricks Delta Dashboard

This repo now includes a Lakeview dashboard template for the Databricks Delta
path:

- `platform/databricks/delta/dashboards/convex_sync_overview.lvdash.json.tmpl`
- `platform/databricks/delta/dashboards/dashboards.json`

Publish it with:

```bash
just databricks-delta-publish-dashboard DEFAULT <warehouse_id>
```

If you pass an existing dashboard ID to the publish script, it updates and
re-publishes that dashboard. Otherwise it creates a new one and prints the new
dashboard ID.

The first dashboard focuses on:

- latest checkpoint freshness
- bronze table count
- silver table count
- recent checkpoint history
- bronze and silver table inventory

## Why Silver Is Empty

Silver is still expected to be empty until a real Lakeflow `AUTO CDC` pipeline
is deployed.

What exists today:

- control schema and checkpoint tables
- bronze CDC landing
- Delta extractor job
- Lakeflow SQL template for per-table `AUTO CDC`

What is still missing:

- a source-aware pipeline generation and deploy path that turns the bronze
  tables for a specific source into a real deployed Lakeflow pipeline

The generic blocker is not Lakeflow itself. It is the missing code path that:

1. enumerates the bronze tables for one source
2. decides the silver target names consistently
3. renders the per-table `AUTO CDC` SQL
4. deploys or updates the pipeline repeatably
