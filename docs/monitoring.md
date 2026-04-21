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

## AUTO CDC Status

The current example source now has a real generated Lakeflow `AUTO CDC`
pipeline. Deploy and run it with:

```bash
just databricks-delta-deploy-pipeline DEFAULT prod
just databricks-delta-run-pipeline DEFAULT prod
```

For a newly onboarded source, silver will stay empty until you run that same
deploy/run sequence for that source.

What exists today:

- control schema and checkpoint tables
- bronze CDC landing
- Delta extractor job
- Lakeflow SQL template for per-table `AUTO CDC`
- generated per-source pipeline scripts

What is still missing:

- only the deploy/run step for any additional source you onboard beyond the
  current example profile
