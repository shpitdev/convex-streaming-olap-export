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

- checkpoint freshness and write volume
- bronze table count
- silver table count
- recent checkpoint history
- per-table bronze vs silver record counts
- a side-by-side bronze/silver table map

The template filters out internal Lakeflow objects from the layer counts,
places recent checkpoints and per-table record counts side by side, and keeps
the bronze/silver map full width below.

## AUTO CDC Status

The current example source now has a real generated Lakeflow `AUTO CDC`
pipeline. Deploy and run it with:

```bash
just databricks-delta-deploy-pipeline DEFAULT prod
just databricks-delta-run-pipeline DEFAULT prod
```

For a newly onboarded source, silver will stay empty until you run that same
deploy/run sequence for that source.

Once deployed:

- the extractor job runs every 5 minutes on a Databricks job schedule
- the Lakeflow pipeline stays continuous after its first `run`

If you want a no-touch proof loop, add a tiny heartbeat write in the upstream
Convex app on a 1-minute cron. That gives Databricks a steady stream of real
changes to ingest and makes the dashboard visibly move without manual reruns.

What exists today:

- control schema and checkpoint tables
- bronze CDC landing
- Delta extractor job
- Lakeflow SQL template for per-table `AUTO CDC`
- generated per-source pipeline scripts

What is still missing:

- only the deploy/run step for any additional source you onboard beyond the
  current example profile
