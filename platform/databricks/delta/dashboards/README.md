# Databricks Delta Dashboards

Lakeview dashboard assets for monitoring the Convex sync at a high level.

Files:

- `dashboards.json`: lightweight manifest for the included dashboards
- `convex_sync_overview.lvdash.json.tmpl`: renderable Lakeview dashboard template

Use the helper scripts:

```bash
./scripts/render-databricks-delta-dashboard.sh /tmp/convex-sync-overview.lvdash.json DEFAULT
./scripts/publish-databricks-delta-dashboard.sh DEFAULT <warehouse_id>
```

Pass the Databricks profile to the render helper if you want the generated
Lakeview dashboard to include per-table bronze and silver row counts. Without a
profile, the dashboard still renders, but the row-count dataset is left empty.

If you pass an existing dashboard ID to the publish script, it updates and
re-publishes that dashboard. Otherwise it creates a new one and prints the
dashboard ID.
