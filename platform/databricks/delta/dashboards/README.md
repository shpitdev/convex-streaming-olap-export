# Databricks Delta Dashboards

Lakeview dashboard assets for monitoring the Convex sync at a high level.

Files:

- `dashboards.json`: lightweight manifest for the included dashboards
- `convex_sync_overview.lvdash.json.tmpl`: renderable Lakeview dashboard template

Use the helper scripts:

```bash
./scripts/render-databricks-delta-dashboard.sh /tmp/convex-sync-overview.lvdash.json
./scripts/publish-databricks-delta-dashboard.sh DEFAULT <warehouse_id>
```

If you pass an existing dashboard ID to the publish script, it updates and
re-publishes that dashboard. Otherwise it creates a new one and prints the
dashboard ID.
