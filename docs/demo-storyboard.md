# Demo Storyboard

Use this when capturing screenshots or assembling a short walkthrough of the pipeline.

## Recommended Order

1. README decision tree
   Explain when this repo is the right tool:
   - one-off manual export
   - recurring local analysis
   - Databricks Delta
   - Databricks over S3
   - Palantir via Databricks or S3

2. Databricks job success
   Show the production Delta job:
   - `convex-sync-kit-meshix-api-prod-delta-extract`
   - latest run succeeded

3. Unity Catalog namespace view
   Show the aligned schemas together:
   - `convex_sync_kit_meshix_api_s3`
   - `convex_sync_kit_meshix_api_delta_control`
   - `convex_sync_kit_meshix_api_delta_bronze`
   - `convex_sync_kit_meshix_api_delta_silver`

4. Delta checkpoint proof
   Show `connector_checkpoint_latest` returning:
   - `meshix-api`
   - `delta_tail`

5. Bronze CDC proof
   Show `SHOW TABLES` in `convex_sync_kit_meshix_api_delta_bronze`
   so viewers can see the many `_cdc` tables.

6. S3-backed reference path proof
   Show the `__source_map` view in `convex_sync_kit_meshix_api_s3`
   and at least one `read_files('s3://...')` view definition.

7. Source config proof
   Show `sources/meshix-api/env.sh` briefly to make it clear that the repo is
   generic and the deployment is source-specific.

## What To Avoid

Do not show:

- raw deploy keys
- Databricks secret values or secret edit UIs
- noisy smoke schemas if they are not the point of the demo
- local `.memory/` paths unless you are explicitly explaining local analysis

## Strong Captions

Short captions that work well:

- `Recurring Convex export, not a one-off dump`
- `Recommended path: Delta into Unity Catalog`
- `Reference path still supported: S3-backed views`
- `One repo, many Convex sources`
- `Foundry path: Databricks virtual tables first, S3 direct as fallback`
