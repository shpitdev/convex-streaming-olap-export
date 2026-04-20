# convex-streaming-olap-export

- `Language`: ![Rust](https://img.shields.io/badge/Rust-000000?logo=rust&logoColor=white)
- `Source`: ![Convex](https://img.shields.io/badge/Convex-EE342F?logo=convex&logoColor=white)
- `Targets`: ![Amazon S3](https://img.shields.io/badge/Amazon%20S3-569A31?logo=amazons3&logoColor=white) ![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)
- `Infra`: ![Terraform](https://img.shields.io/badge/Terraform-844FBA?logo=terraform&logoColor=white)

Convex CDC sync engine with two supported target families:

- `S3/export`: append-only raw parquet -> current-state staging parquet -> S3 publish
- `Databricks/native`: bronze Delta CDC landing -> Lakeflow `AUTO CDC` -> silver current-state Delta tables

The source-side behavior intentionally stays close to the public Convex/Fivetran
extraction model:

- bootstrap with `list_snapshot`
- resume incomplete snapshots from checkpoint
- continue with `document_deltas`
- advance checkpoints only after durable writes succeed

## Layout

- `apps/convex-sync/`: operational CLI for the S3/export path
- `crates/convex-cdc-core/`: shared Convex client, checkpoint FSM, event normalization, sync engine
- `crates/convex-target-s3/`: raw parquet sink, staging materialization, S3 publish flow
- `platform/aws/`: AWS assets for S3 publishing and S3 consumer access
- `platform/databricks/s3/`: Databricks consuming the S3 export path
- `platform/databricks/native/`: Databricks-native extractor, bootstrap SQL, Lakeflow templates

## Install

Release install:

```bash
curl -fsSL https://raw.githubusercontent.com/shpitdev/convex-streaming-olap-export/main/install.sh | bash
```

Local checkout dev install:

```bash
./install.sh --mode dev --force
convex-sync-dev --help
```

Current release coverage:

- stable and prerelease archives target `linux-amd64`
- `convex-sync-dev` is checkout-linked and rebuilds incrementally via Cargo
- release installs go to `~/.local/share/convex-sync/<version>/convex-sync`
- command symlinks go in `~/.local/bin`

## Supported Paths

### `S3/export`

The maintained Rust runtime path:

1. `sync-once` writes append-only parquet batches under `.memory/raw_change_log/`
2. `materialize-staging --incremental` builds `.memory/staging/`
3. `publish-s3` uploads `staging/current/...` plus versioned manifests
4. `run` loops those steps on a poll interval

CLI:

- `cargo run -p convex-sync -- schemas`
- `cargo run -p convex-sync -- snapshot --table-name users`
- `cargo run -p convex-sync -- deltas --cursor 0`
- `cargo run -p convex-sync -- sync-once`
- `cargo run -p convex-sync -- materialize-staging`
- `cargo run -p convex-sync -- publish-s3 --bucket your-bucket`
- `cargo run -p convex-sync -- run --bucket your-bucket`

Or via `just`:

- `just dev-cli --help`
- `just sync-once`
- `just materialize-staging`
- `just publish-s3 --bucket your-bucket`
- `just run --bucket your-bucket`

### `Databricks/native`

Checked-in Databricks-native assets:

- `platform/databricks/native/extractor/convex_cdc_job.py`
- `platform/databricks/native/sql/bootstrap/`
- `platform/databricks/native/lakeflow/bronze_to_silver_template.sql`

Runtime split:

1. a Databricks job runs the extractor and appends bronze CDC rows
2. checkpoint rows land in the control schema
3. Lakeflow `AUTO CDC` materializes silver current-state tables

## Platform Assets

Snapshot templates into `.memory/` before running Terraform:

- `just aws-template-snapshot`
- `just databricks-template-snapshot`

The S3-backed Databricks landing sync remains supported:

- `just databricks-sync-staging-views --warehouse-id <warehouse-id> --bucket <bucket> --prefix <prefix>`

That script renders SQL from
`platform/databricks/s3/sql/register_staging_views.sql.tmpl` and applies stable
views over the published S3 parquet files.

## Verification

Local:

- `just install-hooks` configures a repo-local pre-commit hook
- the hook runs `just verify`

Remote:

- `.depot/workflows/ci.yml` runs fmt/clippy/test
- `.depot/workflows/release.yml` creates stable release PRs and publishes CLI archives
- `.depot/workflows/release-rc.yml` publishes numbered prerelease archives from `main`
- `.github/workflows/semgrep.yml` runs the lightweight security scan

## References

- [docs/architecture.md](docs/architecture.md)
- [docs/public-reference-map.md](docs/public-reference-map.md)
- [docs/release-artifacts.md](docs/release-artifacts.md)
- [Convex streaming export docs](https://docs.convex.dev/production/integrations/streaming-import-export)
- [Convex streaming export API](https://docs.convex.dev/streaming-export-api)
- [Upstream Convex `fivetran_source` crate](https://github.com/get-convex/convex-backend/tree/main/crates/fivetran_source)
- [Databricks `AUTO CDC` docs](https://docs.databricks.com/aws/en/ldp/cdc)
