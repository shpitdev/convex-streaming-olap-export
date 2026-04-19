# Databricks Templates

This folder is the maintained source of truth for Databricks-side Terraform.

The initial focus is Unity Catalog objects for S3-backed external access:

- storage credentials
- external locations
- grants

Use these templates the same way as the AWS templates:

1. Snapshot them into `.memory/`.
2. Edit the copied `.tfvars` file there.
3. Run Terraform from the copied directory, not from the repo checkout.

Recommended entrypoint:

- `just databricks-template-snapshot`

## Current template

- `terraform/unity_catalog_s3_external_location/`
- `sql/register_staging_views.sql.tmpl`

This template assumes:

- the AWS IAM role already exists, or
- you intentionally bootstrap with `skip_validation = true`, then update the AWS
  trust policy and re-apply with `skip_validation = false`

The provider is configured from `~/.databrickscfg` by default, using a profile
such as `DEFAULT`.

## Workspace isolation

The current template manages:

- storage credential
- external location
- grants

Workspace binding / isolation is still best handled as a post-apply Databricks
CLI step using `databricks workspace-bindings ...`, after the securables exist.

## Landing sync

After the external location exists, use the landing sync scripts to create a
generic schema of stable Databricks views over the current parquet files:

- `scripts/sync-databricks-staging-views.sh`
- `scripts/databricks-exec-sql-statement.sh`

This keeps the post-bootstrap layer generic and transformation-ready without
turning every landing object into Terraform state.
