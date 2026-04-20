# Release Artifacts

The operator-facing release artifact is the `convex-sync` CLI.

## Stable and Prerelease Assets

Each GitHub release should publish:

- `convex-sync_<tag>_linux_amd64.tar.gz`
- `SHA256SUMS`

The archive contains:

- `convex-sync`
- `LICENSE`
- `NOTICE`
- `README.md`

For now, that is enough to make the maintained Rust runtime easy to install and
verify.

## Dev Install Surface

This repo also ships a checkout-linked dev command:

- `scripts/convex-sync-dev`
- installed as `convex-sync-dev` via `./install.sh --mode dev`

That is not a release artifact, but it is part of the supported operator
experience for local development.

## Repo-Versioned Platform Assets

These stay versioned with the repo and release tag, but they are not published
as separate binary assets:

- `platform/aws/`
- `platform/databricks/s3/`
- `platform/databricks/native/extractor/convex_cdc_job.py`
- `platform/databricks/native/sql/bootstrap/`
- `platform/databricks/native/lakeflow/`

## Explicit Non-Artifacts

This repo should not publish:

- Terraform state
- rendered `.tfvars`
- Databricks smoke notebooks or secret scopes
- `.memory/` outputs
- internal library crates as standalone public packages

## Follow-up Candidates

Not in the first release slice:

- wider platform matrix beyond `linux-amd64`
- container images for the S3/export runtime
- separate packaging for Databricks-native assets
