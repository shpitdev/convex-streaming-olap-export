# Release Artifacts

This repo produces more than one distributable artifact.

## Primary Artifacts

- `convex-sync`: the Rust CLI binary built from `apps/convex-sync/`
- `convex-cdc-core`: the shared extraction crate consumed by the app and targets
- `convex-target-s3`: the S3/export target crate

The CLI is the only artifact that is directly operator-facing today. The crates
are release artifacts only if we decide to publish internal versioned libraries.

## Platform Artifacts

These are versioned with the repo, but they are not "binaries":

- `platform/aws/`: Terraform templates for S3 publishing and S3 consumer access
- `platform/databricks/s3/`: Terraform + SQL for the S3-backed Databricks path
- `platform/databricks/native/extractor/convex_cdc_job.py`: Databricks-native extractor entrypoint
- `platform/databricks/native/sql/bootstrap/`: ordered bootstrap SQL
- `platform/databricks/native/lakeflow/`: Lakeflow `AUTO CDC` templates

## Sensible Release Shape

If we formalize releases, the clean default would be:

1. GitHub release tag for the repo
2. attached `convex-sync` binaries and checksums
3. optional container image for the S3/export runtime
4. source tarball containing the platform assets

## Out of Scope for This Repo

This repo should not try to publish:

- Terraform state
- rendered `.tfvars`
- Databricks workspace notebooks generated during smoke runs
- Databricks secret scopes
- `.memory/` artifacts

## Future Follow-up

A dedicated release PR can decide:

- binary target matrix
- container image naming/versioning
- whether the library crates are internal-only or published
- whether Databricks assets should be bundled separately from the binary release
