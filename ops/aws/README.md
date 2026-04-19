# AWS Templates

This folder is the maintained source of truth for AWS bootstrap templates.

Use these templates to stage a copy into `.memory/` before running them. That
keeps Terraform state, rendered policy JSON, and any generated access keys out
of tracked git state.

## Pass 1

- `terraform/s3_bucket/`: bucket, versioning, block-public-access, SSE-S3
- `terraform/publisher_user/`: non-admin publisher user, group, policy, access key

## Pass 2

- `terraform/databricks_reader_role/`: read-only Unity Catalog reader role
- `terraform/palantir_cloud_identity_reader_role/`: read-only Palantir Cloud Identity STS role
- `terraform/palantir_oidc_reader_role/`: read-only Palantir OIDC reader role

## Working pattern

1. Snapshot these templates into `.memory/`.
2. Edit the copied `.tfvars` file there.
3. Run Terraform from the copied directory, not from the repo checkout.

Recommended entrypoint:

- `just aws-template-snapshot`

Why:

- Terraform state for `publisher_user` will contain the generated secret access key.
- Rendered provider configs and local overrides are operational artifacts, not source.
