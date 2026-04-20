# AWS Platform Assets

AWS assets grouped by the target or consumer they serve.

## Layout

- `s3_target/terraform/s3_bucket/`: bucket, versioning, block-public-access, SSE-S3
- `s3_target/terraform/publisher_user/`: non-admin publisher user, group, policy, access key
- `s3_consumers/terraform/databricks_reader_role/`: read-only Unity Catalog reader role for the S3 export path
- `s3_consumers/terraform/palantir_cloud_identity_reader_role/`: read-only Palantir Cloud Identity STS role for the S3 export path
- `s3_consumers/terraform/palantir_oidc_reader_role/`: read-only Palantir OIDC reader role for the S3 export path

`s3_target` assets are required to publish staged parquet snapshots.
`s3_consumers` assets are only needed when another platform reads those S3
artifacts directly.

## Working pattern

1. Snapshot these templates into `.memory/`.
2. Edit the copied `.tfvars` file there.
3. Run Terraform from the copied directory, not from the repo checkout.

Recommended entrypoint: `just aws-template-snapshot`

Why:

- Terraform state for `publisher_user` contains the generated secret access key.
- Rendered provider configs and local overrides are operational artifacts, not source.
