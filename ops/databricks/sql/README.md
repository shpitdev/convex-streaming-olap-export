# Databricks Landing SQL

These templates define the minimal Databricks-side registration layer after the
external location exists.

Current approach:

- create a schema in Unity Catalog
- create `VIEW`s over the published `staging/current/...` parquet files
- keep the view names deterministic so downstream transforms can target stable
  object names

Why views instead of more Terraform:

- this keeps the landing step generic and lightweight
- it avoids overloading Terraform with per-table objects
- the sync can be rerun after every S3 publish
