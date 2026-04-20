default:
  just --list

# Repo hygiene
install-hooks:
  git config --local core.hooksPath .githooks
  chmod +x .githooks/pre-commit

fmt:
  cargo fmt --all

fmt-check:
  cargo fmt --all --check

check:
  cargo check --workspace

test:
  cargo test --workspace

clippy:
  cargo clippy --workspace --all-targets --all-features -- -D warnings

verify: fmt-check clippy test

# CI
depot-ci *args:
  depot ci run --workflow .depot/workflows/ci.yml {{args}}

# S3/export CLI
schemas *args:
  cargo run -p convex-sync -- schemas {{args}}

snapshot *args:
  cargo run -p convex-sync -- snapshot {{args}}

deltas *args:
  cargo run -p convex-sync -- deltas {{args}}

sync-once *args:
  cargo run -p convex-sync -- sync-once {{args}}

materialize-staging *args:
  cargo run -p convex-sync -- materialize-staging {{args}}

publish-s3 *args:
  cargo run -p convex-sync -- publish-s3 {{args}}

run *args:
  cargo run -p convex-sync -- run {{args}}

# Platform assets
aws-template-snapshot label="templates":
  ./scripts/snapshot-aws-templates.sh {{label}}

databricks-template-snapshot label="templates":
  ./scripts/snapshot-databricks-templates.sh {{label}}

# Databricks helpers
databricks-sync-staging-views *args:
  ./scripts/sync-databricks-staging-views.sh {{args}}

databricks-apply-sql-dir profile warehouse_id sql_dir:
  ./scripts/apply-databricks-sql-dir.sh {{profile}} {{warehouse_id}} {{sql_dir}}
