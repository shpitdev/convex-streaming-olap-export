default:
  just --list

install-hooks:
  git config --local core.hooksPath .githooks
  chmod +x .githooks/pre-commit

fmt:
  cargo fmt

fmt-check:
  cargo fmt --check

check:
  cargo check

test:
  cargo test

clippy:
  cargo clippy --all-targets --all-features -- -D warnings

verify: fmt-check clippy test

depot-ci *args:
  depot ci run --workflow .depot/workflows/ci.yml {{args}}

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

aws-template-snapshot label="templates":
  ./scripts/snapshot-aws-templates.sh {{label}}

databricks-template-snapshot label="templates":
  ./scripts/snapshot-databricks-templates.sh {{label}}

databricks-sync-staging-views *args:
  ./scripts/sync-databricks-staging-views.sh {{args}}
