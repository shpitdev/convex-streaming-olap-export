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
  cargo run --bin convex-export -- schemas {{args}}

snapshot *args:
  cargo run --bin convex-export -- snapshot {{args}}

deltas *args:
  cargo run --bin convex-export -- deltas {{args}}

sync-once *args:
  cargo run --bin convex-export -- sync-once {{args}}

materialize-staging *args:
  cargo run --bin convex-export -- materialize-staging {{args}}

publish-s3 *args:
  cargo run --bin convex-export -- publish-s3 {{args}}

run *args:
  cargo run --bin convex-export -- run {{args}}

aws-template-snapshot label="templates":
  ./scripts/snapshot-aws-templates.sh {{label}}

databricks-template-snapshot label="templates":
  ./scripts/snapshot-databricks-templates.sh {{label}}

databricks-sync-staging-views *args:
  ./scripts/sync-databricks-staging-views.sh {{args}}
