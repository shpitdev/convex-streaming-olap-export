default:
  just --list

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

schemas *args:
  cargo run --bin convex-export -- schemas {{args}}

snapshot *args:
  cargo run --bin convex-export -- snapshot {{args}}

deltas *args:
  cargo run --bin convex-export -- deltas {{args}}

sync-once *args:
  cargo run --bin convex-export -- sync-once {{args}}
