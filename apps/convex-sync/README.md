# `convex-sync` CLI

Operational CLI for the maintained Rust runtime path.

```mermaid
flowchart LR
  C[Convex]
  S[sync-once]
  M[materialize-staging]
  P[publish-s3]
  R[run]
  C --> S
  S --> M
  M --> P
  R --> S
```

## Commands

- `sync-once`: write append-only raw change-log parquet
- `materialize-staging`: collapse raw changes into latest-state staging parquet
- `publish-s3`: publish staging parquet to S3
- `run`: poll Convex and drive the full S3/export loop

## Help

```bash
cargo run -p convex-sync -- --help
cargo run -p convex-sync -- sync-once --help
```

Direct source inspection now lives in:

- [`apps/convex-inspect/README.md`](../convex-inspect/README.md)

Checkout-linked dev install:

```bash
./install.sh --mode dev --force
convex-sync-dev --help
```
