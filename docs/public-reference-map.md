# Public Reference Map

This repo should actively use the public Convex/Fivetran reference materials
before inventing source-side behavior.

## Local Mirror

The relevant upstream crates are mirrored locally under:

- `.memory/upstream/convex-backend/crates/fivetran_source`
- `.memory/upstream/convex-backend/crates/fivetran_common`
- `.memory/upstream/convex-backend/crates/fivetran_destination`

These are local-only working references and are intentionally ignored by git.

## Use These For

### Extraction And Checkpoint Semantics

Primary source of truth:

- `.memory/upstream/convex-backend/crates/fivetran_source/src/sync.rs`
- `.memory/upstream/convex-backend/crates/fivetran_source/src/convex_api.rs`
- `.memory/upstream/convex-backend/crates/fivetran_source/src/api_types/mod.rs`

Use these files when deciding:

- snapshot resume behavior
- delta handoff behavior
- checkpoint phase structure
- delete semantics
- `has_more` handling
- component-aware table identity

### Source Schema / Selection Behavior

Use:

- `.memory/upstream/convex-backend/crates/fivetran_source/src/schema.rs`
- `.memory/upstream/convex-backend/crates/fivetran_source/src/api_types/selection.rs`
- `.memory/upstream/convex-backend/crates/fivetran_source/src/connector.rs`

Use these when deciding:

- component/table/column selection behavior
- schema naming or component partitioning assumptions
- resync requirements when schema layout changes

### Value Conversion Rules

Use:

- `.memory/upstream/convex-backend/crates/fivetran_source/src/convert.rs`

Use this when deciding:

- which top-level values should stay typed
- when nested values should be serialized as JSON
- special handling for `_creationTime`
- which underscore-prefixed fields are transport metadata instead of user data

### Destination-Side Schema / Metadata Ideas

Use carefully:

- `.memory/upstream/convex-backend/crates/fivetran_destination/src/schema.rs`
- `.memory/upstream/convex-backend/crates/fivetran_destination/src/convert.rs`
- `.memory/upstream/convex-backend/crates/fivetran_destination/README.md`

These are useful for:

- schema compatibility rules
- metadata-column separation
- nullable-vs-non-nullable compatibility thinking

These are **not** the source of truth for our source-side incremental runtime.

## Public Web References

- Convex connector overview: https://fivetran.com/docs/connectors/databases/convex
- Fivetran soft delete mode: https://fivetran.com/docs/core-concepts/sync-modes/soft-delete
- Fivetran history mode: https://fivetran.com/docs/core-concepts/sync-modes/history-mode

These are useful for product-level semantics and user expectations, but not for
the exact implementation details of Fivetran's proprietary destination runtime.

## What Is Public vs Internal

Public and usable:

- Convex source connector extraction logic
- Convex connector request/response shapes
- Convex/Fivetran documented sync semantics

Not public / not directly copyable:

- Fivetran's internal scheduler
- warehouse-specific incremental merge strategies
- internal retry / throttling / billing behavior
- proprietary destination maintenance logic

## Current Alignment Rules

When possible, our source-side behavior should stay aligned with the public
`fivetran_source` behavior:

- explicit snapshot vs delta checkpoint phases
- resume initial snapshot from checkpoint
- hand off deltas from the final snapshot timestamp
- treat deletes as first-class events
- keep transport metadata separate from user fields
- keep nested values JSON-encoded rather than flattening aggressively

## Known Intentional Deviations

Today we intentionally differ from the Fivetran connector in these ways:

- we do not implement the Fivetran gRPC runtime
- we write Parquet datasets instead of pushing rows into a Fivetran destination
- we materialize local `staging` tables directly instead of relying on warehouse-native managed tables
- we currently use our own staging-state/materialization design because Fivetran's internal incremental warehouse maintenance is not public

## Next Slice Guidance

For incremental staging updates:

- use `fivetran_source/src/sync.rs` as the reference for source checkpoint behavior
- do **not** assume Fivetran destination update internals are available
- prefer keeping both:
  - full rebuild path
  - incremental update path

If those two paths ever disagree, the full rebuild path is the correctness reference.
