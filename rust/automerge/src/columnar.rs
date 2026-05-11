//! Legacy encoder primitives for change-chunk op columns.
//!
//! Most of this module was deleted alongside the old `ChangeOpsIter` — the
//! read path moved to `storage::change::iter` (hexane-based). What remains
//! is the encode side that backs `From<ExpandedChange> for Change` (used by
//! the WASM `encodeChange` API), preserving byte-identical output for
//! hand-crafted fixtures.
pub(crate) mod column_range;
pub(crate) mod encoding;
