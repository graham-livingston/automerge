//! Streaming hexane-based iterator over a change chunk's op columns.
//!
//! Modelled directly after `OpIter` / `OpIterUnverified` in
//! `src/storage/bundle/builder.rs`: each per-column field is a
//! `hexane::v1::Decoder` (or `DeltaDecoder`) constructed from the raw byte
//! range carried by a `RawColumn`, dispatched by `ColumnSpec`. No
//! `hexane::v1::Column::load` materialisation, no parsed `ChangeOpsColumns`
//! struct.
//!
//! Termination is signalled by `action`, which `op_set2::change` always
//! writes (`Encoder::encode_to`, never elided): the first time the action
//! decoder yields `None` we're done. Elided columns (`encode_to_unless`
//! produced zero bytes) sit on empty decoders and yield `None` immediately,
//! which is fine — by the time they're read we've already committed to
//! producing this row, and `(None, None)` patterns map to natural defaults
//! (root obj, no expand, no mark name).

use std::borrow::Cow;
use std::num::NonZeroU64;

use crate::columnar::encoding::DecodeColumnError;
use crate::op_set2::meta::{ValueMeta, ValueType};
use crate::op_set2::op::OpBuilder;
use crate::op_set2::types::{Action, ActorIdx, KeyRef, ScalarValue};
use crate::storage::change::{ParseError, ReadChangeOpError};
use crate::storage::columns::compression::Uncompressed;
use crate::storage::columns::ColumnType;
use crate::storage::RawColumns;
use crate::types::{ElemId, ObjId, OpId};

use hexane::v1;

/// Read the next value from a streaming `Decoder` / `DeltaDecoder` and map
/// any decode error into a `ReadChangeOpError`. Three forms:
///
/// - `pull!(d, "col")` — required value: `None` and `null` both error.
/// - `pull!(d, "col", term)` — termination signal: `None` short-circuits
///   the outer `try_next` with `Ok(None)`. Used by the `action` column.
/// - `pull!(d, "col", null_default = expr)` — `None` substitutes `expr`
///   (used for elided columns like `expand`, `mark_name`, `pred_count`).
macro_rules! pull {
    ($d:expr, $col:literal) => {
        match $d.try_next() {
            Some(Ok(v)) => v,
            Some(Err(e)) => return Err(invalid($col, &e.to_string())),
            None => return Err(missing($col)),
        }
    };
    ($d:expr, $col:literal, term) => {
        match $d.try_next() {
            Some(Ok(v)) => v,
            Some(Err(e)) => return Err(invalid($col, &e.to_string())),
            None => return Ok(None),
        }
    };
    ($d:expr, $col:literal, null_default = $default:expr) => {
        match $d.try_next() {
            Some(Ok(v)) => v,
            Some(Err(e)) => return Err(invalid($col, &e.to_string())),
            None => $default,
        }
    };
}

#[rustfmt::skip]
mod ids {
    use crate::storage::columns::ColumnId;
    pub(super) const OBJ_COL_ID:       ColumnId = ColumnId::new(0);
    pub(super) const KEY_COL_ID:       ColumnId = ColumnId::new(1);
    pub(super) const INSERT_COL_ID:    ColumnId = ColumnId::new(3);
    pub(super) const ACTION_COL_ID:    ColumnId = ColumnId::new(4);
    pub(super) const VAL_COL_ID:       ColumnId = ColumnId::new(5);
    pub(super) const PRED_COL_ID:      ColumnId = ColumnId::new(7);
    pub(super) const EXPAND_COL_ID:    ColumnId = ColumnId::new(9);
    pub(super) const MARK_NAME_COL_ID: ColumnId = ColumnId::new(10);
}

#[derive(Clone)]
struct Inner<'a> {
    obj_actor: v1::Decoder<'a, Option<ActorIdx>>,
    obj_ctr: v1::Decoder<'a, Option<u64>>,
    key_actor: v1::Decoder<'a, Option<ActorIdx>>,
    key_ctr: v1::DeltaDecoder<'a, Option<i64>>,
    key_str: v1::Decoder<'a, Option<String>>,
    insert: v1::Decoder<'a, bool>,
    action: v1::Decoder<'a, Option<Action>>,
    meta: v1::Decoder<'a, Option<ValueMeta>>,
    pred_count: v1::Decoder<'a, Option<u64>>,
    pred_actor: v1::Decoder<'a, Option<ActorIdx>>,
    pred_ctr: v1::DeltaDecoder<'a, Option<i64>>,
    expand: v1::Decoder<'a, bool>,
    mark_name: v1::Decoder<'a, Option<String>>,
    value: &'a [u8],
    /// Counter for the next op's `id`: `OpId::new(start_op + next_idx, 0)`.
    /// Local actor=0 means the change author; remap_actors translates it.
    start_op: u64,
    next_idx: u64,
}

impl<'a> Inner<'a> {
    fn try_new(
        columns: &RawColumns<Uncompressed>,
        data: &'a [u8],
        start_op: NonZeroU64,
    ) -> Result<Self, ParseError> {
        let mut obj_actor = v1::decoder::<Option<ActorIdx>>(&[]);
        let mut obj_ctr = v1::decoder::<Option<u64>>(&[]);
        let mut key_actor = v1::decoder::<Option<ActorIdx>>(&[]);
        let mut key_ctr = v1::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut key_str = v1::decoder::<Option<String>>(&[]);
        let mut insert = v1::decoder::<bool>(&[]);
        let mut action = v1::decoder::<Option<Action>>(&[]);
        let mut meta = v1::decoder::<Option<ValueMeta>>(&[]);
        let mut pred_count = v1::decoder::<Option<u64>>(&[]);
        let mut pred_actor = v1::decoder::<Option<ActorIdx>>(&[]);
        let mut pred_ctr = v1::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut expand = v1::decoder::<bool>(&[]);
        let mut mark_name = v1::decoder::<Option<String>>(&[]);
        let mut value: &[u8] = &[];

        for col in columns.iter() {
            let d = &data[col.data()];
            type C = ColumnType;
            match (col.spec().id(), col.spec().col_type()) {
                (ids::OBJ_COL_ID, C::Actor) => obj_actor = v1::decoder::<Option<ActorIdx>>(d),
                (ids::OBJ_COL_ID, C::Integer) => obj_ctr = v1::decoder::<Option<u64>>(d),
                (ids::KEY_COL_ID, C::Actor) => key_actor = v1::decoder::<Option<ActorIdx>>(d),
                (ids::KEY_COL_ID, C::DeltaInteger) => {
                    key_ctr = v1::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ids::KEY_COL_ID, C::String) => key_str = v1::decoder::<Option<String>>(d),
                (ids::INSERT_COL_ID, C::Boolean) => insert = v1::decoder::<bool>(d),
                (ids::ACTION_COL_ID, C::Integer) => action = v1::decoder::<Option<Action>>(d),
                (ids::VAL_COL_ID, C::ValueMetadata) => meta = v1::decoder::<Option<ValueMeta>>(d),
                (ids::VAL_COL_ID, C::Value) => value = d,
                (ids::PRED_COL_ID, C::Group) => pred_count = v1::decoder::<Option<u64>>(d),
                (ids::PRED_COL_ID, C::Actor) => pred_actor = v1::decoder::<Option<ActorIdx>>(d),
                (ids::PRED_COL_ID, C::DeltaInteger) => {
                    pred_ctr = v1::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ids::EXPAND_COL_ID, C::Boolean) => expand = v1::decoder::<bool>(d),
                (ids::MARK_NAME_COL_ID, C::String) => mark_name = v1::decoder::<Option<String>>(d),
                _ => return Err(ParseError::InvalidOpColumn(u32::from(col.spec()))),
            }
        }

        Ok(Self {
            obj_actor,
            obj_ctr,
            key_actor,
            key_ctr,
            key_str,
            insert,
            action,
            meta,
            pred_count,
            pred_actor,
            pred_ctr,
            expand,
            mark_name,
            value,
            start_op: start_op.get(),
            next_idx: 0,
        })
    }

    fn try_next(&mut self) -> Result<Option<OpBuilder<'a>>, ReadChangeOpError> {
        // `action` is the termination signal: when its decoder is exhausted
        // we're past the last op. Out-of-range action codes surface as
        // `PackError` via `try_next`, no panic.
        let action = match pull!(self.action, "action", term) {
            Some(a) => a,
            None => return Err(missing("action")),
        };

        let obj_actor = pull!(self.obj_actor, "obj_actor", null_default = None);
        let obj_ctr = pull!(self.obj_ctr, "obj_ctr", null_default = None);
        let obj = match (obj_actor, obj_ctr) {
            (None, None) => ObjId::root(),
            (_, Some(0)) => ObjId::root(),
            (Some(a), Some(c)) => ObjId(try_op_id(c, usize::from(a))?),
            (None, Some(_)) => return Err(missing("obj_actor")),
            (Some(_), None) => return Err(missing("obj_ctr")),
        };

        let key_actor = pull!(self.key_actor, "key_actor", null_default = None);
        let key_ctr = pull!(self.key_ctr, "key_ctr", null_default = None);
        let key_str = pull!(self.key_str, "key_str", null_default = None);
        let key: KeyRef<'a> = match (key_actor, key_ctr, key_str) {
            (None, None, Some(s)) => KeyRef::Map(Cow::Borrowed(s)),
            (None, Some(0), None) => KeyRef::Seq(ElemId(OpId::new(0, 0))),
            (Some(a), Some(c), None) => {
                let c: u64 = c
                    .try_into()
                    .map_err(|_| ReadChangeOpError::CounterTooLarge)?;
                KeyRef::Seq(ElemId(try_op_id(c, usize::from(a))?))
            }
            _ => return Err(invalid("key", "ambiguous or incomplete key columns")),
        };

        let insert = pull!(self.insert, "insert", null_default = false);

        let value_meta = match pull!(self.meta, "value_meta") {
            Some(m) => m,
            None => return Err(missing("value_meta")),
        };
        let raw_len = value_meta.length();
        if self.value.len() < raw_len {
            return Err(invalid("value", "raw value bytes truncated"));
        }
        let (value_raw, tail) = self.value.split_at(raw_len);
        self.value = tail;
        // Match the legacy iter's strictness on LEB-typed values: the LEB
        // decoder must consume exactly `meta.length()` bytes. `op_set2`'s
        // `from_raw` reads through `parse_uleb128` / `parse_leb128` which
        // discard the unconsumed tail, so we re-verify here.
        match value_meta.type_code() {
            ValueType::Uleb => {
                let mut buf = value_raw;
                leb128::read::unsigned(&mut buf).map_err(|_| invalid("value", "bad uleb"))?;
                if !buf.is_empty() {
                    return Err(invalid("value", "extra bytes"));
                }
            }
            ValueType::Leb | ValueType::Counter | ValueType::Timestamp => {
                let mut buf = value_raw;
                leb128::read::signed(&mut buf).map_err(|_| invalid("value", "bad leb"))?;
                if !buf.is_empty() {
                    return Err(invalid("value", "extra bytes"));
                }
            }
            _ => {}
        }
        let value: ScalarValue<'a> = ScalarValue::from_raw(value_meta, value_raw)
            .map_err(|e| invalid("value", &e.to_string()))?;

        let pred_count =
            pull!(self.pred_count, "pred_count", null_default = Some(0)).unwrap_or(0) as usize;
        let mut pred = Vec::with_capacity(pred_count.min(100));
        for _ in 0..pred_count {
            let pa = pull!(self.pred_actor, "pred_actor", null_default = None);
            let pc = pull!(self.pred_ctr, "pred_ctr", null_default = None);
            match (pa, pc) {
                (Some(a), Some(c)) => {
                    let c: u64 = c
                        .try_into()
                        .map_err(|_| ReadChangeOpError::CounterTooLarge)?;
                    pred.push(try_op_id(c, usize::from(a))?);
                }
                _ => return Err(missing("pred")),
            }
        }

        let expand = pull!(self.expand, "expand", null_default = false);
        let mark_name: Option<Cow<'a, str>> =
            pull!(self.mark_name, "mark_name", null_default = None).map(Cow::Borrowed);

        // Action/value compatibility check. Only `Increment` constrains the
        // value (must be numeric); other actions accept any value. Action
        // codes outside 0..=7 can't appear because we decoded `Action` via
        // `try_next` upstream.
        if action == Action::Increment
            && !matches!(value, ScalarValue::Int(_) | ScalarValue::Uint(_))
        {
            return Err(ReadChangeOpError::InvalidOpType(
                crate::error::InvalidOpType::NonNumericInc,
            ));
        }

        let id = try_op_id(self.start_op + self.next_idx, 0)?;
        self.next_idx += 1;

        Ok(Some(OpBuilder {
            id,
            obj,
            action,
            key,
            value,
            insert,
            expand,
            mark_name,
            pred,
        }))
    }
}

/// Unverified streaming iterator over a change chunk's ops.
///
/// Yields `Result<OpBuilder<'a>, ReadChangeOpError>` with chunk-borrowed
/// strings and *change-local* actor indices (the change author is index 0,
/// `other_actors` start at 1). Translate to global indices with
/// [`OpBuilder::remap_actors`](crate::op_set2::op::OpBuilder::remap_actors).
///
/// Errors arise from malformed column bytes (truncated LEB, invalid action
/// code, value-column extra bytes, etc.) — the underlying hexane decoders
/// surface these via `try_next`, this iter funnels them into
/// `ReadChangeOpError`.
#[derive(Clone)]
pub(crate) struct ChangeOpIterUnverified<'a> {
    inner: Option<Inner<'a>>,
}

impl<'a> ChangeOpIterUnverified<'a> {
    /// Construct a new iter. If the column spec layout is invalid (unknown
    /// column id) the iter yields `None` immediately. Callers that want to
    /// reject malformed layouts up-front can call [`Self::try_new`] instead.
    pub(crate) fn new(
        raw: &RawColumns<Uncompressed>,
        data: &'a [u8],
        start_op: NonZeroU64,
    ) -> Self {
        Self {
            inner: Inner::try_new(raw, data, start_op).ok(),
        }
    }

    /// Like [`Self::new`] but surfaces a layout error from the column spec
    /// dispatch up-front, mirroring `BundleChangeIterUnverified::try_new`.
    #[allow(dead_code)]
    pub(crate) fn try_new(
        raw: &RawColumns<Uncompressed>,
        data: &'a [u8],
        start_op: NonZeroU64,
    ) -> Result<Self, ParseError> {
        Ok(Self {
            inner: Some(Inner::try_new(raw, data, start_op)?),
        })
    }
}

impl<'a> Iterator for ChangeOpIterUnverified<'a> {
    type Item = Result<OpBuilder<'a>, ReadChangeOpError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .as_mut()
            .and_then(|inner| inner.try_next().transpose())
    }
}

/// Verified version: assumes a prior pass through the unverified iter
/// completed without errors. Yields bare `OpBuilder<'a>`. Mirrors `OpIter`
/// in `storage/bundle/builder.rs:704`.
#[derive(Clone)]
pub(crate) struct ChangeOpIter<'a> {
    iter: ChangeOpIterUnverified<'a>,
}

impl<'a> ChangeOpIter<'a> {
    pub(crate) fn new(
        raw: &RawColumns<Uncompressed>,
        data: &'a [u8],
        start_op: NonZeroU64,
    ) -> Self {
        Self {
            iter: ChangeOpIterUnverified::new(raw, data, start_op),
        }
    }
}

impl<'a> Iterator for ChangeOpIter<'a> {
    type Item = OpBuilder<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|r| r.unwrap())
    }
}

fn missing(col: &'static str) -> ReadChangeOpError {
    DecodeColumnError::unexpected_null(col).into()
}

fn invalid(col: &'static str, reason: &str) -> ReadChangeOpError {
    DecodeColumnError::invalid_value(col, reason).into()
}

/// Build an `OpId` from a `u64` counter, mapping the `u32`-overflow error
/// path to [`ReadChangeOpError::CounterTooLarge`] so loaders see the same
/// error the legacy `Change::verify_ops` post-iter check produced.
fn try_op_id(counter: u64, actor: usize) -> Result<OpId, ReadChangeOpError> {
    OpId::try_new(counter, actor).map_err(|_| ReadChangeOpError::CounterTooLarge)
}
