use super::meta::ValueMeta;
use super::op::{AsChangeOp, OpBuilder};
use super::types::{Action, ActorIdx};
use crate::change_graph::ChangeGraph;
use crate::storage::change::Verified;
use crate::storage::{Change, ChunkType, Header};
use crate::types::{ActorId, ChangeHash};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::io::Write;
use std::marker::PhantomData;
use std::num::NonZero;
use std::ops::Range;

pub(crate) mod batch;
pub(crate) mod collector;

pub(crate) use collector::{BuildChangeMetadata, ChangeCollector, CollectedChanges, OutOfMemory};

pub(crate) trait GetHash {
    fn get_hash(&self, index: usize) -> Option<ChangeHash>;
}

impl GetHash for Vec<crate::Change> {
    fn get_hash(&self, index: usize) -> Option<ChangeHash> {
        Some(self.get(index)?.hash())
    }
}

impl GetHash for &[ChangeHash] {
    fn get_hash(&self, index: usize) -> Option<ChangeHash> {
        self.get(index).copied()
    }
}

impl GetHash for ChangeGraph {
    fn get_hash(&self, index: usize) -> Option<ChangeHash> {
        self.index_to_hash(index).copied()
    }
}

pub(crate) fn build_change<T, G>(
    ops: &[T],
    meta: &BuildChangeMetadata<'_>,
    graph: &G,
    actors: &[ActorId],
) -> Change<'static, Verified>
where
    T: AsChangeOp,
    G: GetHash,
{
    let mut mapper = ActorMapper::new(actors);
    build_change_inner(ops, meta, graph, &mut mapper)
}

pub(crate) fn build_change_inner<T, G>(
    ops: &[T],
    meta: &BuildChangeMetadata<'_>,
    graph: &G,
    mapper: &mut ActorMapper<'_>,
) -> Change<'static, Verified>
where
    T: AsChangeOp,
    G: GetHash,
{
    let num_ops = ops.len();
    let mut col_data = Vec::new();

    let actor = mapper.actors[meta.actor].clone();

    let start_op = ops.first().map(T::op_id_ctr).unwrap_or(meta.max_op + 1);

    let ops_meta = write_change_ops(ops, meta.actor, &mut col_data, mapper);

    let other_actors: Vec<_> = mapper.iter().collect();

    let mut data = Vec::with_capacity(col_data.len());
    leb128::write::unsigned(&mut data, meta.deps.len() as u64).unwrap();

    // FIXME missing value here is changes out of order error
    let deps: Vec<_> = meta
        .deps
        .iter()
        .map(|i| graph.get_hash(*i as usize).unwrap())
        .collect();

    for hash in &deps {
        data.write_all(hash.as_bytes()).unwrap();
    }

    length_prefixed_bytes(&actor, &mut data);

    leb128::write::unsigned(&mut data, meta.seq).unwrap();
    leb128::write::unsigned(&mut data, start_op).unwrap();
    leb128::write::signed(&mut data, meta.timestamp).unwrap();

    length_prefixed_bytes(meta.message_str(), &mut data);

    leb128::write::unsigned(&mut data, other_actors.len() as u64).unwrap();

    for actor in other_actors.iter() {
        length_prefixed_bytes(actor, &mut data);
    }

    let raw_cols = ops_meta.to_raw_columns();
    raw_cols.write(&mut data);

    let ops_data_start = data.len();
    let ops_data = ops_data_start..(ops_data_start + col_data.len());

    data.extend(col_data);
    let extra_bytes = data.len()..(data.len() + meta.extra.len());
    if !meta.extra.is_empty() {
        data.extend(meta.extra.as_ref());
    }

    let header = Header::new(ChunkType::Change, &data);

    let mut bytes = Vec::with_capacity(header.len() + data.len());
    header.write(&mut bytes);
    bytes.extend(data);

    let ops_data = shift_range(ops_data, header.len());
    let extra_bytes = shift_range(extra_bytes, header.len());

    Change {
        bytes: Cow::Owned(bytes),
        header,
        dependencies: deps,
        actor,
        other_actors,
        seq: meta.seq,
        start_op: NonZero::new(start_op).unwrap(),
        timestamp: meta.timestamp,
        message: meta.message.as_ref().map(|s| s.to_string()),
        ops_meta: raw_cols,
        ops_data,
        extra_bytes,
        num_ops,
        _phantom: PhantomData,
    }
}

impl PartialOrd for OpBuilder<'_> {
    fn partial_cmp(&self, other: &OpBuilder<'_>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OpBuilder<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ChangeOpsColumns {
    pub(crate) obj_actor: Range<usize>,
    pub(crate) obj_ctr: Range<usize>,
    pub(crate) key_actor: Range<usize>,
    pub(crate) key_ctr: Range<usize>,
    pub(crate) key_str: Range<usize>,
    pub(crate) insert: Range<usize>,
    pub(crate) action: Range<usize>,
    pub(crate) value_meta: Range<usize>,
    pub(crate) value: Range<usize>,
    pub(crate) pred_count: Range<usize>,
    pub(crate) pred_actor: Range<usize>,
    pub(crate) pred_ctr: Range<usize>,
    pub(crate) expand: Range<usize>,
    pub(crate) mark_name: Range<usize>,
}

impl ChangeOpsColumns {
    /// Lower these byte ranges into a `RawColumns<Uncompressed>`, attaching a
    /// `ColumnSpec` to each. Mirrors the chunk's column manifest exactly so
    /// subsequent `RawColumns::write` produces the on-wire byte layout.
    ///
    /// Optional columns (value bytes, pred actor/ctr, expand, mark_name) are
    /// only included when their range is non-empty — matching the
    /// hexane encoders' "elide if all-default" behaviour.
    pub(crate) fn to_raw_columns(
        &self,
    ) -> crate::storage::RawColumns<crate::storage::columns::compression::Uncompressed> {
        use crate::storage::columns::{ColumnId, ColumnSpec, ColumnType, RawColumn};

        const OBJ: ColumnId = ColumnId::new(0);
        const KEY: ColumnId = ColumnId::new(1);
        const INSERT: ColumnId = ColumnId::new(3);
        const ACTION: ColumnId = ColumnId::new(4);
        const VAL: ColumnId = ColumnId::new(5);
        const PRED: ColumnId = ColumnId::new(7);
        const EXPAND: ColumnId = ColumnId::new(9);
        const MARK_NAME: ColumnId = ColumnId::new(10);

        let mut cols = vec![
            RawColumn::new(
                ColumnSpec::new(OBJ, ColumnType::Actor, false),
                self.obj_actor.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(OBJ, ColumnType::Integer, false),
                self.obj_ctr.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY, ColumnType::Actor, false),
                self.key_actor.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY, ColumnType::DeltaInteger, false),
                self.key_ctr.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(KEY, ColumnType::String, false),
                self.key_str.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(INSERT, ColumnType::Boolean, false),
                self.insert.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(ACTION, ColumnType::Integer, false),
                self.action.clone(),
            ),
            RawColumn::new(
                ColumnSpec::new(VAL, ColumnType::ValueMetadata, false),
                self.value_meta.clone(),
            ),
        ];
        if !self.value.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(VAL, ColumnType::Value, false),
                self.value.clone(),
            ));
        }
        cols.push(RawColumn::new(
            ColumnSpec::new(PRED, ColumnType::Group, false),
            self.pred_count.clone(),
        ));
        if !self.pred_actor.is_empty() {
            cols.extend([
                RawColumn::new(
                    ColumnSpec::new(PRED, ColumnType::Actor, false),
                    self.pred_actor.clone(),
                ),
                RawColumn::new(
                    ColumnSpec::new(PRED, ColumnType::DeltaInteger, false),
                    self.pred_ctr.clone(),
                ),
            ]);
        }
        if !self.expand.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(EXPAND, ColumnType::Boolean, false),
                self.expand.clone(),
            ));
        }
        if !self.mark_name.is_empty() {
            cols.push(RawColumn::new(
                ColumnSpec::new(MARK_NAME, ColumnType::String, false),
                self.mark_name.clone(),
            ));
        }
        cols.into_iter().collect()
    }
}

pub(crate) fn shift_range(range: Range<usize>, by: usize) -> Range<usize> {
    range.start + by..range.end + by
}

pub(crate) fn length_prefixed_bytes<B: AsRef<[u8]>>(b: B, out: &mut Vec<u8>) -> usize {
    let prefix_len = leb128::write::unsigned(out, b.as_ref().len() as u64).unwrap();
    out.write_all(b.as_ref()).unwrap();
    prefix_len + b.as_ref().len()
}

impl<'a> PartialEq for OpBuilder<'a> {
    fn eq(&self, other: &OpBuilder<'a>) -> bool {
        self.id == other.id
    }
}

impl Eq for OpBuilder<'_> {}

fn write_change_ops<T>(
    ops: &[T],
    change_actor: usize,
    data: &mut Vec<u8>,
    mapper: &mut ActorMapper<'_>,
) -> ChangeOpsColumns
where
    T: AsChangeOp,
{
    if ops.is_empty() {
        return ChangeOpsColumns::default();
    }

    mapper.remap_actors(ops, change_actor);

    use hexane::v1::EncoderApi;

    let mapping = &mapper.mapping;
    let remap_opt_actor = |actor: Option<ActorIdx>| actor.map(|a| mapping[usize::from(a)].unwrap());
    let remap_actor = |a: ActorIdx| mapping[usize::from(a)].unwrap();

    let obj_actor = hexane::v1::Encoder::<Option<ActorIdx>>::encode_to_unless(
        data,
        ops.iter().map(T::obj_actor).map(&remap_opt_actor),
        None,
    );
    let obj_ctr = hexane::v1::Encoder::<Option<u64>>::encode_to_unless(
        data,
        ops.iter().map(T::obj_ctr),
        None,
    );
    let key_actor = hexane::v1::Encoder::<Option<ActorIdx>>::encode_to_unless(
        data,
        ops.iter().map(T::key_actor).map(&remap_opt_actor),
        None,
    );
    let key_ctr = hexane::v1::DeltaEncoder::<Option<i64>>::encode_to_unless(
        data,
        ops.iter().map(T::key_ctr).map(|c| c.map(|c| *c)),
        None,
    );
    let key_str = hexane::v1::Encoder::<Option<String>>::encode_to_unless(
        data,
        ops.iter().map(T::key_str),
        None,
    );
    let insert = hexane::v1::Encoder::<bool>::encode_to(data, ops.iter().map(T::insert));
    let action = hexane::v1::Encoder::<Action>::encode_to(data, ops.iter().map(T::action));
    let value_meta =
        hexane::v1::Encoder::<ValueMeta>::encode_to(data, ops.iter().map(T::value_meta));
    let value_start = data.len();
    for bytes in ops.iter().filter_map(T::value) {
        data.extend_from_slice(&bytes);
    }
    let value = value_start..data.len();
    let pred_count = hexane::v1::Encoder::<u32>::encode_to(data, ops.iter().map(T::pred_count));
    let pred_iter = ops.iter().map(T::pred).flat_map(|id| id.iter());
    let pred_actor = hexane::v1::Encoder::<ActorIdx>::encode_to(
        data,
        pred_iter.clone().map(T::id_actor).map(&remap_actor),
    );
    let pred_ctr =
        hexane::v1::DeltaEncoder::<i64>::encode_to(data, pred_iter.map(|id| id.icounter()));
    let expand =
        hexane::v1::Encoder::<bool>::encode_to_unless(data, ops.iter().map(T::expand), false);
    let mark_name = hexane::v1::Encoder::<Option<String>>::encode_to_unless(
        data,
        ops.iter().map(T::mark_name),
        None,
    );

    ChangeOpsColumns {
        obj_actor,
        obj_ctr,
        key_actor,
        key_ctr,
        key_str,
        insert,
        action,
        value_meta,
        value,
        pred_count,
        pred_actor,
        pred_ctr,
        expand,
        mark_name,
    }
}

// The many small mallocs in the remap_actors
// was causing some memory thrashing with dmalloc/wasm
// this structure allows for the vectors to be allocated
// once and reused (via trucate()) when creating a large number
// of changes (like on load)
#[derive(Debug, PartialEq)]
pub(crate) struct ActorMapper<'a> {
    seen_actors: Vec<bool>,
    pub(crate) mapping: Vec<Option<ActorIdx>>,
    actors: &'a [ActorId],
    other_actors: Vec<usize>,
}

impl<'a> ActorMapper<'a> {
    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = ActorId> + '_ {
        self.other_actors.iter().map(|i| self.actors[*i].clone())
    }

    pub(crate) fn new(actors: &'a [ActorId]) -> ActorMapper<'a> {
        let len = actors.len();
        ActorMapper {
            seen_actors: vec![false; len],
            mapping: vec![None; len],
            actors,
            other_actors: vec![],
        }
    }

    fn reset(&mut self) {
        let len = self.actors.len();
        self.seen_actors.truncate(0);
        self.mapping.truncate(0);
        self.other_actors.truncate(0);

        self.seen_actors.resize(len, false);
        self.mapping.resize(len, None);
    }

    pub(crate) fn process_actor(&mut self, actor: usize) {
        self.seen_actors[actor] = true;
    }

    pub(crate) fn process_op<C>(&mut self, op: &C)
    where
        C: AsChangeOp,
    {
        if let Some(actor) = C::obj_actor(op) {
            self.process_actor(usize::from(actor))
        }
        if let Some(actor) = C::key_actor(op) {
            self.process_actor(usize::from(actor));
        }
        for id in C::pred(op) {
            self.process_actor(id.actor());
        }
    }

    pub(crate) fn build_mapping(&mut self, default_actor: Option<usize>) {
        let mut seen_index = 0;

        if let Some(actor) = default_actor {
            self.seen_actors[actor] = false;
            self.mapping[actor] = Some(ActorIdx(0));
            seen_index = 1;
        }

        for (index, seen) in self.seen_actors.iter().enumerate() {
            if *seen {
                self.other_actors.push(index);
                self.mapping[index] = Some(ActorIdx(seen_index));
                seen_index += 1;
            }
        }
    }

    fn remap_actors<C>(&mut self, ops: &[C], change_actor: usize)
    where
        C: AsChangeOp,
    {
        self.reset();

        for op in ops {
            self.process_op(op);
        }

        self.build_mapping(Some(change_actor));
    }
}
