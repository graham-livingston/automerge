use std::{borrow::Cow, num::NonZeroU64};

use crate::legacy;
use crate::op_set2::change::{build_change, BuildChangeMetadata};
use crate::op_set2::op::OpBuilder;
use crate::op_set2::types::{Action, KeyRef};
use crate::storage::{change, parse, Change as StoredChange, Chunk, Compressed, ReadChangeOpError};
use crate::types::{ActorId, ChangeHash, ElemId, OpId};
use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    stored: StoredChange<'static, change::Verified>,
    compression: CompressionState,
    len: usize,
}

impl Change {
    pub(crate) fn new(stored: StoredChange<'static, change::Verified>) -> Self {
        let len = stored.len();
        Self {
            stored,
            len,
            compression: CompressionState::NotCompressed,
        }
    }

    pub(crate) fn new_from_unverified(
        stored: StoredChange<'static, change::Unverified>,
        compressed: Option<Compressed<'static>>,
    ) -> Result<Self, ReadChangeOpError> {
        let mut len = 0;
        let stored = stored.verify_ops(|_| len += 1)?;
        let compression = if let Some(c) = compressed {
            CompressionState::Compressed(c)
        } else {
            CompressionState::NotCompressed
        };
        Ok(Self {
            stored,
            len,
            compression,
        })
    }

    pub fn actor_id(&self) -> &ActorId {
        self.stored.actor()
    }

    pub fn actors(&self) -> impl Iterator<Item = &ActorId> {
        Some(self.stored.actor())
            .into_iter()
            .chain(self.stored.other_actors().iter())
    }

    pub fn other_actor_ids(&self) -> &[ActorId] {
        self.stored.other_actors()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn max_op(&self) -> u64 {
        self.stored.start_op().get() + (self.len as u64) - 1
    }

    pub fn start_op(&self) -> NonZeroU64 {
        self.stored.start_op()
    }

    pub fn message(&self) -> Option<&str> {
        self.stored.message()
    }

    pub fn deps(&self) -> &[ChangeHash] {
        self.stored.dependencies()
    }

    pub fn hash(&self) -> ChangeHash {
        self.stored.hash()
    }

    pub fn seq(&self) -> u64 {
        self.stored.seq()
    }

    pub fn timestamp(&self) -> i64 {
        self.stored.timestamp()
    }

    pub fn bytes(&mut self) -> Cow<'_, [u8]> {
        if let CompressionState::NotCompressed = self.compression {
            if let Some(compressed) = self.stored.compress() {
                self.compression = CompressionState::Compressed(compressed);
            } else {
                self.compression = CompressionState::TooSmallToCompress;
            }
        };
        match &self.compression {
            // SAFETY: We just checked this case above
            CompressionState::NotCompressed => unreachable!(),
            CompressionState::TooSmallToCompress => Cow::Borrowed(self.stored.bytes()),
            CompressionState::Compressed(c) => c.bytes(),
        }
    }

    pub fn raw_bytes(&self) -> &[u8] {
        self.stored.bytes()
    }

    pub(crate) fn iter_ops(&self) -> crate::storage::change::ChangeOpIter<'_> {
        self.stored.iter_ops()
    }

    pub fn extra_bytes(&self) -> &[u8] {
        self.stored.extra_bytes()
    }

    // TODO replace all uses of this with TryFrom<&[u8]>
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, LoadError> {
        Self::try_from(&bytes[..])
    }

    pub fn decode(&self) -> crate::ExpandedChange {
        crate::ExpandedChange::from(self)
    }
}

#[derive(Clone, Debug, PartialEq)]
enum CompressionState {
    /// We haven't tried to compress this change
    NotCompressed,
    /// We have compressed this change
    Compressed(Compressed<'static>),
    /// We tried to compress this change but it wasn't big enough to be worth it
    TooSmallToCompress,
}

impl AsRef<StoredChange<'static, change::Verified>> for Change {
    fn as_ref(&self) -> &StoredChange<'static, change::Verified> {
        &self.stored
    }
}

impl From<StoredChange<'static, change::Verified>> for Change {
    fn from(s: StoredChange<'static, change::Verified>) -> Self {
        Change::new(s)
    }
}
impl From<Change> for StoredChange<'static, change::Verified> {
    fn from(c: Change) -> Self {
        c.stored
    }
}

#[derive(thiserror::Error, Debug)]
pub enum LoadError {
    #[error("unable to parse change: {0}")]
    Parse(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("leftover data after parsing")]
    LeftoverData,
    #[error("wrong chunk type")]
    WrongChunkType,
}

impl<'a> TryFrom<&'a [u8]> for Change {
    type Error = LoadError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let input = parse::Input::new(value);
        let (remaining, chunk) = Chunk::parse(input).map_err(|e| LoadError::Parse(Box::new(e)))?;
        if !remaining.is_empty() {
            return Err(LoadError::LeftoverData);
        }
        match chunk {
            Chunk::Change(c) => Self::new_from_unverified(c.into_owned(), None)
                .map_err(|e| LoadError::Parse(Box::new(e))),
            Chunk::CompressedChange(c, compressed) => {
                Self::new_from_unverified(c.into_owned(), Some(compressed.into_owned()))
                    .map_err(|e| LoadError::Parse(Box::new(e)))
            }
            _ => Err(LoadError::WrongChunkType),
        }
    }
}

impl<'a> TryFrom<StoredChange<'a, change::Unverified>> for Change {
    type Error = ReadChangeOpError;

    fn try_from(c: StoredChange<'a, change::Unverified>) -> Result<Self, Self::Error> {
        Self::new_from_unverified(c.into_owned(), None)
    }
}

impl From<crate::ExpandedChange> for Change {
    fn from(e: crate::ExpandedChange) -> Self {
        // Collect all actors (sorted lex via BTreeSet) so the ActorMapper's
        // index-order walk produces the same other_actors ordering the legacy
        // ChangeActors path would have.
        let mut all_actors: BTreeSet<ActorId> = BTreeSet::new();
        all_actors.insert(e.actor_id.clone());
        for op in &e.operations {
            if let legacy::ObjectId::Id(id) = &op.obj {
                all_actors.insert(id.1.clone());
            }
            if let legacy::Key::Seq(legacy::ElementId::Id(id)) = &op.key {
                all_actors.insert(id.1.clone());
            }
            for pred in op.pred.iter() {
                all_actors.insert(pred.1.clone());
            }
        }
        let actors: Vec<ActorId> = all_actors.into_iter().collect();
        let actor_idx: HashMap<ActorId, usize> = actors
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, a)| (a, i))
            .collect();
        let default_actor = *actor_idx.get(&e.actor_id).unwrap();

        let start_op = e.start_op.get();

        let ops: Vec<OpBuilder<'static>> = e
            .operations
            .iter()
            .enumerate()
            .map(|(i, op)| OpBuilder {
                id: OpId::new(start_op + i as u64, default_actor),
                obj: op.obj.import(&actor_idx),
                key: op.key.import(&actor_idx),
                pred: op.pred.iter().map(|p| p.import(&actor_idx)).collect(),
                action: Action::try_from(op.action.action_index())
                    .expect("legacy action_index always returns 0..=7"),
                value: op
                    .primitive_value()
                    .map(|v| v.into_ref())
                    .unwrap_or(crate::op_set2::types::ScalarValue::Null),
                mark_name: match &op.action {
                    legacy::OpType::MarkBegin(legacy::MarkData { name, .. }) => {
                        Some(Cow::Owned(name.to_string()))
                    }
                    _ => None,
                },
                insert: op.insert,
                expand: op.action.expand(),
            })
            .collect();

        let max_op = start_op + e.operations.len() as u64 - 1;

        let meta = BuildChangeMetadata {
            actor: default_actor,
            seq: e.seq,
            max_op,
            timestamp: e.time,
            message: e.message.as_ref().map(|s| Cow::Owned(s.clone())),
            deps: (0..e.deps.len() as u64).collect(),
            extra: Cow::Borrowed(&e.extra_bytes),
            start_op,
            builder: 0,
        };

        Change::new(build_change(&ops, &meta, &e.deps.as_slice(), &actors))
    }
}

impl From<&Change> for crate::ExpandedChange {
    fn from(c: &Change) -> Self {
        let actors = std::iter::once(c.actor_id())
            .chain(c.other_actor_ids().iter())
            .cloned()
            .enumerate()
            .collect::<std::collections::HashMap<_, _>>();
        let operations = c
            .iter_ops()
            .map(|o| legacy::Op {
                action: legacy::OpType::from_parts(legacy::OpTypeParts {
                    action: u64::from(o.action),
                    value: o.value.into_legacy(),
                    expand: o.expand,
                    mark_name: o.mark_name.map(|c| smol_str::SmolStr::from(c.as_ref())),
                }),
                insert: o.insert,
                key: match o.key {
                    KeyRef::Seq(e) if e.is_head() => legacy::Key::Seq(legacy::ElementId::Head),
                    KeyRef::Seq(ElemId(eo)) => legacy::Key::Seq(legacy::ElementId::Id(
                        legacy::OpId::new(eo.counter(), actors.get(&eo.actor()).unwrap()),
                    )),
                    KeyRef::Map(s) => legacy::Key::Map(smol_str::SmolStr::from(s.as_ref())),
                },
                obj: if let Some(id) = o.obj.id() {
                    legacy::ObjectId::Id(legacy::OpId::new(
                        id.counter(),
                        actors.get(&id.actor()).unwrap(),
                    ))
                } else {
                    legacy::ObjectId::Root
                },
                pred: o
                    .pred
                    .into_iter()
                    .map(|p| legacy::OpId::new(p.counter(), actors.get(&p.actor()).unwrap()))
                    .collect(),
            })
            .collect::<Vec<_>>();
        crate::ExpandedChange {
            operations,
            actor_id: actors.get(&0).unwrap().clone(),
            hash: Some(c.hash()),
            time: c.timestamp(),
            deps: c.deps().to_vec(),
            seq: c.seq(),
            start_op: c.start_op(),
            extra_bytes: c.extra_bytes().to_vec(),
            message: c.message().map(|s| s.to_owned()),
        }
    }
}
