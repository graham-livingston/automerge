use std::{borrow::Cow, marker::PhantomData, num::NonZeroU64, ops::Range};

use crate::{ActorId, ChangeHash};

use super::{parse, CheckSum, Header, RawColumns};

mod iter;
pub(crate) use iter::{ChangeOpIter, ChangeOpIterUnverified};

mod compressed;
pub(crate) use compressed::Compressed;

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub enum ReadChangeOpError {
    #[error(transparent)]
    DecodeError(#[from] crate::columnar::encoding::DecodeColumnError),
    #[error(transparent)]
    InvalidOpType(#[from] crate::error::InvalidOpType),
    #[error("counter too large")]
    CounterTooLarge,
}

pub(crate) const DEFLATE_MIN_SIZE: usize = 256;

/// Changes present an iterator over the operations encoded in them. Before we have read these
/// changes we don't know if they are valid, so we expose an iterator with items which are
/// `Result`s. However, frequently we know that the changes are valid, this trait is used as a
/// witness that we have verified the operations in a change so we can expose an iterator which
/// does not return `Results`
pub(crate) trait OpReadState {}
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Verified;
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Unverified;
impl OpReadState for Verified {}
impl OpReadState for Unverified {}

/// A `Change` is the result of parsing a change chunk as specified in [1]
///
/// The type parameter to this type represents whether or not operation have been "verified".
/// Operations in a change chunk are stored in a compressed column oriented storage format. In
/// general there is no guarantee that this storage is valid. Therefore we use the `OpReadState`
/// type parameter to distinguish between contexts where we know that the ops are valid and those
/// where we don't. The `Change::verify_ops` method can be used to obtain a verified `Change` which
/// can provide an iterator over `ChangeOp`s directly, rather than over `Result<ChangeOp,
/// ReadChangeOpError>`.
///
/// [1]: https://alexjg.github.io/automerge-storage-docs/#change-chunks
#[derive(Clone, Debug)]
pub(crate) struct Change<'a, O: OpReadState> {
    /// The raw bytes of the entire chunk containing this change, including the header.
    pub(crate) bytes: Cow<'a, [u8]>,
    pub(crate) header: Header,
    pub(crate) dependencies: Vec<ChangeHash>,
    pub(crate) actor: ActorId,
    pub(crate) other_actors: Vec<ActorId>,
    pub(crate) seq: u64,
    pub(crate) start_op: NonZeroU64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<String>,
    pub(crate) ops_meta: RawColumns<crate::storage::columns::compression::Uncompressed>,
    /// The range in `Self::bytes` where the ops column data is
    pub(crate) ops_data: Range<usize>,
    pub(crate) extra_bytes: Range<usize>,
    pub(crate) num_ops: usize,
    pub(crate) _phantom: PhantomData<O>,
}

impl<O: OpReadState> PartialEq for Change<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ParseError {
    #[error(transparent)]
    Leb128(#[from] parse::leb128::Error),
    #[error(transparent)]
    InvalidUtf8(#[from] parse::InvalidUtf8),
    #[error("failed to parse change columns: {0}")]
    RawColumns(#[from] crate::storage::columns::raw_column::ParseError),
    #[error("failed to parse header: {0}")]
    Header(#[from] super::chunk::error::Header),
    #[error("change contained compressed columns")]
    CompressedChangeCols,
    #[error("invalid op column: {0}")]
    InvalidOpColumn(u32),
}

impl<'a> Change<'a, Unverified> {
    pub(crate) fn parse(
        input: parse::Input<'a>,
    ) -> parse::ParseResult<'a, Change<'a, Unverified>, ParseError> {
        // TODO(alex): check chunk type
        let (i, header) = Header::parse(input)?;
        let parse::Split {
            first: chunk_input,
            remaining,
        } = i.split(header.data_bytes().len());
        let (_, change) = Self::parse_following_header(chunk_input, header)?;
        Ok((remaining, change))
    }

    /// Parse a change chunk. `input` should be the entire chunk, including the header bytes.
    pub(crate) fn parse_following_header(
        input: parse::Input<'a>,
        header: Header,
    ) -> parse::ParseResult<'a, Change<'a, Unverified>, ParseError> {
        let (i, deps) = parse::length_prefixed(parse::change_hash)(input)?;
        let (i, actor) = parse::actor_id(i)?;
        let (i, seq) = parse::leb128_u64(i)?;
        let (i, start_op) = parse::nonzero_leb128_u64(i)?;
        let (i, timestamp) = parse::leb128_i64(i)?;
        let (i, message_len) = parse::leb128_u64(i)?;
        let (i, message) = parse::utf_8(message_len as usize, i)?;
        let (i, other_actors) = parse::length_prefixed(parse::actor_id)(i)?;
        let (i, ops_meta) = RawColumns::parse(i)?;
        let (
            i,
            parse::RangeOf {
                range: ops_data, ..
            },
        ) = parse::range_of(|i| parse::take_n(ops_meta.total_column_len(), i), i)?;

        let (
            _i,
            parse::RangeOf {
                range: extra_bytes, ..
            },
        ) = parse::range_of(parse::take_rest, i)?;

        let ops_meta = ops_meta
            .uncompressed()
            .ok_or(parse::ParseError::Error(ParseError::CompressedChangeCols))?;

        Ok((
            parse::Input::empty(),
            Change {
                bytes: input.bytes().into(),
                header,
                dependencies: deps,
                actor,
                other_actors,
                seq,
                start_op,
                timestamp,
                message: if message.is_empty() {
                    None
                } else {
                    Some(message)
                },
                ops_meta,
                ops_data,
                extra_bytes,
                num_ops: 0,
                _phantom: PhantomData,
            },
        ))
    }

    /// Iterate over the ops in this chunk. The iterator will return an error if any of the ops are
    /// malformed.
    pub(crate) fn iter_ops(&'a self) -> ChangeOpIterUnverified<'a> {
        ChangeOpIterUnverified::new(&self.ops_meta, self.ops_data(), self.start_op)
    }

    /// Verify all the ops in this change executing `f` for each one
    ///
    /// `f` will be called for each op in this change, allowing callers to collect additional
    /// information about the ops (e.g. all the actor IDs in the change, or the number of ops)
    ///
    /// # Errors
    /// * If there is an error reading an operation
    pub(crate) fn verify_ops<F: FnMut(crate::op_set2::op::OpBuilder<'_>)>(
        self,
        mut f: F,
    ) -> Result<Change<'a, Verified>, ReadChangeOpError> {
        let mut num_ops = 0;
        for op in self.iter_ops() {
            f(op?);
            num_ops += 1;
        }
        if u32::try_from(u64::from(self.start_op)).is_err() {
            return Err(ReadChangeOpError::CounterTooLarge);
        }
        Ok(Change {
            bytes: self.bytes,
            header: self.header,
            dependencies: self.dependencies,
            actor: self.actor,
            other_actors: self.other_actors,
            seq: self.seq,
            start_op: self.start_op,
            timestamp: self.timestamp,
            message: self.message,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            extra_bytes: self.extra_bytes,
            num_ops,
            _phantom: PhantomData,
        })
    }
}

impl<'a> Change<'a, Verified> {
    pub(crate) fn len(&self) -> usize {
        self.num_ops
    }

    pub(crate) fn iter_ops(&'a self) -> ChangeOpIter<'a> {
        // SAFETY: This unwrap is okay because a `Change<'_, Verified>` can only be constructed
        // using either `verify_ops` or `Builder::build`, so we know the ops columns are valid.
        ChangeOpIter::new(&self.ops_meta, self.ops_data(), self.start_op)
    }
}

impl<O: OpReadState> Change<'_, O> {
    pub(crate) fn checksum(&self) -> CheckSum {
        self.header.checksum()
    }

    pub(crate) fn actor(&self) -> &ActorId {
        &self.actor
    }
    pub(crate) fn other_actors(&self) -> &[ActorId] {
        &self.other_actors
    }

    pub(crate) fn start_op(&self) -> NonZeroU64 {
        self.start_op
    }

    pub(crate) fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    pub(crate) fn dependencies(&self) -> &[ChangeHash] {
        &self.dependencies
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    pub(crate) fn timestamp(&self) -> i64 {
        self.timestamp
    }

    pub(crate) fn extra_bytes(&self) -> &[u8] {
        &self.bytes[self.extra_bytes.clone()]
    }

    pub(crate) fn checksum_valid(&self) -> bool {
        self.header.checksum_valid()
    }

    pub(crate) fn body_bytes(&self) -> &[u8] {
        &self.bytes[self.header.len()..]
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn hash(&self) -> ChangeHash {
        self.header.hash()
    }

    pub(crate) fn ops_data(&self) -> &[u8] {
        &self.bytes[self.ops_data.clone()]
    }

    pub(crate) fn into_owned(self) -> Change<'static, O> {
        Change {
            dependencies: self.dependencies,
            bytes: Cow::Owned(self.bytes.into_owned()),
            header: self.header,
            actor: self.actor,
            other_actors: self.other_actors,
            seq: self.seq,
            start_op: self.start_op,
            timestamp: self.timestamp,
            message: self.message,
            ops_meta: self.ops_meta,
            ops_data: self.ops_data,
            num_ops: self.num_ops,
            extra_bytes: self.extra_bytes,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn compress(&self) -> Option<Compressed<'static>> {
        if self.bytes.len() > DEFLATE_MIN_SIZE {
            Some(Compressed::compress(self))
        } else {
            None
        }
    }
}
