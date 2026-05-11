use std::ops::Range;

use crate::columnar::column_range::{BooleanRange, DeltaRange, RleRange};

/// The four types of "simple" column defined in the raw format
#[derive(Debug, Clone)]
pub(crate) enum SimpleColRange {
    /// A column containing RLE encoded u64's
    RleInt(RleRange<u64>),
    /// A column containing RLE encoded strings
    RleString(RleRange<smol_str::SmolStr>),
    /// A column containing delta -> RLE encoded i64s
    Delta(DeltaRange),
    /// A column containing boolean values
    Boolean(BooleanRange),
}

impl SimpleColRange {
    pub(crate) fn range(&self) -> Range<usize> {
        match self {
            Self::RleInt(r) => r.clone().into(),
            Self::RleString(r) => r.clone().into(),
            Self::Delta(r) => r.clone().into(),
            Self::Boolean(r) => r.clone().into(),
        }
    }
}
