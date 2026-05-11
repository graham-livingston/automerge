use std::ops::Range;

use super::SimpleColRange;
use crate::columnar::column_range::{RleRange, ValueRange};

/// A group column range is one with a "num" column and zero or more "grouped" columns. The "num"
/// column contains RLE encoded u64s, each `u64` represents the number of values to read from each
/// of the grouped columns in order to produce a `CellValue::Group` for the current row.
#[derive(Debug, Clone)]
pub(crate) struct GroupRange {
    pub(crate) num: RleRange<u64>,
    pub(crate) values: Vec<GroupedColumnRange>,
}

impl GroupRange {
    pub(crate) fn new(num: RleRange<u64>, values: Vec<GroupedColumnRange>) -> Self {
        Self { num, values }
    }

    pub(crate) fn range(&self) -> Range<usize> {
        let start = self.num.start();
        let end = self
            .values
            .last()
            .map(|v| v.range().end)
            .unwrap_or_else(|| self.num.end());
        start..end
    }
}

/// The type of ranges which can be the "grouped" columns in a `GroupRange`
#[derive(Debug, Clone)]
pub(crate) enum GroupedColumnRange {
    Value(ValueRange),
    Simple(SimpleColRange),
}

impl GroupedColumnRange {
    pub(crate) fn range(&self) -> Range<usize> {
        match self {
            Self::Value(vr) => vr.range(),
            Self::Simple(s) => s.range(),
        }
    }
}
