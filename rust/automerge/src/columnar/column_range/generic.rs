use std::ops::Range;

use super::ValueRange;
mod simple;
pub(crate) use simple::SimpleColRange;
mod group;
pub(crate) use group::{GroupRange, GroupedColumnRange};

/// A range which can represent any column which is valid with respect to the data model of the
/// column oriented storage format. Used during chunk parsing as an intermediate step before
/// dispatching to specific range types and during validation of unknown columns.
#[derive(Debug, Clone)]
pub(crate) enum GenericColumnRange {
    /// A "simple" column is one which directly corresponds to a single column in the raw format
    Simple(SimpleColRange),
    /// A value range consists of two columns and produces `ScalarValue`s
    Value(ValueRange),
    /// A "group" range consists of zero or more grouped columns and produces `CellValue::Group`s
    Group(GroupRange),
}

impl GenericColumnRange {
    pub(crate) fn range(&self) -> Range<usize> {
        match self {
            Self::Simple(sc) => sc.range(),
            Self::Value(v) => v.range(),
            Self::Group(g) => g.range(),
        }
    }
}
