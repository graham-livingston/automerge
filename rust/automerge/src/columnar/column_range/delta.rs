use std::ops::Range;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DeltaRange(Range<usize>);

impl AsRef<Range<usize>> for DeltaRange {
    fn as_ref(&self) -> &Range<usize> {
        &self.0
    }
}

impl From<Range<usize>> for DeltaRange {
    fn from(r: Range<usize>) -> DeltaRange {
        DeltaRange(r)
    }
}

impl From<DeltaRange> for Range<usize> {
    fn from(r: DeltaRange) -> Range<usize> {
        r.0
    }
}
