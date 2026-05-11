use std::ops::Range;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BooleanRange(Range<usize>);

impl AsRef<Range<usize>> for BooleanRange {
    fn as_ref(&self) -> &Range<usize> {
        &self.0
    }
}

impl From<Range<usize>> for BooleanRange {
    fn from(r: Range<usize>) -> BooleanRange {
        BooleanRange(r)
    }
}

impl From<BooleanRange> for Range<usize> {
    fn from(r: BooleanRange) -> Range<usize> {
        r.0
    }
}
