use std::{marker::PhantomData, ops::Range};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RleRange<T> {
    range: Range<usize>,
    _phantom: PhantomData<T>,
}

impl<T> RleRange<T> {
    pub(crate) fn start(&self) -> usize {
        self.range.start
    }

    pub(crate) fn end(&self) -> usize {
        self.range.end
    }
}

impl<T> AsRef<Range<usize>> for RleRange<T> {
    fn as_ref(&self) -> &Range<usize> {
        &self.range
    }
}

impl<T> From<Range<usize>> for RleRange<T> {
    fn from(r: Range<usize>) -> RleRange<T> {
        RleRange {
            range: r,
            _phantom: PhantomData,
        }
    }
}

impl<T> From<RleRange<T>> for Range<usize> {
    fn from(r: RleRange<T>) -> Range<usize> {
        r.range
    }
}
