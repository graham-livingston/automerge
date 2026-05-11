use super::{RawRange, RleRange};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ValueRange {
    meta: RleRange<u64>,
    raw: RawRange,
}

impl ValueRange {
    pub(crate) fn new(meta: RleRange<u64>, raw: RawRange) -> Self {
        Self { meta, raw }
    }

    pub(crate) fn range(&self) -> std::ops::Range<usize> {
        // This is a hack, instead `raw` should be `Option<RawRange>`
        if self.raw.is_empty() {
            self.meta.clone().into()
        } else {
            self.meta.start()..self.raw.end()
        }
    }
}
