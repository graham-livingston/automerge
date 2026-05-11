#[derive(Clone, Debug)]
pub struct DecodeColumnError {
    path: String,
    error: DecodeColErrorKind,
}

impl std::error::Error for DecodeColumnError {}

impl std::fmt::Display for DecodeColumnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.error {
            DecodeColErrorKind::UnexpectedNull => {
                write!(f, "unexpected null in column {}", self.path)
            }
            DecodeColErrorKind::InvalidValue { reason } => {
                write!(f, "invalid value in column {}: {}", self.path, reason)
            }
        }
    }
}

#[derive(Clone, Debug)]
enum DecodeColErrorKind {
    UnexpectedNull,
    InvalidValue { reason: String },
}

impl DecodeColumnError {
    pub(crate) fn unexpected_null<S: AsRef<str>>(col: S) -> DecodeColumnError {
        Self {
            path: col.as_ref().to_string(),
            error: DecodeColErrorKind::UnexpectedNull,
        }
    }

    pub(crate) fn invalid_value<S: AsRef<str>, R: AsRef<str>>(
        col: S,
        reason: R,
    ) -> DecodeColumnError {
        Self {
            path: col.as_ref().to_string(),
            error: DecodeColErrorKind::InvalidValue {
                reason: reason.as_ref().to_string(),
            },
        }
    }
}
