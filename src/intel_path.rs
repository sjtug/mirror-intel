use std::fmt::{Display, Formatter};
use std::ops::Deref;

use serde::de::Error;
use serde::{Deserialize, Deserializer};

/// `IntelPath` represents a URL-encoded path which is safe to use both
/// on s3 and on a normal filesystem.
pub struct IntelPath(String);

impl<'de> Deserialize<'de> for IntelPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bad_start = |msg| {
            Error::custom(format!(
                "The segment started with invalid character: {}",
                msg
            ))
        };
        let bad_end =
            |msg| Error::custom(format!("The segment ended with invalid character: {}", msg));
        let bad_char =
            |msg| Error::custom(format!("The segment contained invalid character: {}", msg));

        let decoded = String::deserialize(deserializer)?;
        let segments = decoded
            .split('/')
            .filter(|s| !s.is_empty())
            .filter(|s| *s != ".");

        let mut buf = vec![];
        for segment in segments {
            if segment == ".." {
                buf.pop();
            } else if segment.starts_with('.') {
                return Err(bad_start('.'));
            } else if segment.starts_with('*') {
                return Err(bad_start('*'));
            } else if segment.ends_with(':') {
                return Err(bad_end(':'));
            } else if segment.ends_with('>') {
                return Err(bad_end('>'));
            } else if segment.ends_with('<') {
                return Err(bad_end('<'));
            } else if segment.contains('/') {
                return Err(bad_char('/'));
            } else if cfg!(windows) && segment.contains('\\') {
                return Err(bad_char('\\'));
            } else {
                buf.push(segment);
            }
        }

        Ok(Self(buf.join("/")))
    }
}

impl Display for IntelPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for IntelPath {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
