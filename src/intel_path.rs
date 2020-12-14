use rocket::http::uri::{SegmentError, Segments, Uri};
use rocket::request::FromSegments;
use std::path::PathBuf;

/// `IntelPath` represents a URL-encoded path which is safe to use both
/// on s3 and on a normal filesystem.
pub struct IntelPath(String);

/// This is a modified version of `rocket_http/uri/segments.rs`
impl<'a> FromSegments<'a> for IntelPath {
    type Error = SegmentError;

    fn from_segments(segments: Segments<'a>) -> Result<IntelPath, SegmentError> {
        let mut buf = PathBuf::new();

        for segment in segments {
            let decoded = Uri::percent_decode(segment.as_bytes()).map_err(SegmentError::Utf8)?;

            if decoded == ".." {
                buf.pop();
            } else if decoded.starts_with('.') {
                return Err(SegmentError::BadStart('.'));
            } else if decoded.starts_with('*') {
                return Err(SegmentError::BadStart('*'));
            } else if decoded.ends_with(':') {
                return Err(SegmentError::BadEnd(':'));
            } else if decoded.ends_with('>') {
                return Err(SegmentError::BadEnd('>'));
            } else if decoded.ends_with('<') {
                return Err(SegmentError::BadEnd('<'));
            } else if decoded.contains('/') {
                return Err(SegmentError::BadChar('/'));
            } else if cfg!(windows) && decoded.contains('\\') {
                return Err(SegmentError::BadChar('\\'));
            } else {
                buf.push(segment)
            }
        }

        Ok(IntelPath(buf.into_os_string().into_string().unwrap()))
    }
}

impl AsRef<str> for IntelPath {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Into<String> for IntelPath {
    fn into(self) -> String {
        self.0
    }
}
