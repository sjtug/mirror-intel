use std::fmt::{Display, Formatter};
use std::ops::Deref;

use actix_http::{Payload, StatusCode};
use actix_web::{FromRequest, HttpRequest, ResponseError};
use futures_util::future::{ready, Ready};
use thiserror::Error;

/// `IntelPath` represents a URL-encoded path which is safe to use both
/// on s3 and on a normal filesystem.
pub struct IntelPath<const N: usize = 0>(String);

impl<const N: usize> FromRequest for IntelPath<{ N }> {
    type Error = PathExtractionError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let f = || {
            let match_info = req.match_info();
            let (_, path) = match_info.iter().nth(N).ok_or_else(|| {
                PathExtractionError::OutOfBounds(N, match_info.segment_count() - 1)
            })?;
            let segments = path
                .split('/')
                .filter(|s| !s.is_empty())
                .filter(|s| *s != ".");

            let mut buf = vec![];
            for segment in segments {
                if segment == ".." {
                    buf.pop();
                } else if segment.starts_with('.') {
                    return Err(PathExtractionError::BadStart('.'));
                } else if segment.starts_with('*') {
                    return Err(PathExtractionError::BadStart('*'));
                } else if segment.ends_with(':') {
                    return Err(PathExtractionError::BadEnd(':'));
                } else if segment.ends_with('>') {
                    return Err(PathExtractionError::BadEnd('>'));
                } else if segment.ends_with('<') {
                    return Err(PathExtractionError::BadEnd('<'));
                } else if segment.contains('/') {
                    return Err(PathExtractionError::BadChar('/'));
                } else if cfg!(windows) && segment.contains('\\') {
                    return Err(PathExtractionError::BadChar('\\'));
                } else {
                    buf.push(segment);
                }
            }
            Ok(Self(buf.join("/")))
        };
        ready(f())
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

/// Error type for `IntelPath` extractor.
///
/// For bad character errors, the server responds with a 404 not found.
/// For segment out-of-bounds errors, the server responds with a 500 internal server error because
/// this is a programming error.
#[derive(Debug, Error)]
pub enum PathExtractionError {
    #[error("The segment started with invalid character: {0}")]
    BadStart(char),
    #[error("The segment ended with invalid character: {0}")]
    BadEnd(char),
    #[error("The segment contained invalid character: {0}")]
    BadChar(char),
    #[error("Segment index out of bound: {0} > {1}")]
    OutOfBounds(usize, usize),
}

impl ResponseError for PathExtractionError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::BadStart(_) | Self::BadEnd(_) | Self::BadChar(_) => StatusCode::NOT_FOUND,
            Self::OutOfBounds(_, _) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
