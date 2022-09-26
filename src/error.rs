//! Mirror-intel errors.

use std::result;

use actix_web::ResponseError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to decode path")]
    DecodePathError(()),
    #[error("Failed to send task to pending queue")]
    SendError(()),
    #[error("IO Error {0}")]
    Io(#[from] std::io::Error),
    #[error("Reqwest Error {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("HTTP Error {0}")]
    HTTPError(reqwest::StatusCode),
    #[error("Put Object Error {0}")]
    PutObjectError(#[from] rusoto_core::RusotoError<rusoto_s3::PutObjectError>),
    #[error("{0}")]
    CustomError(String),
    #[error("Too Large")]
    TooLarge(()),
    #[error("Invalid Request")]
    InvalidRequest(()),
    #[error("List Objects Error {0}")]
    ListObjectsError(#[from] rusoto_core::RusotoError<rusoto_s3::ListObjectsError>),
    #[error("Timeout")]
    Timeout(()),
}

impl ResponseError for Error {}

pub type Result<T> = result::Result<T, Error>;
