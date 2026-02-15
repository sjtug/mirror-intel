//! Mirror-intel errors.

use std::result;

use actix_web::ResponseError;
use thiserror::Error;

type PutObjectSdkError =
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>;
type ListObjectsSdkError =
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects::ListObjectsError>;

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
    #[error("{0}")]
    CustomError(String),
    #[error("Too Large")]
    TooLarge(()),
    #[error("Invalid Request")]
    InvalidRequest(()),
    #[error("Put Object Error {0}")]
    PutObjectError(Box<PutObjectSdkError>),
    #[error("List Objects Error {0}")]
    ListObjectsError(Box<ListObjectsSdkError>),
    #[error("Timeout")]
    Timeout(()),
}

impl ResponseError for Error {}

// Fix clippy "the `Err`-variant returned from this function is very large"
impl From<PutObjectSdkError> for Error {
    fn from(error: PutObjectSdkError) -> Self {
        Self::PutObjectError(Box::new(error))
    }
}
impl From<ListObjectsSdkError> for Error {
    fn from(error: ListObjectsSdkError) -> Self {
        Self::ListObjectsError(Box::new(error))
    }
}

pub type Result<T> = result::Result<T, Error>;
