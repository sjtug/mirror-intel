//! Mirror-intel errors.

use std::result;

use actix_web::ResponseError;
use actix_web::http::StatusCode;
use thiserror::Error;

type PutObjectSdkError =
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>;
type GetObjectSdkError =
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>;
type ListObjectsSdkError =
    aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::list_objects::ListObjectsError>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to decode path")]
    DecodePath,
    #[error("Failed to send task to pending queue")]
    Send,
    #[error("IO Error {0}")]
    Io(#[from] std::io::Error),
    #[error("Reqwest Error {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("HTTP Error {0}")]
    Http(StatusCode),
    #[error("{0}")]
    Custom(String),
    #[error("Too Large")]
    TooLarge,
    #[error("Invalid Request")]
    InvalidRequest,
    #[error("Put Object Error {0}")]
    PutObject(Box<PutObjectSdkError>),
    #[error("Get Object Error {0}")]
    GetObjects(Box<GetObjectSdkError>),
    #[error("List Objects Error {0}")]
    ListObjects(Box<ListObjectsSdkError>),
    #[error("Timeout")]
    Timeout,
}

impl ResponseError for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Reqwest(err) if err.is_connect() => StatusCode::BAD_GATEWAY,
            Self::Reqwest(err) if err.is_timeout() => StatusCode::GATEWAY_TIMEOUT,
            Self::Http(status) => *status,
            Self::InvalidRequest => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// Fix clippy "the `Err`-variant returned from this function is very large"
impl From<PutObjectSdkError> for Error {
    fn from(error: PutObjectSdkError) -> Self {
        Self::PutObject(Box::new(error))
    }
}
impl From<GetObjectSdkError> for Error {
    fn from(error: GetObjectSdkError) -> Self {
        Self::GetObjects(Box::new(error))
    }
}
impl From<ListObjectsSdkError> for Error {
    fn from(error: ListObjectsSdkError) -> Self {
        Self::ListObjects(Box::new(error))
    }
}

pub type Result<T> = result::Result<T, Error>;
