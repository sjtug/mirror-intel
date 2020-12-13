use std::io::Cursor;
use std::result;

use rocket::request::Request;
use rocket::response::{self, Responder, Response};
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
}

impl<'r> Responder<'r, 'static> for Error {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        let fmt_str = format!("{:?}", self);
        Response::build()
            .sized_body(fmt_str.len(), Cursor::new(fmt_str))
            .ok()
    }
}

pub type Result<T> = result::Result<T, Error>;
