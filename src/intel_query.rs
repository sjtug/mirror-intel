use rocket::request::{FromQuery, Query};

use crate::error::Error;

pub struct IntelQuery(String);

impl IntelQuery {
    pub fn to_string(self) -> String {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'q> FromQuery<'q> for IntelQuery {
    type Error = Error;

    fn from_query(query: Query<'q>) -> Result<Self, Self::Error> {
        Ok(Self(
            query
                .map(|q| q.raw.as_str())
                .collect::<Vec<&str>>()
                .join("&"),
        ))
    }
}
