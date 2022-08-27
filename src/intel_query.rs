use std::fmt::{Display, Formatter};

use rocket::request::{FromQuery, Query};

use crate::error::Error;

pub struct IntelQuery(String);

impl Display for IntelQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl IntelQuery {
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
