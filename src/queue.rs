use rocket::fairing::{Fairing, Info, Kind};
use rocket::http::Header;
use rocket::http::Method;
use rocket::{Request, Response};

use crate::common::IntelMission;

pub struct QueueLength {
    pub mission: IntelMission,
}

#[rocket::async_trait]
impl Fairing for QueueLength {
    fn info(&self) -> Info {
        Info {
            name: "Queue length in Header",
            kind: Kind::Response,
        }
    }

    async fn on_response<'r>(&self, request: &'r Request<'_>, response: &mut Response<'r>) {
        // Rewrite the response to return the current counts.
        if request.method() == Method::Get {
            response.set_header(Header::new(
                "X-Intel-Queue-Length",
                self.mission.metrics.task_in_queue.get().to_string(),
            ));
        }
    }
}
