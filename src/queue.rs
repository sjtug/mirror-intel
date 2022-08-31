use std::future::Future;

use actix_web::body::BoxBody;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse};
use actix_web::http::header::HeaderName;
use actix_web::http::Method;
use actix_web::web;

use crate::IntelMission;

pub fn queue_length<S>(
    req: ServiceRequest,
    srv: &S,
) -> impl Future<Output = Result<ServiceResponse, actix_web::Error>>
where
    S: Service<ServiceRequest, Response = ServiceResponse<BoxBody>, Error = actix_web::Error>,
{
    let length = (req.method() == Method::GET).then(|| {
        let mission = req
            .app_data::<web::Data<IntelMission>>()
            .expect("mission extension not found")
            .clone();
        mission.metrics.task_in_queue.get()
    });
    let fut = srv.call(req);
    async move {
        let mut resp: ServiceResponse<_> = fut.await?;
        if let Some(length) = length {
            // Rewrite the response to return the current counts.
            resp.headers_mut().append(
                HeaderName::from_static("x-intel-queue-length"),
                length.into(),
            );
            return Ok(resp);
        }

        Ok(resp)
    }
}
