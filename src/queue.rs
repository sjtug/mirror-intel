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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actix_http::Request;
    use actix_web::dev::{Service, ServiceResponse};
    use actix_web::test::{call_service, init_service, TestRequest};
    use actix_web::{web, App};
    use reqwest::Client;

    use crate::common::S3Config;
    use crate::storage::get_anonymous_s3_client;
    use crate::{queue_length, IntelMission, Metrics};

    async fn make_service(
        metrics: Arc<Metrics>,
    ) -> impl Service<Request, Response = ServiceResponse, Error = actix_web::Error> {
        let s3_config = S3Config::default();
        let mission = IntelMission {
            tx: None,
            client: Client::new(),
            metrics,
            s3_client: Arc::new(get_anonymous_s3_client(&s3_config)),
        };

        let app = App::new()
            .app_data(web::Data::new(mission.clone()))
            .route("/", web::get().to(|| async { "test" }))
            .wrap_fn(queue_length);

        init_service(app).await
    }

    #[tokio::test]
    async fn must_queue_length() {
        let metrics = Arc::new(Metrics::default());
        let service = make_service(metrics.clone()).await;

        let assert_queue = |service, expected: u64| async move {
            let req = TestRequest::get().uri("/").to_request();
            let resp: ServiceResponse = call_service(service, req).await;
            let length = resp
                .headers()
                .get("x-intel-queue-length")
                .unwrap()
                .to_str()
                .unwrap();
            assert_eq!(length, &expected.to_string());
        };

        assert_queue(&service, 0).await;
        metrics.task_in_queue.inc();
        assert_queue(&service, 1).await;
    }
}
