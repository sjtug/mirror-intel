//! S3 index page.

use std::borrow::Cow;
use std::time::Duration;

use actix_web::http::header::ContentType;
use actix_web::{web, HttpResponse};
use rusoto_s3::S3;

use crate::common::{Config, IntelMission, IntelResponse, Redirect};
use crate::intel_path::IntelPath;
use crate::{Error, Result};

/// Generate a row for given s3 key.
fn generate_url(key: &str, last_modified: &str, size: i64, prefix: &str) -> String {
    let show_key = if key.len() >= prefix.len() {
        &key[prefix.len()..]
    } else {
        key
    };

    let href = if key.ends_with('/') {
        Cow::Owned(format!("{}?mirror_intel_list", key))
    } else {
        Cow::Borrowed(key)
    };
    format!(
        r#"<tr>
            <td><a href="/{}">{}</a></td>
            <td>{}</td>
            <td>{}</td>
        </tr>"#,
        href, show_key, last_modified, size
    )
}

// pub async fn list_middleware<B: Send + 'static>(req: Request<B>, next: Next<B>) -> Response {
//     if req.uri().query() == Some("mirror_intel_list") {
//         info!("hit middleware: {}", req.uri());
//         return list.call(req, Arc::new(())).await
//     }
//     info!("pass middleware: {}", req.uri());
//     next.run(req).await
// }

/// Directory index page for a given path.
pub async fn list(
    path: web::Path<IntelPath>,
    config: web::Data<Config>,
    intel_mission: web::Data<IntelMission>,
) -> Result<IntelResponse> {
    let path_slash = format!("{}/", path);
    let real_endpoint = format!(
        "{}/{}/{}",
        config.s3.website_endpoint, config.s3.bucket, path_slash
    );
    let mirror_clone_list = "mirror_clone_list.html";

    // First, check if there is mirror-clone index
    let req = rusoto_s3::GetObjectRequest {
        bucket: config.s3.bucket.clone(),
        key: format!("{}{}", path_slash, mirror_clone_list),
        ..Default::default()
    };
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        intel_mission.s3_client.get_object(req),
    )
    .await
    .map_err(|_| Error::Timeout(()))?;

    if result.is_ok() {
        return Ok(Redirect::Permanent(format!("{}{}", real_endpoint, mirror_clone_list)).into());
    }

    // Otherwise, generate a dynamic index
    let req = rusoto_s3::ListObjectsRequest {
        bucket: config.s3.bucket.clone(),
        prefix: Some(path_slash.clone()),
        delimiter: Some("/".to_string()),
        max_keys: Some(100),
        ..Default::default()
    };
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        intel_mission.s3_client.list_objects(req),
    )
    .await
    .map_err(|_| Error::Timeout(()))??;

    let mut body = r#"<tr>
            <td><a href="..?mirror_intel_list">..</a></td>
            <td></td>
        </tr>"#
        .to_string();

    if let Some(common_prefixes) = result.common_prefixes {
        let content = common_prefixes
            .into_iter()
            .filter_map(|x| x.prefix)
            .map(|x| generate_url(&x, "", 0, &path_slash))
            .collect::<Vec<_>>()
            .join("\n");

        body += &content;
    }

    if let Some(contents) = result.contents {
        let content = contents
            .into_iter()
            .filter_map(|x| {
                x.key.as_ref().and_then(|key| {
                    if key == &path_slash {
                        None
                    } else {
                        x.last_modified.as_ref().and_then(|last_modified| {
                            x.size
                                .as_ref()
                                .map(|size| generate_url(key, last_modified, *size, &path_slash))
                        })
                    }
                })
            })
            .collect::<Vec<_>>()
            .join("\n");

        body += &content;
    };

    let body = format!(
        r#"
            <html>
                <head>
                    <meta charset="utf-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1">
                    <title>Index of {}</title>
                    <link href="https://cdn.bootcdn.net/ajax/libs/twitter-bootstrap/4.5.3/css/bootstrap.min.css" rel="stylesheet">
                </head>
                <body>
                    <div class="container">
                        <h1>Index</h1>
                        <table class="table table-sm table-borderless">
                            <thead>
                                <tr>
                                    <th scope="col">Key</th>
                                    <th scope="col">Last Modified</th>
                                    <th scope="col">Size</th>
                                </tr>
                            </thead>
                            <tbody>
                            {}
                            </tbody>
                        </table>
                        <p class="mt-3 small">This page shows cached objects on s3 backend. If an object doesn't show up here, it
                        may still be accessible with the help of our smart cache proxy mirror-intel. On the other hand,
                        if an object is cached, it doesn't necessarily mean that it will be directly served to user.
                        Object serving still subjects to cache rules.</p>
                        <p class="mt-3 small">Maximum keys: {}</p>
                        <p class="mt-3 small text-muted">Generated by mirror-intel from <a href="{}">{}</a></p>
                    </div>
                </body>
            </html>
        "#,
        path_slash,
        body,
        result.max_keys.unwrap_or(0),
        real_endpoint,
        real_endpoint
    );

    Ok(HttpResponse::Ok()
        .content_type(ContentType::html())
        .body(body)
        .into())
}
