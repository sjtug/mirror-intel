use std::io::Cursor;

use crate::common::{Config, IntelMission, IntelResponse};
use crate::intel_path::IntelPath;
use crate::{Error, Result};

use rocket::{http::ContentType, response::Redirect, Response, State};
use rusoto_s3::S3;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Item {
    name: String,
    source: String,
}

fn generate_url(key: &str, last_modified: &str, size: i64, prefix: &str) -> String {
    let show_key = if key.len() >= prefix.len() {
        &key[prefix.len()..]
    } else {
        key
    };
    if key.ends_with("/") {
        return format!(
            r#"<tr>
            <td><a href="/{}?mirror_intel_list">{}</a></td>
            <td>{}</td>
            <td>{}</td>
        </tr>"#,
            key, show_key, last_modified, size
        );
    } else {
        return format!(
            r#"<tr>
            <td><a href="/{}">{}</a></td>
            <td>{}</td>
            <td>{}</td>
        </tr>"#,
            key, show_key, last_modified, size
        );
    }
}

#[get("/<path..>?mirror_intel_list")]
pub async fn list(
    path: IntelPath,
    config: State<'_, Config>,
    intel_mission: State<'_, IntelMission>,
) -> Result<IntelResponse<'static>> {
    let mut path_slash: String = path.into();
    path_slash.push('/');
    let real_endpoint = format!("{}/{}/{}", config.s3.endpoint, config.s3.bucket, path_slash);
    let mirror_clone_list = "mirror_clone_list.html";

    // First, check if there is mirror-clone index
    let mut req = rusoto_s3::GetObjectRequest::default();
    req.bucket = config.s3.bucket.clone();
    req.key = format!("{}{}", path_slash, mirror_clone_list);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        intel_mission.s3_client.get_object(req),
    )
    .await
    .map_err(|_| Error::Timeout(()))?;

    if let Ok(_) = result {
        return Ok(Redirect::permanent(format!("{}{}", real_endpoint, mirror_clone_list)).into());
    }

    // Otherwise, generate a dynamic index
    let mut req = rusoto_s3::ListObjectsRequest::default();
    req.bucket = config.s3.bucket.clone();
    req.prefix = Some(path_slash.clone());
    req.delimiter = Some("/".to_string());
    req.max_keys = Some(100);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        intel_mission.s3_client.list_objects(req),
    )
    .await
    .map_err(|_| Error::Timeout(()))??;

    let mut resp = Response::build();

    let mut body = format!(
        r#"<tr>
            <td><a href="..?mirror_intel_list">..</a></td>
            <td></td>
        </tr>"#
    );

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
                            x.size.as_ref().and_then(|size| {
                                Some(generate_url(&key, &last_modified, *size, &path_slash))
                            })
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

    resp.sized_body(body.len(), Cursor::new(body));
    resp.header(ContentType::HTML);
    Ok(resp.finalize().into())
}
