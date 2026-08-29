//! Serving generated PyPA Simple Repository indexes from S3.
//!
//! The caller provides a storage prefix and logical index path. This module owns
//! Accept negotiation, S3 object naming, content types, and canonical-name
//! fallback for PEP 503/691.

use actix_web::http::{Method, StatusCode, header};
use actix_web::{HttpRequest, HttpResponse, web};

use crate::common::{Config, IntelMission, IntelResponse, Task};
use crate::error::{Error, Result};

const HTML_MEDIA_TYPE: &str = "application/vnd.pypi.simple.v1+html";
const JSON_MEDIA_TYPE: &str = "application/vnd.pypi.simple.v1+json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexFormat {
    Json,
    ModernHtml,
    LegacyHtml,
}

impl IndexFormat {
    fn filename(self) -> &'static str {
        match self {
            Self::Json => "index.v1_json",
            Self::ModernHtml | Self::LegacyHtml => "index.v1_html",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::Json => JSON_MEDIA_TYPE,
            Self::ModernHtml => "application/vnd.pypi.simple.v1+html; charset=utf-8",
            Self::LegacyHtml => "text/html; charset=utf-8",
        }
    }
}

fn media_range_specificity(range: &str, offered: &str) -> Option<u8> {
    let (range_type, range_subtype) = range.trim().split_once('/')?;
    let (offered_type, offered_subtype) = offered.split_once('/')?;
    if range_type == "*" && range_subtype == "*" {
        return Some(0);
    }
    if !range_type.eq_ignore_ascii_case(offered_type) {
        return None;
    }
    if range_subtype == "*" {
        return Some(1);
    }
    range_subtype
        .eq_ignore_ascii_case(offered_subtype)
        .then_some(2)
}

fn quality(value: &str, offered: &[&str]) -> Option<(u8, f32)> {
    let mut best: Option<(u8, f32)> = None;
    for item in value.split(',') {
        let mut parts = item.trim().split(';');
        let Some(range) = parts.next() else {
            continue;
        };
        let mut item_quality = 1.0;
        for parameter in parts {
            let Some((name, value)) = parameter.trim().split_once('=') else {
                continue;
            };
            if name.trim().eq_ignore_ascii_case("q") {
                item_quality = value
                    .trim()
                    .parse::<f32>()
                    .ok()
                    .filter(|quality| (0.0..=1.0).contains(quality))
                    .unwrap_or(0.0);
                break;
            }
        }
        let specificity = offered
            .iter()
            .filter_map(|media_type| media_range_specificity(range, media_type))
            .max();
        if let Some(specificity) = specificity {
            match &mut best {
                Some((best_specificity, best_quality)) if *best_specificity == specificity => {
                    *best_quality = best_quality.max(item_quality);
                }
                Some((best_specificity, _)) if *best_specificity > specificity => {}
                _ => best = Some((specificity, item_quality)),
            }
        }
    }
    best
}

fn negotiate(request: &HttpRequest) -> IndexFormat {
    let Some(value) = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
    else {
        return IndexFormat::LegacyHtml;
    };

    let json = quality(
        value,
        &[JSON_MEDIA_TYPE, "application/vnd.pypi.simple.latest+json"],
    );
    let modern_html = quality(
        value,
        &[HTML_MEDIA_TYPE, "application/vnd.pypi.simple.latest+html"],
    );
    let legacy_html = quality(value, &["text/html"]);
    let max_quality = [json, modern_html, legacy_html]
        .into_iter()
        .flatten()
        .map(|(_, quality)| quality)
        .fold(0.0, f32::max);
    if max_quality == 0.0 {
        return IndexFormat::LegacyHtml;
    }
    let max_specificity = [json, modern_html, legacy_html]
        .into_iter()
        .flatten()
        .filter(|(_, quality)| *quality == max_quality)
        .map(|(specificity, _)| specificity)
        .max()
        .unwrap_or_default();
    // Keep the long-established HTML default for a universal wildcard. More
    // specific ties prefer the richer JSON representation.
    if max_specificity == 0
        && legacy_html.is_some_and(|score| score == (max_specificity, max_quality))
    {
        IndexFormat::LegacyHtml
    } else if json.is_some_and(|score| score == (max_specificity, max_quality)) {
        IndexFormat::Json
    } else if modern_html.is_some_and(|score| score == (max_specificity, max_quality)) {
        IndexFormat::ModernHtml
    } else {
        IndexFormat::LegacyHtml
    }
}

pub fn normalize_name(name: &str) -> String {
    let mut normalized = String::with_capacity(name.len());
    let mut separator = false;
    for ch in name.chars().flat_map(char::to_lowercase) {
        if matches!(ch, '-' | '_' | '.') {
            if !separator {
                normalized.push('-');
                separator = true;
            }
        } else {
            normalized.push(ch);
            separator = false;
        }
    }
    normalized
}

fn index_key(path: &str, format: IndexFormat) -> String {
    if path.is_empty() {
        format.filename().to_string()
    } else {
        format!(
            "{}/{filename}",
            path.trim_matches('/'),
            filename = format.filename()
        )
    }
}

fn canonicalized_path(path: &str) -> String {
    let mut segments = path.split('/').map(ToString::to_string).collect::<Vec<_>>();
    if segments.len() >= 2
        && let Some(project) = segments.last_mut()
    {
        *project = normalize_name(project);
        return segments.join("/");
    }
    normalize_name(path)
}

async fn fetch_index(
    storage: &'static str,
    path: &str,
    format: IndexFormat,
    method: &Method,
    mission: &IntelMission,
    config: &Config,
) -> Result<Option<reqwest::Response>> {
    let task = Task {
        storage,
        origin: String::new(),
        path: index_key(path, format),
        retry_limit: 0,
    };
    let request = if method == Method::HEAD {
        mission.client.head(task.cached_url(config))
    } else {
        mission.client.get(task.cached_url(config))
    };
    let response = request.send().await?;
    let status = response.status();
    if status.is_success() {
        return Ok(Some(response));
    }
    // Anonymous S3 commonly reports a missing private key as either 403 or 404.
    if matches!(status.as_u16(), 403 | 404) {
        return Ok(None);
    }
    let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    Err(Error::Http(if status.is_server_error() {
        StatusCode::BAD_GATEWAY
    } else {
        status
    }))
}

pub async fn canonical_index_path(
    storage: &'static str,
    path: &str,
    mission: &IntelMission,
    config: &Config,
) -> Result<Option<String>> {
    if fetch_index(
        storage,
        path,
        IndexFormat::ModernHtml,
        &Method::HEAD,
        mission,
        config,
    )
    .await?
    .is_some()
    {
        return Ok(Some(path.to_string()));
    }

    let canonical = canonicalized_path(path);
    if canonical != path
        && fetch_index(
            storage,
            &canonical,
            IndexFormat::ModernHtml,
            &Method::HEAD,
            mission,
            config,
        )
        .await?
        .is_some()
    {
        return Ok(Some(canonical));
    }
    Ok(None)
}

pub async fn serve(
    storage: &'static str,
    public_route: &'static str,
    path: &str,
    request: &HttpRequest,
    mission: web::Data<IntelMission>,
    config: web::Data<Config>,
) -> Result<IntelResponse> {
    let format = negotiate(request);
    let method = request.method();
    let mut response = fetch_index(storage, path, format, method, &mission, &config).await?;

    if response.is_none() && !path.is_empty() {
        let canonical = canonicalized_path(path);
        if canonical != path
            && fetch_index(
                storage,
                &canonical,
                format,
                &Method::HEAD,
                &mission,
                &config,
            )
            .await?
            .is_some()
        {
            let mut location = format!("/{public_route}/{canonical}/");
            if let Some(query) = request.uri().query() {
                location.push('?');
                location.push_str(query);
            }
            return Ok(HttpResponse::MovedPermanently()
                .insert_header((header::LOCATION, location))
                .finish()
                .into());
        }
    }

    let Some(response) = response.take() else {
        return Ok(HttpResponse::NotFound().finish().into());
    };
    let content_length = response.content_length();
    let mut builder = HttpResponse::build(StatusCode::OK);
    builder.insert_header((header::CONTENT_TYPE, format.content_type()));
    builder.insert_header((header::VARY, "Accept"));
    builder.insert_header((header::CACHE_CONTROL, "public, max-age=300"));
    if let Some(content_length) = content_length {
        builder.insert_header((header::CONTENT_LENGTH, content_length));
    }
    if method == Method::HEAD {
        return Ok(builder.finish().into());
    }
    Ok(builder.body(response.bytes().await?).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::TestRequest;

    #[test]
    fn negotiates_pip_style_accept_header() {
        let request = TestRequest::default()
            .insert_header((
                header::ACCEPT,
                "application/vnd.pypi.simple.v1+json, application/vnd.pypi.simple.v1+html;q=0.1, text/html;q=0.01",
            ))
            .to_http_request();
        assert_eq!(negotiate(&request), IndexFormat::Json);
    }

    #[test]
    fn negotiates_latest_to_concrete_format() {
        let json = TestRequest::default()
            .insert_header((header::ACCEPT, "application/vnd.pypi.simple.latest+json"))
            .to_http_request();
        assert_eq!(negotiate(&json), IndexFormat::Json);

        let html = TestRequest::default()
            .insert_header((header::ACCEPT, "application/vnd.pypi.simple.latest+html"))
            .to_http_request();
        assert_eq!(negotiate(&html), IndexFormat::ModernHtml);
    }

    #[test]
    fn negotiates_media_ranges_and_specific_exclusions() {
        let application = TestRequest::default()
            .insert_header((header::ACCEPT, "application/*"))
            .to_http_request();
        assert_eq!(negotiate(&application), IndexFormat::Json);

        let excluded_json = TestRequest::default()
            .insert_header((
                header::ACCEPT,
                "application/vnd.pypi.simple.v1+json;q=0, application/*;q=0.8",
            ))
            .to_http_request();
        assert_eq!(negotiate(&excluded_json), IndexFormat::ModernHtml);

        let any = TestRequest::default()
            .insert_header((header::ACCEPT, "*/*"))
            .to_http_request();
        assert_eq!(negotiate(&any), IndexFormat::LegacyHtml);
    }

    #[test]
    fn normalizes_project_names() {
        assert_eq!(normalize_name("Typing_Extensions"), "typing-extensions");
    }
}
