use anyhow::Result as AnyhowResult;
use crate::Router;
use crate::api::{ExternalSpeakRequest, ExternalHttpRequest, InternalSpeakRequest, ErrorResponse, ErrorType};
use crate::newrelic_logger::{NewRelicLogger, MaybeNewRelicTransaction};
use futures::TryStreamExt;
use futures::future::{self, Future};
use hyper::header::HeaderValue;
use hyper::http::header::HeaderName;
use hyper::server::conn::AddrStream;
use hyper::service::{service_fn, make_service_fn};
use hyper::{Body, Request, Response, Server, HeaderMap};
use hyper::{Method, StatusCode};
use hyper_reverse_proxy::ProxyError;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::net::{SocketAddr, IpAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{env, thread};

pub async fn speak_proxy_with_retry(
  ip_addr: IpAddr,
  request: Request<Body>,
  router: Arc<Router>,
  _transaction: MaybeNewRelicTransaction,
  endpoint: &'static str) -> Result<Response<Body>, ErrorResponse>
{
  let mut attempt : u8 = 0;

  let external_http_request = ExternalHttpRequest::decode_request(request).await.unwrap();

  // This is the IP of the Digital Ocean load balancer.
  let gateway_ip = HeaderValue::from_str(&ip_addr.to_string())
    .ok()
    .unwrap_or(HeaderValue::from_static(""));

  // This is the IP of the upstream client browser.
  let upstream_client_ip = external_http_request.headers.get(HeaderName::from_static("x-forwarded-for"))
    .map(|ip| ip.to_str().unwrap_or(""))
    .unwrap_or("");

  let upstream_client_ip = HeaderValue::from_str(upstream_client_ip)
    .ok()
    .unwrap_or(HeaderValue::from_static(""));

  let mut headers_to_proxy = external_http_request.headers.clone();

  headers_to_proxy.insert(HeaderName::from_static("forwarded"), upstream_client_ip.clone());
  headers_to_proxy.insert(HeaderName::from_static("x-forwarded-for"), upstream_client_ip.clone());

  // Unfortunately it looks like this middleware trounces the standard headers, so
  // here we put it somewhere it won't be overwritten.
  headers_to_proxy.insert(HeaderName::from_static("x-voder-proxy-for"), upstream_client_ip.clone());
  headers_to_proxy.insert(HeaderName::from_static("x-gateway-ip"), gateway_ip);

  // We're going to be changing the request body length, so remove the reported value
  // so Hyper doesn't choke on any mismatch.
  headers_to_proxy.remove(HeaderName::from_static("content-length"));

  // The proxy host is a Kubernetes load balancer. We're not doing the round robin magic here,
  // so it makes sense to reuse the same value for each retry.
  let load_balancer_hostname = router.get_speaker_host(&external_http_request.speak_request.speaker);

  loop {
    let internal_speak_request = InternalSpeakRequest {
      speaker: external_http_request.speak_request.speaker.clone(),
      text: external_http_request.speak_request.text.clone(),
      retry_attempt_number: attempt,
      skip_rate_limiter: false,
    };

    let result = speak_proxy(
      ip_addr,
      internal_speak_request,
      headers_to_proxy.clone(),
      &load_balancer_hostname,
      endpoint).await;

    match result {
      Ok(good_response) => return Ok(good_response),
      Err(_) => {},
    }

    attempt += 1;

    thread::sleep(Duration::from_millis(400)); // TODO: Needs backoff + jitter

    if attempt > 2 {
      return Err(ErrorResponse {
        error_type: ErrorType::ProxyError,
        error_description: "max internal retry attempts reached".to_string(),
      });
    }
  }
}

#[derive(Serialize, Debug)]
pub struct ResponseError {
  pub reason: String,
}

async fn speak_proxy(
  ip_addr: IpAddr,
  internal_speak_request: InternalSpeakRequest,
  headers: HeaderMap<HeaderValue>,
  load_balancer_hostname: &str,
  endpoint: &'static str) -> Result<Response<Body>, ()>
{
  info!("Routing {} for {} to {} (attempt # {})", endpoint, &internal_speak_request.speaker,
        &load_balancer_hostname, internal_speak_request.retry_attempt_number);

  let mut request_builder = Request::builder()
    .method(Method::POST)
    .uri(endpoint);

  for (k, v) in headers.iter() {
    // NB: Handling move because of builder class
    request_builder = request_builder.header(k, v);
  }

  let json_speak_request = serde_json::to_string(&internal_speak_request).unwrap();

  let request = request_builder
    .body(Body::from(json_speak_request))
    .unwrap();

  match hyper_reverse_proxy::call(ip_addr, &load_balancer_hostname, request).await {
    Ok(result) => Ok(result),
    Err(_result) => Err(()), // TODO: Surface this?
  }
}
