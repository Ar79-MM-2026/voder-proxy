use anyhow::Result as AnyhowResult;
use crate::newrelic_logger::{NewRelicLogger, MaybeNewRelicTransaction};
use crate::{BoxFut, Router};
use futures::TryStreamExt;
use futures::future::{self, Future};
use hyper::header::HeaderValue;
use hyper::http::header::HeaderName;
use hyper::server::conn::AddrStream;
use hyper::service::{service_fn, make_service_fn};
use hyper::{Body, Request, Response, Server};
use hyper::{Method, StatusCode};
use hyper_reverse_proxy::ProxyError;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::convert::Infallible;
use std::env;
use std::fmt::Debug;
use std::fmt::Display;
use std::net::{SocketAddr, IpAddr};
use std::str::FromStr;
use std::sync::Arc;

/// NB: This much match the shape of SpeakRequest in the 'voder/tts_service' code.
/// This is used for both /speak and /speak_spectrogram requests.
#[derive(Serialize, Deserialize, Debug)]
pub struct SpeakRequest {
  /// Slug for the speaker
  speaker: String,
  /// Raw text to be spoken
  text: String,
}

struct RequestDetails {
  pub request_bytes: Vec<u8>,
  pub speaker: String,
}

pub async fn speak_proxy_with_retry(
  ip_addr: IpAddr,
  req: Request<Body>,
  router: Arc<Router>,
  _transaction: MaybeNewRelicTransaction,
  endpoint: &'static str) -> Result<Response<Body>, ResponseError>
{
  let mut attempt : u8 = 0;

  let headers = req.headers().clone();

  let (_parts, body) = req.into_parts();

  let body_bytes = body_to_bytes(body).await.unwrap();

  let body_string = String::from_utf8(body_bytes)
    .unwrap();

  println!("Body: {:?}", body_string);

  loop {
    info!("Attempt #{}", attempt);

    let mut request_builder = Request::builder();

    for (k, v) in headers.iter() {
      // NB: Builder pattern ownership
      request_builder = request_builder.header(k.clone(), v.clone());
    }

    let request = request_builder
      .body(Body::from(body_string.clone()))
      .unwrap();

    let transaction = MaybeNewRelicTransaction::from_optional(None);
    let result = speak_proxy(ip_addr, request, router.clone(), transaction, endpoint).await;

    if result.is_ok() {
      return result;
    }

    attempt += 1;

    if attempt > 3 {
      return Err(ResponseError { reason: "max attempts reached".to_string() });
    }
  }
}

#[derive(Serialize, Debug)]
pub struct ResponseError {
  pub reason: String,
}

pub async fn speak_proxy(
  ip_addr: IpAddr,
  req: Request<Body>,
  router: Arc<Router>,
  transaction: MaybeNewRelicTransaction,
  endpoint: &'static str) -> Result<Response<Body>, ResponseError>
{
  let mut headers = req.headers().clone();

  // This is the IP of the Digital Ocean load balancer.
  let gateway_ip = HeaderValue::from_str(&ip_addr.to_string())
    .ok()
    .unwrap_or(HeaderValue::from_static(""));

  // This is the IP of the upstream client browser.
  let upstream_client_ip = req.headers().get(HeaderName::from_static("x-forwarded-for"))
    .map(|ip| ip.to_str().unwrap_or(""))
    .unwrap_or("");

  let upstream_client_ip = HeaderValue::from_str(upstream_client_ip)
    .ok()
    .unwrap_or(HeaderValue::from_static(""));

  headers.insert(HeaderName::from_static("forwarded"), upstream_client_ip.clone());
  headers.insert(HeaderName::from_static("x-forwarded-for"), upstream_client_ip.clone());
  // Unfortunately it looks like this middleware trounces the standard headers, so
  // here we put it somewhere it won't be overwritten.
  headers.insert(HeaderName::from_static("x-voder-proxy-for"), upstream_client_ip.clone());
  headers.insert(HeaderName::from_static("x-gateway-ip"), gateway_ip);

  let (_parts, body) = req.into_parts();

  let request_bytes = body_to_bytes(body).await.unwrap();

  let request = serde_json::from_slice::<SpeakRequest>(&request_bytes)
    .unwrap();

  let request_details = RequestDetails {
    request_bytes: request_bytes.clone(),
    speaker: request.speaker,
  };

  let proxy_host = router.get_speaker_host(&request_details.speaker);

  info!("Routing {} for {} to {}", endpoint, &request_details.speaker, &proxy_host);

  transaction.add_attribute("speaker", &request_details.speaker);

  let mut request_builder = Request::builder()
    .method(Method::POST)
    .uri(endpoint);

  for (k, v) in headers.iter() {
    // NB: Handling move because of builder class
    request_builder = request_builder.header(k, v);
  }

  let new_req = request_builder
    .body(Body::from(request_details.request_bytes))
    .unwrap();

  //Box::new(future::ok(Vec::new()))
  match hyper_reverse_proxy::call(ip_addr, &proxy_host, new_req).await {
    Ok(result) => Ok(result),
    Err(_result) => Err(ResponseError {
      reason: "Uknown failure".to_string()
    }),
  }
}

async fn body_to_bytes(body: Body) -> AnyhowResult<Vec<u8>> {
  // Also: let body_bytes = hyper::body::to_bytes(body)
  let bytes = body.try_fold(Vec::new(), |mut data, chunk| async move {
      data.extend_from_slice(&chunk);
      Ok(data)
    })
    .await?;

  Ok(bytes)
}
