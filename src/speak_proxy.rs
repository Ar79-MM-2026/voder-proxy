use anyhow::Result as AnyhowResult;
use crate::{BoxFut, Router};
use crate::newrelic_logger::{NewRelicLogger, MaybeNewRelicTransaction};
use futures::future::{self, Future};
use hyper::{Method, StatusCode};
use hyper::header::HeaderValue;
use hyper::http::header::HeaderName;
use hyper::server::conn::AddrStream;
use hyper::service::{service_fn, make_service_fn};
use hyper::{Body, Request, Response, Server};
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use std::fmt::Display;
use std::net::{SocketAddr, IpAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::convert::Infallible;

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

/*pub fn copy_request(request: &Request<Body>) -> Request<Body> {
  let body = hyper::body::to_bytes(request.body());
  Request::builder()
    .uri(request.uri())
    .method(request.method())
    .unwrap()
}*/

/*pub fn speak_proxy_with_retry(
  req: Request<Body>,
  remote_addr: SocketAddr,
  router: Arc<Router>,
  transaction: MaybeNewRelicTransaction,
  endpoint: &'static str) -> BoxFut
{
  speak_proxy(req, remote_addr, router, transaction, endpoint)
    .then(|result| {
      match result {
        Ok(t) => future::ok(t),
        Err(t) => future::err(t),
      }
    })
    .boxed()
}*/

/*pub fn speak_proxy(
  req: Request<Body>,
  remote_addr: SocketAddr,
  router: Arc<Router>,
  transaction: MaybeNewRelicTransaction,
  endpoint: &'static str) -> BoxFut
{
  let mut headers = req.headers().clone();

  // This is the IP of the Digital Ocean load balancer.
  let gateway_ip = HeaderValue::from_str(&remote_addr.ip().to_string())
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

  Box::new(req.into_body().concat2().map(move |b| { // Builds a BoxedFut to return
    let request_bytes = b.as_ref();

    let request = serde_json::from_slice::<SpeakRequest>(request_bytes)
      .unwrap();

    RequestDetails {
      request_bytes: request_bytes.to_vec(),
      speaker: request.speaker,
    }
  }).and_then(move |request_details: RequestDetails| {
    let proxy_host = router.get_speaker_host(&request_details.speaker);

    info!("Routing {} for {} to {}", endpoint, &request_details.speaker, &proxy_host);

    transaction.add_attribute("speaker", &request_details.speaker);

    let mut request_builder = Request::builder();
    request_builder.method(Method::POST)
      .uri(endpoint);

    for (k, v) in headers.iter() {
      request_builder.header(k, v);
    }

    let new_req = request_builder
      .body(Body::from(request_details.request_bytes))
      .unwrap();

    hyper_reverse_proxy::call(remote_addr.ip(), &proxy_host, new_req)
  }))
}*/


fn debug_request(req: Request<Body>) -> Result<Response<Body>, Infallible>  {
  let body_str = format!("{:?}", req);
  Ok(Response::new(Body::from(body_str)))
}

pub async fn handle(client_ip: IpAddr, req: Request<Body>) -> Result<Response<Body>, Infallible> {
  if req.uri().path().starts_with("/target/first") {
  // will forward requests to port 13901
  match hyper_reverse_proxy::call(client_ip, "http://127.0.0.1:13901", req).await {
    Ok(response) => {Ok(response)}
    Err(error) => {Ok(Response::builder()
      .status(StatusCode::INTERNAL_SERVER_ERROR)
      .body(Body::empty())
      .unwrap())}
    }
  } else if req.uri().path().starts_with("/target/second") {
    // will forward requests to port 13902
    match hyper_reverse_proxy::call(client_ip, "http://127.0.0.1:13902", req).await {
      Ok(response) => {Ok(response)}
      Err(error) => {Ok(Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::empty())
        .unwrap())}
    }
  } else {
    debug_request(req)
  }
}
