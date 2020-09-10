#![feature(toowned_clone_into)]

#[macro_use] extern crate anyhow;
#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;

pub mod api;
pub mod newrelic_logger;
pub mod speak_proxy;

use anyhow::Result as AnyhowResult;
use crate::newrelic_logger::{NewRelicLogger, MaybeNewRelicTransaction};
use crate::speak_proxy::speak_proxy_with_retry;
use futures::future::{self, Future};
use hyper::header::HeaderValue;
use hyper::http::header::HeaderName;
use hyper::server::conn::AddrStream;
use hyper::service::{service_fn, make_service_fn};
use hyper::{Body, Request, Response, Server};
use hyper::{Method, StatusCode};
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::convert::Infallible;
use std::env;
use std::fmt::Debug;
use std::fmt::Display;
use std::net::{SocketAddr, IpAddr};
use std::str::FromStr;
use std::sync::Arc;

const ENV_DEFAULT_ROUTE : &'static str = "DEFAULT_ROUTE";
const ENV_NEWRELIC_API_KEY : &'static str = "NEWRELIC_API_KEY";
const ENV_PROXY_CONFIG_FILE : &'static str = "PROXY_CONFIG_FILE";
const ENV_RUST_LOG : &'static str = "RUST_LOG";
const ENV_SERVICE_PORT : &'static str = "SERVICE_PORT";

const DEFAULT_DEFAULT_ROUTE : &'static str = "http://127.0.0.1:12345";
const DEFAULT_NEWRELIC_API_KEY : &'static str = "";
const DEFAULT_PROXY_CONFIG_FILE : &'static str = "proxy_configs.toml";
const DEFAULT_RUST_LOG : &'static str = "tokio_reactor=warn,hyper=info,debug";
const DEFAULT_SERVICE_PORT : u16 = 5555;

#[derive(Deserialize, Debug, Clone)]
struct ProxyConfigs {
  pub backends: Vec<ProxyConfig>,
}

#[derive(Deserialize, Debug, Clone)]
struct ProxyConfig {
  pub voice : String,
  pub ip: String,
  pub port: String,
}

impl ProxyConfigs {
  pub fn load_from_file(filename: &str) -> AnyhowResult<Self> {
    let contents = std::fs::read_to_string(filename)?;
    let configs = toml::from_str(&contents)?;
    Ok(configs)
  }
}

fn get_env_string(env_name: &str, default: &str) -> String {
  match env::var(env_name).as_ref().ok() {
    Some(s) => s.to_string(),
    None => {
      warn!("Env var '{}' not supplied. Using default '{}'.", env_name, default);
      default.to_string()
    },
  }
}

// the trait `std::error::Error` is not implemented for `<T as std::str::FromStr>::Err`
fn get_env_num<T>(env_name: &str, default: T) -> AnyhowResult<T>
  where T: FromStr + Display,
        T::Err: Debug
{
  match env::var(env_name).as_ref().ok() {
    None => {
      warn!("Env var '{}' not supplied. Using default '{}'.", env_name, default);
      Ok(default)
    },
    Some(val) => {
      match val.parse::<T>() {
        Err(e) => bail!("Can't parse value '{:?}'. Error: {:?}", val, e),
        Ok(val) => Ok(val),
      }
    },
  }
}

pub struct Router {
  pub routes: HashMap<String, String>,
  pub default_route: String,
}

impl Router {
  fn get_random_host(&self) -> String {
    let mut rng = rand::thread_rng();
    self.routes.values()
      .choose(&mut rng)
      .map(|url| url.to_string())
      .unwrap_or(self.default_route.clone())
  }

  fn get_speaker_host(&self, voice: &str) -> String {
    self.routes.get(voice)
      .map(|url| url.to_string())
      .unwrap_or(self.default_route.clone())
  }
}

fn debug_request(req: Request<Body>) -> Result<Response<Body>, Infallible>  {
  let body_str = format!("{:?}", req);
  Ok(Response::new(Body::from(body_str)))
}

fn health_check_response(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
  Ok(Response::new(Body::from("healthy")))
}

pub async fn handle(
  client_ip: IpAddr,
  req: Request<Body>,
  router: Arc<Router>,
  newrelic_logger: Arc<NewRelicLogger>) -> Result<Response<Body>, Infallible>
{
  match req.uri().path() {
    "/speak" => {
      let nr_transaction = newrelic_logger.web_transaction("/speak");
      let result = speak_proxy_with_retry(client_ip, req, router, nr_transaction, "/speak").await;
      match result {
        Ok(response) => Ok(response),
        Err(error) => Ok(error.to_response()),
      }
    },
    "/speak_spectrogram" => {
      let nr_transaction = newrelic_logger.web_transaction("/speak_spectrogram");
      let result = speak_proxy_with_retry(client_ip, req, router, nr_transaction, "/speak_spectrogram").await;
      match result {
        Ok(response) => Ok(response),
        Err(error) => Ok(error.to_response()),
      }
    },
    "/proxy_health" => {
      let _droppable_transaction = newrelic_logger.web_transaction("/proxy_health");
      health_check_response(req)
    },
    _ => {
      let _droppable_transaction = newrelic_logger
        .web_transaction("unmatched endpoint");
      let forward = router.get_random_host();

      info!("Forwarding to `{}` random host: {}", &forward, req.uri());

      match hyper_reverse_proxy::call(client_ip, &forward, req).await {
        Ok(response) => {
          Ok(response)
        },
        Err(_) => {
          Ok(Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("unknown proxy error"))
            .unwrap())
        }
      }
    },
  }
}

#[tokio::main]
async fn main() {
  if env::var(ENV_RUST_LOG)
    .as_ref()
    .ok()
    .is_none()
  {
    println!("Setting default logging level to \"{}\", override with env var {}.",
             DEFAULT_RUST_LOG, ENV_RUST_LOG);
    std::env::set_var(ENV_RUST_LOG, DEFAULT_RUST_LOG);
  }

  env_logger::init();

  let default_route = get_env_string(ENV_DEFAULT_ROUTE, DEFAULT_DEFAULT_ROUTE);
  let service_port = get_env_num::<u16>(ENV_SERVICE_PORT, DEFAULT_SERVICE_PORT)
    .expect("should have env var");
  let newrelic_api_key = get_env_string(ENV_NEWRELIC_API_KEY, DEFAULT_NEWRELIC_API_KEY);

  info!("Default route: {}", default_route);

  let proxy_configs_file = get_env_string(ENV_PROXY_CONFIG_FILE, DEFAULT_PROXY_CONFIG_FILE);
  info!("Proxy config file: {}", proxy_configs_file);

  let proxy_configs = ProxyConfigs::load_from_file(&proxy_configs_file)
    .expect("should have configs");
  info!("Proxy configs: {:?}", proxy_configs);

  let mut route_map = HashMap::new();

  for backend in proxy_configs.backends.iter() {
    let host = format!("http://{}:{}", backend.ip, backend.port);
    route_map.insert(backend.voice.clone(), host);
  }

  let router = Arc::new(Router {
    routes: route_map,
    default_route: default_route,
  });

  let newrelic_logger = if newrelic_api_key.is_empty() {
    NewRelicLogger::null_instance()
  } else {
    NewRelicLogger::try_new_or_null("voder-proxy", &newrelic_api_key)
  };

  let newrelic_logger = Arc::new(newrelic_logger);

  let make_service = make_service_fn(move |conn: &AddrStream| {
    let remote_addr = conn.remote_addr().ip();
    let router2 = router.clone();
    let newrelic_logger2 = newrelic_logger.clone();

    async move {
      let router3 = router2.clone();
      let newrelic_logger3 = newrelic_logger2.clone();

      Ok::<_, Infallible>(service_fn(move |req| {
        let router4 = router3.clone();
        let newrelic_logger4 = newrelic_logger3.clone();
        handle(remote_addr, req, router4, newrelic_logger4)
      }))
    }
  });

  let addr = ([0, 0, 0, 0], service_port).into();

  info!("Running server on {:?}", addr);

  let server = Server::bind(&addr).serve(make_service);

  if let Err(e) = server.await {
    eprintln!("server error: {}", e);
  }
}
