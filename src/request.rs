use anyhow::Result as AnyhowResult;
use futures::TryStreamExt;
use hyper::http::HeaderValue;
use hyper::{Body, Request, HeaderMap};

/// NB: This much match the shape of SpeakRequest in the 'voder/tts_service' code.
/// This is used for both /speak and /speak_spectrogram requests.
#[derive(Deserialize, Debug)]
pub struct ExternalSpeakRequest {
  /// Slug for the speaker
  pub speaker: String,

  /// Raw text to be spoken
  pub text: String,
}

/// The full request
pub struct ExternalHttpRequest {
  pub headers: HeaderMap<HeaderValue>,
  pub speak_request: ExternalSpeakRequest,
}

#[derive(Serialize, Debug)]
pub struct InternalSpeakRequest {
  /// Slug for the speaker
  pub speaker: String,

  /// Raw text to be spoken
  pub text: String,

  /// Retry attempt.
  /// The first request is "0", and all subsequent requests are incremented.
  /// Subsequent requests do not trigger the rate limiter.
  pub retry_attempt_number: u8,

  /// Admin/debug requests get to skip the rate limiter.
  pub skip_rate_limiter: bool,
}


impl ExternalHttpRequest {

  pub async fn decode_request(request: Request<Body>) -> AnyhowResult<Self> {
    let headers = request.headers().clone();
    let (_parts, body) = request.into_parts();

    let body_bytes = body_to_bytes(body).await?;
    let speak_request = serde_json::from_slice::<ExternalSpeakRequest>(&body_bytes)?;

    Ok(ExternalHttpRequest {
      headers: headers,
      speak_request:  speak_request,
    })
  }
}

async fn body_to_bytes(body: Body) -> AnyhowResult<Vec<u8>> {
  // Also: let body_bytes = hyper::body::to_bytes(body)
  let bytes = body.try_fold(Vec::new(), |mut data, chunk| async move {
    data.extend_from_slice(&chunk);
    Ok(data)
  }).await?;

  Ok(bytes)
}
