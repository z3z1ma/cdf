//! Bounded, accounted collection of control-plane HTTP response bodies.

use std::pin::Pin;

use bytes::Bytes;
use cdf_http::HttpResponseBudget;
use cdf_kernel::{CdfError, Result};
use cdf_memory::AccountedBytes;
use futures_util::{Stream, TryStreamExt};

use crate::errors::sanitized_reqwest_error;

pub(crate) async fn read_bounded_response_body(
    response: reqwest::Response,
    declared_length: Option<u64>,
    context: &str,
    budget: &HttpResponseBudget,
) -> Result<Option<AccountedBytes>> {
    let error_context = context.to_owned();
    let stream = response.bytes_stream().map_err(move |error| {
        CdfError::transient(format!(
            "read {error_context} HTTP response body: {}",
            sanitized_reqwest_error(error)
        ))
    });
    collect_bounded_response_body(Box::pin(stream), declared_length, context, budget).await
}

pub(crate) type BoundedBodyStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + 'static>>;

pub(crate) async fn collect_bounded_response_body(
    mut stream: BoundedBodyStream,
    declared_length: Option<u64>,
    context: &str,
    budget: &HttpResponseBudget,
) -> Result<Option<AccountedBytes>> {
    if let Some(bytes) = declared_length
        && bytes > budget.maximum_body_bytes()
    {
        return Err(CdfError::data(format!(
            "{context} HTTP response declares {bytes} body bytes above its {}-byte limit",
            budget.maximum_body_bytes()
        )));
    }
    let reservation_bytes = declared_length.unwrap_or(budget.maximum_body_bytes());
    let lease = budget.reserve_body(reservation_bytes).await?;
    let capacity = usize::try_from(reservation_bytes)
        .map_err(|_| CdfError::data("HTTP response body limit exceeds usize"))?;
    let mut body = Vec::with_capacity(capacity);
    let mut remaining = reservation_bytes;
    while let Some(chunk) = stream.try_next().await? {
        budget.check_cancellation()?;
        let chunk_bytes = u64::try_from(chunk.len())
            .map_err(|_| CdfError::data("HTTP response chunk exceeds u64"))?;
        if chunk_bytes > remaining {
            return Err(CdfError::data(format!(
                "{context} HTTP response exceeds its {}-byte body limit",
                budget.maximum_body_bytes()
            )));
        }
        body.extend_from_slice(&chunk);
        remaining -= chunk_bytes;
    }
    budget.check_cancellation()?;
    if let Some(declared) = declared_length
        && declared != body.len() as u64
    {
        return Err(CdfError::data(format!(
            "{context} HTTP response declared {declared} body bytes but transferred {}",
            body.len()
        )));
    }
    match (lease, body.is_empty()) {
        (Some(lease), false) => budget
            .account_reserved_body(Bytes::from(body), lease)
            .map(Some),
        (Some(_), true) | (None, true) => Ok(None),
        (None, false) => Err(CdfError::internal(
            "nonempty HTTP response body has no memory reservation",
        )),
    }
}
