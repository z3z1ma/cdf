use std::{future::Future, pin::Pin};

use futures_core::Stream;

use crate::{batch::Batch, error::Result};

pub type BatchStream = Pin<Box<dyn Stream<Item = Result<Batch>> + Send + 'static>>;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
