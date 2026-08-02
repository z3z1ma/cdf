use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use cdf_kernel::{CdfError, Result};

#[derive(Debug, Default)]
struct ForeignCancellationState {
    cancelled: AtomicBool,
    waiters: Mutex<Vec<Waker>>,
}

#[derive(Clone, Debug, Default)]
pub struct ForeignCancellation(Arc<ForeignCancellationState>);

impl ForeignCancellation {
    pub fn cancel(&self) {
        if self.0.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        let waiters = std::mem::take(&mut *self.0.waiters.lock().unwrap());
        for waiter in waiters {
            waiter.wake();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(CdfError::transient("foreign stream was cancelled"));
        }
        Ok(())
    }

    pub fn cancelled(&self) -> ForeignCancellationFuture {
        ForeignCancellationFuture {
            cancellation: self.clone(),
            registered: None,
        }
    }

    pub async fn await_or_cancel<T, F>(&self, operation: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let cancelled = self.cancelled();
        futures_util::pin_mut!(operation, cancelled);
        match futures_util::future::select(operation, cancelled).await {
            futures_util::future::Either::Left((result, _)) => result,
            futures_util::future::Either::Right(((), _)) => {
                Err(CdfError::transient("foreign stream was cancelled"))
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn waiter_count_for_test(&self) -> usize {
        self.0.waiters.lock().unwrap().len()
    }
}

pub struct ForeignCancellationFuture {
    cancellation: ForeignCancellation,
    registered: Option<Waker>,
}

impl Future for ForeignCancellationFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.cancellation.is_cancelled() {
            return Poll::Ready(());
        }
        let cancellation = self.cancellation.clone();
        let mut waiters = cancellation.0.waiters.lock().unwrap();
        if cancellation.is_cancelled() {
            return Poll::Ready(());
        }
        if let Some(previous) = self.registered.take()
            && let Some(index) = waiters
                .iter()
                .position(|waiter| waiter.will_wake(&previous))
        {
            waiters.swap_remove(index);
        }
        if !waiters
            .iter()
            .any(|waiter| waiter.will_wake(context.waker()))
        {
            waiters.push(context.waker().clone());
        }
        self.registered = Some(context.waker().clone());
        Poll::Pending
    }
}

impl Drop for ForeignCancellationFuture {
    fn drop(&mut self) {
        let Some(registered) = self.registered.take() else {
            return;
        };
        if let Ok(mut waiters) = self.cancellation.0.waiters.lock()
            && let Some(index) = waiters
                .iter()
                .position(|waiter| waiter.will_wake(&registered))
        {
            waiters.swap_remove(index);
        }
    }
}
