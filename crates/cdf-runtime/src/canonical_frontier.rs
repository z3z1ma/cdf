use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context, Poll},
};

use cdf_kernel::{BoxFuture, CdfError, Result};
use futures_util::{FutureExt, Stream, StreamExt, stream::FuturesUnordered};

pub type CanonicalBoxStream<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>;
pub type CanonicalStreamOpener<T> =
    Box<dyn FnMut(usize) -> Result<CanonicalBoxStream<T>> + Send + 'static>;
pub type CanonicalStreamCompletion = Box<dyn FnMut(usize) -> Result<()> + Send + 'static>;

type PendingPoll<T> = BoxFuture<'static, (usize, CanonicalBoxStream<T>, Result<Option<T>>)>;

struct ReadyPoll<T> {
    stream: Option<CanonicalBoxStream<T>>,
    outcome: Result<Option<T>>,
}

/// A lifecycle-owning, bounded canonical merge for independently running streams.
///
/// At most `maximum_active` streams are opened. Each active ordinal owns exactly
/// one pending poll or one ready outcome; a later ready stream is not polled again
/// until every earlier stream reaches EOF. Dropping the frontier drops every
/// active stream, allowing scoped producers to cancel and join through RAII.
pub struct CanonicalStreamFrontier<T> {
    stream_count: usize,
    maximum_active: usize,
    opener: CanonicalStreamOpener<T>,
    completion: CanonicalStreamCompletion,
    next_to_open: usize,
    canonical_ordinal: usize,
    pending: FuturesUnordered<PendingPoll<T>>,
    ready: BTreeMap<usize, ReadyPoll<T>>,
    admission_stopped: bool,
    terminal: bool,
}

impl<T> Unpin for CanonicalStreamFrontier<T> {}

pub fn canonical_stream_frontier<T: Send + 'static>(
    stream_count: usize,
    maximum_active: usize,
    opener: CanonicalStreamOpener<T>,
) -> Result<CanonicalBoxStream<T>> {
    canonical_stream_frontier_with_completion(
        stream_count,
        maximum_active,
        opener,
        Box::new(|_| Ok(())),
    )
}

/// Creates a canonical frontier that reports each unit only after its stream
/// reaches EOF in canonical order.
pub fn canonical_stream_frontier_with_completion<T: Send + 'static>(
    stream_count: usize,
    maximum_active: usize,
    opener: CanonicalStreamOpener<T>,
    completion: CanonicalStreamCompletion,
) -> Result<CanonicalBoxStream<T>> {
    if maximum_active == 0 {
        return Err(CdfError::contract(
            "canonical stream frontier requires nonzero active capacity",
        ));
    }
    let mut frontier = CanonicalStreamFrontier {
        stream_count,
        maximum_active,
        opener,
        completion,
        next_to_open: 0,
        canonical_ordinal: 0,
        pending: FuturesUnordered::new(),
        ready: BTreeMap::new(),
        admission_stopped: false,
        terminal: false,
    };
    frontier.fill_active();
    Ok(Box::pin(frontier))
}

impl<T: Send + 'static> CanonicalStreamFrontier<T> {
    fn active(&self) -> usize {
        self.pending.len() + self.ready.len()
    }

    fn fill_active(&mut self) {
        while !self.admission_stopped
            && self.next_to_open < self.stream_count
            && self.active() < self.maximum_active
        {
            let ordinal = self.next_to_open;
            self.next_to_open += 1;
            match (self.opener)(ordinal) {
                Ok(stream) => self.push_poll(ordinal, stream),
                Err(error) => {
                    self.ready.insert(
                        ordinal,
                        ReadyPoll {
                            stream: None,
                            outcome: Err(error),
                        },
                    );
                    self.admission_stopped = true;
                }
            }
        }
    }

    fn push_poll(&mut self, ordinal: usize, mut stream: CanonicalBoxStream<T>) {
        self.pending.push(
            async move {
                let outcome = stream.next().await.transpose();
                (ordinal, stream, outcome)
            }
            .boxed(),
        );
    }

    fn fail(&mut self, error: CdfError) -> Poll<Option<Result<T>>> {
        self.terminal = true;
        self.ready.clear();
        self.pending = FuturesUnordered::new();
        Poll::Ready(Some(Err(error)))
    }
}

impl<T: Send + 'static> Stream for CanonicalStreamFrontier<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let frontier = self.get_mut();
        if frontier.terminal {
            return Poll::Ready(None);
        }
        loop {
            if frontier.canonical_ordinal == frontier.stream_count {
                frontier.terminal = true;
                return Poll::Ready(None);
            }
            if let Some(ready) = frontier.ready.remove(&frontier.canonical_ordinal) {
                match ready.outcome {
                    Ok(Some(item)) => {
                        let stream = ready.stream.ok_or_else(|| {
                            CdfError::internal(
                                "canonical frontier ready item omitted its source stream",
                            )
                        });
                        match stream {
                            Ok(stream) => {
                                frontier.push_poll(frontier.canonical_ordinal, stream);
                                return Poll::Ready(Some(Ok(item)));
                            }
                            Err(error) => return frontier.fail(error),
                        }
                    }
                    Ok(None) => {
                        if let Err(error) = (frontier.completion)(frontier.canonical_ordinal) {
                            return frontier.fail(error);
                        }
                        frontier.canonical_ordinal += 1;
                        frontier.fill_active();
                        continue;
                    }
                    Err(error) => return frontier.fail(error),
                }
            }

            match Pin::new(&mut frontier.pending).poll_next(context) {
                Poll::Ready(Some((ordinal, stream, outcome))) => {
                    if outcome.is_err() {
                        frontier.admission_stopped = true;
                    }
                    if ordinal < frontier.canonical_ordinal
                        || frontier
                            .ready
                            .insert(
                                ordinal,
                                ReadyPoll {
                                    stream: Some(stream),
                                    outcome,
                                },
                            )
                            .is_some()
                    {
                        return frontier.fail(CdfError::internal(
                            "canonical frontier received a duplicate or retired ordinal",
                        ));
                    }
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    return frontier.fail(CdfError::internal(
                        "canonical frontier lost an active stream before its ordinal completed",
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Waker},
    };

    use futures_channel::oneshot;
    use futures_util::{StreamExt, TryStreamExt, stream};

    use super::*;

    #[test]
    fn stalled_head_bounds_later_polling_and_releases_in_canonical_order() {
        let (gate_sender, gate_receiver) = oneshot::channel::<()>();
        let mut gate_receiver = Some(gate_receiver);
        let opened = Arc::new(AtomicUsize::new(0));
        let later_items = Arc::new(AtomicUsize::new(0));
        let opened_by_opener = Arc::clone(&opened);
        let observed_later_items = Arc::clone(&later_items);
        let opener: CanonicalStreamOpener<u64> = Box::new(move |ordinal| {
            opened_by_opener.fetch_add(1, Ordering::SeqCst);
            match ordinal {
                0 => {
                    let receiver = gate_receiver
                        .take()
                        .ok_or_else(|| CdfError::internal("head stream gate was opened twice"))?;
                    Ok(Box::pin(stream::once(async move {
                        receiver
                            .await
                            .map(|()| 0)
                            .map_err(|_| CdfError::internal("head stream gate dropped"))
                    })))
                }
                1 => {
                    let counter = Arc::clone(&observed_later_items);
                    Ok(Box::pin(stream::iter([Ok(10), Ok(11)]).inspect(
                        move |_| {
                            counter.fetch_add(1, Ordering::SeqCst);
                        },
                    )))
                }
                2 => Ok(Box::pin(stream::iter([Ok(20)]))),
                _ => Err(CdfError::internal("unexpected test stream ordinal")),
            }
        });
        let mut frontier = canonical_stream_frontier(3, 2, opener).unwrap();
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            frontier.as_mut().poll_next(&mut context),
            Poll::Pending
        ));
        assert_eq!(opened.load(Ordering::SeqCst), 2);
        assert_eq!(later_items.load(Ordering::SeqCst), 1);

        gate_sender.send(()).unwrap();
        let values = futures_executor::block_on(frontier.try_collect::<Vec<_>>()).unwrap();
        assert_eq!(values, vec![0, 10, 11, 20]);
        assert_eq!(opened.load(Ordering::SeqCst), 3);
        assert_eq!(later_items.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn later_error_stops_admission_and_is_released_only_at_its_ordinal() {
        let (gate_sender, gate_receiver) = oneshot::channel::<()>();
        let mut gate_receiver = Some(gate_receiver);
        let opened = Arc::new(AtomicUsize::new(0));
        let opened_by_opener = Arc::clone(&opened);
        let opener: CanonicalStreamOpener<u64> = Box::new(move |ordinal| {
            opened_by_opener.fetch_add(1, Ordering::SeqCst);
            match ordinal {
                0 => {
                    let receiver = gate_receiver
                        .take()
                        .ok_or_else(|| CdfError::internal("head stream gate was opened twice"))?;
                    Ok(Box::pin(stream::once(async move {
                        receiver
                            .await
                            .map(|()| 0)
                            .map_err(|_| CdfError::internal("head stream gate dropped"))
                    })))
                }
                1 => Ok(Box::pin(stream::iter([Err(CdfError::data(
                    "later stream failed",
                ))]))),
                _ => Ok(Box::pin(stream::iter([Ok(99)]))),
            }
        });
        let mut frontier = canonical_stream_frontier(4, 2, opener).unwrap();
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            frontier.as_mut().poll_next(&mut context),
            Poll::Pending
        ));
        assert_eq!(opened.load(Ordering::SeqCst), 2);

        gate_sender.send(()).unwrap();
        assert_eq!(
            futures_executor::block_on(frontier.next())
                .unwrap()
                .unwrap(),
            0
        );
        let error = futures_executor::block_on(frontier.next())
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert_eq!(opened.load(Ordering::SeqCst), 2);
        assert!(futures_executor::block_on(frontier.next()).is_none());
    }

    #[test]
    fn completion_frontiers_publish_once_in_canonical_order() {
        let completed = Arc::new(Mutex::new(Vec::new()));
        let completion_log = Arc::clone(&completed);
        let opener: CanonicalStreamOpener<u64> = Box::new(move |ordinal| {
            Ok(Box::pin(stream::iter([
                Ok(u64::try_from(ordinal).unwrap()),
            ])))
        });
        let completion: CanonicalStreamCompletion = Box::new(move |ordinal| {
            completion_log.lock().unwrap().push(ordinal);
            Ok(())
        });
        let frontier = canonical_stream_frontier_with_completion(4, 3, opener, completion).unwrap();
        let values = futures_executor::block_on(frontier.try_collect::<Vec<_>>()).unwrap();

        assert_eq!(values, vec![0, 1, 2, 3]);
        assert_eq!(*completed.lock().unwrap(), vec![0, 1, 2, 3]);
    }
}
