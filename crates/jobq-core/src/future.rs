use event_listener::Event;
use futures::{
    channel::{mpsc, oneshot},
    future::{BoxFuture, join_all, try_join_all},
    stream::Stream,
};
use mea::mutex::Mutex;
use std::{
    future::{IntoFuture, poll_fn},
    iter::FromIterator,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use crate::error::Error;

struct CompletionState {
    setter: Option<JobFutureSetter<()>>,
    outcome: Option<Result<(), Error>>,
}

impl CompletionState {
    fn new(setter: Option<JobFutureSetter<()>>) -> Self {
        Self { setter, outcome: None }
    }

    fn finish(&mut self, outcome: Result<(), Error>) -> bool {
        if self.outcome.is_some() {
            return false;
        }

        self.outcome = Some(outcome.clone());

        if let Some(mut setter) = self.setter.take() {
            setter.set_result(outcome);
        }

        true
    }

    fn is_finished(&self) -> bool {
        self.outcome.is_some()
    }

    fn failed(&self) -> bool {
        self.outcome
            .as_ref()
            .is_some_and(Result::is_err)
    }
}

struct StreamCompletion {
    state: std::sync::Mutex<CompletionState>,
}

impl StreamCompletion {
    fn new(setter: Option<JobFutureSetter<()>>) -> Self {
        Self {
            state: std::sync::Mutex::new(CompletionState::new(setter)),
        }
    }

    fn finish(&self, outcome: Result<(), Error>) -> bool {
        self.state
            .lock()
            .unwrap()
            .finish(outcome)
    }

    fn is_finished(&self) -> bool {
        self.state.lock().unwrap().is_finished()
    }

    fn failed(&self) -> bool {
        self.state.lock().unwrap().failed()
    }
}

struct CloseSignal {
    closed: AtomicBool,
    event: Event,
}

impl CloseSignal {
    fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            event: Event::new(),
        }
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.event.notify(usize::MAX);
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    async fn closed(&self) {
        let listener = self.event.listen();

        if self.is_closed() {
            return;
        }

        listener.await;
    }
}

struct StreamLifecycleInner {
    completion: StreamCompletion,
    closure: CloseSignal,
    receiver_drop_error: Option<Error>,
}

impl StreamLifecycleInner {
    fn new(setter: Option<JobFutureSetter<()>>, receiver_drop_error: Option<Error>) -> Self {
        Self {
            completion: StreamCompletion::new(setter),
            closure: CloseSignal::new(),
            receiver_drop_error,
        }
    }

    fn set_terminal(&self, outcome: Result<(), Error>) -> bool {
        if !self.completion.finish(outcome) {
            return false;
        }

        self.closure.close();

        true
    }

    fn receiver_dropped(&self) {
        if let Some(error) = &self.receiver_drop_error {
            self.set_terminal(Err(error.clone()));
        }

        self.closure.close();
    }

    fn is_open(&self) -> bool {
        !self.closure.is_closed() && !self.completion.is_finished()
    }

    fn failed(&self) -> bool {
        self.completion.failed()
    }

    async fn closed(&self) {
        self.closure.closed().await;
    }
}

#[derive(Clone)]
pub(crate) struct StreamLifecycle {
    inner: Arc<StreamLifecycleInner>,
}

impl StreamLifecycle {
    pub(crate) fn new(
        setter: Option<JobFutureSetter<()>>,
        receiver_drop_error: Option<Error>,
    ) -> Self {
        Self {
            inner: Arc::new(StreamLifecycleInner::new(setter, receiver_drop_error)),
        }
    }

    pub(crate) fn set_terminal(&self, outcome: Result<(), Error>) -> bool {
        self.inner.set_terminal(outcome)
    }

    pub(crate) fn receiver_dropped(&self) {
        self.inner.receiver_dropped();
    }

    pub(crate) fn is_open(&self) -> bool {
        self.inner.is_open()
    }

    pub(crate) fn failed(&self) -> bool {
        self.inner.failed()
    }

    pub(crate) async fn closed(&self) {
        self.inner.closed().await;
    }
}

/// Represents a future that can be awaited to get the result of a [`Job`](crate::job::Job).
pub struct JobFuture<T>
where
    T: Send + Sync,
{
    inner: Arc<Mutex<JobFutureInner<T>>>,
}

impl<T> JobFuture<T>
where
    T: Send + Sync,
{
    /// Creates a new [`JobFuture`](crate::future::JobFuture) instance with a channel for receiving the job's result.
    ///
    /// # Returns
    /// A tuple containing the [`JobFuture`](crate::future::JobFuture) instance and a [`JobFutureSetter`](crate::future::JobFutureSetter)
    /// that can be used to set the result of the job.
    pub fn new() -> (Self, JobFutureSetter<T>) {
        let (sender, receiver) = oneshot::channel();
        let setter = JobFutureSetter { sender: Some(sender) };

        (
            Self {
                inner: Arc::new(Mutex::new(JobFutureInner {
                    result: None,
                    receiver: Some(receiver),
                    closed: AtomicBool::new(false),
                })),
            },
            setter,
        )
    }

    /// Awaits the result of the [`JobFuture`](crate::future::JobFuture).
    ///
    /// # Returns
    /// A `Result` containing the [`Job`](crate::job::Job)'s output if successful, or an error if the task failed or the future was closed.
    pub async fn result(&self) -> Result<T, Error> {
        let mut inner = self.inner.lock().await;

        if let Some(result) = inner.result.take() {
            return result;
        }

        if inner.closed.load(Ordering::SeqCst) {
            return Err(Error::future_closed());
        }

        match inner.receiver.take() {
            Some(receiver) => match receiver.await {
                Ok(result) => result,
                Err(_) => Err(Error::future_closed()),
            },
            None => Err(Error::future_closed()),
        }
    }

    /// Closes the future, preventing any further awaits on it.
    pub async fn close(&self) {
        let mut inner = self.inner.lock().await;

        inner
            .closed
            .store(true, Ordering::SeqCst);
        inner.receiver.take(); // Drop the receiver to prevent further awaits
        inner.result.take(); // Clear any existing result
    }
}

impl<T> IntoFuture for JobFuture<T>
where
    T: Send + Sync + 'static,
{
    type Output = Result<T, Error>;
    type IntoFuture = BoxFuture<'static, Result<T, Error>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.result().await })
    }
}

pub struct JobFutureInner<T>
where
    T: Send + Sync,
{
    result: Option<Result<T, Error>>,
    receiver: Option<oneshot::Receiver<Result<T, Error>>>,
    closed: AtomicBool,
}

/// A setter for the [`JobFuture`](crate::future::JobFuture) that allows setting the result
/// of the [`Job`](crate::job::Job) associated with the future.
#[derive(Debug)]
pub struct JobFutureSetter<T>
where
    T: Send + Sync,
{
    sender: Option<oneshot::Sender<Result<T, Error>>>,
}

impl<T> JobFutureSetter<T>
where
    T: Send + Sync,
{
    /// Sets the result of the [`JobFuture`](crate::future::JobFuture), sending it through the channel.
    ///
    /// # Arguments
    /// * `result` - The result of the [`Job`](crate::job::Job) to be sent.
    pub fn set_result(&mut self, result: Result<T, Error>) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(result);
        }
    }
}

/// A stream of items produced by a streaming job.
pub struct JobStream<T>
where
    T: Send + Sync,
{
    receiver: mpsc::Receiver<Result<T, Error>>,
    lifecycle: StreamLifecycle,
}

impl<T> JobStream<T>
where
    T: Send + Sync,
{
    pub fn new(capacity: usize) -> Result<(Self, JobStreamSetter<T>), Error> {
        Self::with_lifecycle(capacity, StreamLifecycle::new(None, None))
    }

    fn with_lifecycle(
        capacity: usize,
        lifecycle: StreamLifecycle,
    ) -> Result<(Self, JobStreamSetter<T>), Error> {
        Ok(Self::with_buffer(
            capacity
                .checked_sub(1)
                .filter(|buffer| *buffer < (usize::MAX >> 2))
                .ok_or_else(|| Error::invalid_stream_capacity(capacity))?,
            lifecycle,
        ))
    }

    fn with_buffer(buffer: usize, lifecycle: StreamLifecycle) -> (Self, JobStreamSetter<T>) {
        let (sender, receiver) = mpsc::channel(buffer);

        (
            Self { receiver, lifecycle: lifecycle.clone() },
            JobStreamSetter::new(sender, lifecycle),
        )
    }
}

impl<T> Drop for JobStream<T>
where
    T: Send + Sync,
{
    fn drop(&mut self) {
        self.lifecycle.receiver_dropped();
    }
}

impl<T> Stream for JobStream<T>
where
    T: Send + Sync,
{
    type Item = Result<T, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_next(cx)
    }
}

/// Sends the items produced by a streaming job to its [`JobStream`].
pub struct JobStreamSetter<T>
where
    T: Send + Sync,
{
    sender: mpsc::Sender<Result<T, Error>>,
    lifecycle: StreamLifecycle,
}

impl<T> JobStreamSetter<T>
where
    T: Send + Sync,
{
    fn new(sender: mpsc::Sender<Result<T, Error>>, lifecycle: StreamLifecycle) -> Self {
        Self { sender, lifecycle }
    }

    pub(crate) fn lifecycle(&self) -> StreamLifecycle {
        self.lifecycle.clone()
    }

    /// Sends one item to the [`JobStream`], awaiting if the channel is full.
    pub async fn send(&mut self, item: Result<T, Error>) -> Result<(), Error> {
        poll_fn(|cx| self.sender.poll_ready(cx))
            .await
            .map_err(|_| Error::future_closed())?;

        self.sender
            .start_send(item)
            .map_err(|_| Error::future_closed())
    }

    pub fn try_send(
        &mut self,
        item: Result<T, Error>,
    ) -> Result<(), mpsc::TrySendError<Result<T, Error>>> {
        self.sender.try_send(item)
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

/// The consumer handle for a streaming job: a [`Stream`] of produced items, plus a
/// [`result`](JobStreamHandle::result) for the terminal outcome.
pub struct JobStreamHandle<T>
where
    T: Send + Sync + 'static,
{
    items: JobStream<T>,
    result: JobFuture<()>,
}

impl<T> JobStreamHandle<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn new(
        capacity: usize,
        receiver_drop_error: Option<Error>,
    ) -> Result<(Self, JobStreamSetter<T>), Error> {
        let (result, setter) = JobFuture::new();
        let (items, sender) = JobStream::with_lifecycle(
            capacity,
            StreamLifecycle::new(Some(setter), receiver_drop_error),
        )?;

        Ok((Self { items, result }, sender))
    }

    /// Awaits the terminal outcome of the stream.
    pub async fn result(&self) -> Result<(), Error> {
        self.result.result().await
    }

    /// Splits the handle into its item stream and terminal future.
    pub fn split(self) -> (JobStream<T>, JobFuture<()>) {
        (self.items, self.result)
    }
}

impl<T> Stream for JobStreamHandle<T>
where
    T: Send + Sync + 'static,
{
    type Item = Result<T, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.items).poll_next(cx)
    }
}

/// A collection of [`JobFuture`](crate::future::JobFuture) instances that can be awaited together.
pub struct JobFutureSet<T>
where
    T: Send + Sync + 'static,
{
    futures: Vec<JobFuture<T>>,
}

impl<T> JobFutureSet<T>
where
    T: Send + Sync + 'static,
{
    /// Creates a new [`JobFutureSet`](crate::future::JobFutureSet) with the given futures.
    ///
    /// # Arguments
    /// * `futures` - A vector of [`JobFuture`](crate::future::JobFuture) instances to include in the set.
    ///
    /// # Returns
    /// A new instance of [`JobFutureSet`](crate::future::JobFutureSet).
    pub fn new(futures: Vec<JobFuture<T>>) -> Self {
        Self { futures }
    }

    /// Awaits all futures in the set and returns a vector of their results.
    pub async fn join_all(self) -> Vec<Result<T, Error>> {
        join_all(
            self.futures
                .into_iter()
                .map(|fut| fut.into_future()),
        )
        .await
    }

    /// Awaits all futures in the set and returns a vector of their results, returning an error if any future fails.
    pub async fn try_join_all(self) -> Result<Vec<T>, Error> {
        try_join_all(
            self.futures
                .into_iter()
                .map(|fut| fut.into_future()),
        )
        .await
    }
}

impl<T> IntoFuture for JobFutureSet<T>
where
    T: Send + Sync + 'static,
{
    type Output = Vec<Result<T, Error>>;
    type IntoFuture = BoxFuture<'static, Vec<Result<T, Error>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.join_all().await })
    }
}

impl<T> FromIterator<JobFuture<T>> for JobFutureSet<T>
where
    T: Send + Sync + 'static,
{
    fn from_iter<I: IntoIterator<Item = JobFuture<T>>>(iter: I) -> Self {
        Self { futures: iter.into_iter().collect() }
    }
}

impl<T> IntoIterator for JobFutureSet<T>
where
    T: Send + Sync + 'static,
{
    type Item = JobFuture<T>;
    type IntoIter = std::vec::IntoIter<JobFuture<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.futures.into_iter()
    }
}

impl<T> Deref for JobFutureSet<T>
where
    T: Send + Sync + 'static,
{
    type Target = Vec<JobFuture<T>>;

    fn deref(&self) -> &Self::Target {
        &self.futures
    }
}

impl<T> DerefMut for JobFutureSet<T>
where
    T: Send + Sync + 'static,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.futures
    }
}

impl<T> From<Vec<JobFuture<T>>> for JobFutureSet<T>
where
    T: Send + Sync + 'static,
{
    fn from(futures: Vec<JobFuture<T>>) -> Self {
        Self { futures }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt, StreamExt};

    #[test]
    fn rejects_zero_and_unsupported_capacity() {
        assert!(matches!(JobStream::<u8>::new(0), Err(Error::InvalidStreamCapacity(0))));
        assert!(matches!(
            JobStream::<u8>::new((usize::MAX >> 2) + 1),
            Err(Error::InvalidStreamCapacity(_))
        ));
        assert!(matches!(
            JobStreamHandle::<u8>::new(0, None),
            Err(Error::InvalidStreamCapacity(0))
        ));
    }

    #[tokio::test]
    async fn one_sender_observes_total_capacity() {
        for capacity in [1, 2, 8] {
            let (mut stream, mut setter) = JobStream::<usize>::new(capacity).unwrap();

            for item in 0..capacity {
                setter.try_send(Ok(item)).unwrap();
            }

            let rejected = setter
                .try_send(Ok(capacity))
                .unwrap_err();
            assert!(rejected.is_full());
            assert!(matches!(rejected.into_inner(), Ok(item) if item == capacity));

            for item in 0..capacity {
                assert_eq!(stream.next().await.unwrap().unwrap(), item);
            }
        }
    }

    #[tokio::test]
    async fn full_and_disconnected_attempts_return_the_item() {
        let (stream, mut setter) = JobStream::<u8>::new(1).unwrap();
        setter.try_send(Ok(1)).unwrap();

        let full = setter.try_send(Ok(2)).unwrap_err();
        assert!(full.is_full());
        assert!(matches!(full.into_inner(), Ok(2)));

        drop(stream);

        let closed = setter.try_send(Ok(3)).unwrap_err();
        assert!(closed.is_disconnected());
        assert!(matches!(closed.into_inner(), Ok(3)));
    }

    #[tokio::test]
    async fn awaited_send_waits_only_for_channel_capacity() {
        let (mut stream, mut setter) = JobStream::<u8>::new(1).unwrap();
        setter.try_send(Ok(1)).unwrap();

        let send = setter.send(Ok(2));
        futures::pin_mut!(send);
        assert!(send.as_mut().now_or_never().is_none());

        assert!(matches!(stream.next().await, Some(Ok(1))));
        send.await.unwrap();
        assert!(matches!(stream.next().await, Some(Ok(2))));
    }

    #[tokio::test]
    async fn receiver_drop_wakes_and_settles_batch_result() {
        let (handle, setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let lifecycle = setter.lifecycle();
        let (items, result) = handle.split();
        let closed = lifecycle.closed();
        futures::pin_mut!(closed);

        assert!(closed.as_mut().now_or_never().is_none());
        drop(items);
        closed.await;

        assert!(matches!(result.result().await, Err(Error::ConsumerCancelled)));
    }

    #[tokio::test]
    async fn first_terminal_result_survives_receiver_drop() {
        let (handle, setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let lifecycle = setter.lifecycle();
        let (items, result) = handle.split();

        assert!(lifecycle.set_terminal(Ok(())));
        drop(items);
        assert!(!lifecycle.set_terminal(Err(Error::incomplete_stream_batch())));
        assert!(result.result().await.is_ok());
    }

    #[tokio::test]
    async fn accepted_items_drain_after_terminal_result() {
        let (mut handle, mut setter) = JobStreamHandle::<u8>::new(2, None).unwrap();
        setter.try_send(Ok(1)).unwrap();
        setter.try_send(Ok(2)).unwrap();
        setter.lifecycle().set_terminal(Ok(()));
        drop(setter);

        assert!(handle.result().await.is_ok());
        assert!(matches!(handle.next().await, Some(Ok(1))));
        assert!(matches!(handle.next().await, Some(Ok(2))));
        assert!(handle.next().await.is_none());
    }
}
