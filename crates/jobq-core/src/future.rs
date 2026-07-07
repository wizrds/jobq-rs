use futures::{
    SinkExt, Stream,
    channel::mpsc,
    channel::oneshot::{Receiver, Sender, channel},
    future::{BoxFuture, join_all, try_join_all},
};
use mea::mutex::Mutex;
use std::{
    future::IntoFuture,
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
        let (sender, receiver) = channel();
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
    receiver: Option<Receiver<Result<T, Error>>>,
    closed: AtomicBool,
}

/// A setter for the [`JobFuture`](crate::future::JobFuture) that allows setting the result
/// of the [`Job`](crate::job::Job) associated with the future.
#[derive(Debug)]
pub struct JobFutureSetter<T>
where
    T: Send + Sync,
{
    sender: Option<Sender<Result<T, Error>>>,
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
}

impl<T> JobStream<T>
where
    T: Send + Sync,
{
    /// Creates a bounded stream and its setter with the given channel capacity.
    pub fn new(capacity: usize) -> (Self, JobStreamSetter<T>) {
        let (sender, receiver) = mpsc::channel(capacity);

        (Self { receiver }, JobStreamSetter { sender })
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
}

impl<T> JobStreamSetter<T>
where
    T: Send + Sync,
{
    /// Sends one item to the [`JobStream`], awaiting if the channel is full.
    pub async fn send(&mut self, item: Result<T, Error>) -> Result<(), Error> {
        self.sender
            .send(item)
            .await
            .map_err(|_| Error::future_closed())
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
    pub(crate) fn new(items: JobStream<T>, result: JobFuture<()>) -> Self {
        Self { items, result }
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
