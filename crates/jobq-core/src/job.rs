use async_trait::async_trait;
use futures::{Stream, StreamExt, future::FutureExt};
use std::{
    any::Any,
    future::Future,
    iter::repeat_with,
    panic::AssertUnwindSafe,
    sync::{Arc, Weak},
};

use crate::{
    batch::{BatchPolicy, Batcher, StreamBatcher, Window},
    error::Error,
    executable::{AnyExecutable, Executable, Execute, ExecuteStream, Single},
    future::{JobFuture, JobFutureSetter, JobStream, JobStreamHandle, JobStreamSetter},
    queue::{fifo::FifoQueue, lifo::LifoQueue, priority::PriorityQueue, traits::Queue},
    task::{BatchStreamTask, BatchTask, FnStreamTask, FnTask, StreamTask, Task},
};

pub(crate) struct JobDelivery<O>
where
    O: Send + Sync,
{
    setter: JobFutureSetter<O>,
    attempts: usize,
    max_retries: usize,
}

impl<O> JobDelivery<O>
where
    O: Send + Sync,
{
    pub(crate) fn new(setter: JobFutureSetter<O>, max_retries: usize) -> Self {
        Self { setter, attempts: 0, max_retries }
    }

    fn retry(&mut self) -> bool {
        self.attempts += 1;
        self.attempts < self.max_retries
    }

    fn deliver(&mut self, result: Result<O, Error>) {
        self.setter.set_result(result);
    }
}

pub(crate) struct StreamDelivery<I>
where
    I: Send + Sync,
{
    items: JobStreamSetter<I>,
    completion: JobFutureSetter<()>,
}

impl<I> StreamDelivery<I>
where
    I: Send + Sync,
{
    pub(crate) fn new(items: JobStreamSetter<I>, completion: JobFutureSetter<()>) -> Self {
        Self { items, completion }
    }

    async fn send(&mut self, item: Result<I, Error>) {
        let _ = self.items.send(item).await;
    }

    fn is_closed(&self) -> bool {
        self.items.is_closed()
    }

    fn complete(&mut self, result: Result<(), Error>) {
        self.completion.set_result(result);
    }
}

/// Options for configuring a job in the job queue.
#[derive(Debug, Clone)]
pub struct JobOptions<T, Q>
where
    Q: Queue,
{
    pub task: T,
    pub max_retries: usize,
    pub queue_options: Option<Q::Options>,
}

impl<T, Q> JobOptions<T, Q>
where
    Q: Queue,
{
    /// Creates a new [`JobOptions`](crate::job::JobOptions) instance with the specified task.
    ///
    /// # Arguments
    /// * `task` - The task to be executed by the job.
    ///
    /// # Returns
    /// A new [`JobOptions`](crate::job::JobOptions) instance with the specified task and default
    /// values for max retries and queue options.
    pub fn new(task: T) -> Self {
        Self {
            task,
            max_retries: 1,
            queue_options: None,
        }
    }

    /// Sets the maximum number of retries for the job.
    ///
    /// # Arguments
    /// * `retries` - The maximum number of retries for the job.
    ///
    /// # Returns
    /// The updated [`JobOptions`](crate::job::JobOptions) instance with the specified maximum retries.
    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Sets the queue options for the job.
    ///
    /// # Arguments
    /// * `options` - The queue options to be used for the job.
    ///
    /// # Returns
    /// The updated [`JobOptions`](crate::job::JobOptions) instance with the specified queue options.
    pub fn with_queue_options(mut self, options: Q::Options) -> Self {
        self.queue_options = Some(options);
        self
    }

    /// Consumes the [`JobOptions`](crate::job::JobOptions) and returns the contained task, maximum retries, and queue options.
    pub fn into_parts(self) -> (T, usize, Option<Q::Options>) {
        (self.task, self.max_retries, self.queue_options)
    }
}

/// Options for configuring a streaming job in the job queue.
#[derive(Debug, Clone)]
pub struct JobStreamOptions<S, Q>
where
    Q: Queue,
{
    pub task: S,
    pub capacity: usize,
    pub queue_options: Option<Q::Options>,
}

impl<S, Q> JobStreamOptions<S, Q>
where
    Q: Queue,
{
    /// Creates a new [`JobStreamOptions`](crate::job::JobStreamOptions) with the specified task.
    pub fn new(task: S) -> Self {
        Self { task, capacity: 16, queue_options: None }
    }

    /// Sets the channel capacity.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    /// Sets the queue options for the streaming job.
    pub fn with_queue_options(mut self, options: Q::Options) -> Self {
        self.queue_options = Some(options);
        self
    }

    /// Consumes the options and returns the contained task, capacity, and queue options.
    pub fn into_parts(self) -> (S, usize, Option<Q::Options>) {
        (self.task, self.capacity, self.queue_options)
    }
}

/// Represents the status of a [`Job`](crate::job::Job) in the [`JobQueue`](crate::job::JobQueue).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

/// Represents a job that can be executed in the [`JobQueue`](crate::job::JobQueue).
pub struct Job<X>
where
    X: Execute,
{
    window: Arc<Window<X, X::Input, JobDelivery<X::Output>>>,
    status: JobStatus,
}

impl<T> Job<Single<T>>
where
    T: Task + 'static,
    T::Output: 'static,
{
    /// Creates a new [`Job`](crate::job::Job) instance with the specified task and maximum retries.
    ///
    /// # Arguments
    /// * `task` - The task to be executed by the job.
    /// * `max_retries` - The maximum number of retries for the job.
    ///
    /// # Returns
    /// A tuple containing the [`Job`](crate::job::Job) instance and a [`JobFuture`](crate::future::JobFuture) that
    /// can be awaited for the job's result.
    pub fn new(task: T, max_retries: usize) -> (Self, JobFuture<T::Output>) {
        let (future, setter) = JobFuture::new();

        (
            Self::from_window(Arc::new(Window::new(
                Arc::new(Single::new(task)),
                BatchPolicy::default(),
                (),
                JobDelivery::new(setter, max_retries),
            ))),
            future,
        )
    }
}

impl<X> Job<X>
where
    X: Execute,
{
    pub(crate) fn from_window(window: Arc<Window<X, X::Input, JobDelivery<X::Output>>>) -> Self {
        Self { window, status: JobStatus::Pending }
    }

    fn panic_message(panic: Box<dyn Any + Send>) -> String {
        if let Some(s) = panic.downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic.downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown panic".to_string()
        }
    }

    /// Returns the task associated with the job.
    pub fn status(&self) -> JobStatus {
        self.status
    }
}

#[async_trait]
impl<X> Executable for Job<X>
where
    X: Execute,
{
    async fn execute(&mut self) {
        self.status = JobStatus::Running;

        self.window.ready().await;

        let (mut inputs, mut deliveries) = self.window.seal();
        let mut status = JobStatus::Completed;

        while !inputs.is_empty() {
            let size = inputs.len();

            let results = match AssertUnwindSafe(self.window.executor().execute(&inputs))
                .catch_unwind()
                .await
            {
                Ok(Ok(results)) if results.len() == size => results,
                Ok(Ok(results)) => {
                    repeat_with(|| Err(Error::batch_size_mismatch(size, results.len())))
                        .take(size)
                        .collect()
                }
                Ok(Err(error)) => repeat_with(|| Err(error.clone()))
                    .take(size)
                    .collect(),
                Err(panic) => {
                    let message = Self::panic_message(panic);

                    repeat_with(|| Err(Error::task_panic(message.clone())))
                        .take(size)
                        .collect()
                }
            };

            let (mut retry_inputs, mut retry_deliveries) = (Vec::new(), Vec::new());

            for ((input, mut delivery), result) in inputs
                .into_iter()
                .zip(deliveries)
                .zip(results)
            {
                match result {
                    Err(Error::TaskExecution { .. }) if delivery.retry() => {
                        retry_inputs.push(input);
                        retry_deliveries.push(delivery);
                    }
                    result => {
                        if result.is_err() {
                            status = JobStatus::Failed;
                        }

                        delivery.deliver(result);
                    }
                }
            }

            inputs = retry_inputs;
            deliveries = retry_deliveries;
        }

        self.status = status;
    }

    fn status(&self) -> JobStatus {
        self.status
    }
}

/// A job that drives a [`StreamTask`](crate::task::StreamTask) and forwards its items to a
/// [`StreamHandle`](crate::future::StreamHandle).
pub struct StreamJob<X>
where
    X: ExecuteStream,
{
    window: Arc<Window<X, X::Input, StreamDelivery<X::Item>>>,
    status: JobStatus,
}

impl<S> StreamJob<Single<S>>
where
    S: StreamTask + 'static,
{
    /// Creates a new [`StreamJob`](crate::job::StreamJob) and the
    /// [`StreamHandle`](crate::future::StreamHandle) used to consume it.
    ///
    /// # Arguments
    /// * `task` - The streaming task to be driven by the job.
    /// * `capacity` - The item channel capacity.
    pub fn new(task: S, capacity: usize) -> (Self, JobStreamHandle<S::Item>) {
        let (items, item_setter) = JobStream::new(capacity);
        let (completion, completion_setter) = JobFuture::new();

        (
            Self::from_window(Arc::new(Window::new(
                Arc::new(Single::new(task)),
                BatchPolicy::default(),
                (),
                StreamDelivery::new(item_setter, completion_setter),
            ))),
            JobStreamHandle::new(items, completion),
        )
    }
}

impl<X> StreamJob<X>
where
    X: ExecuteStream,
{
    pub(crate) fn from_window(window: Arc<Window<X, X::Input, StreamDelivery<X::Item>>>) -> Self {
        Self { window, status: JobStatus::Pending }
    }

    fn panic_message(panic: Box<dyn Any + Send>) -> String {
        if let Some(s) = panic.downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic.downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown panic".to_string()
        }
    }

    /// Returns the current status of the streaming job.
    pub fn status(&self) -> JobStatus {
        self.status
    }
}

#[async_trait]
impl<X> Executable for StreamJob<X>
where
    X: ExecuteStream,
{
    async fn execute(&mut self) {
        self.status = JobStatus::Running;

        self.window.ready().await;

        let (inputs, mut deliveries) = self.window.seal();
        let mut stream = self.window.executor().execute(&inputs);

        loop {
            match AssertUnwindSafe(stream.next())
                .catch_unwind()
                .await
            {
                Ok(Some((index, item))) => {
                    if let Some(delivery) = deliveries.get_mut(index) {
                        delivery.send(item).await;
                    }

                    if deliveries
                        .iter()
                        .all(StreamDelivery::is_closed)
                    {
                        break;
                    }
                }
                Ok(None) => break,
                Err(panic) => {
                    drop(stream);

                    let message = Self::panic_message(panic);

                    deliveries
                        .iter_mut()
                        .for_each(|delivery| {
                            delivery.complete(Err(Error::task_panic(message.clone())))
                        });

                    self.status = JobStatus::Failed;

                    return;
                }
            }
        }

        drop(stream);

        deliveries
            .iter_mut()
            .for_each(|delivery| delivery.complete(Ok(())));

        self.status = JobStatus::Completed;
    }

    fn status(&self) -> JobStatus {
        self.status
    }
}

/// A job queue that manages executable items held in a queue of type `Q`.
///
/// The queue's item type is recovered as `Q::Item`, which must implement
/// [`Executable`](crate::executable::Executable). When that item is
/// [`Box<dyn Executable>`](crate::executable::Executable), the queue accepts both ordinary and
/// streaming jobs through the `enqueue_*` methods below.
#[derive(Debug)]
pub struct JobQueue<Q>
where
    Q: Queue<Item: Executable>,
{
    inner: Arc<Q>,
}

impl<Q> Clone for JobQueue<Q>
where
    Q: Queue<Item: Executable>,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<Q> JobQueue<Q>
where
    Q: Queue<Item: Executable>,
{
    /// Creates a new [`JobQueue`](crate::job::JobQueue) instance with the specified queue.
    ///
    /// # Arguments
    /// * `queue` - The queue to be used for managing jobs.
    ///
    /// # Returns
    /// A new [`JobQueue`](crate::job::JobQueue) instance with the specified queue.
    pub fn new(queue: Q) -> Self {
        Self { inner: Arc::new(queue) }
    }

    pub fn downgrade(&self) -> WeakJobQueue<Q> {
        WeakJobQueue { inner: Arc::downgrade(&self.inner) }
    }

    /// Enqueues an executable item with optional queue options.
    ///
    /// # Arguments
    /// * `job` - The executable item to be enqueued.
    /// * `options` - Optional queue options for the item.
    ///
    /// # Returns
    /// A `Result` indicating success or failure of the enqueue operation.
    pub async fn enqueue(&self, job: Q::Item, options: Option<Q::Options>) -> Result<(), Error> {
        self.inner
            .enqueue(job, options)
            .await
            .map_err(Error::queue)
    }

    /// Dequeues an executable item from the queue.
    ///
    /// # Returns
    /// A `Result` containing an `Option<Q::Item>`, which is `Some` if an item was successfully dequeued, or `None` if the queue is closed.
    pub async fn dequeue_job(&self) -> Result<Option<Q::Item>, Error> {
        self.inner
            .dequeue()
            .await
            .map_err(Error::queue)
    }

    /// Returns the number of executable items currently in the queue.
    ///
    /// # Returns
    /// A `Result` containing the number of jobs in the queue.
    pub async fn len(&self) -> usize {
        self.inner.len().await
    }

    /// Returns `true` if the queue currently contains no executable items.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Closes the [`JobQueue`](crate::job::JobQueue), preventing any further executable items from being enqueued.
    ///
    /// # Returns
    /// A `Result` indicating success or failure of the close operation.
    pub async fn close(&self) -> Result<(), Error> {
        self.inner
            .close()
            .await
            .map_err(Error::queue)
    }

    /// Get a [`JobQueueBuilder`] for creating a [`JobQueue`](crate::job::JobQueue) with the specified queue type.
    pub fn builder() -> JobQueueBuilder<Q> {
        JobQueueBuilder::new()
    }
}

impl<Q> JobQueue<Q>
where
    Q: Queue<Item = AnyExecutable>,
{
    /// Enqueues a [`Task`](crate::task::Task) and returns a
    /// [`JobFuture`](crate::future::JobFuture) for its result.
    pub async fn enqueue_job<T>(
        &self,
        options: JobOptions<T, Q>,
    ) -> Result<JobFuture<T::Output>, Error>
    where
        T: Task + 'static,
        T::Output: 'static,
    {
        let (task, max_retries, queue_options) = options.into_parts();
        let (job, future) = Job::new(task, max_retries);

        self.enqueue(AnyExecutable::new(job), queue_options)
            .await?;

        Ok(future)
    }

    /// Enqueues a closure as a one-off [`Task`](crate::task::Task) without a hand-written struct.
    pub async fn enqueue_fn<F, Fut, O, E>(&self, f: F) -> Result<JobFuture<O>, Error>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, E>> + Send + 'static,
        O: Send + Sync + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.enqueue_job(JobOptions::new(FnTask::new(f)))
            .await
    }

    /// Enqueues a [`StreamTask`](crate::task::StreamTask) and returns a
    /// [`StreamHandle`](crate::future::StreamHandle) for its items.
    pub async fn enqueue_stream<S>(
        &self,
        options: JobStreamOptions<S, Q>,
    ) -> Result<JobStreamHandle<S::Item>, Error>
    where
        S: StreamTask + 'static,
    {
        let (task, capacity, queue_options) = options.into_parts();
        let (job, handle) = StreamJob::new(task, capacity);

        self.enqueue(AnyExecutable::new(job), queue_options)
            .await?;

        Ok(handle)
    }

    /// Enqueues a closure returning a [`Stream`](futures::Stream) as a one-off streaming task.
    pub async fn enqueue_stream_fn<F, St, Item, E>(
        &self,
        f: F,
        capacity: usize,
    ) -> Result<JobStreamHandle<Item>, Error>
    where
        F: Fn() -> St + Send + Sync + 'static,
        St: Stream<Item = Result<Item, E>> + Send + 'static,
        Item: Send + Sync + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.enqueue_stream(JobStreamOptions::new(FnStreamTask::new(f)).with_capacity(capacity))
            .await
    }
}

impl<Q> JobQueue<Q>
where
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq>,
{
    pub fn batcher<B>(&self, task: B, policy: BatchPolicy) -> Batcher<B, Q>
    where
        B: BatchTask + 'static,
    {
        Batcher::new(self.clone(), task, policy)
    }

    pub fn stream_batcher<B>(&self, task: B, policy: BatchPolicy) -> StreamBatcher<B, Q>
    where
        B: BatchStreamTask + 'static,
    {
        StreamBatcher::new(self.clone(), task, policy)
    }
}

pub struct WeakJobQueue<Q>
where
    Q: Queue<Item: Executable>,
{
    inner: Weak<Q>,
}

impl<Q> Clone for WeakJobQueue<Q>
where
    Q: Queue<Item: Executable>,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<Q> WeakJobQueue<Q>
where
    Q: Queue<Item: Executable>,
{
    pub fn upgrade(&self) -> Option<JobQueue<Q>> {
        self.inner
            .upgrade()
            .map(|inner| JobQueue { inner })
    }
}

/// A builder for creating a [`JobQueue`](crate::job::JobQueue).
pub struct JobQueueBuilder<Q>
where
    Q: Queue<Item: Executable> + 'static,
{
    queue: Option<Q>,
}

impl<I> JobQueueBuilder<FifoQueue<I>>
where
    I: Executable + 'static,
{
    /// Configures this builder with a [`FifoQueue`](crate::queue::fifo::FifoQueue) of the
    /// specified maximum capacity.
    pub fn fifo(mut self, max_capacity: usize) -> Self {
        self.queue = Some(FifoQueue::new(max_capacity));
        self
    }
}

impl<I> JobQueueBuilder<LifoQueue<I>>
where
    I: Executable + 'static,
{
    /// Configures this builder with a [`LifoQueue`](crate::queue::lifo::LifoQueue) of the
    /// specified maximum capacity.
    pub fn lifo(mut self, max_capacity: usize) -> Self {
        self.queue = Some(LifoQueue::new(max_capacity));
        self
    }
}

impl<I> JobQueueBuilder<PriorityQueue<I>>
where
    I: Executable + 'static,
{
    /// Configures this builder with a [`PriorityQueue`](crate::queue::priority::PriorityQueue)
    /// of the specified maximum capacity.
    pub fn priority(mut self, max_capacity: usize) -> Self {
        self.queue = Some(PriorityQueue::new(max_capacity));
        self
    }
}

impl<Q> Default for JobQueueBuilder<Q>
where
    Q: Queue<Item: Executable> + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Q> JobQueueBuilder<Q>
where
    Q: Queue<Item: Executable> + 'static,
{
    /// Creates a new [`JobQueueBuilder`](crate::job::JobQueueBuilder) instance.
    pub fn new() -> Self {
        Self { queue: None }
    }

    /// Builds a [`JobQueue`](crate::job::JobQueue) using the configured queue.
    ///
    /// # Returns
    /// A [`JobQueue`](crate::job::JobQueue) instance with the configured queue.
    pub fn build(self) -> JobQueue<Q> {
        JobQueue::new(
            self.queue
                .expect("Queue must be set before building"),
        )
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use futures::{FutureExt, StreamExt, channel::mpsc, stream};

    use super::*;
    use crate::{
        builder::JobQueueSystemBuilder,
        task::{StreamTask, Task},
    };
    use std::{
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    #[derive(Debug, thiserror::Error)]
    #[error("{0}")]
    struct TestError(String);

    struct PanicTask {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Task for PanicTask {
        type Output = u32;
        type Error = TestError;

        async fn execute(&self) -> std::result::Result<Self::Output, Self::Error> {
            self.calls
                .fetch_add(1, Ordering::SeqCst);
            panic!("boom");
        }
    }

    struct DoubleTask {
        n: u32,
    }

    #[async_trait]
    impl Task for DoubleTask {
        type Output = u32;
        type Error = TestError;

        async fn execute(&self) -> std::result::Result<Self::Output, Self::Error> {
            Ok(self.n * 2)
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("boom: {0}")]
    struct RichError(u32);

    struct FailingTask;

    #[async_trait]
    impl Task for FailingTask {
        type Output = u32;
        type Error = RichError;

        async fn execute(&self) -> std::result::Result<Self::Output, Self::Error> {
            Err(RichError(7))
        }
    }

    struct CountStreamTask;

    impl StreamTask for CountStreamTask {
        type Item = u32;
        type Error = TestError;

        fn execute(&self) -> futures::stream::BoxStream<'_, Result<Self::Item, Self::Error>> {
            stream::iter(vec![Ok(1), Ok(2), Ok(3)]).boxed()
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("stream item failed: {0}")]
    struct StreamItemError(u32);

    struct ErrorStreamTask;

    impl StreamTask for ErrorStreamTask {
        type Item = u32;
        type Error = StreamItemError;

        fn execute(&self) -> futures::stream::BoxStream<'_, Result<Self::Item, Self::Error>> {
            stream::iter(vec![Ok(1), Err(StreamItemError(7))]).boxed()
        }
    }

    struct PanicStreamTask;

    impl StreamTask for PanicStreamTask {
        type Item = u32;
        type Error = TestError;

        fn execute(&self) -> futures::stream::BoxStream<'_, Result<Self::Item, Self::Error>> {
            stream::iter(vec![0, 1])
                .map(|step| {
                    if step == 0 {
                        Ok(1)
                    } else {
                        panic!("boom");
                    }
                })
                .boxed()
        }
    }

    // Produces `items` values, but only advances to item N after the consumer releases the
    // gate for it, so the stream cannot run ahead of what has been consumed.
    struct GatedStreamTask {
        releases: Mutex<Option<mpsc::Receiver<()>>>,
        produced: Arc<AtomicUsize>,
        items: usize,
    }

    impl StreamTask for GatedStreamTask {
        type Item = u32;
        type Error = TestError;

        fn execute(&self) -> futures::stream::BoxStream<'_, Result<Self::Item, Self::Error>> {
            let releases = self
                .releases
                .lock()
                .unwrap()
                .take()
                .expect("stream task executed more than once");
            let produced = self.produced.clone();
            let items = self.items;

            stream::unfold(
                (0usize, releases, produced),
                move |(index, mut releases, produced)| async move {
                    if index >= items {
                        return None;
                    }

                    releases.next().await?;
                    produced.fetch_add(1, Ordering::SeqCst);

                    Some((Ok::<u32, TestError>(index as u32), (index + 1, releases, produced)))
                },
            )
            .boxed()
        }
    }

    struct FlakyBatch {
        calls: Arc<Mutex<Vec<Vec<u32>>>>,
    }

    #[async_trait]
    impl BatchTask for FlakyBatch {
        type Input = u32;
        type Output = u32;
        type Error = TestError;

        async fn execute(
            &self,
            inputs: &[Self::Input],
        ) -> Result<Vec<Result<Self::Output, Self::Error>>, Self::Error> {
            let mut calls = self.calls.lock().unwrap();

            calls.push(inputs.to_vec());

            let round = calls.len() as u32;

            Ok(inputs
                .iter()
                .map(|&input| {
                    if round > input {
                        Ok(input)
                    } else {
                        Err(TestError(format!("round {round}")))
                    }
                })
                .collect())
        }
    }

    struct ShortBatch;

    #[async_trait]
    impl BatchTask for ShortBatch {
        type Input = u32;
        type Output = u32;
        type Error = TestError;

        async fn execute(
            &self,
            _inputs: &[Self::Input],
        ) -> Result<Vec<Result<Self::Output, Self::Error>>, Self::Error> {
            Ok(Vec::new())
        }
    }

    struct PanicBatch;

    #[async_trait]
    impl BatchTask for PanicBatch {
        type Input = u32;
        type Output = u32;
        type Error = TestError;

        async fn execute(
            &self,
            _inputs: &[Self::Input],
        ) -> Result<Vec<Result<Self::Output, Self::Error>>, Self::Error> {
            panic!("boom");
        }
    }

    struct TagStream;

    impl BatchStreamTask for TagStream {
        type Input = u32;
        type Item = u32;
        type Error = TestError;

        fn execute<'a>(
            &'a self,
            inputs: &'a [Self::Input],
        ) -> futures::stream::BoxStream<'a, (usize, Result<Self::Item, Self::Error>)> {
            stream::iter(
                inputs
                    .iter()
                    .enumerate()
                    .flat_map(|(index, input)| {
                        [(index, Ok(input * 10)), (index, Ok(input * 10 + 1))]
                    }),
            )
            .boxed()
        }
    }

    #[tokio::test]
    async fn panic_in_task_is_contained_and_fails_fast() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut job, future) = Job::new(PanicTask { calls: calls.clone() }, 3);

        job.execute().await;

        assert_eq!(job.status(), JobStatus::Failed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(future.result().await, Err(Error::TaskPanic(_))));
    }

    #[tokio::test]
    async fn healthy_task_still_completes() {
        let (mut job, future) = Job::new(DoubleTask { n: 21 }, 1);

        job.execute().await;

        assert_eq!(job.status(), JobStatus::Completed);
        assert_eq!(future.result().await.unwrap(), 42);
    }

    #[tokio::test]
    async fn task_error_identity_is_preserved_through_job_execute() {
        let (mut job, future) = Job::new(FailingTask, 1);

        job.execute().await;

        match future.result().await {
            Err(Error::TaskExecution { source, .. }) => {
                let original = source
                    .downcast_ref::<RichError>()
                    .expect("original error type should be recoverable");

                assert_eq!(original.0, 7);
            }
            other => panic!("expected Err(Error::TaskExecution {{ .. }}), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn stream_items_arrive_live_on_shared_erased_queue() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let ordinary_future = queue
            .enqueue_fn(|| async { Ok::<u32, TestError>(42) })
            .await
            .unwrap();
        let mut handle = queue
            .enqueue_stream(JobStreamOptions::new(CountStreamTask))
            .await
            .unwrap();

        assert_eq!(handle.next().await.unwrap().unwrap(), 1);
        assert_eq!(handle.next().await.unwrap().unwrap(), 2);
        assert_eq!(handle.next().await.unwrap().unwrap(), 3);
        assert!(handle.next().await.is_none());
        assert!(matches!(handle.result().await, Ok(())));
        assert_eq!(ordinary_future.result().await.unwrap(), 42);

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn stream_item_error_identity_is_preserved() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let mut handle = queue
            .enqueue_stream(JobStreamOptions::new(ErrorStreamTask))
            .await
            .unwrap();

        assert_eq!(handle.next().await.unwrap().unwrap(), 1);

        match handle.next().await.unwrap() {
            Err(Error::TaskExecution { source, .. }) => {
                let original = source
                    .downcast_ref::<StreamItemError>()
                    .expect("original error type should be recoverable");

                assert_eq!(original.0, 7);
            }
            other => panic!("expected Err(Error::TaskExecution {{ .. }}), got {other:?}"),
        }

        assert!(handle.next().await.is_none());
        assert!(matches!(handle.result().await, Ok(())));

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn stream_panic_surfaces_on_result_after_sent_items() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let mut handle = queue
            .enqueue_stream(JobStreamOptions::new(PanicStreamTask))
            .await
            .unwrap();

        assert_eq!(handle.next().await.unwrap().unwrap(), 1);
        assert!(handle.next().await.is_none());
        assert!(matches!(handle.result().await, Err(Error::TaskPanic(_))));

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn stream_item_is_not_produced_until_the_previous_one_is_consumed() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let produced = Arc::new(AtomicUsize::new(0));
        let (mut release_tx, release_rx) = mpsc::channel::<()>(8);

        let mut handle = queue
            .enqueue_stream(JobStreamOptions::new(GatedStreamTask {
                releases: Mutex::new(Some(release_rx)),
                produced: produced.clone(),
                items: 3,
            }))
            .await
            .unwrap();

        for index in 0..3 {
            // The worker has produced exactly the items consumed so far, and no more.
            assert_eq!(produced.load(Ordering::SeqCst), index);

            // With the gate still closed, no next item can be ready to consume.
            assert!(handle.next().now_or_never().is_none());

            release_tx.try_send(()).unwrap();

            assert_eq!(handle.next().await.unwrap().unwrap(), index as u32);
            assert_eq!(produced.load(Ordering::SeqCst), index + 1);
        }

        assert!(handle.next().await.is_none());
        assert!(matches!(handle.result().await, Ok(())));

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }
    #[tokio::test]
    async fn failed_batch_members_retry_alone_within_their_own_budget() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let calls = Arc::new(Mutex::new(Vec::new()));
        let batcher = queue.batcher(
            FlakyBatch { calls: calls.clone() },
            BatchPolicy {
                max_size: 3,
                max_wait: Duration::from_secs(60),
            },
        );
        let zero = batcher
            .enqueue(JobOptions::new(0).with_max_retries(1))
            .await
            .unwrap();
        let one = batcher
            .enqueue(JobOptions::new(1).with_max_retries(2))
            .await
            .unwrap();
        let five = batcher
            .enqueue(JobOptions::new(5).with_max_retries(3))
            .await
            .unwrap();

        assert_eq!(zero.result().await.unwrap(), 0);
        assert_eq!(one.result().await.unwrap(), 1);
        assert!(matches!(five.result().await, Err(Error::TaskExecution { .. })));
        assert_eq!(*calls.lock().unwrap(), vec![vec![0, 1, 5], vec![1, 5], vec![5]]);

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn batch_size_mismatch_reaches_every_member() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let batcher = queue.batcher(
            ShortBatch,
            BatchPolicy {
                max_size: 2,
                max_wait: Duration::from_secs(60),
            },
        );
        let mut futures = Vec::new();

        for input in [1, 2] {
            futures.push(
                batcher
                    .enqueue(JobOptions::new(input))
                    .await
                    .unwrap(),
            );
        }

        for future in futures {
            assert!(matches!(
                future.result().await,
                Err(Error::BatchSizeMismatch { expected: 2, actual: 0 })
            ));
        }

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn batch_panic_reaches_every_member() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let batcher = queue.batcher(
            PanicBatch,
            BatchPolicy {
                max_size: 2,
                max_wait: Duration::from_secs(60),
            },
        );
        let mut futures = Vec::new();

        for input in [1, 2] {
            futures.push(
                batcher
                    .enqueue(JobOptions::new(input))
                    .await
                    .unwrap(),
            );
        }

        for future in futures {
            assert!(matches!(future.result().await, Err(Error::TaskPanic(_))));
        }

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn batched_stream_routes_items_to_their_member() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let batcher = queue.stream_batcher(
            TagStream,
            BatchPolicy {
                max_size: 2,
                max_wait: Duration::from_secs(60),
            },
        );
        let mut handles = Vec::new();

        for input in [1, 2] {
            handles.push(
                batcher
                    .enqueue(JobStreamOptions::new(input))
                    .await
                    .unwrap(),
            );
        }

        for (input, mut handle) in [1, 2].into_iter().zip(handles) {
            assert_eq!(handle.next().await.unwrap().unwrap(), input * 10);
            assert_eq!(handle.next().await.unwrap().unwrap(), input * 10 + 1);
            assert!(handle.next().await.is_none());
            assert!(matches!(handle.result().await, Ok(())));
        }

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }
}
