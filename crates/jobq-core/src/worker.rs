use async_trait::async_trait;
use event_listener::Event;
use futures::{
    future::{FutureExt, join_all},
    select,
};
use futures_timeout::TimeoutExt;
use std::{marker::PhantomData, sync::Arc, time::Duration};

use crate::{
    job::{Job, JobQueue},
    queue::traits::Queue,
    task::Task,
};

/// Trait for defining a worker that processes jobs from a job queue.
#[async_trait]
pub trait Worker<T, Q>: Send + Sync
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
{
    type Options: Default + Clone + Send + Sync + 'static;

    /// Returns the unique identifier of the worker.
    fn id(&self) -> usize;

    /// Creates a new worker instance.
    ///
    /// # Arguments
    /// * `id` - The unique identifier for the worker.
    /// * `queue` - The job queue from which the worker will dequeue jobs.
    /// * `options` - Additional options for configuring the worker.
    ///
    /// # Returns
    /// A new worker instance.
    fn create(id: usize, queue: Arc<JobQueue<T, Q>>, options: Self::Options) -> Self
    where
        Self: Sized;

    /// Runs the worker, processing jobs from the queue until shutdown is requested.
    async fn run(&self);

    /// Shuts down the worker, stopping it from processing any more jobs.
    async fn shutdown(&self);
}

/// A worker that processes jobs one at a time from a job queue.
#[derive(Debug)]
pub struct JobWorker<T, Q>
where
    T: Task,
    Q: Queue<Item = Job<T>>,
{
    id: usize,
    queue: Arc<JobQueue<T, Q>>,
    shutdown: Arc<Event>,
}

#[async_trait]
impl<T, Q> Worker<T, Q> for JobWorker<T, Q>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
{
    type Options = ();

    fn id(&self) -> usize {
        self.id
    }

    fn create(id: usize, queue: Arc<JobQueue<T, Q>>, _options: Self::Options) -> Self {
        Self {
            id,
            queue,
            shutdown: Arc::new(Event::new()),
        }
    }

    async fn run(&self) {
        loop {
            select! {
                _ = self.shutdown.listen().fuse() => break,
                job = self.queue.dequeue_job().fuse() => {
                    match job {
                        Ok(Some(mut j)) => j.execute().await,
                        _ => continue,
                    }
                }
            }
        }
    }

    async fn shutdown(&self) {
        self.shutdown.notify(usize::MAX);
    }
}

/// Options for configuring a [`BatchJobWorker`](crate::worker::BatchJobWorker).
#[derive(Debug, Clone)]
pub struct BatchJobWorkerOptions {
    /// Maximum number of jobs to process in a single batch.
    pub batch_size: usize,
    /// Timeout for collecting jobs in a batch.
    /// If the timeout is reached, the batch will be processed even if it is not full.
    pub batch_timeout: Duration,
}

impl Default for BatchJobWorkerOptions {
    fn default() -> Self {
        Self {
            batch_size: 4,
            batch_timeout: Duration::from_millis(50),
        }
    }
}

/// A worker that processes jobs in batches from a [`JobQueue`](crate::job::JobQueue).
#[derive(Debug)]
pub struct BatchJobWorker<T, Q>
where
    T: Task,
    Q: Queue<Item = Job<T>>,
{
    id: usize,
    queue: Arc<JobQueue<T, Q>>,
    options: BatchJobWorkerOptions,
    shutdown: Arc<Event>,
}

impl<T, Q> BatchJobWorker<T, Q>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
{
    /// Creates a new [`BatchJobWorker`](crate::worker::BatchJobWorker) instance.
    ///
    /// # Arguments
    /// * `id` - The unique identifier for the worker.
    /// * `queue` - The job queue from which the worker will dequeue jobs.
    /// * `options` - Options for configuring the batch processing behavior.
    pub fn new(id: usize, queue: Arc<JobQueue<T, Q>>, options: BatchJobWorkerOptions) -> Self {
        Self {
            id,
            queue,
            options,
            shutdown: Arc::new(Event::new()),
        }
    }

    /// Collects a batch of jobs from the queue.
    async fn collect_batch(&self, batch_size: usize, batch_timeout: Duration) -> Vec<Job<T>> {
        let mut jobs = Vec::with_capacity(batch_size);

        while jobs.len() < batch_size {
            match self
                .queue
                .dequeue_job()
                .timeout(batch_timeout)
                .await
                .map_err(|_| ())
                .and_then(|res| res.map_err(|_| ()))
            {
                Ok(Some(job)) => jobs.push(job),
                _ => break,
            }
        }

        jobs
    }
}

#[async_trait]
impl<T, Q> Worker<T, Q> for BatchJobWorker<T, Q>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
{
    type Options = BatchJobWorkerOptions;

    fn id(&self) -> usize {
        self.id
    }

    fn create(id: usize, queue: Arc<JobQueue<T, Q>>, options: Self::Options) -> Self {
        Self::new(id, queue, options)
    }

    async fn run(&self) {
        loop {
            select! {
                _ = self.shutdown.listen().fuse() => break,
                jobs = self.collect_batch(
                    self.options.batch_size,
                    self.options.batch_timeout
                ).fuse() => {
                    if !jobs.is_empty() {
                        join_all(
                            jobs
                                .into_iter()
                                .map(|mut job| async move { job.execute().await })
                        ).await;
                    }
                }
            }
        }
    }

    async fn shutdown(&self) {
        self.shutdown.notify(usize::MAX);
    }
}

/// A worker pool that manages multiple workers processing jobs from a job queue.
#[derive(Debug)]
pub struct WorkerPool<T, Q, W>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
    W: Worker<T, Q> + 'static,
{
    workers: Vec<Arc<W>>,
    _marker: PhantomData<(T, Q)>,
}

impl<T, Q, W> WorkerPool<T, Q, W>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
    W: Worker<T, Q> + 'static,
{
    /// Creates a new [`WorkerPool`](crate::worker::WorkerPool) instance with the specified workers.
    ///
    /// # Arguments
    /// * `workers` - A vector of worker instances to be managed by the pool.
    ///
    /// # Returns
    /// A new [`WorkerPool`](crate::worker::WorkerPool) instance.
    pub fn new(workers: Vec<Arc<W>>) -> Self {
        Self { workers, _marker: PhantomData }
    }

    /// Executes all workers in the pool concurrently.
    ///
    /// This method will run all workers until they complete or are shut down.
    ///
    /// # Returns
    /// A future that resolves when all workers have completed their execution.
    pub async fn run(&self) {
        join_all(
            self.workers
                .iter()
                .map(|worker| worker.run())
                .collect::<Vec<_>>(),
        )
        .await;
    }

    /// Shuts down all workers in the pool.
    ///
    /// This method will signal all workers to stop processing jobs.
    ///
    /// # Returns
    /// A future that resolves when all workers have been shut down.
    pub async fn shutdown(&self) {
        join_all(
            self.workers
                .iter()
                .map(|worker| worker.shutdown())
                .collect::<Vec<_>>(),
        )
        .await;
    }

    /// Returns a reference to the workers in the pool.
    ///
    /// # Returns
    /// A slice of `Arc<W>` containing the workers in the pool.
    pub fn workers(&self) -> &[Arc<W>] {
        &self.workers
    }

    /// Returns the number of workers in the pool.
    ///
    /// # Returns
    /// The number of workers as a `usize`.
    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }

    /// Gets a worker by its unique identifier.
    ///
    /// # Arguments
    /// * `id` - The unique identifier of the worker to retrieve.
    ///
    /// # Returns
    /// An `Option<Arc<W>>` containing the worker if found, or `None` if no worker with the given ID exists.
    pub fn get_worker(&self, id: usize) -> Option<Arc<W>> {
        self.workers
            .iter()
            .find(|worker| worker.id() == id)
            .map(Arc::clone)
    }

    /// Get a [`WorkerPoolBuilder`](crate::worker::WorkerPoolBuilder) for creating a [`WorkerPool`](crate::worker::WorkerPool)
    /// with the specified task and queue types.
    pub fn builder() -> WorkerPoolBuilder<T, Q, W> {
        WorkerPoolBuilder::new()
    }
}

/// A builder for creating a [`WorkerPool`](crate::worker::WorkerPool).
#[derive(Debug)]
pub struct WorkerPoolBuilder<T, Q, W>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
    W: Worker<T, Q> + 'static,
{
    num_workers: usize,
    worker_options: Option<W::Options>,
    queue: Option<Arc<JobQueue<T, Q>>>,
    _marker: PhantomData<(T, Q, W)>,
}

impl<T, Q, W> Default for WorkerPoolBuilder<T, Q, W>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
    W: Worker<T, Q> + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, Q, W> WorkerPoolBuilder<T, Q, W>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
    W: Worker<T, Q> + 'static,
{
    /// Creates a new [`WorkerPoolBuilder`](crate::worker::WorkerPoolBuilder) instance.
    pub fn new() -> Self {
        Self {
            num_workers: 1,
            worker_options: None,
            queue: None,
            _marker: PhantomData,
        }
    }

    /// Sets the number of workers in the pool.
    ///
    /// # Arguments
    /// * `num_workers` - The number of workers to create in the pool.
    ///
    /// # Returns
    /// A mutable reference to the builder instance for method chaining.
    pub fn with_num_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers;
        self
    }

    /// Sets the worker options for the worker pool.
    ///
    /// # Arguments
    /// * `options` - The options to configure the workers in the pool.
    ///
    /// # Returns
    /// A mutable reference to the builder instance for method chaining.
    pub fn with_options(mut self, options: W::Options) -> Self {
        self.worker_options = Some(options);
        self
    }

    /// Sets the [`JobQueue`](crate::job::JobQueue) for the worker pool.
    ///
    /// # Arguments
    /// * `queue` - The job queue to be used by the workers in the pool
    ///
    /// # Returns
    /// A mutable reference to the builder instance for method chaining.
    pub fn with_queue(mut self, queue: Arc<JobQueue<T, Q>>) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Builds the [`WorkerPool`](crate::worker::WorkerPool) with the specified workers.
    ///
    /// # Returns
    /// A new [`WorkerPool`](crate::worker::WorkerPool) instance wrapped in `Arc` containing the configured workers.
    pub fn build(self) -> Arc<WorkerPool<T, Q, W>> {
        Arc::new(WorkerPool {
            workers: (0..self.num_workers)
                .map(|id| {
                    Arc::new(W::create(
                        id,
                        self.queue
                            .clone()
                            .expect("Queue must be set before building"),
                        self.worker_options
                            .clone()
                            .unwrap_or_default(),
                    ))
                })
                .collect(),
            _marker: PhantomData,
        })
    }
}
