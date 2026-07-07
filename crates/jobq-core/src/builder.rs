use std::sync::Arc;

use crate::{
    executable::Executable,
    job::{JobQueue, JobQueueBuilder},
    queue::{fifo::FifoQueue, lifo::LifoQueue, priority::PriorityQueue, traits::Queue},
    worker::{BatchJobWorker, JobWorker, Worker, WorkerPool, WorkerPoolBuilder},
};

/// A built job queue system: the [`JobQueue`](crate::job::JobQueue) paired with its [`WorkerPool`](crate::worker::WorkerPool).
pub type QueueSystem<Q, W> = (Arc<JobQueue<Q>>, Arc<WorkerPool<Q, W>>);

/// A builder for creating a complete job queue system.
pub struct QueueSystemBuilder<Q, W>
where
    Q: Queue<Item: Executable> + 'static,
    W: Worker<Q> + 'static,
{
    job_queue_builder: JobQueueBuilder<Q>,
    worker_pool_builder: WorkerPoolBuilder<Q, W>,
}

impl<I, W> QueueSystemBuilder<FifoQueue<I>, W>
where
    I: Executable + 'static,
    W: Worker<FifoQueue<I>> + 'static,
{
    /// Creates a new [`QueueSystemBuilder`](crate::builder::QueueSystemBuilder) instance with a FIFO queue.
    pub fn fifo(max_capacity: usize) -> Self {
        Self::new(
            JobQueueBuilder::<FifoQueue<I>>::new().fifo(max_capacity),
            WorkerPoolBuilder::new(),
        )
    }
}

impl<I, W> QueueSystemBuilder<LifoQueue<I>, W>
where
    I: Executable + 'static,
    W: Worker<LifoQueue<I>> + 'static,
{
    /// Creates a new [`QueueSystemBuilder`](crate::builder::QueueSystemBuilder) instance with a LIFO queue.
    pub fn lifo(max_capacity: usize) -> Self {
        Self::new(
            JobQueueBuilder::<LifoQueue<I>>::new().lifo(max_capacity),
            WorkerPoolBuilder::new(),
        )
    }
}

impl<I, W> QueueSystemBuilder<PriorityQueue<I>, W>
where
    I: Executable + 'static,
    W: Worker<PriorityQueue<I>> + 'static,
{
    /// Creates a new [`QueueSystemBuilder`](crate::builder::QueueSystemBuilder) instance with a priority queue.
    pub fn priority(max_capacity: usize) -> Self {
        Self::new(
            JobQueueBuilder::<PriorityQueue<I>>::new().priority(max_capacity),
            WorkerPoolBuilder::new(),
        )
    }
}

impl<Q, W> QueueSystemBuilder<Q, W>
where
    Q: Queue<Item: Executable> + 'static,
    W: Worker<Q> + 'static,
{
    /// Creates a new [`QueueSystemBuilder`](crate::builder::QueueSystemBuilder) instance.
    pub fn new(
        job_queue_builder: JobQueueBuilder<Q>,
        worker_pool_builder: WorkerPoolBuilder<Q, W>,
    ) -> Self {
        Self { job_queue_builder, worker_pool_builder }
    }

    /// Sets the number of workers in the worker pool.
    ///
    /// # Arguments
    /// * `num_workers` - The number of workers to create in the pool.
    ///
    /// # Returns
    /// A mutable reference to the builder instance for method chaining.
    pub fn with_num_workers(mut self, num_workers: usize) -> Self {
        self.worker_pool_builder = self
            .worker_pool_builder
            .with_num_workers(num_workers);

        self
    }

    /// Sets the worker options for the worker pool.
    ///
    /// # Arguments
    /// * `options` - The options to configure the workers in the pool.
    ///
    /// # Returns
    /// A mutable reference to the builder instance for method chaining.
    pub fn with_worker_options(mut self, options: W::Options) -> Self {
        self.worker_pool_builder = self
            .worker_pool_builder
            .with_options(options);

        self
    }

    /// Builds the complete job queue system.
    ///
    /// # Returns
    /// A tuple containing the job queue and the worker pool.
    pub fn build(self) -> QueueSystem<Q, W> {
        let job_queue = self.job_queue_builder.build();
        let worker_pool = self
            .worker_pool_builder
            .with_queue(job_queue.clone())
            .build();

        (job_queue, worker_pool)
    }
}

/// A type alias for a job queue system builder using the [`JobWorker`](crate::worker::JobWorker)
/// as the worker type.
pub type JobQueueSystemBuilder<Q> = QueueSystemBuilder<Q, JobWorker<Q>>;

/// A type alias for a job queue system builder using the [`BatchJobWorker`](crate::worker::BatchJobWorker)
/// as the worker type.
pub type BatchJobQueueSystemBuilder<Q> = QueueSystemBuilder<Q, BatchJobWorker<Q>>;
