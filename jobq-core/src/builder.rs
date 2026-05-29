use std::sync::Arc;

use crate::{
    job::{Job, JobQueue, JobQueueBuilder},
    worker::{Worker, WorkerPool, WorkerPoolBuilder, JobWorker, BatchJobWorker},
    task::Task,
    queue::{traits::Queue, fifo::FifoQueue, lifo::LifoQueue, priority::PriorityQueue},
};


/// A built job queue system: the [`JobQueue`](crate::job::JobQueue) paired with its [`WorkerPool`](crate::worker::WorkerPool).
pub type QueueSystem<T, Q, W> = (Arc<JobQueue<T, Q>>, Arc<WorkerPool<T, Q, W>>);


/// A builder for creating a complete job queue system.
pub struct QueueSystemBuilder<T, Q, W>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
    W: Worker<T, Q> + 'static,
{
    job_queue_builder: JobQueueBuilder<T, Q>,
    worker_pool_builder: WorkerPoolBuilder<T, Q, W>,
}

impl<T, W> QueueSystemBuilder<T, FifoQueue<Job<T>>, W>
where
    T: Task + 'static,
    W: Worker<T, FifoQueue<Job<T>>> + 'static,
{
    /// Creates a new [`QueueSystemBuilder`](crate::builder::QueueSystemBuilder) instance with a FIFO queue.
    pub fn fifo(max_capacity: usize) -> Self {
        Self::new(
            JobQueueBuilder::new().fifo(max_capacity),
            WorkerPoolBuilder::new()
        )
    }
}

impl<T, W> QueueSystemBuilder<T, LifoQueue<Job<T>>, W>
where
    T: Task + 'static,
    W: Worker<T, LifoQueue<Job<T>>> + 'static,
{
    /// Creates a new [`QueueSystemBuilder`](crate::builder::QueueSystemBuilder) instance with a LIFO queue.
    pub fn lifo(max_capacity: usize) -> Self {
        Self::new(
            JobQueueBuilder::new().lifo(max_capacity),
            WorkerPoolBuilder::new()
        )
    }
}

impl<T, W> QueueSystemBuilder<T, PriorityQueue<Job<T>>, W>
where
    T: Task + 'static,
    W: Worker<T, PriorityQueue<Job<T>>> + 'static,
{
    /// Creates a new [`QueueSystemBuilder`](crate::builder::QueueSystemBuilder) instance with a priority queue.
    pub fn priority(max_capacity: usize) -> Self {
        Self::new(
            JobQueueBuilder::new().priority(max_capacity),
            WorkerPoolBuilder::new()
        )
    }
}

impl<T, Q, W> QueueSystemBuilder<T, Q, W>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
    W: Worker<T, Q> + 'static,
{
    /// Creates a new [`QueueSystemBuilder`](crate::builder::QueueSystemBuilder) instance.
    pub fn new(job_queue_builder: JobQueueBuilder<T, Q>, worker_pool_builder: WorkerPoolBuilder<T, Q, W>) -> Self {
        Self {
            job_queue_builder,
            worker_pool_builder,
        }
    }

    /// Sets the number of workers in the worker pool.
    /// 
    /// # Arguments
    /// * `num_workers` - The number of workers to create in the pool.
    /// 
    /// # Returns
    /// A mutable reference to the builder instance for method chaining.
    pub fn with_num_workers(mut self, num_workers: usize) -> Self {
        self.worker_pool_builder = self.worker_pool_builder.with_num_workers(num_workers);
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
        self.worker_pool_builder = self.worker_pool_builder.with_options(options);
        self
    }

    /// Builds the complete job queue system.
    /// 
    /// # Returns
    /// A tuple containing the job queue and the worker pool.
    pub fn build(self) -> QueueSystem<T, Q, W> {
        let job_queue = self.job_queue_builder
            .build();
        let worker_pool = self.worker_pool_builder
            .with_queue(job_queue.clone())
            .build();
        (job_queue, worker_pool)
    }
}


/// A type alias for a job queue system builder using the [`JobWorker`](crate::worker::JobWorker)
/// as the worker type.
pub type JobQueueSystemBuilder<T, Q> = QueueSystemBuilder<T, Q, JobWorker<T, Q>>;

/// A type alias for a job queue system builder using the [`BatchJobWorker`](crate::worker::BatchJobWorker)
/// as the worker type.
pub type BatchJobQueueSystemBuilder<T, Q> = QueueSystemBuilder<T, Q, BatchJobWorker<T, Q>>;
