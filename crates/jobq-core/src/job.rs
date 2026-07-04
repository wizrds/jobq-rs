use std::{sync::Arc, marker::PhantomData, panic::AssertUnwindSafe, any::Any};
use futures::future::FutureExt;

use crate::{
    task::Task,
    future::{JobFuture, JobFutureSetter},
    error::{Error, Result},
    queue::{traits::Queue, fifo::FifoQueue, lifo::LifoQueue, priority::PriorityQueue},
};


/// Options for configuring a job in the job queue.
#[derive(Debug, Clone)]
pub struct JobOptions<T, Q>
where 
    T: Task,
    Q: Queue<Item = Job<T>>,
{
    pub task: T,
    pub max_retries: usize,
    pub queue_options: Option<Q::Options>,
}

impl<T, Q> JobOptions<T, Q>
where 
    T: Task,
    Q: Queue<Item = Job<T>>,
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


/// Represents the status of a [`Job`](crate::job::Job) in the [`JobQueue`](crate::job::JobQueue).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}


/// Represents a job that can be executed in the [`JobQueue`](crate::job::JobQueue).
#[derive(Debug)]
pub struct Job<T>
where 
    T: Task,
{
    task: T,
    max_retries: usize,
    retries: usize,
    status: JobStatus,
    future_setter: JobFutureSetter<T::Output>,
}


impl<T> Job<T>
where 
    T: Task,
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

        (Self {
            task,
            max_retries,
            retries: 0,
            status: JobStatus::Pending,
            future_setter: setter,
        }, future)
    }

    /// Returns the task associated with the job.
    pub fn status(&self) -> JobStatus {
        self.status
    }

    /// Executes the job's task and updates its status based on the result.
    pub async fn execute(&mut self) {
        self.status = JobStatus::Running;
        let mut result = None;

        while self.retries < self.max_retries && result.is_none() {
            self.retries += 1;

            match AssertUnwindSafe(self.task.execute())
                .catch_unwind()
                .await
            {
                Ok(Ok(output)) => {
                    result = Some(Ok(output));
                    self.status = JobStatus::Completed;
                },
                Ok(Err(e)) if self.retries >= self.max_retries => {
                    result = Some(Err(Error::TaskExecution(e.to_string())));
                    self.status = JobStatus::Failed;
                },
                Ok(Err(_)) => continue,
                // A panic fails the job immediately, regardless of remaining retries.
                Err(panic) => {
                    result = Some(Err(Error::TaskPanic(panic_message(panic))));
                    self.status = JobStatus::Failed;
                },
            }
        }

        if let Some(res) = result {
            self.future_setter
                .set_result(res);
        }
    }
}


/// Extracts a human-readable message from a caught panic payload.
fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}


/// Represents a job queue that manages jobs with the task of type `T` using a queue of type `Q`.
#[derive(Debug)]
pub struct JobQueue<T, Q>
where
    T: Task,
    Q: Queue<Item = Job<T>>,
{
    inner: Q,
}

impl<T, Q> JobQueue<T, Q>
where
    T: Task,
    Q: Queue<Item = Job<T>>,
{
    /// Creates a new [`JobQueue`](crate::job::JobQueue) instance with the specified queue.
    /// 
    /// # Arguments
    /// * `queue` - The queue to be used for managing jobs.
    /// 
    /// # Returns
    /// A new [`JobQueue`](crate::job::JobQueue) instance with the specified queue.
    pub fn new(queue: Q) -> Self {
        Self { inner: queue }
    }

    /// Enqueues a [`Job`](crate::job::Job) with the specified options and returns a [`JobFuture`](crate::future::JobFuture)
    /// that can be awaited for the job's result.
    /// 
    /// # Arguments
    /// * `options` - The options for the job, including the task, maximum retries
    /// 
    /// # Returns
    /// A `Result` containing a [`JobFuture`](crate::future::JobFuture) that can be awaited for the
    /// [`Job`](crate::job::Job)'s result, or an error if the job could not be enqueued.
    pub async fn enqueue_job(&self, options: JobOptions<T, Q>) -> Result<JobFuture<T::Output>> {
        let (task, max_retries, queue_options) = options.into_parts();
        let (job, future) = Job::new(task, max_retries);
        self.enqueue(job, queue_options).await?;
        Ok(future)
    }

    /// Enqueues a [`Job`](crate::job::Job) with optional queue options.
    /// 
    /// # Arguments
    /// * `job` - The job to be enqueued.
    /// * `options` - Optional queue options for the job.
    /// 
    /// # Returns
    /// A `Result` indicating success or failure of the enqueue operation.
    pub async fn enqueue(&self, job: Job<T>, options: Option<Q::Options>) -> Result<()> {
        self.inner.enqueue(job, options)
            .await
            .map_err(Error::from)
    }

    /// Dequeues a [`Job`](crate::job::Job) from the queue.
    /// 
    /// # Returns
    /// A `Result` containing an `Option<Job<T>>`, which is `Some` if a job was successfully dequeued, or `None` if the queue is closed.
    pub async fn dequeue_job(&self) -> Result<Option<Job<T>>> {
        self.inner.dequeue()
            .await
            .map_err(Error::from)
    }

    /// Returns the number of [`Job`](crate::job::Job)s currently in the queue.
    /// 
    /// # Returns
    /// A `Result` containing the number of jobs in the queue.
    pub async fn len(&self) -> usize {
        self.inner.len()
            .await
    }

    /// Returns `true` if the queue currently contains no [`Job`](crate::job::Job)s.
    pub async fn is_empty(&self) -> bool {
        self.len()
            .await == 0
    }

    /// Closes the [`JobQueue`](crate::job::JobQueue), preventing any further [`Job`](crate::job::Job)s from being enqueued.
    /// 
    /// # Returns
    /// A `Result` indicating success or failure of the close operation.
    pub async fn close(&self) -> Result<()> {
        self.inner.close()
            .await
            .map_err(Error::from)
    }

    /// Get a [`JobQueueBuilder`] for creating a [`JobQueue`](crate::job::JobQueue) with the specified task and queue types.
    pub fn builder() -> JobQueueBuilder<T, Q> {
        JobQueueBuilder::new()
    }
}


/// A builder for creating a [`JobQueue`](crate::job::JobQueue).
pub struct JobQueueBuilder<T, Q>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
{
    queue: Option<Q>,
    __marker: PhantomData<T>,
}

impl<T> JobQueueBuilder<T, FifoQueue<Job<T>>>
where
    T: Task,
{
    /// Creates a [`JobQueueBuilder`](crate::job::JobQueueBuilder) instance with a [`FIFOQueue`](crate::queue::fifo::FifoQueue) 
    /// and the specified maximum capacity.
    /// 
    /// # Arguments
    /// * `max_capacity` - The maximum capacity of the FIFO queue.
    /// 
    /// # Returns
    /// A [`JobQueueBuilder`](crate::job::JobQueueBuilder) instance.
    pub fn fifo(mut self, max_capacity: usize) -> Self {
        self.queue = Some(FifoQueue::new(max_capacity));
        self
    }
}

impl<T> JobQueueBuilder<T, LifoQueue<Job<T>>>
where
    T: Task,
{
    /// Creates a [`JobQueueBuilder`](crate::job::JobQueueBuilder) instance with a [`LIFOQueue`](crate::queue::lifo::LifoQueue)
    /// and the specified maximum capacity.
    /// 
    /// # Arguments
    /// * `max_capacity` - The maximum capacity of the LIFO queue.
    /// 
    /// # Returns
    /// A [`JobQueueBuilder`](crate::job::JobQueueBuilder) instance.
    pub fn lifo(mut self, max_capacity: usize) -> Self {
        self.queue = Some(LifoQueue::new(max_capacity));
        self
    }
}

impl<T> JobQueueBuilder<T, PriorityQueue<Job<T>>>
where
    T: Task,
{
    /// Creates a [`JobQueueBuilder`](crate::job::JobQueueBuilder) instance with a [`PriorityQueue`](crate::queue::priority::PriorityQueue)
    /// and the specified maximum capacity.
    /// 
    /// # Arguments
    /// * `max_capacity` - The maximum capacity of the priority queue.
    /// 
    /// # Returns
    /// A [`JobQueueBuilder`](crate::job::JobQueueBuilder) instance.
    pub fn priority(mut self, max_capacity: usize) -> Self {
        self.queue = Some(PriorityQueue::new(max_capacity));
        self
    }
}

impl<T, Q> Default for JobQueueBuilder<T, Q>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, Q> JobQueueBuilder<T, Q>
where
    T: Task + 'static,
    Q: Queue<Item = Job<T>> + 'static,
{
    /// Creates a new [`JobQueueBuilder`](crate::job::JobQueueBuilder) instance.
    pub fn new() -> Self {
        Self {
            queue: None,
            __marker: PhantomData,
        }
    }

    /// Builds a [`JobQueue`](crate::job::JobQueue) using the configured queue.
    /// 
    /// # Returns
    /// A [`JobQueue`](crate::job::JobQueue) instance with the configured queue.
    pub fn build(self) -> Arc<JobQueue<T, Q>> {
        Arc::new(JobQueue::new(self.queue.expect("Queue must be set before building")))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use crate::task::Task;

    struct PanicTask {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Task for PanicTask {
        type Output = u32;
        type Error = String;

        async fn execute(&self) -> std::result::Result<Self::Output, Self::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("boom");
        }
    }

    struct DoubleTask {
        n: u32,
    }

    #[async_trait::async_trait]
    impl Task for DoubleTask {
        type Output = u32;
        type Error = String;

        async fn execute(&self) -> std::result::Result<Self::Output, Self::Error> {
            Ok(self.n * 2)
        }
    }

    #[tokio::test]
    async fn panic_in_task_is_contained_and_fails_fast() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut job, future) = Job::new(
            PanicTask { calls: calls.clone() },
            3,
        );

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
}
