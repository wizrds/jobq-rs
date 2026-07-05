//! A lightweight, in-memory job queue for asynchronous task processing within a single
//! process. It allows for simple job scheduling and processing, suitable for applications
//! that require asynchronous task handling without the need for distributed messaging
//! systems.
//!
//! # Features
//!
//! - **Simple API**: easy-to-use functions for creating jobs and processing them.
//! - **Concurrency safe**: safely handles multiple concurrent job enqueuers and workers.
//! - **Retry logic**: supports retry logic for jobs.
//! - **Future results**: implements a future pattern for job results, allowing asynchronous
//!   result retrieval, or fire-and-forget job execution.
//!
//! # Basic concepts
//!
//! - **[`Job`]**: a unit of work that needs to be executed.
//! - **[`Task`]**: an interface that your work units must implement.
//! - **[`JobFuture`]**: a mechanism to retrieve the result of a job asynchronously.
//! - **[`JobQueue`]**: a queue that holds and manages the jobs.
//! - **[`Worker`]**: a worker that processes jobs from the queue.
//! - **[`WorkerPool`]**: a pool of workers that execute jobs from the queue.
//!
//! # Creating a task
//!
//! Implement the [`Task`] interface for the work you want to perform. A task's `Error` type
//! must implement [`std::error::Error`] (`#[derive(thiserror::Error)]` is an easy way to get
//! this):
//!
//! ```rust,ignore
//! use jobq::Task;
//!
//! #[derive(Debug, thiserror::Error)]
//! #[error("cannot process zero")]
//! pub struct MyTaskError;
//!
//! pub struct MyTask {
//!     n: u32,
//! }
//!
//! #[async_trait::async_trait]
//! impl Task for MyTask {
//!     type Output = u32;
//!     type Error = MyTaskError;
//!
//!     async fn execute(&self) -> Result<Self::Output, Self::Error> {
//!         if self.n == 0 {
//!             Err(MyTaskError)
//!         } else {
//!             Ok(self.n * 2)
//!         }
//!     }
//! }
//! ```
//!
//! If a task's `execute` method panics, the panic is caught and surfaced as a failed job
//! result ([`Error::TaskPanic`]) rather than taking down the worker. A panic fails the job
//! immediately and is never retried. Tasks should avoid panicking while holding shared
//! invariants (for example, data behind interior mutability shared with other tasks), since
//! the catch boundary cannot guarantee such state is left in a consistent state.
//!
//! # Creating a JobQueue and enqueueing a job
//!
//! ```rust,ignore
//! use jobq::{Error, JobOptions, JobQueueSystemBuilder, Task};
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create a JobQueue with a FIFO queue implementation with a max capacity of 10 and a
//!     // WorkerPool to process jobs, with 2 workers.
//!     let (job_queue, worker_pool) = JobQueueSystemBuilder::<MyTask, _>::fifo(10)
//!         .with_num_workers(2)
//!         .build();
//!
//!     // Start the worker pool's `run` method in a separate task.
//!     let worker_pool_clone = worker_pool.clone();
//!     let handle = tokio::spawn(async move {
//!         worker_pool_clone.run().await;
//!     });
//!
//!     // Enqueueing the job returns a JobFuture, which can be used to retrieve the result
//!     // later, or ignored for fire-and-forget.
//!     let future = job_queue
//!         .enqueue_job(JobOptions::new(MyTask { n: 42 }).with_max_retries(3))
//!         .await
//!         .unwrap();
//!
//!     // A task's own error type is preserved through `Error::TaskExecution`'s `source`
//!     // field, so it can be downcast back to the concrete type the task itself produced.
//!     match future.result().await {
//!         Ok(result) => println!("Job completed with result: {result}"),
//!         Err(Error::TaskExecution { source, .. }) => {
//!             if let Some(original) = source.downcast_ref::<MyTaskError>() {
//!                 println!("Task failed with its own error type: {original}");
//!             }
//!         }
//!         Err(e) => println!("Job failed with error: {e}"),
//!     };
//!
//!     worker_pool.shutdown().await;
//!     handle.await.unwrap();
//! }
//! ```
//!
//! Use [`BatchJobQueueSystemBuilder`] instead of [`JobQueueSystemBuilder`] for a worker pool
//! that processes jobs in batches (configured via [`BatchJobWorkerOptions`]).
//!
//! # Queue implementations
//!
//! - **[`FifoQueue`]**: holds jobs in the order they were enqueued.
//! - **[`LifoQueue`]**: holds jobs in the reverse order they were enqueued.
//! - **[`PriorityQueue`]**: holds jobs in priority order. Jobs with a lower priority value
//!   are processed first; the priority is set via [`PriorityOptions`] on [`JobOptions`]'s
//!   `with_queue_options`.
//!
//! # Dynamic dispatch
//!
//! A [`JobQueue<T, Q>`](JobQueue) normally accepts only one concrete [`Task`] type `T`. To
//! run several unrelated kinds of work through a single pool of workers, build a queue over
//! [`AnyTask`] instead, then use [`enqueue_any`](JobQueue::enqueue_any) (for any `Task`
//! implementation) or [`enqueue_fn`](JobQueue::enqueue_fn) (for a one-off async closure).
//! Each call still returns a future typed to that specific task's own output, even though the
//! queue itself only stores one erased representation internally:
//!
//! ```rust,ignore
//! use jobq::{AnyTask, Error, JobQueueSystemBuilder, Task};
//!
//! #[tokio::main]
//! async fn main() {
//!     let (job_queue, worker_pool) = JobQueueSystemBuilder::<AnyTask, _>::fifo(10)
//!         .with_num_workers(2)
//!         .build();
//!
//!     let worker_pool_clone = worker_pool.clone();
//!     let handle = tokio::spawn(async move {
//!         worker_pool_clone.run().await;
//!     });
//!
//!     // MyTask is the same task type shown above; its output type, u32, still flows
//!     // through to `.result()` even though the queue itself only stores erased tasks.
//!     let number_future = job_queue.enqueue_any(MyTask { n: 21 }).await.unwrap();
//!
//!     // enqueue_fn wraps a closure as a task without a hand-written struct.
//!     let string_future = job_queue
//!         .enqueue_fn(|| async { Ok::<String, MyTaskError>("hello!".to_string()) })
//!         .await
//!         .unwrap();
//!
//!     println!("number: {}", number_future.result().await.unwrap());
//!     println!("string: {}", string_future.result().await.unwrap());
//!
//!     worker_pool.shutdown().await;
//!     handle.await.unwrap();
//! }
//! ```
//!
//! A failing erased task's own error type is preserved too, exactly like a non-erased
//! task's: [`AnyTask::Error`](Task::Error) is [`AnyTaskError`], a wrapper around the
//! original task's own boxed error, and `.result()` unwraps it automatically, so
//! `Error::TaskExecution`'s `source` field can still be downcast back to whatever concrete
//! error the original task produced.
//!
//! [`enqueue_any`](JobQueue::enqueue_any) and [`enqueue_fn`](JobQueue::enqueue_fn) always use
//! the same defaults as `JobOptions::new(..)` (a single attempt, no queue options). For
//! retries or queue options on an erased task, call the lower-level `enqueue_job` directly:
//! `job_queue.enqueue_job(JobOptions::new(AnyTask::new(MyTask { n: 21 })).with_max_retries(3))`.

#[allow(unused_extern_crates)]
extern crate self as jobq;

pub use jobq_core::{
    builder::{BatchJobQueueSystemBuilder, JobQueueSystemBuilder, QueueSystemBuilder},
    error::Error,
    future::{AnyJobFuture, JobFuture, JobFutureSet},
    job::{Job, JobOptions, JobQueue, JobQueueBuilder, JobStatus},
    queue::{
        fifo::FifoQueue,
        lifo::LifoQueue,
        priority::{PriorityOptions, PriorityQueue},
        traits::Queue,
    },
    task::{AnyTask, AnyTaskError, FnTask, Task},
    worker::{
        BatchJobWorker, BatchJobWorkerOptions, JobWorker, Worker, WorkerPool, WorkerPoolBuilder,
    },
};
