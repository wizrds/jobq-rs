//! A lightweight, in-memory job queue for asynchronous task processing within a single
//! process.
//!
//! # Features
//!
//! - **Simple API**: enqueue ordinary tasks or streaming tasks on the same worker pool.
//! - **Concurrency safe**: safely handles multiple concurrent enqueuers and workers.
//! - **Retry logic**: supports retries for ordinary tasks.
//! - **Live streaming**: supports worker-driven streams of items produced over time.
//!
//! # Basic concepts
//!
//! - **[`Task`]**: an async unit of work that returns one final result.
//! - **[`StreamTask`]**: a task that yields many items over time.
//! - **[`JobFuture`]**: a handle for awaiting an ordinary task's final result.
//! - **[`JobStreamHandle`]**: a live stream of produced items, plus a `result()` for the outcome.
//! - **[`JobQueue`]**: a queue that stores executable jobs.
//! - **[`Worker`]**: a worker that dequeues and runs jobs.
//! - **[`WorkerPool`]**: a pool of workers that execute jobs concurrently.
//!
//! # Creating a task
//!
//! Implement [`Task`] for work that produces one final result:
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
//! If a task panics, the panic is caught and surfaced as [`Error::TaskPanic`] rather than
//! taking down the worker. A panic fails the job immediately and is never retried.
//!
//! # Creating a job queue and enqueueing a job
//!
//! ```rust,ignore
//! use jobq::{JobQueueSystemBuilder, Error, JobOptions, Task};
//!
//! #[tokio::main]
//! async fn main() {
//!     let (job_queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
//!         .with_num_workers(2)
//!         .build();
//!
//!     let worker_pool_clone = worker_pool.clone();
//!     let handle = tokio::spawn(async move {
//!         worker_pool_clone.run().await;
//!     });
//!
//!     let future = job_queue
//!         .enqueue_job(JobOptions::new(MyTask { n: 42 }).with_max_retries(3))
//!         .await
//!         .unwrap();
//!
//!     match future.result().await {
//!         Ok(result) => println!("Job completed with result: {result}"),
//!         Err(Error::TaskExecution { source, .. }) => {
//!             if let Some(original) = source.downcast_ref::<MyTaskError>() {
//!                 println!("Task failed with its own error type: {original}");
//!             }
//!         }
//!         Err(error) => println!("Job failed with error: {error}"),
//!     }
//!
//!     worker_pool.shutdown().await;
//!     handle.await.unwrap();
//! }
//! ```
//!
//! Use [`BatchJobQueueSystemBuilder`] instead of [`JobQueueSystemBuilder`] for a worker pool
//! that processes jobs in batches.
//!
//! # Queue implementations
//!
//! - **[`FifoQueue`]**: holds jobs in the order they were enqueued.
//! - **[`LifoQueue`]**: holds jobs in the reverse order they were enqueued.
//! - **[`PriorityQueue`]**: holds jobs in priority order. Lower numeric priority values are
//!   processed first.
//!
//! # Shared erased queue
//!
//! Build a [`JobQueueSystemBuilder`] when you want one queue to carry several unrelated
//! ordinary task types through the same worker pool:
//!
//! ```rust,ignore
//! use jobq::{JobQueueSystemBuilder, JobOptions, Task};
//!
//! #[tokio::main]
//! async fn main() {
//!     let (job_queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
//!         .with_num_workers(2)
//!         .build();
//!
//!     let worker_pool_clone = worker_pool.clone();
//!     let handle = tokio::spawn(async move {
//!         worker_pool_clone.run().await;
//!     });
//!
//!     let number_future = job_queue
//!         .enqueue_job(JobOptions::new(MyTask { n: 21 }))
//!         .await
//!         .unwrap();
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
//! Each enqueue call still returns a fully typed future for that specific task's own output.
//!
//! # Streaming tasks
//!
//! Implement [`StreamTask`] for work that produces items over time:
//!
//! ```rust,ignore
//! use futures::{stream, StreamExt};
//! use jobq::{JobQueueSystemBuilder, JobStreamOptions, StreamTask};
//!
//! #[derive(Debug, thiserror::Error)]
//! #[error("stream failed")]
//! pub struct MyStreamError;
//!
//! pub struct MyStreamTask;
//!
//! impl StreamTask for MyStreamTask {
//!     type Item = u32;
//!     type Error = MyStreamError;
//!
//!     fn execute(
//!         &self,
//!     ) -> futures::stream::BoxStream<'_, Result<Self::Item, Self::Error>> {
//!         stream::iter(vec![Ok(1), Ok(2), Ok(3)]).boxed()
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let (job_queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
//!         .with_num_workers(2)
//!         .build();
//!
//!     let worker_pool_clone = worker_pool.clone();
//!     let handle = tokio::spawn(async move {
//!         worker_pool_clone.run().await;
//!     });
//!
//!     let mut stream_handle = job_queue
//!         .enqueue_stream(JobStreamOptions::new(MyStreamTask).with_capacity(16))
//!         .await
//!         .unwrap();
//!
//!     while let Some(item) = stream_handle.next().await {
//!         println!("item: {item:?}");
//!     }
//!
//!     stream_handle.result().await.unwrap();
//!
//!     worker_pool.shutdown().await;
//!     handle.await.unwrap();
//! }
//! ```

#[allow(unused_extern_crates)]
extern crate self as jobq;

pub use jobq_core::{
    builder::{BatchJobQueueSystemBuilder, JobQueueSystemBuilder, QueueSystemBuilder},
    error::Error,
    executable::{Executable, AnyExecutable},
    future::{JobFuture, JobFutureSet, JobStream, JobStreamHandle},
    job::{Job, JobOptions, JobQueue, JobQueueBuilder, JobStatus, JobStreamOptions, StreamJob},
    queue::{
        fifo::FifoQueue,
        lifo::LifoQueue,
        priority::{PriorityOptions, PriorityQueue},
        traits::Queue,
    },
    task::{FnStreamTask, FnTask, StreamTask, Task},
    worker::{
        BatchJobWorker, BatchJobWorkerOptions, JobWorker, Worker, WorkerPool, WorkerPoolBuilder,
    },
};
