//! A lightweight, in-memory job queue for asynchronous task processing within a single
//! process.
//!
//! # Features
//!
//! - **Shared queue**: enqueue different task and stream types on the same worker pool.
//! - **Concurrency safe**: safely handles multiple concurrent enqueuers and workers.
//! - **True batching**: collect inputs and execute them together in one task call.
//! - **Retry logic**: retry failed results independently for each ordinary or batched input.
//! - **Live streaming**: supports worker-driven streams of items produced over time.
//!
//! # Basic concepts
//!
//! - **[`Task`]**: an async unit of work that returns one final result.
//! - **[`StreamTask`]**: a task that yields many items over time.
//! - **[`BatchTask`]**: one async call that processes inputs and returns one result per input.
//! - **[`MultiplexedBatchStreamTask`]**: one stream whose items identify their input by index.
//! - **[`IndependentBatchStreamTask`]**: a per-input stream that shares one prepared value.
//! - **[`Multiplexed`] / [`Independent`]**: the two [`StreamBatchMode`] implementations
//!   selecting between them.
//! - **[`BatchPolicy`]**: a batch's maximum size and wait time.
//! - **[`Batcher`] / [`StreamBatcher`]**: handles for submitting inputs to batch tasks and streams.
//! - **[`JobFuture`]**: a handle for awaiting one task or batch input's final result.
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
//!     let pool_handle = worker_pool.spawn(tokio::spawn);
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
//!     pool_handle.await.unwrap();
//! }
//! ```
//!
//! `build()` returns a cloneable [`JobQueue`] handle and a worker pool. The same queue accepts
//! unrelated task and stream types, and each submission returns a typed future or stream handle.
//!
//! # Queue implementations
//!
//! - **[`FifoQueue`]**: holds jobs in the order they were enqueued.
//! - **[`LifoQueue`]**: holds jobs in the reverse order they were enqueued.
//! - **[`PriorityQueue`]**: dequeues higher numeric priority values first; equal priorities
//!   retain insertion order.
//!
//! # Batch tasks
//!
//! Implement [`BatchTask`] to receive a slice of inputs in one call and return one result per
//! input in the same order. Create a [`Batcher`] from a [`JobQueue`] and submit each input with
//! [`JobOptions`]:
//!
//! ```rust,ignore
//! use std::{convert::Infallible, time::Duration};
//! use jobq::{BatchPolicy, BatchTask, JobOptions, JobQueueSystemBuilder};
//!
//! struct DoubleBatch;
//!
//! #[async_trait::async_trait]
//! impl BatchTask for DoubleBatch {
//!     type Input = u32;
//!     type Output = u32;
//!     type Error = Infallible;
//!
//!     async fn execute(
//!         &self,
//!         inputs: &[Self::Input],
//!     ) -> Result<Vec<Result<Self::Output, Self::Error>>, Self::Error> {
//!         Ok(inputs.iter().map(|&input| Ok(input * 2)).collect())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
//!         .with_num_workers(1)
//!         .build();
//!
//!     let pool_handle = worker_pool.spawn(tokio::spawn);
//!
//!     let batcher = queue
//!         .batcher(DoubleBatch)
//!         .policy(BatchPolicy {
//!             max_size: 2,
//!             max_wait: Duration::from_millis(50),
//!         })
//!         .build();
//!
//!     let first = batcher.enqueue(JobOptions::new(21)).await.unwrap();
//!     let second = batcher.enqueue(JobOptions::new(7)).await.unwrap();
//!
//!     assert_eq!(first.result().await.unwrap(), 42);
//!     assert_eq!(second.result().await.unwrap(), 14);
//!
//!     worker_pool.shutdown().await;
//!     pool_handle.await.unwrap();
//! }
//! ```
//!
//! `max_size` seals a batch when it fills. `max_wait` is measured from the first input; once a
//! worker picks up the queued batch, it waits only for the remaining time. Each batcher collects
//! its own inputs; clones share its open batch. A different queue option seals the current batch
//! and opens another. `BatchPolicy::default()` uses a size of one and no wait.
//!
//! Each input has its own [`JobFuture`] and retry budget. `with_max_retries(n)` permits at most
//! `n` total attempts per input; zero still executes once. Failed inputs are retried without
//! re-running successful ones. An outer task error counts as a failure for every input; an
//! exhausted input receives its error. A wrong number of results reaches every member as
//! [`Error::BatchSizeMismatch`], and a panic reaches every member as [`Error::TaskPanic`]. Only
//! task execution errors are retried.
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
//!     let pool_handle = worker_pool.spawn(tokio::spawn);
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
//!     pool_handle.await.unwrap();
//! }
//! ```
//!
//! The worker polls the stream. Each [`JobStreamHandle`] yields produced items and its `result()`
//! reports the terminal outcome. [`JobStreamOptions::with_capacity`] sets the handle's channel
//! capacity; a capacity of zero is rejected as [`Error::InvalidStreamCapacity`] before the job
//! reaches the queue.
//!
//! # Batch streams
//!
//! A batch stream shares one batching window across several inputs, each with its own
//! [`JobStreamHandle`], and delivers items to those handles as they are produced. Two modes
//! select how the shared work relates to each input:
//!
//! | Mode | Producer | Full member channel |
//! | --- | --- | --- |
//! | [`Multiplexed`] | One stream tags each item with its input's index. | [`Error::ConsumerLag`] fails only that member. |
//! | [`Independent`] | One stream per input, sharing one prepared value. | Backpressure; a slow member never affects its siblings. |
//!
//! Implement [`MultiplexedBatchStreamTask`] when one stream naturally produces items for every
//! input together:
//!
//! ```rust,ignore
//! use std::{convert::Infallible, time::Duration};
//! use futures::{StreamExt, stream::{self, BoxStream}};
//! use jobq::{
//!     BatchPolicy, BatchStreamEvent, JobQueueSystemBuilder, JobStreamOptions,
//!     Multiplexed, MultiplexedBatchStreamTask, StreamBatchActivity,
//! };
//!
//! struct TimesTen;
//!
//! impl MultiplexedBatchStreamTask for TimesTen {
//!     type Input = u32;
//!     type Item = u32;
//!     type Error = Infallible;
//!
//!     fn execute<'a>(
//!         &'a self,
//!         inputs: &'a [Self::Input],
//!         _activity: &'a StreamBatchActivity,
//!     ) -> BoxStream<'a, BatchStreamEvent<Self::Item, Self::Error>> {
//!         stream::iter(inputs.iter().copied().enumerate().flat_map(|(index, input)| {
//!             [
//!                 BatchStreamEvent::Item { index, item: Ok(input * 10) },
//!                 BatchStreamEvent::Finished { index, outcome: Ok(()) },
//!             ]
//!         }))
//!         .boxed()
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
//!         .with_num_workers(1)
//!         .build();
//!
//!     let pool_handle = worker_pool.spawn(tokio::spawn);
//!
//!     let batcher = queue
//!         .stream_batcher(TimesTen)
//!         .mode(Multiplexed)
//!         .policy(BatchPolicy {
//!             max_size: 2,
//!             max_wait: Duration::from_millis(50),
//!         })
//!         .build();
//!
//!     let mut first = batcher.enqueue(JobStreamOptions::new(2)).await.unwrap();
//!     let mut second = batcher.enqueue(JobStreamOptions::new(3)).await.unwrap();
//!
//!     assert_eq!(first.next().await.unwrap().unwrap(), 20);
//!     assert_eq!(second.next().await.unwrap().unwrap(), 30);
//!     assert!(first.next().await.is_none());
//!     assert!(second.next().await.is_none());
//!
//!     first.result().await.unwrap();
//!     second.result().await.unwrap();
//!
//!     worker_pool.shutdown().await;
//!     pool_handle.await.unwrap();
//! }
//! ```
//!
//! Implement [`IndependentBatchStreamTask`] when each input needs its own stream built from one
//! value shared across the batch:
//!
//! ```rust,ignore
//! use std::{convert::Infallible, time::Duration};
//! use futures::{StreamExt, stream::{self, BoxStream}};
//! use jobq::{
//!     BatchPolicy, Independent, IndependentBatchStreamTask, JobQueueSystemBuilder,
//!     JobStreamOptions,
//! };
//!
//! struct Ranges;
//!
//! #[async_trait::async_trait]
//! impl IndependentBatchStreamTask for Ranges {
//!     type Input = u32;
//!     type Shared = u32;
//!     type Item = u32;
//!     type Error = Infallible;
//!
//!     async fn prepare(&self, inputs: &[Self::Input]) -> Result<Self::Shared, Self::Error> {
//!         Ok(inputs.iter().sum())
//!     }
//!
//!     fn stream<'a>(
//!         &'a self,
//!         shared: &'a Self::Shared,
//!         input: &'a Self::Input,
//!     ) -> BoxStream<'a, Result<Self::Item, Self::Error>> {
//!         stream::iter(0..*input)
//!             .map(move |offset| Ok(shared + offset))
//!             .boxed()
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
//!         .with_num_workers(1)
//!         .build();
//!
//!     let pool_handle = worker_pool.spawn(tokio::spawn);
//!
//!     let batcher = queue
//!         .stream_batcher(Ranges)
//!         .mode(Independent)
//!         .policy(BatchPolicy {
//!             max_size: 2,
//!             max_wait: Duration::from_millis(50),
//!         })
//!         .build();
//!
//!     let mut first = batcher.enqueue(JobStreamOptions::new(2)).await.unwrap();
//!     let mut second = batcher.enqueue(JobStreamOptions::new(1)).await.unwrap();
//!
//!     assert_eq!(first.next().await.unwrap().unwrap(), 3);
//!     assert_eq!(first.next().await.unwrap().unwrap(), 4);
//!     assert!(first.next().await.is_none());
//!     assert_eq!(second.next().await.unwrap().unwrap(), 3);
//!     assert!(second.next().await.is_none());
//!
//!     first.result().await.unwrap();
//!     second.result().await.unwrap();
//!
//!     worker_pool.shutdown().await;
//!     pool_handle.await.unwrap();
//! }
//! ```
//!
//! Each submission has its own [`JobStreamHandle`] and [`JobStreamOptions`], including its
//! channel capacity; a zero capacity is rejected the same way as for a plain stream.
//! [`IndependentBatchStreamTask::prepare`] runs once per batch: an error or panic there fails
//! every member that hasn't already finished. A panic constructing or polling one member's
//! stream fails only that member; its siblings continue. When a member's consumer drops its
//! [`JobStreamHandle`], that member receives [`Error::ConsumerCancelled`]. Rejection at the
//! queue reaches every pending caller directly, and dropping one caller's future before it joins
//! the batch does not cancel a sibling that already joined. Batch streams are not retried.
//!
//! Implementing a mode beyond [`Multiplexed`] and [`Independent`] means implementing
//! [`StreamBatchMode`] directly. Its `run` method receives the task, the sealed inputs, and a
//! [`StreamBatchContext`] whose members are exposed as [`StreamBatchMember`] values for sending
//! items and finishing each one, plus a [`StreamBatchActivity`] for observing per-member
//! cancellation.
//!
//! # Shared handles
//!
//! Cloning a [`JobQueue`] shares its underlying queue. Cloning a [`Batcher`] or
//! [`StreamBatcher`] shares its open batch. Their `downgrade()` methods create
//! [`WeakJobQueue`], [`WeakBatcher`], and [`WeakStreamBatcher`] handles; `upgrade()` returns a
//! strong handle while one still exists.

#[allow(unused_extern_crates)]
extern crate self as jobq;

pub use jobq_core::{
    batch::*,
    builder::*,
    error::*,
    executable::*,
    future::*,
    job::*,
    queue::{fifo::*, lifo::*, priority::*, traits::*},
    task::*,
    worker::*,
};
