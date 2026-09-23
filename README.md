# JobQ

## Overview

JobQ is a lightweight, in-memory job queue for asynchronous work inside a single
process. One worker pool can run tasks that return one result, tasks that stream
items, and batches of either kind.

## Features

- **Shared queue**: enqueue different task and stream types on the same worker pool.
- **Concurrency safe**: safely handles multiple concurrent enqueuers and workers.
- **Batching**: collect inputs and execute them together in one task call.
- **Retry logic**: retry failed results independently for each ordinary or batched input.
- **Live streaming**: supports worker-driven streams of items produced over time.

## Installation

```bash
cargo add jobq --git https://github.com/wizrds/jobq-rs.git
```

## Usage

### Basic concepts

- **Task**: an async unit of work that returns one final result.
- **StreamTask**: a task that yields many items over time.
- **BatchTask**: one async call that processes a slice of inputs and returns one
  result per input.
- **BatchStreamTask**: one stream that tags each item with its input index.
- **BatchPolicy**: the maximum batch size and the time to wait for more inputs.
- **Batcher / StreamBatcher**: handles for submitting inputs to batch tasks and
  batch stream tasks.
- **JobFuture**: a handle for awaiting one task or batch input's final result.
- **JobStreamHandle**: a live stream of produced items, plus a `result()` for the outcome.
- **JobQueue**: a queue that stores executable jobs.
- **Worker**: a worker that dequeues and runs jobs.
- **WorkerPool**: a pool of workers that execute jobs concurrently.

### Creating a task

Implement `Task` for work that produces one final result:

```rust
use jobq::Task;

#[derive(Debug, thiserror::Error)]
#[error("cannot process zero")]
pub struct MyTaskError;

pub struct MyTask {
    n: u32,
}

#[async_trait::async_trait]
impl Task for MyTask {
    type Output = u32;
    type Error = MyTaskError;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        if self.n == 0 {
            Err(MyTaskError)
        } else {
            Ok(self.n * 2)
        }
    }
}
```

A task's `Error` type must implement `std::error::Error`. If a task panics, the
panic is caught and surfaced as `Error::TaskPanic` rather than taking down the
worker. A panic fails the job immediately and is never retried.

### Creating a job queue and enqueueing a job

```rust
use jobq::{JobQueueSystemBuilder, Error, JobOptions, Task};

#[tokio::main]
async fn main() {
    let (job_queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
        .with_num_workers(2)
        .build();

    let worker_pool_clone = worker_pool.clone();
    let handle = tokio::spawn(async move {
        worker_pool_clone.run().await;
    });

    let future = job_queue
        .enqueue_job(JobOptions::new(MyTask { n: 42 }).with_max_retries(3))
        .await
        .unwrap();

    match future.result().await {
        Ok(result) => println!("Job completed with result: {result}"),
        Err(Error::TaskExecution { source, .. }) => {
            if let Some(original) = source.downcast_ref::<MyTaskError>() {
                println!("Task failed with its own error type: {original}");
            }
        }
        Err(error) => println!("Job failed with error: {error}"),
    }

    worker_pool.shutdown().await;
    handle.await.unwrap();
}
```

### Batch tasks

Implement `BatchTask` to process several inputs in one call. Return one result
per input, in the same order:

```rust
use std::{convert::Infallible, time::Duration};
use jobq::{BatchPolicy, BatchTask, JobOptions, JobQueueSystemBuilder};

struct DoubleBatch;

#[async_trait::async_trait]
impl BatchTask for DoubleBatch {
    type Input = u32;
    type Output = u32;
    type Error = Infallible;

    async fn execute(
        &self,
        inputs: &[Self::Input],
    ) -> Result<Vec<Result<Self::Output, Self::Error>>, Self::Error> {
        Ok(inputs.iter().map(|&input| Ok(input * 2)).collect())
    }
}

#[tokio::main]
async fn main() {
    let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
        .with_num_workers(1)
        .build();

    let running = tokio::spawn({
        let worker_pool = worker_pool.clone();
        async move { worker_pool.run().await }
    });

    let batcher = queue.batcher(
        DoubleBatch,
        BatchPolicy {
            max_size: 2,
            max_wait: Duration::from_millis(50),
        },
    );
    let first = batcher.enqueue(JobOptions::new(21)).await.unwrap();
    let second = batcher.enqueue(JobOptions::new(7)).await.unwrap();

    assert_eq!(first.result().await.unwrap(), 42);
    assert_eq!(second.result().await.unwrap(), 14);

    worker_pool.shutdown().await;
    running.await.unwrap();
}
```

`BatchPolicy::max_size` seals a batch when it reaches that many inputs.
`max_wait` is measured from the first input; once a worker picks up the queued
batch, it waits only for the remaining time. Each batcher collects its own
inputs; clones share its open batch. A submission with different queue options
seals the current batch and opens another. `BatchPolicy::default()` uses a size
of one and no wait.

Each submitted input has its own `JobFuture` and `JobOptions`.
`with_max_retries(n)` allows up to `n` total attempts per input; zero still
executes once. Only task execution errors are retried. Failed inputs retry
together without re-running successful inputs, and an outer task error counts
as a failure for every input. An exhausted input receives its task error. If
the task returns the wrong number of results, every future receives
`Error::BatchSizeMismatch`; a panic reaches each future as `Error::TaskPanic`.

### Queue implementations

JobQ provides three queue implementations:

- **FifoQueue**: jobs are processed in the order they were enqueued.
- **LifoQueue**: jobs are processed in reverse enqueue order.
- **PriorityQueue**: higher numeric priority values are dequeued first. Jobs with
  the same priority are dequeued in insertion order.

### Streaming tasks

Implement `StreamTask` for work that produces items over time:

```rust
use futures::{stream, StreamExt};
use jobq::{JobQueueSystemBuilder, JobStreamOptions, StreamTask};

#[derive(Debug, thiserror::Error)]
#[error("stream failed")]
pub struct MyStreamError;

pub struct MyStreamTask;

impl StreamTask for MyStreamTask {
    type Item = u32;
    type Error = MyStreamError;

    fn execute(
        &self,
    ) -> futures::stream::BoxStream<'_, Result<Self::Item, Self::Error>> {
        stream::iter(vec![Ok(1), Ok(2), Ok(3)]).boxed()
    }
}

#[tokio::main]
async fn main() {
    let (job_queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
        .with_num_workers(2)
        .build();

    let worker_pool_clone = worker_pool.clone();
    let handle = tokio::spawn(async move {
        worker_pool_clone.run().await;
    });

    let mut stream_handle = job_queue
        .enqueue_stream(JobStreamOptions::new(MyStreamTask).with_capacity(16))
        .await
        .unwrap();

    while let Some(item) = stream_handle.next().await {
        println!("item: {item:?}");
    }

    stream_handle.result().await.unwrap();

    worker_pool.shutdown().await;
    handle.await.unwrap();
}
```

The worker polls the underlying stream. The caller only consumes already-produced
items by iterating the `JobStreamHandle` directly, then awaits `JobStreamHandle::result`
for the terminal outcome.

### Batch streams

Implement `BatchStreamTask` to produce items for several inputs from one stream.
Each item carries the zero-based index of the input whose handle should receive
it. One input can produce any number of items:

```rust
use std::{convert::Infallible, time::Duration};
use futures::{StreamExt, stream::{self, BoxStream}};
use jobq::{BatchPolicy, BatchStreamTask, JobQueueSystemBuilder, JobStreamOptions};

struct TimesTen;

impl BatchStreamTask for TimesTen {
    type Input = u32;
    type Item = u32;
    type Error = Infallible;

    fn execute<'a>(
        &'a self,
        inputs: &'a [Self::Input],
    ) -> BoxStream<'a, (usize, Result<Self::Item, Self::Error>)> {
        stream::iter(
            inputs
                .iter()
                .copied()
                .enumerate()
                .map(|(index, input)| (index, Ok(input * 10))),
        )
        .boxed()
    }
}

#[tokio::main]
async fn main() {
    let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
        .with_num_workers(1)
        .build();

    let running = tokio::spawn({
        let worker_pool = worker_pool.clone();
        async move { worker_pool.run().await }
    });

    let batcher = queue.stream_batcher(
        TimesTen,
        BatchPolicy {
            max_size: 2,
            max_wait: Duration::from_millis(50),
        },
    );
    let mut first = batcher.enqueue(JobStreamOptions::new(2)).await.unwrap();
    let mut second = batcher.enqueue(JobStreamOptions::new(3)).await.unwrap();

    assert_eq!(first.next().await.unwrap().unwrap(), 20);
    assert_eq!(second.next().await.unwrap().unwrap(), 30);
    assert!(first.next().await.is_none());
    assert!(second.next().await.is_none());
    first.result().await.unwrap();
    second.result().await.unwrap();

    worker_pool.shutdown().await;
    running.await.unwrap();
}
```

Each submission has its own `JobStreamHandle` and `JobStreamOptions`, including
its channel capacity. Stream item errors arrive on that input's handle. An
out-of-range index is ignored, and a panic fails every member's terminal
`result()`. Streaming tasks are not retried.

## License

This project is licensed under the ISC License.

## Support and feedback

If you encounter any issues or have feedback, please open an issue.
