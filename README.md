# JobQ

## Overview

JobQ is a lightweight, in-memory job queue for asynchronous work inside a single
process. It supports both ordinary tasks that produce one final result and
streaming tasks that yield items over time while workers drive the underlying
work.

## Features

- **Simple API**: enqueue ordinary tasks or streaming tasks on the same worker pool.
- **Concurrency safe**: safely handles multiple concurrent enqueuers and workers.
- **Retry logic**: supports retries for ordinary tasks.
- **Live streaming**: supports worker-driven streams of items produced over time.

## Installation

```bash
cargo add jobq --git https://github.com/wizrds/jobq-rs.git
```

## Usage

### Basic concepts

- **Task**: an async unit of work that returns one final result.
- **StreamTask**: a task that yields many items over time.
- **JobFuture**: a handle for awaiting an ordinary task's final result.
- **StreamHandle**: a live stream of produced items, plus a `result()` for the outcome.
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

Use `BatchJobQueueSystemBuilder` instead of `JobQueueSystemBuilder` for a worker
pool that processes jobs in batches.

### Queue implementations

JobQ provides three queue implementations:

- **FifoQueue**: jobs are processed in the order they were enqueued.
- **LifoQueue**: jobs are processed in reverse enqueue order.
- **PriorityQueue**: jobs are processed by priority. Lower numeric priority values
  are processed first.

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

## License

This project is licensed under the ISC License.

## Support and feedback

If you encounter any issues or have feedback, please open an issue.
