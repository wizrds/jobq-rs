# JobQ

## Overview

JobQ is a lightweight, in-memory job queue implementation in Rust designed for asynchronous task processing within the same process. It allows for simple job scheduling and processing, suitable for applications that require asynchronous task handling without the need for distributed messaging systems.

## Features

- **Simple API**: Easy-to-use functions for creating jobs and processing them.
- **Concurrency Safe**: Safely handles multiple concurrent job enqueuers and workers.
- **Retry Logic**: Supports retry logic for jobs.
- **Future Results**: Implements a Future pattern for job results, allowing asynchronous result retrieval, or fire-and-forget job execution.


## Installation

```bash
cargo add jobq --git https://github.com/wizrds/jobq-rs.git
```

## Usage

### Basic Concepts

- **Job**: A unit of work that needs to be executed.
- **Task**: An interface that your work units must implement.
- **JobFuture**: A mechanism to retrieve the result of a job asynchronously.
- **JobQueue**: A queue that holds and manages the jobs.
- **Worker**: A worker that processes jobs from the queue.
- **WorkerPool**: A pool of workers that execute jobs from the queue.


### Creating a Task

Implement the `Task` interface for the work you want to perform:

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

A task's `Error` type must implement `std::error::Error`, which is what `#[derive(thiserror::Error)]` provides above (add `thiserror` to your own `Cargo.toml` for this, or implement `std::error::Error` by hand if you would rather not take the dependency).

If a task's `execute` method panics, the panic is caught and surfaced as a failed job result (`Error::TaskPanic`) rather than taking down the worker. A panic fails the job immediately and is never retried. Note that tasks should avoid panicking while holding shared invariants (for example, data behind interior mutability shared with other tasks), since the catch boundary cannot guarantee such state is left in a consistent state.

### Creating a JobQueue and enqueueing a Job

```rust
use jobq::{Error, JobOptions, JobQueueSystemBuilder, Task};


#[tokio::main]
async fn main() {
    // Create a JobQueue with a FIFO queue implementation with a max capacity of 10 and a WorkerPool to process jobs,
    // with 2 workers.
    let (job_queue, worker_pool) = JobQueueSystemBuilder::<MyTask, _>::fifo(10)
        .with_num_workers(2)
        .build();

    // Use the `BatchJobQueueSystemBuilder` to create a JobQueue with a worker that processes jobs in batches.
    // let (job_queue, worker_pool) = BatchJobQueueSystemBuilder::<MyTask, _>::fifo(10)
    //     .with_num_workers(2)
    //     .with_worker_options(BatchJobWorkerOptions {
    //         batch_size: 3,
    //         batch_timeout: std::time::Duration::from_millis(10),
    //     })
    //     .build();

    // Start the worker pool `run` method in a separate task.
    let worker_pool_clone = worker_pool.clone();
    let handle = tokio::spawn(async move {
        worker_pool_clone.run().await;
    });

    // Enqueueing the job returns a JobFuture which can be used to retrieve the result later,
    // or it can be ignored if you need to fire-and-forget the job.
    let future = job_queue
        .enqueue_job(
            JobOptions::new(MyTask { n: 42 })
                .with_max_retries(3)
        )
        .await
        .unwrap();

    // Wait for the job to complete and retrieve the result. A task's own error type
    // is preserved through `Error::TaskExecution`'s `source` field, so it can be
    // downcast back to the concrete type the task itself produced.
    match future.result().await {
        Ok(result) => println!("Job completed with result: {}", result),
        Err(Error::TaskExecution { source, .. }) => {
            if let Some(original) = source.downcast_ref::<MyTaskError>() {
                println!("Task failed with its own error type: {original}");
            }
        }
        Err(e) => println!("Job failed with error: {}", e),
    };

    // Shutdown the worker pool gracefully
    worker_pool.shutdown().await;
    // Wait for the worker pool to finish processing all jobs
    handle.await.unwrap();
}
```

### Queue implementations

JobQ provides two queue implementations:

- **FIFOQueue**: A FIFO queue that holds jobs in the order they were enqueued.
- **LIFOQueue**: A LIFO queue that holds jobs in the reverse order they were enqueued.
- **PriorityQueue**: A priority queue that holds jobs in priority order. Jobs with a lower priority value will be processed first. Defining the priority is done in the `PriorityOptions` when enqueuing a job. This can be set via the `with_queue_options` method on the `JobOptions` struct.

### Dynamic dispatch

Every `JobQueue<T, Q>` normally accepts only one concrete `Task` type `T`. If you would rather run several unrelated kinds of work through a single pool of workers, build a queue over `AnyTask` instead, then use `enqueue_any` (for any `Task` implementation) or `enqueue_fn` (for a one-off async closure) to enqueue whatever you like. Each call still returns a future typed to that specific task's own output, even though the queue itself only ever stores one erased representation internally.

```rust
use jobq::{AnyTask, Error, JobQueueSystemBuilder, Task};

#[tokio::main]
async fn main() {
    let (job_queue, worker_pool) = JobQueueSystemBuilder::<AnyTask, _>::fifo(10)
        .with_num_workers(2)
        .build();

    let worker_pool_clone = worker_pool.clone();
    let handle = tokio::spawn(async move {
        worker_pool_clone.run().await;
    });

    // MyTask is the same task type shown above; its output type, u32, still flows
    // through to `.result()` even though the queue itself only stores erased tasks.
    let number_future = job_queue.enqueue_any(MyTask { n: 21 }).await.unwrap();

    // enqueue_fn wraps a closure as a task without a hand-written struct.
    let string_future = job_queue
        .enqueue_fn(|| async { Ok::<String, MyTaskError>("hello!".to_string()) })
        .await
        .unwrap();

    println!("number: {}", number_future.result().await.unwrap());
    println!("string: {}", string_future.result().await.unwrap());

    worker_pool.shutdown().await;
    handle.await.unwrap();
}
```

A failing erased task's own error type is preserved too, exactly like a non-erased task's: `AnyTask::Error` is `Box<dyn std::error::Error + Send + Sync>`, so `Error::TaskExecution`'s `source` field can still be downcast back to whatever concrete error the original task produced.

```rust
match number_future.result().await {
    Err(Error::TaskExecution { source, .. }) => {
        if let Some(original) = source.downcast_ref::<MyTaskError>() {
            println!("Task failed with its own error type: {original}");
        }
    }
    _ => {}
}
```

`enqueue_any` and `enqueue_fn` always use the same defaults as `JobOptions::new(..)` (a single attempt, no queue options). If you need retries or queue options for an erased task, call the lower-level `enqueue_job` directly: `job_queue.enqueue_job(JobOptions::new(AnyTask::new(MyTask { n: 21 })).with_max_retries(3)).await`.

## License
This project is licensed under ISC License.

## Support & Feedback
If you encounter any issues or have feedback, please open an issue.

Made with ❤️ by Tim Pogue
