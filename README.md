# JobQ

## Overview

JobQ is a lightweight, in-memory job queue implementation in Rust designed for asynchronous task processing within the same process. It allows for simple job scheduling and processing, suitable for applications that require asynchronous task handling without the need for distributed messaging systems. It is derived from its Golang counterpart found [here](https://gitlab.com/emergentmethods/jobq).

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

pub struct MyTask {
    n: u32,
}

#[async_trait::async_trait]
impl Task for MyTask {
    type Output = u32;
    type Error = String;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        if self.n == 0 {
            Err("Cannot process zero".to_string())
        } else {
            Ok(self.n * 2)
        }
    }
}
```

### Creating a JobQueue and enqueueing a Job

```rust
use jobq::{JobQueueSystemBuilder, Task, JobOptions};


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

    // Wait for the job to complete and retrieve the result
    match future.result().await {
        Ok(result) => println!("Job completed with result: {}", result),
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


## License
This project is licensed under ISC License.

## Support & Feedback
If you encounter any issues or have feedback, please open an issue.

Made with ❤️ by Tim Pogue
