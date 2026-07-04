#[allow(unused_extern_crates)]
extern crate self as jobq;

pub use jobq_core::{
    task::Task,
    future::{JobFuture, JobFutureSet},
    builder::{QueueSystemBuilder, JobQueueSystemBuilder, BatchJobQueueSystemBuilder},
    job::{Job, JobQueue, JobQueueBuilder, JobOptions, JobStatus},
    worker::{Worker, WorkerPool, WorkerPoolBuilder, JobWorker, BatchJobWorker, BatchJobWorkerOptions},
    error::{Error, Result},
    queue::{
        traits::Queue,
        fifo::FifoQueue,
        lifo::LifoQueue,
        priority::{PriorityQueue, PriorityOptions},
    },
};