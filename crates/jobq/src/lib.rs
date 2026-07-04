#[allow(unused_extern_crates)]
extern crate self as jobq;

pub use jobq_core::{
    builder::{BatchJobQueueSystemBuilder, JobQueueSystemBuilder, QueueSystemBuilder},
    error::{Error, Result},
    future::{JobFuture, JobFutureSet},
    job::{Job, JobOptions, JobQueue, JobQueueBuilder, JobStatus},
    queue::{
        fifo::FifoQueue,
        lifo::LifoQueue,
        priority::{PriorityOptions, PriorityQueue},
        traits::Queue,
    },
    task::Task,
    worker::{
        BatchJobWorker, BatchJobWorkerOptions, JobWorker, Worker, WorkerPool, WorkerPoolBuilder,
    },
};
