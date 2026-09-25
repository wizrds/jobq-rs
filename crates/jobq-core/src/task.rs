use std::{future::Future, marker::PhantomData};

use async_trait::async_trait;
use futures::{
    StreamExt,
    stream::{BoxStream, Stream},
};

/// Trait representing a task that can be executed by the job queue.
///
/// Tasks must implement the `execute` method, which performs the task's logic and returns a result.
/// The `Output` type is the result of the task, and the `Error` type is the error that can be returned if the task fails.
#[async_trait]
pub trait Task: Send + Sync {
    type Output: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn execute(&self) -> Result<Self::Output, Self::Error>;
}

/// Wraps a closure as a [`Task`](crate::task::Task) without requiring a hand-written
/// struct and `impl Task` block for simple, one-off work.
pub struct FnTask<F, Fut> {
    f: F,
    _marker: PhantomData<fn() -> Fut>,
}

impl<F, Fut> FnTask<F, Fut> {
    /// Wraps a closure as a [`Task`](crate::task::Task).
    pub fn new(f: F) -> Self {
        Self { f, _marker: PhantomData }
    }
}

#[async_trait]
impl<F, Fut, O, E> Task for FnTask<F, Fut>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<O, E>> + Send,
    O: Send + Sync,
    E: std::error::Error + Send + Sync + 'static,
{
    type Output = O;
    type Error = E;

    async fn execute(&self) -> Result<O, E> {
        (self.f)().await
    }
}

/// Trait representing a task that produces a stream of items driven inside the worker.
///
/// A streaming task is never retried.
pub trait StreamTask: Send + Sync {
    type Item: Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute(&self) -> BoxStream<'_, Result<Self::Item, Self::Error>>;
}

/// Wraps a closure returning a [`Stream`](futures::Stream) as a
/// [`StreamTask`](crate::task::StreamTask) without a hand-written struct.
pub struct FnStreamTask<F, St> {
    f: F,
    _marker: PhantomData<fn() -> St>,
}

impl<F, St> FnStreamTask<F, St> {
    /// Wraps a closure as a [`StreamTask`](crate::task::StreamTask).
    pub fn new(f: F) -> Self {
        Self { f, _marker: PhantomData }
    }
}

impl<F, St, Item, E> StreamTask for FnStreamTask<F, St>
where
    F: Fn() -> St + Send + Sync,
    St: Stream<Item = Result<Item, E>> + Send + 'static,
    Item: Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Item;
    type Error = E;

    fn execute(&self) -> BoxStream<'_, Result<Item, E>> {
        (self.f)().boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{builder::JobQueueSystemBuilder, error::Error, job::JobOptions};

    #[derive(Debug, thiserror::Error)]
    #[error("cannot double zero")]
    struct DoubleError;

    struct DoubleTask {
        n: u32,
    }

    #[async_trait]
    impl Task for DoubleTask {
        type Output = u32;
        type Error = DoubleError;

        async fn execute(&self) -> Result<Self::Output, Self::Error> {
            if self.n == 0 {
                Err(DoubleError)
            } else {
                Ok(self.n * 2)
            }
        }
    }

    struct ShoutTask {
        message: String,
    }

    #[async_trait]
    impl Task for ShoutTask {
        type Output = String;
        type Error = DoubleError;

        async fn execute(&self) -> Result<Self::Output, Self::Error> {
            Ok(format!("{}!", self.message))
        }
    }

    #[tokio::test]
    async fn heterogeneous_outputs_share_one_queue() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(2)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let number_future = queue
            .enqueue_job(JobOptions::new(DoubleTask { n: 21 }))
            .await
            .unwrap();
        let string_future = queue
            .enqueue_job(JobOptions::new(ShoutTask { message: "hello".to_string() }))
            .await
            .unwrap();

        assert_eq!(number_future.result().await.unwrap(), 42);
        assert_eq!(string_future.result().await.unwrap(), "hello!");

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn enqueue_fn_runs_a_closure() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let future = queue
            .enqueue_fn(|| async { Ok::<u32, DoubleError>(21 * 2) })
            .await
            .unwrap();

        assert_eq!(future.result().await.unwrap(), 42);

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn erased_task_error_identity_is_preserved() {
        #[derive(Debug, thiserror::Error)]
        #[error("boom: {0}")]
        struct RichError(u32);

        struct FailingTask;

        #[async_trait]
        impl Task for FailingTask {
            type Output = u32;
            type Error = RichError;

            async fn execute(&self) -> Result<Self::Output, Self::Error> {
                Err(RichError(7))
            }
        }

        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let future = queue
            .enqueue_job(JobOptions::new(FailingTask))
            .await
            .unwrap();

        match future.result().await {
            Err(Error::TaskExecution { source, .. }) => {
                let original = source
                    .downcast_ref::<RichError>()
                    .expect("original error type should be recoverable");

                assert_eq!(original.0, 7);
            }
            other => panic!("expected Err(Error::TaskExecution {{ .. }}), got {other:?}"),
        }

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }
}
