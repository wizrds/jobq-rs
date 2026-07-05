use std::{any::Any, fmt, future::Future, marker::PhantomData};

use async_trait::async_trait;

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

/// The error type of an [`AnyTask`](crate::task::AnyTask): a `Sized` wrapper around the
/// original task's own boxed error.
#[derive(Debug)]
pub struct AnyTaskError(Box<dyn std::error::Error + Send + Sync>);

impl AnyTaskError {
    pub fn new<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self(Box::new(error))
    }

    /// Attempts to downcast the wrapped error back to its original concrete type.
    pub fn downcast_ref<E: std::error::Error + 'static>(&self) -> Option<&E> {
        self.0.downcast_ref::<E>()
    }

    /// Consumes the wrapper, returning the original boxed error.
    pub fn into_inner(self) -> Box<dyn std::error::Error + Send + Sync> {
        self.0
    }
}

impl fmt::Display for AnyTaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for AnyTaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

// Object-safe counterpart of `Task` so `AnyTask` can store any concrete `Task` behind one boxed trait object.
#[async_trait]
trait ErasedTask: Send + Sync {
    async fn execute_erased(&self) -> Result<Box<dyn Any + Send + Sync>, AnyTaskError>;
}

#[async_trait]
impl<T> ErasedTask for T
where
    T: Task + 'static,
    T::Output: 'static,
{
    async fn execute_erased(&self) -> Result<Box<dyn Any + Send + Sync>, AnyTaskError> {
        self.execute()
            .await
            .map(|output| Box::new(output) as Box<dyn Any + Send + Sync>)
            .map_err(AnyTaskError::new)
    }
}

/// A type-erased [`Task`](crate::task::Task) that can share a queue with tasks of other,
/// unrelated concrete types and output types.
pub struct AnyTask {
    inner: Box<dyn ErasedTask>,
}

impl AnyTask {
    /// Erases a concrete [`Task`](crate::task::Task) so it can share a queue with
    /// tasks of other, unrelated concrete types and output types.
    pub fn new<T>(task: T) -> Self
    where
        T: Task + 'static,
        T::Output: 'static,
    {
        Self { inner: Box::new(task) }
    }
}

#[async_trait]
impl Task for AnyTask {
    type Output = Box<dyn Any + Send + Sync>;
    type Error = AnyTaskError;

    async fn execute(&self) -> Result<Self::Output, Self::Error> {
        self.inner.execute_erased().await
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{builder::JobQueueSystemBuilder, error::Error};

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
        let (queue, worker_pool) = JobQueueSystemBuilder::<AnyTask, _>::fifo(10)
            .with_num_workers(2)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let number_future = queue
            .enqueue_any(DoubleTask { n: 21 })
            .await
            .unwrap();
        let string_future = queue
            .enqueue_any(ShoutTask { message: "hello".to_string() })
            .await
            .unwrap();

        assert_eq!(number_future.result().await.unwrap(), 42);
        assert_eq!(string_future.result().await.unwrap(), "hello!");

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn enqueue_fn_runs_a_closure() {
        let (queue, worker_pool) = JobQueueSystemBuilder::<AnyTask, _>::fifo(10)
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

        let (queue, worker_pool) = JobQueueSystemBuilder::<AnyTask, _>::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let future = queue
            .enqueue_any(FailingTask)
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
