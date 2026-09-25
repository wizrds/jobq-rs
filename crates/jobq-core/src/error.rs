use std::{any::Any, sync::Arc};
use thiserror::Error;

use crate::queue::error::Error as QueueError;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("queue error: {0}")]
    Queue(#[from] QueueError),
    #[error("future closed")]
    FutureClosed,
    #[error("job timeout")]
    JobTimeout,
    #[error("task execution error: {message}")]
    TaskExecution {
        message: String,
        #[source]
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
    #[error("task panicked: {0}")]
    TaskPanic(String),
    #[error("batch returned {actual} results for {expected} members")]
    BatchSizeMismatch { expected: usize, actual: usize },
    #[error("invalid stream capacity: {0}")]
    InvalidStreamCapacity(usize),
    #[error("stream consumer cancelled")]
    ConsumerCancelled,
    #[error("stream consumer lagged")]
    ConsumerLag,
    #[error("stream batch member did not complete")]
    IncompleteStreamBatch,
}

impl Error {
    pub fn queue(error: impl Into<QueueError>) -> Self {
        Self::Queue(error.into())
    }

    pub fn future_closed() -> Self {
        Self::FutureClosed
    }

    pub fn job_timeout() -> Self {
        Self::JobTimeout
    }

    pub fn task_execution<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::TaskExecution {
            message: error.to_string(),
            source: Arc::new(error),
        }
    }

    pub fn task_panic(message: impl Into<String>) -> Self {
        Self::TaskPanic(message.into())
    }

    pub fn from_panic(panic: Box<dyn Any + Send>) -> Self {
        Self::task_panic(
            panic
                .downcast_ref::<&str>()
                .map(ToString::to_string)
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string()),
        )
    }

    pub fn batch_size_mismatch(expected: usize, actual: usize) -> Self {
        Self::BatchSizeMismatch { expected, actual }
    }

    pub fn invalid_stream_capacity(capacity: usize) -> Self {
        Self::InvalidStreamCapacity(capacity)
    }

    pub fn consumer_cancelled() -> Self {
        Self::ConsumerCancelled
    }

    pub fn consumer_lag() -> Self {
        Self::ConsumerLag
    }

    pub fn incomplete_stream_batch() -> Self {
        Self::IncompleteStreamBatch
    }
}
