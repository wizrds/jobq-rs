use std::sync::Arc;
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
    BatchSizeMismatch {
        expected: usize,
        actual: usize,
    },
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

    pub fn batch_size_mismatch(expected: usize, actual: usize) -> Self {
        Self::BatchSizeMismatch { expected, actual }
    }
}
