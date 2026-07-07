use thiserror::Error;

use crate::queue::error::Error as QueueError;

#[derive(Error, Debug)]
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
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("task panicked: {0}")]
    TaskPanic(String),
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

    pub fn task_execution(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        let source = error.into();

        Self::TaskExecution { message: source.to_string(), source }
    }

    pub fn task_panic(message: impl Into<String>) -> Self {
        Self::TaskPanic(message.into())
    }
}
