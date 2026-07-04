use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("queue is closed")]
    Closed,
    #[error("failed to enqueue item to the queue: {0}")]
    EnqueueError(String),
    #[error("failed to dequeue item from the queue: {0}")]
    DequeueError(String),
    #[error("unexpected error occurred: {0}")]
    UnexpectedError(String),
}

impl Error {
    pub fn closed() -> Self {
        Self::Closed
    }

    pub fn enqueue_error(message: impl Into<String>) -> Self {
        Self::EnqueueError(message.into())
    }

    pub fn dequeue_error(message: impl Into<String>) -> Self {
        Self::DequeueError(message.into())
    }

    pub fn unexpected_error(message: impl Into<String>) -> Self {
        Self::UnexpectedError(message.into())
    }
}
