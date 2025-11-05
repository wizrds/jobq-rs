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

pub type Result<T> = std::result::Result<T, Error>;