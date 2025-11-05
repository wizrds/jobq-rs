use thiserror::Error;

use crate::queue::error::{Error as QueueError};


#[derive(Error, Debug)]
pub enum Error {
    #[error("queue error: {0}")]
    Queue(#[from] QueueError),
    #[error("future closeD")]
    FutureClosed,
    #[error("job timeout")]
    JobTimeout,
    #[error("task execution error: {0}")]
    TaskExecution(String),
}

pub type Result<T> = std::result::Result<T, Error>;