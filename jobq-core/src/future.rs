use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use futures::channel::oneshot::{Sender, Receiver, channel};
use mea::mutex::Mutex;

use crate::error::{Error, Result};


/// Represents a future that can be awaited to get the result of a [`Job`](crate::job::Job).
pub struct JobFuture<T>
where
    T: Send + Sync,
{
    inner: Arc<Mutex<JobFutureInner<T>>>,
}

impl<T> JobFuture<T>
where 
    T: Send + Sync,
{
    /// Creates a new [`JobFuture`](crate::future::JobFuture) instance with a channel for receiving the job's result.
    /// 
    /// # Returns
    /// A tuple containing the [`JobFuture`](crate::future::JobFuture) instance and a [`JobFutureSetter`](crate::future::JobFutureSetter)
    /// that can be used to set the result of the job.
    pub fn new() -> (Self, JobFutureSetter<T>) {
        let (sender, receiver) = channel();
        let setter = JobFutureSetter { sender: Some(sender) };

        (
            Self {
                inner: Arc::new(Mutex::new(JobFutureInner {
                    result: None,
                    receiver: Some(receiver),
                    closed: AtomicBool::new(false),
                }))
            },
            setter
        )
    }

    /// Awaits the result of the [`JobFuture`](crate::future::JobFuture).
    /// 
    /// # Returns
    /// A `Result` containing the [`Job`](crate::job::Job)'s output if successful, or an error if the task failed or the future was closed.
    pub async fn result(&self) -> Result<T> {
        let mut inner = self.inner.lock().await;

        if let Some(result) = inner.result.take() {
            return result;
        }

        if inner.closed.load(Ordering::SeqCst) {
            return Err(Error::FutureClosed);
        }

        match inner.receiver.take() {
            Some(receiver) => match receiver.await {
                Ok(result) => result,
                Err(_) => Err(Error::FutureClosed),
            },
            None => Err(Error::FutureClosed),
        }
    }

    /// Closes the future, preventing any further awaits on it.
    pub async fn close(&self) {
        let mut inner = self.inner.lock().await;
        inner.closed.store(true, Ordering::SeqCst);
        inner.receiver.take(); // Drop the receiver to prevent further awaits
        inner.result.take(); // Clear any existing result
    }
}


pub struct JobFutureInner<T>
where
    T: Send + Sync,
{
    result: Option<Result<T>>,
    receiver: Option<Receiver<Result<T>>>,
    closed: AtomicBool,
}


/// A setter for the [`JobFuture`](crate::future::JobFuture) that allows setting the result
/// of the [`Job`](crate::job::Job) associated with the future.
#[derive(Debug)]
pub struct JobFutureSetter<T>
where
    T: Send + Sync,
{
    sender: Option<Sender<Result<T>>>,
}

impl<T> JobFutureSetter<T>
where 
    T: Send + Sync,
{
    /// Sets the result of the [`JobFuture`](crate::future::JobFuture), sending it through the channel.
    /// 
    /// # Arguments
    /// * `result` - The result of the [`Job`](crate::job::Job) to be sent.
    pub fn set_result(&mut self, result: Result<T>) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(result);
        }
    }
}