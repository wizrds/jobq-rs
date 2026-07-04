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
