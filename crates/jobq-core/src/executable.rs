use async_trait::async_trait;

use crate::job::JobStatus;

/// A unit of work a worker can execute without knowing its concrete type.
#[async_trait]
pub trait Executable: Send + Sync {
    async fn execute(&mut self);

    fn status(&self) -> JobStatus;
}

#[async_trait]
impl Executable for Box<dyn Executable> {
    async fn execute(&mut self) {
        (**self).execute().await
    }

    fn status(&self) -> JobStatus {
        (**self).status()
    }
}
