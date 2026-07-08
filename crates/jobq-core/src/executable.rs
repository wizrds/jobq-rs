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

/// A type-erased wrapper around any `Executable` type.
pub struct AnyExecutable(Box<dyn Executable>);

impl AnyExecutable {
    pub fn new<E>(inner: E) -> Self
    where
        E: Executable + 'static,
    {
        Self(Box::new(inner))
    }

    pub fn as_inner(&self) -> &dyn Executable {
        &*self.0
    }

    pub fn as_inner_mut(&mut self) -> &mut dyn Executable {
        &mut *self.0
    }

    pub fn into_inner(self) -> Box<dyn Executable> {
        self.0
    }
}

#[async_trait]
impl Executable for AnyExecutable {
    async fn execute(&mut self) {
        self.as_inner_mut()
            .execute()
            .await
    }

    fn status(&self) -> JobStatus {
        self.as_inner()
            .status()
    }
}

