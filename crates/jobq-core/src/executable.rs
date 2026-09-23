use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};

use crate::{
    error::Error,
    job::JobStatus,
    task::{BatchStreamTask, BatchTask, StreamTask, Task},
};

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

pub struct Single<T>(T);

impl<T> Single<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self(inner)
    }
}

pub struct Batched<B>(B);

impl<B> Batched<B> {
    pub(crate) fn new(inner: B) -> Self {
        Self(inner)
    }
}

#[async_trait]
pub trait Execute: Send + Sync + 'static {
    type Input: Send + Sync + 'static;
    type Output: Send + Sync + 'static;

    async fn execute(
        &self,
        inputs: &[Self::Input],
    ) -> Result<Vec<Result<Self::Output, Error>>, Error>;
}

#[async_trait]
impl<T> Execute for Single<T>
where
    T: Task + 'static,
    T::Output: 'static,
{
    type Input = ();
    type Output = T::Output;

    async fn execute(&self, _inputs: &[()]) -> Result<Vec<Result<T::Output, Error>>, Error> {
        Ok(vec![
            self.0
                .execute()
                .await
                .map_err(Error::task_execution),
        ])
    }
}

#[async_trait]
impl<B> Execute for Batched<B>
where
    B: BatchTask + 'static,
{
    type Input = B::Input;
    type Output = B::Output;

    async fn execute(&self, inputs: &[B::Input]) -> Result<Vec<Result<B::Output, Error>>, Error> {
        self.0
            .execute(inputs)
            .await
            .map(|results| {
                results
                    .into_iter()
                    .map(|result| result.map_err(Error::task_execution))
                    .collect()
            })
            .map_err(Error::task_execution)
    }
}

pub trait ExecuteStream: Send + Sync + 'static {
    type Input: Send + Sync + 'static;
    type Item: Send + Sync + 'static;

    fn execute<'a>(
        &'a self,
        inputs: &'a [Self::Input],
    ) -> BoxStream<'a, (usize, Result<Self::Item, Error>)>;
}

impl<S> ExecuteStream for Single<S>
where
    S: StreamTask + 'static,
{
    type Input = ();
    type Item = S::Item;

    fn execute<'a>(&'a self, _inputs: &'a [()]) -> BoxStream<'a, (usize, Result<S::Item, Error>)> {
        self.0
            .execute()
            .map(|item| (0, item.map_err(Error::task_execution)))
            .boxed()
    }
}

impl<B> ExecuteStream for Batched<B>
where
    B: BatchStreamTask + 'static,
{
    type Input = B::Input;
    type Item = B::Item;

    fn execute<'a>(
        &'a self,
        inputs: &'a [B::Input],
    ) -> BoxStream<'a, (usize, Result<B::Item, Error>)> {
        self.0
            .execute(inputs)
            .map(|(index, item)| (index, item.map_err(Error::task_execution)))
            .boxed()
    }
}
