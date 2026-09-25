use async_trait::async_trait;
use std::sync::{Arc, Weak};

use crate::{
    batch::window::{BatchPolicy, TaskWindow, WindowSlot},
    error::Error,
    executable::{AnyExecutable, Batched},
    future::JobFuture,
    job::{Job, JobDelivery, JobOptions, JobQueue},
    queue::traits::Queue,
};

struct BatcherInner<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable> + 'static,
{
    executor: Arc<Batched<B>>,
    queue: JobQueue<Q>,
    policy: BatchPolicy,
    window: Arc<WindowSlot<TaskWindow<Batched<B>, B::Input, B::Output>, Q::Options>>,
}

impl<B, Q> BatcherInner<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
{
    fn new(queue: JobQueue<Q>, task: B, policy: BatchPolicy) -> Self {
        Self {
            executor: Arc::new(Batched::new(task)),
            queue,
            policy,
            window: Arc::new(WindowSlot::new()),
        }
    }

    async fn enqueue(
        &self,
        options: JobOptions<B::Input, Q>,
    ) -> Result<JobFuture<B::Output>, Error> {
        let (input, max_retries, queue_options) = options.into_parts();
        let (future, setter) = JobFuture::new();

        if let Some(admission) = self.window.join(
            &self.executor,
            self.policy,
            queue_options,
            input,
            JobDelivery::new(setter, max_retries),
            |window, options| {
                let queue = self.queue.clone();

                async move {
                    queue
                        .enqueue(AnyExecutable::new(Job::from_window(window)), options)
                        .await
                }
            },
        ) {
            admission.wait().await?;
        }

        Ok(future)
    }
}

#[async_trait]
pub trait BatchTask: Send + Sync {
    type Input: Send + Sync + 'static;
    type Output: Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn execute(
        &self,
        inputs: &[Self::Input],
    ) -> Result<Vec<Result<Self::Output, Self::Error>>, Self::Error>;
}

pub struct BatcherBuilder<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
{
    queue: JobQueue<Q>,
    task: B,
    policy: BatchPolicy,
}

impl<B, Q> BatcherBuilder<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
{
    pub fn new(queue: JobQueue<Q>, task: B) -> Self {
        Self {
            queue,
            task,
            policy: BatchPolicy::default(),
        }
    }

    pub fn policy(mut self, policy: BatchPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn build(self) -> Batcher<B, Q> {
        Batcher::new(self.queue, self.task, self.policy)
    }
}

pub struct Batcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable> + 'static,
{
    inner: Arc<BatcherInner<B, Q>>,
}

impl<B, Q> Clone for Batcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable> + 'static,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<B, Q> Batcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
{
    pub(crate) fn new(queue: JobQueue<Q>, task: B, policy: BatchPolicy) -> Self {
        Self {
            inner: Arc::new(BatcherInner::new(queue, task, policy)),
        }
    }

    fn from_inner(inner: Arc<BatcherInner<B, Q>>) -> Self {
        Self { inner }
    }

    pub fn downgrade(&self) -> WeakBatcher<B, Q> {
        WeakBatcher::new(Arc::downgrade(&self.inner))
    }

    pub async fn enqueue(
        &self,
        options: JobOptions<B::Input, Q>,
    ) -> Result<JobFuture<B::Output>, Error> {
        self.inner.enqueue(options).await
    }
}

pub struct WeakBatcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable> + 'static,
{
    inner: Weak<BatcherInner<B, Q>>,
}

impl<B, Q> Clone for WeakBatcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable> + 'static,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<B, Q> WeakBatcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
{
    fn new(inner: Weak<BatcherInner<B, Q>>) -> Self {
        Self { inner }
    }

    pub fn upgrade(&self) -> Option<Batcher<B, Q>> {
        self.inner
            .upgrade()
            .map(Batcher::from_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{executable::Executable, queue::fifo::FifoQueue};
    use futures::{FutureExt, channel::oneshot};
    use std::{convert::Infallible, sync::Mutex, time::Duration};

    struct DoubleBatch {
        calls: Arc<Mutex<Vec<Vec<u32>>>>,
    }

    #[async_trait]
    impl BatchTask for DoubleBatch {
        type Input = u32;
        type Output = u32;
        type Error = Infallible;

        async fn execute(
            &self,
            inputs: &[Self::Input],
        ) -> Result<Vec<Result<Self::Output, Self::Error>>, Self::Error> {
            self.calls
                .lock()
                .unwrap()
                .push(inputs.to_vec());

            Ok(inputs
                .iter()
                .map(|input| Ok(input * 2))
                .collect())
        }
    }

    struct GateQueue {
        queue: FifoQueue<AnyExecutable>,
        gate: Mutex<Option<oneshot::Receiver<()>>>,
        reject: bool,
    }

    impl GateQueue {
        fn new(gate: oneshot::Receiver<()>, reject: bool) -> Self {
            Self {
                queue: FifoQueue::new(10),
                gate: Mutex::new(Some(gate)),
                reject,
            }
        }
    }

    #[async_trait]
    impl Queue for GateQueue {
        type Item = AnyExecutable;
        type Options = ();

        async fn enqueue(
            &self,
            item: Self::Item,
            options: Option<Self::Options>,
        ) -> Result<(), crate::queue::error::Error> {
            let gate = self.gate.lock().unwrap().take();

            if let Some(gate) = gate {
                gate.await.unwrap();
            }

            if self.reject {
                return Err(crate::queue::error::Error::closed());
            }

            self.queue.enqueue(item, options).await
        }

        async fn dequeue(&self) -> Result<Option<Self::Item>, crate::queue::error::Error> {
            self.queue.dequeue().await
        }

        async fn len(&self) -> usize {
            self.queue.len().await
        }

        async fn close(&self) -> Result<(), crate::queue::error::Error> {
            self.queue.close().await
        }
    }

    #[tokio::test]
    async fn builder_defaults_to_single_member_windows() {
        let queue = JobQueue::new(FifoQueue::<AnyExecutable>::new(10));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let batcher = queue
            .batcher(DoubleBatch { calls: calls.clone() })
            .build();
        let result = batcher
            .enqueue(JobOptions::new(4))
            .await
            .unwrap();
        let mut job = queue
            .dequeue_job()
            .await
            .unwrap()
            .unwrap();

        job.execute().await;

        assert_eq!(result.result().await.unwrap(), 8);
        assert_eq!(*calls.lock().unwrap(), vec![vec![4]]);
    }

    #[tokio::test]
    async fn cancelled_opener_does_not_cancel_joiner() {
        let (release, gate) = oneshot::channel();
        let queue = JobQueue::new(GateQueue::new(gate, false));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let batcher = queue
            .batcher(DoubleBatch { calls: calls.clone() })
            .policy(BatchPolicy { max_size: 2, max_wait: Duration::ZERO })
            .build();
        let mut first = Box::pin(batcher.enqueue(JobOptions::new(1)));
        let mut second = Box::pin(batcher.enqueue(JobOptions::new(2)));

        assert!(first.as_mut().now_or_never().is_none());
        assert!(second.as_mut().now_or_never().is_none());
        drop(first);
        release.send(()).unwrap();

        let result = second.await.unwrap();
        let mut job = queue
            .dequeue_job()
            .await
            .unwrap()
            .unwrap();
        job.execute().await;

        assert_eq!(result.result().await.unwrap(), 4);
        assert_eq!(*calls.lock().unwrap(), vec![vec![1, 2]]);
    }

    #[tokio::test]
    async fn queue_rejection_reaches_both_pending_callers_directly() {
        let (release, gate) = oneshot::channel();
        let queue = JobQueue::new(GateQueue::new(gate, true));
        let batcher = queue
            .batcher(DoubleBatch { calls: Arc::new(Mutex::new(Vec::new())) })
            .policy(BatchPolicy { max_size: 2, max_wait: Duration::ZERO })
            .build();
        let mut first = Box::pin(batcher.enqueue(JobOptions::new(1)));
        let mut second = Box::pin(batcher.enqueue(JobOptions::new(2)));

        assert!(first.as_mut().now_or_never().is_none());
        assert!(second.as_mut().now_or_never().is_none());
        release.send(()).unwrap();

        assert!(matches!(first.await, Err(Error::Queue(_))));
        assert!(matches!(second.await, Err(Error::Queue(_))));
    }

    #[test]
    fn weak_batcher_tracks_strong_owners() {
        let queue = JobQueue::new(FifoQueue::<AnyExecutable>::new(10));
        let batcher = queue
            .batcher(DoubleBatch { calls: Arc::new(Mutex::new(Vec::new())) })
            .build();
        let weak = batcher.downgrade();
        let clone = batcher.clone();

        drop(batcher);
        assert!(weak.upgrade().is_some());
        drop(clone);
        assert!(weak.upgrade().is_none());
    }
}
