use std::sync::{Arc, Weak};

use crate::{
    batch::{
        stream::{member::StreamBatchContext, mode::StreamBatchMode},
        window::{BatchPolicy, StreamWindow, WindowSlot},
    },
    error::Error,
    executable::AnyExecutable,
    future::JobStreamHandle,
    job::{JobQueue, JobStreamOptions, StreamBatchJob, StreamDelivery},
    queue::traits::Queue,
};

struct StreamBatcherInner<B, Q, M>
where
    B: Send + Sync + 'static,
    M: StreamBatchMode<B>,
    Q: Queue<Item = AnyExecutable> + 'static,
{
    executor: Arc<StreamBatchExecutor<B, M>>,
    queue: JobQueue<Q>,
    policy: BatchPolicy,
    window: Arc<WindowSlot<StreamWindow<B, M, M::Input, M::Item>, Q::Options>>,
}

impl<B, Q, M> StreamBatcherInner<B, Q, M>
where
    B: Send + Sync + 'static,
    M: StreamBatchMode<B>,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
{
    fn new(queue: JobQueue<Q>, task: B, mode: M, policy: BatchPolicy) -> Self {
        Self {
            executor: Arc::new(StreamBatchExecutor::new(task, mode)),
            queue,
            policy,
            window: Arc::new(WindowSlot::new()),
        }
    }

    async fn enqueue(
        &self,
        options: JobStreamOptions<M::Input, Q>,
    ) -> Result<JobStreamHandle<M::Item>, Error> {
        let (input, capacity, queue_options) = options.into_parts();
        let (handle, setter) = JobStreamHandle::new(capacity, Some(Error::consumer_cancelled()))?;

        if let Some(admission) = self.window.join(
            &self.executor,
            self.policy,
            queue_options,
            input,
            StreamDelivery::new(setter),
            |window, options| {
                let queue = self.queue.clone();

                async move {
                    queue
                        .enqueue(AnyExecutable::new(StreamBatchJob::from_window(window)), options)
                        .await
                }
            },
        ) {
            admission.wait().await?;
        }

        Ok(handle)
    }
}

pub struct StreamBatchExecutor<B, M> {
    task: B,
    mode: M,
}

impl<B, M> StreamBatchExecutor<B, M>
where
    B: Send + Sync + 'static,
    M: StreamBatchMode<B>,
{
    pub fn new(task: B, mode: M) -> Self {
        Self { task, mode }
    }

    pub async fn run(
        &self,
        inputs: &[M::Input],
        context: StreamBatchContext<M::Item>,
    ) -> crate::job::JobStatus {
        self.mode
            .run(&self.task, inputs, context)
            .await
    }
}

pub struct StreamBatcherBuilder<B, Q, M>
where
    B: Send + Sync + 'static,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
{
    queue: JobQueue<Q>,
    task: B,
    policy: BatchPolicy,
    mode: M,
}

impl<B, Q> StreamBatcherBuilder<B, Q, ()>
where
    B: Send + Sync + 'static,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
{
    pub fn new(queue: JobQueue<Q>, task: B) -> Self {
        Self {
            queue,
            task,
            policy: BatchPolicy::default(),
            mode: (),
        }
    }
}

impl<B, Q, M> StreamBatcherBuilder<B, Q, M>
where
    B: Send + Sync + 'static,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
{
    pub fn policy(mut self, policy: BatchPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn mode<N>(self, mode: N) -> StreamBatcherBuilder<B, Q, N>
    where
        N: StreamBatchMode<B>,
    {
        StreamBatcherBuilder {
            queue: self.queue,
            task: self.task,
            policy: self.policy,
            mode,
        }
    }
}

impl<B, Q, M> StreamBatcherBuilder<B, Q, M>
where
    B: Send + Sync + 'static,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
    M: StreamBatchMode<B>,
{
    pub fn build(self) -> StreamBatcher<B, Q, M> {
        StreamBatcher::new(self.queue, self.task, self.mode, self.policy)
    }
}

pub struct StreamBatcher<B, Q, M>
where
    B: Send + Sync + 'static,
    M: StreamBatchMode<B>,
    Q: Queue<Item = AnyExecutable> + 'static,
{
    inner: Arc<StreamBatcherInner<B, Q, M>>,
}

impl<B, Q, M> Clone for StreamBatcher<B, Q, M>
where
    B: Send + Sync + 'static,
    M: StreamBatchMode<B>,
    Q: Queue<Item = AnyExecutable> + 'static,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<B, Q, M> StreamBatcher<B, Q, M>
where
    B: Send + Sync + 'static,
    M: StreamBatchMode<B>,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
{
    pub(crate) fn new(queue: JobQueue<Q>, task: B, mode: M, policy: BatchPolicy) -> Self {
        Self {
            inner: Arc::new(StreamBatcherInner::new(queue, task, mode, policy)),
        }
    }

    fn from_inner(inner: Arc<StreamBatcherInner<B, Q, M>>) -> Self {
        Self { inner }
    }

    pub fn downgrade(&self) -> WeakStreamBatcher<B, Q, M> {
        WeakStreamBatcher::new(Arc::downgrade(&self.inner))
    }

    pub async fn enqueue(
        &self,
        options: JobStreamOptions<M::Input, Q>,
    ) -> Result<JobStreamHandle<M::Item>, Error> {
        self.inner.enqueue(options).await
    }
}

pub struct WeakStreamBatcher<B, Q, M>
where
    B: Send + Sync + 'static,
    M: StreamBatchMode<B>,
    Q: Queue<Item = AnyExecutable> + 'static,
{
    inner: Weak<StreamBatcherInner<B, Q, M>>,
}

impl<B, Q, M> Clone for WeakStreamBatcher<B, Q, M>
where
    B: Send + Sync + 'static,
    M: StreamBatchMode<B>,
    Q: Queue<Item = AnyExecutable> + 'static,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<B, Q, M> WeakStreamBatcher<B, Q, M>
where
    B: Send + Sync + 'static,
    M: StreamBatchMode<B>,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq> + 'static,
{
    fn new(inner: Weak<StreamBatcherInner<B, Q, M>>) -> Self {
        Self { inner }
    }

    pub fn upgrade(&self) -> Option<StreamBatcher<B, Q, M>> {
        self.inner
            .upgrade()
            .map(StreamBatcher::from_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{executable::Executable, job::JobStatus, queue::fifo::FifoQueue};
    use async_trait::async_trait;
    use futures::{FutureExt, StreamExt, channel::oneshot};
    use std::{sync::Mutex, time::Duration};

    struct Finishes;

    #[async_trait]
    impl StreamBatchMode<()> for Finishes {
        type Input = u8;
        type Item = u8;

        async fn run(
            &self,
            _task: &(),
            inputs: &[Self::Input],
            context: StreamBatchContext<Self::Item>,
        ) -> JobStatus {
            let mut members = context.into_members();

            for (input, member) in inputs.iter().zip(&mut members) {
                if member.is_open() {
                    member.send(Ok(*input)).await.unwrap();
                    member.finish(Ok(()));
                } else {
                    member.finish(Err(Error::consumer_cancelled()));
                }
            }

            JobStatus::Completed
        }
    }

    struct Incomplete;

    #[async_trait]
    impl StreamBatchMode<()> for Incomplete {
        type Input = u8;
        type Item = u8;

        async fn run(
            &self,
            _task: &(),
            _inputs: &[Self::Input],
            _context: StreamBatchContext<Self::Item>,
        ) -> JobStatus {
            JobStatus::Completed
        }
    }

    struct GateQueue {
        queue: FifoQueue<AnyExecutable>,
        gate: Mutex<Option<oneshot::Receiver<()>>>,
    }

    impl GateQueue {
        fn new(gate: oneshot::Receiver<()>) -> Self {
            Self {
                queue: FifoQueue::new(10),
                gate: Mutex::new(Some(gate)),
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
    async fn selected_external_mode_delivers_one_result_per_member() {
        let queue = JobQueue::new(FifoQueue::<AnyExecutable>::new(10));
        let batcher = queue
            .stream_batcher(())
            .mode(Finishes)
            .policy(BatchPolicy { max_size: 2, max_wait: Duration::ZERO })
            .build();
        let first = batcher
            .enqueue(JobStreamOptions::new(1))
            .await
            .unwrap();
        let second = batcher
            .enqueue(JobStreamOptions::new(2))
            .await
            .unwrap();
        let mut job = queue
            .dequeue_job()
            .await
            .unwrap()
            .unwrap();

        job.execute().await;

        let mut first = first;
        let mut second = second;
        assert!(matches!(first.next().await, Some(Ok(1))));
        assert!(matches!(second.next().await, Some(Ok(2))));
        assert!(first.result().await.is_ok());
        assert!(second.result().await.is_ok());
    }

    #[tokio::test]
    async fn incomplete_external_mode_gets_explicit_terminal_error() {
        let queue = JobQueue::new(FifoQueue::<AnyExecutable>::new(10));
        let batcher = queue
            .stream_batcher(())
            .mode(Incomplete)
            .build();
        let handle = batcher
            .enqueue(JobStreamOptions::new(1))
            .await
            .unwrap();
        let mut job = queue
            .dequeue_job()
            .await
            .unwrap()
            .unwrap();

        job.execute().await;

        assert!(matches!(job.status(), JobStatus::Failed));
        assert!(matches!(handle.result().await, Err(Error::IncompleteStreamBatch)));
    }

    #[tokio::test]
    async fn cancelled_opener_does_not_cancel_joined_stream_member() {
        let (release, gate) = oneshot::channel();
        let queue = JobQueue::new(GateQueue::new(gate));
        let batcher = queue
            .stream_batcher(())
            .policy(BatchPolicy { max_size: 2, max_wait: Duration::ZERO })
            .mode(Finishes)
            .build();
        let mut first = Box::pin(batcher.enqueue(JobStreamOptions::new(1)));
        let mut second = Box::pin(batcher.enqueue(JobStreamOptions::new(2)));

        assert!(first.as_mut().now_or_never().is_none());
        assert!(second.as_mut().now_or_never().is_none());
        drop(first);
        release.send(()).unwrap();

        let mut handle = second.await.unwrap();
        let mut job = queue
            .dequeue_job()
            .await
            .unwrap()
            .unwrap();
        job.execute().await;

        assert!(matches!(handle.next().await, Some(Ok(2))));
        assert!(handle.result().await.is_ok());
    }

    #[tokio::test]
    async fn invalid_capacity_rejects_before_queue_submission() {
        let queue = JobQueue::new(FifoQueue::<AnyExecutable>::new(10));
        let batcher = queue
            .stream_batcher(())
            .mode(Finishes)
            .build();

        assert!(matches!(
            batcher
                .enqueue(JobStreamOptions::new(1).with_capacity(0))
                .await,
            Err(Error::InvalidStreamCapacity(0))
        ));
        assert_eq!(queue.len().await, 0);
    }
}
