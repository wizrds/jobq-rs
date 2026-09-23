use event_listener::Event;
use futures_timeout::TimeoutExt;
use std::{
    mem::take,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use crate::{
    error::Error,
    executable::{AnyExecutable, Batched},
    future::{JobFuture, JobStream, JobStreamHandle},
    job::{Job, JobDelivery, JobOptions, JobQueue, JobStreamOptions, StreamDelivery, StreamJob},
    queue::traits::Queue,
    task::{BatchStreamTask, BatchTask},
};

struct WindowState<I, D> {
    inputs: Vec<I>,
    deliveries: Vec<D>,
    sealed: bool,
}

pub(crate) struct OpenWindow<W, O> {
    window: Arc<W>,
    queue_options: Option<O>,
}

pub(crate) struct Window<X, I, D> {
    executor: Arc<X>,
    policy: BatchPolicy,
    opened_at: Instant,
    state: Mutex<WindowState<I, D>>,
    full: Event,
}

impl<X, I, D> Window<X, I, D> {
    pub(crate) fn new(executor: Arc<X>, policy: BatchPolicy, input: I, delivery: D) -> Self {
        let mut inputs = Vec::with_capacity(policy.max_size);
        let mut deliveries = Vec::with_capacity(policy.max_size);

        inputs.push(input);
        deliveries.push(delivery);

        Self {
            executor,
            policy,
            opened_at: Instant::now(),
            state: Mutex::new(WindowState {
                inputs,
                deliveries,
                sealed: policy.max_size <= 1,
            }),
            full: Event::new(),
        }
    }

    fn is_sealed(&self) -> bool {
        self.state.lock().unwrap().sealed
    }

    fn push(&self, input: I, delivery: D) -> Result<bool, (I, D)> {
        let mut state = self.state.lock().unwrap();

        if state.sealed {
            return Err((input, delivery));
        }

        state.inputs.push(input);
        state.deliveries.push(delivery);

        if state.inputs.len() >= self.policy.max_size {
            state.sealed = true;
            self.full.notify(usize::MAX);
        }

        Ok(state.sealed)
    }

    fn close(&self) {
        self.state.lock().unwrap().sealed = true;

        self.full.notify(usize::MAX);
    }

    pub(crate) fn join<O>(
        slot: &Mutex<Option<OpenWindow<Self, O>>>,
        executor: &Arc<X>,
        policy: BatchPolicy,
        queue_options: Option<O>,
        input: I,
        delivery: D,
    ) -> Option<(Arc<Self>, Option<O>)>
    where
        O: Clone + PartialEq,
    {
        let mut slot = slot.lock().unwrap();

        let (input, delivery) = match slot.take() {
            Some(open) if open.queue_options == queue_options => {
                match open.window.push(input, delivery) {
                    Ok(full) => {
                        if !full {
                            *slot = Some(open);
                        }

                        return None;
                    }
                    Err(member) => member,
                }
            }
            Some(open) => {
                open.window.close();

                (input, delivery)
            }
            None => (input, delivery),
        };

        let window = Arc::new(Self::new(executor.clone(), policy, input, delivery));

        if !window.is_sealed() {
            *slot = Some(OpenWindow {
                window: window.clone(),
                queue_options: queue_options.clone(),
            });
        }

        Some((window, queue_options))
    }

    pub(crate) fn evict<O>(slot: &Mutex<Option<OpenWindow<Self, O>>>, window: &Arc<Self>) {
        let mut slot = slot.lock().unwrap();

        if slot
            .as_ref()
            .is_some_and(|open| Arc::ptr_eq(&open.window, window))
        {
            *slot = None;
        }
    }

    pub(crate) fn executor(&self) -> &X {
        &self.executor
    }

    pub(crate) async fn ready(&self) {
        let listener = self.full.listen();

        if self.is_sealed() {
            return;
        }

        let _ = listener
            .timeout(
                self.policy
                    .max_wait
                    .saturating_sub(self.opened_at.elapsed()),
            )
            .await;
    }

    pub(crate) fn seal(&self) -> (Vec<I>, Vec<D>) {
        let mut state = self.state.lock().unwrap();

        state.sealed = true;

        (take(&mut state.inputs), take(&mut state.deliveries))
    }
}

struct BatcherInner<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    executor: Arc<Batched<B>>,
    queue: JobQueue<Q>,
    policy: BatchPolicy,
    window:
        Mutex<Option<OpenWindow<Window<Batched<B>, B::Input, JobDelivery<B::Output>>, Q::Options>>>,
}

struct StreamBatcherInner<B, Q>
where
    B: BatchStreamTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    executor: Arc<Batched<B>>,
    queue: JobQueue<Q>,
    policy: BatchPolicy,
    window: Mutex<
        Option<OpenWindow<Window<Batched<B>, B::Input, StreamDelivery<B::Item>>, Q::Options>>,
    >,
}

#[derive(Debug, Clone, Copy)]
pub struct BatchPolicy {
    pub max_size: usize,
    pub max_wait: Duration,
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self { max_size: 1, max_wait: Duration::ZERO }
    }
}

pub struct Batcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    inner: Arc<BatcherInner<B, Q>>,
}

impl<B, Q> Clone for Batcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<B, Q> Batcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq>,
{
    pub(crate) fn new(queue: JobQueue<Q>, task: B, policy: BatchPolicy) -> Self {
        Self {
            inner: Arc::new(BatcherInner {
                executor: Arc::new(Batched::new(task)),
                queue,
                policy,
                window: Mutex::new(None),
            }),
        }
    }

    pub fn downgrade(&self) -> WeakBatcher<B, Q> {
        WeakBatcher { inner: Arc::downgrade(&self.inner) }
    }

    pub async fn enqueue(
        &self,
        options: JobOptions<B::Input, Q>,
    ) -> Result<JobFuture<B::Output>, Error> {
        let (input, max_retries, queue_options) = options.into_parts();
        let (future, setter) = JobFuture::new();

        if let Some((window, queue_options)) = Window::join(
            &self.inner.window,
            &self.inner.executor,
            self.inner.policy,
            queue_options,
            input,
            JobDelivery::new(setter, max_retries),
        ) {
            if let Err(error) = self
                .inner
                .queue
                .enqueue(AnyExecutable::new(Job::from_window(window.clone())), queue_options)
                .await
            {
                Window::evict(&self.inner.window, &window);

                return Err(error);
            }
        }

        Ok(future)
    }
}

pub struct WeakBatcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    inner: Weak<BatcherInner<B, Q>>,
}

impl<B, Q> Clone for WeakBatcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<B, Q> WeakBatcher<B, Q>
where
    B: BatchTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    pub fn upgrade(&self) -> Option<Batcher<B, Q>> {
        self.inner
            .upgrade()
            .map(|inner| Batcher { inner })
    }
}

pub struct StreamBatcher<B, Q>
where
    B: BatchStreamTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    inner: Arc<StreamBatcherInner<B, Q>>,
}

impl<B, Q> Clone for StreamBatcher<B, Q>
where
    B: BatchStreamTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<B, Q> StreamBatcher<B, Q>
where
    B: BatchStreamTask + 'static,
    Q: Queue<Item = AnyExecutable, Options: Clone + PartialEq>,
{
    pub(crate) fn new(queue: JobQueue<Q>, task: B, policy: BatchPolicy) -> Self {
        Self {
            inner: Arc::new(StreamBatcherInner {
                executor: Arc::new(Batched::new(task)),
                queue,
                policy,
                window: Mutex::new(None),
            }),
        }
    }

    pub fn downgrade(&self) -> WeakStreamBatcher<B, Q> {
        WeakStreamBatcher { inner: Arc::downgrade(&self.inner) }
    }

    pub async fn enqueue(
        &self,
        options: JobStreamOptions<B::Input, Q>,
    ) -> Result<JobStreamHandle<B::Item>, Error> {
        let (input, capacity, queue_options) = options.into_parts();
        let (items, item_setter) = JobStream::new(capacity);
        let (completion, completion_setter) = JobFuture::new();

        if let Some((window, queue_options)) = Window::join(
            &self.inner.window,
            &self.inner.executor,
            self.inner.policy,
            queue_options,
            input,
            StreamDelivery::new(item_setter, completion_setter),
        ) {
            if let Err(error) = self
                .inner
                .queue
                .enqueue(AnyExecutable::new(StreamJob::from_window(window.clone())), queue_options)
                .await
            {
                Window::evict(&self.inner.window, &window);

                return Err(error);
            }
        }

        Ok(JobStreamHandle::new(items, completion))
    }
}

pub struct WeakStreamBatcher<B, Q>
where
    B: BatchStreamTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    inner: Weak<StreamBatcherInner<B, Q>>,
}

impl<B, Q> Clone for WeakStreamBatcher<B, Q>
where
    B: BatchStreamTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<B, Q> WeakStreamBatcher<B, Q>
where
    B: BatchStreamTask + 'static,
    Q: Queue<Item = AnyExecutable>,
{
    pub fn upgrade(&self) -> Option<StreamBatcher<B, Q>> {
        self.inner
            .upgrade()
            .map(|inner| StreamBatcher { inner })
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::convert::Infallible;

    use super::*;
    use crate::{builder::JobQueueSystemBuilder, queue::priority::PriorityOptions};

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

    #[tokio::test]
    async fn batch_invokes_executor_once_with_every_input() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let calls = Arc::new(Mutex::new(Vec::new()));
        let batcher = queue.batcher(
            DoubleBatch { calls: calls.clone() },
            BatchPolicy {
                max_size: 3,
                max_wait: Duration::from_secs(60),
            },
        );
        let mut futures = Vec::new();

        for input in [1, 2, 3] {
            futures.push(
                batcher
                    .enqueue(JobOptions::new(input))
                    .await
                    .unwrap(),
            );
        }

        for (input, future) in [1, 2, 3].into_iter().zip(futures) {
            assert_eq!(future.result().await.unwrap(), input * 2);
        }

        assert_eq!(*calls.lock().unwrap(), vec![vec![1, 2, 3]]);

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn max_size_seals_and_max_wait_releases_partial_window() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let calls = Arc::new(Mutex::new(Vec::new()));
        let batcher = queue.batcher(
            DoubleBatch { calls: calls.clone() },
            BatchPolicy {
                max_size: 2,
                max_wait: Duration::from_millis(20),
            },
        );
        let mut futures = Vec::new();

        for input in [1, 2, 3] {
            futures.push(
                batcher
                    .enqueue(JobOptions::new(input))
                    .await
                    .unwrap(),
            );
        }

        for future in futures {
            future.result().await.unwrap();
        }

        assert_eq!(*calls.lock().unwrap(), vec![vec![1, 2], vec![3]]);

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn queue_options_partition_windows() {
        let (queue, worker_pool) = JobQueueSystemBuilder::priority(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let calls = Arc::new(Mutex::new(Vec::new()));
        let batcher = queue.batcher(
            DoubleBatch { calls: calls.clone() },
            BatchPolicy {
                max_size: 2,
                max_wait: Duration::from_secs(60),
            },
        );
        let mut futures = Vec::new();

        for (input, priority) in [(1, 1), (2, 9), (3, 9)] {
            futures.push(
                batcher
                    .enqueue(
                        JobOptions::new(input).with_queue_options(PriorityOptions { priority }),
                    )
                    .await
                    .unwrap(),
            );
        }

        for future in futures {
            future.result().await.unwrap();
        }

        let mut calls = calls.lock().unwrap().clone();

        calls.sort();

        assert_eq!(calls, vec![vec![1], vec![2, 3]]);

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn member_after_seal_opens_new_window() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let calls = Arc::new(Mutex::new(Vec::new()));
        let batcher = queue.batcher(
            DoubleBatch { calls: calls.clone() },
            BatchPolicy {
                max_size: 10,
                max_wait: Duration::from_millis(20),
            },
        );

        for input in [1, 2] {
            assert_eq!(
                batcher
                    .enqueue(JobOptions::new(input))
                    .await
                    .unwrap()
                    .result()
                    .await
                    .unwrap(),
                input * 2
            );
        }

        assert_eq!(*calls.lock().unwrap(), vec![vec![1], vec![2]]);

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }

    #[tokio::test]
    async fn batcher_clones_join_the_same_window() {
        let (queue, worker_pool) = JobQueueSystemBuilder::fifo(10)
            .with_num_workers(1)
            .build();

        let run_handle = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move { worker_pool.run().await }
        });

        let calls = Arc::new(Mutex::new(Vec::new()));
        let batcher = queue.batcher(
            DoubleBatch { calls: calls.clone() },
            BatchPolicy {
                max_size: 2,
                max_wait: Duration::from_secs(60),
            },
        );
        let first = batcher
            .enqueue(JobOptions::new(1))
            .await
            .unwrap();
        let second = batcher
            .clone()
            .enqueue(JobOptions::new(2))
            .await
            .unwrap();

        assert_eq!(first.result().await.unwrap(), 2);
        assert_eq!(second.result().await.unwrap(), 4);
        assert_eq!(*calls.lock().unwrap(), vec![vec![1, 2]]);

        worker_pool.shutdown().await;
        run_handle.await.unwrap();
    }
}
