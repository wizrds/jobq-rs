use event_listener::Event;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use futures_timeout::TimeoutExt;
use std::{
    future::Future,
    mem::take,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use crate::{
    batch::stream::StreamBatchExecutor,
    error::Error,
    job::{JobDelivery, StreamDelivery},
};

struct WindowState<I, D> {
    inputs: Vec<I>,
    deliveries: Vec<D>,
    sealed: bool,
}

type AdmissionFuture = Shared<BoxFuture<'static, Result<(), Error>>>;

enum AdmissionState<W, O> {
    Pending(Weak<WindowAdmission<W, O>>),
    Accepted,
}

pub(crate) struct WindowAdmission<W, O> {
    window: Arc<W>,
    slot: Weak<WindowSlot<W, O>>,
    future: AdmissionFuture,
}

impl<W, O> WindowAdmission<W, O>
where
    W: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    fn new<F>(window: Arc<W>, slot: Weak<WindowSlot<W, O>>, submission: F) -> Arc<Self>
    where
        F: Future<Output = Result<(), Error>> + Send + 'static,
    {
        let accepted_window = window.clone();
        let accepted_slot = slot.clone();

        Arc::new(Self {
            window,
            slot,
            future: async move {
                let result = submission.await;

                if let Some(slot) = accepted_slot.upgrade() {
                    match &result {
                        Ok(()) => slot.accept(&accepted_window),
                        Err(_) => slot.evict(&accepted_window),
                    }
                }

                result
            }
            .boxed()
            .shared(),
        })
    }

    pub(crate) async fn wait(&self) -> Result<(), Error> {
        self.future.clone().await
    }
}

impl<W, O> Drop for WindowAdmission<W, O> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.upgrade() {
            slot.abandon(&self.window);
        }
    }
}

pub(crate) struct OpenWindow<W, O> {
    window: Arc<W>,
    queue_options: Option<O>,
    admission: AdmissionState<W, O>,
}

impl<W, O> OpenWindow<W, O> {
    fn new(
        window: Arc<W>,
        queue_options: Option<O>,
        admission: &Arc<WindowAdmission<W, O>>,
    ) -> Self {
        Self {
            window,
            queue_options,
            admission: AdmissionState::Pending(Arc::downgrade(admission)),
        }
    }

    fn contains(&self, window: &Arc<W>) -> bool {
        Arc::ptr_eq(&self.window, window)
    }

    fn matches_options(&self, queue_options: &Option<O>) -> bool
    where
        O: PartialEq,
    {
        self.queue_options.as_ref() == queue_options.as_ref()
    }

    fn window(&self) -> &Arc<W> {
        &self.window
    }

    fn is_pending(&self) -> bool {
        matches!(&self.admission, AdmissionState::Pending(_))
    }

    fn admission(&self) -> Option<Arc<WindowAdmission<W, O>>> {
        match &self.admission {
            AdmissionState::Pending(admission) => admission.upgrade(),
            AdmissionState::Accepted => None,
        }
    }

    fn accept(&mut self) {
        self.admission = AdmissionState::Accepted;
    }
}

pub(crate) struct WindowSlot<W, O> {
    open: Mutex<Option<OpenWindow<W, O>>>,
}

impl<W, O> WindowSlot<W, O> {
    pub(crate) fn new() -> Self {
        Self { open: Mutex::new(None) }
    }

    fn accept(&self, window: &Arc<W>) {
        let mut open = self.open.lock().unwrap();

        if let Some(entry) = open.as_mut()
            && entry.contains(window)
        {
            entry.accept();
        }
    }

    fn abandon(&self, window: &Arc<W>) {
        let mut open = self.open.lock().unwrap();

        if open
            .as_ref()
            .is_some_and(|entry| entry.contains(window) && entry.is_pending())
        {
            *open = None;
        }
    }

    pub(crate) fn evict(&self, window: &Arc<W>) {
        let mut open = self.open.lock().unwrap();

        if open
            .as_ref()
            .is_some_and(|entry| entry.contains(window))
        {
            *open = None;
        }
    }
}

impl<X, I, D, O> WindowSlot<Window<X, I, D>, O>
where
    Window<X, I, D>: Send + Sync + 'static,
    O: Clone + PartialEq + Send + Sync + 'static,
{
    pub(crate) fn join<F, Fut>(
        self: &Arc<Self>,
        executor: &Arc<X>,
        policy: BatchPolicy,
        queue_options: Option<O>,
        input: I,
        delivery: D,
        submit: F,
    ) -> Option<Arc<WindowAdmission<Window<X, I, D>, O>>>
    where
        F: FnOnce(Arc<Window<X, I, D>>, Option<O>) -> Fut,
        Fut: Future<Output = Result<(), Error>> + Send + 'static,
    {
        let mut discarded_admission = None;

        let admission = {
            let mut open_slot = self.open.lock().unwrap();

            let (input, delivery) = match open_slot.take() {
                Some(open) if open.matches_options(&queue_options) => {
                    let pending = open.admission();

                    if open.is_pending() && pending.is_none() {
                        open.window().close();

                        (input, delivery)
                    } else {
                        match open.window().push(input, delivery) {
                            Ok(full) => {
                                if !full {
                                    *open_slot = Some(open);
                                }

                                return pending;
                            }
                            Err(member) => {
                                discarded_admission = pending;

                                member
                            }
                        }
                    }
                }
                Some(open) => {
                    open.window().close();

                    (input, delivery)
                }
                None => (input, delivery),
            };

            let window = Arc::new(Window::new(executor.clone(), policy, input, delivery));
            let admission = WindowAdmission::new(
                window.clone(),
                Arc::downgrade(self),
                submit(window.clone(), queue_options.clone()),
            );

            if !window.is_sealed() {
                *open_slot = Some(OpenWindow::new(window, queue_options, &admission));
            }

            admission
        };

        drop(discarded_admission);

        Some(admission)
    }
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

pub(crate) type TaskWindow<X, I, O> = Window<X, I, JobDelivery<O>>;

pub(crate) type StreamWindow<B, M, I, T> = Window<StreamBatchExecutor<B, M>, I, StreamDelivery<T>>;

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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::oneshot;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn pending_joiners_share_one_admission_and_aligned_window() {
        let slot = Arc::new(WindowSlot::<Window<(), usize, usize>, ()>::new());
        let executor = Arc::new(());
        let policy = BatchPolicy { max_size: 3, max_wait: Duration::ZERO };
        let openings = Arc::new(AtomicUsize::new(0));
        let captured = Arc::new(Mutex::new(None));
        let (release, admitted) = oneshot::channel::<()>();
        let first = slot
            .join(&executor, policy, None, 10, 100, {
                let openings = openings.clone();
                let captured = captured.clone();

                move |window, _| {
                    openings.fetch_add(1, Ordering::SeqCst);
                    *captured.lock().unwrap() = Some(window);

                    async move {
                        admitted.await.unwrap();
                        Ok(())
                    }
                }
            })
            .unwrap();
        let second = slot
            .join(&executor, policy, None, 20, 200, |_, _| async {
                panic!("a join must not submit again")
            })
            .unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(openings.load(Ordering::SeqCst), 1);
        assert_eq!(
            captured
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .seal(),
            (vec![10, 20], vec![100, 200]),
        );

        release.send(()).unwrap();
        first.wait().await.unwrap();
        second.wait().await.unwrap();
    }

    #[tokio::test]
    async fn one_waiter_cancel_does_not_cancel_sibling() {
        let slot = Arc::new(WindowSlot::<Window<(), usize, usize>, ()>::new());
        let executor = Arc::new(());
        let policy = BatchPolicy { max_size: 3, max_wait: Duration::ZERO };
        let (release, admitted) = oneshot::channel::<()>();
        let first = slot
            .join(&executor, policy, None, 1, 10, |_, _| async move {
                admitted.await.unwrap();
                Ok(())
            })
            .unwrap();
        let second = slot
            .join(&executor, policy, None, 2, 20, |_, _| async {
                panic!("a join must not submit again")
            })
            .unwrap();

        drop(first);
        release.send(()).unwrap();
        second.wait().await.unwrap();
    }

    #[tokio::test]
    async fn final_waiter_drop_abandons_pending_entry() {
        let slot = Arc::new(WindowSlot::<Window<(), usize, usize>, ()>::new());
        let executor = Arc::new(());
        let policy = BatchPolicy { max_size: 3, max_wait: Duration::ZERO };
        let openings = Arc::new(AtomicUsize::new(0));
        let first = slot
            .join(&executor, policy, None, 1, 10, {
                let openings = openings.clone();

                move |_, _| {
                    openings.fetch_add(1, Ordering::SeqCst);
                    futures::future::pending::<Result<(), Error>>()
                }
            })
            .unwrap();
        let second = slot
            .join(&executor, policy, None, 2, 20, |_, _| async {
                panic!("a join must not submit again")
            })
            .unwrap();

        drop(first);
        drop(second);

        slot.join(&executor, policy, None, 3, 30, {
            let openings = openings.clone();

            move |_, _| {
                openings.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }
        })
        .unwrap()
        .wait()
        .await
        .unwrap();

        assert_eq!(openings.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn accepted_window_needs_no_completed_admission() {
        let slot = Arc::new(WindowSlot::<Window<(), usize, usize>, ()>::new());
        let executor = Arc::new(());
        let policy = BatchPolicy { max_size: 3, max_wait: Duration::ZERO };
        let openings = Arc::new(AtomicUsize::new(0));
        let first = slot
            .join(&executor, policy, None, 1, 10, {
                let openings = openings.clone();

                move |_, _| {
                    openings.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                }
            })
            .unwrap();
        first.wait().await.unwrap();
        drop(first);

        assert!(
            slot.join(&executor, policy, None, 2, 20, |_, _| async {
                panic!("an accepted join must not submit again")
            })
            .is_none()
        );
        assert_eq!(openings.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rejection_reaches_all_waiters_without_evicting_replacement() {
        let slot = Arc::new(WindowSlot::<Window<(), usize, usize>, u8>::new());
        let executor = Arc::new(());
        let policy = BatchPolicy { max_size: 3, max_wait: Duration::ZERO };
        let (release, rejected) = oneshot::channel::<()>();
        let first = slot
            .join(&executor, policy, Some(1), 1, 10, |_, _| async move {
                rejected.await.unwrap();
                Err(Error::queue(crate::queue::error::Error::closed()))
            })
            .unwrap();
        let second = slot
            .join(&executor, policy, Some(1), 2, 20, |_, _| async {
                panic!("a join must not submit again")
            })
            .unwrap();
        let replacement = slot
            .join(&executor, policy, Some(2), 3, 30, |_, _| async { Ok(()) })
            .unwrap();

        release.send(()).unwrap();
        assert!(matches!(first.wait().await, Err(Error::Queue(_))));
        assert!(matches!(second.wait().await, Err(Error::Queue(_))));
        assert!(
            slot.join(&executor, policy, Some(2), 4, 40, |_, _| async {
                panic!("W1 rejection must not evict W2")
            })
            .is_some()
        );
        replacement.wait().await.unwrap();
    }

    #[tokio::test]
    async fn full_window_keeps_admission_after_replacement() {
        let slot = Arc::new(WindowSlot::<Window<(), usize, usize>, ()>::new());
        let executor = Arc::new(());
        let policy = BatchPolicy { max_size: 2, max_wait: Duration::ZERO };
        let first = slot
            .join(&executor, policy, None, 1, 10, |_, _| async { Ok(()) })
            .unwrap();
        let second = slot
            .join(&executor, policy, None, 2, 20, |_, _| async {
                panic!("a join must not submit again")
            })
            .unwrap();
        let third = slot
            .join(&executor, policy, None, 3, 30, |_, _| async { Ok(()) })
            .unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &third));
        first.wait().await.unwrap();
        second.wait().await.unwrap();
        third.wait().await.unwrap();
    }
}
