use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    task::Poll,
};

use async_trait::async_trait;
use futures::{
    FutureExt, StreamExt, future::poll_fn, pin_mut, select_biased, stream::FuturesUnordered,
};

use crate::{
    batch::stream::{
        member::{MemberSendError, StreamBatchContext, StreamBatchMember},
        traits::{BatchStreamEvent, IndependentBatchStreamTask, MultiplexedBatchStreamTask},
    },
    error::Error,
    job::JobStatus,
};

#[async_trait]
pub trait StreamBatchMode<B>: Send + Sync + 'static
where
    B: Send + Sync + 'static,
{
    type Input: Send + Sync + 'static;
    type Item: Send + Sync + 'static;

    async fn run(
        &self,
        task: &B,
        inputs: &[Self::Input],
        context: StreamBatchContext<Self::Item>,
    ) -> JobStatus;
}

pub struct Multiplexed;

#[async_trait]
impl<B> StreamBatchMode<B> for Multiplexed
where
    B: MultiplexedBatchStreamTask + 'static,
{
    type Input = B::Input;
    type Item = B::Item;

    async fn run(
        &self,
        task: &B,
        inputs: &[Self::Input],
        context: StreamBatchContext<Self::Item>,
    ) -> JobStatus {
        const ROUTE_BUDGET: usize = 64;

        let activity = context.activity();
        let mut members = context.into_members();

        if members.is_empty() {
            return JobStatus::Completed;
        }

        let mut producer = task.execute(inputs, &activity);
        let mut closures = activity.closures();
        let mut remaining = members.len();
        let mut routed = 0;

        while remaining > 0 {
            if routed == ROUTE_BUDGET {
                let mut yielded = false;

                poll_fn(|cx| {
                    if yielded {
                        Poll::Ready(())
                    } else {
                        yielded = true;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;

                routed = 0;
            }

            select_biased! {
                closed = closures.next().fuse() => {
                    match closed {
                        Some(index) => {
                            if let Some(member) = members.get_mut(index) {
                                member.finish(Err(Error::consumer_cancelled()));
                            }

                            remaining -= 1;
                        }
                        None => break,
                    }
                }
                event = producer.next().fuse() => {
                    match event {
                        Some(BatchStreamEvent::Item { index, item }) => {
                            if let Some(member) = members.get_mut(index)
                                && member.is_open()
                            {
                                match member.try_send(item.map_err(Error::task_execution)) {
                                    Ok(()) => {}
                                    Err(MemberSendError::Full(_)) => {
                                        member.finish(Err(Error::consumer_lag()));
                                    }
                                    Err(MemberSendError::Closed(_)) => {
                                        member.finish(Err(Error::consumer_cancelled()));
                                    }
                                }
                            }
                        }
                        Some(BatchStreamEvent::Finished { index, outcome }) => {
                            if let Some(member) = members.get_mut(index)
                                && member.is_open()
                            {
                                member.finish(outcome.map_err(Error::task_execution));
                            }
                        }
                        None => {
                            for member in members.iter_mut() {
                                member.finish(Ok(()));
                            }

                            break;
                        }
                    }
                }
            }

            routed += 1;
        }

        if members
            .iter()
            .any(StreamBatchMember::failed)
        {
            JobStatus::Failed
        } else {
            JobStatus::Completed
        }
    }
}

pub struct Independent;

impl Independent {
    async fn run_member<B>(
        &self,
        task: &B,
        shared: &B::Shared,
        input: &B::Input,
        mut member: StreamBatchMember<B::Item>,
    ) -> bool
    where
        B: IndependentBatchStreamTask,
    {
        if !member.is_open() {
            member.finish(Err(Error::consumer_cancelled()));
            return member.failed();
        }

        let mut stream = match catch_unwind(AssertUnwindSafe(|| task.stream(shared, input))) {
            Ok(stream) => stream,
            Err(panic) => {
                member.finish(Err(Error::from_panic(panic)));
                return member.failed();
            }
        };

        loop {
            match AssertUnwindSafe(stream.next())
                .catch_unwind()
                .await
            {
                Err(panic) => {
                    member.finish(Err(Error::from_panic(panic)));
                    return member.failed();
                }
                Ok(None) => {
                    member.finish(Ok(()));
                    return member.failed();
                }
                Ok(Some(item)) => {
                    if let Err(error) = member
                        .send(item.map_err(Error::task_execution))
                        .await
                    {
                        member.finish(Err(error));
                        return member.failed();
                    }
                }
            }
        }
    }
}

#[async_trait]
impl<B> StreamBatchMode<B> for Independent
where
    B: IndependentBatchStreamTask + 'static,
{
    type Input = B::Input;
    type Item = B::Item;

    async fn run(
        &self,
        task: &B,
        inputs: &[Self::Input],
        context: StreamBatchContext<Self::Item>,
    ) -> JobStatus {
        let activity = context.activity();
        let mut members = context.into_members();

        if members.is_empty() {
            return JobStatus::Completed;
        }

        let mut closures = activity.closures();
        let prepare = AssertUnwindSafe(async { task.prepare(inputs).await })
            .catch_unwind()
            .fuse();
        let mut closed = 0;

        pin_mut!(prepare);

        let prepared = loop {
            select_biased! {
                departure = closures.next().fuse() => {
                    if departure.is_none() {
                        return JobStatus::Failed;
                    }

                    closed += 1;

                    if closed == members.len() {
                        return JobStatus::Failed;
                    }
                }
                result = prepare => break result,
            }
        };

        let shared = match prepared {
            Ok(Ok(shared)) => shared,
            Ok(Err(error)) => {
                let error = Error::task_execution(error);

                for member in members.iter_mut() {
                    member.finish(Err(error.clone()));
                }

                return JobStatus::Failed;
            }
            Err(panic) => {
                let error = Error::from_panic(panic);

                for member in members.iter_mut() {
                    member.finish(Err(error.clone()));
                }

                return JobStatus::Failed;
            }
        };

        let mut pending = FuturesUnordered::new();

        for (input, member) in inputs.iter().zip(members) {
            pending.push(self.run_member(task, &shared, input, member));
        }

        let mut failed = false;

        while let Some(member_failed) = pending.next().await {
            failed |= member_failed;
        }

        if failed {
            JobStatus::Failed
        } else {
            JobStatus::Completed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{future::JobStreamHandle, job::StreamDelivery};
    use futures::{stream, stream::BoxStream};
    use std::{
        convert::Infallible,
        io,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    use tokio::{
        sync::Notify,
        time::{Duration, timeout},
    };

    use crate::batch::stream::member::StreamBatchActivity;

    struct Events;

    impl MultiplexedBatchStreamTask for Events {
        type Input = ();
        type Item = u8;
        type Error = io::Error;

        fn execute<'a>(
            &'a self,
            _inputs: &'a [Self::Input],
            _activity: &'a StreamBatchActivity,
        ) -> BoxStream<'a, BatchStreamEvent<Self::Item, Self::Error>> {
            stream::iter(vec![
                BatchStreamEvent::Item {
                    index: 0,
                    item: Err(io::Error::other("recoverable")),
                },
                BatchStreamEvent::Item { index: 0, item: Ok(1) },
                BatchStreamEvent::Finished { index: 0, outcome: Ok(()) },
                BatchStreamEvent::Item { index: 0, item: Ok(9) },
                BatchStreamEvent::Item { index: 99, item: Ok(9) },
                BatchStreamEvent::Item { index: 1, item: Ok(2) },
                BatchStreamEvent::Finished {
                    index: 1,
                    outcome: Err(io::Error::other("terminal")),
                },
            ])
            .boxed()
        }
    }

    struct LagEvents;

    impl MultiplexedBatchStreamTask for LagEvents {
        type Input = ();
        type Item = u8;
        type Error = Infallible;

        fn execute<'a>(
            &'a self,
            _inputs: &'a [Self::Input],
            _activity: &'a StreamBatchActivity,
        ) -> BoxStream<'a, BatchStreamEvent<Self::Item, Self::Error>> {
            stream::iter([
                BatchStreamEvent::Item { index: 0, item: Ok(1) },
                BatchStreamEvent::Item { index: 0, item: Ok(2) },
                BatchStreamEvent::Item { index: 1, item: Ok(3) },
                BatchStreamEvent::Finished { index: 1, outcome: Ok(()) },
            ])
            .boxed()
        }
    }

    struct DropStream(Arc<AtomicBool>);

    impl futures::Stream for DropStream {
        type Item = BatchStreamEvent<u8, Infallible>;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl Drop for DropStream {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct PendingEvents(Arc<AtomicBool>);

    impl MultiplexedBatchStreamTask for PendingEvents {
        type Input = ();
        type Item = u8;
        type Error = Infallible;

        fn execute<'a>(
            &'a self,
            _inputs: &'a [Self::Input],
            _activity: &'a StreamBatchActivity,
        ) -> BoxStream<'a, BatchStreamEvent<Self::Item, Self::Error>> {
            Box::pin(DropStream(self.0.clone()))
        }
    }

    struct RecoverableItems;

    #[async_trait]
    impl IndependentBatchStreamTask for RecoverableItems {
        type Input = ();
        type Shared = ();
        type Item = u8;
        type Error = io::Error;

        async fn prepare(&self, _inputs: &[Self::Input]) -> Result<Self::Shared, Self::Error> {
            Ok(())
        }

        fn stream<'a>(
            &'a self,
            _shared: &'a Self::Shared,
            _input: &'a Self::Input,
        ) -> BoxStream<'a, Result<Self::Item, Self::Error>> {
            stream::iter([Err(io::Error::other("recoverable")), Ok(7)]).boxed()
        }
    }

    struct IndependentRanges(Arc<AtomicUsize>);

    #[async_trait]
    impl IndependentBatchStreamTask for IndependentRanges {
        type Input = u8;
        type Shared = ();
        type Item = u8;
        type Error = Infallible;

        async fn prepare(&self, _inputs: &[Self::Input]) -> Result<Self::Shared, Self::Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn stream<'a>(
            &'a self,
            _shared: &'a Self::Shared,
            input: &'a Self::Input,
        ) -> BoxStream<'a, Result<Self::Item, Self::Error>> {
            stream::iter(0..=*input).map(Ok).boxed()
        }
    }

    struct Gated {
        release: Arc<Notify>,
        wound_down: Arc<AtomicBool>,
    }

    #[async_trait]
    impl IndependentBatchStreamTask for Gated {
        type Input = u8;
        type Shared = ();
        type Item = u8;
        type Error = Infallible;

        async fn prepare(&self, _inputs: &[Self::Input]) -> Result<Self::Shared, Self::Error> {
            Ok(())
        }

        fn stream<'a>(
            &'a self,
            _shared: &'a Self::Shared,
            input: &'a Self::Input,
        ) -> BoxStream<'a, Result<Self::Item, Self::Error>> {
            stream::once(async move { Ok(*input) })
                .chain(stream::unfold((), move |_| async move {
                    self.release.notified().await;
                    self.wound_down
                        .store(true, Ordering::SeqCst);

                    None::<(Result<u8, Infallible>, ())>
                }))
                .boxed()
        }
    }

    struct ConstructorPanic;

    #[async_trait]
    impl IndependentBatchStreamTask for ConstructorPanic {
        type Input = u8;
        type Shared = ();
        type Item = u8;
        type Error = Infallible;

        async fn prepare(&self, _inputs: &[Self::Input]) -> Result<Self::Shared, Self::Error> {
            Ok(())
        }

        fn stream<'a>(
            &'a self,
            _shared: &'a Self::Shared,
            input: &'a Self::Input,
        ) -> BoxStream<'a, Result<Self::Item, Self::Error>> {
            if *input == 0 {
                panic!("member constructor");
            }

            stream::iter([Ok(*input)]).boxed()
        }
    }

    struct PreparationFailure {
        panic: bool,
    }

    #[async_trait]
    impl IndependentBatchStreamTask for PreparationFailure {
        type Input = ();
        type Shared = ();
        type Item = u8;
        type Error = io::Error;

        async fn prepare(&self, _inputs: &[Self::Input]) -> Result<Self::Shared, Self::Error> {
            if self.panic {
                panic!("shared preparation");
            }

            Err(io::Error::other("shared preparation failed"))
        }

        fn stream<'a>(
            &'a self,
            _shared: &'a Self::Shared,
            _input: &'a Self::Input,
        ) -> BoxStream<'a, Result<Self::Item, Self::Error>> {
            stream::empty().boxed()
        }
    }

    struct PendingPreparation(Arc<AtomicUsize>);

    #[async_trait]
    impl IndependentBatchStreamTask for PendingPreparation {
        type Input = ();
        type Shared = ();
        type Item = u8;
        type Error = Infallible;

        async fn prepare(&self, _inputs: &[Self::Input]) -> Result<Self::Shared, Self::Error> {
            futures::future::pending().await
        }

        fn stream<'a>(
            &'a self,
            _shared: &'a Self::Shared,
            _input: &'a Self::Input,
        ) -> BoxStream<'a, Result<Self::Item, Self::Error>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            stream::empty().boxed()
        }
    }

    struct PollPanic;

    #[async_trait]
    impl IndependentBatchStreamTask for PollPanic {
        type Input = u8;
        type Shared = ();
        type Item = u8;
        type Error = Infallible;

        async fn prepare(&self, _inputs: &[Self::Input]) -> Result<Self::Shared, Self::Error> {
            Ok(())
        }

        fn stream<'a>(
            &'a self,
            _shared: &'a Self::Shared,
            input: &'a Self::Input,
        ) -> BoxStream<'a, Result<Self::Item, Self::Error>> {
            if *input == 0 {
                stream::poll_fn(|_| panic!("member poll")).boxed()
            } else {
                stream::iter([Ok(*input)]).boxed()
            }
        }
    }

    struct HotEvents(Arc<AtomicUsize>);

    impl MultiplexedBatchStreamTask for HotEvents {
        type Input = ();
        type Item = u8;
        type Error = Infallible;

        fn execute<'a>(
            &'a self,
            _inputs: &'a [Self::Input],
            _activity: &'a StreamBatchActivity,
        ) -> BoxStream<'a, BatchStreamEvent<Self::Item, Self::Error>> {
            let count = self.0.clone();

            stream::poll_fn(move |_| {
                count.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Some(BatchStreamEvent::Item { index: usize::MAX, item: Ok(1) }))
            })
            .boxed()
        }
    }

    #[tokio::test]
    async fn independent_poll_panic_is_member_local() {
        let (first, first_setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (mut second, second_setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (context, _) = StreamBatchContext::new(vec![
            StreamDelivery::new(first_setter),
            StreamDelivery::new(second_setter),
        ]);

        let status = Independent
            .run(&PollPanic, &[0, 7], context)
            .await;

        assert!(matches!(status, JobStatus::Failed));
        assert!(matches!(first.result().await, Err(Error::TaskPanic(_))));
        assert!(matches!(second.next().await, Some(Ok(7))));
        assert!(second.result().await.is_ok());
    }

    #[tokio::test]
    async fn hot_producer_yields_after_bounded_work_and_prioritizes_departure() {
        let count = Arc::new(AtomicUsize::new(0));
        let task = HotEvents(count.clone());
        let (handle, setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (context, _) = StreamBatchContext::new(vec![StreamDelivery::new(setter)]);
        let inputs = [()];
        let mut running = Box::pin(Multiplexed.run(&task, &inputs, context));

        assert!(
            running
                .as_mut()
                .now_or_never()
                .is_none()
        );
        assert_eq!(count.load(Ordering::SeqCst), 64);

        drop(handle);

        assert!(matches!(running.await, JobStatus::Failed));
        assert_eq!(count.load(Ordering::SeqCst), 64);
    }

    #[tokio::test]
    async fn multiplexed_item_error_is_recoverable_and_finished_is_terminal() {
        let (mut first, first_setter) =
            JobStreamHandle::<u8>::new(4, Some(Error::consumer_cancelled())).unwrap();
        let (mut second, second_setter) =
            JobStreamHandle::<u8>::new(4, Some(Error::consumer_cancelled())).unwrap();
        let (context, _) = StreamBatchContext::new(vec![
            StreamDelivery::new(first_setter),
            StreamDelivery::new(second_setter),
        ]);

        let status = Multiplexed
            .run(&Events, &[(), ()], context)
            .await;

        assert!(matches!(status, JobStatus::Failed));
        assert!(matches!(first.next().await, Some(Err(Error::TaskExecution { .. }))));
        assert!(matches!(first.next().await, Some(Ok(1))));
        assert!(first.next().await.is_none());
        assert!(first.result().await.is_ok());
        assert!(matches!(second.next().await, Some(Ok(2))));
        assert!(second.next().await.is_none());
        assert!(matches!(second.result().await, Err(Error::TaskExecution { .. })));
    }

    #[tokio::test]
    async fn multiplexed_lag_is_member_local_and_preserves_accepted_items() {
        let (mut lagging, lagging_setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (mut healthy, healthy_setter) =
            JobStreamHandle::<u8>::new(2, Some(Error::consumer_cancelled())).unwrap();
        let (context, _) = StreamBatchContext::new(vec![
            StreamDelivery::new(lagging_setter),
            StreamDelivery::new(healthy_setter),
        ]);

        let status = Multiplexed
            .run(&LagEvents, &[(), ()], context)
            .await;

        assert!(matches!(status, JobStatus::Failed));
        assert!(matches!(lagging.result().await, Err(Error::ConsumerLag)));
        assert!(matches!(lagging.next().await, Some(Ok(1))));
        assert!(lagging.next().await.is_none());
        assert!(matches!(healthy.next().await, Some(Ok(3))));
        assert!(healthy.result().await.is_ok());
    }

    #[tokio::test]
    async fn final_departure_drops_pending_multiplexed_producer() {
        let dropped = Arc::new(AtomicBool::new(false));
        let task = PendingEvents(dropped.clone());
        let (handle, setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (context, _) = StreamBatchContext::new(vec![StreamDelivery::new(setter)]);
        let inputs = [()];
        let mut running = Box::pin(Multiplexed.run(&task, &inputs, context));

        assert!(
            running
                .as_mut()
                .now_or_never()
                .is_none()
        );
        drop(handle);
        assert!(matches!(running.await, JobStatus::Failed));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn independent_backpressure_does_not_block_sibling_completion() {
        let calls = Arc::new(AtomicUsize::new(0));
        let task = IndependentRanges(calls.clone());
        let (blocked, blocked_setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (healthy, healthy_setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (context, _) = StreamBatchContext::new(vec![
            StreamDelivery::new(blocked_setter),
            StreamDelivery::new(healthy_setter),
        ]);
        let inputs = [1, 0];
        let running = tokio::spawn(async move {
            Independent
                .run(&task, &inputs, context)
                .await
        });

        assert!(healthy.result().await.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(blocked);
        assert!(matches!(running.await.unwrap(), JobStatus::Failed));
    }

    #[tokio::test]
    async fn independent_member_finishes_stream_after_consumer_closes() {
        let release = Arc::new(Notify::new());
        let wound_down = Arc::new(AtomicBool::new(false));
        let (mut handle, setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (context, _) = StreamBatchContext::new(vec![StreamDelivery::new(setter)]);
        let inputs = [7];
        let task = Gated {
            release: release.clone(),
            wound_down: wound_down.clone(),
        };
        let running = tokio::spawn(async move {
            Independent
                .run(&task, &inputs, context)
                .await
        });

        assert!(matches!(handle.next().await, Some(Ok(7))));

        drop(handle);
        release.notify_one();

        assert!(matches!(
            timeout(Duration::from_secs(1), running)
                .await
                .unwrap()
                .unwrap(),
            JobStatus::Failed
        ));
        assert!(wound_down.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn independent_constructor_panic_is_member_local() {
        let (first, first_setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (mut second, second_setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (context, _) = StreamBatchContext::new(vec![
            StreamDelivery::new(first_setter),
            StreamDelivery::new(second_setter),
        ]);

        let status = Independent
            .run(&ConstructorPanic, &[0, 7], context)
            .await;

        assert!(matches!(status, JobStatus::Failed));
        assert!(matches!(first.result().await, Err(Error::TaskPanic(_))));
        assert!(matches!(second.next().await, Some(Ok(7))));
        assert!(second.result().await.is_ok());
    }

    #[tokio::test]
    async fn shared_preparation_error_and_panic_fail_all_members() {
        for panic in [false, true] {
            let (first, first_setter) =
                JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
            let (second, second_setter) =
                JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
            let (context, _) = StreamBatchContext::new(vec![
                StreamDelivery::new(first_setter),
                StreamDelivery::new(second_setter),
            ]);

            let status = Independent
                .run(&PreparationFailure { panic }, &[(), ()], context)
                .await;

            assert!(matches!(status, JobStatus::Failed));
            if panic {
                assert!(matches!(first.result().await, Err(Error::TaskPanic(_))));
                assert!(matches!(second.result().await, Err(Error::TaskPanic(_))));
            } else {
                assert!(matches!(first.result().await, Err(Error::TaskExecution { .. })));
                assert!(matches!(second.result().await, Err(Error::TaskExecution { .. })));
            }
        }
    }

    #[tokio::test]
    async fn last_departure_cancels_pending_preparation() {
        let constructions = Arc::new(AtomicUsize::new(0));
        let task = PendingPreparation(constructions.clone());
        let (handle, setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (context, _) = StreamBatchContext::new(vec![StreamDelivery::new(setter)]);
        let inputs = [()];
        let mut running = Box::pin(Independent.run(&task, &inputs, context));

        assert!(
            running
                .as_mut()
                .now_or_never()
                .is_none()
        );
        drop(handle);
        assert!(matches!(running.await, JobStatus::Failed));
        assert_eq!(constructions.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn independent_item_error_is_recoverable() {
        let (mut handle, setter) =
            JobStreamHandle::<u8>::new(2, Some(Error::consumer_cancelled())).unwrap();
        let (context, _) = StreamBatchContext::new(vec![StreamDelivery::new(setter)]);

        let status = Independent.run(&RecoverableItems, &[()], context).await;

        assert!(matches!(status, JobStatus::Completed));
        assert!(matches!(
            handle.next().await,
            Some(Err(Error::TaskExecution { .. }))
        ));
        assert!(matches!(handle.next().await, Some(Ok(7))));
        assert!(handle.next().await.is_none());
        assert!(handle.result().await.is_ok());
    }
}
