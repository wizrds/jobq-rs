use futures::{FutureExt, future::BoxFuture, stream::FuturesUnordered};

use crate::{error::Error, future::StreamLifecycle, job::StreamDelivery};

pub(crate) struct BatchCompletion {
    lifecycles: Vec<StreamLifecycle>,
}

impl BatchCompletion {
    fn new(lifecycles: Vec<StreamLifecycle>) -> Self {
        Self { lifecycles }
    }

    pub(crate) fn fail_unfinished(&self, error: Error) {
        for lifecycle in &self.lifecycles {
            lifecycle.set_terminal(Err(error.clone()));
        }
    }

    pub(crate) fn has_failure(&self) -> bool {
        self.lifecycles
            .iter()
            .any(StreamLifecycle::failed)
    }
}

pub type StreamBatchClosures = FuturesUnordered<BoxFuture<'static, usize>>;

pub enum MemberSendError<T> {
    Full(Result<T, Error>),
    Closed(Result<T, Error>),
}

pub struct StreamBatchMember<T>
where
    T: Send + Sync + 'static,
{
    delivery: Option<StreamDelivery<T>>,
    lifecycle: StreamLifecycle,
}

impl<T> StreamBatchMember<T>
where
    T: Send + Sync + 'static,
{
    fn new(delivery: StreamDelivery<T>) -> Self {
        Self {
            lifecycle: delivery.lifecycle(),
            delivery: Some(delivery),
        }
    }

    pub fn is_open(&self) -> bool {
        self.delivery.is_some() && self.lifecycle.is_open()
    }

    pub async fn closed(&self) {
        self.lifecycle.closed().await;
    }

    pub fn failed(&self) -> bool {
        self.lifecycle.failed()
    }

    pub async fn send(&mut self, item: Result<T, Error>) -> Result<(), Error> {
        if !self.is_open() {
            return Err(Error::consumer_cancelled());
        }

        self.delivery
            .as_mut()
            .expect("an open member has a delivery")
            .send(item)
            .await
            .map_err(|_| Error::consumer_cancelled())
    }

    pub fn try_send(&mut self, item: Result<T, Error>) -> Result<(), MemberSendError<T>> {
        if !self.is_open() {
            return Err(MemberSendError::Closed(item));
        }

        self.delivery
            .as_mut()
            .expect("an open member has a delivery")
            .try_send(item)
            .map_err(|error| {
                if error.is_full() {
                    MemberSendError::Full(error.into_inner())
                } else {
                    MemberSendError::Closed(error.into_inner())
                }
            })
    }

    pub fn finish(&mut self, outcome: Result<(), Error>) {
        if let Some(delivery) = self.delivery.take() {
            delivery.finish(outcome);
        }
    }
}

#[derive(Clone)]
pub struct StreamBatchActivity {
    lifecycles: Vec<StreamLifecycle>,
}

impl StreamBatchActivity {
    fn new(lifecycles: Vec<StreamLifecycle>) -> Self {
        Self { lifecycles }
    }

    pub fn len(&self) -> usize {
        self.lifecycles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lifecycles.is_empty()
    }

    pub fn is_open(&self, index: usize) -> bool {
        self.lifecycles
            .get(index)
            .is_some_and(StreamLifecycle::is_open)
    }

    pub async fn closed(&self, index: usize) {
        if let Some(lifecycle) = self.lifecycles.get(index) {
            lifecycle.closed().await;
        }
    }

    pub fn closures(&self) -> StreamBatchClosures {
        self.lifecycles
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, lifecycle)| {
                async move {
                    lifecycle.closed().await;
                    index
                }
                .boxed()
            })
            .collect()
    }
}

pub struct StreamBatchContext<T>
where
    T: Send + Sync + 'static,
{
    activity: StreamBatchActivity,
    members: Vec<StreamBatchMember<T>>,
}

impl<T> StreamBatchContext<T>
where
    T: Send + Sync + 'static,
{
    pub(crate) fn new(deliveries: Vec<StreamDelivery<T>>) -> (Self, BatchCompletion) {
        let lifecycles = deliveries
            .iter()
            .map(StreamDelivery::lifecycle)
            .collect::<Vec<_>>();

        (
            Self {
                activity: StreamBatchActivity::new(lifecycles.clone()),
                members: deliveries
                    .into_iter()
                    .map(StreamBatchMember::new)
                    .collect(),
            },
            BatchCompletion::new(lifecycles),
        )
    }

    pub fn activity(&self) -> StreamBatchActivity {
        self.activity.clone()
    }

    pub fn into_members(self) -> Vec<StreamBatchMember<T>> {
        self.members
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::future::JobStreamHandle;
    use futures::StreamExt;

    #[tokio::test]
    async fn first_terminal_claim_preserves_items_and_result() {
        let (mut handle, setter) =
            JobStreamHandle::<u8>::new(2, Some(Error::consumer_cancelled())).unwrap();
        let (context, completion) = StreamBatchContext::new(vec![StreamDelivery::new(setter)]);
        let activity = context.activity();
        let mut member = context.into_members().pop().unwrap();

        assert!(member.try_send(Ok(1)).is_ok());
        assert!(member.try_send(Ok(2)).is_ok());
        member.finish(Ok(()));
        completion.fail_unfinished(Error::incomplete_stream_batch());

        assert!(!activity.is_open(0));
        assert!(!completion.has_failure());
        assert!(handle.result().await.is_ok());
        assert!(matches!(handle.next().await, Some(Ok(1))));
        assert!(matches!(handle.next().await, Some(Ok(2))));
        assert!(handle.next().await.is_none());
    }

    #[tokio::test]
    async fn full_attempt_returns_undelivered_item() {
        let (handle, setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (context, _) = StreamBatchContext::new(vec![StreamDelivery::new(setter)]);
        let mut member = context.into_members().pop().unwrap();

        assert!(member.try_send(Ok(1)).is_ok());
        assert!(matches!(member.try_send(Ok(2)), Err(MemberSendError::Full(Ok(2)))));

        drop(handle);
        assert!(matches!(member.try_send(Ok(3)), Err(MemberSendError::Closed(Ok(3)))));
    }

    #[tokio::test]
    async fn split_receiver_drop_closes_only_its_member() {
        let (first, first_setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (second, second_setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (context, completion) = StreamBatchContext::new(vec![
            StreamDelivery::new(first_setter),
            StreamDelivery::new(second_setter),
        ]);
        let activity = context.activity();
        let mut members = context.into_members();
        let (first_items, first_result) = first.split();
        let mut closures = activity.closures();

        drop(first_items);

        assert_eq!(closures.next().await, Some(0));
        assert!(!activity.is_open(0));
        assert!(activity.is_open(1));
        assert!(!activity.is_open(usize::MAX));
        assert!(matches!(first_result.result().await, Err(Error::ConsumerCancelled)));

        members[1].finish(Ok(()));
        completion.fail_unfinished(Error::incomplete_stream_batch());
        assert!(second.result().await.is_ok());
    }

    #[tokio::test]
    async fn tracker_settles_unfinished_member() {
        let (handle, setter) =
            JobStreamHandle::<u8>::new(1, Some(Error::consumer_cancelled())).unwrap();
        let (context, completion) = StreamBatchContext::new(vec![StreamDelivery::new(setter)]);

        drop(context);
        completion.fail_unfinished(Error::incomplete_stream_batch());

        assert!(completion.has_failure());
        assert!(matches!(handle.result().await, Err(Error::IncompleteStreamBatch)));
    }
}
