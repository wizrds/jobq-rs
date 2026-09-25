use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::batch::stream::member::StreamBatchActivity;

pub enum BatchStreamEvent<T, E> {
    Item {
        index: usize,
        item: Result<T, E>,
    },
    Finished {
        index: usize,
        outcome: Result<(), E>,
    },
}

pub trait MultiplexedBatchStreamTask: Send + Sync {
    type Input: Send + Sync + 'static;
    type Item: Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute<'a>(
        &'a self,
        inputs: &'a [Self::Input],
        activity: &'a StreamBatchActivity,
    ) -> BoxStream<'a, BatchStreamEvent<Self::Item, Self::Error>>;
}

#[async_trait]
pub trait IndependentBatchStreamTask: Send + Sync {
    type Input: Send + Sync + 'static;
    type Shared: Send + Sync;
    type Item: Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn prepare(&self, inputs: &[Self::Input]) -> Result<Self::Shared, Self::Error>;

    fn stream<'a>(
        &'a self,
        shared: &'a Self::Shared,
        input: &'a Self::Input,
    ) -> BoxStream<'a, Result<Self::Item, Self::Error>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, stream};
    use std::convert::Infallible;

    struct IndependentFixture;

    #[async_trait]
    impl IndependentBatchStreamTask for IndependentFixture {
        type Input = u32;
        type Shared = u32;
        type Item = u32;
        type Error = Infallible;

        async fn prepare(&self, inputs: &[Self::Input]) -> Result<Self::Shared, Self::Error> {
            Ok(inputs.iter().sum())
        }

        fn stream<'a>(
            &'a self,
            shared: &'a Self::Shared,
            input: &'a Self::Input,
        ) -> BoxStream<'a, Result<Self::Item, Self::Error>> {
            stream::iter([Ok(*shared + *input)]).boxed()
        }
    }

    #[test]
    fn event_distinguishes_recoverable_item_from_terminal_outcome() {
        let item = BatchStreamEvent::<u32, Infallible>::Item { index: 2, item: Ok(3) };
        let terminal = BatchStreamEvent::<u32, Infallible>::Finished { index: 2, outcome: Ok(()) };

        assert!(matches!(item, BatchStreamEvent::Item { index: 2, .. }));
        assert!(matches!(terminal, BatchStreamEvent::Finished { index: 2, .. }));
    }

    #[tokio::test]
    async fn independent_contract_prepares_once_and_borrows_shared_state() {
        let task = IndependentFixture;
        let inputs = [2, 3];
        let shared = task.prepare(&inputs).await.unwrap();
        let first = task
            .stream(&shared, &inputs[0])
            .collect::<Vec<_>>()
            .await;
        let second = task
            .stream(&shared, &inputs[1])
            .collect::<Vec<_>>()
            .await;

        assert!(matches!(first.as_slice(), [Ok(7)]));
        assert!(matches!(second.as_slice(), [Ok(8)]));
    }
}
