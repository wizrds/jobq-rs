use async_trait::async_trait;
use event_listener::Event;
use mea::mutex::Mutex;
use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use crate::queue::{error::Error, traits::Queue};

/// A Last In First Out (LIFO) queue implementation.
pub struct LifoQueue<T>
where
    T: Send + Sync + 'static,
{
    inner: Mutex<VecDeque<T>>,
    notify: Arc<Event>,
    closed: AtomicBool,
}

impl<T> LifoQueue<T>
where
    T: Send + Sync + 'static,
{
    /// Creates a new [`LifoQueue`](crate::queue::lifo::LifoQueue) instance with the specified maximum capacity.
    ///
    /// # Arguments
    /// * `max_capacity` - The maximum capacity of the queue.
    ///
    /// # Returns
    /// A new [`LifoQueue`](crate::queue::lifo::LifoQueue) instance with the specified maximum capacity.
    pub fn new(max_capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(max_capacity)),
            notify: Arc::new(Event::new()),
            closed: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl<T> Queue for LifoQueue<T>
where
    T: Send + Sync + 'static,
{
    type Item = T;
    type Options = ();

    async fn enqueue(
        &self,
        item: Self::Item,
        _options: Option<Self::Options>,
    ) -> Result<(), Error> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::closed());
        }

        let mut guard = self.inner.lock().await;
        guard.push_back(item);
        drop(guard);

        self.notify.notify(1);
        Ok(())
    }

    async fn dequeue(&self) -> Result<Option<Self::Item>, Error> {
        loop {
            if self.closed.load(Ordering::SeqCst) {
                let mut guard = self.inner.lock().await;
                return Ok(guard.pop_back());
            }

            let mut guard = self.inner.lock().await;
            if let Some(item) = guard.pop_back() {
                drop(guard);
                return Ok(Some(item));
            }

            drop(guard);
            self.notify.listen().await;
        }
    }

    async fn len(&self) -> usize {
        let guard = self.inner.lock().await;
        guard.len()
    }

    async fn close(&self) -> Result<(), Error> {
        self.closed
            .store(true, Ordering::SeqCst);
        self.notify.notify(usize::MAX);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::task;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_enqueue_dequeue_lifo_order() {
        let queue = LifoQueue::new(10);

        queue.enqueue(1, None).await.unwrap();
        queue.enqueue(2, None).await.unwrap();
        queue.enqueue(3, None).await.unwrap();

        assert_eq!(queue.dequeue().await.unwrap(), Some(3));
        assert_eq!(queue.dequeue().await.unwrap(), Some(2));
        assert_eq!(queue.dequeue().await.unwrap(), Some(1));
    }

    #[tokio::test]
    async fn test_dequeue_blocks_until_item_available() {
        let queue = Arc::new(LifoQueue::new(10));

        let q_clone = queue.clone();
        let handle = task::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            q_clone.enqueue(42, None).await.unwrap();
        });

        let value = queue.dequeue().await.unwrap();
        handle.await.unwrap();

        assert_eq!(value, Some(42));
    }

    #[tokio::test]
    async fn test_concurrent_enqueue_and_dequeue() {
        let queue = Arc::new(LifoQueue::new(10));

        let producer = {
            let queue = queue.clone();
            task::spawn(async move {
                for i in 0..10 {
                    queue.enqueue(i, None).await.unwrap();
                }
            })
        };

        let consumer = {
            let queue = queue.clone();
            task::spawn(async move {
                let mut results = Vec::new();
                for _ in 0..10 {
                    if let Some(val) = queue.dequeue().await.unwrap() {
                        results.push(val);
                    }
                }
                results
            })
        };

        let (_, results) = tokio::join!(producer, consumer);
        let mut consumed = results.unwrap();

        // Should be in reverse (LIFO)
        assert_eq!(consumed.len(), 10);
        consumed.sort();
        assert_eq!(consumed, (0..10).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_len_tracking() {
        let queue = LifoQueue::new(10);

        assert_eq!(queue.len().await, 0);
        queue.enqueue("a", None).await.unwrap();
        queue.enqueue("b", None).await.unwrap();
        assert_eq!(queue.len().await, 2);
        queue.dequeue().await.unwrap();
        assert_eq!(queue.len().await, 1);
    }

    #[tokio::test]
    async fn test_close_and_dequeue_remaining_items() {
        let queue = Arc::new(LifoQueue::new(10));

        queue.enqueue("a", None).await.unwrap();
        queue.enqueue("b", None).await.unwrap();
        queue.close().await.unwrap();

        assert_eq!(queue.dequeue().await.unwrap(), Some("b"));
        assert_eq!(queue.dequeue().await.unwrap(), Some("a"));
        assert_eq!(queue.dequeue().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_enqueue_after_close_fails() {
        let queue = LifoQueue::new(10);
        queue.close().await.unwrap();

        let result = queue.enqueue("x", None).await;
        assert!(matches!(result, Err(Error::Closed)));
    }

    #[tokio::test]
    async fn test_blocked_dequeue_wakes_on_close() {
        let queue = Arc::new(LifoQueue::<i32>::new(10));

        let q_clone = queue.clone();
        let handle = task::spawn(async move {
            let result = q_clone.dequeue().await.unwrap();
            assert_eq!(result, None);
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        queue.close().await.unwrap();

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_notify_reset_behavior_under_load() {
        let queue = Arc::new(LifoQueue::<i32>::new(10));

        let producers = (0..10).map(|i| {
            let q = queue.clone();
            task::spawn(async move {
                q.enqueue(i, None).await.unwrap();
            })
        });

        let consumers = (0..10).map(|_| {
            let q = queue.clone();
            task::spawn(async move { q.dequeue().await.unwrap() })
        });

        let (_, results) = tokio::join!(
            futures::future::join_all(producers),
            futures::future::join_all(consumers)
        );

        let values = results
            .into_iter()
            .filter_map(|r| r.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 10);
    }

    #[tokio::test]
    async fn test_dequeue_timeout_if_never_signaled() {
        let queue = Arc::new(LifoQueue::<i32>::new(10));

        let result = timeout(Duration::from_millis(100), queue.dequeue()).await;
        assert!(result.is_err(), "dequeue should have timed out");
    }
}
