use std::sync::{Arc, atomic::{AtomicUsize, AtomicBool, Ordering}};
use async_trait::async_trait;
use futures::channel::mpsc::{channel, Sender, Receiver};
use futures::{SinkExt, StreamExt};
use mea::mutex::Mutex;

use crate::queue::{traits::Queue, error::{Error, Result}};


/// A First In First Out (FIFO) queue implementation.
#[derive(Debug)]
pub struct FifoQueue<T>
where 
    T: Send + Sync + 'static,
{
    sender: Arc<Mutex<Option<Sender<T>>>>,
    receiver: Arc<Mutex<Receiver<T>>>,
    length: AtomicUsize,
    closed: AtomicBool,
}

impl<T> FifoQueue<T>
where 
    T: Send + Sync + 'static,
{
    /// Creates a new [`FifoQueue`](crate::queue::fifo::FifoQueue) instance with the specified maximum capacity.
    ///
    /// # Arguments
    /// * `max_capacity` - The maximum capacity of the queue.
    /// 
    /// # Returns
    /// A new [`FifoQueue`](crate::queue::fifo::FifoQueue) instance with the specified maximum capacity.
    pub fn new(max_capacity: usize) -> Self {
        let (sender, receiver) = channel(max_capacity);

        FifoQueue {
            sender: Arc::new(Mutex::new(Some(sender))),
            receiver: Arc::new(Mutex::new(receiver)),
            length: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }
    }
}


#[async_trait]
impl<T> Queue for FifoQueue<T>
where 
    T: Send + Sync + 'static,
{
    type Item = T;
    type Options = ();

    async fn enqueue(&self, item: Self::Item, _options: Option<Self::Options>) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }

        let sender = self.sender
            .lock()
            .await
            .as_ref()
            .cloned();

        match sender {
            Some(mut s) => {
                s.send(item)
                    .await
                    .map_err(|e| Error::EnqueueError(e.to_string()))?;
                self.length.fetch_add(1, Ordering::SeqCst);

                Ok(())
            },
            None => Err(Error::Closed),
        }
    }

    async fn dequeue(&self) -> Result<Option<Self::Item>> {
        let mut receiver = self.receiver.lock().await;
        
        let item = receiver.next().await;
        if item.is_some() {
            self.length.fetch_sub(1, Ordering::SeqCst);
        }

        Ok(item)
    }

    async fn len(&self) -> usize {
        self.length.load(Ordering::SeqCst)
    }

    async fn close(&self) -> Result<()> {
        let _ = self.sender.lock().await.take();
        self.length.store(0, Ordering::SeqCst);
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::task;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_enqueue_dequeue_single_item() {
        let queue = FifoQueue::new(10);
        queue.enqueue(42, None).await.unwrap();

        assert_eq!(queue.len().await, 1);

        let item = queue.dequeue().await.unwrap();
        assert_eq!(item, Some(42));

        assert_eq!(queue.len().await, 0);
    }

    #[tokio::test]
    async fn test_fifo_ordering() {
        let queue = FifoQueue::new(10);

        queue.enqueue(1, None).await.unwrap();
        queue.enqueue(2, None).await.unwrap();
        queue.enqueue(3, None).await.unwrap();

        assert_eq!(queue.dequeue().await.unwrap(), Some(1));
        assert_eq!(queue.dequeue().await.unwrap(), Some(2));
        assert_eq!(queue.dequeue().await.unwrap(), Some(3));
    }

    #[tokio::test]
    async fn test_len_reflects_state() {
        let queue = FifoQueue::new(10);

        assert_eq!(queue.len().await, 0);

        queue.enqueue("a", None).await.unwrap();
        queue.enqueue("b", None).await.unwrap();

        assert_eq!(queue.len().await, 2);

        queue.dequeue().await.unwrap();
        assert_eq!(queue.len().await, 1);

        queue.dequeue().await.unwrap();
        assert_eq!(queue.len().await, 0);
    }

    #[tokio::test]
    async fn test_close_prevents_enqueue() {
        let queue = FifoQueue::new(10);
        queue.enqueue("hello", None).await.unwrap();

        queue.close().await.unwrap();

        let result = queue.enqueue("world", None).await;
        assert!(matches!(result, Err(Error::Closed)));

        // Dequeue should still work
        assert_eq!(queue.dequeue().await.unwrap(), Some("hello"));
        assert_eq!(queue.dequeue().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_concurrent_enqueue_dequeue() {
        let queue = Arc::new(FifoQueue::new(100));

        let producer = {
            let queue = queue.clone();
            task::spawn(async move {
                for i in 0..50 {
                    queue.enqueue(i, None).await.unwrap();
                }
            })
        };

        let consumer = {
            let queue = queue.clone();
            task::spawn(async move {
                let mut collected = vec![];
                for _ in 0..50 {
                    if let Some(item) = queue.dequeue().await.unwrap() {
                        collected.push(item);
                    }
                }
                collected
            })
        };

        let (_, result) = tokio::join!(producer, consumer);
        let collected = result.unwrap();

        assert_eq!(collected.len(), 50);
        assert_eq!(collected[0], 0);
        assert_eq!(collected.last().unwrap(), &49);
    }

    #[tokio::test]
    async fn test_dequeue_after_close_returns_remaining() {
        let queue = FifoQueue::new(10);
        queue.enqueue(1, None).await.unwrap();
        queue.enqueue(2, None).await.unwrap();
        queue.enqueue(3, None).await.unwrap();

        queue.close().await.unwrap();

        assert_eq!(queue.dequeue().await.unwrap(), Some(1));
        assert_eq!(queue.dequeue().await.unwrap(), Some(2));
        assert_eq!(queue.dequeue().await.unwrap(), Some(3));

        // Closed + empty = None
        assert_eq!(queue.dequeue().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_enqueue_blocks_if_full_then_unblocks() {
        let queue = Arc::new(FifoQueue::new(1));

        queue.enqueue(1, None).await.unwrap();

        let q2 = queue.clone();
        let enqueue_future = tokio::spawn(async move {
            // Should block until item is dequeued
            q2.enqueue(2, None).await.unwrap();
        });

        // Wait a bit to ensure it's blocked
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(queue.len().await, 1);

        // Now dequeue, which should unblock
        assert_eq!(queue.dequeue().await.unwrap(), Some(1));
        enqueue_future.await.unwrap();

        assert_eq!(queue.len().await, 1);
        assert_eq!(queue.dequeue().await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn test_enqueue_after_receiver_dropped_fails() {
        let queue = FifoQueue::new(1);
        queue.close().await.unwrap();

        let result = queue.enqueue(123, None).await;
        assert!(matches!(result, Err(Error::Closed)));
    }

    #[tokio::test]
    async fn test_dequeue_returns_none_after_close_and_empty() {
        let queue = FifoQueue::<i32>::new(1);
        queue.close().await.unwrap();

        assert_eq!(queue.dequeue().await.unwrap(), None);
    }
}
