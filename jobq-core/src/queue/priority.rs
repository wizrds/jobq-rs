use async_trait::async_trait;
use mea::mutex::Mutex;
use std::{collections::BinaryHeap, cmp::Ordering, sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering}}};
use event_listener::Event;

use crate::queue::{traits::Queue, error::{Error, Result}};


/// Wrapper for items with priority
#[derive(Debug)]
struct PriorityItem<T> {
    item: T,
    priority: u32,
    idx: usize,
}

impl<T> PartialEq for PriorityItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
            && self.idx == other.idx
    }
}

impl<T> Eq for PriorityItem<T> {}

impl<T> PartialOrd for PriorityItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for PriorityItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
            .then_with(|| other.idx.cmp(&self.idx))
    }
}


#[derive(Debug, Clone)]
pub struct PriorityOptions {
    pub priority: u32,
}

/// A priority-based queue implementation.
pub struct PriorityQueue<T>
where
    T: Send + Sync + 'static,
{
    heap: Mutex<BinaryHeap<PriorityItem<T>>>,
    length: AtomicUsize,
    closed: AtomicBool,
    notify: Arc<Event>,
    idx_counter: AtomicUsize,
}

impl<T> PriorityQueue<T>
where
    T: Send + Sync + 'static,
{
    /// Creates a new [`PriorityQueue`](crate::queue::priority::PriorityQueue) instance.
    /// 
    /// # Returns
    /// A new [`PriorityQueue`](crate::queue::priority::PriorityQueue) instance.
    pub fn new(max_capacity: usize) -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::with_capacity(max_capacity)),
            length: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            notify: Arc::new(Event::new()),
            idx_counter: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl<T> Queue for PriorityQueue<T>
where
    T: Send + Sync + 'static,
{
    type Item = T;
    type Options = PriorityOptions;

    async fn enqueue(&self, item: Self::Item, options: Option<Self::Options>) -> Result<()> {
        if self.closed.load(AtomicOrdering::SeqCst) {
            return Err(Error::Closed);
        }

        let priority = options
            .map(|opt| opt.priority)
            .unwrap_or(0);
        let idx = self.idx_counter.fetch_add(1, AtomicOrdering::SeqCst);

        let mut guard = self.heap.lock().await;
        guard.push(PriorityItem { item, priority: priority as u32, idx });
        drop(guard);
        
        self.length.fetch_add(1, AtomicOrdering::SeqCst);
        self.notify.notify(1);

        Ok(())
    }

    async fn dequeue(&self) -> Result<Option<Self::Item>> {
        loop {
            if self.closed.load(AtomicOrdering::SeqCst) {
                let mut guard = self.heap.lock().await;
                return Ok(guard.pop().map(|p| p.item));
            }

            let mut guard = self.heap.lock().await;
            if let Some(item) = guard.pop() {
                self.length.fetch_sub(1, AtomicOrdering::SeqCst);
                return Ok(Some(item.item));
            }

            drop(guard);
            self.notify.listen().await;
        }
    }

    async fn len(&self) -> usize {
        self.length.load(AtomicOrdering::SeqCst)
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, AtomicOrdering::SeqCst);
        self.notify.notify(usize::MAX);

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task;
    use tokio::time::Duration;

    fn opts(priority: u32) -> Option<PriorityOptions> {
        Some(PriorityOptions { priority })
    }

   #[tokio::test]
    async fn test_priority_order_with_insertion_stability() {
        let queue = PriorityQueue::new(10);

        queue.enqueue("low1", opts(1)).await.unwrap();
        queue.enqueue("medium1", opts(5)).await.unwrap();
        queue.enqueue("high1", opts(10)).await.unwrap();
        queue.enqueue("low2", opts(1)).await.unwrap();
        queue.enqueue("medium2", opts(5)).await.unwrap();
        queue.enqueue("high2", opts(10)).await.unwrap();

        queue.close().await.unwrap();

        let mut results = Vec::new();

        while let Some(item) = queue.dequeue().await.unwrap() {
            results.push(item);
        }

        assert_eq!(results, vec!["high1", "high2", "medium1", "medium2", "low1", "low2"]);
    }

    #[tokio::test]
    async fn test_len_tracking() {
        let queue = PriorityQueue::new(10);

        assert_eq!(queue.len().await, 0);

        queue.enqueue("task1", opts(1)).await.unwrap();
        assert_eq!(queue.len().await, 1);

        queue.enqueue("task2", opts(2)).await.unwrap();
        assert_eq!(queue.len().await, 2);

        queue.dequeue().await.unwrap(); // Removes one item
        assert_eq!(queue.len().await, 1);
    }

    #[tokio::test]
    async fn test_dequeue_blocks_until_item_available() {
        let queue = Arc::new(PriorityQueue::new(10));
        let cloned = queue.clone();

        let handle = task::spawn(async move {
            let item = cloned.dequeue().await.unwrap();
            assert_eq!(item, Some("delayed"));
        });

        // Let the dequeue start and block
        tokio::time::sleep(Duration::from_millis(100)).await;

        queue.enqueue("delayed", opts(5)).await.unwrap();

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_enqueue_and_dequeue() {
        let queue = Arc::new(PriorityQueue::new(15));

        let producers: Vec<_> = (0..10).map(|i| {
            let q = queue.clone();
            task::spawn(async move {
                q.enqueue(format!("item{}", i), opts(i)).await.unwrap();
            })
        }).collect();

        let consumers: Vec<_> = (0..10).map(|_| {
            let q = queue.clone();
            task::spawn(async move {
                loop {
                    if let Some(item) = q.dequeue().await.unwrap() {
                        break Some(item);
                    }
                }
            })
        }).collect();

        for p in producers {
            p.await.unwrap();
        }

        queue.close().await.unwrap();

        let mut items: Vec<String> = Vec::new();
        for c in consumers {
            let item = c.await.unwrap();
            if let Some(it) = item {
                items.push(it);
            }
        }

        // Should get all unique items from "item0" to "item9"
        items.sort();
        assert_eq!(items.len(), 10);
        for i in 0..10 {
            assert_eq!(items[i], format!("item{}", i));
        }
    }

    #[tokio::test]
    async fn test_close_stops_blocking_dequeue() {
        let queue = Arc::new(PriorityQueue::<i32>::new(10));
        let cloned = queue.clone();

        let handle = task::spawn(async move {
            let item = cloned.dequeue().await.unwrap();
            assert_eq!(item, None); // Should receive None after close
        });

        // Let the dequeue start and block
        tokio::time::sleep(Duration::from_millis(100)).await;
        queue.close().await.unwrap();

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_stability_same_priority() {
        let queue = PriorityQueue::new(10);

        queue.enqueue("a", opts(3)).await.unwrap();
        queue.enqueue("b", opts(3)).await.unwrap();
        queue.enqueue("c", opts(3)).await.unwrap();

        queue.close().await.unwrap();

        let mut result = Vec::new();
        while let Some(item) = queue.dequeue().await.unwrap() {
            result.push(item);
        }

        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn test_default_priority_zero() {
        let queue = PriorityQueue::new(10);

        queue.enqueue("explicit_high", opts(10)).await.unwrap();
        queue.enqueue("default", None).await.unwrap(); // Priority 0

        queue.close().await.unwrap();

        let mut result = Vec::new();
        while let Some(item) = queue.dequeue().await.unwrap() {
            result.push(item);
        }

        assert_eq!(result, vec!["explicit_high", "default"]);
    }

    #[tokio::test]
    async fn test_enqueue_after_close_fails() {
        let queue = PriorityQueue::new(10);

        queue.close().await.unwrap();

        let result = queue.enqueue("should_fail", opts(1)).await;
        assert!(matches!(result, Err(Error::Closed)));
    }
}
