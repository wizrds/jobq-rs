use async_trait::async_trait;

use crate::queue::error::Result;


/// Trait defining the behavior of a queue in the job queue system.
/// 
/// This trait is generic over the item type `Item` and the options type `Options`.
/// It provides methods for enqueueing and dequeueing items, checking the length of the queue,
/// and closing the queue.
#[async_trait]
pub trait Queue: Send + Sync {
    type Item: Send + Sync + 'static;
    type Options: Send + Sync + 'static;

    /// Enqueues an item into the queue with optional options.
    /// 
    /// # Arguments
    /// * `item` - The item to be enqueued.
    /// * `options` - Optional options for the enqueue operation.
    /// 
    /// # Returns
    /// A `Result` indicating success or failure of the enqueue operation.
    async fn enqueue(&self, item: Self::Item, options: Option<Self::Options>) -> Result<()>;

    /// Dequeues an item from the queue.
    /// 
    /// # Returns
    /// A `Result` containing an `Option<Self::Item>`, which is `Some` if an item was successfully dequeued, or `None` if the queue is closed.
    async fn dequeue(&self) -> Result<Option<Self::Item>>;
    
    /// Returns the number of items currently in the queue.
    async fn len(&self) -> usize;

    /// Closes the queue, preventing any further items from being enqueued.
    /// 
    /// # Returns
    /// A `Result` indicating success or failure of the close operation.
    async fn close(&self) -> Result<()>;
}