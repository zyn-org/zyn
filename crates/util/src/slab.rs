// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// A reference to a slot in the slab.
#[derive(Debug)]
pub struct SlabRef<T: Default> {
  /// The handler for the slot.
  pub handler: usize,

  /// A reference to the slots in the slab.
  slots: Arc<Vec<RwLock<T>>>,

  /// A reference to the slab's inner state.
  inner: Arc<RwLock<SlabInner<T>>>,
}

// === impl SlabRef =====

impl<T: Default> Clone for SlabRef<T> {
  fn clone(&self) -> Self {
    Self { handler: self.handler, slots: self.slots.clone(), inner: self.inner.clone() }
  }
}

impl<T: Default> SlabRef<T> {
  /// Returns a read-only reference to the slot in the slab.
  ///
  /// # Returns
  ///
  /// A `RwLockReadGuard` that allows read access to the slot.
  pub async fn read(&self) -> RwLockReadGuard<'_, T> {
    self.slots[self.handler - 1].read().await
  }

  /// Returns a mutable reference to the slot in the slab.
  ///
  /// # Returns
  ///
  /// A `RwLockWriteGuard` that allows mutable access to the slot.
  pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
    self.slots[self.handler - 1].write().await
  }
}

impl<T: Default> SlabRef<T> {
  /// Releases the slab reference, making it available for reuse.
  ///
  /// This method resets the object in the specified slot to its default state and marks the slot as free.
  /// The slot can then be reused by subsequent calls to `acquire`.
  ///
  /// # Panics
  ///
  /// Panics if the slot is not in use or has already been released.
  pub async fn release(&self) {
    let mut inner = self.inner.write().await;

    let index = self.handler - 1;

    assert!(inner.in_use[index], "attempting to release a slot that is not in use or already released");

    {
      let mut slot = inner.slots[index].write().await;
      *slot = Default::default();
    }
    inner.in_use[index] = false;
    inner.free_slots.push(index);
  }
}

/// A simple slab allocator for managing reusable resources.
#[derive(Debug)]
pub struct Slab<T: Default>(Arc<RwLock<SlabInner<T>>>);

// == impl Slab =====

impl<T: Default> Clone for Slab<T> {
  #[inline]
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

#[derive(Debug)]
struct SlabInner<T: Default> {
  slots: Arc<Vec<RwLock<T>>>,
  in_use: Vec<bool>,
  free_slots: Vec<usize>,
}

// == impl Slab =====

impl<T: Default> Slab<T> {
  /// Creates a new slab allocator with the given capacity.
  ///
  /// # Arguments
  ///
  /// * `size` - The maximum number of objects that the slab can hold.
  ///
  /// # Returns
  ///
  /// A new instance of `Slab` initialized with the specified capacity.
  pub fn with_capacity(size: usize) -> Self {
    let mut free_slots = Vec::with_capacity(size);
    for i in (0..size).rev() {
      free_slots.push(i);
    }
    let mut slots = Vec::with_capacity(size);
    for _ in 0..size {
      slots.push(RwLock::new(Default::default()));
    }

    let inner = SlabInner { slots: Arc::new(slots), in_use: vec![false; size], free_slots };

    Self(Arc::new(RwLock::new(inner)))
  }

  /// Acquires a slot from the slab allocator, marking it as used.
  ///
  /// # Returns
  ///
  /// An `Option<SlabRef<T>>` containing the reference of the acquired slot if
  /// available, or `None` if no free slots remain.
  pub async fn acquire(&mut self) -> Option<SlabRef<T>> {
    let mut slab = self.0.write().await;

    if slab.free_slots.is_empty() {
      return None;
    }
    let index = slab.free_slots.pop().unwrap();
    slab.in_use[index] = true;

    let slots = slab.slots.clone();

    Some(SlabRef { handler: index + 1, slots, inner: self.0.clone() })
  }

  /// Converts a raw handler value to a SlabHandler.
  ///
  /// # Arguments
  ///
  /// * `raw_handler` - The raw handler value (must be > 0 and <= capacity).
  ///
  /// # Returns
  ///
  /// An `Option<SlabRef<T>>` containing the reference if the slot exists and is in use,
  /// or `None` if the handler is invalid or the slot is not in use.
  pub async fn ref_from_handler(&self, handler: usize) -> Option<SlabRef<T>> {
    // Handler values are 1-indexed, so subtract 1 to get the actual index
    if handler == 0 {
      return None;
    }
    let index = handler - 1;
    let slab = self.0.read().await;

    // Check if the index is within range and the slot is in use.
    if index >= slab.slots.len() || !slab.in_use[index] {
      return None;
    }

    Some(SlabRef { handler, slots: slab.slots.clone(), inner: self.0.clone() })
  }

  /// Returns the number of slots currently in use.
  ///
  /// # Returns
  ///
  /// A `usize` representing the number of slots that are occupied.
  #[inline]
  pub async fn len(&self) -> usize {
    let slab = self.0.read().await;
    slab.slots.len() - slab.free_slots.len()
  }

  /// Checks if the slab is empty (all slots have been acquired).
  ///
  /// # Returns
  ///
  /// A `bool` indicating whether the slab is empty.
  #[inline]
  pub async fn is_empty(&self) -> bool {
    let slab = self.0.read().await;
    slab.free_slots.is_empty()
  }

  /// Returns the total number of slots in the slab.
  ///
  /// # Returns
  ///
  /// A `usize` representing the total capacity of the slab.
  #[inline]
  pub async fn capacity(&self) -> usize {
    let slab = self.0.read().await;
    slab.slots.len()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test_create_slab() {
    let slab = Slab::<String>::with_capacity(10);
    assert_eq!(slab.capacity().await, 10);
    assert_eq!(slab.len().await, 0);
  }

  #[tokio::test]
  async fn test_acquire_release() {
    let mut slab = Slab::<String>::with_capacity(5);

    // Acquire a slot
    let s_ref = slab.acquire().await.unwrap();
    assert_eq!(s_ref.handler, 1); // handlers start at 1, and are given in reverse order
    assert_eq!(slab.len().await, 1);

    // Release the slot
    s_ref.release().await;
    assert_eq!(slab.len().await, 0);
  }

  #[tokio::test]
  async fn test_acquire_all_slots() {
    let mut slab = Slab::<u32>::with_capacity(3);

    // Acquire all slots
    let s_ref_1 = slab.acquire().await.unwrap();
    let s_ref_2 = slab.acquire().await.unwrap();
    let s_ref_3 = slab.acquire().await.unwrap();

    // Verify handlers
    assert_eq!(s_ref_1.handler, 1);
    assert_eq!(s_ref_2.handler, 2);
    assert_eq!(s_ref_3.handler, 3);

    // Try to acquire when full
    let s_ref_4 = slab.acquire().await;
    assert!(s_ref_4.is_none());

    // Release one slot
    s_ref_2.release().await;

    // Should be able to acquire again
    let s_ref_5 = slab.acquire().await;
    assert!(s_ref_5.is_some());
    assert_eq!(s_ref_5.unwrap().handler, 2);
  }

  #[tokio::test]
  async fn test_read_write() {
    let mut slab = Slab::<String>::with_capacity(1);
    let s_ref = slab.acquire().await.unwrap();

    // Write to the slot
    {
      let mut data = s_ref.write().await;
      *data = "Hello, world!".to_string();
    }

    // Read from the slot
    {
      let data = s_ref.read().await;
      assert_eq!(*data, "Hello, world!".to_string());
    }
  }

  #[tokio::test]
  async fn test_default_value() {
    let mut slab = Slab::<String>::with_capacity(1);
    let s_ref = slab.acquire().await.unwrap();

    // Verify default value
    {
      let data = s_ref.read().await;
      assert_eq!(*data, String::default());
    }
  }

  #[tokio::test]
  async fn test_reset_on_release() {
    let mut slab = Slab::<String>::with_capacity(1);

    // Acquire, modify and release
    {
      let s_ref = slab.acquire().await.unwrap();
      {
        let mut data = s_ref.write().await;
        *data = "Modified data".to_string();
      }
      s_ref.release().await;
    }

    // Acquire again and verify it's reset
    {
      let s_ref = slab.acquire().await.unwrap();
      let data = s_ref.read().await;
      assert_eq!(*data, String::default());
    }
  }

  #[tokio::test]
  async fn test_concurrent_access() {
    let slab = Slab::<u32>::with_capacity(5);
    let mut slab1 = slab.clone();
    let mut slab2 = slab.clone();

    // Spawn two tasks that acquire/release slots concurrently
    let task1 = tokio::spawn(async move {
      let mut refs = Vec::new();
      for _ in 0..2 {
        if let Some(s_ref) = slab1.acquire().await {
          refs.push(s_ref);
        }
      }
      refs
    });

    let task2 = tokio::spawn(async move {
      let mut refs = Vec::new();
      for _ in 0..2 {
        if let Some(s_ref) = slab2.acquire().await {
          refs.push(s_ref);
        }
      }
      refs
    });

    let s_ref_1 = task1.await.unwrap();
    let s_ref_2 = task2.await.unwrap();

    // We should have been able to acquire 4 slots in total
    assert_eq!(s_ref_1.len() + s_ref_2.len(), 4);

    // Verify the slab length
    assert_eq!(slab.len().await, 4);
  }

  #[tokio::test]
  async fn test_concurrent_read_write() {
    let mut slab = Slab::<u32>::with_capacity(1);
    let s_ref = slab.acquire().await.unwrap();

    // Write initial value
    {
      let mut data = s_ref.write().await;
      *data = 42;
    }

    let s_ref_clone = s_ref.clone();

    // Spawn a task that reads the value
    let read_task = tokio::spawn(async move {
      let data = s_ref_clone.read().await;
      *data
    });

    // Wait for the read task and check the result
    let read_value = read_task.await.unwrap();
    assert_eq!(read_value, 42);
  }

  #[tokio::test]
  async fn test_reuse_released_slots() {
    let mut slab = Slab::<u32>::with_capacity(3);

    // Acquire all slots
    let s_ref_1 = slab.acquire().await.unwrap();
    let s_ref_2 = slab.acquire().await.unwrap();
    let s_ref_3 = slab.acquire().await.unwrap();

    // Modify values
    {
      let mut data = s_ref_1.write().await;
      *data = 100;
    }
    {
      let mut data = s_ref_2.write().await;
      *data = 200;
    }
    {
      let mut data = s_ref_3.write().await;
      *data = 300;
    }

    // Release slots in different order
    s_ref_2.release().await; // Release middle slot
    s_ref_1.release().await; // Release first slot

    // Acquire slots again - they should be reused in LIFO order
    let s_ref_4 = slab.acquire().await.unwrap();
    let s_ref_5 = slab.acquire().await.unwrap();

    // Check handlers and that values were reset
    assert_eq!(s_ref_4.handler, 1); // Last released
    assert_eq!(s_ref_5.handler, 2); // Released before last

    {
      let data = s_ref_4.read().await;
      assert_eq!(*data, 0); // Should be reset to default
    }
    {
      let data = s_ref_5.read().await;
      assert_eq!(*data, 0); // Should be reset to default
    }

    // The unreleased slot should still have its value
    {
      let data = s_ref_3.read().await;
      assert_eq!(*data, 300);
    }
  }
}
