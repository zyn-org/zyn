// SPDX-License-Identifier: AGPL-3.0-only

use std::fmt;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;
use rand::Rng;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Maximum number of buckets supported in a buffered pool
const MAX_POOL_BUCKETS: usize = 64;

/// A generic buffer pool that allows sharing buffers across threads.
#[derive(Clone)]
pub struct Pool(Arc<PoolInner>);

/// The inner state of a pool.
struct PoolInner {
  /// Queue of available buffers.
  available: ArrayQueue<BytesMut>,

  /// Semaphore for limiting the number of in-use buffers.
  sem: Arc<Semaphore>,

  /// Size of each buffer.
  buffer_size: usize,
}

// ==== impl Pool =====

impl Pool {
  /// Create a new buffer pool with the specified number of buffers and buffer size
  pub fn new(count: usize, buffer_size: usize) -> Self {
    Self::from_buffer(count, buffer_size, BytesMut::zeroed(buffer_size * count))
  }

  /// Creates a new buffer pool from a pre-allocated BytesMut buffer.
  fn from_buffer(count: usize, buffer_size: usize, mut buffer: BytesMut) -> Self {
    assert!(buffer_size * count <= buffer.len(), "buffer size is too small");

    let available = ArrayQueue::new(count);
    for _ in 0..count {
      available.force_push(buffer.split_to(buffer_size));
    }
    let sem = Semaphore::new(count);

    let inner = PoolInner { available, sem: Arc::new(sem), buffer_size };

    Self(Arc::new(inner))
  }

  /// Get a mutable buffer from the pool.
  pub async fn acquire(&self) -> MutablePoolBuffer {
    let permit = self.0.sem.clone().acquire_owned().await.unwrap();
    let buffer = self.0.available.pop().unwrap();

    MutablePoolBuffer(PoolBufferInner { data: Some(buffer), pool: Some(self.0.clone()), permit: Some(permit) })
  }

  /// Get the number of buffers currently in use.
  pub fn in_use_count(&self) -> usize {
    self.0.available.capacity() - self.0.available.len()
  }

  /// Get the number of available buffers.
  pub fn available_count(&self) -> usize {
    self.0.available.len()
  }

  /// Check if there are any available buffers.
  pub fn has_available(&self) -> bool {
    self.available_count() > 0
  }

  /// Get the size of the buffers in the pool.
  pub fn buffer_size(&self) -> usize {
    self.0.buffer_size
  }
}

impl std::fmt::Debug for Pool {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Pool")
      .field("in_use_count", &self.in_use_count())
      .field("available_count", &self.available_count())
      .field("buffer_size", &self.buffer_size())
      .finish()
  }
}

/// A pool that manages multiple buckets of different sized buffers
#[derive(Clone)]
pub struct BucketedPool {
  buckets: Arc<[Pool]>,
}

// ==== impl BucketedPool =====

impl BucketedPool {
  /// Create a new bucketed pool with memory budget and exponential decay distribution.
  ///
  /// # Arguments
  ///
  /// * `min_buffer_size` - The minimum buffer size for the smallest bucket
  /// * `max_buffer_size` - The maximum buffer size for the largest bucket
  /// * `memory_budget` - The total memory budget in bytes across all buckets
  /// * `max_buffers_per_bucket` - The maximum number of buffers per bucket
  /// * `growth_factor` - The growth factor for each bucket (e.g., 2 means each bucket has buffers 2x the size of the previous)
  /// * `decay_factor` - The fraction of memory allocated to each bucket (e.g., 0.5 means each bucket gets 50% of remaining budget)
  pub fn new_with_memory_budget(
    min_buffer_size: usize,
    max_buffer_size: usize,
    memory_budget: usize,
    max_buffers: usize,
    growth_factor: usize,
    decay_factor: f64,
  ) -> Self {
    assert!(min_buffer_size > 0, "minimum buffer size must be greater than 0");
    assert!(max_buffer_size > 0, "maximum buffer size must be greater than 0");
    assert!(
      min_buffer_size <= max_buffer_size,
      "minimum buffer size must be less than or equal to maximum buffer size"
    );
    assert!(growth_factor > 1, "growth factor must be greater than 1");
    assert!(memory_budget > 0, "memory budget must be greater than 0");
    assert!(decay_factor > 0.0 && decay_factor < 1.0, "decay factor must be between 0.0 and 1.0");

    // First pass: calculate initial bucket sizes and track unused budget
    let mut bucket_configs = Vec::new();
    let mut current_buffer_size = min_buffer_size;
    let mut remaining_budget = memory_budget;
    let mut total_unused_budget = 0;

    // Calculate how many buckets we need and distribute memory budget with exponential decay
    while current_buffer_size <= max_buffer_size && remaining_budget >= current_buffer_size {
      let bucket_budget = if bucket_configs.is_empty() {
        ((memory_budget as f64) * decay_factor).floor() as usize
      } else {
        (remaining_budget as f64 * decay_factor).floor() as usize
      };

      // Ensure bucket budget is at least one buffer's worth and doesn't exceed remaining
      let bucket_budget = bucket_budget.max(current_buffer_size).min(remaining_budget);

      // Calculate how many buffers fit in this bucket's budget
      let uncapped_buffers = bucket_budget / current_buffer_size;
      let bucket_buffers = uncapped_buffers.min(max_buffers);

      if bucket_buffers > 0 {
        // Track unused budget due to capping
        let actual_used = bucket_buffers * current_buffer_size;
        let allocated_budget = uncapped_buffers * current_buffer_size;
        total_unused_budget += allocated_budget.saturating_sub(actual_used);

        bucket_configs.push((bucket_buffers, current_buffer_size));
        remaining_budget -= allocated_budget.min(bucket_budget);
      }

      current_buffer_size *= growth_factor;
    }

    // Add any remaining budget to the unused pool
    total_unused_budget += remaining_budget;

    // Second pass: redistribute unused budget evenly to uncapped buckets
    if total_unused_budget > 0 && !bucket_configs.is_empty() {
      // Find all buckets that haven't reached the cap
      let uncapped_buckets: Vec<usize> =
        bucket_configs.iter().enumerate().filter(|(_, (count, _))| *count < max_buffers).map(|(i, _)| i).collect();

      if !uncapped_buckets.is_empty() {
        // Distribute unused budget evenly among uncapped buckets
        let budget_per_bucket = total_unused_budget / uncapped_buckets.len();

        for &i in &uncapped_buckets {
          let (current_count, buffer_size) = bucket_configs[i];

          // Calculate how many additional buffers this bucket can take
          let additional_buffers = (budget_per_bucket / buffer_size).min(max_buffers - current_count);

          if additional_buffers > 0 {
            bucket_configs[i].0 += additional_buffers;
            total_unused_budget -= additional_buffers * buffer_size;
          }
        }

        // If there's still budget left (due to rounding), do another pass
        // giving one more buffer to each uncapped bucket until budget is exhausted
        for &i in &uncapped_buckets {
          if total_unused_budget == 0 {
            break;
          }

          let (current_count, buffer_size) = bucket_configs[i];
          if current_count < max_buffers && total_unused_budget >= buffer_size {
            bucket_configs[i].0 += 1;
            total_unused_budget -= buffer_size;
          }
        }
      }
    }

    // Only allocate what we'll actually use, not the full budget
    let actual_memory_to_allocate =
      bucket_configs.iter().map(|(count, size)| count * size).sum::<usize>().min(memory_budget);

    let mut master_buffer = BytesMut::zeroed(actual_memory_to_allocate);

    // Create the actual pools from the configurations
    let mut buckets = Vec::with_capacity(bucket_configs.len());
    for (count, size) in bucket_configs {
      let bucket_total_size = count * size;

      let bucket_buffer = master_buffer.split_to(bucket_total_size);

      buckets.push(Pool::from_buffer(count, size, bucket_buffer));
    }

    assert!(
      buckets.len() <= MAX_POOL_BUCKETS,
      "number of buckets ({}) exceeds MAX_POOL_BUCKETS ({})",
      buckets.len(),
      MAX_POOL_BUCKETS
    );

    Self { buckets: Arc::from(buckets) }
  }

  /// Acquire a mutable buffer of at least the requested size
  /// Returns `None` if no bucket can satisfy the requested size.
  ///
  /// # Arguments
  ///
  /// * `size` - The minimum size of the buffer to acquire
  ///
  /// # Returns
  ///
  /// * `Some(MutablePoolBuffer)` - A buffer of at least the requested size
  /// * `None` - If the requested size exceeds all configured bucket sizes
  pub async fn acquire(&self, size: usize) -> Option<MutablePoolBuffer> {
    // Collect all buckets that can satisfy the request
    let mut suitable_buckets: [Option<&Pool>; MAX_POOL_BUCKETS] = [None; MAX_POOL_BUCKETS];
    let mut count = 0;

    for bucket in self.buckets.iter() {
      if bucket.buffer_size() < size {
        continue;
      }
      // If the bucket has available buffers, try to acquire one and return it
      if bucket.has_available() {
        return Some(bucket.acquire().await);
      }
      // Keep track of all buckets that can satisfy the request
      assert!(count < MAX_POOL_BUCKETS);

      suitable_buckets[count] = Some(bucket);
      count += 1;
    }

    if count == 0 {
      return None;
    }

    // If none of the buckets that could satisfy the request have available buffers,
    // pick one at random and wait until one of the buffers currently in use is returned
    // to the pool
    let idx = rand::rng().random_range(0..count);
    let bucket = suitable_buckets[idx].unwrap();

    Some(bucket.acquire().await)
  }

  /// Get the total number of buffers currently in use across all buckets
  pub fn total_in_use_count(&self) -> usize {
    self.buckets.iter().map(|pool| pool.in_use_count()).sum()
  }

  /// Get the total number of available buffers across all buckets
  pub fn total_available_count(&self) -> usize {
    self.buckets.iter().map(|pool| pool.available_count()).sum()
  }
}

impl std::fmt::Debug for BucketedPool {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("BucketedPool")
      .field("num_buckets", &self.buckets.len())
      .field("total_in_use", &self.total_in_use_count())
      .field("total_available", &self.total_available_count())
      .finish()
  }
}

/// The inner state of a pool buffer.
struct PoolBufferInner {
  /// The actual buffer data.
  data: Option<BytesMut>,

  /// Reference to the pool for returning the buffer.
  pool: Option<Arc<PoolInner>>,

  /// Semaphore permit for the buffer.
  permit: Option<OwnedSemaphorePermit>,
}

impl Debug for PoolBufferInner {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    f.debug_struct("PoolBufferInner").field("data", &self.data.as_ref().map(|d| d.len())).finish()
  }
}

// ==== impl PoolBufferInner =====

impl PoolBufferInner {
  /// Get a read-only reference to the buffer data.
  fn as_slice(&self) -> &[u8] {
    assert!(self.data.is_some());
    let data = self.data.as_ref().unwrap();
    data.deref()
  }

  /// Get a mutable reference to the buffer data.
  fn as_mut_slice(&mut self) -> &mut [u8] {
    assert!(self.data.is_some());
    let data = self.data.as_mut().unwrap();
    data.deref_mut()
  }

  /// Return the length of the buffer.
  fn len(&self) -> usize {
    assert!(self.data.is_some());
    let data = self.data.as_ref().unwrap();
    data.len()
  }
}

impl Drop for PoolBufferInner {
  fn drop(&mut self) {
    if let (Some(buffer), Some(pool)) = (self.data.take(), self.pool.take()) {
      pool.available.force_push(buffer);
    }
  }
}

/// A mutable buffer from the pool.
/// This buffer can be converted into a shared read-only buffer.
#[derive(Debug)]
pub struct MutablePoolBuffer(PoolBufferInner);

// ==== impl MutablePoolBuffer =====

impl MutablePoolBuffer {
  /// Get a read-only reference to the buffer data.
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    self.0.as_slice()
  }

  /// Get a mutable reference to the buffer data.
  #[inline]
  pub fn as_mut_slice(&mut self) -> &mut [u8] {
    self.0.as_mut_slice()
  }

  /// Return the length of the buffer.
  #[inline]
  #[allow(clippy::len_without_is_empty)]
  pub fn len(&self) -> usize {
    self.0.len()
  }

  /// Convert a mutable buffer into a shareable read-only buffer.
  pub fn freeze(&mut self, size: usize) -> PoolBuffer {
    let data = self.0.data.take();
    let pool = self.0.pool.take();
    let permit = self.0.permit.take();

    let inner = PoolBufferInner { data, pool, permit };

    PoolBuffer { inner: Arc::new(inner), len: size }
  }
}

/// A read-only buffer from the pool.
#[derive(Clone, Debug)]
pub struct PoolBuffer {
  /// The inner state of the buffer.
  inner: Arc<PoolBufferInner>,

  /// The length of the buffer.
  len: usize,
}

// ==== impl PoolBuffer =====

impl PoolBuffer {
  /// Get a read-only reference to the buffer data.
  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    &self.inner.as_slice()[..self.len]
  }

  /// Get the length of the buffer.
  #[inline]
  pub fn len(&self) -> usize {
    self.len
  }

  /// Tells whether the buffer is empty.
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len == 0
  }
}

impl Deref for PoolBuffer {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}

#[cfg(test)]
mod pool_tests {
  use super::*;

  #[tokio::test]
  async fn test_buffer_pool() {
    let pool = Pool::new(5, 1024);

    // Check initial state
    assert_eq!(pool.in_use_count(), 0);
    assert_eq!(pool.available_count(), 5);

    // Get some buffers
    let buf1 = pool.acquire().await;
    let buf2 = pool.acquire().await;

    assert_eq!(pool.in_use_count(), 2);
    assert_eq!(pool.available_count(), 3);

    // Return a buffer
    drop(buf1);

    assert_eq!(pool.in_use_count(), 1);
    assert_eq!(pool.available_count(), 4);

    drop(buf2);
  }

  #[tokio::test]
  async fn test_shared_buffer_conversion() {
    let pool = Pool::new(1, 1024);
    let mut buf = pool.acquire().await;

    // Write some data
    buf.as_mut_slice()[0] = 42;

    // Convert to shared buffer
    let shared_buf = buf.freeze(100);

    // Verify data is preserved
    assert_eq!(shared_buf.as_slice()[0], 42);
    assert_eq!(shared_buf.len(), 100);

    // Original buffer should be consumed (freeze consumes it)
    // Note: buf is moved by freeze(), so we can't access it here
  }

  #[tokio::test]
  async fn test_multithreaded() {
    use std::sync::Arc;
    use std::thread;

    let pool = Arc::new(Pool::new(10, 1024));
    let mut handles = vec![];

    for i in 0..10 {
      let pool_clone = pool.clone();
      let handle = thread::spawn(async move || {
        let mut buf = pool_clone.acquire().await;
        buf.as_mut_slice()[0] = i as u8;
        assert_eq!(buf.as_slice()[0], i as u8);
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap().await;
    }

    // All buffers should be returned
    assert_eq!(pool.in_use_count(), 0);
    assert_eq!(pool.available_count(), 10);
  }
}

#[cfg(test)]
mod bucketed_pool_tests {
  use super::*;
  use std::thread;

  #[test]
  fn test_bucketed_pool_with_memory_budget_creation() {
    // Test memory budget constructor with 100MB, 10K max buffers, 50% decay
    let pool = BucketedPool::new_with_memory_budget(
      1024,              // 1KB min
      131072,            // 128KB max
      100 * 1024 * 1024, // 100MB budget
      10000,             // max 10K buffers per bucket
      2,                 // 2x growth
      0.5,               // 50% decay
    );

    // Should create multiple buckets with exponential decay distribution
    assert!(pool.buckets.len() > 1);

    // Total memory used should not exceed budget
    let mut total_memory = 0;
    for bucket in pool.buckets.iter() {
      total_memory += bucket.available_count() * bucket.buffer_size();
    }
    assert!(total_memory <= 100 * 1024 * 1024);
  }

  #[test]
  fn test_memory_budget_distribution() {
    // Test with 10MB budget
    let pool = BucketedPool::new_with_memory_budget(
      4096,             // 4KB min
      65536,            // 64KB max
      10 * 1024 * 1024, // 10MB budget
      1000,             // max 1000 buffers per bucket
      2,                // 2x growth
      1.0 / 3.0,        // 33% decay
    );

    // Calculate actual memory usage
    let mut total_memory = 0;
    for bucket in pool.buckets.iter() {
      total_memory += bucket.available_count() * bucket.buffer_size();
    }

    // Should use most of the budget but not exceed it
    assert!(total_memory <= 10 * 1024 * 1024);
    assert!(total_memory >= 8 * 1024 * 1024); // Should use at least 80% of budget
  }

  #[test]
  fn test_max_buffers_cap() {
    // Test that max_buffers parameter properly caps each bucket
    let pool = BucketedPool::new_with_memory_budget(
      1024,              // 1KB min
      8192,              // 8KB max
      100 * 1024 * 1024, // 100MB budget (very large)
      100,               // max 100 buffers per bucket
      2,                 // 2x growth
      0.5,               // 50% decay
    );

    // No bucket should have more than 100 buffers
    for bucket in pool.buckets.iter() {
      assert!(bucket.available_count() <= 100);
    }
  }

  #[test]
  fn test_small_memory_budget() {
    // Test with small memory budget
    let pool = BucketedPool::new_with_memory_budget(
      4096,       // 4KB min
      16384,      // 16KB max
      100 * 1024, // 100KB budget
      100,        // max 100 buffers per bucket
      2,          // 2x growth
      0.5,        // 50% decay
    );

    // Should still create buckets within budget
    let mut total_memory = 0;
    for bucket in pool.buckets.iter() {
      total_memory += bucket.available_count() * bucket.buffer_size();
    }
    assert!(total_memory <= 100 * 1024);
    assert!(!pool.buckets.is_empty());
  }

  #[test]
  fn test_memory_budget_edge_cases() {
    // Test with aggressive decay
    let pool = BucketedPool::new_with_memory_budget(
      1024,        // 1KB
      8192,        // 8KB
      1024 * 1024, // 1MB budget
      1000,        // max buffers
      2,           // 2x growth
      0.9,         // 90% decay
    );

    // First bucket should get most of the budget
    let first_bucket_memory = pool.buckets[0].available_count() * pool.buckets[0].buffer_size();
    assert!(first_bucket_memory >= 800 * 1024); // At least 80% of budget

    // Test with minimal decay
    let pool = BucketedPool::new_with_memory_budget(
      1024,        // 1KB
      8192,        // 8KB
      1024 * 1024, // 1MB budget
      1000,        // max buffers
      2,           // 2x growth
      0.1,         // 10% decay
    );

    // Budget should be more evenly distributed
    let total_buckets = pool.buckets.len();
    assert!(total_buckets >= 3); // Should have several buckets
  }

  #[test]
  fn test_budget_redistribution_when_capped() {
    // Test that unused budget from capped buckets gets redistributed
    let pool = BucketedPool::new_with_memory_budget(
      4096,              // 4KB min
      65536,             // 64KB max
      256 * 1024 * 1024, // 256MB budget
      10000,             // max 10K buffers per bucket (will cap first bucket)
      2,                 // 2x growth
      1.0 / 3.0,         // 33% decay
    );

    // Calculate actual memory usage
    let mut total_memory = 0;
    let mut buffer_counts = Vec::new();

    for bucket in pool.buckets.iter() {
      let count = bucket.available_count();
      let size = bucket.buffer_size();
      total_memory += count * size;
      buffer_counts.push((size, count));
    }

    // First bucket (4KB) should be capped at 10,000
    assert_eq!(buffer_counts[0].1, 10000, "First bucket should be capped at max_buffers");

    // Total memory should use most of the budget (at least 95%)
    let budget = 256 * 1024 * 1024;
    assert!(
      total_memory >= (budget * 95 / 100),
      "Should use at least 95% of budget. Used: {}MB of {}MB",
      total_memory / 1024 / 1024,
      budget / 1024 / 1024
    );

    // Check that multiple buckets got redistribution (not just the last one)
    // With 256MB and first bucket capped, the redistribution should spread across other buckets
    let mut buckets_with_extra = 0;

    // Expected counts without redistribution (roughly):
    // 4KB: 10000 (capped), 8KB: ~7281, 16KB: ~2427, 32KB: ~809, 64KB: ~269
    // With redistribution of ~79MB spread evenly, each uncapped should get more

    if buffer_counts.len() > 1 && buffer_counts[1].1 > 8000 {
      buckets_with_extra += 1; // 8KB bucket got extra
    }
    if buffer_counts.len() > 2 && buffer_counts[2].1 > 3000 {
      buckets_with_extra += 1; // 16KB bucket got extra
    }
    if buffer_counts.len() > 3 && buffer_counts[3].1 > 1000 {
      buckets_with_extra += 1; // 32KB bucket got extra
    }
    if buffer_counts.len() > 4 && buffer_counts[4].1 > 500 {
      buckets_with_extra += 1; // 64KB bucket got extra
    }

    assert!(
      buckets_with_extra >= 2,
      "At least 2 buckets should have gotten redistribution. Buffer counts: {:?}",
      buffer_counts
    );
  }

  #[test]
  #[should_panic(expected = "decay factor must be between 0.0 and 1.0")]
  fn test_invalid_memory_budget_decay_zero() {
    BucketedPool::new_with_memory_budget(1024, 8192, 1024 * 1024, 100, 2, 0.0);
  }

  #[test]
  #[should_panic(expected = "decay factor must be between 0.0 and 1.0")]
  fn test_invalid_memory_budget_decay_one() {
    BucketedPool::new_with_memory_budget(1024, 8192, 1024 * 1024, 100, 2, 1.0);
  }

  #[test]
  #[should_panic(expected = "memory budget must be greater than 0")]
  fn test_invalid_zero_budget() {
    BucketedPool::new_with_memory_budget(1024, 8192, 0, 100, 2, 0.5);
  }

  #[tokio::test]
  async fn test_acquire_appropriate_bucket() {
    // Test that acquire() returns the right bucket size for different requests
    let pool = BucketedPool::new_with_memory_budget(100, 800, 100 * 1024, 100, 2, 0.5);

    // Request 50 bytes - should get from bucket 0 (100 bytes)
    let buf1 = pool.acquire(50).await.unwrap();
    assert_eq!(buf1.len(), 100);

    // Request 150 bytes - should get from bucket 1 (200 bytes)
    let buf2 = pool.acquire(150).await.unwrap();
    assert_eq!(buf2.len(), 200);

    // Request 350 bytes - should get from bucket 2 (400 bytes)
    let buf3 = pool.acquire(350).await.unwrap();
    assert_eq!(buf3.len(), 400);

    // Request 700 bytes - should get from bucket 3 (800 bytes)
    let buf4 = pool.acquire(700).await.unwrap();
    assert_eq!(buf4.len(), 800);
  }

  #[tokio::test]
  async fn test_no_available_for_oversized_requests() {
    // Test that oversized requests return None
    let pool = BucketedPool::new_with_memory_budget(100, 400, 10 * 1024, 100, 2, 0.5);

    // Request 500 bytes - larger than max bucket (400), should return None
    let buf = pool.acquire(500).await;
    assert!(buf.is_none());

    // Verify pool stats remain unchanged
    let initial_available = pool.total_available_count();
    assert_eq!(pool.total_available_count(), initial_available);
  }

  #[tokio::test]
  async fn test_bucket_reuse() {
    // Test that buffers are properly returned to their buckets
    let pool = BucketedPool::new_with_memory_budget(100, 400, 10 * 1024, 100, 2, 0.5);

    let initial_available = pool.total_available_count();
    let initial_in_use = pool.total_in_use_count();

    // Acquire and release buffers
    {
      let _buf1 = pool.acquire(50).await.unwrap();
      let _buf2 = pool.acquire(150).await.unwrap();
      let _buf3 = pool.acquire(350).await.unwrap();

      assert_eq!(pool.total_in_use_count(), 3);
      assert!(pool.total_available_count() < initial_available);
    }

    // After dropping, buffers should be returned to pool
    assert_eq!(pool.total_available_count(), initial_available);
    assert_eq!(pool.total_in_use_count(), initial_in_use);
  }

  #[tokio::test]
  async fn test_multithreaded_bucketed_pool() {
    // Test concurrent access to the bucketed pool
    let pool = Arc::new(BucketedPool::new_with_memory_budget(1024, 4096, 200 * 1024, 100, 2, 0.5));
    let mut handles = vec![];

    for i in 0..10 {
      let pool_clone = pool.clone();
      let handle = thread::spawn(async move || {
        // Each thread requests a different size (max 4000 to stay within 4096 limit)
        let size = 400 + i * 350;
        let mut buf = pool_clone.acquire(size).await.unwrap();

        // Write some data
        buf.as_mut_slice()[0] = i as u8;

        // Verify we can read it back
        assert_eq!(buf.as_slice()[0], i as u8);

        // Simulate some work
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap().await;
    }

    // All pool-managed buffers should be returned
    assert_eq!(pool.total_in_use_count(), 0);
  }

  #[tokio::test]
  async fn test_stats_tracking() {
    // Test that statistics are properly tracked across all buckets
    let pool = BucketedPool::new_with_memory_budget(100, 800, 10 * 1024, 100, 2, 0.5);

    // Check initial state
    assert_eq!(pool.total_in_use_count(), 0);
    let initial_available = pool.total_available_count();
    assert!(initial_available > 0);

    // Acquire some buffers
    let buf1 = pool.acquire(50).await.unwrap(); // From first bucket
    let buf2 = pool.acquire(150).await.unwrap(); // From second bucket
    let buf3 = pool.acquire(350).await.unwrap(); // From third bucket

    assert_eq!(pool.total_in_use_count(), 3);
    assert_eq!(pool.total_available_count(), initial_available - 3);

    // Return one buffer
    drop(buf1);
    assert_eq!(pool.total_in_use_count(), 2);
    assert_eq!(pool.total_available_count(), initial_available - 2);

    // Return remaining buffers
    drop(buf2);
    drop(buf3);
    assert_eq!(pool.total_in_use_count(), 0);
    assert_eq!(pool.total_available_count(), initial_available);
  }

  #[tokio::test]
  async fn test_edge_case_exact_size_match() {
    // Create pool with enough buffers for all requests
    let pool = BucketedPool::new_with_memory_budget(100, 400, 10 * 1024, 100, 2, 0.5);

    // Request exactly the bucket sizes
    let buf1 = pool.acquire(100).await.unwrap();
    assert_eq!(buf1.len(), 100);

    let buf2 = pool.acquire(200).await.unwrap();
    assert_eq!(buf2.len(), 200);

    let buf3 = pool.acquire(400).await.unwrap();
    assert_eq!(buf3.len(), 400);
  }

  #[tokio::test]
  async fn test_large_growth_factor() {
    // Test with larger growth factor
    let pool = BucketedPool::new_with_memory_budget(100, 1600, 100 * 1024, 100, 4, 0.5);

    // Should create buckets with 4x growth
    assert!(pool.buckets.len() >= 2);

    // Verify bucket sizes follow the growth factor
    if pool.buckets.len() >= 2 {
      let first_bucket_size = pool.buckets[0].buffer_size();
      let second_bucket_size = pool.buckets[1].buffer_size();
      assert_eq!(second_bucket_size, first_bucket_size * 4);
    }
  }

  #[tokio::test]
  async fn test_mixed_pool_and_none() {
    // Test mix of pool allocations and None for oversized
    let pool = BucketedPool::new_with_memory_budget(100, 200, 10 * 1024, 100, 2, 0.5);

    let initial_available = pool.total_available_count();

    // Mix of pool allocations and None
    let buf1 = pool.acquire(50).await.unwrap(); // Pool: bucket 0
    let buf2 = pool.acquire(150).await.unwrap(); // Pool: bucket 1
    let buf3 = pool.acquire(300).await; // None: too large

    assert_eq!(buf1.len(), 100);
    assert_eq!(buf2.len(), 200);
    assert!(buf3.is_none());

    // Track stats before dropping
    let in_use_before = pool.total_in_use_count();
    assert_eq!(in_use_before, 2); // Only pool-managed buffers

    // Drop pool-managed buffers
    drop(buf1);
    drop(buf2);
    assert_eq!(pool.total_in_use_count(), 0);
    assert_eq!(pool.total_available_count(), initial_available);
  }

  #[test]
  fn test_debug_formatting() {
    // Test that debug formatting works correctly
    let pool = BucketedPool::new_with_memory_budget(100, 400, 10 * 1024, 100, 2, 0.5);
    let debug_str = format!("{:?}", pool);

    // Should contain basic pool information
    assert!(debug_str.contains("BucketedPool"));
    assert!(debug_str.contains("num_buckets"));
    assert!(debug_str.contains("total_in_use"));
    assert!(debug_str.contains("total_available"));
  }

  #[tokio::test]
  async fn test_clone_behavior() {
    // Test that cloning works correctly
    let pool = BucketedPool::new_with_memory_budget(100, 400, 10 * 1024, 100, 2, 0.5);
    let cloned_pool = pool.clone();

    // Both should have the same initial state
    assert_eq!(pool.total_available_count(), cloned_pool.total_available_count());
    assert_eq!(pool.total_in_use_count(), cloned_pool.total_in_use_count());

    // Acquire from one pool
    let buf = pool.acquire(50).await.unwrap();
    assert_eq!(pool.total_in_use_count(), 1);
    // Since BucketedPool uses Arc internally, both clones share the same state
    assert_eq!(cloned_pool.total_in_use_count(), 1);

    drop(buf);

    // After dropping, both should show 0 in use
    assert_eq!(pool.total_in_use_count(), 0);
    assert_eq!(cloned_pool.total_in_use_count(), 0);
  }

  #[tokio::test]
  async fn test_fallback_to_next_bucket() {
    // Test that when a bucket is exhausted, we fall back to the next larger bucket
    // Create pool with small memory budget to ensure limited buffers per bucket
    let pool = BucketedPool::new_with_memory_budget(100, 400, 1000, 5, 2, 0.5);

    // With 1KB budget and 50% decay:
    // First bucket (100 bytes) should get ~500 bytes -> 5 buffers (capped at 5)
    // Second bucket (200 bytes) should get ~250 bytes -> 1 buffer
    // Third bucket (400 bytes) should get ~125 bytes -> 0 buffers (not enough)

    // Verify we have some buffers available
    let initial_available = pool.total_available_count();
    assert!(initial_available > 0);

    // Acquire multiple 50-byte buffers (should come from 100-byte bucket)
    let mut small_buffers = Vec::new();
    let mut count_from_100 = 0;

    // Keep acquiring until we get a 200-byte buffer (indicating 100-byte bucket is exhausted)
    loop {
      let buf = pool.acquire(50).await.unwrap();
      if buf.len() == 200 {
        // We've fallen back to the next bucket
        assert!(count_from_100 > 0, "should have gotten at least one buffer from 100-byte bucket");
        drop(buf);
        break;
      }
      assert_eq!(buf.len(), 100);
      count_from_100 += 1;
      small_buffers.push(buf);

      if count_from_100 > 20 {
        panic!("too many buffers from 100-byte bucket");
      }
    }

    // Now verify that subsequent small requests also fall back
    let buf2 = pool.acquire(50).await.unwrap();
    assert_eq!(buf2.len(), 200, "should still get from 200-byte bucket");
    drop(buf2);

    // Acquire all remaining buffers
    let mut remaining_buffers = Vec::new();
    while let Some(buf) = pool.acquire(50).await {
      remaining_buffers.push(buf);
    }

    // Now all buckets should be exhausted
    let buf = pool.acquire(50).await;
    assert!(buf.is_none(), "all buckets should be exhausted");

    // Clean up
    drop(small_buffers);
    drop(remaining_buffers);
  }

  #[tokio::test]
  async fn test_excessive_budget_all_buckets_capped() {
    // Test scenario where budget is so large that all buckets hit max_buffers cap
    // This verifies that excess budget is properly handled and doesn't cause issues
    let max_buffers = 100;
    let pool = BucketedPool::new_with_memory_budget(
      1024,                    // 1KB min
      16384,                   // 16KB max
      10 * 1024 * 1024 * 1024, // 10GB budget (excessive)
      max_buffers,             // max 100 buffers per bucket
      2,                       // 2x growth
      0.5,                     // 50% decay
    );

    // Calculate expected buckets: 1KB, 2KB, 4KB, 8KB, 16KB = 5 buckets
    assert_eq!(pool.buckets.len(), 5, "should have 5 buckets");

    // Verify all buckets are capped at max_buffers
    let mut total_memory_used = 0;
    let mut all_capped = true;

    for bucket in pool.buckets.iter() {
      let count = bucket.available_count();
      let size = bucket.buffer_size();

      // Each bucket should be at max_buffers
      if count != max_buffers {
        all_capped = false;
      }
      total_memory_used += count * size;
    }

    assert!(all_capped, "all buckets should be capped at max_buffers");

    // Verify total memory used is much less than the budget
    let budget = 10 * 1024 * 1024 * 1024; // 10GB
    let expected_max_memory = (1024 + 2048 + 4096 + 8192 + 16384) * max_buffers; // ~3.1MB

    assert_eq!(total_memory_used, expected_max_memory, "total memory should be exactly the sum of all capped buckets");

    assert!(
      total_memory_used < budget / 1000,
      "memory used ({} MB) should be much less than budget ({} MB)",
      total_memory_used / 1024 / 1024,
      budget / 1024 / 1024
    );

    // Verify the pool still works correctly
    let buf1 = pool.acquire(500).await.unwrap();
    assert_eq!(buf1.len(), 1024);

    let buf2 = pool.acquire(3000).await.unwrap();
    assert_eq!(buf2.len(), 4096);

    let buf3 = pool.acquire(10000).await.unwrap();
    assert_eq!(buf3.len(), 16384);

    // Track available before cleanup
    let available_before = pool.total_available_count();

    drop(buf1);
    drop(buf2);
    drop(buf3);

    // Verify buffers are returned
    assert_eq!(pool.total_available_count(), available_before + 3);
    assert_eq!(pool.total_in_use_count(), 0);
  }
}
