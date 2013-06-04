package org.apache.hadoop.hbase.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class that can be used to implement
 * queues with limited capacity (in terms of memory).
 * It maintains internal counter and provides
 * two operations: increase and decrease.
 * Increase blocks until internal counter is lower than
 * given threshold and then increases internal counter.
 * Decrease decreases internal counter and wakes up
 * waiting threads if counter is lower than threshold.
 *
 * This implementation allows you to set the value of internal
 * counter to be greater than threshold. It happens
 * when internal counter is lower than threshold and
 * increase method is called with parameter 'delta' big enough
 * so that sum of delta and internal counter is greater than
 * threshold. This is not a bug, this is a feature.
 * It solves some problems:
 *   - thread calling increase with big parameter will not be
 *     starved by other threads calling increase with small
 *     arguments.
 *   - thread calling increase with argument greater than
 *     threshold won't deadlock. This is useful when throttling
 *     queues - you can submit object that is bigger than limit.
 *
 * This implementation introduces small costs in terms of
 * synchronization (no synchronization in most cases at all).
 */
public class SizeBasedThrottler {

  private final long threshold;
  private final AtomicLong currentSize;

  /**
   * Creates SizeBoundary with provided threshold
   *
   * @param threshold threshold used by instance
   */
  public SizeBasedThrottler(long threshold) {
    if (threshold <= 0) {
      throw new IllegalArgumentException("Treshold must be greater than 0");
    }
    this.threshold = threshold;
    this.currentSize = new AtomicLong(0);
  }

  /**
   * Blocks until internal counter is lower than threshold
   * and then increases value of internal counter.
   *
   * Note that order in which increases will be granted is not guaranteed,
   * i.e. some request can wait, while newer request are granted in the meantime
   *
   * @param delta increase internal counter by this value
   * @return new value of internal counter
   * @throws InterruptedException when interrupted during waiting
   */
  public long increase(long delta) throws InterruptedException{
    while (true) {
      long size = currentSize.get();
      if (size >= threshold) {
        synchronized (this) {
          while ((size = currentSize.get()) >= threshold) {
            wait();
          }
        }
      }

      if (currentSize.compareAndSet(size, size + delta)) {
        return size + delta;
      }
    }
  }


  /**
   * Decreases value of internal counter. Wakes up waiting threads if required.
   *
   * @param delta decrease internal counter by this value
   * @return new value of internal counter
   */
  public long decrease(long delta) {
    final long newSize = currentSize.addAndGet(-delta);

    if (newSize < threshold && newSize + delta >= threshold) {
      synchronized (this) {
        notifyAll();
      }
    }

    return newSize;
  }

  /**
   *
   * @return current value of internal counter
   */
  public long getCurrentValue(){
    return currentSize.get();
  }

  /**
   * @return threshold
   */
  public long getThreshold(){
    return threshold;
  }
}
