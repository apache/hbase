/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.metrics.histogram;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.util.Threads;

/**
 * An exponentially-decaying random sample of {@code long}s. 
 * Uses Cormode et al's forward-decaying priority reservoir sampling method 
 * to produce a statistically representative sample, exponentially biased 
 * towards newer entries.
 *
 * see Cormode et al. 
 * Forward Decay: A Practical Time Decay Model for Streaming Systems. ICDE '09
 */
public class ExponentiallyDecayingSample implements Sample {

  private static final Random RANDOM = new Random();  
  private static final long RESCALE_THRESHOLD = TimeUnit.HOURS.toNanos(1);

  private static final ScheduledExecutorService TICK_SERVICE = 
      Executors.newScheduledThreadPool(1, 
          Threads.getNamedThreadFactory("decayingSampleTick", true));

  private static volatile long CURRENT_TICK = 
      TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

  static {
    // sample at twice our signal's frequency (1Hz) per the Nyquist theorem
    TICK_SERVICE.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        CURRENT_TICK = 
            TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
      }
    }, 0, 500, TimeUnit.MILLISECONDS);
  }
  
  private final ConcurrentSkipListMap<Double, Long> values = 
      new ConcurrentSkipListMap<Double, Long>();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final AtomicLong count = new AtomicLong(0);
  private final AtomicLong nextScaleTime = new AtomicLong(0);

  private final double alpha;
  private final int reservoirSize;
  private volatile long startTime;

  /**
   * Constructor for an ExponentiallyDecayingSample.
   *
   * @param reservoirSize the number of samples to keep in the reservoir
   * @param alpha         the exponential decay factor; the higher this is, 
   *                      the more biased the sample will be towards newer 
   *                      values
   */
  public ExponentiallyDecayingSample(int reservoirSize, double alpha) {
    this.alpha = alpha;
    this.reservoirSize = reservoirSize;
    clear();
  }

  @Override
  public void clear() {
    lockForRescale();
    try {
      values.clear();
      count.set(0);
      this.startTime = CURRENT_TICK;
      nextScaleTime.set(System.nanoTime() + RESCALE_THRESHOLD);
    } finally {
      unlockForRescale();
    }
  }

  @Override
  public int size() {
    return (int) Math.min(reservoirSize, count.get());
  }

  @Override
  public void update(long value) {
    update(value, CURRENT_TICK);
  }

  /**
   * Adds an old value with a fixed timestamp to the sample.
   *
   * @param value     the value to be added
   * @param timestamp the epoch timestamp of {@code value} in seconds
   */
  public void update(long value, long timestamp) {
    lockForRegularUsage();
    try {
      final double priority = weight(timestamp - startTime) 
          / RANDOM.nextDouble();
      final long newCount = count.incrementAndGet();
      if (newCount <= reservoirSize) {
        values.put(priority, value);
      } else {
        Double first = values.firstKey();
        if (first < priority) {
          if (values.putIfAbsent(priority, value) == null) {
            // ensure we always remove an item
            while (values.remove(first) == null) {
              first = values.firstKey();
            }
          }
        }
      }
    } finally {
      unlockForRegularUsage();
    }

    final long now = System.nanoTime();
    final long next = nextScaleTime.get();
    if (now >= next) {
      rescale(now, next);
    }
  }

  @Override
  public Snapshot getSnapshot() {
    lockForRegularUsage();
    try {
      return new Snapshot(values.values());
    } finally {
      unlockForRegularUsage();
    }
  }

  private double weight(long t) {
    return Math.exp(alpha * t);
  }

  /* "A common feature of the above techniques—indeed, the key technique that
   * allows us to track the decayed weights efficiently—is that they maintain
   * counts and other quantities based on g(ti − L), and only scale by g(t − L)
   * at query time. But while g(ti −L)/g(t−L) is guaranteed to lie between zero
   * and one, the intermediate values of g(ti − L) could become very large. For
   * polynomial functions, these values should not grow too large, and should 
   * be effectively represented in practice by floating point values without 
   * loss of precision. For exponential functions, these values could grow 
   * quite large as new values of (ti − L) become large, and potentially 
   * exceed the capacity of common floating point types. However, since the 
   * values stored by the algorithms are linear combinations of g values 
   * (scaled sums), they can be rescaled relative to a new landmark. That is, 
   * by the analysis of exponential decay in Section III-A, the choice of L 
   * does not affect the final result. We can therefore multiply each value 
   * based on L by a factor of exp(−α(L′ − L)), and obtain the correct value 
   * as if we had instead computed relative to a new landmark L′ (and then use 
   * this new L′ at query time). This can be done with a linear pass over 
   * whatever data structure is being used."
   */
  private void rescale(long now, long next) {
    if (nextScaleTime.compareAndSet(next, now + RESCALE_THRESHOLD)) {
      lockForRescale();
      try {
        final long oldStartTime = startTime;
        this.startTime = CURRENT_TICK;
        final ArrayList<Double> keys = new ArrayList<Double>(values.keySet());
        for (Double key : keys) {
          final Long value = values.remove(key);
          values.put(key * Math.exp(-alpha * (startTime - oldStartTime)), 
              value);
        }
      } finally {
        unlockForRescale();
      }
    }
  }

  private void unlockForRescale() {
    lock.writeLock().unlock();
  }

  private void lockForRescale() {
    lock.writeLock().lock();
  }

  private void lockForRegularUsage() {
    lock.readLock().lock();
  }

  private void unlockForRegularUsage() {
    lock.readLock().unlock();
  }
}
