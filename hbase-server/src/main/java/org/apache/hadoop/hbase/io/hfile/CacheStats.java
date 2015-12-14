/**
 *
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
package org.apache.hadoop.hbase.io.hfile;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * Class that implements cache metrics.
 */
@InterfaceAudience.Private
public class CacheStats {
  /**
   * Needed making histograms.
   */
  private static final MetricsRegistry METRICS = new MetricsRegistry();

  /** Sliding window statistics. The number of metric periods to include in
   * sliding window hit ratio calculations.
   */
  static final int DEFAULT_WINDOW_PERIODS = 5;

  /** The number of getBlock requests that were cache hits */
  private final AtomicLong hitCount = new AtomicLong(0);

  /**
   * The number of getBlock requests that were cache hits, but only from
   * requests that were set to use the block cache.  This is because all reads
   * attempt to read from the block cache even if they will not put new blocks
   * into the block cache.  See HBASE-2253 for more information.
   */
  private final AtomicLong hitCachingCount = new AtomicLong(0);

  /** The number of getBlock requests that were cache misses */
  private final AtomicLong missCount = new AtomicLong(0);

  /**
   * The number of getBlock requests that were cache misses, but only from
   * requests that were set to use the block cache.
   */
  private final AtomicLong missCachingCount = new AtomicLong(0);

  /** The number of times an eviction has occurred */
  private final AtomicLong evictionCount = new AtomicLong(0);

  /** The total number of blocks that have been evicted */
  private final AtomicLong evictedBlockCount = new AtomicLong(0);

  /** The number of metrics periods to include in window */
  private final int numPeriodsInWindow;
  /** Hit counts for each period in window */
  private final long [] hitCounts;
  /** Caching hit counts for each period in window */
  private final long [] hitCachingCounts;
  /** Access counts for each period in window */
  private final long [] requestCounts;
  /** Caching access counts for each period in window */
  private final long [] requestCachingCounts;
  /** Last hit count read */
  private long lastHitCount = 0;
  /** Last hit caching count read */
  private long lastHitCachingCount = 0;
  /** Last request count read */
  private long lastRequestCount = 0;
  /** Last request caching count read */
  private long lastRequestCachingCount = 0;
  /** Current window index (next to be updated) */
  private int windowIndex = 0;
  /**
   * Keep running age at eviction time
   */
  private Histogram ageAtEviction;
  private long startTime = System.nanoTime();

  public CacheStats(final String name) {
    this(name, DEFAULT_WINDOW_PERIODS);
  }

  public CacheStats(final String name, int numPeriodsInWindow) {
    this.numPeriodsInWindow = numPeriodsInWindow;
    this.hitCounts = initializeZeros(numPeriodsInWindow);
    this.hitCachingCounts = initializeZeros(numPeriodsInWindow);
    this.requestCounts = initializeZeros(numPeriodsInWindow);
    this.requestCachingCounts = initializeZeros(numPeriodsInWindow);
    this.ageAtEviction = METRICS.newHistogram(CacheStats.class, name + ".ageAtEviction");
  }

  @Override
  public String toString() {
    AgeSnapshot snapshot = getAgeAtEvictionSnapshot();
    return "hitCount=" + getHitCount() + ", hitCachingCount=" + getHitCachingCount() +
      ", missCount=" + getMissCount() + ", missCachingCount=" + getMissCachingCount() +
      ", evictionCount=" + getEvictionCount() +
      ", evictedBlockCount=" + getEvictedCount() +
      ", evictedAgeMean=" + snapshot.getMean() +
      ", evictedAgeStdDev=" + snapshot.getStdDev();
  }

  public void miss(boolean caching) {
    missCount.incrementAndGet();
    if (caching) missCachingCount.incrementAndGet();
  }

  public void hit(boolean caching) {
    hitCount.incrementAndGet();
    if (caching) hitCachingCount.incrementAndGet();
  }

  public void evict() {
    evictionCount.incrementAndGet();
  }

  public void evicted(final long t) {
    if (t > this.startTime) this.ageAtEviction.update(t - this.startTime);
    this.evictedBlockCount.incrementAndGet();
  }

  public long getRequestCount() {
    return getHitCount() + getMissCount();
  }

  public long getRequestCachingCount() {
    return getHitCachingCount() + getMissCachingCount();
  }

  public long getMissCount() {
    return missCount.get();
  }

  public long getMissCachingCount() {
    return missCachingCount.get();
  }

  public long getHitCount() {
    return hitCount.get();
  }

  public long getHitCachingCount() {
    return hitCachingCount.get();
  }

  public long getEvictionCount() {
    return evictionCount.get();
  }

  public long getEvictedCount() {
    return this.evictedBlockCount.get();
  }

  public double getHitRatio() {
    return ((float)getHitCount()/(float)getRequestCount());
  }

  public double getHitCachingRatio() {
    return ((float)getHitCachingCount()/(float)getRequestCachingCount());
  }

  public double getMissRatio() {
    return ((float)getMissCount()/(float)getRequestCount());
  }

  public double getMissCachingRatio() {
    return ((float)getMissCachingCount()/(float)getRequestCachingCount());
  }

  public double evictedPerEviction() {
    return ((float)getEvictedCount()/(float)getEvictionCount());
  }

  public void rollMetricsPeriod() {
    hitCounts[windowIndex] = getHitCount() - lastHitCount;
    lastHitCount = getHitCount();
    hitCachingCounts[windowIndex] =
      getHitCachingCount() - lastHitCachingCount;
    lastHitCachingCount = getHitCachingCount();
    requestCounts[windowIndex] = getRequestCount() - lastRequestCount;
    lastRequestCount = getRequestCount();
    requestCachingCounts[windowIndex] =
      getRequestCachingCount() - lastRequestCachingCount;
    lastRequestCachingCount = getRequestCachingCount();
    windowIndex = (windowIndex + 1) % numPeriodsInWindow;
  }

  public long getSumHitCountsPastNPeriods() {
    return sum(hitCounts);
  }

  public long getSumRequestCountsPastNPeriods() {
    return sum(requestCounts);
  }

  public long getSumHitCachingCountsPastNPeriods() {
    return sum(hitCachingCounts);
  }

  public long getSumRequestCachingCountsPastNPeriods() {
    return sum(requestCachingCounts);
  }

  public double getHitRatioPastNPeriods() {
    double ratio = ((double)getSumHitCountsPastNPeriods() /
        (double)getSumRequestCountsPastNPeriods());
    return Double.isNaN(ratio) ? 0 : ratio;
  }

  public double getHitCachingRatioPastNPeriods() {
    double ratio = ((double)getSumHitCachingCountsPastNPeriods() /
        (double)getSumRequestCachingCountsPastNPeriods());
    return Double.isNaN(ratio) ? 0 : ratio;
  }

  public AgeSnapshot getAgeAtEvictionSnapshot() {
    return new AgeSnapshot(this.ageAtEviction);
  }

  private static long sum(long [] counts) {
    long sum = 0;
    for (long count : counts) sum += count;
    return sum;
  }

  private static long [] initializeZeros(int n) {
    long [] zeros = new long [n];
    for (int i=0; i<n; i++) {
      zeros[i] = 0L;
    }
    return zeros;
  }
}
