/*
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
package org.apache.hadoop.hbase.io.hfile.cache;

import java.util.Objects;
import java.util.function.ToLongFunction;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Aggregate cache statistics backed by multiple cache-engine statistics instances.
 */
@InterfaceAudience.Private
final class AggregateCacheStats extends CacheStats {

  private final CacheStats[] delegates;

  /**
   * Creates aggregate cache statistics.
   * @param name      aggregate statistics name
   * @param delegates cache statistics to aggregate
   */
  AggregateCacheStats(String name, CacheStats... delegates) {
    super(name);
    Objects.requireNonNull(delegates, "delegates must not be null");

    int count = 0;
    for (CacheStats delegate : delegates) {
      if (delegate != null) {
        count++;
      }
    }

    this.delegates = new CacheStats[count];
    int index = 0;
    for (CacheStats delegate : delegates) {
      if (delegate != null) {
        this.delegates[index++] = delegate;
      }
    }
  }

  /**
   * Sums a value across all underlying cache statistics.
   * @param extractor value extractor
   * @return aggregate value
   */
  private long sum(ToLongFunction<CacheStats> extractor) {
    long result = 0L;
    for (CacheStats delegate : delegates) {
      result += extractor.applyAsLong(delegate);
    }
    return result;
  }

  /**
   * Returns the aggregate data-block miss count.
   * @return aggregate data-block miss count
   */
  @Override
  public long getDataMissCount() {
    return sum(CacheStats::getDataMissCount);
  }

  /**
   * Returns the aggregate leaf-index miss count.
   * @return aggregate leaf-index miss count
   */
  @Override
  public long getLeafIndexMissCount() {
    return sum(CacheStats::getLeafIndexMissCount);
  }

  /**
   * Returns the aggregate bloom-chunk miss count.
   * @return aggregate bloom-chunk miss count
   */
  @Override
  public long getBloomChunkMissCount() {
    return sum(CacheStats::getBloomChunkMissCount);
  }

  /**
   * Returns the aggregate metadata miss count.
   * @return aggregate metadata miss count
   */
  @Override
  public long getMetaMissCount() {
    return sum(CacheStats::getMetaMissCount);
  }

  /**
   * Returns the aggregate root-index miss count.
   * @return aggregate root-index miss count
   */
  @Override
  public long getRootIndexMissCount() {
    return sum(CacheStats::getRootIndexMissCount);
  }

  /**
   * Returns the aggregate intermediate-index miss count.
   * @return aggregate intermediate-index miss count
   */
  @Override
  public long getIntermediateIndexMissCount() {
    return sum(CacheStats::getIntermediateIndexMissCount);
  }

  /**
   * Returns the aggregate file-info miss count.
   * @return aggregate file-info miss count
   */
  @Override
  public long getFileInfoMissCount() {
    return sum(CacheStats::getFileInfoMissCount);
  }

  /**
   * Returns the aggregate general-bloom metadata miss count.
   * @return aggregate general-bloom metadata miss count
   */
  @Override
  public long getGeneralBloomMetaMissCount() {
    return sum(CacheStats::getGeneralBloomMetaMissCount);
  }

  /**
   * Returns the aggregate delete-family bloom miss count.
   * @return aggregate delete-family bloom miss count
   */
  @Override
  public long getDeleteFamilyBloomMissCount() {
    return sum(CacheStats::getDeleteFamilyBloomMissCount);
  }

  /**
   * Returns the aggregate trailer miss count.
   * @return aggregate trailer miss count
   */
  @Override
  public long getTrailerMissCount() {
    return sum(CacheStats::getTrailerMissCount);
  }

  /**
   * Returns the aggregate data-block hit count.
   * @return aggregate data-block hit count
   */
  @Override
  public long getDataHitCount() {
    return sum(CacheStats::getDataHitCount);
  }

  /**
   * Returns the aggregate leaf-index hit count.
   * @return aggregate leaf-index hit count
   */
  @Override
  public long getLeafIndexHitCount() {
    return sum(CacheStats::getLeafIndexHitCount);
  }

  /**
   * Returns the aggregate bloom-chunk hit count.
   * @return aggregate bloom-chunk hit count
   */
  @Override
  public long getBloomChunkHitCount() {
    return sum(CacheStats::getBloomChunkHitCount);
  }

  /**
   * Returns the aggregate metadata hit count.
   * @return aggregate metadata hit count
   */
  @Override
  public long getMetaHitCount() {
    return sum(CacheStats::getMetaHitCount);
  }

  /**
   * Returns the aggregate root-index hit count.
   * @return aggregate root-index hit count
   */
  @Override
  public long getRootIndexHitCount() {
    return sum(CacheStats::getRootIndexHitCount);
  }

  /**
   * Returns the aggregate intermediate-index hit count.
   * @return aggregate intermediate-index hit count
   */
  @Override
  public long getIntermediateIndexHitCount() {
    return sum(CacheStats::getIntermediateIndexHitCount);
  }

  /**
   * Returns the aggregate file-info hit count.
   * @return aggregate file-info hit count
   */
  @Override
  public long getFileInfoHitCount() {
    return sum(CacheStats::getFileInfoHitCount);
  }

  /**
   * Returns the aggregate general-bloom metadata hit count.
   * @return aggregate general-bloom metadata hit count
   */
  @Override
  public long getGeneralBloomMetaHitCount() {
    return sum(CacheStats::getGeneralBloomMetaHitCount);
  }

  /**
   * Returns the aggregate delete-family bloom hit count.
   * @return aggregate delete-family bloom hit count
   */
  @Override
  public long getDeleteFamilyBloomHitCount() {
    return sum(CacheStats::getDeleteFamilyBloomHitCount);
  }

  /**
   * Returns the aggregate trailer hit count.
   * @return aggregate trailer hit count
   */
  @Override
  public long getTrailerHitCount() {
    return sum(CacheStats::getTrailerHitCount);
  }

  /**
   * Returns the aggregate miss count.
   * @return aggregate miss count
   */
  @Override
  public long getMissCount() {
    return sum(CacheStats::getMissCount);
  }

  /**
   * Returns the aggregate primary-replica miss count.
   * @return aggregate primary-replica miss count
   */
  @Override
  public long getPrimaryMissCount() {
    return sum(CacheStats::getPrimaryMissCount);
  }

  /**
   * Returns the aggregate caching-request miss count.
   * @return aggregate caching-request miss count
   */
  @Override
  public long getMissCachingCount() {
    return sum(CacheStats::getMissCachingCount);
  }

  /**
   * Returns the aggregate hit count.
   * @return aggregate hit count
   */
  @Override
  public long getHitCount() {
    return sum(CacheStats::getHitCount);
  }

  /**
   * Returns the aggregate primary-replica hit count.
   * @return aggregate primary-replica hit count
   */
  @Override
  public long getPrimaryHitCount() {
    return sum(CacheStats::getPrimaryHitCount);
  }

  /**
   * Returns the aggregate caching-request hit count.
   * @return aggregate caching-request hit count
   */
  @Override
  public long getHitCachingCount() {
    return sum(CacheStats::getHitCachingCount);
  }

  /**
   * Returns the aggregate eviction count.
   * @return aggregate eviction count
   */
  @Override
  public long getEvictionCount() {
    return sum(CacheStats::getEvictionCount);
  }

  /**
   * Returns the aggregate number of evicted blocks.
   * @return aggregate evicted-block count
   */
  @Override
  public long getEvictedCount() {
    return sum(CacheStats::getEvictedCount);
  }

  /**
   * Returns the aggregate number of evicted primary-replica blocks.
   * @return aggregate primary evicted-block count
   */
  @Override
  public long getPrimaryEvictedCount() {
    return sum(CacheStats::getPrimaryEvictedCount);
  }

  /**
   * Returns the aggregate failed-insert count.
   * @return aggregate failed-insert count
   */
  @Override
  public long getFailedInserts() {
    return sum(CacheStats::getFailedInserts);
  }

  /**
   * Rolls the metrics period for all underlying cache statistics.
   */
  @Override
  public void rollMetricsPeriod() {
    for (CacheStats delegate : delegates) {
      delegate.rollMetricsPeriod();
    }
  }

  /**
   * Returns the aggregate hit count over the configured rolling window.
   * @return aggregate rolling hit count
   */
  @Override
  public long getSumHitCountsPastNPeriods() {
    return sum(CacheStats::getSumHitCountsPastNPeriods);
  }

  /**
   * Returns the aggregate request count over the configured rolling window.
   * @return aggregate rolling request count
   */
  @Override
  public long getSumRequestCountsPastNPeriods() {
    return sum(CacheStats::getSumRequestCountsPastNPeriods);
  }

  /**
   * Returns the aggregate caching-hit count over the configured rolling window.
   * @return aggregate rolling caching-hit count
   */
  @Override
  public long getSumHitCachingCountsPastNPeriods() {
    return sum(CacheStats::getSumHitCachingCountsPastNPeriods);
  }

  /**
   * Returns the aggregate caching-request count over the configured rolling window.
   * @return aggregate rolling caching-request count
   */
  @Override
  public long getSumRequestCachingCountsPastNPeriods() {
    return sum(CacheStats::getSumRequestCachingCountsPastNPeriods);
  }
}
