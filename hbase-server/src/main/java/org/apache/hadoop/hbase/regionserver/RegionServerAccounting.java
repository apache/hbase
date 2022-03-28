/*
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
package org.apache.hadoop.hbase.regionserver;

import java.lang.management.MemoryType;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.util.Pair;

/**
 * RegionServerAccounting keeps record of some basic real time information about
 * the Region Server. Currently, it keeps record the global memstore size and global memstore
 * on-heap and off-heap overhead. It also tracks the replay edits per region.
 */
@InterfaceAudience.Private
public class RegionServerAccounting {
  // memstore data size
  private final LongAdder globalMemStoreDataSize = new LongAdder();
  // memstore heap size.
  private final LongAdder globalMemStoreHeapSize = new LongAdder();
  // memstore off-heap size.
  private final LongAdder globalMemStoreOffHeapSize = new LongAdder();

  private long globalMemStoreLimit;
  private final float globalMemStoreLimitLowMarkPercent;
  private long globalMemStoreLimitLowMark;
  private final MemoryType memType;
  private long globalOnHeapMemstoreLimit;
  private long globalOnHeapMemstoreLimitLowMark;

  // encoded region name -> Pair -> read count as first, write count as second.
  // when region close and target rs is the current server, we will put an entry,
  // and will remove it when reigon open after recover them.
  private ConcurrentMap<String, Pair<Long, Long>> retainedRegionRWRequestsCnt;

  public RegionServerAccounting(Configuration conf) {
    Pair<Long, MemoryType> globalMemstoreSizePair = MemorySizeUtil.getGlobalMemStoreSize(conf);
    this.globalMemStoreLimit = globalMemstoreSizePair.getFirst();
    this.memType = globalMemstoreSizePair.getSecond();
    this.globalMemStoreLimitLowMarkPercent =
        MemorySizeUtil.getGlobalMemStoreHeapLowerMark(conf, this.memType == MemoryType.HEAP);
    // When off heap memstore in use we configure the global off heap space for memstore as bytes
    // not as % of max memory size. In such case, the lower water mark should be specified using the
    // key "hbase.regionserver.global.memstore.size.lower.limit" which says % of the global upper
    // bound and defaults to 95%. In on heap case also specifying this way is ideal. But in the past
    // we used to take lower bound also as the % of xmx (38% as default). For backward compatibility
    // for this deprecated config,we will fall back to read that config when new one is missing.
    // Only for on heap case, do this fallback mechanism. For off heap it makes no sense.
    // TODO When to get rid of the deprecated config? ie
    // "hbase.regionserver.global.memstore.lowerLimit". Can get rid of this boolean passing then.
    this.globalMemStoreLimitLowMark =
        (long) (this.globalMemStoreLimit * this.globalMemStoreLimitLowMarkPercent);
    this.globalOnHeapMemstoreLimit = MemorySizeUtil.getOnheapGlobalMemStoreSize(conf);
    this.globalOnHeapMemstoreLimitLowMark =
        (long) (this.globalOnHeapMemstoreLimit * this.globalMemStoreLimitLowMarkPercent);
    this.retainedRegionRWRequestsCnt = new ConcurrentHashMap<>();
  }

  long getGlobalMemStoreLimit() {
    return this.globalMemStoreLimit;
  }

  long getGlobalOnHeapMemStoreLimit() {
    return this.globalOnHeapMemstoreLimit;
  }

  // Called by the tuners.
  void setGlobalMemStoreLimits(long newGlobalMemstoreLimit) {
    if (this.memType == MemoryType.HEAP) {
      this.globalMemStoreLimit = newGlobalMemstoreLimit;
      this.globalMemStoreLimitLowMark =
          (long) (this.globalMemStoreLimit * this.globalMemStoreLimitLowMarkPercent);
    } else {
      this.globalOnHeapMemstoreLimit = newGlobalMemstoreLimit;
      this.globalOnHeapMemstoreLimitLowMark =
          (long) (this.globalOnHeapMemstoreLimit * this.globalMemStoreLimitLowMarkPercent);
    }
  }

  boolean isOffheap() {
    return this.memType == MemoryType.NON_HEAP;
  }

  long getGlobalMemStoreLimitLowMark() {
    return this.globalMemStoreLimitLowMark;
  }

  float getGlobalMemStoreLimitLowMarkPercent() {
    return this.globalMemStoreLimitLowMarkPercent;
  }

  /**
   * @return the global Memstore data size in the RegionServer
   */
  public long getGlobalMemStoreDataSize() {
    return globalMemStoreDataSize.sum();
  }

  /**
   * @return the global memstore heap size in the RegionServer
   */
  public long getGlobalMemStoreHeapSize() {
    return this.globalMemStoreHeapSize.sum();
  }

  /**
   * @return the global memstore heap size in the RegionServer
   */
  public long getGlobalMemStoreOffHeapSize() {
    return this.globalMemStoreOffHeapSize.sum();
  }

  /**
   * @return the retained metrics of region's read and write requests count
   */
  protected ConcurrentMap<String, Pair<Long, Long>> getRetainedRegionRWRequestsCnt() {
    return this.retainedRegionRWRequestsCnt;
  }

  void incGlobalMemStoreSize(MemStoreSize mss) {
    incGlobalMemStoreSize(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize());
  }

  public void incGlobalMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta) {
    globalMemStoreDataSize.add(dataSizeDelta);
    globalMemStoreHeapSize.add(heapSizeDelta);
    globalMemStoreOffHeapSize.add(offHeapSizeDelta);
  }

  public void decGlobalMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta) {
    globalMemStoreDataSize.add(-dataSizeDelta);
    globalMemStoreHeapSize.add(-heapSizeDelta);
    globalMemStoreOffHeapSize.add(-offHeapSizeDelta);
  }

  /**
   * Return true if we are above the memstore high water mark
   * @return the flushtype
   */
  public FlushType isAboveHighWaterMark() {
    // for onheap memstore we check if the global memstore size and the
    // global heap overhead is greater than the global memstore limit
    if (memType == MemoryType.HEAP) {
      if (getGlobalMemStoreHeapSize() >= globalMemStoreLimit) {
        return FlushType.ABOVE_ONHEAP_HIGHER_MARK;
      }
    } else {
      // If the configured memstore is offheap, check for two things
      // 1) If the global memstore off-heap size is greater than the configured
      // 'hbase.regionserver.offheap.global.memstore.size'
      // 2) If the global memstore heap size is greater than the configured onheap
      // global memstore limit 'hbase.regionserver.global.memstore.size'.
      // We do this to avoid OOME incase of scenarios where the heap is occupied with
      // lot of onheap references to the cells in memstore
      if (getGlobalMemStoreOffHeapSize() >= globalMemStoreLimit) {
        // Indicates that global memstore size is above the configured
        // 'hbase.regionserver.offheap.global.memstore.size'
        return FlushType.ABOVE_OFFHEAP_HIGHER_MARK;
      } else if (getGlobalMemStoreHeapSize() >= this.globalOnHeapMemstoreLimit) {
        // Indicates that the offheap memstore's heap overhead is greater than the
        // configured 'hbase.regionserver.global.memstore.size'.
        return FlushType.ABOVE_ONHEAP_HIGHER_MARK;
      }
    }
    return FlushType.NORMAL;
  }

  /**
   * Return true if we're above the low watermark
   */
  public FlushType isAboveLowWaterMark() {
    // for onheap memstore we check if the global memstore size and the
    // global heap overhead is greater than the global memstore lower mark limit
    if (memType == MemoryType.HEAP) {
      if (getGlobalMemStoreHeapSize() >= globalMemStoreLimitLowMark) {
        return FlushType.ABOVE_ONHEAP_LOWER_MARK;
      }
    } else {
      if (getGlobalMemStoreOffHeapSize() >= globalMemStoreLimitLowMark) {
        // Indicates that the offheap memstore's size is greater than the global memstore
        // lower limit
        return FlushType.ABOVE_OFFHEAP_LOWER_MARK;
      } else if (getGlobalMemStoreHeapSize() >= globalOnHeapMemstoreLimitLowMark) {
        // Indicates that the offheap memstore's heap overhead is greater than the global memstore
        // onheap lower limit
        return FlushType.ABOVE_ONHEAP_LOWER_MARK;
      }
    }
    return FlushType.NORMAL;
  }

  /**
   * @return the flush pressure of all stores on this regionserver. The value should be greater than
   *         or equal to 0.0, and any value greater than 1.0 means we enter the emergency state that
   *         global memstore size already exceeds lower limit.
   */
  public double getFlushPressure() {
    if (memType == MemoryType.HEAP) {
      return (getGlobalMemStoreHeapSize()) * 1.0 / globalMemStoreLimitLowMark;
    } else {
      return Math.max(getGlobalMemStoreOffHeapSize() * 1.0 / globalMemStoreLimitLowMark,
          getGlobalMemStoreHeapSize() * 1.0 / globalOnHeapMemstoreLimitLowMark);
    }
  }
}
