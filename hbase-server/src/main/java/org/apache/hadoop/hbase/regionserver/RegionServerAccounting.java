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
package org.apache.hadoop.hbase.regionserver;

import java.lang.management.MemoryType;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * RegionServerAccounting keeps record of some basic real time information about
 * the Region Server. Currently, it keeps record the global memstore size and global memstore heap
 * overhead. It also tracks the replay edits per region.
 */
@InterfaceAudience.Private
public class RegionServerAccounting {

  // memstore data size
  private final AtomicLong globalMemstoreDataSize = new AtomicLong(0);
  // memstore heap over head size
  private final AtomicLong globalMemstoreHeapOverhead = new AtomicLong(0);

  // Store the edits size during replaying WAL. Use this to roll back the
  // global memstore size once a region opening failed.
  private final ConcurrentMap<byte[], MemstoreSize> replayEditsPerRegion =
    new ConcurrentSkipListMap<byte[], MemstoreSize>(Bytes.BYTES_COMPARATOR);

  private final Configuration conf;

  private long globalMemStoreLimit;
  private final float globalMemStoreLimitLowMarkPercent;
  private long globalMemStoreLimitLowMark;
  private final MemoryType memType;
  private long globalOnHeapMemstoreLimit;
  private long globalOnHeapMemstoreLimitLowMark;

  public RegionServerAccounting(Configuration conf) {
    this.conf = conf;
    Pair<Long, MemoryType> globalMemstoreSizePair = MemorySizeUtil.getGlobalMemstoreSize(conf);
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
    this.globalOnHeapMemstoreLimit = MemorySizeUtil.getOnheapGlobalMemstoreSize(conf);
    this.globalOnHeapMemstoreLimitLowMark =
        (long) (this.globalOnHeapMemstoreLimit * this.globalMemStoreLimitLowMarkPercent);
  }

  public long getGlobalMemstoreLimit() {
    return this.globalMemStoreLimit;
  }

  public long getOnheapGlobalMemstoreLimit() {
    return this.globalOnHeapMemstoreLimit;
  }

  // Called by the tuners.
  public void setGlobalMemstoreLimits(long newGlobalMemstoreLimit) {
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

  public boolean isOffheap() {
    return this.memType == MemoryType.NON_HEAP;
  }

  public long getGlobalMemstoreLimitLowMark() {
    return this.globalMemStoreLimitLowMark;
  }

  public float getGlobalMemstoreLimitLowMarkPercent() {
    return this.globalMemStoreLimitLowMarkPercent;
  }

  /**
   * @return the global Memstore data size in the RegionServer
   */
  public long getGlobalMemstoreDataSize() {
    return globalMemstoreDataSize.get();
  }
  /**
   * @return the global memstore heap overhead size in the RegionServer
   */
  public long getGlobalMemstoreHeapOverhead() {
    return this.globalMemstoreHeapOverhead.get();
  }

  /**
   * @return the global memstore data size and heap overhead size for an onheap memstore
   * whereas return the heap overhead size for an offheap memstore
   */
  public long getGlobalMemstoreSize() {
    if (isOffheap()) {
      // get only the heap overhead for offheap memstore
      return getGlobalMemstoreHeapOverhead();
    } else {
      return getGlobalMemstoreDataSize() + getGlobalMemstoreHeapOverhead();
    }
  }

  /**
   * @param memStoreSize the Memstore size will be added to 
   *        the global Memstore size 
   */
  public void incGlobalMemstoreSize(MemstoreSize memStoreSize) {
    globalMemstoreDataSize.addAndGet(memStoreSize.getDataSize());
    globalMemstoreHeapOverhead.addAndGet(memStoreSize.getHeapOverhead());
  }

  public void decGlobalMemstoreSize(MemstoreSize memStoreSize) {
    globalMemstoreDataSize.addAndGet(-memStoreSize.getDataSize());
    globalMemstoreHeapOverhead.addAndGet(-memStoreSize.getHeapOverhead());
  }

  /**
   * Return true if we are above the memstore high water mark
   * @return the flushtype
   */
  public FlushType isAboveHighWaterMark() {
    // for onheap memstore we check if the global memstore size and the
    // global heap overhead is greater than the global memstore limit
    if (memType == MemoryType.HEAP) {
      if (getGlobalMemstoreDataSize() + getGlobalMemstoreHeapOverhead() >= globalMemStoreLimit) {
        return FlushType.ABOVE_ONHEAP_HIGHER_MARK;
      }
    } else {
      // If the configured memstore is offheap, check for two things
      // 1) If the global memstore data size is greater than the configured
      // 'hbase.regionserver.offheap.global.memstore.size'
      // 2) If the global memstore heap size is greater than the configured onheap
      // global memstore limit 'hbase.regionserver.global.memstore.size'.
      // We do this to avoid OOME incase of scenarios where the heap is occupied with
      // lot of onheap references to the cells in memstore
      if (getGlobalMemstoreDataSize() >= globalMemStoreLimit) {
        // Indicates that global memstore size is above the configured
        // 'hbase.regionserver.offheap.global.memstore.size'
        return FlushType.ABOVE_OFFHEAP_HIGHER_MARK;
      } else if (getGlobalMemstoreHeapOverhead() >= this.globalOnHeapMemstoreLimit) {
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
      if (getGlobalMemstoreDataSize() + getGlobalMemstoreHeapOverhead() >= globalMemStoreLimitLowMark) {
        return FlushType.ABOVE_ONHEAP_LOWER_MARK;
      }
    } else {
      if (getGlobalMemstoreDataSize() >= globalMemStoreLimitLowMark) {
        // Indicates that the offheap memstore's data size is greater than the global memstore
        // lower limit
        return FlushType.ABOVE_OFFHEAP_LOWER_MARK;
      } else if (getGlobalMemstoreHeapOverhead() >= globalOnHeapMemstoreLimitLowMark) {
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
      return (getGlobalMemstoreDataSize() + getGlobalMemstoreHeapOverhead()) * 1.0
          / globalMemStoreLimitLowMark;
    } else {
      return Math.max(getGlobalMemstoreDataSize() * 1.0 / globalMemStoreLimitLowMark,
        getGlobalMemstoreHeapOverhead() * 1.0 / globalOnHeapMemstoreLimitLowMark);
    }
  }

  /***
   * Add memStoreSize to replayEditsPerRegion.
   *
   * @param regionName region name.
   * @param memStoreSize the Memstore size will be added to replayEditsPerRegion.
   */
  public void addRegionReplayEditsSize(byte[] regionName, MemstoreSize memStoreSize) {
    MemstoreSize replayEdistsSize = replayEditsPerRegion.get(regionName);
    // All ops on the same MemstoreSize object is going to be done by single thread, sequentially
    // only. First calls to this method to increment the per region reply edits size and then call
    // to either rollbackRegionReplayEditsSize or clearRegionReplayEditsSize as per the result of
    // the region open operation. No need to handle multi thread issues on one region's entry in
    // this Map.
    if (replayEdistsSize == null) {
      replayEdistsSize = new MemstoreSize();
      replayEditsPerRegion.put(regionName, replayEdistsSize);
    }
    replayEdistsSize.incMemstoreSize(memStoreSize);
  }

  /**
   * Roll back the global MemStore size for a specified region when this region
   * can't be opened.
   * 
   * @param regionName the region which could not open.
   */
  public void rollbackRegionReplayEditsSize(byte[] regionName) {
    MemstoreSize replayEditsSize = replayEditsPerRegion.get(regionName);
    if (replayEditsSize != null) {
      clearRegionReplayEditsSize(regionName);
      decGlobalMemstoreSize(replayEditsSize);
    }
  }

  /**
   * Clear a region from replayEditsPerRegion.
   * 
   * @param regionName region name.
   */
  public void clearRegionReplayEditsSize(byte[] regionName) {
    replayEditsPerRegion.remove(regionName);
  }
}
