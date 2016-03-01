/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.io.hfile.bucket;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.BucketEntry;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

/**
 * This class is used to allocate a block with specified size and free the block
 * when evicting. It manages an array of buckets, each bucket is associated with
 * a size and caches elements up to this size. For a completely empty bucket, this
 * size could be re-specified dynamically.
 * 
 * This class is not thread safe.
 */
@InterfaceAudience.Private
@JsonIgnoreProperties({"indexStatistics", "freeSize", "usedSize"})
public final class BucketAllocator {
  private static final Log LOG = LogFactory.getLog(BucketAllocator.class);

  @JsonIgnoreProperties({"completelyFree", "uninstantiated"})
  public final static class Bucket {
    private long baseOffset;
    private int itemAllocationSize, sizeIndex;
    private int itemCount;
    private int freeList[];
    private int freeCount, usedCount;

    public Bucket(long offset) {
      baseOffset = offset;
      sizeIndex = -1;
    }

    void reconfigure(int sizeIndex, int[] bucketSizes, long bucketCapacity) {
      Preconditions.checkElementIndex(sizeIndex, bucketSizes.length);
      this.sizeIndex = sizeIndex;
      itemAllocationSize = bucketSizes[sizeIndex];
      itemCount = (int) (bucketCapacity / (long) itemAllocationSize);
      freeCount = itemCount;
      usedCount = 0;
      freeList = new int[itemCount];
      for (int i = 0; i < freeCount; ++i)
        freeList[i] = i;
    }

    public boolean isUninstantiated() {
      return sizeIndex == -1;
    }

    public int sizeIndex() {
      return sizeIndex;
    }

    public int getItemAllocationSize() {
      return itemAllocationSize;
    }

    public boolean hasFreeSpace() {
      return freeCount > 0;
    }

    public boolean isCompletelyFree() {
      return usedCount == 0;
    }

    public int freeCount() {
      return freeCount;
    }

    public int usedCount() {
      return usedCount;
    }

    public int getFreeBytes() {
      return freeCount * itemAllocationSize;
    }

    public int getUsedBytes() {
      return usedCount * itemAllocationSize;
    }

    public long getBaseOffset() {
      return baseOffset;
    }

    /**
     * Allocate a block in this bucket, return the offset representing the
     * position in physical space
     * @return the offset in the IOEngine
     */
    public long allocate() {
      assert freeCount > 0; // Else should not have been called
      assert sizeIndex != -1;
      ++usedCount;
      long offset = baseOffset + (freeList[--freeCount] * itemAllocationSize);
      assert offset >= 0;
      return offset;
    }

    public void addAllocation(long offset) throws BucketAllocatorException {
      offset -= baseOffset;
      if (offset < 0 || offset % itemAllocationSize != 0)
        throw new BucketAllocatorException(
            "Attempt to add allocation for bad offset: " + offset + " base="
                + baseOffset + ", bucket size=" + itemAllocationSize);
      int idx = (int) (offset / itemAllocationSize);
      boolean matchFound = false;
      for (int i = 0; i < freeCount; ++i) {
        if (matchFound) freeList[i - 1] = freeList[i];
        else if (freeList[i] == idx) matchFound = true;
      }
      if (!matchFound)
        throw new BucketAllocatorException("Couldn't find match for index "
            + idx + " in free list");
      ++usedCount;
      --freeCount;
    }

    private void free(long offset) {
      offset -= baseOffset;
      assert offset >= 0;
      assert offset < itemCount * itemAllocationSize;
      assert offset % itemAllocationSize == 0;
      assert usedCount > 0;
      assert freeCount < itemCount; // Else duplicate free
      int item = (int) (offset / (long) itemAllocationSize);
      assert !freeListContains(item);
      --usedCount;
      freeList[freeCount++] = item;
    }

    private boolean freeListContains(int blockNo) {
      for (int i = 0; i < freeCount; ++i) {
        if (freeList[i] == blockNo) return true;
      }
      return false;
    }
  }

  final class BucketSizeInfo {
    // Free bucket means it has space to allocate a block;
    // Completely free bucket means it has no block.
    private LinkedMap bucketList, freeBuckets, completelyFreeBuckets;
    private int sizeIndex;

    BucketSizeInfo(int sizeIndex) {
      bucketList = new LinkedMap();
      freeBuckets = new LinkedMap();
      completelyFreeBuckets = new LinkedMap();
      this.sizeIndex = sizeIndex;
    }

    public synchronized void instantiateBucket(Bucket b) {
      assert b.isUninstantiated() || b.isCompletelyFree();
      b.reconfigure(sizeIndex, bucketSizes, bucketCapacity);
      bucketList.put(b, b);
      freeBuckets.put(b, b);
      completelyFreeBuckets.put(b, b);
    }

    public int sizeIndex() {
      return sizeIndex;
    }

    /**
     * Find a bucket to allocate a block
     * @return the offset in the IOEngine
     */
    public long allocateBlock() {
      Bucket b = null;
      if (freeBuckets.size() > 0) {
        // Use up an existing one first...
        b = (Bucket) freeBuckets.lastKey();
      }
      if (b == null) {
        b = grabGlobalCompletelyFreeBucket();
        if (b != null) instantiateBucket(b);
      }
      if (b == null) return -1;
      long result = b.allocate();
      blockAllocated(b);
      return result;
    }

    void blockAllocated(Bucket b) {
      if (!b.isCompletelyFree()) completelyFreeBuckets.remove(b);
      if (!b.hasFreeSpace()) freeBuckets.remove(b);
    }

    public Bucket findAndRemoveCompletelyFreeBucket() {
      Bucket b = null;
      assert bucketList.size() > 0;
      if (bucketList.size() == 1) {
        // So we never get complete starvation of a bucket for a size
        return null;
      }

      if (completelyFreeBuckets.size() > 0) {
        b = (Bucket) completelyFreeBuckets.firstKey();
        removeBucket(b);
      }
      return b;
    }

    private synchronized void removeBucket(Bucket b) {
      assert b.isCompletelyFree();
      bucketList.remove(b);
      freeBuckets.remove(b);
      completelyFreeBuckets.remove(b);
    }

    public void freeBlock(Bucket b, long offset) {
      assert bucketList.containsKey(b);
      // else we shouldn't have anything to free...
      assert (!completelyFreeBuckets.containsKey(b));
      b.free(offset);
      if (!freeBuckets.containsKey(b)) freeBuckets.put(b, b);
      if (b.isCompletelyFree()) completelyFreeBuckets.put(b, b);
    }

    public synchronized IndexStatistics statistics() {
      long free = 0, used = 0;
      for (Object obj : bucketList.keySet()) {
        Bucket b = (Bucket) obj;
        free += b.freeCount();
        used += b.usedCount();
      }
      return new IndexStatistics(free, used, bucketSizes[sizeIndex]);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this.getClass())
        .add("sizeIndex", sizeIndex)
        .add("bucketSize", bucketSizes[sizeIndex])
        .toString();
    }
  }

  // Default block size in hbase is 64K, so we choose more sizes near 64K, you'd better
  // reset it according to your cluster's block size distribution
  // TODO Support the view of block size distribution statistics
  // TODO: Why we add the extra 1024 bytes? Slop?
  private static final int DEFAULT_BUCKET_SIZES[] = { 4 * 1024 + 1024, 8 * 1024 + 1024,
      16 * 1024 + 1024, 32 * 1024 + 1024, 40 * 1024 + 1024, 48 * 1024 + 1024,
      56 * 1024 + 1024, 64 * 1024 + 1024, 96 * 1024 + 1024, 128 * 1024 + 1024,
      192 * 1024 + 1024, 256 * 1024 + 1024, 384 * 1024 + 1024,
      512 * 1024 + 1024 };

  /**
   * Round up the given block size to bucket size, and get the corresponding
   * BucketSizeInfo
   */
  public BucketSizeInfo roundUpToBucketSizeInfo(int blockSize) {
    for (int i = 0; i < bucketSizes.length; ++i)
      if (blockSize <= bucketSizes[i])
        return bucketSizeInfos[i];
    return null;
  }

  /**
   * So, what is the minimum amount of items we'll tolerate in a single bucket?
   */
  static public final int FEWEST_ITEMS_IN_BUCKET = 4;

  private final int[] bucketSizes;
  private final int bigItemSize;
  // The capacity size for each bucket
  private final long bucketCapacity;
  private Bucket[] buckets;
  private BucketSizeInfo[] bucketSizeInfos;
  private final long totalSize;
  private long usedSize = 0;

  BucketAllocator(long availableSpace, int[] bucketSizes)
      throws BucketAllocatorException {
    this.bucketSizes = bucketSizes == null ? DEFAULT_BUCKET_SIZES : bucketSizes;
    Arrays.sort(this.bucketSizes);
    this.bigItemSize = Ints.max(this.bucketSizes);
    this.bucketCapacity = FEWEST_ITEMS_IN_BUCKET * bigItemSize;
    buckets = new Bucket[(int) (availableSpace / bucketCapacity)];
    if (buckets.length < this.bucketSizes.length)
      throw new BucketAllocatorException("Bucket allocator size too small (" + buckets.length +
        "); must have room for at least " + this.bucketSizes.length + " buckets");
    bucketSizeInfos = new BucketSizeInfo[this.bucketSizes.length];
    for (int i = 0; i < this.bucketSizes.length; ++i) {
      bucketSizeInfos[i] = new BucketSizeInfo(i);
    }
    for (int i = 0; i < buckets.length; ++i) {
      buckets[i] = new Bucket(bucketCapacity * i);
      bucketSizeInfos[i < this.bucketSizes.length ? i : this.bucketSizes.length - 1]
          .instantiateBucket(buckets[i]);
    }
    this.totalSize = ((long) buckets.length) * bucketCapacity;
    if (LOG.isInfoEnabled()) {
      LOG.info("Cache totalSize=" + this.totalSize + ", buckets=" + this.buckets.length +
        ", bucket capacity=" + this.bucketCapacity +
        "=(" + FEWEST_ITEMS_IN_BUCKET + "*" + this.bigItemSize + ")=" +
        "(FEWEST_ITEMS_IN_BUCKET*(largest configured bucketcache size))");
    }
  }

  /**
   * Rebuild the allocator's data structures from a persisted map.
   * @param availableSpace capacity of cache
   * @param map A map stores the block key and BucketEntry(block's meta data
   *          like offset, length)
   * @param realCacheSize cached data size statistics for bucket cache
   * @throws BucketAllocatorException
   */
  BucketAllocator(long availableSpace, int[] bucketSizes, Map<BlockCacheKey, BucketEntry> map,
      AtomicLong realCacheSize) throws BucketAllocatorException {
    this(availableSpace, bucketSizes);

    // each bucket has an offset, sizeindex. probably the buckets are too big
    // in our default state. so what we do is reconfigure them according to what
    // we've found. we can only reconfigure each bucket once; if more than once,
    // we know there's a bug, so we just log the info, throw, and start again...
    boolean[] reconfigured = new boolean[buckets.length];
    for (Map.Entry<BlockCacheKey, BucketEntry> entry : map.entrySet()) {
      long foundOffset = entry.getValue().offset();
      int foundLen = entry.getValue().getLength();
      int bucketSizeIndex = -1;
      for (int i = 0; i < bucketSizes.length; ++i) {
        if (foundLen <= bucketSizes[i]) {
          bucketSizeIndex = i;
          break;
        }
      }
      if (bucketSizeIndex == -1) {
        throw new BucketAllocatorException(
            "Can't match bucket size for the block with size " + foundLen);
      }
      int bucketNo = (int) (foundOffset / bucketCapacity);
      if (bucketNo < 0 || bucketNo >= buckets.length)
        throw new BucketAllocatorException("Can't find bucket " + bucketNo
            + ", total buckets=" + buckets.length
            + "; did you shrink the cache?");
      Bucket b = buckets[bucketNo];
      if (reconfigured[bucketNo]) {
        if (b.sizeIndex() != bucketSizeIndex)
          throw new BucketAllocatorException(
              "Inconsistent allocation in bucket map;");
      } else {
        if (!b.isCompletelyFree())
          throw new BucketAllocatorException("Reconfiguring bucket "
              + bucketNo + " but it's already allocated; corrupt data");
        // Need to remove the bucket from whichever list it's currently in at
        // the moment...
        BucketSizeInfo bsi = bucketSizeInfos[bucketSizeIndex];
        BucketSizeInfo oldbsi = bucketSizeInfos[b.sizeIndex()];
        oldbsi.removeBucket(b);
        bsi.instantiateBucket(b);
        reconfigured[bucketNo] = true;
      }
      realCacheSize.addAndGet(foundLen);
      buckets[bucketNo].addAllocation(foundOffset);
      usedSize += buckets[bucketNo].getItemAllocationSize();
      bucketSizeInfos[bucketSizeIndex].blockAllocated(b);
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder(1024);
    for (int i = 0; i < buckets.length; ++i) {
      Bucket b = buckets[i];
      if (i > 0) sb.append(", ");
      sb.append("bucket.").append(i).append(": size=").append(b.getItemAllocationSize());
      sb.append(", freeCount=").append(b.freeCount()).append(", used=").append(b.usedCount());
    }
    return sb.toString();
  }

  public long getUsedSize() {
    return this.usedSize;
  }

  public long getFreeSize() {
    return this.totalSize - getUsedSize();
  }

  public long getTotalSize() {
    return this.totalSize;
  }

  /**
   * Allocate a block with specified size. Return the offset
   * @param blockSize size of block
   * @throws BucketAllocatorException
   * @throws CacheFullException
   * @return the offset in the IOEngine
   */
  public synchronized long allocateBlock(int blockSize) throws CacheFullException,
      BucketAllocatorException {
    assert blockSize > 0;
    BucketSizeInfo bsi = roundUpToBucketSizeInfo(blockSize);
    if (bsi == null) {
      throw new BucketAllocatorException("Allocation too big size=" + blockSize +
        "; adjust BucketCache sizes " + CacheConfig.BUCKET_CACHE_BUCKETS_KEY +
        " to accomodate if size seems reasonable and you want it cached.");
    }
    long offset = bsi.allocateBlock();

    // Ask caller to free up space and try again!
    if (offset < 0)
      throw new CacheFullException(blockSize, bsi.sizeIndex());
    usedSize += bucketSizes[bsi.sizeIndex()];
    return offset;
  }

  private Bucket grabGlobalCompletelyFreeBucket() {
    for (BucketSizeInfo bsi : bucketSizeInfos) {
      Bucket b = bsi.findAndRemoveCompletelyFreeBucket();
      if (b != null) return b;
    }
    return null;
  }

  /**
   * Free a block with the offset
   * @param offset block's offset
   * @return size freed
   */
  public synchronized int freeBlock(long offset) {
    int bucketNo = (int) (offset / bucketCapacity);
    assert bucketNo >= 0 && bucketNo < buckets.length;
    Bucket targetBucket = buckets[bucketNo];
    bucketSizeInfos[targetBucket.sizeIndex()].freeBlock(targetBucket, offset);
    usedSize -= targetBucket.getItemAllocationSize();
    return targetBucket.getItemAllocationSize();
  }

  public int sizeIndexOfAllocation(long offset) {
    int bucketNo = (int) (offset / bucketCapacity);
    assert bucketNo >= 0 && bucketNo < buckets.length;
    Bucket targetBucket = buckets[bucketNo];
    return targetBucket.sizeIndex();
  }

  public int sizeOfAllocation(long offset) {
    int bucketNo = (int) (offset / bucketCapacity);
    assert bucketNo >= 0 && bucketNo < buckets.length;
    Bucket targetBucket = buckets[bucketNo];
    return targetBucket.getItemAllocationSize();
  }

  static class IndexStatistics {
    private long freeCount, usedCount, itemSize, totalCount;

    public long freeCount() {
      return freeCount;
    }

    public long usedCount() {
      return usedCount;
    }

    public long totalCount() {
      return totalCount;
    }

    public long freeBytes() {
      return freeCount * itemSize;
    }

    public long usedBytes() {
      return usedCount * itemSize;
    }

    public long totalBytes() {
      return totalCount * itemSize;
    }

    public long itemSize() {
      return itemSize;
    }

    public IndexStatistics(long free, long used, long itemSize) {
      setTo(free, used, itemSize);
    }

    public IndexStatistics() {
      setTo(-1, -1, 0);
    }

    public void setTo(long free, long used, long itemSize) {
      this.itemSize = itemSize;
      this.freeCount = free;
      this.usedCount = used;
      this.totalCount = free + used;
    }
  }

  public Bucket [] getBuckets() {
    return this.buckets;
  }

  void logStatistics() {
    IndexStatistics total = new IndexStatistics();
    IndexStatistics[] stats = getIndexStatistics(total);
    LOG.info("Bucket allocator statistics follow:\n");
    LOG.info("  Free bytes=" + total.freeBytes() + "+; used bytes="
        + total.usedBytes() + "; total bytes=" + total.totalBytes());
    for (IndexStatistics s : stats) {
      LOG.info("  Object size " + s.itemSize() + " used=" + s.usedCount()
          + "; free=" + s.freeCount() + "; total=" + s.totalCount());
    }
  }

  IndexStatistics[] getIndexStatistics(IndexStatistics grandTotal) {
    IndexStatistics[] stats = getIndexStatistics();
    long totalfree = 0, totalused = 0;
    for (IndexStatistics stat : stats) {
      totalfree += stat.freeBytes();
      totalused += stat.usedBytes();
    }
    grandTotal.setTo(totalfree, totalused, 1);
    return stats;
  }

  IndexStatistics[] getIndexStatistics() {
    IndexStatistics[] stats = new IndexStatistics[bucketSizes.length];
    for (int i = 0; i < stats.length; ++i)
      stats[i] = bucketSizeInfos[i].statistics();
    return stats;
  }

  public long freeBlock(long freeList[]) {
    long sz = 0;
    for (int i = 0; i < freeList.length; ++i)
      sz += freeBlock(freeList[i]);
    return sz;
  }

}
