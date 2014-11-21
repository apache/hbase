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

package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.hfile.bucket.CacheFullException;

/**
 * This class is used to allocate a block with specified size and free the block
 * when evicting. It manages an array of buckets, each bucket is associated with
 * a size and caches elements up to this size. For completely empty bucket, this
 * size could be re-specified dynamically.
 *
 * This class is not thread safe.
 */
public final class BucketAllocator {
  static final Log LOG = LogFactory.getLog(BucketAllocator.class);

  public static final int[] DEFAULT_BUCKETS = { 4 * 1024 + 1024,
    8 * 1024 + 1024 };

  final private static class Bucket {
    private final int[] bucketSizes;
    private final long bucketCapacity;
    private long baseOffset;
    private int itemAllocationSize, sizeIndex;
    private int itemCount;
    private int freeList[];
    private int freeCount, usedCount;

    public Bucket(int[] bucketSizes, long bucketCapacity, long offset) {
      this.bucketSizes = bucketSizes;
      this.bucketCapacity = bucketCapacity;
      baseOffset = offset;
      sizeIndex = -1;
    }

    void reconfigure(int sizeIndex) {
      this.sizeIndex = Preconditions.checkPositionIndex(sizeIndex,
          bucketSizes.length);
      itemAllocationSize = bucketSizes[sizeIndex];
      itemCount = (int) ((bucketCapacity) / (long) itemAllocationSize);
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

    public int itemAllocationSize() {
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

    public int freeBytes() {
      return freeCount * itemAllocationSize;
    }

    public int usedBytes() {
      return usedCount * itemAllocationSize;
    }

    public long baseOffset() {
      return baseOffset;
    }

    /**
     * Allocate a block in this bucket, return the offset representing the
     * position in physical space
     * @return the offset in the IOEngine
     */
    public long allocate() {
      Preconditions.checkState(freeCount > 0, "No space to allocate!");
      Preconditions.checkState(sizeIndex != -1);
      ++usedCount;
      return ConditionUtil
          .checkPositiveOffset(baseOffset + (freeList[--freeCount] * itemAllocationSize));
    }

    private void free(long offset) {
      Preconditions.checkState(usedCount > 0);
      Preconditions.checkState(freeCount < itemCount,
          "duplicate free, offset: " + offset);
      offset = ConditionUtil.checkOffset(offset - baseOffset,
          itemCount * itemAllocationSize);
      Preconditions.checkState(offset % itemAllocationSize == 0);
      int item = (int) (offset / (long) itemAllocationSize);
      Preconditions.checkState(!freeListContains(item), "Item at " + offset +
          " already on freelist!");

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

  public final class BucketSizeInfo {
    // Free bucket means it has space to allocate a block;
    // Completely free bucket means it has no block.
    private List<Bucket> bucketList, freeBuckets, completelyFreeBuckets;
    private int sizeIndex;

    BucketSizeInfo(int sizeIndex) {
      bucketList = new ArrayList<Bucket>();
      freeBuckets = new ArrayList<Bucket>();
      completelyFreeBuckets = new ArrayList<Bucket>();
      this.sizeIndex = sizeIndex;
    }

    public void instantiateBucket(Bucket b) {
      Preconditions.checkArgument(b.isUninstantiated() || b.isCompletelyFree());
      b.reconfigure(sizeIndex);
      bucketList.add(b);
      freeBuckets.add(b);
      completelyFreeBuckets.add(b);
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
      if (freeBuckets.size() > 0) // Use up an existing one first...
        b = freeBuckets.get(freeBuckets.size() - 1);
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
      Preconditions.checkState(bucketList.size() > 0);
      if (bucketList.size() == 1) {
        // So we never get complete starvation of a bucket for a size
        return null;
      }

      if (completelyFreeBuckets.size() > 0) {
        b = completelyFreeBuckets.get(0);
        removeBucket(b);
      }
      return b;
    }

    private void removeBucket(Bucket b) {
      Preconditions.checkArgument(b.isCompletelyFree());
      bucketList.remove(b);
      freeBuckets.remove(b);
      completelyFreeBuckets.remove(b);
    }

    public void freeBlock(Bucket b, long offset) {
      Preconditions.checkArgument(bucketList.contains(b));
      // else we shouldn't have anything to free...
      Preconditions.checkArgument(!completelyFreeBuckets.contains(b),
          "nothing to free!");
      b.free(offset);
      if (!freeBuckets.contains(b)) freeBuckets.add(b);
      if (b.isCompletelyFree()) completelyFreeBuckets.add(b);
    }

    public IndexStatistics statistics() {
      long free = 0, used = 0;
      for (Bucket b : bucketList) {
        free += b.freeCount();
        used += b.usedCount();
      }
      return new IndexStatistics(free, used, bucketSizes[sizeIndex]);
    }
  }

  private final int bucketSizes[];

  /**
   * Round up the given block size to bucket size, and get the corresponding
   * BucketSizeInfo
   * @param blockSize
   * @return BucketSizeInfo
   */
  public BucketSizeInfo roundUpToBucketSizeInfo(int blockSize) {
    for (int i = 0; i < bucketSizes.length; ++i)
      if (blockSize <= bucketSizes[i])
        return bucketSizeInfos[i];
    return null;
  }


  static public final int FEWEST_ITEMS_IN_BUCKET = 4;
  // The capacity size for each bucket

  private final long bucketCapacity;

  private final Bucket[] buckets;
  private final BucketSizeInfo[] bucketSizeInfos;
  private final long totalSize;

  private long usedSize = 0;

  public BucketAllocator(int[] bucketSizes, long availableSpace) throws
    BucketAllocatorException {
    this.bucketSizes = bucketSizes;
    int bigItemSize = bucketSizes[bucketSizes.length - 1];
    bucketCapacity = FEWEST_ITEMS_IN_BUCKET * bigItemSize;
    buckets = new Bucket[(int) (availableSpace / bucketCapacity)];
    if (buckets.length < bucketSizes.length)
      throw new BucketAllocatorException(
          "Bucket allocator size too small - must have room for at least "
              + bucketSizes.length + " buckets");
    bucketSizeInfos = new BucketSizeInfo[bucketSizes.length];
    for (int i = 0; i < bucketSizes.length; ++i) {
      bucketSizeInfos[i] = new BucketSizeInfo(i);
    }
    for (int i = 0; i < buckets.length; ++i) {
      buckets[i] = new Bucket(bucketSizes, bucketCapacity, bucketCapacity * i);
      bucketSizeInfos[i < bucketSizes.length ? i : bucketSizes.length - 1]
          .instantiateBucket(buckets[i]);
    }
    this.totalSize = ((long) buckets.length) * bucketCapacity;
  }

  public String getInfo() {
    StringBuilder sb = new StringBuilder(1024);
    for (int i = 0; i < buckets.length; ++i) {
      Bucket b = buckets[i];
      sb.append("    Bucket ").append(i).append(": ").append(b.itemAllocationSize());
      sb.append(" freeCount=").append(b.freeCount()).append(" used=")
          .append(b.usedCount());
      sb.append('\n');
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
   * @throws BucketAllocatorException,org.apache.hadoop.hbase.io.hfile.bucket.CacheFullException
   * @return the offset in the IOEngine
   */
  public synchronized long allocateBlock(int blockSize) throws
    CacheFullException,
      BucketAllocatorException {
    Preconditions.checkArgument(blockSize > 0);
    BucketSizeInfo bsi = roundUpToBucketSizeInfo(blockSize);
    if (bsi == null) {
      throw new BucketAllocatorException("Allocation too big size=" + blockSize);
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
    Preconditions.checkPositionIndex(bucketNo, buckets.length);
    Bucket targetBucket = buckets[bucketNo];
    bucketSizeInfos[targetBucket.sizeIndex()].freeBlock(targetBucket, offset);
    usedSize -= targetBucket.itemAllocationSize();
    return targetBucket.itemAllocationSize();
  }

  public int sizeOfAllocation(long offset) {
    int bucketNo = (int) (offset / bucketCapacity);
    Preconditions.checkPositionIndex(bucketNo, buckets.length);
    Bucket targetBucket = buckets[bucketNo];
    return targetBucket.itemAllocationSize();
  }

  public static class IndexStatistics {
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

  public void dumpToLog() {
    logStatistics();
    StringBuilder sb = new StringBuilder();
    for (Bucket b : buckets) {
      sb.append("Bucket:").append(b.baseOffset).append('\n');
      sb.append("  Size index: " + b.sizeIndex() + "; Free:" + b.freeCount
          + "; used:" + b.usedCount + "; freelist\n");
      for (int i = 0; i < b.freeCount(); ++i)
        sb.append(b.freeList[i]).append(',');
      sb.append('\n');
    }
    LOG.info(sb);
  }

  public void logStatistics() {
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

  public IndexStatistics[] getIndexStatistics(IndexStatistics grandTotal) {
    IndexStatistics[] stats = getIndexStatistics();
    long totalfree = 0, totalused = 0;
    for (IndexStatistics stat : stats) {
      totalfree += stat.freeBytes();
      totalused += stat.usedBytes();
    }
    grandTotal.setTo(totalfree, totalused, 1);
    return stats;
  }

  public IndexStatistics[] getIndexStatistics() {
    IndexStatistics[] stats = new IndexStatistics[bucketSizes.length];
    for (int i = 0; i < stats.length; ++i)
      stats[i] = bucketSizeInfos[i].statistics();
    return stats;
  }
}

