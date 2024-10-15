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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.MoreObjects;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hbase.thirdparty.com.google.common.primitives.Ints;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.map.LinkedMap;

/**
 * This class is used to allocate a block with specified size and free the block when evicting. It
 * manages an array of buckets, each bucket is associated with a size and caches elements up to this
 * size. For a completely empty bucket, this size could be re-specified dynamically.
 * <p/>
 * This class is not thread safe.
 */
@InterfaceAudience.Private
public final class BucketAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(BucketAllocator.class);

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
     * Allocate a block in this bucket, return the offset representing the position in physical
     * space
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

    public boolean addAllocation(long offset) throws BucketAllocatorException {
      offset -= baseOffset;
      if (offset < 0 || offset % itemAllocationSize != 0)
        throw new BucketAllocatorException("Attempt to add allocation for bad offset: " + offset
          + " base=" + baseOffset + ", bucket size=" + itemAllocationSize);
      int idx = (int) (offset / itemAllocationSize);
      boolean matchFound = false;
      for (int i = 0; i < freeCount; ++i) {
        if (matchFound) freeList[i - 1] = freeList[i];
        else if (freeList[i] == idx) matchFound = true;
      }
      if (!matchFound) {
        LOG.warn("We found more entries for bucket starting at offset {} for blocks of {} size. "
          + "Skipping entry at cache offset {}", baseOffset, itemAllocationSize, offset);
        return false;
      }
      ++usedCount;
      --freeCount;
      return true;
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
    // only modified under synchronization, but also read outside it.
    private volatile long fragmentationBytes;
    private int sizeIndex;

    BucketSizeInfo(int sizeIndex) {
      bucketList = new LinkedMap();
      freeBuckets = new LinkedMap();
      completelyFreeBuckets = new LinkedMap();
      fragmentationBytes = 0;
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
    public long allocateBlock(int blockSize) {
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
      if (blockSize < b.getItemAllocationSize()) {
        fragmentationBytes += b.getItemAllocationSize() - blockSize;
      }
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

    public void freeBlock(Bucket b, long offset, int length) {
      assert bucketList.containsKey(b);
      // else we shouldn't have anything to free...
      assert (!completelyFreeBuckets.containsKey(b));
      b.free(offset);
      if (length < b.getItemAllocationSize()) {
        fragmentationBytes -= b.getItemAllocationSize() - length;
      }
      if (!freeBuckets.containsKey(b)) freeBuckets.put(b, b);
      if (b.isCompletelyFree()) completelyFreeBuckets.put(b, b);
    }

    public synchronized IndexStatistics statistics() {
      long free = 0, used = 0;
      int full = 0;
      for (Object obj : bucketList.keySet()) {
        Bucket b = (Bucket) obj;
        free += b.freeCount();
        used += b.usedCount();
        if (!b.hasFreeSpace()) {
          full++;
        }
      }
      int bucketObjectSize = bucketSizes[sizeIndex];
      // this is most likely to always be 1 or 0
      int fillingBuckets = Math.max(0, freeBuckets.size() - completelyFreeBuckets.size());
      // if bucket capacity is not perfectly divisible by a bucket's object size, there will
      // be some left over per bucket. for some object sizes this may be large enough to be
      // non-trivial and worth tuning by choosing a more divisible object size.
      long wastedBytes = (bucketCapacity % bucketObjectSize) * (full + fillingBuckets);
      return new IndexStatistics(free, used, bucketObjectSize, full, completelyFreeBuckets.size(),
        wastedBytes, fragmentationBytes);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this.getClass()).add("sizeIndex", sizeIndex)
        .add("bucketSize", bucketSizes[sizeIndex]).toString();
    }
  }

  // Default block size in hbase is 64K, so we choose more sizes near 64K, you'd better
  // reset it according to your cluster's block size distribution
  // The real block size in hfile maybe a little larger than the size we configured ,
  // so we need add extra 1024 bytes for fit.
  // TODO Support the view of block size distribution statistics
  private static final int DEFAULT_BUCKET_SIZES[] =
    { 4 * 1024 + 1024, 8 * 1024 + 1024, 16 * 1024 + 1024, 32 * 1024 + 1024, 40 * 1024 + 1024,
      48 * 1024 + 1024, 56 * 1024 + 1024, 64 * 1024 + 1024, 96 * 1024 + 1024, 128 * 1024 + 1024,
      192 * 1024 + 1024, 256 * 1024 + 1024, 384 * 1024 + 1024, 512 * 1024 + 1024 };

  /**
   * Round up the given block size to bucket size, and get the corresponding BucketSizeInfo
   */
  public BucketSizeInfo roundUpToBucketSizeInfo(int blockSize) {
    for (int i = 0; i < bucketSizes.length; ++i)
      if (blockSize <= bucketSizes[i]) return bucketSizeInfos[i];
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
  private transient long usedSize = 0;

  BucketAllocator(long availableSpace, int[] bucketSizes) throws BucketAllocatorException {
    this.bucketSizes = bucketSizes == null ? DEFAULT_BUCKET_SIZES : bucketSizes;
    Arrays.sort(this.bucketSizes);
    this.bigItemSize = Ints.max(this.bucketSizes);
    this.bucketCapacity = FEWEST_ITEMS_IN_BUCKET * (long) bigItemSize;
    buckets = new Bucket[(int) (availableSpace / bucketCapacity)];
    if (buckets.length < this.bucketSizes.length)
      throw new BucketAllocatorException("Bucket allocator size too small (" + buckets.length
        + "); must have room for at least " + this.bucketSizes.length + " buckets");
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
      LOG.info("Cache totalSize=" + this.totalSize + ", buckets=" + this.buckets.length
        + ", bucket capacity=" + this.bucketCapacity + "=(" + FEWEST_ITEMS_IN_BUCKET + "*"
        + this.bigItemSize + ")="
        + "(FEWEST_ITEMS_IN_BUCKET*(largest configured bucketcache size))");
    }
  }

  /**
   * Rebuild the allocator's data structures from a persisted map.
   * @param availableSpace capacity of cache
   * @param map            A map stores the block key and BucketEntry(block's meta data like offset,
   *                       length)
   * @param realCacheSize  cached data size statistics for bucket cache
   */
  BucketAllocator(long availableSpace, int[] bucketSizes, Map<BlockCacheKey, BucketEntry> map,
    LongAdder realCacheSize) throws BucketAllocatorException {
    this(availableSpace, bucketSizes);

    // each bucket has an offset, sizeindex. probably the buckets are too big
    // in our default state. so what we do is reconfigure them according to what
    // we've found. we can only reconfigure each bucket once; if more than once,
    // we know there's a bug, so we just log the info, throw, and start again...
    boolean[] reconfigured = new boolean[buckets.length];
    int sizeNotMatchedCount = 0;
    int insufficientCapacityCount = 0;
    Iterator<Map.Entry<BlockCacheKey, BucketEntry>> iterator = map.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<BlockCacheKey, BucketEntry> entry = iterator.next();
      long foundOffset = entry.getValue().offset();
      int foundLen = entry.getValue().getLength();
      int bucketSizeIndex = -1;
      for (int i = 0; i < this.bucketSizes.length; ++i) {
        if (foundLen <= this.bucketSizes[i]) {
          bucketSizeIndex = i;
          break;
        }
      }
      if (bucketSizeIndex == -1) {
        sizeNotMatchedCount++;
        iterator.remove();
        continue;
      }
      int bucketNo = (int) (foundOffset / bucketCapacity);
      if (bucketNo < 0 || bucketNo >= buckets.length) {
        insufficientCapacityCount++;
        iterator.remove();
        continue;
      }
      Bucket b = buckets[bucketNo];
      if (reconfigured[bucketNo]) {
        if (b.sizeIndex() != bucketSizeIndex) {
          throw new BucketAllocatorException("Inconsistent allocation in bucket map;");
        }
      } else {
        if (!b.isCompletelyFree()) {
          throw new BucketAllocatorException(
            "Reconfiguring bucket " + bucketNo + " but it's already allocated; corrupt data");
        }
        // Need to remove the bucket from whichever list it's currently in at
        // the moment...
        BucketSizeInfo bsi = bucketSizeInfos[bucketSizeIndex];
        BucketSizeInfo oldbsi = bucketSizeInfos[b.sizeIndex()];
        oldbsi.removeBucket(b);
        bsi.instantiateBucket(b);
        reconfigured[bucketNo] = true;
      }
      if (buckets[bucketNo].addAllocation(foundOffset)) {
        realCacheSize.add(foundLen);
        usedSize += buckets[bucketNo].getItemAllocationSize();
        bucketSizeInfos[bucketSizeIndex].blockAllocated(b);
      }
    }

    if (sizeNotMatchedCount > 0) {
      LOG.warn("There are " + sizeNotMatchedCount + " blocks which can't be rebuilt because "
        + "there is no matching bucket size for these blocks");
    }
    if (insufficientCapacityCount > 0) {
      LOG.warn("There are " + insufficientCapacityCount + " blocks which can't be rebuilt - "
        + "did you shrink the cache?");
    }
  }

  @Override
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
   * @return the offset in the IOEngine
   */
  public synchronized long allocateBlock(int blockSize)
    throws CacheFullException, BucketAllocatorException {
    assert blockSize > 0;
    BucketSizeInfo bsi = roundUpToBucketSizeInfo(blockSize);
    if (bsi == null) {
      throw new BucketAllocatorException("Allocation too big size=" + blockSize
        + "; adjust BucketCache sizes " + BlockCacheFactory.BUCKET_CACHE_BUCKETS_KEY
        + " to accomodate if size seems reasonable and you want it cached.");
    }
    long offset = bsi.allocateBlock(blockSize);

    // Ask caller to free up space and try again!
    if (offset < 0) throw new CacheFullException(blockSize, bsi.sizeIndex());
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
  public synchronized int freeBlock(long offset, int length) {
    int bucketNo = (int) (offset / bucketCapacity);
    assert bucketNo >= 0 && bucketNo < buckets.length;
    Bucket targetBucket = buckets[bucketNo];
    bucketSizeInfos[targetBucket.sizeIndex()].freeBlock(targetBucket, offset, length);
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

  /**
   * Statistics to give a glimpse into the distribution of BucketCache objects. Each configured
   * bucket size, denoted by {@link BucketSizeInfo}, gets an IndexStatistic. A BucketSizeInfo
   * allocates blocks of a configured size from claimed buckets. If you have a bucket size of 512k,
   * the corresponding BucketSizeInfo will always allocate chunks of 512k at a time regardless of
   * actual request.
   * <p>
   * Over time, as a BucketSizeInfo gets more allocations, it will claim more buckets from the total
   * pool of completelyFreeBuckets. As blocks are freed from a BucketSizeInfo, those buckets may be
   * returned to the completelyFreeBuckets pool.
   * <p>
   * The IndexStatistics help visualize how these buckets are currently distributed, through counts
   * of items, bytes, and fullBuckets. Additionally, mismatches between block sizes and bucket sizes
   * can manifest in inefficient cache usage. These typically manifest in three ways:
   * <p>
   * 1. Allocation failures, because block size is larger than max bucket size. These show up in
   * logs and can be alleviated by adding larger bucket sizes if appropriate.<br>
   * 2. Memory fragmentation, because blocks are typically smaller than the bucket size. See
   * {@link #fragmentationBytes()} for details.<br>
   * 3. Memory waste, because a bucket's itemSize is not a perfect divisor of bucketCapacity. see
   * {@link #wastedBytes()} for details.<br>
   */
  static class IndexStatistics {
    private long freeCount, usedCount, itemSize, totalCount, wastedBytes, fragmentationBytes;
    private int fullBuckets, completelyFreeBuckets;

    /**
     * How many more items can be allocated from the currently claimed blocks of this bucket size
     */
    public long freeCount() {
      return freeCount;
    }

    /**
     * How many items are currently taking up space in this bucket size's buckets
     */
    public long usedCount() {
      return usedCount;
    }

    /**
     * Combined {@link #freeCount()} + {@link #usedCount()}
     */
    public long totalCount() {
      return totalCount;
    }

    /**
     * How many more bytes can be allocated from the currently claimed blocks of this bucket size
     */
    public long freeBytes() {
      return freeCount * itemSize;
    }

    /**
     * How many bytes are currently taking up space in this bucket size's buckets Note: If your
     * items are less than the bucket size of this bucket, the actual used bytes by items will be
     * lower than this value. But since a bucket size can only allocate items of a single size, this
     * value is the true number of used bytes. The difference will be counted in
     * {@link #fragmentationBytes()}.
     */
    public long usedBytes() {
      return usedCount * itemSize;
    }

    /**
     * Combined {@link #totalCount()} * {@link #itemSize()}
     */
    public long totalBytes() {
      return totalCount * itemSize;
    }

    /**
     * This bucket size can only allocate items of this size, even if the requested allocation size
     * is smaller. The rest goes towards {@link #fragmentationBytes()}.
     */
    public long itemSize() {
      return itemSize;
    }

    /**
     * How many buckets have been completely filled by blocks for this bucket size. These buckets
     * can't accept any more blocks unless some existing are freed.
     */
    public int fullBuckets() {
      return fullBuckets;
    }

    /**
     * How many buckets are currently claimed by this bucket size but as yet totally unused. These
     * buckets are available for reallocation to other bucket sizes if those fill up.
     */
    public int completelyFreeBuckets() {
      return completelyFreeBuckets;
    }

    /**
     * If {@link #bucketCapacity} is not perfectly divisible by this {@link #itemSize()}, the
     * remainder will be unusable by in buckets of this size. A high value here may be optimized by
     * trying to choose bucket sizes which can better divide {@link #bucketCapacity}.
     */
    public long wastedBytes() {
      return wastedBytes;
    }

    /**
     * Every time you allocate blocks in these buckets where the block size is less than the bucket
     * size, fragmentation increases by that difference. You can reduce fragmentation by lowering
     * the bucket size so that it is closer to the typical block size. This may have the consequence
     * of bumping some blocks to the next larger bucket size, so experimentation may be needed.
     */
    public long fragmentationBytes() {
      return fragmentationBytes;
    }

    public IndexStatistics(long free, long used, long itemSize, int fullBuckets,
      int completelyFreeBuckets, long wastedBytes, long fragmentationBytes) {
      setTo(free, used, itemSize, fullBuckets, completelyFreeBuckets, wastedBytes,
        fragmentationBytes);
    }

    public IndexStatistics() {
      setTo(-1, -1, 0, 0, 0, 0, 0);
    }

    public void setTo(long free, long used, long itemSize, int fullBuckets,
      int completelyFreeBuckets, long wastedBytes, long fragmentationBytes) {
      this.itemSize = itemSize;
      this.freeCount = free;
      this.usedCount = used;
      this.totalCount = free + used;
      this.fullBuckets = fullBuckets;
      this.completelyFreeBuckets = completelyFreeBuckets;
      this.wastedBytes = wastedBytes;
      this.fragmentationBytes = fragmentationBytes;
    }
  }

  public Bucket[] getBuckets() {
    return this.buckets;
  }

  void logDebugStatistics() {
    if (!LOG.isDebugEnabled()) {
      return;
    }

    IndexStatistics total = new IndexStatistics();
    IndexStatistics[] stats = getIndexStatistics(total);
    LOG.debug("Bucket allocator statistics follow:");
    LOG.debug(
      "  Free bytes={}; used bytes={}; total bytes={}; wasted bytes={}; fragmentation bytes={}; "
        + "completelyFreeBuckets={}",
      total.freeBytes(), total.usedBytes(), total.totalBytes(), total.wastedBytes(),
      total.fragmentationBytes(), total.completelyFreeBuckets());
    for (IndexStatistics s : stats) {
      LOG.debug(
        "  Object size {}; used={}; free={}; total={}; wasted bytes={}; fragmentation bytes={}, "
          + "full buckets={}",
        s.itemSize(), s.usedCount(), s.freeCount(), s.totalCount(), s.wastedBytes(),
        s.fragmentationBytes(), s.fullBuckets());
    }
  }

  IndexStatistics[] getIndexStatistics(IndexStatistics grandTotal) {
    IndexStatistics[] stats = getIndexStatistics();
    long totalfree = 0, totalused = 0, totalWasted = 0, totalFragmented = 0;
    int fullBuckets = 0, completelyFreeBuckets = 0;

    for (IndexStatistics stat : stats) {
      totalfree += stat.freeBytes();
      totalused += stat.usedBytes();
      totalWasted += stat.wastedBytes();
      totalFragmented += stat.fragmentationBytes();
      fullBuckets += stat.fullBuckets();
      completelyFreeBuckets += stat.completelyFreeBuckets();
    }
    grandTotal.setTo(totalfree, totalused, 1, fullBuckets, completelyFreeBuckets, totalWasted,
      totalFragmented);
    return stats;
  }

  IndexStatistics[] getIndexStatistics() {
    IndexStatistics[] stats = new IndexStatistics[bucketSizes.length];
    for (int i = 0; i < stats.length; ++i)
      stats[i] = bucketSizeInfos[i].statistics();
    return stats;
  }

  public int getBucketIndex(long offset) {
    return (int) (offset / bucketCapacity);
  }

  /**
   * Returns a set of indices of the buckets that are least filled excluding the offsets, we also
   * the fully free buckets for the BucketSizes where everything is empty and they only have one
   * completely free bucket as a reserved
   * @param excludedBuckets the buckets that need to be excluded due to currently being in used
   * @param bucketCount     max Number of buckets to return
   * @return set of bucket indices which could be used for eviction
   */
  public Set<Integer> getLeastFilledBuckets(Set<Integer> excludedBuckets, int bucketCount) {
    Queue<Integer> queue = MinMaxPriorityQueue.<Integer> orderedBy(new Comparator<Integer>() {
      @Override
      public int compare(Integer left, Integer right) {
        // We will always get instantiated buckets
        return Float.compare(((float) buckets[left].usedCount) / buckets[left].itemCount,
          ((float) buckets[right].usedCount) / buckets[right].itemCount);
      }
    }).maximumSize(bucketCount).create();

    for (int i = 0; i < buckets.length; i++) {
      if (!excludedBuckets.contains(i) && !buckets[i].isUninstantiated() &&
      // Avoid the buckets that are the only buckets for a sizeIndex
        bucketSizeInfos[buckets[i].sizeIndex()].bucketList.size() != 1
      ) {
        queue.add(i);
      }
    }

    Set<Integer> result = new HashSet<>(bucketCount);
    result.addAll(queue);

    return result;
  }
}
