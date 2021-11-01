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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The compaction pipeline of a {@link CompactingMemStore}, is a FIFO queue of segments.
 * It supports pushing a segment at the head of the pipeline and removing a segment from the
 * tail when it is flushed to disk.
 * It also supports swap method to allow the in-memory compaction swap a subset of the segments
 * at the tail of the pipeline with a new (compacted) one. This swap succeeds only if the version
 * number passed with the list of segments to swap is the same as the current version of the
 * pipeline.
 * Essentially, there are two methods which can change the structure of the pipeline: pushHead()
 * and swap(), the later is used both by a flush to disk and by an in-memory compaction.
 * The pipeline version is updated by swap(); it allows to identify conflicting operations at the
 * suffix of the pipeline.
 *
 * The synchronization model is copy-on-write. Methods which change the structure of the
 * pipeline (pushHead() and swap()) apply their changes in the context of a lock. They also make
 * a read-only copy of the pipeline's list. Read methods read from a read-only copy. If a read
 * method accesses the read-only copy more than once it makes a local copy of it
 * to ensure it accesses the same copy.
 *
 * The methods getVersionedList(), getVersionedTail(), and flattenOneSegment() are also
 * protected by a lock since they need to have a consistent (atomic) view of the pipeline list
 * and version number.
 */
@InterfaceAudience.Private
public class CompactionPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionPipeline.class);

  public final static long FIXED_OVERHEAD = ClassSize
      .align(ClassSize.OBJECT + (3 * ClassSize.REFERENCE) + Bytes.SIZEOF_LONG);
  public final static long DEEP_OVERHEAD = FIXED_OVERHEAD + (2 * ClassSize.LINKEDLIST);

  private final RegionServicesForStores region;
  private final LinkedList<ImmutableSegment> pipeline = new LinkedList<>();
  // The list is volatile to avoid reading a new allocated reference before the c'tor is executed
  private volatile LinkedList<ImmutableSegment> readOnlyCopy = new LinkedList<>();
  /**
   * <pre>
   * Version is volatile to ensure it is atomically read when not using a lock.
   * To indicate whether the suffix of pipleline changes：
   * 1.for {@link CompactionPipeline#pushHead(MutableSegment)},new {@link ImmutableSegment} only
   *   added at Head, {@link #version} not change.
   * 2.for {@link CompactionPipeline#swap},{@link #version} increase.
   * 3.for {@link CompactionPipeline#replaceAtIndex},{@link #version} increase.
   * </pre>
   */
  private volatile long version = 0;

  public CompactionPipeline(RegionServicesForStores region) {
    this.region = region;
  }

  public boolean pushHead(MutableSegment segment) {
    // Record the ImmutableSegment' heap overhead when initialing
    MemStoreSizing memstoreAccounting = new NonThreadSafeMemStoreSizing();
    ImmutableSegment immutableSegment = SegmentFactory.instance().
        createImmutableSegment(segment, memstoreAccounting);
    if (region != null) {
      region.addMemStoreSize(memstoreAccounting.getDataSize(), memstoreAccounting.getHeapSize(),
        memstoreAccounting.getOffHeapSize(), memstoreAccounting.getCellsCount());
    }
    synchronized (pipeline){
      boolean res = addFirst(immutableSegment);
      readOnlyCopy = new LinkedList<>(pipeline);
      return res;
    }
  }

  public VersionedSegmentsList getVersionedList() {
    synchronized (pipeline){
      return new VersionedSegmentsList(readOnlyCopy, version);
    }
  }

  public VersionedSegmentsList getVersionedTail() {
    synchronized (pipeline){
      ArrayList<ImmutableSegment> segmentList = new ArrayList<>();
      if(!pipeline.isEmpty()) {
        segmentList.add(0, pipeline.getLast());
      }
      return new VersionedSegmentsList(segmentList, version);
    }
  }

  /**
   * Swaps the versioned list at the tail of the pipeline with a new segment.
   * Swapping only if there were no changes to the suffix of the list since the version list was
   * created.
   * @param versionedList suffix of the pipeline to be replaced can be tail or all the pipeline
   * @param segment new segment to replace the suffix. Can be null if the suffix just needs to be
   *                removed.
   * @param closeSuffix whether to close the suffix (to release memory), as part of swapping it out
   *        During index merge op this will be false and for compaction it will be true.
   * @param updateRegionSize whether to update the region size. Update the region size,
   *                         when the pipeline is swapped as part of in-memory-flush and
   *                         further merge/compaction. Don't update the region size when the
   *                         swap is result of the snapshot (flush-to-disk).
   * @return true iff swapped tail with new segment
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="VO_VOLATILE_INCREMENT",
      justification="Increment is done under a synchronize block so safe")
  public boolean swap(VersionedSegmentsList versionedList, ImmutableSegment segment,
      boolean closeSuffix, boolean updateRegionSize) {
    if (versionedList.getVersion() != version) {
      return false;
    }
    List<ImmutableSegment> suffix;
    synchronized (pipeline){
      if(versionedList.getVersion() != version) {
        return false;
      }
      suffix = versionedList.getStoreSegments();
      LOG.debug("Swapping pipeline suffix; before={}, new segment={}",
          versionedList.getStoreSegments().size(), segment);
      swapSuffix(suffix, segment, closeSuffix);
      readOnlyCopy = new LinkedList<>(pipeline);
      version++;
    }
    if (updateRegionSize && region != null) {
      // update the global memstore size counter
      long suffixDataSize = getSegmentsKeySize(suffix);
      long suffixHeapSize = getSegmentsHeapSize(suffix);
      long suffixOffHeapSize = getSegmentsOffHeapSize(suffix);
      int suffixCellsCount = getSegmentsCellsCount(suffix);
      long newDataSize = 0;
      long newHeapSize = 0;
      long newOffHeapSize = 0;
      int newCellsCount = 0;
      if (segment != null) {
        newDataSize = segment.getDataSize();
        newHeapSize = segment.getHeapSize();
        newOffHeapSize = segment.getOffHeapSize();
        newCellsCount = segment.getCellsCount();
      }
      long dataSizeDelta = suffixDataSize - newDataSize;
      long heapSizeDelta = suffixHeapSize - newHeapSize;
      long offHeapSizeDelta = suffixOffHeapSize - newOffHeapSize;
      int cellsCountDelta = suffixCellsCount - newCellsCount;
      region.addMemStoreSize(-dataSizeDelta, -heapSizeDelta, -offHeapSizeDelta, -cellsCountDelta);
      LOG.debug(
        "Suffix data size={}, new segment data size={}, suffix heap size={},new segment heap "
            + "size={} 　suffix off heap size={}, new segment off heap size={}, suffix cells "
            + "count={}, new segment cells count={}",
        suffixDataSize, newDataSize, suffixHeapSize, newHeapSize, suffixOffHeapSize, newOffHeapSize,
        suffixCellsCount, newCellsCount);
    }
    return true;
  }

  private static long getSegmentsHeapSize(List<? extends Segment> list) {
    long res = 0;
    for (Segment segment : list) {
      res += segment.getHeapSize();
    }
    return res;
  }

  private static long getSegmentsOffHeapSize(List<? extends Segment> list) {
    long res = 0;
    for (Segment segment : list) {
      res += segment.getOffHeapSize();
    }
    return res;
  }

  private static long getSegmentsKeySize(List<? extends Segment> list) {
    long res = 0;
    for (Segment segment : list) {
      res += segment.getDataSize();
    }
    return res;
  }

  private static int getSegmentsCellsCount(List<? extends Segment> list) {
    int res = 0;
    for (Segment segment : list) {
      res += segment.getCellsCount();
    }
    return res;
  }

  /**
   * If the caller holds the current version, go over the the pipeline and try to flatten each
   * segment. Flattening is replacing the ConcurrentSkipListMap based CellSet to CellArrayMap based.
   * Flattening of the segment that initially is not based on ConcurrentSkipListMap has no effect.
   * Return after one segment is successfully flatten.
   *
   * @return true iff a segment was successfully flattened
   */
  public boolean flattenOneSegment(long requesterVersion,
      CompactingMemStore.IndexType idxType,
      MemStoreCompactionStrategy.Action action) {

    if(requesterVersion != version) {
      LOG.warn("Segment flattening failed, because versions do not match. Requester version: "
          + requesterVersion + ", actual version: " + version);
      return false;
    }

    synchronized (pipeline){
      if(requesterVersion != version) {
        LOG.warn("Segment flattening failed, because versions do not match");
        return false;
      }
      int i = -1;
      for (ImmutableSegment s : pipeline) {
        i++;
        if ( s.canBeFlattened() ) {
          s.waitForUpdates(); // to ensure all updates preceding s in-memory flush have completed
          if (s.isEmpty()) {
            // after s.waitForUpdates() is called, there is no updates pending,if no cells in s,
            // we can skip it.
            continue;
          }
          // size to be updated
          MemStoreSizing newMemstoreAccounting = new NonThreadSafeMemStoreSizing();
          ImmutableSegment newS = SegmentFactory.instance().createImmutableSegmentByFlattening(
              (CSLMImmutableSegment)s,idxType,newMemstoreAccounting,action);
          replaceAtIndex(i,newS);
          if (region != null) {
            // Update the global memstore size counter upon flattening there is no change in the
            // data size
            MemStoreSize mss = newMemstoreAccounting.getMemStoreSize();
            region.addMemStoreSize(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(),
              mss.getCellsCount());
          }
          LOG.debug("Compaction pipeline segment {} flattened", s);
          return true;
        }
      }
    }
    // do not update the global memstore size counter and do not increase the version,
    // because all the cells remain in place
    return false;
  }

  public boolean isEmpty() {
    return readOnlyCopy.isEmpty();
  }

  public List<? extends Segment> getSegments() {
    return readOnlyCopy;
  }

  public long size() {
    return readOnlyCopy.size();
  }

  public long getMinSequenceId() {
    long minSequenceId = Long.MAX_VALUE;
    LinkedList<? extends Segment> localCopy = readOnlyCopy;
    if (!localCopy.isEmpty()) {
      minSequenceId = localCopy.getLast().getMinSequenceId();
    }
    return minSequenceId;
  }

  public MemStoreSize getTailSize() {
    LinkedList<? extends Segment> localCopy = readOnlyCopy;
    return localCopy.isEmpty()? new MemStoreSize(): localCopy.peekLast().getMemStoreSize();
  }

  public MemStoreSize getPipelineSize() {
    MemStoreSizing memStoreSizing = new NonThreadSafeMemStoreSizing();
    LinkedList<? extends Segment> localCopy = readOnlyCopy;
    for (Segment segment : localCopy) {
      memStoreSizing.incMemStoreSize(segment.getMemStoreSize());
    }
    return memStoreSizing.getMemStoreSize();
  }

  /**
   * Must be called under the {@link CompactionPipeline#pipeline} Lock.
   */
  private void swapSuffix(List<? extends Segment> suffix, ImmutableSegment segment,
      boolean closeSegmentsInSuffix) {
    matchAndRemoveSuffixFromPipeline(suffix);
    if (segment != null) {
      pipeline.addLast(segment);
    }
    // During index merge we won't be closing the segments undergoing the merge. Segment#close()
    // will release the MSLAB chunks to pool. But in case of index merge there wont be any data copy
    // from old MSLABs. So the new cells in new segment also refers to same chunks. In case of data
    // compaction, we would have copied the cells data from old MSLAB chunks into a new chunk
    // created for the result segment. So we can release the chunks associated with the compacted
    // segments.
    if (closeSegmentsInSuffix) {
      for (Segment itemInSuffix : suffix) {
        itemInSuffix.close();
      }
    }
  }

  /**
   * Checking that the {@link Segment}s in suffix input parameter is same as the {@link Segment}s in
   * {@link CompactionPipeline#pipeline} one by one from the last element to the first element of
   * suffix. If matched, remove suffix from {@link CompactionPipeline#pipeline}. <br/>
   * Must be called under the {@link CompactionPipeline#pipeline} Lock.
   */
  private void matchAndRemoveSuffixFromPipeline(List<? extends Segment> suffix) {
    if (suffix.isEmpty()) {
      return;
    }
    if (pipeline.size() < suffix.size()) {
      throw new IllegalStateException(
          "CODE-BUG:pipleine size:[" + pipeline.size() + "],suffix size:[" + suffix.size()
              + "],pipeline size must greater than or equals suffix size");
    }

    ListIterator<? extends Segment> suffixIterator = suffix.listIterator(suffix.size());
    ListIterator<? extends Segment> pipelineIterator = pipeline.listIterator(pipeline.size());
    int count = 0;
    while (suffixIterator.hasPrevious()) {
      Segment suffixSegment = suffixIterator.previous();
      Segment pipelineSegment = pipelineIterator.previous();
      if (suffixSegment != pipelineSegment) {
        throw new IllegalStateException("CODE-BUG:suffix last:[" + count + "]" + suffixSegment
            + " is not pipleline segment:[" + pipelineSegment + "]");
      }
      count++;
    }

    for (int index = 1; index <= count; index++) {
      pipeline.pollLast();
    }

  }

  // replacing one segment in the pipeline with a new one exactly at the same index
  // need to be called only within synchronized block
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "VO_VOLATILE_INCREMENT",
      justification = "replaceAtIndex is invoked under a synchronize block so safe")
  private void replaceAtIndex(int idx, ImmutableSegment newSegment) {
    pipeline.set(idx, newSegment);
    readOnlyCopy = new LinkedList<>(pipeline);
    // the version increment is indeed needed, because the swap uses removeAll() method of the
    // linked-list that compares the objects to find what to remove.
    // The flattening changes the segment object completely (creation pattern) and so
    // swap will not proceed correctly after concurrent flattening.
    version++;
  }

  public Segment getTail() {
    List<? extends Segment> localCopy = getSegments();
    if(localCopy.isEmpty()) {
      return null;
    }
    return localCopy.get(localCopy.size() - 1);
  }

  private boolean addFirst(ImmutableSegment segment) {
    pipeline.addFirst(segment);
    return true;
  }

  // debug method
  private boolean validateSuffixList(LinkedList<ImmutableSegment> suffix) {
    if(suffix.isEmpty()) {
      // empty suffix is always valid
      return true;
    }
    Iterator<ImmutableSegment> pipelineBackwardIterator = pipeline.descendingIterator();
    Iterator<ImmutableSegment> suffixBackwardIterator = suffix.descendingIterator();
    ImmutableSegment suffixCurrent;
    ImmutableSegment pipelineCurrent;
    for( ; suffixBackwardIterator.hasNext(); ) {
      if(!pipelineBackwardIterator.hasNext()) {
        // a suffix longer than pipeline is invalid
        return false;
      }
      suffixCurrent = suffixBackwardIterator.next();
      pipelineCurrent = pipelineBackwardIterator.next();
      if(suffixCurrent != pipelineCurrent) {
        // non-matching suffix
        return false;
      }
    }
    // suffix matches pipeline suffix
    return true;
  }

}
