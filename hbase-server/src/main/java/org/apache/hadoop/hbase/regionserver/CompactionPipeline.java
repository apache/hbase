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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * The compaction pipeline of a {@link CompactingMemStore}, is a FIFO queue of segments.
 * It supports pushing a segment at the head of the pipeline and pulling a segment from the
 * tail to flush to disk.
 * It also supports swap operation to allow the compactor swap a subset of the segments with a new
 * (compacted) one. This swap succeeds only if the version number passed with the list of segments
 * to swap is the same as the current version of the pipeline.
 * The pipeline version is updated whenever swapping segments or pulling the segment at the tail.
 */
@InterfaceAudience.Private
public class CompactionPipeline {
  private static final Log LOG = LogFactory.getLog(CompactionPipeline.class);

  public final static long FIXED_OVERHEAD = ClassSize
      .align(ClassSize.OBJECT + (2 * ClassSize.REFERENCE) + Bytes.SIZEOF_LONG);
  public final static long DEEP_OVERHEAD = FIXED_OVERHEAD + ClassSize.LINKEDLIST;

  private final RegionServicesForStores region;
  private LinkedList<ImmutableSegment> pipeline;
  private long version;

  public CompactionPipeline(RegionServicesForStores region) {
    this.region = region;
    this.pipeline = new LinkedList<>();
    this.version = 0;
  }

  public boolean pushHead(MutableSegment segment) {
    ImmutableSegment immutableSegment = SegmentFactory.instance().
        createImmutableSegment(segment);
    synchronized (pipeline){
      return addFirst(immutableSegment);
    }
  }

  public VersionedSegmentsList getVersionedList() {
    synchronized (pipeline){
      List<ImmutableSegment> segmentList = new ArrayList<>(pipeline);
      return new VersionedSegmentsList(segmentList, version);
    }
  }

  public VersionedSegmentsList getVersionedTail() {
    synchronized (pipeline){
      List<ImmutableSegment> segmentList = new ArrayList<>();
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
   * @return true iff swapped tail with new segment
   */
  public boolean swap(
      VersionedSegmentsList versionedList, ImmutableSegment segment, boolean closeSuffix) {
    if (versionedList.getVersion() != version) {
      return false;
    }
    List<ImmutableSegment> suffix;
    synchronized (pipeline){
      if(versionedList.getVersion() != version) {
        return false;
      }
      suffix = versionedList.getStoreSegments();
      if (LOG.isDebugEnabled()) {
        int count = 0;
        if(segment != null) {
          count = segment.getCellsCount();
        }
        LOG.debug("Swapping pipeline suffix. "
            + "Just before the swap the number of segments in pipeline is:"
            + versionedList.getStoreSegments().size()
            + ", and the number of cells in new segment is:" + count);
      }
      swapSuffix(suffix, segment, closeSuffix);
    }
    if (closeSuffix && region != null) {
      // update the global memstore size counter
      long suffixDataSize = getSegmentsKeySize(suffix);
      long newDataSize = 0;
      if(segment != null) newDataSize = segment.keySize();
      long dataSizeDelta = suffixDataSize - newDataSize;
      long suffixHeapOverhead = getSegmentsHeapOverhead(suffix);
      long newHeapOverhead = 0;
      if(segment != null) newHeapOverhead = segment.heapOverhead();
      long heapOverheadDelta = suffixHeapOverhead - newHeapOverhead;
      region.addMemstoreSize(new MemstoreSize(-dataSizeDelta, -heapOverheadDelta));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Suffix data size: " + suffixDataSize + " new segment data size: "
            + newDataSize + ". Suffix heap overhead: " + suffixHeapOverhead
            + " new segment heap overhead: " + newHeapOverhead);
      }
    }
    return true;
  }

  private static long getSegmentsHeapOverhead(List<? extends Segment> list) {
    long res = 0;
    for (Segment segment : list) {
      res += segment.heapOverhead();
    }
    return res;
  }

  private static long getSegmentsKeySize(List<? extends Segment> list) {
    long res = 0;
    for (Segment segment : list) {
      res += segment.keySize();
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
  public boolean flattenYoungestSegment(long requesterVersion) {

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

      for (ImmutableSegment s : pipeline) {
        // remember the old size in case this segment is going to be flatten
        MemstoreSize memstoreSize = new MemstoreSize();
        if (s.flatten(memstoreSize)) {
          if(region != null) {
            region.addMemstoreSize(memstoreSize);
          }
          LOG.debug("Compaction pipeline segment " + s + " was flattened");
          return true;
        }
      }

    }
    // do not update the global memstore size counter and do not increase the version,
    // because all the cells remain in place
    return false;
  }

  public boolean isEmpty() {
    return pipeline.isEmpty();
  }

  public List<Segment> getSegments() {
    synchronized (pipeline){
      return new LinkedList<>(pipeline);
    }
  }

  public long size() {
    return pipeline.size();
  }

  public long getMinSequenceId() {
    long minSequenceId = Long.MAX_VALUE;
    if (!isEmpty()) {
      minSequenceId = pipeline.getLast().getMinSequenceId();
    }
    return minSequenceId;
  }

  public MemstoreSize getTailSize() {
    if (isEmpty()) return MemstoreSize.EMPTY_SIZE;
    return new MemstoreSize(pipeline.peekLast().keySize(), pipeline.peekLast().heapOverhead());
  }

  private void swapSuffix(List<ImmutableSegment> suffix, ImmutableSegment segment,
      boolean closeSegmentsInSuffix) {
    version++;
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
    pipeline.removeAll(suffix);
    if(segment != null) pipeline.addLast(segment);
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
