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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

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

  private final RegionServicesForStores region;
  private LinkedList<ImmutableSegment> pipeline;
  private long version;

  private static final ImmutableSegment EMPTY_MEM_STORE_SEGMENT = SegmentFactory.instance()
      .createImmutableSegment(null,
          CompactingMemStore.DEEP_OVERHEAD_PER_PIPELINE_ITEM);

  public CompactionPipeline(RegionServicesForStores region) {
    this.region = region;
    this.pipeline = new LinkedList<ImmutableSegment>();
    this.version = 0;
  }

  public boolean pushHead(MutableSegment segment) {
    ImmutableSegment immutableSegment = SegmentFactory.instance().
        createImmutableSegment(segment);
    synchronized (pipeline){
      return addFirst(immutableSegment);
    }
  }

  public ImmutableSegment pullTail() {
    synchronized (pipeline){
      if(pipeline.isEmpty()) {
        return EMPTY_MEM_STORE_SEGMENT;
      }
      return removeLast();
    }
  }

  public VersionedSegmentsList getVersionedList() {
    synchronized (pipeline){
      LinkedList<ImmutableSegment> segmentList = new LinkedList<ImmutableSegment>(pipeline);
      VersionedSegmentsList res = new VersionedSegmentsList(segmentList, version);
      return res;
    }
  }

  /**
   * Swaps the versioned list at the tail of the pipeline with the new compacted segment.
   * Swapping only if there were no changes to the suffix of the list while it was compacted.
   * @param versionedList tail of the pipeline that was compacted
   * @param segment new compacted segment
   * @return true iff swapped tail with new compacted segment
   */
  public boolean swap(VersionedSegmentsList versionedList, ImmutableSegment segment) {
    if (versionedList.getVersion() != version) {
      return false;
    }
    LinkedList<ImmutableSegment> suffix;
    synchronized (pipeline){
      if(versionedList.getVersion() != version) {
        return false;
      }
      suffix = versionedList.getStoreSegments();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Swapping pipeline suffix with compacted item. "
            + "Just before the swap the number of segments in pipeline is:"
            + versionedList.getStoreSegments().size()
            + ", and the number of cells in new segment is:" + segment.getCellsCount());
      }
      swapSuffix(suffix,segment);
    }
    if (region != null) {
      // update the global memstore size counter
      long suffixSize = CompactingMemStore.getSegmentsSize(suffix);
      long newSize = CompactingMemStore.getSegmentSize(segment);
      long delta = suffixSize - newSize;
      long globalMemstoreSize = region.addAndGetGlobalMemstoreSize(-delta);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Suffix size: " + suffixSize + " compacted item size: " + newSize
            + " globalMemstoreSize: " + globalMemstoreSize);
      }
    }
    return true;
  }

  public boolean isEmpty() {
    return pipeline.isEmpty();
  }

  public List<Segment> getSegments() {
    synchronized (pipeline){
      List<Segment> res = new LinkedList<Segment>(pipeline);
      return res;
    }
  }

  public long size() {
    return pipeline.size();
  }

  public long getMinSequenceId() {
    long minSequenceId = Long.MAX_VALUE;
    if(!isEmpty()) {
      minSequenceId = pipeline.getLast().getMinSequenceId();
    }
    return minSequenceId;
  }

  public long getTailSize() {
    if(isEmpty()) return 0;
    return CompactingMemStore.getSegmentSize(pipeline.peekLast());
  }

  private void swapSuffix(LinkedList<ImmutableSegment> suffix, ImmutableSegment segment) {
    version++;
    for(Segment itemInSuffix : suffix) {
      itemInSuffix.close();
    }
    pipeline.removeAll(suffix);
    pipeline.addLast(segment);
  }

  private ImmutableSegment removeLast() {
    version++;
    return pipeline.removeLast();
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
