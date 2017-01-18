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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor.MemoryCompaction;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The ongoing MemStore Compaction manager, dispatches a solo running compaction and interrupts
 * the compaction if requested. The compaction is interrupted and stopped by CompactingMemStore,
 * for example when another compaction needs to be started.
 * Prior to compaction the MemStoreCompactor evaluates
 * the compacting ratio and aborts the compaction if it is not worthy.
 * The MemStoreScanner is used to traverse the compaction pipeline. The MemStoreScanner
 * is included in internal store scanner, where all compaction logic is implemented.
 * Threads safety: It is assumed that the compaction pipeline is immutable,
 * therefore no special synchronization is required.
 */
@InterfaceAudience.Private
public class MemStoreCompactor {

  public static final long DEEP_OVERHEAD = ClassSize
      .align(ClassSize.OBJECT
          + 4 * ClassSize.REFERENCE
          // compactingMemStore, versionedList, action, isInterrupted (the reference)
          // "action" is an enum and thus it is a class with static final constants,
          // so counting only the size of the reference to it and not the size of the internals
          + Bytes.SIZEOF_INT            // compactionKVMax
          + ClassSize.ATOMIC_BOOLEAN    // isInterrupted (the internals)
      );

  // The upper bound for the number of segments we store in the pipeline prior to merging.
  // This constant is subject to further experimentation.
  private static final int THRESHOLD_PIPELINE_SEGMENTS = 30; // stands here for infinity

  private static final Log LOG = LogFactory.getLog(MemStoreCompactor.class);

  private CompactingMemStore compactingMemStore;

  // a static version of the segment list from the pipeline
  private VersionedSegmentsList versionedList;

  // a flag raised when compaction is requested to stop
  private final AtomicBoolean isInterrupted = new AtomicBoolean(false);

  // the limit to the size of the groups to be later provided to MemStoreSegmentsIterator
  private final int compactionKVMax;

  /**
   * Types of actions to be done on the pipeline upon MemStoreCompaction invocation.
   * Note that every value covers the previous ones, i.e. if MERGE is the action it implies
   * that the youngest segment is going to be flatten anyway.
   */
  private enum Action {
    NOOP,
    FLATTEN,  // flatten the youngest segment in the pipeline
    MERGE,    // merge all the segments in the pipeline into one
    COMPACT   // copy-compact the data of all the segments in the pipeline
  }

  private Action action = Action.FLATTEN;

  public MemStoreCompactor(CompactingMemStore compactingMemStore,
      MemoryCompaction compactionPolicy) {
    this.compactingMemStore = compactingMemStore;
    this.compactionKVMax = compactingMemStore.getConfiguration()
        .getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);
    initiateAction(compactionPolicy);
  }

  /**----------------------------------------------------------------------
   * The request to dispatch the compaction asynchronous task.
   * The method returns true if compaction was successfully dispatched, or false if there
   * is already an ongoing compaction or no segments to compact.
   */
  public boolean start() throws IOException {
    if (!compactingMemStore.hasImmutableSegments()) { // no compaction on empty pipeline
      return false;
    }

    // get a snapshot of the list of the segments from the pipeline,
    // this local copy of the list is marked with specific version
    versionedList = compactingMemStore.getImmutableSegments();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting the In-Memory Compaction for store "
          + compactingMemStore.getStore().getColumnFamilyName());
    }

    doCompaction();
    return true;
  }

  /**----------------------------------------------------------------------
  * The request to cancel the compaction asynchronous task
  * The compaction may still happen if the request was sent too late
  * Non-blocking request
  */
  public void stop() {
      isInterrupted.compareAndSet(false, true);
  }

  /**----------------------------------------------------------------------
   * The interface to check whether user requested the index-compaction
   */
  public boolean isIndexCompaction() {
    return (action == Action.MERGE);
  }

  /**----------------------------------------------------------------------
  * Reset the interruption indicator and clear the pointers in order to allow good
  * garbage collection
  */
  private void releaseResources() {
    isInterrupted.set(false);
    versionedList = null;
  }

  /**----------------------------------------------------------------------
   * Decide what to do with the new and old segments in the compaction pipeline.
   * Implements basic in-memory compaction policy.
   */
  private Action policy() {

    if (isInterrupted.get()) {      // if the entire process is interrupted cancel flattening
      return Action.NOOP;           // the compaction also doesn't start when interrupted
    }

    if (action == Action.COMPACT) { // compact according to the user request
      LOG.debug("In-Memory Compaction Pipeline for store " + compactingMemStore.getFamilyName()
          + " is going to be compacted, number of"
          + " cells before compaction is " + versionedList.getNumOfCells());
      return Action.COMPACT;
    }

    // compaction shouldn't happen or doesn't worth it
    // limit the number of the segments in the pipeline
    int numOfSegments = versionedList.getNumOfSegments();
    if (numOfSegments > THRESHOLD_PIPELINE_SEGMENTS) {
      LOG.debug("In-Memory Compaction Pipeline for store " + compactingMemStore.getFamilyName()
          + " is going to be merged, as there are " + numOfSegments + " segments");
      return Action.MERGE;          // to avoid too many segments, merge now
    }

    // if nothing of the above, then just flatten the newly joined segment
    LOG.debug("The youngest segment in the in-Memory Compaction Pipeline for store "
        + compactingMemStore.getFamilyName() + " is going to be flattened");
    return Action.FLATTEN;
  }

  /**----------------------------------------------------------------------
  * The worker thread performs the compaction asynchronously.
  * The solo (per compactor) thread only reads the compaction pipeline.
  * There is at most one thread per memstore instance.
  */
  private void doCompaction() {
    ImmutableSegment result = null;
    boolean resultSwapped = false;
    Action nextStep = null;
    try {
      nextStep = policy();

      if (nextStep == Action.NOOP) {
        return;
      }
      if (nextStep == Action.FLATTEN) {
        // Youngest Segment in the pipeline is with SkipList index, make it flat
        compactingMemStore.flattenOneSegment(versionedList.getVersion());
        return;
      }

      // Create one segment representing all segments in the compaction pipeline,
      // either by compaction or by merge
      if (!isInterrupted.get()) {
        result = createSubstitution();
      }

      // Substitute the pipeline with one segment
      if (!isInterrupted.get()) {
        if (resultSwapped = compactingMemStore.swapCompactedSegments(
            versionedList, result, (action==Action.MERGE))) {
          // update the wal so it can be truncated and not get too long
          compactingMemStore.updateLowestUnflushedSequenceIdInWAL(true); // only if greater
        }
      }
    } catch (IOException e) {
      LOG.debug("Interrupting the MemStore in-memory compaction for store "
          + compactingMemStore.getFamilyName());
      Thread.currentThread().interrupt();
    } finally {
      // For the MERGE case, if the result was created, but swap didn't happen,
      // we DON'T need to close the result segment (meaning its MSLAB)!
      // Because closing the result segment means closing the chunks of all segments
      // in the compaction pipeline, which still have ongoing scans.
      if (nextStep != Action.MERGE) {
        if ((result != null) && (!resultSwapped)) {
          result.close();
        }
      }
      releaseResources();
    }

  }

  /**----------------------------------------------------------------------
   * Creation of the ImmutableSegment either by merge or copy-compact of the segments of the
   * pipeline, based on the Compactor Iterator. The new ImmutableSegment is returned.
   */
  private ImmutableSegment createSubstitution() throws IOException {

    ImmutableSegment result = null;
    MemStoreSegmentsIterator iterator = null;

    switch (action) {
    case COMPACT:
      iterator =
          new MemStoreCompactorSegmentsIterator(versionedList.getStoreSegments(),
              compactingMemStore.getComparator(),
              compactionKVMax, compactingMemStore.getStore());

      result = SegmentFactory.instance().createImmutableSegmentByCompaction(
          compactingMemStore.getConfiguration(), compactingMemStore.getComparator(), iterator,
          versionedList.getNumOfCells(), ImmutableSegment.Type.ARRAY_MAP_BASED);
      iterator.close();
      break;
    case MERGE:
      iterator =
          new MemStoreMergerSegmentsIterator(versionedList.getStoreSegments(),
              compactingMemStore.getComparator(),
              compactionKVMax, compactingMemStore.getStore());

      result = SegmentFactory.instance().createImmutableSegmentByMerge(
          compactingMemStore.getConfiguration(), compactingMemStore.getComparator(), iterator,
          versionedList.getNumOfCells(), ImmutableSegment.Type.ARRAY_MAP_BASED,
          versionedList.getStoreSegments());
      iterator.close();
      break;
    default: throw new RuntimeException("Unknown action " + action); // sanity check
    }

    return result;
  }

  /**----------------------------------------------------------------------
   * Initiate the action according to user config, after its default is Action.MERGE
   */
  @VisibleForTesting
  void initiateAction(MemoryCompaction compType) {

    switch (compType){
    case NONE: action = Action.NOOP;
      break;
    case BASIC: action = Action.MERGE;
      break;
    case EAGER: action = Action.COMPACT;
      break;
    default:
      throw new RuntimeException("Unknown memstore type " + compType); // sanity check
    }
  }
}
