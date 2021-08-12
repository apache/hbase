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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      .align(ClassSize.OBJECT + 4 * ClassSize.REFERENCE
          // compactingMemStore, versionedList, isInterrupted, strategy (the reference)
          // "action" is an enum and thus it is a class with static final constants,
          // so counting only the size of the reference to it and not the size of the internals
          + Bytes.SIZEOF_INT        // compactionKVMax
          + ClassSize.ATOMIC_BOOLEAN    // isInterrupted (the internals)
      );

  private static final Logger LOG = LoggerFactory.getLogger(MemStoreCompactor.class);
  private CompactingMemStore compactingMemStore;

  // a static version of the segment list from the pipeline
  private VersionedSegmentsList versionedList;

  // a flag raised when compaction is requested to stop
  private final AtomicBoolean isInterrupted = new AtomicBoolean(false);

  // the limit to the size of the groups to be later provided to MemStoreSegmentsIterator
  private final int compactionKVMax;

  private MemStoreCompactionStrategy strategy;

  public MemStoreCompactor(CompactingMemStore compactingMemStore,
      MemoryCompactionPolicy compactionPolicy) throws IllegalArgumentIOException {
    this.compactingMemStore = compactingMemStore;
    this.compactionKVMax = compactingMemStore.getConfiguration()
        .getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);
    initiateCompactionStrategy(compactionPolicy, compactingMemStore.getConfiguration(),
        compactingMemStore.getFamilyName());
  }

  @Override
  public String toString() {
    return this.strategy + ", compactionCellMax=" + this.compactionKVMax;
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
    LOG.trace("Speculative compaction starting on {}/{}",
        compactingMemStore.getStore().getHRegion().getRegionInfo().getEncodedName(),
        compactingMemStore.getStore().getColumnFamilyName());
    HStore store = compactingMemStore.getStore();
    RegionCoprocessorHost cpHost = store.getCoprocessorHost();
    if (cpHost != null) {
      cpHost.preMemStoreCompaction(store);
    }
    try {
      doCompaction();
    } finally {
      if (cpHost != null) {
        cpHost.postMemStoreCompaction(store);
      }
    }
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


  public void resetStats() {
    strategy.resetStats();
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
  * The worker thread performs the compaction asynchronously.
  * The solo (per compactor) thread only reads the compaction pipeline.
  * There is at most one thread per memstore instance.
  */
  private void doCompaction() {
    ImmutableSegment result = null;
    boolean resultSwapped = false;
    MemStoreCompactionStrategy.Action nextStep = strategy.getAction(versionedList);
    boolean merge = (nextStep == MemStoreCompactionStrategy.Action.MERGE ||
        nextStep == MemStoreCompactionStrategy.Action.MERGE_COUNT_UNIQUE_KEYS);
    try {
      if (isInterrupted.get()) {      // if the entire process is interrupted cancel flattening
        return;           // the compaction also doesn't start when interrupted
      }

      if (nextStep == MemStoreCompactionStrategy.Action.NOOP) {
        return;
      }
      if (nextStep == MemStoreCompactionStrategy.Action.FLATTEN
          || nextStep == MemStoreCompactionStrategy.Action.FLATTEN_COUNT_UNIQUE_KEYS) {
        // some Segment in the pipeline is with SkipList index, make it flat
        compactingMemStore.flattenOneSegment(versionedList.getVersion(), nextStep);
        return;
      }

      // Create one segment representing all segments in the compaction pipeline,
      // either by compaction or by merge
      if (!isInterrupted.get()) {
        result = createSubstitution(nextStep);
      }

      // Substitute the pipeline with one segment
      if (!isInterrupted.get()) {
        resultSwapped = compactingMemStore.swapCompactedSegments(versionedList, result, merge);
        if (resultSwapped) {
          // update compaction strategy
          strategy.updateStats(result);
          // update the wal so it can be truncated and not get too long
          compactingMemStore.updateLowestUnflushedSequenceIdInWAL(true); // only if greater
        }
      }
    } catch (IOException e) {
      LOG.trace("Interrupting in-memory compaction for store={}",
          compactingMemStore.getFamilyName());
      Thread.currentThread().interrupt();
    } finally {
      // For the MERGE case, if the result was created, but swap didn't happen,
      // we DON'T need to close the result segment (meaning its MSLAB)!
      // Because closing the result segment means closing the chunks of all segments
      // in the compaction pipeline, which still have ongoing scans.
      if (!merge && (result != null) && !resultSwapped) {
        result.close();
      }
      releaseResources();
      compactingMemStore.setInMemoryCompactionCompleted();
    }

  }

  /**----------------------------------------------------------------------
   * Creation of the ImmutableSegment either by merge or copy-compact of the segments of the
   * pipeline, based on the Compactor Iterator. The new ImmutableSegment is returned.
   */
  private ImmutableSegment createSubstitution(MemStoreCompactionStrategy.Action action) throws
      IOException {

    ImmutableSegment result = null;
    MemStoreSegmentsIterator iterator = null;
    List<ImmutableSegment> segments = versionedList.getStoreSegments();
    for (ImmutableSegment s : segments) {
      s.waitForUpdates(); // to ensure all updates preceding s in-memory flush have completed.
      // we skip empty segment when create MemStoreSegmentsIterator following.
    }

    switch (action) {
      case COMPACT:
        iterator = new MemStoreCompactorSegmentsIterator(segments,
            compactingMemStore.getComparator(),
            compactionKVMax, compactingMemStore.getStore());

        result = SegmentFactory.instance().createImmutableSegmentByCompaction(
          compactingMemStore.getConfiguration(), compactingMemStore.getComparator(), iterator,
          versionedList.getNumOfCells(), compactingMemStore.getIndexType(), action);
        iterator.close();
        break;
      case MERGE:
      case MERGE_COUNT_UNIQUE_KEYS:
        iterator =
            new MemStoreMergerSegmentsIterator(segments,
                compactingMemStore.getComparator(), compactionKVMax);

        result = SegmentFactory.instance().createImmutableSegmentByMerge(
          compactingMemStore.getConfiguration(), compactingMemStore.getComparator(), iterator,
          versionedList.getNumOfCells(), segments, compactingMemStore.getIndexType(), action);
        iterator.close();
        break;
      default:
        throw new RuntimeException("Unknown action " + action); // sanity check
    }

    return result;
  }

  void initiateCompactionStrategy(MemoryCompactionPolicy compType,
      Configuration configuration, String cfName) throws IllegalArgumentIOException {

    assert (compType !=MemoryCompactionPolicy.NONE);

    switch (compType){
      case BASIC: strategy = new BasicMemStoreCompactionStrategy(configuration, cfName);
        break;
      case EAGER: strategy = new EagerMemStoreCompactionStrategy(configuration, cfName);
        break;
      case ADAPTIVE: strategy = new AdaptiveMemStoreCompactionStrategy(configuration, cfName);
        break;
      default:
        // sanity check
        throw new IllegalArgumentIOException("Unknown memory compaction type " + compType);
    }
  }
}
