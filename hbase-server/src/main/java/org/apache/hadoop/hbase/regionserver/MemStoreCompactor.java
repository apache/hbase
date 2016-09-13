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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
      .align(ClassSize.OBJECT + 4 * ClassSize.REFERENCE + 2 * Bytes.SIZEOF_INT + Bytes.SIZEOF_DOUBLE
          + ClassSize.ATOMIC_BOOLEAN);

  // Option for external guidance whether flattening is allowed
  static final String MEMSTORE_COMPACTOR_FLATTENING = "hbase.hregion.compacting.memstore.flatten";
  static final boolean MEMSTORE_COMPACTOR_FLATTENING_DEFAULT = true;

  // Option for external setting of the compacted structure (SkipList, CellArray, etc.)
  static final String COMPACTING_MEMSTORE_TYPE_KEY = "hbase.hregion.compacting.memstore.type";
  static final int COMPACTING_MEMSTORE_TYPE_DEFAULT = 2;  // COMPACT_TO_ARRAY_MAP as default

  // What percentage of the duplications is causing compaction?
  static final String COMPACTION_THRESHOLD_REMAIN_FRACTION
      = "hbase.hregion.compacting.memstore.comactPercent";
  static final double COMPACTION_THRESHOLD_REMAIN_FRACTION_DEFAULT = 0.2;

  // Option for external guidance whether the flattening is allowed
  static final String MEMSTORE_COMPACTOR_AVOID_SPECULATIVE_SCAN
      = "hbase.hregion.compacting.memstore.avoidSpeculativeScan";
  static final boolean MEMSTORE_COMPACTOR_AVOID_SPECULATIVE_SCAN_DEFAULT = false;

  private static final Log LOG = LogFactory.getLog(MemStoreCompactor.class);

  /**
   * Types of Compaction
   */
  private enum Type {
    COMPACT_TO_SKIPLIST_MAP,
    COMPACT_TO_ARRAY_MAP
  }

  private CompactingMemStore compactingMemStore;

  // a static version of the segment list from the pipeline
  private VersionedSegmentsList versionedList;

  // a flag raised when compaction is requested to stop
  private final AtomicBoolean isInterrupted = new AtomicBoolean(false);

  // the limit to the size of the groups to be later provided to MemStoreCompactorIterator
  private final int compactionKVMax;

  double fraction = 0.8;

  int immutCellsNum = 0;  // number of immutable for compaction cells

  private Type type = Type.COMPACT_TO_ARRAY_MAP;

  public MemStoreCompactor(CompactingMemStore compactingMemStore) {
    this.compactingMemStore = compactingMemStore;
    this.compactionKVMax = compactingMemStore.getConfiguration().getInt(
        HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);
    this.fraction = 1 - compactingMemStore.getConfiguration().getDouble(
        COMPACTION_THRESHOLD_REMAIN_FRACTION,
        COMPACTION_THRESHOLD_REMAIN_FRACTION_DEFAULT);
  }

  /**----------------------------------------------------------------------
   * The request to dispatch the compaction asynchronous task.
   * The method returns true if compaction was successfully dispatched, or false if there
   * is already an ongoing compaction or no segments to compact.
   */
  public boolean start() throws IOException {
    if (!compactingMemStore.hasImmutableSegments()) return false;  // no compaction on empty

    int t = compactingMemStore.getConfiguration().getInt(COMPACTING_MEMSTORE_TYPE_KEY,
        COMPACTING_MEMSTORE_TYPE_DEFAULT);

    switch (t) {
      case 1: type = Type.COMPACT_TO_SKIPLIST_MAP;
        break;
      case 2: type = Type.COMPACT_TO_ARRAY_MAP;
        break;
      default: throw new RuntimeException("Unknown type " + type); // sanity check
    }

    // get a snapshot of the list of the segments from the pipeline,
    // this local copy of the list is marked with specific version
    versionedList = compactingMemStore.getImmutableSegments();
    immutCellsNum = versionedList.getNumOfCells();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting the MemStore In-Memory Shrink of type " + type + " for store "
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
  * Close the scanners and clear the pointers in order to allow good
  * garbage collection
  */
  private void releaseResources() {
    isInterrupted.set(false);
    versionedList = null;
  }

  /**----------------------------------------------------------------------
   * Check whether there are some signs to definitely not to flatten,
   * returns false if we must compact. If this method returns true we
   * still need to evaluate the compaction.
   */
  private boolean shouldFlatten() {
    boolean userToFlatten =         // the user configurable option to flatten or not to flatten
        compactingMemStore.getConfiguration().getBoolean(MEMSTORE_COMPACTOR_FLATTENING,
            MEMSTORE_COMPACTOR_FLATTENING_DEFAULT);
    if (userToFlatten==false) {
      LOG.debug("In-Memory shrink is doing compaction, as user asked to avoid flattening");
      return false;                 // the user doesn't want to flatten
    }

    // limit the number of the segments in the pipeline
    int numOfSegments = versionedList.getNumOfSegments();
    if (numOfSegments > 3) {        // hard-coded for now as it is going to move to policy
      LOG.debug("In-Memory shrink is doing compaction, as there already are " + numOfSegments
          + " segments in the compaction pipeline");
      return false;                 // to avoid "too many open files later", compact now
    }
    // till here we hvae all the signs that it is possible to flatten, run the speculative scan
    // (if allowed by the user) to check the efficiency of compaction
    boolean avoidSpeculativeScan =   // the user configurable option to avoid the speculative scan
        compactingMemStore.getConfiguration().getBoolean(MEMSTORE_COMPACTOR_AVOID_SPECULATIVE_SCAN,
            MEMSTORE_COMPACTOR_AVOID_SPECULATIVE_SCAN_DEFAULT);
    if (avoidSpeculativeScan==true) {
      LOG.debug("In-Memory shrink is doing flattening, as user asked to avoid compaction "
          + "evaluation");
      return true;                  // flatten without checking the compaction expedience
    }
    try {
      immutCellsNum = countCellsForCompaction();
      if (immutCellsNum > fraction * versionedList.getNumOfCells()) {
        return true;
      }
    } catch(Exception e) {
      return true;
    }
    return false;
  }

  /**----------------------------------------------------------------------
  * The worker thread performs the compaction asynchronously.
  * The solo (per compactor) thread only reads the compaction pipeline.
  * There is at most one thread per memstore instance.
  */
  private void doCompaction() {
    ImmutableSegment result = null;
    boolean resultSwapped = false;

    try {
      // PHASE I: estimate the compaction expedience - EVALUATE COMPACTION
      if (shouldFlatten()) {
        // too much cells "survive" the possible compaction, we do not want to compact!
        LOG.debug("In-Memory compaction does not pay off - storing the flattened segment"
            + " for store: " + compactingMemStore.getFamilyName());
        // Looking for Segment in the pipeline with SkipList index, to make it flat
        compactingMemStore.flattenOneSegment(versionedList.getVersion());
        return;
      }

      // PHASE II: create the new compacted ImmutableSegment - START COPY-COMPACTION
      if (!isInterrupted.get()) {
        result = compact(immutCellsNum);
      }

      // Phase III: swap the old compaction pipeline - END COPY-COMPACTION
      if (!isInterrupted.get()) {
        if (resultSwapped = compactingMemStore.swapCompactedSegments(versionedList, result)) {
          // update the wal so it can be truncated and not get too long
          compactingMemStore.updateLowestUnflushedSequenceIdInWAL(true); // only if greater
        }
      }
    } catch (Exception e) {
      LOG.debug("Interrupting the MemStore in-memory compaction for store "
          + compactingMemStore.getFamilyName());
      Thread.currentThread().interrupt();
    } finally {
      if ((result != null) && (!resultSwapped)) result.close();
      releaseResources();
    }

  }

  /**----------------------------------------------------------------------
   * The copy-compaction is the creation of the ImmutableSegment (from the relevant type)
   * based on the Compactor Iterator. The new ImmutableSegment is returned.
   */
  private ImmutableSegment compact(int numOfCells) throws IOException {

    LOG.debug("In-Memory compaction does pay off - The estimated number of cells "
        + "after compaction is " + numOfCells + ", while number of cells before is " + versionedList
        .getNumOfCells() + ". The fraction of remaining cells should be: " + fraction);

    ImmutableSegment result = null;
    MemStoreCompactorIterator iterator =
        new MemStoreCompactorIterator(versionedList.getStoreSegments(),
            compactingMemStore.getComparator(),
            compactionKVMax, compactingMemStore.getStore());
    try {
      switch (type) {
      case COMPACT_TO_SKIPLIST_MAP:
        result = SegmentFactory.instance().createImmutableSegment(
            compactingMemStore.getConfiguration(), compactingMemStore.getComparator(), iterator);
        break;
      case COMPACT_TO_ARRAY_MAP:
        result = SegmentFactory.instance().createImmutableSegment(
            compactingMemStore.getConfiguration(), compactingMemStore.getComparator(), iterator,
            numOfCells, ImmutableSegment.Type.ARRAY_MAP_BASED);
        break;
      default: throw new RuntimeException("Unknown type " + type); // sanity check
      }
    } finally {
      iterator.close();
    }

    return result;
  }

  /**----------------------------------------------------------------------
   * Count cells to estimate the efficiency of the future compaction
   */
  private int countCellsForCompaction() throws IOException {

    int cnt = 0;
    MemStoreCompactorIterator iterator =
        new MemStoreCompactorIterator(
            versionedList.getStoreSegments(), compactingMemStore.getComparator(),
            compactionKVMax, compactingMemStore.getStore());

    try {
      while (iterator.next() != null) {
        cnt++;
      }
    } finally {
      iterator.close();
    }

    return cnt;
  }
}
