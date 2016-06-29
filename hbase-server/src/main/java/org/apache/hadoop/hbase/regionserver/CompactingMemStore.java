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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;

/**
 * A memstore implementation which supports in-memory compaction.
 * A compaction pipeline is added between the active set and the snapshot data structures;
 * it consists of a list of kv-sets that are subject to compaction.
 * Like the snapshot, all pipeline components are read-only; updates only affect the active set.
 * To ensure this property we take advantage of the existing blocking mechanism -- the active set
 * is pushed to the pipeline while holding the region's updatesLock in exclusive mode.
 * Periodically, a compaction is applied in the background to all pipeline components resulting
 * in a single read-only component. The ``old'' components are discarded when no scanner is reading
 * them.
 */
@InterfaceAudience.Private
public class CompactingMemStore extends AbstractMemStore {
  public final static long DEEP_OVERHEAD_PER_PIPELINE_ITEM = ClassSize.align(
      ClassSize.TIMERANGE_TRACKER + ClassSize.TIMERANGE +
          ClassSize.CELL_SKIPLIST_SET + ClassSize.CONCURRENT_SKIPLISTMAP);
  // Default fraction of in-memory-flush size w.r.t. flush-to-disk size
  public static final String IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY =
      "hbase.memestore.inmemoryflush.threshold.factor";
  private static final double IN_MEMORY_FLUSH_THRESHOLD_FACTOR_DEFAULT = 0.25;

  private static final Log LOG = LogFactory.getLog(CompactingMemStore.class);
  private Store store;
  private RegionServicesForStores regionServices;
  private CompactionPipeline pipeline;
  private MemStoreCompactor compactor;
  // the threshold on active size for in-memory flush
  private long inmemoryFlushSize;
  private final AtomicBoolean inMemoryFlushInProgress = new AtomicBoolean(false);
  @VisibleForTesting
  private final AtomicBoolean allowCompaction = new AtomicBoolean(true);

  public CompactingMemStore(Configuration conf, CellComparator c,
      HStore store, RegionServicesForStores regionServices) throws IOException {
    super(conf, c);
    this.store = store;
    this.regionServices = regionServices;
    this.pipeline = new CompactionPipeline(getRegionServices());
    this.compactor = new MemStoreCompactor(this);
    initInmemoryFlushSize(conf);
  }

  private void initInmemoryFlushSize(Configuration conf) {
    long memstoreFlushSize = getRegionServices().getMemstoreFlushSize();
    int numStores = getRegionServices().getNumStores();
    if (numStores <= 1) {
      // Family number might also be zero in some of our unit test case
      numStores = 1;
    }
    inmemoryFlushSize = memstoreFlushSize / numStores;
    // multiply by a factor
    double factor =  conf.getDouble(IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY,
        IN_MEMORY_FLUSH_THRESHOLD_FACTOR_DEFAULT);
    inmemoryFlushSize *= factor;
    LOG.debug("Setting in-memory flush size threshold to " + inmemoryFlushSize);
  }

  public static long getSegmentSize(Segment segment) {
    return segment.getSize() - DEEP_OVERHEAD_PER_PIPELINE_ITEM;
  }

  public static long getSegmentsSize(List<? extends Segment> list) {
    long res = 0;
    for (Segment segment : list) {
      res += getSegmentSize(segment);
    }
    return res;
  }

  /**
   * @return Total memory occupied by this MemStore.
   * This is not thread safe and the memstore may be changed while computing its size.
   * It is the responsibility of the caller to make sure this doesn't happen.
   */
  @Override
  public long size() {
    long res = 0;
    for (Segment item : getSegments()) {
      res += item.getSize();
    }
    return res;
  }

  /**
   * This method is called when it is clear that the flush to disk is completed.
   * The store may do any post-flush actions at this point.
   * One example is to update the WAL with sequence number that is known only at the store level.
   */
  @Override public void finalizeFlush() {
    updateLowestUnflushedSequenceIdInWAL(false);
  }

  @Override public boolean isSloppy() {
    return true;
  }

  /**
   * Push the current active memstore segment into the pipeline
   * and create a snapshot of the tail of current compaction pipeline
   * Snapshot must be cleared by call to {@link #clearSnapshot}.
   * {@link #clearSnapshot(long)}.
   * @return {@link MemStoreSnapshot}
   */
  @Override
  public MemStoreSnapshot snapshot() {
    MutableSegment active = getActive();
    // If snapshot currently has entries, then flusher failed or didn't call
    // cleanup.  Log a warning.
    if (!getSnapshot().isEmpty()) {
      LOG.warn("Snapshot called again without clearing previous. " +
          "Doing nothing. Another ongoing flush or did we fail last attempt?");
    } else {
      LOG.info("FLUSHING TO DISK: region "+ getRegionServices().getRegionInfo()
          .getRegionNameAsString() + "store: "+ getFamilyName());
      stopCompaction();
      pushActiveToPipeline(active);
      snapshotId = EnvironmentEdgeManager.currentTime();
      pushTailToSnapshot();
    }
    return new MemStoreSnapshot(snapshotId, getSnapshot());
  }

  /**
   * On flush, how much memory we will clear.
   * @return size of data that is going to be flushed
   */
  @Override public long getFlushableSize() {
    long snapshotSize = getSnapshot().getSize();
    if(snapshotSize == 0) {
      //if snapshot is empty the tail of the pipeline is flushed
      snapshotSize = pipeline.getTailSize();
    }
    return snapshotSize > 0 ? snapshotSize : keySize();
  }

  @Override
  public void updateLowestUnflushedSequenceIdInWAL(boolean onlyIfGreater) {
    long minSequenceId = pipeline.getMinSequenceId();
    if(minSequenceId != Long.MAX_VALUE) {
      byte[] encodedRegionName = getRegionServices().getRegionInfo().getEncodedNameAsBytes();
      byte[] familyName = getFamilyNameInByte();
      WAL WAL = getRegionServices().getWAL();
      if (WAL != null) {
        WAL.updateStore(encodedRegionName, familyName, minSequenceId, onlyIfGreater);
      }
    }
  }

  @Override
  public List<Segment> getSegments() {
    List<Segment> pipelineList = pipeline.getSegments();
    List<Segment> list = new LinkedList<Segment>();
    list.add(getActive());
    list.addAll(pipelineList);
    list.add(getSnapshot());
    return list;
  }

  public void setInMemoryFlushInProgress(boolean inMemoryFlushInProgress) {
    this.inMemoryFlushInProgress.set(inMemoryFlushInProgress);
  }

  public void swapCompactedSegments(VersionedSegmentsList versionedList, ImmutableSegment result) {
    pipeline.swap(versionedList, result);
  }

  public boolean hasCompactibleSegments() {
    return !pipeline.isEmpty();
  }

  public VersionedSegmentsList getCompactibleSegments() {
    return pipeline.getVersionedList();
  }

  public long getSmallestReadPoint() {
    return store.getSmallestReadPoint();
  }

  public Store getStore() {
    return store;
  }

  public String getFamilyName() {
    return Bytes.toString(getFamilyNameInByte());
  }

  @Override
  /*
   * Scanners are ordered from 0 (oldest) to newest in increasing order.
   */
  public List<KeyValueScanner> getScanners(long readPt) throws IOException {
    List<Segment> pipelineList = pipeline.getSegments();
    long order = pipelineList.size();
    // The list of elements in pipeline + the active element + the snapshot segment
    // TODO : This will change when the snapshot is made of more than one element
    List<KeyValueScanner> list = new ArrayList<KeyValueScanner>(pipelineList.size() + 2);
    list.add(getActive().getSegmentScanner(readPt, order + 1));
    for (Segment item : pipelineList) {
      list.add(item.getSegmentScanner(readPt, order));
      order--;
    }
    list.add(getSnapshot().getSegmentScanner(readPt, order));
    return Collections.<KeyValueScanner> singletonList(
      new MemStoreScanner((AbstractMemStore) this, list, readPt));
  }

  /**
   * Check whether anything need to be done based on the current active set size.
   * The method is invoked upon every addition to the active set.
   * For CompactingMemStore, flush the active set to the read-only memory if it's
   * size is above threshold
   */
  @Override
  protected void checkActiveSize() {
    if (shouldFlushInMemory()) {
      /* The thread is dispatched to flush-in-memory. This cannot be done
      * on the same thread, because for flush-in-memory we require updatesLock
      * in exclusive mode while this method (checkActiveSize) is invoked holding updatesLock
      * in the shared mode. */
      InMemoryFlushRunnable runnable = new InMemoryFlushRunnable();
      if (LOG.isTraceEnabled()) {
        LOG.trace(
          "Dispatching the MemStore in-memory flush for store " + store.getColumnFamilyName());
      }
      getPool().execute(runnable);
    }
  }

  // internally used method, externally visible only for tests
  // when invoked directly from tests it must be verified that the caller doesn't hold updatesLock,
  // otherwise there is a deadlock
  @VisibleForTesting
  void flushInMemory() throws IOException {
    // Phase I: Update the pipeline
    getRegionServices().blockUpdates();
    try {
      MutableSegment active = getActive();
      LOG.info("IN-MEMORY FLUSH: Pushing active segment into compaction pipeline, " +
          "and initiating compaction.");
      pushActiveToPipeline(active);
    } finally {
      getRegionServices().unblockUpdates();
    }
    // Phase II: Compact the pipeline
    try {
      if (allowCompaction.get() && inMemoryFlushInProgress.compareAndSet(false, true)) {
        // setting the inMemoryFlushInProgress flag again for the case this method is invoked
        // directly (only in tests) in the common path setting from true to true is idempotent
        // Speculative compaction execution, may be interrupted if flush is forced while
        // compaction is in progress
        compactor.startCompaction();
      }
    } catch (IOException e) {
      LOG.warn("Unable to run memstore compaction. region "
          + getRegionServices().getRegionInfo().getRegionNameAsString()
          + "store: "+ getFamilyName(), e);
    }
  }

  private byte[] getFamilyNameInByte() {
    return store.getFamily().getName();
  }

  private ThreadPoolExecutor getPool() {
    return getRegionServices().getInMemoryCompactionPool();
  }

  private boolean shouldFlushInMemory() {
    if(getActive().getSize() > inmemoryFlushSize) {
      // size above flush threshold
      return (allowCompaction.get() && !inMemoryFlushInProgress.get());
    }
    return false;
  }

  /**
   * The request to cancel the compaction asynchronous task (caused by in-memory flush)
   * The compaction may still happen if the request was sent too late
   * Non-blocking request
   */
  private void stopCompaction() {
    if (inMemoryFlushInProgress.get()) {
      compactor.stopCompact();
      inMemoryFlushInProgress.set(false);
    }
  }

  private void pushActiveToPipeline(MutableSegment active) {
    if (!active.isEmpty()) {
      long delta = DEEP_OVERHEAD_PER_PIPELINE_ITEM - DEEP_OVERHEAD;
      active.setSize(active.getSize() + delta);
      pipeline.pushHead(active);
      resetCellSet();
    }
  }

  private void pushTailToSnapshot() {
    ImmutableSegment tail = pipeline.pullTail();
    if (!tail.isEmpty()) {
      setSnapshot(tail);
      long size = getSegmentSize(tail);
      setSnapshotSize(size);
    }
  }

  private RegionServicesForStores getRegionServices() {
    return regionServices;
  }

  /**
  * The in-memory-flusher thread performs the flush asynchronously.
  * There is at most one thread per memstore instance.
  * It takes the updatesLock exclusively, pushes active into the pipeline, releases updatesLock
  * and compacts the pipeline.
  */
  private class InMemoryFlushRunnable implements Runnable {

    @Override public void run() {
      try {
        flushInMemory();
      } catch (IOException e) {
        LOG.warn("Unable to run memstore compaction. region "
            + getRegionServices().getRegionInfo().getRegionNameAsString()
            + "store: "+ getFamilyName(), e);
      }
    }
  }

  //----------------------------------------------------------------------
  //methods for tests
  //----------------------------------------------------------------------
  boolean isMemStoreFlushingInMemory() {
    return inMemoryFlushInProgress.get();
  }

  void disableCompaction() {
    allowCompaction.set(false);
  }

  void enableCompaction() {
    allowCompaction.set(true);
  }

  /**
   * @param cell Find the row that comes after this one.  If null, we return the
   *             first.
   * @return Next row or null if none found.
   */
  Cell getNextRow(final Cell cell) {
    Cell lowest = null;
    List<Segment> segments = getSegments();
    for (Segment segment : segments) {
      if (lowest == null) {
        lowest = getNextRow(cell, segment.getCellSet());
      } else {
        lowest = getLowest(lowest, getNextRow(cell, segment.getCellSet()));
      }
    }
    return lowest;
  }

  // debug method
  private void debug() {
    String msg = "active size="+getActive().getSize();
    msg += " threshold="+IN_MEMORY_FLUSH_THRESHOLD_FACTOR_DEFAULT* inmemoryFlushSize;
    msg += " allow compaction is "+ (allowCompaction.get() ? "true" : "false");
    msg += " inMemoryFlushInProgress is "+ (inMemoryFlushInProgress.get() ? "true" : "false");
    LOG.debug(msg);
  }
}
