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
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HColumnDescriptor;
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

  // The external setting of the compacting MemStore behaviour
  public static final String COMPACTING_MEMSTORE_TYPE_KEY =
      "hbase.hregion.compacting.memstore.type";
  public static final String COMPACTING_MEMSTORE_TYPE_DEFAULT =
      String.valueOf(HColumnDescriptor.MemoryCompaction.NONE);
  // Default fraction of in-memory-flush size w.r.t. flush-to-disk size
  public static final String IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY =
      "hbase.memstore.inmemoryflush.threshold.factor";
  private static final double IN_MEMORY_FLUSH_THRESHOLD_FACTOR_DEFAULT = 0.25;

  private static final Log LOG = LogFactory.getLog(CompactingMemStore.class);
  private Store store;
  private RegionServicesForStores regionServices;
  private CompactionPipeline pipeline;
  private MemStoreCompactor compactor;

  private long inmemoryFlushSize;       // the threshold on active size for in-memory flush
  private final AtomicBoolean inMemoryFlushInProgress = new AtomicBoolean(false);
  @VisibleForTesting
  private final AtomicBoolean allowCompaction = new AtomicBoolean(true);

  public static final long DEEP_OVERHEAD = AbstractMemStore.DEEP_OVERHEAD
      + 6 * ClassSize.REFERENCE // Store, RegionServicesForStores, CompactionPipeline,
                                // MemStoreCompactor, inMemoryFlushInProgress, allowCompaction
      + Bytes.SIZEOF_LONG // inmemoryFlushSize
      + 2 * ClassSize.ATOMIC_BOOLEAN// inMemoryFlushInProgress and allowCompaction
      + CompactionPipeline.DEEP_OVERHEAD + MemStoreCompactor.DEEP_OVERHEAD;

  public CompactingMemStore(Configuration conf, CellComparator c,
      HStore store, RegionServicesForStores regionServices,
      HColumnDescriptor.MemoryCompaction compactionPolicy) throws IOException {
    super(conf, c);
    this.store = store;
    this.regionServices = regionServices;
    this.pipeline = new CompactionPipeline(getRegionServices());
    this.compactor = new MemStoreCompactor(this, compactionPolicy);
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
    LOG.info("Setting in-memory flush size threshold to " + inmemoryFlushSize);
  }

  /**
   * @return Total memory occupied by this MemStore. This won't include any size occupied by the
   *         snapshot. We assume the snapshot will get cleared soon. This is not thread safe and
   *         the memstore may be changed while computing its size. It is the responsibility of the
   *         caller to make sure this doesn't happen.
   */
  @Override
  public MemstoreSize size() {
    MemstoreSize memstoreSize = new MemstoreSize();
    memstoreSize.incMemstoreSize(this.active.keySize(), this.active.heapOverhead());
    for (Segment item : pipeline.getSegments()) {
      memstoreSize.incMemstoreSize(item.keySize(), item.heapOverhead());
    }
    return memstoreSize;
  }

  /**
   * This method is called when it is clear that the flush to disk is completed.
   * The store may do any post-flush actions at this point.
   * One example is to update the WAL with sequence number that is known only at the store level.
   */
  @Override
  public void finalizeFlush() {
    updateLowestUnflushedSequenceIdInWAL(false);
  }

  @Override
  public boolean isSloppy() {
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
    // If snapshot currently has entries, then flusher failed or didn't call
    // cleanup.  Log a warning.
    if (!this.snapshot.isEmpty()) {
      LOG.warn("Snapshot called again without clearing previous. " +
          "Doing nothing. Another ongoing flush or did we fail last attempt?");
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("FLUSHING TO DISK: region "
            + getRegionServices().getRegionInfo().getRegionNameAsString() + "store: "
            + getFamilyName());
      }
      stopCompaction();
      pushActiveToPipeline(this.active);
      snapshotId = EnvironmentEdgeManager.currentTime();
      pushTailToSnapshot();
    }
    return new MemStoreSnapshot(snapshotId, this.snapshot);
  }

  /**
   * On flush, how much memory we will clear.
   * @return size of data that is going to be flushed
   */
  @Override
  public MemstoreSize getFlushableSize() {
    MemstoreSize snapshotSize = getSnapshotSize();
    if (snapshotSize.getDataSize() == 0) {
      // if snapshot is empty the tail of the pipeline is flushed
      snapshotSize = pipeline.getTailSize();
    }
    return snapshotSize.getDataSize() > 0 ? snapshotSize
        : new MemstoreSize(this.active.keySize(), this.active.heapOverhead());
  }

  @Override
  protected long keySize() {
    // Need to consider keySize of all segments in pipeline and active
    long k = this.active.keySize();
    for (Segment segment : this.pipeline.getSegments()) {
      k += segment.keySize();
    }
    return k;
  }

  @Override
  protected long heapOverhead() {
    // Need to consider heapOverhead of all segments in pipeline and active
    long h = this.active.heapOverhead();
    for (Segment segment : this.pipeline.getSegments()) {
      h += segment.heapOverhead();
    }
    return h;
  }

  @Override
  public void updateLowestUnflushedSequenceIdInWAL(boolean onlyIfGreater) {
    long minSequenceId = pipeline.getMinSequenceId();
    if(minSequenceId != Long.MAX_VALUE) {
      byte[] encodedRegionName = getRegionServices().getRegionInfo().getEncodedNameAsBytes();
      byte[] familyName = getFamilyNameInBytes();
      WAL WAL = getRegionServices().getWAL();
      if (WAL != null) {
        WAL.updateStore(encodedRegionName, familyName, minSequenceId, onlyIfGreater);
      }
    }
  }

  @Override
  public List<Segment> getSegments() {
    List<Segment> pipelineList = pipeline.getSegments();
    List<Segment> list = new ArrayList<Segment>(pipelineList.size() + 2);
    list.add(this.active);
    list.addAll(pipelineList);
    list.add(this.snapshot);
    return list;
  }

  public boolean swapCompactedSegments(VersionedSegmentsList versionedList, ImmutableSegment result,
      boolean merge) {
    return pipeline.swap(versionedList, result, !merge);
  }

  /**
   * @param requesterVersion The caller must hold the VersionedList of the pipeline
   *           with version taken earlier. This version must be passed as a parameter here.
   *           The flattening happens only if versions match.
   */
  public void flattenOneSegment(long requesterVersion) {
    pipeline.flattenYoungestSegment(requesterVersion);
  }

  public boolean hasImmutableSegments() {
    return !pipeline.isEmpty();
  }

  public VersionedSegmentsList getImmutableSegments() {
    return pipeline.getVersionedList();
  }

  public long getSmallestReadPoint() {
    return store.getSmallestReadPoint();
  }

  public Store getStore() {
    return store;
  }

  public String getFamilyName() {
    return Bytes.toString(getFamilyNameInBytes());
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
    list.add(this.active.getScanner(readPt, order + 1));
    for (Segment item : pipelineList) {
      list.add(item.getScanner(readPt, order));
      order--;
    }
    list.add(this.snapshot.getScanner(readPt, order));
    return Collections.<KeyValueScanner> singletonList(new MemStoreScanner(getComparator(), list));
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
    // setting the inMemoryFlushInProgress flag again for the case this method is invoked
    // directly (only in tests) in the common path setting from true to true is idempotent
    inMemoryFlushInProgress.set(true);
    try {
      // Phase I: Update the pipeline
      getRegionServices().blockUpdates();
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("IN-MEMORY FLUSH: Pushing active segment into compaction pipeline");
        }
        pushActiveToPipeline(this.active);
      } finally {
        getRegionServices().unblockUpdates();
      }

      // Used by tests
      if (!allowCompaction.get()) {
        return;
      }
      // Phase II: Compact the pipeline
      try {
        // Speculative compaction execution, may be interrupted if flush is forced while
        // compaction is in progress
        compactor.start();
      } catch (IOException e) {
        LOG.warn("Unable to run memstore compaction. region "
            + getRegionServices().getRegionInfo().getRegionNameAsString() + "store: "
            + getFamilyName(), e);
      }
    } finally {
      inMemoryFlushInProgress.set(false);
    }
  }

  private byte[] getFamilyNameInBytes() {
    return store.getFamily().getName();
  }

  private ThreadPoolExecutor getPool() {
    return getRegionServices().getInMemoryCompactionPool();
  }

  private boolean shouldFlushInMemory() {
    if (this.active.keySize() > inmemoryFlushSize) { // size above flush threshold
        // the inMemoryFlushInProgress is CASed to be true here in order to mutual exclude
        // the insert of the active into the compaction pipeline
        return (inMemoryFlushInProgress.compareAndSet(false,true));
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
      compactor.stop();
      inMemoryFlushInProgress.set(false);
    }
  }

  private void pushActiveToPipeline(MutableSegment active) {
    if (!active.isEmpty()) {
      pipeline.pushHead(active);
      resetActive();
    }
  }

  private void pushTailToSnapshot() {
    ImmutableSegment tail = pipeline.pullTail();
    if (!tail.isEmpty()) {
      this.snapshot = tail;
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

    @Override
    public void run() {
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
  @VisibleForTesting
  boolean isMemStoreFlushingInMemory() {
    return inMemoryFlushInProgress.get();
  }

  @VisibleForTesting
  void disableCompaction() {
    allowCompaction.set(false);
  }

  @VisibleForTesting
  void enableCompaction() {
    allowCompaction.set(true);
  }

  @VisibleForTesting
  void initiateType(HColumnDescriptor.MemoryCompaction compactionType) {
    compactor.initiateAction(compactionType);
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
  public void debug() {
    String msg = "active size=" + this.active.keySize();
    msg += " threshold="+IN_MEMORY_FLUSH_THRESHOLD_FACTOR_DEFAULT* inmemoryFlushSize;
    msg += " allow compaction is "+ (allowCompaction.get() ? "true" : "false");
    msg += " inMemoryFlushInProgress is "+ (inMemoryFlushInProgress.get() ? "true" : "false");
    LOG.debug(msg);
  }

}
