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

import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
      String.valueOf(MemoryCompactionPolicy.NONE);
  // Default fraction of in-memory-flush size w.r.t. flush-to-disk size
  public static final String IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY =
      "hbase.memstore.inmemoryflush.threshold.factor";
  private static final double IN_MEMORY_FLUSH_THRESHOLD_FACTOR_DEFAULT = 0.1;

  private static final Logger LOG = LoggerFactory.getLogger(CompactingMemStore.class);
  private HStore store;
  private RegionServicesForStores regionServices;
  private CompactionPipeline pipeline;
  protected MemStoreCompactor compactor;

  private long inmemoryFlushSize;       // the threshold on active size for in-memory flush
  private final AtomicBoolean inMemoryFlushInProgress = new AtomicBoolean(false);

  // inWalReplay is true while we are synchronously replaying the edits from WAL
  private boolean inWalReplay = false;

  @VisibleForTesting
  protected final AtomicBoolean allowCompaction = new AtomicBoolean(true);
  private boolean compositeSnapshot = true;

  /**
   * Types of indexes (part of immutable segments) to be used after flattening,
   * compaction, or merge are applied.
   */
  public enum IndexType {
    CSLM_MAP,   // ConcurrentSkipLisMap
    ARRAY_MAP,  // CellArrayMap
    CHUNK_MAP   // CellChunkMap
  }

  private IndexType indexType = IndexType.ARRAY_MAP;  // default implementation

  public static final long DEEP_OVERHEAD = ClassSize.align( AbstractMemStore.DEEP_OVERHEAD
      + 7 * ClassSize.REFERENCE     // Store, RegionServicesForStores, CompactionPipeline,
                                    // MemStoreCompactor, inMemoryFlushInProgress, allowCompaction,
                                    // indexType
      + Bytes.SIZEOF_LONG           // inmemoryFlushSize
      + 2 * Bytes.SIZEOF_BOOLEAN    // compositeSnapshot and inWalReplay
      + 2 * ClassSize.ATOMIC_BOOLEAN// inMemoryFlushInProgress and allowCompaction
      + CompactionPipeline.DEEP_OVERHEAD + MemStoreCompactor.DEEP_OVERHEAD);

  public CompactingMemStore(Configuration conf, CellComparator c,
      HStore store, RegionServicesForStores regionServices,
      MemoryCompactionPolicy compactionPolicy) throws IOException {
    super(conf, c);
    this.store = store;
    this.regionServices = regionServices;
    this.pipeline = new CompactionPipeline(getRegionServices());
    this.compactor = createMemStoreCompactor(compactionPolicy);
    if (conf.getBoolean(MemStoreLAB.USEMSLAB_KEY, MemStoreLAB.USEMSLAB_DEFAULT)) {
      // if user requested to work with MSLABs (whether on- or off-heap), then the
      // immutable segments are going to use CellChunkMap as their index
      indexType = IndexType.CHUNK_MAP;
    } else {
      indexType = IndexType.ARRAY_MAP;
    }
    // initialization of the flush size should happen after initialization of the index type
    // so do not transfer the following method
    initInmemoryFlushSize(conf);
    LOG.info("Store={}, in-memory flush size threshold={}, immutable segments index type={}, " +
            "compactor={}", this.store.getColumnFamilyName(),
        StringUtils.byteDesc(this.inmemoryFlushSize), this.indexType,
        (this.compactor == null? "NULL": this.compactor.toString()));
  }

  @VisibleForTesting
  protected MemStoreCompactor createMemStoreCompactor(MemoryCompactionPolicy compactionPolicy)
      throws IllegalArgumentIOException {
    return new MemStoreCompactor(this, compactionPolicy);
  }

  private void initInmemoryFlushSize(Configuration conf) {
    double factor = 0;
    long memstoreFlushSize = getRegionServices().getMemStoreFlushSize();
    int numStores = getRegionServices().getNumStores();
    if (numStores <= 1) {
      // Family number might also be zero in some of our unit test case
      numStores = 1;
    }
    inmemoryFlushSize = memstoreFlushSize / numStores;
    // multiply by a factor (the same factor for all index types)
    factor = conf.getDouble(IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY,
          IN_MEMORY_FLUSH_THRESHOLD_FACTOR_DEFAULT);

    inmemoryFlushSize = (long) (inmemoryFlushSize * factor);
  }

  /**
   * @return Total memory occupied by this MemStore. This won't include any size occupied by the
   *         snapshot. We assume the snapshot will get cleared soon. This is not thread safe and
   *         the memstore may be changed while computing its size. It is the responsibility of the
   *         caller to make sure this doesn't happen.
   */
  @Override
  public MemStoreSize size() {
    MemStoreSizing memstoreSizing = new MemStoreSizing();
    memstoreSizing.incMemStoreSize(active.getMemStoreSize());
    for (Segment item : pipeline.getSegments()) {
      memstoreSizing.incMemStoreSize(item.getMemStoreSize());
    }
    return memstoreSizing;
  }

  /**
   * This method is called before the flush is executed.
   * @return an estimation (lower bound) of the unflushed sequence id in memstore after the flush
   * is executed. if memstore will be cleared returns {@code HConstants.NO_SEQNUM}.
   */
  @Override
  public long preFlushSeqIDEstimation() {
    if(compositeSnapshot) {
      return HConstants.NO_SEQNUM;
    }
    Segment segment = getLastSegment();
    if(segment == null) {
      return HConstants.NO_SEQNUM;
    }
    return segment.getMinSequenceId();
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
      LOG.debug("FLUSHING TO DISK {}, store={}",
            getRegionServices().getRegionInfo().getEncodedName(), getFamilyName());
      stopCompaction();
      pushActiveToPipeline(this.active);
      snapshotId = EnvironmentEdgeManager.currentTime();
      // in both cases whatever is pushed to snapshot is cleared from the pipeline
      if (compositeSnapshot) {
        pushPipelineToSnapshot();
      } else {
        pushTailToSnapshot();
      }
      compactor.resetStats();
    }
    return new MemStoreSnapshot(snapshotId, this.snapshot);
  }

  /**
   * On flush, how much memory we will clear.
   * @return size of data that is going to be flushed
   */
  @Override
  public MemStoreSize getFlushableSize() {
    MemStoreSizing snapshotSizing = getSnapshotSizing();
    if (snapshotSizing.getDataSize() == 0) {
      // if snapshot is empty the tail of the pipeline (or everything in the memstore) is flushed
      if (compositeSnapshot) {
        snapshotSizing = pipeline.getPipelineSizing();
        snapshotSizing.incMemStoreSize(active.getMemStoreSize());
      } else {
        snapshotSizing = pipeline.getTailSizing();
      }
    }
    return snapshotSizing.getDataSize() > 0 ? snapshotSizing
        : new MemStoreSize(active.getMemStoreSize());
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
  protected long heapSize() {
    // Need to consider heapOverhead of all segments in pipeline and active
    long h = this.active.heapSize();
    for (Segment segment : this.pipeline.getSegments()) {
      h += segment.heapSize();
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

  /**
   * This message intends to inform the MemStore that next coming updates
   * are going to be part of the replaying edits from WAL
   */
  @Override
  public void startReplayingFromWAL() {
    inWalReplay = true;
  }

  /**
   * This message intends to inform the MemStore that the replaying edits from WAL
   * are done
   */
  @Override
  public void stopReplayingFromWAL() {
    inWalReplay = false;
  }

  // the getSegments() method is used for tests only
  @VisibleForTesting
  @Override
  protected List<Segment> getSegments() {
    List<? extends Segment> pipelineList = pipeline.getSegments();
    List<Segment> list = new ArrayList<>(pipelineList.size() + 2);
    list.add(this.active);
    list.addAll(pipelineList);
    list.addAll(this.snapshot.getAllSegments());

    return list;
  }

  // the following three methods allow to manipulate the settings of composite snapshot
  public void setCompositeSnapshot(boolean useCompositeSnapshot) {
    this.compositeSnapshot = useCompositeSnapshot;
  }

  public boolean swapCompactedSegments(VersionedSegmentsList versionedList, ImmutableSegment result,
      boolean merge) {
    // last true stands for updating the region size
    return pipeline.swap(versionedList, result, !merge, true);
  }

  /**
   * @param requesterVersion The caller must hold the VersionedList of the pipeline
   *           with version taken earlier. This version must be passed as a parameter here.
   *           The flattening happens only if versions match.
   */
  public void flattenOneSegment(long requesterVersion,  MemStoreCompactionStrategy.Action action) {
    pipeline.flattenOneSegment(requesterVersion, indexType, action);
  }

  // setter is used only for testability
  @VisibleForTesting
  void setIndexType(IndexType type) {
    indexType = type;
    // Because this functionality is for testing only and tests are setting in-memory flush size
    // according to their need, there is no setting of in-memory flush size, here.
    // If it is needed, please change in-memory flush size explicitly
  }

  public IndexType getIndexType() {
    return indexType;
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

  public HStore getStore() {
    return store;
  }

  public String getFamilyName() {
    return Bytes.toString(getFamilyNameInBytes());
  }

  @Override
  public List<KeyValueScanner> getScanners(long readPt) throws IOException {
    MutableSegment activeTmp = active;
    List<? extends Segment> pipelineList = pipeline.getSegments();
    List<? extends Segment> snapshotList = snapshot.getAllSegments();
    long numberOfSegments = 1L + pipelineList.size() + snapshotList.size();
    // The list of elements in pipeline + the active element + the snapshot segment
    List<KeyValueScanner> list = createList((int) numberOfSegments);
    addToScanners(activeTmp, readPt, list);
    addToScanners(pipelineList, readPt, list);
    addToScanners(snapshotList, readPt, list);
    return list;
  }

   @VisibleForTesting
   protected List<KeyValueScanner> createList(int capacity) {
     return new ArrayList<>(capacity);
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
        LOG.trace("IN-MEMORY FLUSH: Pushing active segment into compaction pipeline");
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
        LOG.warn("Unable to run in-memory compaction on {}/{}; exception={}",
            getRegionServices().getRegionInfo().getEncodedName(), getFamilyName(), e);
      }
    } finally {
      inMemoryFlushInProgress.set(false);
      LOG.trace("IN-MEMORY FLUSH: end");
    }
  }

  private Segment getLastSegment() {
    Segment localActive = getActive();
    Segment tail = pipeline.getTail();
    return tail == null ? localActive : tail;
  }

  private byte[] getFamilyNameInBytes() {
    return store.getColumnFamilyDescriptor().getName();
  }

  private ThreadPoolExecutor getPool() {
    return getRegionServices().getInMemoryCompactionPool();
  }

  @VisibleForTesting
  protected boolean shouldFlushInMemory() {
    if (this.active.keySize() > inmemoryFlushSize) { // size above flush threshold
      if (inWalReplay) {  // when replaying edits from WAL there is no need in in-memory flush
        return false;     // regardless the size
      }
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
    }
  }

  protected void pushActiveToPipeline(MutableSegment active) {
    if (!active.isEmpty()) {
      pipeline.pushHead(active);
      resetActive();
    }
  }

  private void pushTailToSnapshot() {
    VersionedSegmentsList segments = pipeline.getVersionedTail();
    pushToSnapshot(segments.getStoreSegments());
    // In Swap: don't close segments (they are in snapshot now) and don't update the region size
    pipeline.swap(segments,null,false, false);
  }

  private void pushPipelineToSnapshot() {
    int iterationsCnt = 0;
    boolean done = false;
    while (!done) {
      iterationsCnt++;
      VersionedSegmentsList segments = pipeline.getVersionedList();
      pushToSnapshot(segments.getStoreSegments());
      // swap can return false in case the pipeline was updated by ongoing compaction
      // and the version increase, the chance of it happenning is very low
      // In Swap: don't close segments (they are in snapshot now) and don't update the region size
      done = pipeline.swap(segments, null, false, false);
      if (iterationsCnt>2) {
        // practically it is impossible that this loop iterates more than two times
        // (because the compaction is stopped and none restarts it while in snapshot request),
        // however stopping here for the case of the infinite loop causing by any error
        LOG.warn("Multiple unsuccessful attempts to push the compaction pipeline to snapshot," +
            " while flushing to disk.");
        this.snapshot = SegmentFactory.instance().createImmutableSegment(getComparator());
        break;
      }
    }
  }

  private void pushToSnapshot(List<ImmutableSegment> segments) {
    if(segments.isEmpty()) return;
    if(segments.size() == 1 && !segments.get(0).isEmpty()) {
      this.snapshot = segments.get(0);
      return;
    } else { // create composite snapshot
      this.snapshot =
          SegmentFactory.instance().createCompositeImmutableSegment(getComparator(), segments);
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

  @VisibleForTesting
  boolean isMemStoreFlushingInMemory() {
    return inMemoryFlushInProgress.get();
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

  @VisibleForTesting
  long getInmemoryFlushSize() {
    return inmemoryFlushSize;
  }

  // debug method
  public void debug() {
    String msg = "active size=" + this.active.keySize();
    msg += " in-memory flush size is "+ inmemoryFlushSize;
    msg += " allow compaction is "+ (allowCompaction.get() ? "true" : "false");
    msg += " inMemoryFlushInProgress is "+ (inMemoryFlushInProgress.get() ? "true" : "false");
    LOG.debug(msg);
  }

}
