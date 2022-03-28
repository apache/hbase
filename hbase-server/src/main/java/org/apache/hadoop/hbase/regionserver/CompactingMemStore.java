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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A memstore implementation which supports in-memory compaction.
 * A compaction pipeline is added between the active set and the snapshot data structures;
 * it consists of a list of segments that are subject to compaction.
 * Like the snapshot, all pipeline segments are read-only; updates only affect the active set.
 * To ensure this property we take advantage of the existing blocking mechanism -- the active set
 * is pushed to the pipeline while holding the region's updatesLock in exclusive mode.
 * Periodically, a compaction is applied in the background to all pipeline segments resulting
 * in a single read-only component. The ``old'' segments are discarded when no scanner is reading
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
  private static final int IN_MEMORY_FLUSH_MULTIPLIER = 1;
  // In-Memory compaction pool size
  public static final String IN_MEMORY_CONPACTION_POOL_SIZE_KEY =
      "hbase.regionserver.inmemory.compaction.pool.size";
  public static final int IN_MEMORY_CONPACTION_POOL_SIZE_DEFAULT = 10;

  private static final Logger LOG = LoggerFactory.getLogger(CompactingMemStore.class);
  private HStore store;
  private CompactionPipeline pipeline;
  protected MemStoreCompactor compactor;

  private long inmemoryFlushSize;       // the threshold on active size for in-memory flush
  private final AtomicBoolean inMemoryCompactionInProgress = new AtomicBoolean(false);

  // inWalReplay is true while we are synchronously replaying the edits from WAL
  private boolean inWalReplay = false;

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
      + 6 * ClassSize.REFERENCE     // Store, CompactionPipeline,
      // MemStoreCompactor, inMemoryCompactionInProgress,
      // allowCompaction, indexType
      + Bytes.SIZEOF_LONG           // inmemoryFlushSize
      + 2 * Bytes.SIZEOF_BOOLEAN    // compositeSnapshot and inWalReplay
      + 2 * ClassSize.ATOMIC_BOOLEAN// inMemoryCompactionInProgress and allowCompaction
      + CompactionPipeline.DEEP_OVERHEAD + MemStoreCompactor.DEEP_OVERHEAD);

  public CompactingMemStore(Configuration conf, CellComparator c,
      HStore store, RegionServicesForStores regionServices,
      MemoryCompactionPolicy compactionPolicy) throws IOException {
    super(conf, c, regionServices);
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
    factor = conf.getDouble(IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.0);
    if(factor != 0.0) {
      // multiply by a factor (the same factor for all index types)
      inmemoryFlushSize = (long) (factor * memstoreFlushSize) / numStores;
    } else {
      inmemoryFlushSize = IN_MEMORY_FLUSH_MULTIPLIER *
          conf.getLong(MemStoreLAB.CHUNK_SIZE_KEY, MemStoreLAB.CHUNK_SIZE_DEFAULT);
      inmemoryFlushSize -= ChunkCreator.SIZEOF_CHUNK_HEADER;
    }
  }

  /**
   * @return Total memory occupied by this MemStore. This won't include any size occupied by the
   *         snapshot. We assume the snapshot will get cleared soon. This is not thread safe and
   *         the memstore may be changed while computing its size. It is the responsibility of the
   *         caller to make sure this doesn't happen.
   */
  @Override
  public MemStoreSize size() {
    MemStoreSizing memstoreSizing = new NonThreadSafeMemStoreSizing();
    memstoreSizing.incMemStoreSize(getActive().getMemStoreSize());
    for (Segment item : pipeline.getSegments()) {
      memstoreSizing.incMemStoreSize(item.getMemStoreSize());
    }
    return memstoreSizing.getMemStoreSize();
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
      // region level lock ensures pushing active to pipeline is done in isolation
      // no concurrent update operations trying to flush the active segment
      pushActiveToPipeline(getActive(), true);
      resetTimeOfOldestEdit();
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

  @Override
  public MemStoreSize getFlushableSize() {
    MemStoreSize mss = getSnapshotSize();
    if (mss.getDataSize() == 0) {
      // if snapshot is empty the tail of the pipeline (or everything in the memstore) is flushed
      if (compositeSnapshot) {
        MemStoreSizing memStoreSizing = new NonThreadSafeMemStoreSizing(pipeline.getPipelineSize());
        MutableSegment currActive = getActive();
        if(!currActive.isEmpty()) {
          memStoreSizing.incMemStoreSize(currActive.getMemStoreSize());
        }
        mss = memStoreSizing.getMemStoreSize();
      } else {
        mss = pipeline.getTailSize();
      }
    }
    return mss.getDataSize() > 0? mss: getActive().getMemStoreSize();
  }


  public void setInMemoryCompactionCompleted() {
    inMemoryCompactionInProgress.set(false);
  }

  protected boolean setInMemoryCompactionFlag() {
    return inMemoryCompactionInProgress.compareAndSet(false, true);
  }

  @Override
  protected long keySize() {
    // Need to consider dataSize/keySize of all segments in pipeline and active
    long keySize = getActive().getDataSize();
    for (Segment segment : this.pipeline.getSegments()) {
      keySize += segment.getDataSize();
    }
    return keySize;
  }

  @Override
  protected long heapSize() {
    // Need to consider heapOverhead of all segments in pipeline and active
    long h = getActive().getHeapSize();
    for (Segment segment : this.pipeline.getSegments()) {
      h += segment.getHeapSize();
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

  /**
   * Issue any synchronization and test needed before applying the update
   * For compacting memstore this means checking the update can increase the size without
   * overflow
   * @param currentActive the segment to be updated
   * @param cell the cell to be added
   * @param memstoreSizing object to accumulate region size changes
   * @return true iff can proceed with applying the update
   */
  @Override
  protected boolean preUpdate(MutableSegment currentActive, Cell cell,
      MemStoreSizing memstoreSizing) {
    if (currentActive.sharedLock()) {
      if (checkAndAddToActiveSize(currentActive, cell, memstoreSizing)) {
        return true;
      }
      currentActive.sharedUnlock();
    }
    return false;
  }

  @Override protected void postUpdate(MutableSegment currentActive) {
    currentActive.sharedUnlock();
  }

  @Override protected boolean sizeAddedPreOperation() {
    return true;
  }

  // the getSegments() method is used for tests only
  @Override
  protected List<Segment> getSegments() {
    List<? extends Segment> pipelineList = pipeline.getSegments();
    List<Segment> list = new ArrayList<>(pipelineList.size() + 2);
    list.add(getActive());
    list.addAll(pipelineList);
    list.addAll(snapshot.getAllSegments());

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

  /**
   * This method is protected under {@link HStore#lock} read lock.
   */
  @Override
  public List<KeyValueScanner> getScanners(long readPt) throws IOException {
    MutableSegment activeTmp = getActive();
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

  protected List<KeyValueScanner> createList(int capacity) {
    return new ArrayList<>(capacity);
  }

  /**
   * Check whether anything need to be done based on the current active set size. The method is
   * invoked upon every addition to the active set. For CompactingMemStore, flush the active set to
   * the read-only memory if it's size is above threshold
   * @param currActive intended segment to update
   * @param cellToAdd cell to be added to the segment
   * @param memstoreSizing object to accumulate changed size
   * @return true if the cell can be added to the currActive
   */
  protected boolean checkAndAddToActiveSize(MutableSegment currActive, Cell cellToAdd,
      MemStoreSizing memstoreSizing) {
    long cellSize = MutableSegment.getCellLength(cellToAdd);
    boolean successAdd = false;
    while (true) {
      long segmentDataSize = currActive.getDataSize();
      if (!inWalReplay && segmentDataSize > inmemoryFlushSize) {
        // when replaying edits from WAL there is no need in in-memory flush regardless the size
        // otherwise size below flush threshold try to update atomically
        break;
      }
      if (currActive.compareAndSetDataSize(segmentDataSize, segmentDataSize + cellSize)) {
        if (memstoreSizing != null) {
          memstoreSizing.incMemStoreSize(cellSize, 0, 0, 0);
        }
        successAdd = true;
        break;
      }
    }

    if (!inWalReplay && currActive.getDataSize() > inmemoryFlushSize) {
      // size above flush threshold so we flush in memory
      this.tryFlushInMemoryAndCompactingAsync(currActive);
    }
    return successAdd;
  }

  /**
   * Try to flush the currActive in memory and submit the background
   * {@link InMemoryCompactionRunnable} to
   * {@link RegionServicesForStores#getInMemoryCompactionPool()}. Just one thread can do the actual
   * flushing in memory.
   * @param currActive current Active Segment to be flush in memory.
   */
  private void tryFlushInMemoryAndCompactingAsync(MutableSegment currActive) {
    if (currActive.setInMemoryFlushed()) {
      flushInMemory(currActive);
      if (setInMemoryCompactionFlag()) {
        // The thread is dispatched to do in-memory compaction in the background
        InMemoryCompactionRunnable runnable = new InMemoryCompactionRunnable();
        if (LOG.isTraceEnabled()) {
          LOG.trace(
            "Dispatching the MemStore in-memory flush for store " + store.getColumnFamilyName());
        }
        getPool().execute(runnable);
      }
    }
  }

  // externally visible only for tests
  // when invoked directly from tests it must be verified that the caller doesn't hold updatesLock,
  // otherwise there is a deadlock
  void flushInMemory() {
    MutableSegment currActive = getActive();
    if(currActive.setInMemoryFlushed()) {
      flushInMemory(currActive);
    }
    inMemoryCompaction();
  }

  protected void flushInMemory(MutableSegment currActive) {
    LOG.trace("IN-MEMORY FLUSH: Pushing active segment into compaction pipeline");
    // NOTE: Due to concurrent writes and because we first add cell size to currActive.getDataSize
    // and then actually add cell to currActive.cellSet, it is possible that
    // currActive.getDataSize could not accommodate cellToAdd but currActive.cellSet is still
    // empty if pending writes which not yet add cells to currActive.cellSet.
    // so here we should not check currActive.isEmpty or not.
    pushActiveToPipeline(currActive, false);
  }

  void inMemoryCompaction() {
    // setting the inMemoryCompactionInProgress flag again for the case this method is invoked
    // directly (only in tests) in the common path setting from true to true is idempotent
    inMemoryCompactionInProgress.set(true);
    // Used by tests
    if (!allowCompaction.get()) {
      return;
    }
    try {
      // Speculative compaction execution, may be interrupted if flush is forced while
      // compaction is in progress
      if(!compactor.start()) {
        setInMemoryCompactionCompleted();
      }
    } catch (IOException e) {
      LOG.warn("Unable to run in-memory compaction on {}/{}; exception={}",
          getRegionServices().getRegionInfo().getEncodedName(), getFamilyName(), e);
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

  /**
   * The request to cancel the compaction asynchronous task (caused by in-memory flush)
   * The compaction may still happen if the request was sent too late
   * Non-blocking request
   */
  private void stopCompaction() {
    if (inMemoryCompactionInProgress.get()) {
      compactor.stop();
    }
  }

  /**
   * NOTE: When {@link CompactingMemStore#flushInMemory(MutableSegment)} calls this method, due to
   * concurrent writes and because we first add cell size to currActive.getDataSize and then
   * actually add cell to currActive.cellSet, it is possible that currActive.getDataSize could not
   * accommodate cellToAdd but currActive.cellSet is still empty if pending writes which not yet add
   * cells to currActive.cellSet,so for
   * {@link CompactingMemStore#flushInMemory(MutableSegment)},checkEmpty parameter is false. But if
   * {@link CompactingMemStore#snapshot} called this method,because there is no pending
   * write,checkEmpty parameter could be true.
   * @param currActive
   * @param checkEmpty
   */
  protected void pushActiveToPipeline(MutableSegment currActive, boolean checkEmpty) {
    if (!checkEmpty || !currActive.isEmpty()) {
      pipeline.pushHead(currActive);
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
      VersionedSegmentsList segments = getImmutableSegments();
      pushToSnapshot(segments.getStoreSegments());
      // swap can return false in case the pipeline was updated by ongoing compaction
      // and the version increase, the chance of it happenning is very low
      // In Swap: don't close segments (they are in snapshot now) and don't update the region size
      done = swapPipelineWithNull(segments);
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

  protected boolean swapPipelineWithNull(VersionedSegmentsList segments) {
    return pipeline.swap(segments, null, false, false);
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
  private class InMemoryCompactionRunnable implements Runnable {
    @Override
    public void run() {
      inMemoryCompaction();
    }
  }

  boolean isMemStoreFlushingInMemory() {
    return inMemoryCompactionInProgress.get();
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

  long getInmemoryFlushSize() {
    return inmemoryFlushSize;
  }

  // debug method
  public void debug() {
    String msg = "active size=" + getActive().getDataSize();
    msg += " allow compaction is "+ (allowCompaction.get() ? "true" : "false");
    msg += " inMemoryCompactionInProgress is "+ (inMemoryCompactionInProgress.get() ? "true" :
        "false");
    LOG.debug(msg);
  }

}
