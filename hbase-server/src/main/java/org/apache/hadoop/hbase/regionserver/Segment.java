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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;

/**
 * This is an abstraction of a segment maintained in a memstore, e.g., the active
 * cell set or its snapshot.
 *
 * This abstraction facilitates the management of the compaction pipeline and the shifts of these
 * segments from active set to snapshot set in the default implementation.
 */
@InterfaceAudience.Private
public abstract class Segment implements MemStoreSizing {

  public final static long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT
      + 6 * ClassSize.REFERENCE // cellSet, comparator, updatesLock, memStoreLAB, memStoreSizing,
                                // and timeRangeTracker
      + Bytes.SIZEOF_LONG // minSequenceId
      + Bytes.SIZEOF_BOOLEAN); // tagsPresent
  public final static long DEEP_OVERHEAD = FIXED_OVERHEAD + ClassSize.ATOMIC_REFERENCE
      + ClassSize.CELL_SET + 2 * ClassSize.ATOMIC_LONG
      + ClassSize.REENTRANT_LOCK;

  private AtomicReference<CellSet> cellSet= new AtomicReference<>();
  private final CellComparator comparator;
  private ReentrantReadWriteLock updatesLock;
  protected long minSequenceId;
  private MemStoreLAB memStoreLAB;
  // Sum of sizes of all Cells added to this Segment. Cell's HeapSize is considered. This is not
  // including the heap overhead of this class.
  protected final MemStoreSizing memStoreSizing;
  protected final TimeRangeTracker timeRangeTracker;
  protected volatile boolean tagsPresent;

  // Empty constructor to be used when Segment is used as interface,
  // and there is no need in true Segments state
  protected Segment(CellComparator comparator, TimeRangeTracker trt) {
    this.comparator = comparator;
    // Do we need to be thread safe always? What if ImmutableSegment?
    // DITTO for the TimeRangeTracker below.
    this.memStoreSizing = new ThreadSafeMemStoreSizing();
    this.timeRangeTracker = trt;
  }

  protected Segment(CellComparator comparator, List<ImmutableSegment> segments,
      TimeRangeTracker trt) {
    long dataSize = 0;
    long heapSize = 0;
    long OffHeapSize = 0;
    int cellsCount = 0;
    for (Segment segment : segments) {
      MemStoreSize memStoreSize = segment.getMemStoreSize();
      dataSize += memStoreSize.getDataSize();
      heapSize += memStoreSize.getHeapSize();
      OffHeapSize += memStoreSize.getOffHeapSize();
      cellsCount += memStoreSize.getCellsCount();
    }
    this.comparator = comparator;
    this.updatesLock = new ReentrantReadWriteLock();
    // Do we need to be thread safe always? What if ImmutableSegment?
    // DITTO for the TimeRangeTracker below.
    this.memStoreSizing = new ThreadSafeMemStoreSizing(dataSize, heapSize, OffHeapSize, cellsCount);
    this.timeRangeTracker = trt;
  }

  // This constructor is used to create empty Segments.
  protected Segment(CellSet cellSet, CellComparator comparator, MemStoreLAB memStoreLAB, TimeRangeTracker trt) {
    this.cellSet.set(cellSet);
    this.comparator = comparator;
    this.updatesLock = new ReentrantReadWriteLock();
    this.minSequenceId = Long.MAX_VALUE;
    this.memStoreLAB = memStoreLAB;
    // Do we need to be thread safe always? What if ImmutableSegment?
    // DITTO for the TimeRangeTracker below.
    this.memStoreSizing = new ThreadSafeMemStoreSizing();
    this.tagsPresent = false;
    this.timeRangeTracker = trt;
  }

  protected Segment(Segment segment) {
    this.cellSet.set(segment.getCellSet());
    this.comparator = segment.getComparator();
    this.updatesLock = segment.getUpdatesLock();
    this.minSequenceId = segment.getMinSequenceId();
    this.memStoreLAB = segment.getMemStoreLAB();
    this.memStoreSizing = segment.memStoreSizing;
    this.tagsPresent = segment.isTagsPresent();
    this.timeRangeTracker = segment.getTimeRangeTracker();
  }

  /**
   * Creates the scanner for the given read point
   * @return a scanner for the given read point
   */
  protected KeyValueScanner getScanner(long readPoint) {
    return new SegmentScanner(this, readPoint);
  }

  public List<KeyValueScanner> getScanners(long readPoint) {
    return Collections.singletonList(new SegmentScanner(this, readPoint));
  }

  /**
   * @return whether the segment has any cells
   */
  public boolean isEmpty() {
    return getCellSet().isEmpty();
  }


  /**
   * Closing a segment before it is being discarded
   */
  public void close() {
    if (this.memStoreLAB != null) {
      this.memStoreLAB.close();
    }
    // do not set MSLab to null as scanners may still be reading the data here and need to decrease
    // the counter when they finish
  }

  /**
   * If the segment has a memory allocator the cell is being cloned to this space, and returned;
   * otherwise the given cell is returned
   *
   * When a cell's size is too big (bigger than maxAlloc), it is not allocated on MSLAB.
   * Since the process of flattening to CellChunkMap assumes that all cells
   * are allocated on MSLAB, during this process, the input parameter
   * forceCloneOfBigCell is set to 'true' and the cell is copied into MSLAB.
   *
   * @return either the given cell or its clone
   */
  public Cell maybeCloneWithAllocator(Cell cell, boolean forceCloneOfBigCell) {
    if (this.memStoreLAB == null) {
      return cell;
    }

    Cell cellFromMslab;
    if (forceCloneOfBigCell) {
      cellFromMslab = this.memStoreLAB.forceCopyOfBigCellInto(cell);
    } else {
      cellFromMslab = this.memStoreLAB.copyCellInto(cell);
    }
    return (cellFromMslab != null) ? cellFromMslab : cell;
  }

  /**
   * Get cell length after serialized in {@link KeyValue}
   */
  static int getCellLength(Cell cell) {
    return cell.getSerializedSize();
  }

  public boolean shouldSeek(TimeRange tr, long oldestUnexpiredTS) {
    return !isEmpty()
        && (tr.isAllTime() || timeRangeTracker.includesTimeRange(tr))
        && timeRangeTracker.getMax() >= oldestUnexpiredTS;
  }

  public boolean isTagsPresent() {
    return tagsPresent;
  }

  public void incScannerCount() {
    if (this.memStoreLAB != null) {
      this.memStoreLAB.incScannerCount();
    }
  }

  public void decScannerCount() {
    if (this.memStoreLAB != null) {
      this.memStoreLAB.decScannerCount();
    }
  }

  /**
   * Setting the CellSet of the segment - used only for flat immutable segment for setting
   * immutable CellSet after its creation in immutable segment constructor
   * @return this object
   */

  protected Segment setCellSet(CellSet cellSetOld, CellSet cellSetNew) {
    this.cellSet.compareAndSet(cellSetOld, cellSetNew);
    return this;
  }

  @Override
  public MemStoreSize getMemStoreSize() {
    return this.memStoreSizing.getMemStoreSize();
  }

  @Override
  public long getDataSize() {
    return this.memStoreSizing.getDataSize();
  }

  @Override
  public long getHeapSize() {
    return this.memStoreSizing.getHeapSize();
  }

  @Override
  public long getOffHeapSize() {
    return this.memStoreSizing.getOffHeapSize();
  }

  @Override
  public int getCellsCount() {
    return memStoreSizing.getCellsCount();
  }

  @Override
  public long incMemStoreSize(long delta, long heapOverhead, long offHeapOverhead, int cellsCount) {
    return this.memStoreSizing.incMemStoreSize(delta, heapOverhead, offHeapOverhead, cellsCount);
  }

  public boolean sharedLock() {
    return updatesLock.readLock().tryLock();
  }

  public void sharedUnlock() {
    updatesLock.readLock().unlock();
  }

  public void waitForUpdates() {
    if(!updatesLock.isWriteLocked()) {
      updatesLock.writeLock().lock();
    }
  }

  @Override
  public boolean compareAndSetDataSize(long expected, long updated) {
    return memStoreSizing.compareAndSetDataSize(expected, updated);
  }

  public long getMinSequenceId() {
    return minSequenceId;
  }

  public TimeRangeTracker getTimeRangeTracker() {
    return this.timeRangeTracker;
  }

  //*** Methods for SegmentsScanner
  public Cell last() {
    return getCellSet().last();
  }

  public Iterator<Cell> iterator() {
    return getCellSet().iterator();
  }

  public SortedSet<Cell> headSet(Cell firstKeyOnRow) {
    return getCellSet().headSet(firstKeyOnRow);
  }

  public int compare(Cell left, Cell right) {
    return getComparator().compare(left, right);
  }

  public int compareRows(Cell left, Cell right) {
    return getComparator().compareRows(left, right);
  }

  /**
   * @return a set of all cells in the segment
   */
  protected CellSet getCellSet() {
    return cellSet.get();
  }

  /**
   * Returns the Cell comparator used by this segment
   * @return the Cell comparator used by this segment
   */
  protected CellComparator getComparator() {
    return comparator;
  }

  protected void internalAdd(Cell cell, boolean mslabUsed, MemStoreSizing memstoreSizing,
      boolean sizeAddedPreOperation) {
    boolean succ = getCellSet().add(cell);
    updateMetaInfo(cell, succ, mslabUsed, memstoreSizing, sizeAddedPreOperation);
  }

  protected void updateMetaInfo(Cell cellToAdd, boolean succ, boolean mslabUsed,
      MemStoreSizing memstoreSizing, boolean sizeAddedPreOperation) {
    long delta = 0;
    long cellSize = getCellLength(cellToAdd);
    int cellsCount = succ ? 1 : 0;
    // If there's already a same cell in the CellSet and we are using MSLAB, we must count in the
    // MSLAB allocation size as well, or else there will be memory leak (occupied heap size larger
    // than the counted number)
    if (succ || mslabUsed) {
      delta = cellSize;
    }
    if (sizeAddedPreOperation) {
      delta -= cellSize;
    }
    long heapSize = heapSizeChange(cellToAdd, succ || mslabUsed);
    long offHeapSize = offHeapSizeChange(cellToAdd, succ || mslabUsed);
    incMemStoreSize(delta, heapSize, offHeapSize, cellsCount);
    if (memstoreSizing != null) {
      memstoreSizing.incMemStoreSize(delta, heapSize, offHeapSize, cellsCount);
    }
    getTimeRangeTracker().includeTimestamp(cellToAdd);
    minSequenceId = Math.min(minSequenceId, cellToAdd.getSequenceId());
    // In no tags case this NoTagsKeyValue.getTagsLength() is a cheap call.
    // When we use ACL CP or Visibility CP which deals with Tags during
    // mutation, the TagRewriteCell.getTagsLength() is a cheaper call. We do not
    // parse the byte[] to identify the tags length.
    if (cellToAdd.getTagsLength() > 0) {
      tagsPresent = true;
    }
  }

  protected void updateMetaInfo(Cell cellToAdd, boolean succ, MemStoreSizing memstoreSizing) {
    updateMetaInfo(cellToAdd, succ, (getMemStoreLAB()!=null), memstoreSizing, false);
  }

  /**
   * @return The increase in heap size because of this cell addition. This includes this cell POJO's
   *         heap size itself and additional overhead because of addition on to CSLM.
   */
  protected long heapSizeChange(Cell cell, boolean allocated) {
    long res = 0;
    if (allocated) {
      boolean onHeap = true;
      MemStoreLAB memStoreLAB = getMemStoreLAB();
      if(memStoreLAB != null) {
        onHeap = memStoreLAB.isOnHeap();
      }
      res += indexEntryOnHeapSize(onHeap);
      if(onHeap) {
        res += cell.heapSize();
      }
      res = ClassSize.align(res);
    }
    return res;
  }

  protected long offHeapSizeChange(Cell cell, boolean allocated) {
    long res = 0;
    if (allocated) {
      boolean offHeap = false;
      MemStoreLAB memStoreLAB = getMemStoreLAB();
      if(memStoreLAB != null) {
        offHeap = memStoreLAB.isOffHeap();
      }
      res += indexEntryOffHeapSize(offHeap);
      if(offHeap) {
        res += cell.heapSize();
      }
      res = ClassSize.align(res);
    }
    return res;
  }

  protected long indexEntryOnHeapSize(boolean onHeap) {
    // in most cases index is allocated on-heap
    // override this method when it is not always the case, e.g., in CCM
    return indexEntrySize();
  }

  protected long indexEntryOffHeapSize(boolean offHeap) {
    // in most cases index is allocated on-heap
    // override this method when it is not always the case, e.g., in CCM
    return 0;
  }

  protected abstract long indexEntrySize();

  /**
   * Returns a subset of the segment cell set, which starts with the given cell
   * @param firstCell a cell in the segment
   * @return a subset of the segment cell set, which starts with the given cell
   */
  protected SortedSet<Cell> tailSet(Cell firstCell) {
    return getCellSet().tailSet(firstCell);
  }

  MemStoreLAB getMemStoreLAB() {
    return memStoreLAB;
  }

  // Debug methods
  /**
   * Dumps all cells of the segment into the given log
   */
  void dump(Logger log) {
    for (Cell cell: getCellSet()) {
      log.debug(Objects.toString(cell));
    }
  }

  @Override
  public String toString() {
    String res = "type=" + this.getClass().getSimpleName() + ", ";
    res += "empty=" + (isEmpty()? "yes": "no") + ", ";
    res += "cellCount=" + getCellsCount() + ", ";
    res += "cellSize=" + getDataSize() + ", ";
    res += "totalHeapSize=" + getHeapSize() + ", ";
    res += "min timestamp=" + timeRangeTracker.getMin() + ", ";
    res += "max timestamp=" + timeRangeTracker.getMax();
    return res;
  }

  private ReentrantReadWriteLock getUpdatesLock() {
    return updatesLock;
  }
}
