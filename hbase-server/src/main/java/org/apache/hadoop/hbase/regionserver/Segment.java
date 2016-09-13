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
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import com.google.common.annotations.VisibleForTesting;

/**
 * This is an abstraction of a segment maintained in a memstore, e.g., the active
 * cell set or its snapshot.
 *
 * This abstraction facilitates the management of the compaction pipeline and the shifts of these
 * segments from active set to snapshot set in the default implementation.
 */
@InterfaceAudience.Private
public abstract class Segment {

  final static long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT
      + 5 * ClassSize.REFERENCE // cellSet, comparator, memStoreLAB, size, timeRangeTracker
      + Bytes.SIZEOF_LONG // minSequenceId
      + Bytes.SIZEOF_BOOLEAN); // tagsPresent
  public final static long DEEP_OVERHEAD = FIXED_OVERHEAD + ClassSize.ATOMIC_REFERENCE
      + ClassSize.CELL_SET + ClassSize.ATOMIC_LONG + ClassSize.TIMERANGE_TRACKER;

  private AtomicReference<CellSet> cellSet= new AtomicReference<CellSet>();
  private final CellComparator comparator;
  private long minSequenceId;
  private MemStoreLAB memStoreLAB;
  // Sum of sizes of all Cells added to this Segment. Cell's heapSize is considered. This is not
  // including the heap overhead of this class.
  protected final AtomicLong size;
  protected final TimeRangeTracker timeRangeTracker;
  protected volatile boolean tagsPresent;

  // This constructor is used to create empty Segments.
  protected Segment(CellSet cellSet, CellComparator comparator, MemStoreLAB memStoreLAB) {
    this.cellSet.set(cellSet);
    this.comparator = comparator;
    this.minSequenceId = Long.MAX_VALUE;
    this.memStoreLAB = memStoreLAB;
    this.size = new AtomicLong(0);
    this.tagsPresent = false;
    this.timeRangeTracker = new TimeRangeTracker();
  }

  protected Segment(Segment segment) {
    this.cellSet.set(segment.getCellSet());
    this.comparator = segment.getComparator();
    this.minSequenceId = segment.getMinSequenceId();
    this.memStoreLAB = segment.getMemStoreLAB();
    this.size = new AtomicLong(segment.keySize());
    this.tagsPresent = segment.isTagsPresent();
    this.timeRangeTracker = segment.getTimeRangeTracker();
  }

  /**
   * Creates the scanner for the given read point
   * @return a scanner for the given read point
   */
  public SegmentScanner getScanner(long readPoint) {
    return new SegmentScanner(this, readPoint);
  }

  /**
   * Creates the scanner for the given read point, and a specific order in a list
   * @return a scanner for the given read point
   */
  public SegmentScanner getScanner(long readPoint, long order) {
    return new SegmentScanner(this, readPoint, order);
  }

  /**
   * @return whether the segment has any cells
   */
  public boolean isEmpty() {
    return getCellSet().isEmpty();
  }

  /**
   * @return number of cells in segment
   */
  public int getCellsCount() {
    return getCellSet().size();
  }

  /**
   * @return the first cell in the segment that has equal or greater key than the given cell
   */
  public Cell getFirstAfter(Cell cell) {
    SortedSet<Cell> snTailSet = tailSet(cell);
    if (!snTailSet.isEmpty()) {
      return snTailSet.first();
    }
    return null;
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
   * @return either the given cell or its clone
   */
  public Cell maybeCloneWithAllocator(Cell cell) {
    if (this.memStoreLAB == null) {
      return cell;
    }

    int len = getCellLength(cell);
    ByteRange alloc = this.memStoreLAB.allocateBytes(len);
    if (alloc == null) {
      // The allocation was too large, allocator decided
      // not to do anything with it.
      return cell;
    }
    assert alloc.getBytes() != null;
    KeyValueUtil.appendToByteArray(cell, alloc.getBytes(), alloc.getOffset());
    KeyValue newKv = new KeyValue(alloc.getBytes(), alloc.getOffset(), len);
    newKv.setSequenceId(cell.getSequenceId());
    return newKv;
  }

  /**
   * Get cell length after serialized in {@link KeyValue}
   */
  @VisibleForTesting
  int getCellLength(Cell cell) {
    return KeyValueUtil.length(cell);
  }

  public abstract boolean shouldSeek(Scan scan, long oldestUnexpiredTS);

  public abstract long getMinTimestamp();

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

  /**
   * @return Sum of all cell's size.
   */
  public long keySize() {
    return this.size.get();
  }

  /**
   * @return the heap size of the segment
   */
  public abstract long size();

  /**
   * Updates the heap size counter of the segment by the given delta
   */
  public void incSize(long delta) {
    this.size.addAndGet(delta);
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

  protected long internalAdd(Cell cell, boolean mslabUsed) {
    boolean succ = getCellSet().add(cell);
    long s = updateMetaInfo(cell, succ, mslabUsed);
    return s;
  }

  protected long updateMetaInfo(Cell cellToAdd, boolean succ, boolean mslabUsed) {
    long s = heapSizeChange(cellToAdd, succ);
    // If there's already a same cell in the CellSet and we are using MSLAB, we must count in the
    // MSLAB allocation size as well, or else there will be memory leak (occupied heap size larger
    // than the counted number)
    if (!succ && mslabUsed) {
      s += getCellLength(cellToAdd);
    }
    getTimeRangeTracker().includeTimestamp(cellToAdd);
    incSize(s);
    minSequenceId = Math.min(minSequenceId, cellToAdd.getSequenceId());
    // In no tags case this NoTagsKeyValue.getTagsLength() is a cheap call.
    // When we use ACL CP or Visibility CP which deals with Tags during
    // mutation, the TagRewriteCell.getTagsLength() is a cheaper call. We do not
    // parse the byte[] to identify the tags length.
    if( cellToAdd.getTagsLength() > 0) {
      tagsPresent = true;
    }
    return s;
  }

  protected long heapSizeChange(Cell cell, boolean succ) {
    return succ ? ClassSize
        .align(ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY + CellUtil.estimatedHeapSizeOf(cell)) : 0;
  }

  /**
   * Returns a subset of the segment cell set, which starts with the given cell
   * @param firstCell a cell in the segment
   * @return a subset of the segment cell set, which starts with the given cell
   */
  protected SortedSet<Cell> tailSet(Cell firstCell) {
    return getCellSet().tailSet(firstCell);
  }

  @VisibleForTesting
  MemStoreLAB getMemStoreLAB() {
    return memStoreLAB;
  }

  // Debug methods
  /**
   * Dumps all cells of the segment into the given log
   */
  void dump(Log log) {
    for (Cell cell: getCellSet()) {
      log.debug(cell);
    }
  }

  @Override
  public String toString() {
    String res = "Store segment of type "+this.getClass().getName()+"; ";
    res += "isEmpty "+(isEmpty()?"yes":"no")+"; ";
    res += "cellCount "+getCellsCount()+"; ";
    res += "cellsSize "+keySize()+"; ";
    res += "heapSize "+size()+"; ";
    res += "Min ts "+getMinTimestamp()+"; ";
    return res;
  }
}
