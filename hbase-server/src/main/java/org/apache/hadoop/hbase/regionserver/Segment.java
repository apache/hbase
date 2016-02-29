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

import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.ByteRange;

/**
 * This is an abstraction of a segment maintained in a memstore, e.g., the active
 * cell set or its snapshot.
 *
 * This abstraction facilitates the management of the compaction pipeline and the shifts of these
 * segments from active set to snapshot set in the default implementation.
 */
@InterfaceAudience.Private
public abstract class Segment {

  private volatile CellSet cellSet;
  private final CellComparator comparator;
  private volatile MemStoreLAB memStoreLAB;
  private final AtomicLong size;
  private final TimeRangeTracker timeRangeTracker;
  protected volatile boolean tagsPresent;

  protected Segment(CellSet cellSet, CellComparator comparator, MemStoreLAB memStoreLAB, long
      size) {
    this.cellSet = cellSet;
    this.comparator = comparator;
    this.memStoreLAB = memStoreLAB;
    this.size = new AtomicLong(size);
    this.timeRangeTracker = new TimeRangeTracker();
    this.tagsPresent = false;
  }

  protected Segment(Segment segment) {
    this.cellSet = segment.getCellSet();
    this.comparator = segment.getComparator();
    this.memStoreLAB = segment.getMemStoreLAB();
    this.size = new AtomicLong(segment.getSize());
    this.timeRangeTracker = segment.getTimeRangeTracker();
    this.tagsPresent = segment.isTagsPresent();
  }

  /**
   * Creates the scanner for the given read point
   * @return a scanner for the given read point
   */
  public SegmentScanner getSegmentScanner(long readPoint) {
    return new SegmentScanner(this, readPoint);
  }

  /**
   * Returns whether the segment has any cells
   * @return whether the segment has any cells
   */
  public boolean isEmpty() {
    return getCellSet().isEmpty();
  }

  /**
   * Returns number of cells in segment
   * @return number of cells in segment
   */
  public int getCellsCount() {
    return getCellSet().size();
  }

  /**
   * Returns the first cell in the segment that has equal or greater key than the given cell
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
    MemStoreLAB mslab = getMemStoreLAB();
    if(mslab != null) {
      mslab.close();
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
    if (getMemStoreLAB() == null) {
      return cell;
    }

    int len = KeyValueUtil.length(cell);
    ByteRange alloc = getMemStoreLAB().allocateBytes(len);
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

  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return (getTimeRangeTracker().includesTimeRange(scan.getTimeRange())
        && (getTimeRangeTracker().getMaximumTimestamp() >=
        oldestUnexpiredTS));
  }

  public long getMinTimestamp() {
    return getTimeRangeTracker().getMinimumTimestamp();
  }

  public boolean isTagsPresent() {
    return tagsPresent;
  }

  public void incScannerCount() {
    if(getMemStoreLAB() != null) {
      getMemStoreLAB().incScannerCount();
    }
  }

  public void decScannerCount() {
    if(getMemStoreLAB() != null) {
      getMemStoreLAB().decScannerCount();
    }
  }

  /**
   * Setting the heap size of the segment - used to account for different class overheads
   * @return this object
   */

  public Segment setSize(long size) {
    this.size.set(size);
    return this;
  }

  /**
   * Returns the heap size of the segment
   * @return the heap size of the segment
   */
  public long getSize() {
    return size.get();
  }

  /**
   * Increases the heap size counter of the segment by the given delta
   */
  public void incSize(long delta) {
    size.addAndGet(delta);
  }

  public TimeRangeTracker getTimeRangeTracker() {
    return timeRangeTracker;
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
   * Returns a set of all cells in the segment
   * @return a set of all cells in the segment
   */
  protected CellSet getCellSet() {
    return cellSet;
  }

  /**
   * Returns the Cell comparator used by this segment
   * @return the Cell comparator used by this segment
   */
  protected CellComparator getComparator() {
    return comparator;
  }

  protected long internalAdd(Cell cell) {
    boolean succ = getCellSet().add(cell);
    long s = AbstractMemStore.heapSizeChange(cell, succ);
    updateMetaInfo(cell, s);
    // In no tags case this NoTagsKeyValue.getTagsLength() is a cheap call.
    // When we use ACL CP or Visibility CP which deals with Tags during
    // mutation, the TagRewriteCell.getTagsLength() is a cheaper call. We do not
    // parse the byte[] to identify the tags length.
    if(cell.getTagsLength() > 0) {
      tagsPresent = true;
    }
    return s;
  }

  protected void updateMetaInfo(Cell toAdd, long s) {
    getTimeRangeTracker().includeTimestamp(toAdd);
    size.addAndGet(s);
  }

  /**
   * Returns a subset of the segment cell set, which starts with the given cell
   * @param firstCell a cell in the segment
   * @return a subset of the segment cell set, which starts with the given cell
   */
  protected SortedSet<Cell> tailSet(Cell firstCell) {
    return getCellSet().tailSet(firstCell);
  }

  private MemStoreLAB getMemStoreLAB() {
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
    res += "size "+getSize()+"; ";
    res += "Min ts "+getMinTimestamp()+"; ";
    return res;
  }

}
