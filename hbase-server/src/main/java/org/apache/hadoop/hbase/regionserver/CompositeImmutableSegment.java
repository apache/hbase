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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

/**
 * The CompositeImmutableSegments is created as a collection of ImmutableSegments and supports
 * the interface of a single ImmutableSegments.
 * The CompositeImmutableSegments is planned to be used only as a snapshot,
 * thus only relevant interfaces are supported
 */
@InterfaceAudience.Private
public class CompositeImmutableSegment extends ImmutableSegment {

  private final List<ImmutableSegment> segments;
  private final CellComparator comparator;
  // CompositeImmutableSegment is used for snapshots and snapshot should
  // support getTimeRangeTracker() interface.
  // Thus we hold a constant TRT build in the construction time from TRT of the given segments.
  private final TimeRangeTracker timeRangeTracker;

  private long keySize = 0;

  public CompositeImmutableSegment(CellComparator comparator, List<ImmutableSegment> segments) {
    super(comparator);
    this.comparator = comparator;
    this.segments = segments;
    this.timeRangeTracker = new TimeRangeTracker();
    for (ImmutableSegment s : segments) {
      this.timeRangeTracker.includeTimestamp(s.getTimeRangeTracker().getMax());
      this.timeRangeTracker.includeTimestamp(s.getTimeRangeTracker().getMin());
      this.keySize += s.keySize();
    }
  }

  @VisibleForTesting
  public List<Segment> getAllSegments() {
    return new LinkedList<Segment>(segments);
  }

  public int getNumOfSegments() {
    return segments.size();
  }

  /**
   * Builds a special scanner for the MemStoreSnapshot object that is different than the
   * general segment scanner.
   * @return a special scanner for the MemStoreSnapshot object
   */
  public KeyValueScanner getSnapshotScanner() {
    return getScanner(Long.MAX_VALUE, Long.MAX_VALUE);
  }

  /**
   * @return whether the segment has any cells
   */
  public boolean isEmpty() {
    for (ImmutableSegment s : segments) {
      if (!s.isEmpty()) return false;
    }
    return true;
  }

  /**
   * @return number of cells in segment
   */
  public int getCellsCount() {
    int result = 0;
    for (ImmutableSegment s : segments) {
      result += s.getCellsCount();
    }
    return result;
  }

  /**
   * @return the first cell in the segment that has equal or greater key than the given cell
   */
  public Cell getFirstAfter(Cell cell) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  /**
   * Closing a segment before it is being discarded
   */
  public void close() {
    for (ImmutableSegment s : segments) {
      s.close();
    }
  }

  /**
   * If the segment has a memory allocator the cell is being cloned to this space, and returned;
   * otherwise the given cell is returned
   * @return either the given cell or its clone
   */
  public Cell maybeCloneWithAllocator(Cell cell) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS){
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  public long getMinTimestamp(){
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  /**
   * Creates the scanner for the given read point
   * @return a scanner for the given read point
   */
  public KeyValueScanner getScanner(long readPoint) {
    // Long.MAX_VALUE is DEFAULT_SCANNER_ORDER
    return getScanner(readPoint,Long.MAX_VALUE);
  }

  /**
   * Creates the scanner for the given read point, and a specific order in a list
   * @return a scanner for the given read point
   */
  public KeyValueScanner getScanner(long readPoint, long order) {
    KeyValueScanner resultScanner;
    List<KeyValueScanner> list = new ArrayList<KeyValueScanner>(segments.size());
    for (ImmutableSegment s : segments) {
      list.add(s.getScanner(readPoint, order));
    }

    try {
      resultScanner = new MemStoreScanner(getComparator(), list);
    } catch (IOException ie) {
      throw new IllegalStateException(ie);
    }

    return resultScanner;
  }

  public boolean isTagsPresent() {
    for (ImmutableSegment s : segments) {
      if (s.isTagsPresent()) return true;
    }
    return false;
  }

  public void incScannerCount() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  public void decScannerCount() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  /**
   * Setting the CellSet of the segment - used only for flat immutable segment for setting
   * immutable CellSet after its creation in immutable segment constructor
   * @return this object
   */

  protected CompositeImmutableSegment setCellSet(CellSet cellSetOld, CellSet cellSetNew) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  /**
   * @return Sum of all cell sizes.
   */
  public long keySize() {
    return this.keySize;
  }

  /**
   * @return The heap overhead of this segment.
   */
  public long heapOverhead() {
    long result = 0;
    for (ImmutableSegment s : segments) {
      result += s.heapOverhead();
    }
    return result;
  }

  /**
   * Updates the heap size counter of the segment by the given delta
   */
  protected void incSize(long delta, long heapOverhead) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  protected void incHeapOverheadSize(long delta) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  public long getMinSequenceId() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  public TimeRangeTracker getTimeRangeTracker() {
    return this.timeRangeTracker;
  }

  //*** Methods for SegmentsScanner
  public Cell last() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  public Iterator<Cell> iterator() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  public SortedSet<Cell> headSet(Cell firstKeyOnRow) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  public int compare(Cell left, Cell right) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  public int compareRows(Cell left, Cell right) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  /**
   * @return a set of all cells in the segment
   */
  protected CellSet getCellSet() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  /**
   * Returns the Cell comparator used by this segment
   * @return the Cell comparator used by this segment
   */
  protected CellComparator getComparator() {
    return comparator;
  }

  protected void internalAdd(Cell cell, boolean mslabUsed, MemstoreSize memstoreSize) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  protected void updateMetaInfo(Cell cellToAdd, boolean succ, boolean mslabUsed,
      MemstoreSize memstoreSize) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  protected long heapOverheadChange(Cell cell, boolean succ) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  /**
   * Returns a subset of the segment cell set, which starts with the given cell
   * @param firstCell a cell in the segment
   * @return a subset of the segment cell set, which starts with the given cell
   */
  protected SortedSet<Cell> tailSet(Cell firstCell) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  // Debug methods
  /**
   * Dumps all cells of the segment into the given log
   */
  void dump(Log log) {
    for (ImmutableSegment s : segments) {
      s.dump(log);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder("This is CompositeImmutableSegment and those are its segments:: ");
    for (ImmutableSegment s : segments) {
      sb.append(s.toString());
    }
    return sb.toString();
  }
}
