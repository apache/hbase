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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;

/**
 * The CompositeImmutableSegments is created as a collection of ImmutableSegments and supports
 * the interface of a single ImmutableSegments.
 * The CompositeImmutableSegments is planned to be used only as a snapshot,
 * thus only relevant interfaces are supported
 */
@InterfaceAudience.Private
public class CompositeImmutableSegment extends ImmutableSegment {

  private final List<ImmutableSegment> segments;
  private long keySize = 0;

  public CompositeImmutableSegment(CellComparator comparator, List<ImmutableSegment> segments) {
    super(comparator, segments);
    this.segments = segments;
    for (ImmutableSegment s : segments) {
      this.timeRangeTracker.includeTimestamp(s.getTimeRangeTracker().getMax());
      this.timeRangeTracker.includeTimestamp(s.getTimeRangeTracker().getMin());
      this.keySize += s.getDataSize();
    }
  }

  @Override
  public List<Segment> getAllSegments() {
    return new ArrayList<>(segments);
  }

  @Override
  public int getNumOfSegments() {
    return segments.size();
  }

  /**
   * @return whether the segment has any cells
   */
  @Override
  public boolean isEmpty() {
    for (ImmutableSegment s : segments) {
      if (!s.isEmpty()) return false;
    }
    return true;
  }

  /**
   * @return number of cells in segment
   */
  @Override
  public int getCellsCount() {
    int result = 0;
    for (ImmutableSegment s : segments) {
      result += s.getCellsCount();
    }
    return result;
  }

  /**
   * Closing a segment before it is being discarded
   */
  @Override
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
  @Override
  public Cell maybeCloneWithAllocator(Cell cell, boolean forceCloneOfBigCell) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  @Override
  public boolean shouldSeek(TimeRange tr, long oldestUnexpiredTS){
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  /**
   * Creates the scanner for the given read point
   * @return a scanner for the given read point
   */
  @Override
  public KeyValueScanner getScanner(long readPoint) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }


  @Override
  public List<KeyValueScanner> getScanners(long readPoint) {
    List<KeyValueScanner> list = new ArrayList<>(segments.size());
    AbstractMemStore.addToScanners(segments, readPoint, list);
    return list;
  }

  @Override
  public boolean isTagsPresent() {
    for (ImmutableSegment s : segments) {
      if (s.isTagsPresent()) return true;
    }
    return false;
  }

  @Override
  public void incScannerCount() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  @Override
  public void decScannerCount() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  /**
   * Setting the CellSet of the segment - used only for flat immutable segment for setting
   * immutable CellSet after its creation in immutable segment constructor
   * @return this object
   */
  @Override
  protected CompositeImmutableSegment setCellSet(CellSet cellSetOld, CellSet cellSetNew) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }


  @Override
  protected long indexEntrySize() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  @Override protected boolean canBeFlattened() {
    return false;
  }

  /**
   * @return Sum of all cell sizes.
   */
  @Override
  public long getDataSize() {
    return this.keySize;
  }

  /**
   * @return The heap size of this segment.
   */
  @Override
  public long getHeapSize() {
    long result = 0;
    for (ImmutableSegment s : segments) {
      result += s.getHeapSize();
    }
    return result;
  }

  /**
   * Updates the heap size counter of the segment by the given delta
   */
  @Override
  public long incMemStoreSize(long delta, long heapOverhead, long offHeapOverhead, int cellsCount) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  @Override
  public long getMinSequenceId() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  @Override
  public TimeRangeTracker getTimeRangeTracker() {
    return this.timeRangeTracker;
  }

  //*** Methods for SegmentsScanner
  @Override
  public Cell last() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  @Override
  public Iterator<Cell> iterator() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  @Override
  public SortedSet<Cell> headSet(Cell firstKeyOnRow) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  @Override
  public int compare(Cell left, Cell right) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  @Override
  public int compareRows(Cell left, Cell right) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  /**
   * @return a set of all cells in the segment
   */
  @Override
  protected CellSet getCellSet() {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  @Override
  protected void internalAdd(Cell cell, boolean mslabUsed, MemStoreSizing memstoreSizing,
      boolean sizeAddedPreOperation) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  @Override
  protected void updateMetaInfo(Cell cellToAdd, boolean succ, boolean mslabUsed,
      MemStoreSizing memstoreSizing, boolean sizeAddedPreOperation) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  /**
   * Returns a subset of the segment cell set, which starts with the given cell
   * @param firstCell a cell in the segment
   * @return a subset of the segment cell set, which starts with the given cell
   */
  @Override
  protected SortedSet<Cell> tailSet(Cell firstCell) {
    throw new IllegalStateException("Not supported by CompositeImmutableScanner");
  }

  // Debug methods
  /**
   * Dumps all cells of the segment into the given log
   */
  @Override
  void dump(Logger log) {
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

  @Override
  List<KeyValueScanner> getSnapshotScanners() {
    List<KeyValueScanner> list = new ArrayList<>(this.segments.size());
    for (ImmutableSegment segment: this.segments) {
      list.add(new SnapshotSegmentScanner(segment));
    }
    return list;
  }
}
