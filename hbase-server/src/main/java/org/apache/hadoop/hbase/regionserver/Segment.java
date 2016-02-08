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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.Cell;
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

  private volatile MemStoreLAB memStoreLAB;
  private final AtomicLong size;
  private final TimeRangeTracker timeRangeTracker;
  protected volatile boolean tagsPresent;

  protected Segment(MemStoreLAB memStoreLAB, long size) {
    this.memStoreLAB = memStoreLAB;
    this.size = new AtomicLong(size);
    this.timeRangeTracker = new TimeRangeTracker();
    this.tagsPresent = false;
  }

  protected Segment(Segment segment) {
    this.memStoreLAB = segment.getMemStoreLAB();
    this.size = new AtomicLong(segment.getSize());
    this.timeRangeTracker = segment.getTimeRangeTracker();
    this.tagsPresent = segment.isTagsPresent();
  }

  /**
   * Creates the scanner that is able to scan the concrete segment
   * @return a scanner for the given read point
   */
  public abstract SegmentScanner getSegmentScanner(long readPoint);

  /**
   * Returns whether the segment has any cells
   * @return whether the segment has any cells
   */
  public abstract boolean isEmpty();

  /**
   * Returns number of cells in segment
   * @return number of cells in segment
   */
  public abstract int getCellsCount();

  /**
   * Adds the given cell into the segment
   * @return the change in the heap size
   */
  public abstract long add(Cell cell);

  /**
   * Removes the given cell from the segment
   * @return the change in the heap size
   */
  public abstract long rollback(Cell cell);

  /**
   * Returns the first cell in the segment that has equal or greater key than the given cell
   * @return the first cell in the segment that has equal or greater key than the given cell
   */
  public abstract Cell getFirstAfter(Cell cell);

  /**
   * Returns a set of all cells in the segment
   * @return a set of all cells in the segment
   */
  public abstract CellSet getCellSet();

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

  protected void updateMetaInfo(Cell toAdd, long s) {
    getTimeRangeTracker().includeTimestamp(toAdd);
    size.addAndGet(s);
  }

  private MemStoreLAB getMemStoreLAB() {
    return memStoreLAB;
  }

  // Debug methods
  /**
   * Dumps all cells of the segment into the given log
   */
  public abstract void dump(Log log);

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
