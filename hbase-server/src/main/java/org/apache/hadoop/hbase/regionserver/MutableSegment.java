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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.ClassSize;

import com.google.common.annotations.VisibleForTesting;

/**
 * A mutable segment in memstore, specifically the active segment.
 */
@InterfaceAudience.Private
public class MutableSegment extends Segment {

  public final static long DEEP_OVERHEAD = Segment.DEEP_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP;

  protected MutableSegment(CellSet cellSet, CellComparator comparator, MemStoreLAB memStoreLAB) {
    super(cellSet, comparator, memStoreLAB);
  }

  /**
   * Adds the given cell into the segment
   * @param cell the cell to add
   * @param mslabUsed whether using MSLAB
   * @param memstoreSize
   */
  public void add(Cell cell, boolean mslabUsed, MemstoreSize memstoreSize) {
    internalAdd(cell, mslabUsed, memstoreSize);
  }

  public void upsert(Cell cell, long readpoint, MemstoreSize memstoreSize) {
    internalAdd(cell, false, memstoreSize);

    // Get the Cells for the row/family/qualifier regardless of timestamp.
    // For this case we want to clean up any other puts
    Cell firstCell = CellUtil.createFirstOnRowColTS(cell, HConstants.LATEST_TIMESTAMP);
    SortedSet<Cell> ss = this.tailSet(firstCell);
    Iterator<Cell> it = ss.iterator();
    // versions visible to oldest scanner
    int versionsVisible = 0;
    while (it.hasNext()) {
      Cell cur = it.next();

      if (cell == cur) {
        // ignore the one just put in
        continue;
      }
      // check that this is the row and column we are interested in, otherwise bail
      if (CellUtil.matchingRows(cell, cur) && CellUtil.matchingQualifier(cell, cur)) {
        // only remove Puts that concurrent scanners cannot possibly see
        if (cur.getTypeByte() == KeyValue.Type.Put.getCode() && cur.getSequenceId() <= readpoint) {
          if (versionsVisible >= 1) {
            // if we get here we have seen at least one version visible to the oldest scanner,
            // which means we can prove that no scanner will see this version

            // false means there was a change, so give us the size.
            // TODO when the removed cell ie.'cur' having its data in MSLAB, we can not release that
            // area. Only the Cell object as such going way. We need to consider cellLen to be
            // decreased there as 0 only. Just keeping it as existing code now. We need to know the
            // removed cell is from MSLAB or not. Will do once HBASE-16438 is in
            int cellLen = getCellLength(cur);
            long heapOverheadDelta = heapOverheadChange(cur, true);
            this.incSize(-cellLen, -heapOverheadDelta);
            if (memstoreSize != null) {
              memstoreSize.decMemstoreSize(cellLen, heapOverheadDelta);
            }
            it.remove();
          } else {
            versionsVisible++;
          }
        }
      } else {
        // past the row or column, done
        break;
      }
    }
  }

  /**
   * Returns the first cell in the segment
   * @return the first cell in the segment
   */
  @VisibleForTesting
  Cell first() {
    return this.getCellSet().first();
  }

  @Override
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return (this.timeRangeTracker.includesTimeRange(scan.getTimeRange())
        && (this.timeRangeTracker.getMax() >= oldestUnexpiredTS));
  }

  @Override
  public long getMinTimestamp() {
    return this.timeRangeTracker.getMin();
  }
}
