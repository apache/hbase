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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.client.Scan;

/**
 * A mutable segment in memstore, specifically the active segment.
 */
@InterfaceAudience.Private
public class MutableSegment extends Segment {
  protected MutableSegment(CellSet cellSet, CellComparator comparator, MemStoreLAB memStoreLAB,
      long size) {
    super(cellSet, comparator, memStoreLAB, size, ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY);
  }

  /**
   * Adds the given cell into the segment
   * @param cell the cell to add
   * @param mslabUsed whether using MSLAB
   * @return the change in the heap size
   */
  public long add(Cell cell, boolean mslabUsed) {
    return internalAdd(cell, mslabUsed);
  }

  //methods for test

  /**
   * Returns the first cell in the segment
   * @return the first cell in the segment
   */
  Cell first() {
    return this.getCellSet().first();
  }

  @Override
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return (getTimeRangeTracker().includesTimeRange(scan.getTimeRange())
        && (getTimeRangeTracker().getMax() >= oldestUnexpiredTS));
  }

  @Override
  public long getMinTimestamp() {
    return getTimeRangeTracker().getMin();
  }

  @Override
  public long keySize() {
    return size.get() - CompactingMemStore.DEEP_OVERHEAD_PER_PIPELINE_SKIPLIST_ITEM;
  }
}
