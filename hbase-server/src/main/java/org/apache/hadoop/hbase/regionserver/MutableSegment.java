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
import org.apache.hadoop.hbase.client.Scan;

/**
 * A mutable segment in memstore, specifically the active segment.
 */
@InterfaceAudience.Private
public class MutableSegment extends Segment {
  protected MutableSegment(CellSet cellSet, CellComparator comparator, MemStoreLAB memStoreLAB,
      long size) {
    super(cellSet, comparator, memStoreLAB, size);
  }

  /**
   * Adds the given cell into the segment
   * @return the change in the heap size
   */
  public long add(Cell cell) {
    return internalAdd(cell);
  }

  /**
   * Removes the given cell from the segment
   * @return the change in the heap size
   */
  public long rollback(Cell cell) {
    Cell found = getCellSet().get(cell);
    if (found != null && found.getSequenceId() == cell.getSequenceId()) {
      long sz = AbstractMemStore.heapSizeChange(cell, true);
      getCellSet().remove(cell);
      incSize(-sz);
      return sz;
    }
    return 0;
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
  protected void updateMetaInfo(Cell toAdd, long s) {
    getTimeRangeTracker().includeTimestamp(toAdd);
    size.addAndGet(s);
    // In no tags case this NoTagsKeyValue.getTagsLength() is a cheap call.
    // When we use ACL CP or Visibility CP which deals with Tags during
    // mutation, the TagRewriteCell.getTagsLength() is a cheaper call. We do not
    // parse the byte[] to identify the tags length.
    if(toAdd.getTagsLength() > 0) {
      tagsPresent = true;
    }
  }
}