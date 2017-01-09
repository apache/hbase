/**
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
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Scans the snapshot. Acts as a simple scanner that just iterates over all the cells
 * in the segment
 */
@InterfaceAudience.Private
public class SnapshotScanner extends SegmentScanner {

  public SnapshotScanner(Segment immutableSegment) {
    // Snapshot scanner does not need readpoint. It should read all the cells in the
    // segment
    super(immutableSegment, Long.MAX_VALUE);
  }

  @Override
  public Cell peek() { // sanity check, the current should be always valid
    if (closed) {
      return null;
    }
    return current;
  }

  @Override
  public boolean shouldUseScanner(Scan scan, Store store, long oldestUnexpiredTS) {
    return true;
  }

  @Override
  public boolean backwardSeek(Cell key) throws IOException {
    throw new NotImplementedException(
        "backwardSeek must not be called on a " + "non-reversed scanner");
  }

  @Override
  public boolean seekToPreviousRow(Cell key) throws IOException {
    throw new NotImplementedException(
        "seekToPreviousRow must not be called on a " + "non-reversed scanner");
  }

  @Override
  public boolean seekToLastRow() throws IOException {
    throw new NotImplementedException(
        "seekToLastRow must not be called on a " + "non-reversed scanner");
  }

  @Override
  protected Iterator<Cell> getIterator(Cell cell) {
    return segment.iterator();
  }

  @Override
  protected void updateCurrent() {
    if (iter.hasNext()) {
      current = iter.next();
    } else {
      current = null;
    }
  }

  @Override
  public boolean seek(Cell seekCell) {
    // restart iterator
    iter = getIterator(seekCell);
    return reseek(seekCell);
  }

  @Override
  public boolean reseek(Cell seekCell) {
    while (iter.hasNext()) {
      Cell next = iter.next();
      int ret = segment.getComparator().compare(next, seekCell);
      if (ret >= 0) {
        current = next;
        return true;
      }
    }
    return false;
  }
}
