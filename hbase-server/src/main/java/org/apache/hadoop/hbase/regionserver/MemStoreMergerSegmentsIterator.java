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
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The MemStoreMergerSegmentsIterator extends MemStoreSegmentsIterator
 * and performs the scan for simple merge operation meaning it is NOT based on SQM
 */
@InterfaceAudience.Private
public class MemStoreMergerSegmentsIterator extends MemStoreSegmentsIterator {

  // heap of scanners, lazily initialized
  private KeyValueHeap heap = null;
  // remember the initial version of the scanners list
  List<KeyValueScanner> scanners  = new ArrayList<KeyValueScanner>();

  private boolean closed = false;

  // C-tor
  public MemStoreMergerSegmentsIterator(List<ImmutableSegment> segments, CellComparator comparator,
      int compactionKVMax) throws IOException {
    super(compactionKVMax);
    // create the list of scanners to traverse over all the data
    // no dirty reads here as these are immutable segments
    AbstractMemStore.addToScanners(segments, Long.MAX_VALUE, scanners);
    heap = new KeyValueHeap(scanners, comparator);
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }
    if (this.heap != null) {
      return (this.heap.peek() != null);
    }
    // Doing this way in case some test cases tries to peek directly
    return false;
  }

  @Override
  public Cell next()  {
    try {                 // try to get next
      if (!closed && heap != null) {
        return heap.next();
      }
    } catch (IOException ie) {
      throw new IllegalStateException(ie);
    }
    return null;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    // Ensuring that all the segment scanners are closed
    if (heap != null) {
      heap.close();
      // It is safe to do close as no new calls will be made to this scanner.
      heap = null;
    } else {
      for (KeyValueScanner scanner : scanners) {
        scanner.close();
      }
    }
    closed = true;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
