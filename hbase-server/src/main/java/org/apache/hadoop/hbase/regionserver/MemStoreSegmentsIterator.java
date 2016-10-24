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

import java.io.IOException;
import java.util.*;

/**
 * The MemStoreSegmentsIterator is designed to perform one iteration over given list of segments
 * For another iteration new instance of MemStoreSegmentsIterator needs to be created
 * The iterator is not thread-safe and must have only one instance per MemStore
 * in each period of time
 */
@InterfaceAudience.Private
public abstract class MemStoreSegmentsIterator implements Iterator<Cell> {

  // scanner for full or partial pipeline (heap of segment scanners)
  // we need to keep those scanners in order to close them at the end
  protected KeyValueScanner scanner;

  protected final ScannerContext scannerContext;


  // C-tor
  public MemStoreSegmentsIterator(List<ImmutableSegment> segments, CellComparator comparator,
      int compactionKVMax, Store store) throws IOException {

    this.scannerContext = ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

    // list of Scanners of segments in the pipeline, when compaction starts
    List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();

    // create the list of scanners with the smallest read point, meaning that
    // only relevant KVs are going to be returned by the pipeline traversing
    for (Segment segment : segments) {
      scanners.add(segment.getScanner(store.getSmallestReadPoint()));
    }

    scanner = new MemStoreScanner(comparator, scanners, true);
  }

  public abstract void close();
}