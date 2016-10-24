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
import java.util.List;

/**
 * The MemStoreMergerSegmentsIterator extends MemStoreSegmentsIterator
 * and performs the scan for simple merge operation meaning it is NOT based on SQM
 */
@InterfaceAudience.Private
public class MemStoreMergerSegmentsIterator extends MemStoreSegmentsIterator {

  // C-tor
  public MemStoreMergerSegmentsIterator(List<ImmutableSegment> segments, CellComparator comparator,
      int compactionKVMax, Store store
  ) throws IOException {
    super(segments,comparator,compactionKVMax,store);
  }

  @Override
  public boolean hasNext() {
    return (scanner.peek()!=null);
  }

  @Override
  public Cell next()  {
    Cell result = null;
    try {                 // try to get next
      result = scanner.next();
    } catch (IOException ie) {
      throw new IllegalStateException(ie);
    }
    return result;
  }

  public void close() {
    scanner.close();
    scanner = null;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}