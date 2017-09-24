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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The MemStoreCompactorSegmentsIterator extends MemStoreSegmentsIterator
 * and performs the scan for compaction operation meaning it is based on SQM
 */
@InterfaceAudience.Private
public class MemStoreCompactorSegmentsIterator extends MemStoreSegmentsIterator {

  private List<Cell> kvs = new ArrayList<>();
  private boolean hasMore;
  private Iterator<Cell> kvsIterator;

  // scanner on top of pipeline scanner that uses ScanQueryMatcher
  private StoreScanner compactingScanner;

  // C-tor
  public MemStoreCompactorSegmentsIterator(List<ImmutableSegment> segments,
      CellComparator comparator, int compactionKVMax, HStore store) throws IOException {
    super(compactionKVMax);

    List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
    // create the list of scanners to traverse over all the data
    // no dirty reads here as these are immutable segments
    int order = segments.size();
    AbstractMemStore.addToScanners(segments, Integer.MAX_VALUE, order, scanners);
    // build the scanner based on Query Matcher
    // reinitialize the compacting scanner for each instance of iterator
    compactingScanner = createScanner(store, scanners);

    hasMore = compactingScanner.next(kvs, scannerContext);

    if (!kvs.isEmpty()) {
      kvsIterator = kvs.iterator();
    }

  }

  @Override
  public boolean hasNext() {
    if (kvsIterator == null)  { // for the case when the result is empty
      return false;
    }
    if (!kvsIterator.hasNext()) {
      // refillKVS() method should be invoked only if !kvsIterator.hasNext()
      if (!refillKVS()) {
        return false;
      }
    }
    return kvsIterator.hasNext();
  }

  @Override
  public Cell next()  {
    if (kvsIterator == null)  { // for the case when the result is empty
      return null;
    }
    if (!kvsIterator.hasNext()) {
      // refillKVS() method should be invoked only if !kvsIterator.hasNext()
      if (!refillKVS())  return null;
    }
    return (!hasMore) ? null : kvsIterator.next();
  }

  public void close() {
    compactingScanner.close();
    compactingScanner = null;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Creates the scanner for compacting the pipeline.
   * @return the scanner
   */
  private StoreScanner createScanner(HStore store, List<KeyValueScanner> scanners)
      throws IOException {
    // Get all available versions
    return new StoreScanner(store, store.getScanInfo(), OptionalInt.of(Integer.MAX_VALUE), scanners,
        ScanType.COMPACT_RETAIN_DELETES, store.getSmallestReadPoint(), HConstants.OLDEST_TIMESTAMP);
  }

  /* Refill kev-value set (should be invoked only when KVS is empty)
   * Returns true if KVS is non-empty */
  private boolean refillKVS() {
    kvs.clear();          // clear previous KVS, first initiated in the constructor
    if (!hasMore) {       // if there is nothing expected next in compactingScanner
      return false;
    }

    try {                 // try to get next KVS
      hasMore = compactingScanner.next(kvs, scannerContext);
    } catch (IOException ie) {
      throw new IllegalStateException(ie);
    }

    if (!kvs.isEmpty() ) {// is the new KVS empty ?
      kvsIterator = kvs.iterator();
      return true;
    } else {
      // KVS is empty, but hasMore still true?
      if (hasMore) {      // try to move to next row
        return refillKVS();
      }

    }
    return hasMore;
  }
}
