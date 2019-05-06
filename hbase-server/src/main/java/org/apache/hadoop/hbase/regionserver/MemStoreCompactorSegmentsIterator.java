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
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * The MemStoreCompactorSegmentsIterator extends MemStoreSegmentsIterator
 * and performs the scan for compaction operation meaning it is based on SQM
 */
@InterfaceAudience.Private
public class MemStoreCompactorSegmentsIterator extends MemStoreSegmentsIterator {
  private static final Logger LOG =
      LoggerFactory.getLogger(MemStoreCompactorSegmentsIterator.class);

  private final List<Cell> kvs = new ArrayList<>();
  private boolean hasMore = true;
  private Iterator<Cell> kvsIterator;

  // scanner on top of pipeline scanner that uses ScanQueryMatcher
  private InternalScanner compactingScanner;

  // C-tor
  public MemStoreCompactorSegmentsIterator(List<ImmutableSegment> segments,
      CellComparator comparator, int compactionKVMax, HStore store) throws IOException {
    super(compactionKVMax);

    List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
    AbstractMemStore.addToScanners(segments, Long.MAX_VALUE, scanners);
    // build the scanner based on Query Matcher
    // reinitialize the compacting scanner for each instance of iterator
    compactingScanner = createScanner(store, scanners);
    refillKVS();
  }

  @Override
  public boolean hasNext() {
    if (kvsIterator == null) { // for the case when the result is empty
      return false;
    }
    // return true either we have cells in buffer or we can get more.
    return kvsIterator.hasNext() || refillKVS();
  }

  @Override
  public Cell next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return kvsIterator.next();
  }

  @Override
  public void close() {
    try {
      compactingScanner.close();
    } catch (IOException e) {
      LOG.warn("close store scanner failed", e);
    }
    compactingScanner = null;
    kvs.clear();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Creates the scanner for compacting the pipeline.
   * @return the scanner
   */
  private InternalScanner createScanner(HStore store, List<KeyValueScanner> scanners)
      throws IOException {
    InternalScanner scanner = null;
    boolean success = false;
    try {
      RegionCoprocessorHost cpHost = store.getCoprocessorHost();
      ScanInfo scanInfo;
      if (cpHost != null) {
        scanInfo = cpHost.preMemStoreCompactionCompactScannerOpen(store);
      } else {
        scanInfo = store.getScanInfo();
      }
      scanner = new StoreScanner(store, scanInfo, scanners, ScanType.COMPACT_RETAIN_DELETES,
          store.getSmallestReadPoint(), HConstants.OLDEST_TIMESTAMP);
      if (cpHost != null) {
        InternalScanner scannerFromCp = cpHost.preMemStoreCompactionCompact(store, scanner);
        if (scannerFromCp == null) {
          throw new CoprocessorException("Got a null InternalScanner when calling" +
              " preMemStoreCompactionCompact which is not acceptable");
        }
        success = true;
        return scannerFromCp;
      } else {
        success = true;
        return scanner;
      }
    } finally {
      if (!success) {
        Closeables.close(scanner, true);
        scanners.forEach(KeyValueScanner::close);
      }
    }
  }

  /*
   * Refill kev-value set (should be invoked only when KVS is empty) Returns true if KVS is
   * non-empty
   */
  private boolean refillKVS() {
    // if there is nothing expected next in compactingScanner
    if (!hasMore) {
      return false;
    }
    // clear previous KVS, first initiated in the constructor
    kvs.clear();
    for (;;) {
      try {
        hasMore = compactingScanner.next(kvs, scannerContext);
      } catch (IOException e) {
        // should not happen as all data are in memory
        throw new IllegalStateException(e);
      }
      if (!kvs.isEmpty()) {
        kvsIterator = kvs.iterator();
        return true;
      } else if (!hasMore) {
        return false;
      }
    }
  }
}
