/**
 * Copyright 2010 The Apache Software Foundation
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
import java.util.List;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Scanner scans both the memstore and the HStore. Coalesce KeyValue stream
 * into List<KeyValue> for a single row.
 */
class StoreScanner extends NonLazyKeyValueScanner
    implements KeyValueScanner, InternalScanner, ChangedReadersObserver {
  static final Log LOG = LogFactory.getLog(StoreScanner.class);
  private Store store;
  private ScanQueryMatcher matcher;
  private KeyValueHeap heap;
  private boolean cacheBlocks;

  private String metricNameGetSize;

  // Used to indicate that the scanner has closed (see HBASE-1107)
  // Doesnt need to be volatile because it's always accessed via synchronized methods
  private boolean closing = false;
  private final boolean isGet;
  private final boolean explicitColumnQuery;
  private final boolean useRowColBloom;

  /** We don't ever expect to change this, the constant is just for clarity. */
  static final boolean LAZY_SEEK_ENABLED_BY_DEFAULT = true;

  /** Used during unit testing to ensure that lazy seek does save seek ops */
  private static boolean lazySeekEnabledGlobally =
      LAZY_SEEK_ENABLED_BY_DEFAULT;

  // if heap == null and lastTop != null, you need to reseek given the key below
  private KeyValue lastTop = null;

  /** An internal constructor. */
  private StoreScanner(Store store, boolean cacheBlocks, Scan scan,
      final NavigableSet<byte[]> columns){
    this.store = store;
    this.cacheBlocks = cacheBlocks;
    isGet = scan.isGetScan();
    int numCol = columns == null ? 0 : columns.size();
    explicitColumnQuery = numCol > 0;

    // We look up row-column Bloom filters for multi-column queries as part of
    // the seek operation. However, we also look the row-column Bloom filter
    // for multi-row (non-"get") scans because this is not done in
    // StoreFile.passesBloomFilter(Scan, SortedSet<byte[]>).
    useRowColBloom = numCol > 1 || (!isGet && numCol == 1);
  }

  /**
   * Opens a scanner across memstore, snapshot, and all StoreFiles.
   *
   * @param store who we scan
   * @param scan the spec
   * @param columns which columns we are scanning
   * @throws IOException
   */
  StoreScanner(Store store, Scan scan, final NavigableSet<byte[]> columns)
                              throws IOException {
    this(store, scan.getCacheBlocks(), scan, columns);
    initializeMetricNames();
    if (columns != null && scan.isRaw()) {
      throw new DoNotRetryIOException(
          "Cannot specify any column for a raw scan");
    }
    matcher = new ScanQueryMatcher(scan, store.scanInfo, columns,
        ScanType.USER_SCAN, Long.MAX_VALUE, HConstants.LATEST_TIMESTAMP);

    // Pass columns to try to filter out unnecessary StoreFiles.
    List<KeyValueScanner> scanners = getScanners(scan, columns);

    // Seek all scanners to the start of the Row (or if the exact matching row
    // key does not exist, then to the start of the next matching Row).
    // Always check bloom filter to optimize the top row seek for delete
    // family marker.
    if (explicitColumnQuery && lazySeekEnabledGlobally) {
      for (KeyValueScanner scanner : scanners) {
        scanner.requestSeek(matcher.getStartKey(), false, true);
      }
    } else {
      for (KeyValueScanner scanner : scanners) {
        scanner.seek(matcher.getStartKey());
      }
    }

    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(scanners, store.comparator);

    this.store.addChangedReaderObserver(this);
  }

  /**
   * Used for major compactions.<p>
   *
   * Opens a scanner across specified StoreFiles.
   * @param store who we scan
   * @param scan the spec
   * @param scanners ancilliary scanners
   * @param smallestReadPoint the readPoint that we should use for tracking versions
   * @param retainDeletesInOutput should we retain deletes after compaction?
   */
  StoreScanner(Store store, Scan scan,
      List<? extends KeyValueScanner> scanners, ScanType scanType,
      long smallestReadPoint, long earliestPutTs) throws IOException {
    this(store, false, scan, null);
    initializeMetricNames();
    matcher = new ScanQueryMatcher(scan, store.scanInfo, null, scanType,
        smallestReadPoint, earliestPutTs);

    // Seek all scanners to the initial key
    for(KeyValueScanner scanner : scanners) {
      scanner.seek(matcher.getStartKey());
    }

    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(scanners, store.comparator);
  }

  // Constructor for testing.
  StoreScanner(final Scan scan, Store.ScanInfo scanInfo,
      StoreScanner.ScanType scanType, final NavigableSet<byte[]> columns,
      final List<KeyValueScanner> scanners) throws IOException {
    this(null, scan.getCacheBlocks(), scan, columns);
    this.initializeMetricNames();
    this.matcher = new ScanQueryMatcher(scan, scanInfo, columns, scanType,
        Long.MAX_VALUE, HConstants.LATEST_TIMESTAMP);

    // Seek all scanners to the initial key
    for (KeyValueScanner scanner : scanners) {
      scanner.seek(matcher.getStartKey());
    }
    heap = new KeyValueHeap(scanners, scanInfo.getComparator());
  }

  /**
   * Method used internally to initialize metric names throughout the
   * constructors.
   * 
   * To be called after the store variable has been initialized!
   */
  private void initializeMetricNames() {
    String tableName = SchemaMetrics.UNKNOWN;
    String family = SchemaMetrics.UNKNOWN;
    if (store != null) {
      tableName = store.getTableName();
      family = Bytes.toString(store.getFamily().getName());
    }
    metricNameGetSize = SchemaMetrics.generateSchemaMetricsPrefix(
        tableName, family) + "getsize";
  }

  /*
   * @return List of scanners ordered properly.
   */
  private List<KeyValueScanner> getScanners() throws IOException {
    return this.store.getScanners(cacheBlocks, isGet, false, null);
  }

  /*
   * @return List of scanners to seek, possibly filtered by StoreFile.
   */
  private List<KeyValueScanner> getScanners(Scan scan,
      final NavigableSet<byte[]> columns) throws IOException {
    boolean memOnly;
    boolean filesOnly;
    if (scan instanceof InternalScan) {
      InternalScan iscan = (InternalScan)scan;
      memOnly = iscan.isCheckOnlyMemStore();
      filesOnly = iscan.isCheckOnlyStoreFiles();
    } else {
      memOnly = false;
      filesOnly = false;
    }
    List<KeyValueScanner> allStoreScanners =
      this.store.getScanners(cacheBlocks, isGet, false, this.matcher);

    List<KeyValueScanner> scanners =
      new ArrayList<KeyValueScanner>(allStoreScanners.size());

    // include only those scan files which pass all filters
    for (KeyValueScanner kvs : allStoreScanners) {
      if (kvs instanceof StoreFileScanner) {
        if (memOnly == false && ((StoreFileScanner)kvs).shouldSeek(scan, columns)) {
          scanners.add(kvs);
        }
      }
      else {
        // kvs is a MemStoreScanner
        if (filesOnly == false && this.store.memstore.shouldSeek(scan)) {
          scanners.add(kvs);
        }
      }
    }

    return scanners;
  }

  @Override
  public synchronized KeyValue peek() {
    if (this.heap == null) {
      return this.lastTop;
    }
    return this.heap.peek();
  }

  @Override
  public KeyValue next() {
    // throw runtime exception perhaps?
    throw new RuntimeException("Never call StoreScanner.next()");
  }

  @Override
  public synchronized void close() {
    if (this.closing) return;
    this.closing = true;
    // under test, we dont have a this.store
    if (this.store != null)
      this.store.deleteChangedReaderObserver(this);
    if (this.heap != null)
      this.heap.close();
    this.heap = null; // CLOSED!
    this.lastTop = null; // If both are null, we are closed.
  }

  @Override
  public synchronized boolean seek(KeyValue key) throws IOException {
    if (this.heap == null) {

      List<KeyValueScanner> scanners = getScanners();

      heap = new KeyValueHeap(scanners, store.comparator);
    }

    return this.heap.seek(key);
  }

  /**
   * Get the next row of values from this Store.
   * @param outResult
   * @param limit
   * @return true if there are more rows, false if scanner is done
   */
  @Override
  public synchronized boolean next(List<KeyValue> outResult, int limit) throws IOException {

    checkReseek();

    // if the heap was left null, then the scanners had previously run out anyways, close and
    // return.
    if (this.heap == null) {
      close();
      return false;
    }

    KeyValue peeked = this.heap.peek();
    if (peeked == null) {
      close();
      return false;
    }

    // only call setRow if the row changes; avoids confusing the query matcher
    // if scanning intra-row
    if ((matcher.row == null) || !peeked.matchingRow(matcher.row)) {
      matcher.setRow(peeked.getRow());
    }

    KeyValue kv;
    KeyValue prevKV = null;
    List<KeyValue> results = new ArrayList<KeyValue>();

    // Only do a sanity-check if store and comparator are available.
    KeyValue.KVComparator comparator =
        store != null ? store.getComparator() : null;

    LOOP: while((kv = this.heap.peek()) != null) {
      // Check that the heap gives us KVs in an increasing order.
      if (prevKV != null && comparator != null
          && comparator.compare(prevKV, kv) > 0) {
        throw new IOException("Key " + prevKV + " followed by a " +
            "smaller key " + kv + " in cf " + store);
      }
      prevKV = kv;
      ScanQueryMatcher.MatchCode qcode = matcher.match(kv);
      switch(qcode) {
        case INCLUDE:
        case INCLUDE_AND_SEEK_NEXT_ROW:
        case INCLUDE_AND_SEEK_NEXT_COL:

          Filter f = matcher.getFilter();
          results.add(f == null ? kv : f.transform(kv));

          if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
            if (!matcher.moreRowsMayExistAfter(kv)) {
              outResult.addAll(results);
              return false;
            }
            reseek(matcher.getKeyForNextRow(kv));
          } else if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
            reseek(matcher.getKeyForNextColumn(kv));
          } else {
            this.heap.next();
          }

          HRegion.incrNumericMetric(metricNameGetSize, kv.getLength());
          if (limit > 0 && (results.size() == limit)) {
            break LOOP;
          }
          continue;

        case DONE:
          // copy jazz
          outResult.addAll(results);
          return true;

        case DONE_SCAN:
          close();

          // copy jazz
          outResult.addAll(results);

          return false;

        case SEEK_NEXT_ROW:
          // This is just a relatively simple end of scan fix, to short-cut end
          // us if there is an endKey in the scan.
          if (!matcher.moreRowsMayExistAfter(kv)) {
            outResult.addAll(results);
            return false;
          }

          reseek(matcher.getKeyForNextRow(kv));
          break;

        case SEEK_NEXT_COL:
          reseek(matcher.getKeyForNextColumn(kv));
          break;

        case SKIP:
          this.heap.next();
          break;

        case SEEK_NEXT_USING_HINT:
          KeyValue nextKV = matcher.getNextKeyHint(kv);
          if (nextKV != null) {
            reseek(nextKV);
          } else {
            heap.next();
          }
          break;

        default:
          throw new RuntimeException("UNEXPECTED");
      }
    }

    if (!results.isEmpty()) {
      // copy jazz
      outResult.addAll(results);
      return true;
    }

    // No more keys
    close();
    return false;
  }

  @Override
  public synchronized boolean next(List<KeyValue> outResult) throws IOException {
    return next(outResult, -1);
  }

  // Implementation of ChangedReadersObserver
  @Override
  public synchronized void updateReaders() throws IOException {
    if (this.closing) return;

    // All public synchronized API calls will call 'checkReseek' which will cause
    // the scanner stack to reseek if this.heap==null && this.lastTop != null.
    // But if two calls to updateReaders() happen without a 'next' or 'peek' then we
    // will end up calling this.peek() which would cause a reseek in the middle of a updateReaders
    // which is NOT what we want, not to mention could cause an NPE. So we early out here.
    if (this.heap == null) return;

    // this could be null.
    this.lastTop = this.peek();

    //DebugPrint.println("SS updateReaders, topKey = " + lastTop);

    // close scanners to old obsolete Store files
    this.heap.close(); // bubble thru and close all scanners.
    this.heap = null; // the re-seeks could be slow (access HDFS) free up memory ASAP

    // Let the next() call handle re-creating and seeking
  }

  private void checkReseek() throws IOException {
    if (this.heap == null && this.lastTop != null) {
      resetScannerStack(this.lastTop);
      this.lastTop = null; // gone!
    }
    // else dont need to reseek
  }

  private void resetScannerStack(KeyValue lastTopKey) throws IOException {
    if (heap != null) {
      throw new RuntimeException("StoreScanner.reseek run on an existing heap!");
    }

    /* When we have the scan object, should we not pass it to getScanners()
     * to get a limited set of scanners? We did so in the constructor and we
     * could have done it now by storing the scan object from the constructor */
    List<KeyValueScanner> scanners = getScanners();

    for(KeyValueScanner scanner : scanners) {
      scanner.seek(lastTopKey);
    }

    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(scanners, store.comparator);

    // Reset the state of the Query Matcher and set to top row.
    // Only reset and call setRow if the row changes; avoids confusing the
    // query matcher if scanning intra-row.
    KeyValue kv = heap.peek();
    if (kv == null) {
      kv = lastTopKey;
    }
    if ((matcher.row == null) || !kv.matchingRow(matcher.row)) {
      matcher.reset();
      matcher.setRow(kv.getRow());
    }
  }

  @Override
  public synchronized boolean reseek(KeyValue kv) throws IOException {
    //Heap cannot be null, because this is only called from next() which
    //guarantees that heap will never be null before this call.
    if (explicitColumnQuery && lazySeekEnabledGlobally) {
      return heap.requestSeek(kv, true, useRowColBloom);
    } else {
      return heap.reseek(kv);
    }
  }

  @Override
  public long getSequenceID() {
    return 0;
  }

  /**
   * Used in testing.
   * @return all scanners in no particular order
   */
  List<KeyValueScanner> getAllScannersForTesting() {
    List<KeyValueScanner> allScanners = new ArrayList<KeyValueScanner>();
    KeyValueScanner current = heap.getCurrentForTesting();
    if (current != null)
      allScanners.add(current);
    for (KeyValueScanner scanner : heap.getHeap())
      allScanners.add(scanner);
    return allScanners;
  }

  static void enableLazySeekGlobally(boolean enable) {
    lazySeekEnabledGlobally = enable;
  }

  /**
   * Enum to distinguish general scan types.
   */
  public static enum ScanType {
    MAJOR_COMPACT,
    MINOR_COMPACT,
    USER_SCAN
  }
}

