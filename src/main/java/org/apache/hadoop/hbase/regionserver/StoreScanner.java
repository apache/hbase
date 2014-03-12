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
import java.util.ListIterator;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueContext;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HBaseServer.Call;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;
import org.apache.hadoop.hbase.regionserver.kvaggregator.KeyValueAggregator;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;


/**
 * Scanner scans both the memstore and the HStore. Coalesce KeyValue stream
 * into List<KeyValue> for a single row.
 */
public class StoreScanner extends NonLazyKeyValueScanner
    implements KeyValueScanner, InternalScanner, ChangedReadersObserver {
  static final Log LOG = LogFactory.getLog(StoreScanner.class);
  private Store store;
  private ScanQueryMatcher matcher;
  private KeyValueHeap heap;
  private boolean cacheBlocks;
  private int countPerRow = 0;
  private int storeLimit = -1;
  private int storeOffset = 0;
  private String metricNamePrefix;
  // Used to indicate that the scanner has closed (see HBASE-1107)
  // Doesnt need to be volatile because it's always accessed via synchronized methods
  private boolean closing = false;
  private final boolean isGet;
  private final boolean explicitColumnQuery;
  private final boolean useRowColBloom;
  private final Scan scan;
  private final KeyValueAggregator keyValueAggregator;
  private final NavigableSet<byte[]> columns;
  private final long oldestUnexpiredTS;
  /**
   * Whether the deleteColBloomFilter is enabled for usage
   */
  private boolean isUsingDeleteColBloom = false;

  /** We don't ever expect to change this, the constant is just for clarity. */
  static final boolean LAZY_SEEK_ENABLED_BY_DEFAULT = true;

  /** Used during unit testing to ensure that lazy seek does save seek ops */
  private static boolean lazySeekEnabledGlobally =
      LAZY_SEEK_ENABLED_BY_DEFAULT;

  // if heap == null and lastTop != null, you need to reseek given the key below
  private KeyValue lastTop = null;

  private StoreScanner(Store store, boolean cacheBlocks, Scan scan,
      final NavigableSet<byte[]> columns, long ttl, KeyValueAggregator keyValueAggregator) {
    this(store, cacheBlocks, scan, columns, ttl, keyValueAggregator, 0);
  }

  /** An internal constructor. */
  private StoreScanner(Store store, boolean cacheBlocks, Scan scan,
      final NavigableSet<byte[]> columns, long ttl,
      KeyValueAggregator keyValueAggregator, long flashBackQueryLimit) {
    this.store = store;
    initializeMetricNames();
    this.cacheBlocks = cacheBlocks;
    isGet = scan.isGetScan();
    int numCol = columns == null ? 0 : columns.size();
    explicitColumnQuery = numCol > 0;
    this.scan = scan;
    this.keyValueAggregator = keyValueAggregator;
    this.columns = columns;
    long currentTime = (scan.getEffectiveTS() == HConstants.LATEST_TIMESTAMP) ? EnvironmentEdgeManager
        .currentTimeMillis() : scan.getEffectiveTS();
    oldestUnexpiredTS = currentTime - ttl
        - flashBackQueryLimit;

    // We look up row-column Bloom filters for multi-column queries as part of
    // the seek operation. However, we also look the row-column Bloom filter
    // for multi-row (non-"get") scans because this is not done in
    // StoreFile.passesBloomFilter(Scan, SortedSet<byte[]>).
    useRowColBloom = numCol > 1 || (!isGet && numCol == 1);
    if (store != null) {
      // check first whether the delete column bloom filter is enabled for write/read
      if (BloomFilterFactory.isDeleteColumnBloomEnabled(store.conf)) {
        this.isUsingDeleteColBloom = store.conf.getBoolean(
            HConstants.USE_DELETE_COLUMN_BLOOM_FILTER_STRING,
            HConstants.USE_DELETE_COLUMN_BLOOM_FILTER);
      }
    }
  }

  /**
   * Opens a scanner across memstore, snapshot, and all StoreFiles. Assumes we
   * are not in a compaction.
   *
   * @param store who we scan
   * @param scan the spec
   * @param columns which columns we are scanning
   * @throws IOException
   */
  public StoreScanner(Store store, Scan scan, final NavigableSet<byte[]> columns,
      KeyValueAggregator keyValueAggregator) throws IOException {
    this(store, scan.getCacheBlocks(), scan, columns, store.ttl, keyValueAggregator);
    matcher =
        new ScanQueryMatcher(scan, store.getFamily().getName(), columns,
            store.comparator.getRawComparator(),
            store.versionsToReturn(scan.getMaxVersions()), Long.MAX_VALUE,
            Long.MAX_VALUE, // do not include the deletes
            oldestUnexpiredTS, isUsingDeleteColBloom);
  }

  public synchronized void initialize() throws IOException {
    // We don't need to guard it with a read lock because
    // getScannersNoCompaction is guarded by a readLock already and guarantees
    // that this happens before getScannersNoCompaction
    this.store.addChangedReaderObserver(this);
    List<KeyValueScanner> scanners = null;
    // Pass columns to try to filter out unnecessary StoreFiles.
    scanners = getScannersNoCompaction();
    // Seek all scanners to the start of the Row (or if the exact matching row
    // key does not exist, then to the start of the next matching Row).
    // Always check bloom filter to optimize the top row seek for delete
    // family marker.
    InjectionHandler.processEvent(
        InjectionEvent.STORESCANNER_COMPACTION_RACE, new Object[] {0});
    if (explicitColumnQuery && lazySeekEnabledGlobally) {
      for (KeyValueScanner scanner : scanners) {
        scanner.requestSeek(matcher.getStartKey(), false, true);
      }
    } else {
      for (KeyValueScanner scanner : scanners) {
        scanner.seek(matcher.getStartKey());
      }
    }

    // set storeLimit
    this.storeLimit = scan.getMaxResultsPerColumnFamily();

    // set rowOffset
    this.storeOffset = scan.getRowOffsetPerColumnFamily();

    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(scanners, store.comparator);
  }

  public static StoreScanner createScanner(Store store, Scan scan,
      final NavigableSet<byte[]> columns, KeyValueAggregator keyValueAggregator)
          throws IOException {
    StoreScanner scanner = new StoreScanner(store, scan, columns,
        keyValueAggregator);
    scanner.initialize();
    InjectionHandler.processEvent(
        InjectionEvent.STORESCANNER_COMPACTION_RACE, new Object[] {1});
    return scanner;
  }

  StoreScanner(Store store, Scan scan,
      List<? extends KeyValueScanner> scanners, long smallestReadPoint,
      long retainDeletesInOutputUntil, KeyValueAggregator keyValueAggregator)
      throws IOException {
    this(store, scan, scanners, smallestReadPoint, retainDeletesInOutputUntil,
        keyValueAggregator, 0);
  }

  /**
   * Opens a scanner across specified StoreFiles. Can be used in compactions.
   * @param store who we scan
   * @param scan the spec
   * @param scanners ancillary scanners
   * @param smallestReadPoint the readPoint that we should use for tracking
   *          versions
   * @param retainDeletesInOutputUntil should we retain deletes after compaction?
   */
  StoreScanner(Store store, Scan scan,
      List<? extends KeyValueScanner> scanners, long smallestReadPoint,
      long retainDeletesInOutputUntil, KeyValueAggregator keyValueAggregator,
      long flashBackQueryLimit) throws IOException {
    this(store, false, scan, null, store.ttl, keyValueAggregator, flashBackQueryLimit);

    // This is the oldest TS upto which flashback queries are supported.
    long oldestFlashBackTS = (flashBackQueryLimit != 0) ? EnvironmentEdgeManager
        .currentTimeMillis() - flashBackQueryLimit
        : HConstants.LATEST_TIMESTAMP;

    matcher =
        new ScanQueryMatcher(scan, store.getFamily().getName(), null,
            store.comparator.getRawComparator(),
            store.versionsToReturn(scan.getMaxVersions()), smallestReadPoint,
            retainDeletesInOutputUntil, oldestUnexpiredTS, oldestFlashBackTS, isUsingDeleteColBloom);

    // Filter the list of scanners using Bloom filters, time range, TTL, etc.
    scanners = selectScannersFrom(scanners);

    // Seek all scanners to the initial key
    for (KeyValueScanner scanner : scanners) {
      scanner.seek(matcher.getStartKey());
    }

    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(scanners, store.comparator);
  }

  /** Constructor for testing. */
  StoreScanner(final Scan scan, final byte [] colFamily, final long ttl,
      final KeyValue.KVComparator comparator,
      final NavigableSet<byte[]> columns,
      final List<KeyValueScanner> scanners,
      final KeyValueAggregator keyValueAggregator)
        throws IOException {
    this(scan, colFamily, ttl, comparator, columns, scanners, Long.MAX_VALUE, keyValueAggregator);
  }

  /** Constructor for testing. */
  StoreScanner(final Scan scan, final byte [] colFamily, final long ttl,
      final KeyValue.KVComparator comparator,
      final NavigableSet<byte[]> columns,
      final List<KeyValueScanner> scanners,
      final long retainDeletesInOutputUntil,
      final KeyValueAggregator keyValueAggregator)
        throws IOException {
    this(null, scan.getCacheBlocks(), scan, columns, ttl, keyValueAggregator);
    this.matcher =
        new ScanQueryMatcher(scan, colFamily, columns,
            comparator.getRawComparator(), scan.getMaxVersions(),
            Long.MAX_VALUE,
            retainDeletesInOutputUntil,
            oldestUnexpiredTS, isUsingDeleteColBloom);

    // Seek all scanners to the initial key
    for(KeyValueScanner scanner : scanners) {
      scanner.seek(matcher.getStartKey());
    }
    heap = new KeyValueHeap(scanners, comparator);
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

    this.metricNamePrefix =
        SchemaMetrics.generateSchemaMetricsPrefix(tableName, family);
  }

  /**
   * Get a filtered list of scanners. Assumes we are not in a compaction.
   * @return list of scanners to seek
   */
  private List<KeyValueScanner> getScannersNoCompaction()
      throws IOException {
    return selectScannersFrom(store.getScanners(cacheBlocks, false,
      scan.isPreloadBlocks(), matcher));
  }

  /**
   * Filters the given list of scanners using Bloom filter, time range, and
   * TTL.
   */
  private List<KeyValueScanner> selectScannersFrom(
      final List<? extends KeyValueScanner> allScanners) {
    List<KeyValueScanner> scanners =
      new ArrayList<KeyValueScanner>(allScanners.size());
    // include only those scan files which pass all filters
    for (KeyValueScanner kvs : allScanners) {
      if (kvs.shouldUseScanner(scan, columns, oldestUnexpiredTS)) {
        scanners.add(kvs);
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

      List<KeyValueScanner> scanners = getScannersNoCompaction();

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
    return next(outResult, limit, null, null);
  }

  /**
   * Get the next row of values from this Store.
   * @param outResult
   * @param limit
   * @param metic
   * @param kvContext
   * @return true if there are more rows, false if scanner is done
   */
  public synchronized boolean next(List<KeyValue> outResult, int limit,
      String metric, KeyValueContext kvContext) throws IOException {

    if (kvContext != null) {
      kvContext.setObtainedFromCache(false);
    }
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

    if ((matcher.row == null) || !peeked.matchingRow(matcher.row)) {
      this.countPerRow = 0;
      matcher.setRow(peeked.getRow());
    }
    KeyValue kv;
    KeyValue prevKV = null;
    int numNewKeyValues = 0;
    keyValueAggregator.reset();
    Call call = HRegionServer.callContext.get();
    long quotaRemaining = (call == null) ? Long.MAX_VALUE
        : HRegionServer.getResponseSizeLimit() - call.getPartialResponseSize();
    // Only do a sanity-check if store and comparator are available.
    KeyValue.KVComparator comparator =
        store != null ? store.getComparator() : null;

    int addedResultsSize = 0;
    // set the responseSize so that it now can fetch records
    // in terms of keyvalue's boundary rather than row's boundary
    int remainingResponseSize = scan.getMaxResponseSize()
                              - scan.getCurrentPartialResponseSize();
    try {
      LOOP: while((kv = this.heap.peek()) != null) {
        // kv is no longer immutable due to KeyOnlyFilter! use copy for safety
        KeyValue copyKv = kv.shallowCopy();
        // Check that the heap gives us KVs in an increasing order.
        if (prevKV != null && comparator != null
            && comparator.compare(prevKV, kv) > 0) {
          throw new IOException("Key " + prevKV + " followed by a " +
              "smaller key " + kv + " in cf " + store);
        }
        prevKV = kv;
        ScanQueryMatcher.MatchCode qcode = matcher.match(copyKv, this.heap.getActiveScanners());
        if ((qcode == MatchCode.INCLUDE) ||
          (qcode == MatchCode.INCLUDE_AND_SEEK_NEXT_COL) ||
          (qcode == MatchCode.INCLUDE_AND_SEEK_NEXT_ROW)) {
          copyKv = keyValueAggregator.process(copyKv);
          // A null return is an indication to skip the KV.
          if (copyKv == null) {
            qcode = MatchCode.SKIP;
          } else {
            qcode = keyValueAggregator.nextAction(qcode);
          }
        }
        switch (qcode) {
          case SEEK_TO_EXACT_KV:
            KeyValue queriedKV = kv.createFirstOnRowColTS(Math.max(
                matcher.tr.getMax() - 1, matcher.tr.getMin()));
            reseek(queriedKV);
            break;
          case SEEK_TO_EFFECTIVE_TS:
            reseek(matcher.getKeyForEffectiveTSOnRow(kv));
            break;
          case INCLUDE:
          case INCLUDE_AND_SEEK_NEXT_ROW:
          case INCLUDE_AND_SEEK_NEXT_COL:
            this.countPerRow++;
            if (storeLimit > -1 &&
                this.countPerRow > (storeLimit + storeOffset)) {
              // do what SEEK_NEXT_ROW does.
              if (!matcher.moreRowsMayExistAfter(kv)) {
                numNewKeyValues += processLastKeyValue(outResult, kvContext);
                return false;
              }
              reseek(matcher.getKeyForNextRow(kv));
              break LOOP;
            }

            // add to results only if we have skipped #rowOffset kvs
            // also update metric accordingly
            if (this.countPerRow > storeOffset) {
              addedResultsSize += copyKv.getLength();
              if (addedResultsSize > quotaRemaining) {
                LOG.warn("Result too large. Please consider using Batching."
                    + " Cannot allow  operations that fetch more than "
                    + HRegionServer.getResponseSizeLimit() + " bytes.");
                throw new DoNotRetryIOException("Result too large");
              }
              if (kvContext != null) {

                // If any of the KV is obtained from cache, then lets tell the caller
                // that all the KVs are obtained from cache
                kvContext.setObtainedFromCache(
                    kvContext.getObtainedFromCache() |
                    this.currKeyValueObtainedFromCache());
              }
              outResult.add(copyKv);
              numNewKeyValues++;
            }

            if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
              if (!matcher.moreRowsMayExistAfter(kv)) {
                numNewKeyValues += processLastKeyValue(outResult, kvContext);
                return false;
              }
              reseek(matcher.getKeyForNextRow(kv));
            } else if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
            // we check this since some tests have store = null
              if (this.store == null) {
                reseek(matcher.getKeyForNextColumn(kv, null, false));
              } else {
                reseek(matcher.getKeyForNextColumn(kv,
                    this.heap.getActiveScanners(), isUsingDeleteColBloom));
              }
            } else {
              this.heap.next();
            }
            // If the user is strict about the response size and allows
            // partialRow scanning, we only return the number of keyvalue 
            // pairs to fill up the request size. Otherwise when partialRow 
            // is false, we just fetch the entire row
            if (scan.isPartialRow() && addedResultsSize >= remainingResponseSize) {
              break LOOP;
            }
            if (limit > 0 && (numNewKeyValues == limit)) {
              break LOOP;
            }
            continue;

          case DONE:
            numNewKeyValues += processLastKeyValue(outResult, kvContext);
            return true;

          case DONE_SCAN:
            close();
            numNewKeyValues += processLastKeyValue(outResult, kvContext);
            return false;

          case SEEK_NEXT_ROW:
            // This is just a relatively simple end of scan fix, to short-cut end
            // us if there is an endKey in the scan.
            if (!matcher.moreRowsMayExistAfter(kv)) {
              numNewKeyValues += processLastKeyValue(outResult, kvContext);
              return false;
            }

            reseek(matcher.getKeyForNextRow(kv));
            break;

          case SEEK_NEXT_COL:
          // we check this since some tests have store = null
            if (this.store == null) {
              reseek(matcher.getKeyForNextColumn(kv, null, false));
            } else {
              reseek(matcher.getKeyForNextColumn(kv, this.heap.getActiveScanners(),isUsingDeleteColBloom));
            }
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
    } catch (IOException e) {
      /*
       * Function should not modify its outResult argument if
       * exception was thrown. In ths case we should remove
       * last numNewKeyValues elements from outResults.
       */

      final int length = outResult.size();

      // this should be rare situation, we can use reflection
      if (outResult instanceof ArrayList<?>){
        // efficient code for ArrayList
        ArrayList<?> asArrayList = (ArrayList<?>)outResult;
        asArrayList.subList(length - numNewKeyValues, length).clear();
      } else {
        // generic case
        ListIterator<?> iterator = outResult.listIterator(length);
        while (numNewKeyValues-- > 0) {
          iterator.previous();
          iterator.remove();
        }
      }

      throw e;

    } finally {
      // update the remaining response size
      scan.setCurrentPartialResponseSize(scan.getCurrentPartialResponseSize()
          + addedResultsSize);
      // update the counter
      if (addedResultsSize > 0 && metric != null) {
        HRegion.incrNumericMetric(this.metricNamePrefix + metric,
            addedResultsSize);
      }
      // update the partial results size
      if (call != null) {
        call.setPartialResponseSize(call.getPartialResponseSize()
            + addedResultsSize);
      }
    }

    numNewKeyValues += processLastKeyValue(outResult, kvContext);
    if (numNewKeyValues > 0) {
      return true;
    }

    // No more keys
    close();
    return false;
  }

  private byte processLastKeyValue(List<KeyValue> outResult, KeyValueContext kvContext){
    KeyValue lastKV = keyValueAggregator.finalizeKeyValues();
    if (lastKV != null) {
      outResult.add(lastKV);
      if (kvContext != null) {
        // If any of the KV is obtained from cache, then lets tell the caller
        // that all the KVs are obtained from cache
        kvContext.setObtainedFromCache(kvContext.getObtainedFromCache() |
            this.currKeyValueObtainedFromCache());
      }
      return 1;
    }
    return 0;
  }

  @Override
  public synchronized boolean next(List<KeyValue> outResult) throws IOException {
    return next(outResult, -1, null, null);
  }

  @Override
  public synchronized boolean next(List<KeyValue> outResult, String metric)
      throws IOException {
    return next(outResult, -1, metric);
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

    List<KeyValueScanner> scanners = getScannersNoCompaction();

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
      this.countPerRow = 0;
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

  @Override
  public boolean passesDeleteColumnCheck(KeyValue kv) {
    return true;
  }
  @Override
  public boolean next(List<KeyValue> kvs, int limit,
      KeyValueContext kvContext) throws IOException {
    return next(kvs, limit, null, kvContext);
  }

  @Override
  public boolean currKeyValueObtainedFromCache() {
    if (this.heap != null) {
      return this.heap.currKeyValueObtainedFromCache();
    }
    return false;
  }

  @Override
  public boolean next(List<KeyValue> result, int limit, String metric)
      throws IOException {
    return next(result, limit, metric, null);
  }

  @Override
  public boolean passesRowKeyPrefixBloomFilter(KeyValue kv) {
    return true;
  }
}

