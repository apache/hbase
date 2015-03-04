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
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.regionserver.handler.ParallelSeekHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Scanner scans both the memstore and the Store. Coalesce KeyValue stream
 * into List<KeyValue> for a single row.
 */
@InterfaceAudience.Private
public class StoreScanner extends NonReversedNonLazyKeyValueScanner
    implements KeyValueScanner, InternalScanner, ChangedReadersObserver {
  static final Log LOG = LogFactory.getLog(StoreScanner.class);
  protected Store store;
  protected ScanQueryMatcher matcher;
  protected KeyValueHeap heap;
  protected boolean cacheBlocks;

  protected int countPerRow = 0;
  protected int storeLimit = -1;
  protected int storeOffset = 0;

  // Used to indicate that the scanner has closed (see HBASE-1107)
  // Doesnt need to be volatile because it's always accessed via synchronized methods
  protected boolean closing = false;
  protected final boolean isGet;
  protected final boolean explicitColumnQuery;
  protected final boolean useRowColBloom;
  /**
   * A flag that enables StoreFileScanner parallel-seeking
   */
  protected boolean isParallelSeekEnabled = false;
  protected ExecutorService executor;
  protected final Scan scan;
  protected final NavigableSet<byte[]> columns;
  protected final long oldestUnexpiredTS;
  protected final long now;
  protected final int minVersions;

  /**
   * The number of KVs seen by the scanner. Includes explicitly skipped KVs, but not
   * KVs skipped via seeking to next row/column. TODO: estimate them?
   */
  private long kvsScanned = 0;
  private KeyValue prevKV = null;

  /** We don't ever expect to change this, the constant is just for clarity. */
  static final boolean LAZY_SEEK_ENABLED_BY_DEFAULT = true;
  public static final String STORESCANNER_PARALLEL_SEEK_ENABLE =
      "hbase.storescanner.parallel.seek.enable";

  /** Used during unit testing to ensure that lazy seek does save seek ops */
  protected static boolean lazySeekEnabledGlobally =
      LAZY_SEEK_ENABLED_BY_DEFAULT;

  // if heap == null and lastTop != null, you need to reseek given the key below
  protected KeyValue lastTop = null;

  // A flag whether use pread for scan
  private boolean scanUsePread = false;
  protected ReentrantLock lock = new ReentrantLock();
  
  private final long readPt;

  // used by the injection framework to test race between StoreScanner construction and compaction
  enum StoreScannerCompactionRace {
    BEFORE_SEEK,
    AFTER_SEEK,
    COMPACT_COMPLETE
  }
  
  /** An internal constructor. */
  protected StoreScanner(Store store, boolean cacheBlocks, Scan scan,
      final NavigableSet<byte[]> columns, long ttl, int minVersions, long readPt) {
    this.readPt = readPt;
    this.store = store;
    this.cacheBlocks = cacheBlocks;
    isGet = scan.isGetScan();
    int numCol = columns == null ? 0 : columns.size();
    explicitColumnQuery = numCol > 0;
    this.scan = scan;
    this.columns = columns;
    this.now = EnvironmentEdgeManager.currentTimeMillis();
    this.oldestUnexpiredTS = now - ttl;
    this.minVersions = minVersions;

    if (store != null && ((HStore)store).getHRegion() != null
        && ((HStore)store).getHRegion().getBaseConf() != null) {
      Configuration conf = ((HStore) store).getHRegion().getBaseConf();
      this.scanUsePread = conf.getBoolean("hbase.storescanner.use.pread", scan.isSmall());
    } else {
      this.scanUsePread = scan.isSmall();
    }

    // We look up row-column Bloom filters for multi-column queries as part of
    // the seek operation. However, we also look the row-column Bloom filter
    // for multi-row (non-"get") scans because this is not done in
    // StoreFile.passesBloomFilter(Scan, SortedSet<byte[]>).
    useRowColBloom = numCol > 1 || (!isGet && numCol == 1);

    // The parallel-seeking is on :
    // 1) the config value is *true*
    // 2) store has more than one store file
    if (store != null && ((HStore)store).getHRegion() != null
        && store.getStorefilesCount() > 1) {
      RegionServerServices rsService = ((HStore)store).getHRegion().getRegionServerServices();
      if (rsService == null || !rsService.getConfiguration().getBoolean(
            STORESCANNER_PARALLEL_SEEK_ENABLE, false)) return;
      isParallelSeekEnabled = true;
      executor = rsService.getExecutorService();
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
  public StoreScanner(Store store, ScanInfo scanInfo, Scan scan, final NavigableSet<byte[]> columns,
      long readPt)
                              throws IOException {
    this(store, scan.getCacheBlocks(), scan, columns, scanInfo.getTtl(),
        scanInfo.getMinVersions(), readPt);
    if (columns != null && scan.isRaw()) {
      throw new DoNotRetryIOException(
          "Cannot specify any column for a raw scan");
    }
    matcher = new ScanQueryMatcher(scan, scanInfo, columns,
        ScanType.USER_SCAN, Long.MAX_VALUE, HConstants.LATEST_TIMESTAMP,
        oldestUnexpiredTS, now, store.getCoprocessorHost());

    this.store.addChangedReaderObserver(this);

    // Pass columns to try to filter out unnecessary StoreFiles.
    List<KeyValueScanner> scanners = getScannersNoCompaction();

    // Seek all scanners to the start of the Row (or if the exact matching row
    // key does not exist, then to the start of the next matching Row).
    // Always check bloom filter to optimize the top row seek for delete
    // family marker.
    seekScanners(scanners, matcher.getStartKey(), explicitColumnQuery
        && lazySeekEnabledGlobally, isParallelSeekEnabled);

    // set storeLimit
    this.storeLimit = scan.getMaxResultsPerColumnFamily();

    // set rowOffset
    this.storeOffset = scan.getRowOffsetPerColumnFamily();

    // Combine all seeked scanners with a heap
    resetKVHeap(scanners, store.getComparator());
  }

  /**
   * Used for compactions.<p>
   *
   * Opens a scanner across specified StoreFiles.
   * @param store who we scan
   * @param scan the spec
   * @param scanners ancillary scanners
   * @param smallestReadPoint the readPoint that we should use for tracking
   *          versions
   */
  public StoreScanner(Store store, ScanInfo scanInfo, Scan scan,
      List<? extends KeyValueScanner> scanners, ScanType scanType,
      long smallestReadPoint, long earliestPutTs) throws IOException {
    this(store, scanInfo, scan, scanners, scanType, smallestReadPoint, earliestPutTs, null, null);
  }

  /**
   * Used for compactions that drop deletes from a limited range of rows.<p>
   *
   * Opens a scanner across specified StoreFiles.
   * @param store who we scan
   * @param scan the spec
   * @param scanners ancillary scanners
   * @param smallestReadPoint the readPoint that we should use for tracking versions
   * @param dropDeletesFromRow The inclusive left bound of the range; can be EMPTY_START_ROW.
   * @param dropDeletesToRow The exclusive right bound of the range; can be EMPTY_END_ROW.
   */
  public StoreScanner(Store store, ScanInfo scanInfo, Scan scan,
      List<? extends KeyValueScanner> scanners, long smallestReadPoint, long earliestPutTs,
      byte[] dropDeletesFromRow, byte[] dropDeletesToRow) throws IOException {
    this(store, scanInfo, scan, scanners, ScanType.COMPACT_RETAIN_DELETES, smallestReadPoint,
        earliestPutTs, dropDeletesFromRow, dropDeletesToRow);
  }

  private StoreScanner(Store store, ScanInfo scanInfo, Scan scan,
      List<? extends KeyValueScanner> scanners, ScanType scanType, long smallestReadPoint,
      long earliestPutTs, byte[] dropDeletesFromRow, byte[] dropDeletesToRow) throws IOException {
    this(store, false, scan, null, scanInfo.getTtl(), scanInfo.getMinVersions(),
        ((HStore)store).getHRegion().getReadpoint(IsolationLevel.READ_COMMITTED));
    if (dropDeletesFromRow == null) {
      matcher = new ScanQueryMatcher(scan, scanInfo, null, scanType, smallestReadPoint,
          earliestPutTs, oldestUnexpiredTS, now, store.getCoprocessorHost());
    } else {
      matcher = new ScanQueryMatcher(scan, scanInfo, null, smallestReadPoint, earliestPutTs,
          oldestUnexpiredTS, now, dropDeletesFromRow, dropDeletesToRow, store.getCoprocessorHost());
    }

    // Filter the list of scanners using Bloom filters, time range, TTL, etc.
    scanners = selectScannersFrom(scanners);

    // Seek all scanners to the initial key
    seekScanners(scanners, matcher.getStartKey(), false, isParallelSeekEnabled);

    // Combine all seeked scanners with a heap
    resetKVHeap(scanners, store.getComparator());
  }

  /** Constructor for testing. */
  StoreScanner(final Scan scan, ScanInfo scanInfo,
      ScanType scanType, final NavigableSet<byte[]> columns,
      final List<KeyValueScanner> scanners) throws IOException {
    this(scan, scanInfo, scanType, columns, scanners,
        HConstants.LATEST_TIMESTAMP,
        // 0 is passed as readpoint because the test bypasses Store
        0);
  }

  // Constructor for testing.
  StoreScanner(final Scan scan, ScanInfo scanInfo,
    ScanType scanType, final NavigableSet<byte[]> columns,
    final List<KeyValueScanner> scanners, long earliestPutTs)
        throws IOException {
    this(scan, scanInfo, scanType, columns, scanners, earliestPutTs,
      // 0 is passed as readpoint because the test bypasses Store
      0);
  }
  
  private StoreScanner(final Scan scan, ScanInfo scanInfo,
      ScanType scanType, final NavigableSet<byte[]> columns,
      final List<KeyValueScanner> scanners, long earliestPutTs, long readPt)
          throws IOException {
    this(null, scan.getCacheBlocks(), scan, columns, scanInfo.getTtl(),
        scanInfo.getMinVersions(), readPt);
    this.matcher = new ScanQueryMatcher(scan, scanInfo, columns, scanType,
        Long.MAX_VALUE, earliestPutTs, oldestUnexpiredTS, now, null);

    // In unit tests, the store could be null
    if (this.store != null) {
      this.store.addChangedReaderObserver(this);
    }
    // Seek all scanners to the initial key
    seekScanners(scanners, matcher.getStartKey(), false, isParallelSeekEnabled);
    resetKVHeap(scanners, scanInfo.getComparator());
  }

  /**
   * Get a filtered list of scanners. Assumes we are not in a compaction.
   * @return list of scanners to seek
   */
  protected List<KeyValueScanner> getScannersNoCompaction() throws IOException {
    final boolean isCompaction = false;
    boolean usePread = isGet || scanUsePread;
    return selectScannersFrom(store.getScanners(cacheBlocks, isGet, usePread,
        isCompaction, matcher, scan.getStartRow(), scan.getStopRow(), this.readPt));
  }

  /**
   * Seek the specified scanners with the given key
   * @param scanners
   * @param seekKey
   * @param isLazy true if using lazy seek
   * @param isParallelSeek true if using parallel seek
   * @throws IOException
   */
  protected void seekScanners(List<? extends KeyValueScanner> scanners,
      KeyValue seekKey, boolean isLazy, boolean isParallelSeek)
      throws IOException {
    // Seek all scanners to the start of the Row (or if the exact matching row
    // key does not exist, then to the start of the next matching Row).
    // Always check bloom filter to optimize the top row seek for delete
    // family marker.
    if (isLazy) {
      for (KeyValueScanner scanner : scanners) {
        scanner.requestSeek(seekKey, false, true);
      }
    } else {
      if (!isParallelSeek) {
        for (KeyValueScanner scanner : scanners) {
          scanner.seek(seekKey);
        }
      } else {
        parallelSeek(scanners, seekKey);
      }
    }
  }

  protected void resetKVHeap(List<? extends KeyValueScanner> scanners,
      KVComparator comparator) throws IOException {
    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(scanners, comparator);
  }

  /**
   * Filters the given list of scanners using Bloom filter, time range, and
   * TTL.
   */
  protected List<KeyValueScanner> selectScannersFrom(
      final List<? extends KeyValueScanner> allScanners) {
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

    List<KeyValueScanner> scanners =
        new ArrayList<KeyValueScanner>(allScanners.size());

    // We can only exclude store files based on TTL if minVersions is set to 0.
    // Otherwise, we might have to return KVs that have technically expired.
    long expiredTimestampCutoff = minVersions == 0 ? oldestUnexpiredTS :
        Long.MIN_VALUE;

    // include only those scan files which pass all filters
    for (KeyValueScanner kvs : allScanners) {
      boolean isFile = kvs.isFileScanner();
      if ((!isFile && filesOnly) || (isFile && memOnly)) {
        continue;
      }

      if (kvs.shouldUseScanner(scan, columns, expiredTimestampCutoff)) {
        scanners.add(kvs);
      }
    }
    return scanners;
  }

  @Override
  public KeyValue peek() {
    lock.lock();
    try {
    if (this.heap == null) {
      return this.lastTop;
    }
    return this.heap.peek();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public KeyValue next() {
    // throw runtime exception perhaps?
    throw new RuntimeException("Never call StoreScanner.next()");
  }

  @Override
  public void close() {
    lock.lock();
    try {
    if (this.closing) return;
    this.closing = true;
    // under test, we dont have a this.store
    if (this.store != null)
      this.store.deleteChangedReaderObserver(this);
    if (this.heap != null)
      this.heap.close();
    this.heap = null; // CLOSED!
    this.lastTop = null; // If both are null, we are closed.
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean seek(KeyValue key) throws IOException {
    lock.lock();
    try {
    // reset matcher state, in case that underlying store changed
    checkReseek();
    return this.heap.seek(key);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get the next row of values from this Store.
   * @param outResult
   * @param limit
   * @return true if there are more rows, false if scanner is done
   */
  @Override
  public boolean next(List<Cell> outResult, int limit) throws IOException {
    lock.lock();
    try {
    if (checkReseek()) {
      return true;
    }

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
    byte[] row = peeked.getBuffer();
    int offset = peeked.getRowOffset();
    short length = peeked.getRowLength();
    if (limit < 0 || matcher.row == null || !Bytes.equals(row, offset, length, matcher.row,
        matcher.rowOffset, matcher.rowLength)) {
      this.countPerRow = 0;
      matcher.setRow(row, offset, length);
    }

    KeyValue kv;

    // Only do a sanity-check if store and comparator are available.
    KeyValue.KVComparator comparator =
        store != null ? store.getComparator() : null;

    int count = 0;
    LOOP: while((kv = this.heap.peek()) != null) {
      if (prevKV != kv) ++kvsScanned; // Do object compare - we set prevKV from the same heap.
      checkScanOrder(prevKV, kv, comparator);
      prevKV = kv;

      ScanQueryMatcher.MatchCode qcode = matcher.match(kv);
      qcode = optimize(qcode, kv);
      switch(qcode) {
        case INCLUDE:
        case INCLUDE_AND_SEEK_NEXT_ROW:
        case INCLUDE_AND_SEEK_NEXT_COL:

          Filter f = matcher.getFilter();
          if (f != null) {
            // TODO convert Scan Query Matcher to be Cell instead of KV based ?
            kv = KeyValueUtil.ensureKeyValue(f.transformCell(kv));
          }

          this.countPerRow++;
          if (storeLimit > -1 &&
              this.countPerRow > (storeLimit + storeOffset)) {
            // do what SEEK_NEXT_ROW does.
            if (!matcher.moreRowsMayExistAfter(kv)) {
              return false;
            }
            seekToNextRow(kv);
            break LOOP;
          }

          // add to results only if we have skipped #storeOffset kvs
          // also update metric accordingly
          if (this.countPerRow > storeOffset) {
            outResult.add(kv);
            count++;
          }

          if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
            if (!matcher.moreRowsMayExistAfter(kv)) {
              return false;
            }
            seekToNextRow(kv);
          } else if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
            seekAsDirection(matcher.getKeyForNextColumn(kv));
          } else {
            this.heap.next();
          }

          if (limit > 0 && (count == limit)) {
            break LOOP;
          }
          continue;

        case DONE:
          return true;

        case DONE_SCAN:
          close();
          return false;

        case SEEK_NEXT_ROW:
          // This is just a relatively simple end of scan fix, to short-cut end
          // us if there is an endKey in the scan.
          if (!matcher.moreRowsMayExistAfter(kv)) {
            return false;
          }

          seekToNextRow(kv);
          break;

        case SEEK_NEXT_COL:
          seekAsDirection(matcher.getKeyForNextColumn(kv));
          break;

        case SKIP:
          this.heap.next();
          break;

        case SEEK_NEXT_USING_HINT:
          // TODO convert resee to Cell?
          KeyValue nextKV = KeyValueUtil.ensureKeyValue(matcher.getNextKeyHint(kv));
          if (nextKV != null) {
            seekAsDirection(nextKV);
          } else {
            heap.next();
          }
          break;

        default:
          throw new RuntimeException("UNEXPECTED");
      }
    }

    if (count > 0) {
      return true;
    }

    // No more keys
    close();
    return false;
    } finally {
      lock.unlock();
    }
  }

  /*
   * See if we should actually SEEK or rather just SKIP to the next Cell.
   * (See HBASE-13109)
   */
  private ScanQueryMatcher.MatchCode optimize(ScanQueryMatcher.MatchCode qcode, Cell cell) {
    byte[] nextIndexedKey = getNextIndexedKey();
    if (nextIndexedKey == null || nextIndexedKey == HConstants.NO_NEXT_INDEXED_KEY || store == null) {
      return qcode;
    }
    switch(qcode) {
    case INCLUDE_AND_SEEK_NEXT_COL:
    case SEEK_NEXT_COL:
    {
      if (matcher.compareKeyForNextColumn(nextIndexedKey, cell) >= 0) {
        return qcode == MatchCode.SEEK_NEXT_COL ? MatchCode.SKIP : MatchCode.INCLUDE;
      }
      break;
    }
    case INCLUDE_AND_SEEK_NEXT_ROW:
    case SEEK_NEXT_ROW:
    {
      if (matcher.compareKeyForNextRow(nextIndexedKey, cell) >= 0) {
        return qcode == MatchCode.SEEK_NEXT_ROW ? MatchCode.SKIP : MatchCode.INCLUDE;
      }
      break;
    }
    default:
      break;
    }
    return qcode;
  }

  @Override
  public boolean next(List<Cell> outResult) throws IOException {
    return next(outResult, -1);
  }

  // Implementation of ChangedReadersObserver
  @Override
  public void updateReaders() throws IOException {
    lock.lock();
    try {
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
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return true if top of heap has changed (and KeyValueHeap has to try the
   *         next KV)
   * @throws IOException
   */
  protected boolean checkReseek() throws IOException {
    if (this.heap == null && this.lastTop != null) {
      resetScannerStack(this.lastTop);
      if (this.heap.peek() == null
          || store.getComparator().compareRows(this.lastTop, this.heap.peek()) != 0) {
        LOG.debug("Storescanner.peek() is changed where before = "
            + this.lastTop.toString() + ",and after = " + this.heap.peek());
        this.lastTop = null;
        return true;
      }
      this.lastTop = null; // gone!
    }
    // else dont need to reseek
    return false;
  }

  protected void resetScannerStack(KeyValue lastTopKey) throws IOException {
    if (heap != null) {
      throw new RuntimeException("StoreScanner.reseek run on an existing heap!");
    }

    /* When we have the scan object, should we not pass it to getScanners()
     * to get a limited set of scanners? We did so in the constructor and we
     * could have done it now by storing the scan object from the constructor */
    List<KeyValueScanner> scanners = getScannersNoCompaction();

    // Seek all scanners to the initial key
    seekScanners(scanners, lastTopKey, false, isParallelSeekEnabled);

    // Combine all seeked scanners with a heap
    resetKVHeap(scanners, store.getComparator());

    // Reset the state of the Query Matcher and set to top row.
    // Only reset and call setRow if the row changes; avoids confusing the
    // query matcher if scanning intra-row.
    KeyValue kv = heap.peek();
    if (kv == null) {
      kv = lastTopKey;
    }
    byte[] row = kv.getBuffer();
    int offset = kv.getRowOffset();
    short length = kv.getRowLength();
    if ((matcher.row == null) || !Bytes.equals(row, offset, length, matcher.row,
        matcher.rowOffset, matcher.rowLength)) {
      this.countPerRow = 0;
      matcher.reset();
      matcher.setRow(row, offset, length);
    }
  }

  /**
   * Check whether scan as expected order
   * @param prevKV
   * @param kv
   * @param comparator
   * @throws IOException
   */
  protected void checkScanOrder(KeyValue prevKV, KeyValue kv,
      KeyValue.KVComparator comparator) throws IOException {
    // Check that the heap gives us KVs in an increasing order.
    assert prevKV == null || comparator == null
        || comparator.compare(prevKV, kv) <= 0 : "Key " + prevKV
        + " followed by a " + "smaller key " + kv + " in cf " + store;
  }

  protected boolean seekToNextRow(KeyValue kv) throws IOException {
    return reseek(matcher.getKeyForNextRow(kv));
  }

  /**
   * Do a reseek in a normal StoreScanner(scan forward)
   * @param kv
   * @return true if scanner has values left, false if end of scanner
   * @throws IOException
   */
  protected boolean seekAsDirection(KeyValue kv)
      throws IOException {
    return reseek(kv);
  }

  @Override
  public boolean reseek(KeyValue kv) throws IOException {
    lock.lock();
    try {
    //Heap will not be null, if this is called from next() which.
    //If called from RegionScanner.reseek(...) make sure the scanner
    //stack is reset if needed.
    checkReseek();
    if (explicitColumnQuery && lazySeekEnabledGlobally) {
      return heap.requestSeek(kv, true, useRowColBloom);
    }
    return heap.reseek(kv);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long getSequenceID() {
    return 0;
  }

  /**
   * Seek storefiles in parallel to optimize IO latency as much as possible
   * @param scanners the list {@link KeyValueScanner}s to be read from
   * @param kv the KeyValue on which the operation is being requested
   * @throws IOException
   */
  private void parallelSeek(final List<? extends KeyValueScanner>
      scanners, final KeyValue kv) throws IOException {
    if (scanners.isEmpty()) return;
    int storeFileScannerCount = scanners.size();
    CountDownLatch latch = new CountDownLatch(storeFileScannerCount);
    List<ParallelSeekHandler> handlers = 
        new ArrayList<ParallelSeekHandler>(storeFileScannerCount);
    for (KeyValueScanner scanner : scanners) {
      if (scanner instanceof StoreFileScanner) {
        ParallelSeekHandler seekHandler = new ParallelSeekHandler(scanner, kv,
          this.readPt, latch);
        executor.submit(seekHandler);
        handlers.add(seekHandler);
      } else {
        scanner.seek(kv);
        latch.countDown();
      }
    }

    try {
      latch.await();
    } catch (InterruptedException ie) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(ie);
    }

    for (ParallelSeekHandler handler : handlers) {
      if (handler.getErr() != null) {
        throw new IOException(handler.getErr());
      }
    }
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
   * @return The estimated number of KVs seen by this scanner (includes some skipped KVs).
   */
  public long getEstimatedNumberOfKvsScanned() {
    return this.kvsScanned;
  }

  @Override
  public byte[] getNextIndexedKey() {
    return this.heap.getNextIndexedKey();
  }
}

