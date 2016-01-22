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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;
import org.apache.hadoop.hbase.regionserver.handler.ParallelSeekHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.annotations.VisibleForTesting;

/**
 * Scanner scans both the memstore and the Store. Coalesce KeyValue stream
 * into List&lt;KeyValue&gt; for a single row.
 */
@InterfaceAudience.Private
public class StoreScanner extends NonReversedNonLazyKeyValueScanner
    implements KeyValueScanner, InternalScanner, ChangedReadersObserver {
  private static final Log LOG = LogFactory.getLog(StoreScanner.class);
  // In unit tests, the store could be null
  protected final Store store;
  protected ScanQueryMatcher matcher;
  protected KeyValueHeap heap;
  protected boolean cacheBlocks;

  protected long countPerRow = 0;
  protected int storeLimit = -1;
  protected int storeOffset = 0;

  // Used to indicate that the scanner has closed (see HBASE-1107)
  // Doesnt need to be volatile because it's always accessed via synchronized methods
  protected boolean closing = false;
  protected final boolean get;
  protected final boolean explicitColumnQuery;
  protected final boolean useRowColBloom;
  /**
   * A flag that enables StoreFileScanner parallel-seeking
   */
  protected boolean parallelSeekEnabled = false;
  protected ExecutorService executor;
  protected final Scan scan;
  protected final NavigableSet<byte[]> columns;
  protected final long oldestUnexpiredTS;
  protected final long now;
  protected final int minVersions;
  protected final long maxRowSize;
  protected final long cellsPerHeartbeatCheck;

  /**
   * The number of KVs seen by the scanner. Includes explicitly skipped KVs, but not
   * KVs skipped via seeking to next row/column. TODO: estimate them?
   */
  private long kvsScanned = 0;
  private Cell prevCell = null;

  /** We don't ever expect to change this, the constant is just for clarity. */
  static final boolean LAZY_SEEK_ENABLED_BY_DEFAULT = true;
  public static final String STORESCANNER_PARALLEL_SEEK_ENABLE =
      "hbase.storescanner.parallel.seek.enable";

  /** Used during unit testing to ensure that lazy seek does save seek ops */
  protected static boolean lazySeekEnabledGlobally =
      LAZY_SEEK_ENABLED_BY_DEFAULT;

  /**
   * The number of cells scanned in between timeout checks. Specifying a larger value means that
   * timeout checks will occur less frequently. Specifying a small value will lead to more frequent
   * timeout checks.
   */
  public static final String HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK =
      "hbase.cells.scanned.per.heartbeat.check";

  /**
   * Default value of {@link #HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK}.
   */
  public static final long DEFAULT_HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK = 10000;

  // if heap == null and lastTop != null, you need to reseek given the key below
  protected Cell lastTop = null;

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
  protected StoreScanner(Store store, Scan scan, final ScanInfo scanInfo,
      final NavigableSet<byte[]> columns, long readPt, boolean cacheBlocks) {
    this.readPt = readPt;
    this.store = store;
    this.cacheBlocks = cacheBlocks;
    get = scan.isGetScan();
    int numCol = columns == null ? 0 : columns.size();
    explicitColumnQuery = numCol > 0;
    this.scan = scan;
    this.columns = columns;
    this.now = EnvironmentEdgeManager.currentTime();
    this.oldestUnexpiredTS = now - scanInfo.getTtl();
    this.minVersions = scanInfo.getMinVersions();

     // We look up row-column Bloom filters for multi-column queries as part of
     // the seek operation. However, we also look the row-column Bloom filter
     // for multi-row (non-"get") scans because this is not done in
     // StoreFile.passesBloomFilter(Scan, SortedSet<byte[]>).
     this.useRowColBloom = numCol > 1 || (!get && numCol == 1);

     this.maxRowSize = scanInfo.getTableMaxRowSize();
     this.scanUsePread = scan.isSmall()? true: scanInfo.isUsePread();
     this.cellsPerHeartbeatCheck = scanInfo.getCellsPerTimeoutCheck();
     // Parallel seeking is on if the config allows and more there is more than one store file.
     if (this.store != null && this.store.getStorefilesCount() > 1) {
       RegionServerServices rsService = ((HStore)store).getHRegion().getRegionServerServices();
       if (rsService != null && scanInfo.isParallelSeekEnabled()) {
         this.parallelSeekEnabled = true;
         this.executor = rsService.getExecutorService();
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
  public StoreScanner(Store store, ScanInfo scanInfo, Scan scan, final NavigableSet<byte[]> columns,
      long readPt)
  throws IOException {
    this(store, scan, scanInfo, columns, readPt, scan.getCacheBlocks());
    if (columns != null && scan.isRaw()) {
      throw new DoNotRetryIOException("Cannot specify any column for a raw scan");
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
        && lazySeekEnabledGlobally, parallelSeekEnabled);

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
    this(store, scan, scanInfo, null,
      ((HStore)store).getHRegion().getReadpoint(IsolationLevel.READ_COMMITTED), false);
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
    seekScanners(scanners, matcher.getStartKey(), false, parallelSeekEnabled);

    // Combine all seeked scanners with a heap
    resetKVHeap(scanners, store.getComparator());
  }

  @VisibleForTesting
  StoreScanner(final Scan scan, ScanInfo scanInfo,
      ScanType scanType, final NavigableSet<byte[]> columns,
      final List<KeyValueScanner> scanners) throws IOException {
    this(scan, scanInfo, scanType, columns, scanners,
        HConstants.LATEST_TIMESTAMP,
        // 0 is passed as readpoint because the test bypasses Store
        0);
  }

  @VisibleForTesting
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
    this(null, scan, scanInfo, columns, readPt, scan.getCacheBlocks());
    this.matcher = new ScanQueryMatcher(scan, scanInfo, columns, scanType,
        Long.MAX_VALUE, earliestPutTs, oldestUnexpiredTS, now, null);

    // In unit tests, the store could be null
    if (this.store != null) {
      this.store.addChangedReaderObserver(this);
    }
    // Seek all scanners to the initial key
    seekScanners(scanners, matcher.getStartKey(), false, parallelSeekEnabled);
    resetKVHeap(scanners, scanInfo.getComparator());
  }

  /**
   * Get a filtered list of scanners. Assumes we are not in a compaction.
   * @return list of scanners to seek
   */
  protected List<KeyValueScanner> getScannersNoCompaction() throws IOException {
    final boolean isCompaction = false;
    boolean usePread = get || scanUsePread;
    return selectScannersFrom(store.getScanners(cacheBlocks, get, usePread,
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
      Cell seekKey, boolean isLazy, boolean isParallelSeek)
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
        long totalScannersSoughtBytes = 0;
        for (KeyValueScanner scanner : scanners) {
          if (totalScannersSoughtBytes >= maxRowSize) {
            throw new RowTooBigException("Max row size allowed: " + maxRowSize
              + ", but row is bigger than that");
          }
          scanner.seek(seekKey);
          Cell c = scanner.peek();
          if (c != null) {
            totalScannersSoughtBytes += CellUtil.estimatedSerializedSizeOf(c);
          }
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

      if (kvs.shouldUseScanner(scan, store, expiredTimestampCutoff)) {
        scanners.add(kvs);
      }
    }
    return scanners;
  }

  @Override
  public Cell peek() {
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
    // Under test, we dont have a this.store
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
  public boolean seek(Cell key) throws IOException {
    lock.lock();
    try {
    // reset matcher state, in case that underlying store changed
    checkReseek();
    return this.heap.seek(key);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean next(List<Cell> outResult) throws IOException {
    return next(outResult, NoLimitScannerContext.getInstance());
  }

  /**
   * Get the next row of values from this Store.
   * @param outResult
   * @param scannerContext
   * @return true if there are more rows, false if scanner is done
   */
  @Override
  public boolean next(List<Cell> outResult, ScannerContext scannerContext) throws IOException {
    lock.lock();

    try {
    if (scannerContext == null) {
      throw new IllegalArgumentException("Scanner context cannot be null");
    }
    if (checkReseek()) {
      return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
    }

    // if the heap was left null, then the scanners had previously run out anyways, close and
    // return.
    if (this.heap == null) {
      close();
      return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    }

    Cell cell = this.heap.peek();
    if (cell == null) {
      close();
      return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    }

    // only call setRow if the row changes; avoids confusing the query matcher
    // if scanning intra-row
    byte[] row = cell.getRowArray();
    int offset = cell.getRowOffset();
    short length = cell.getRowLength();

    // If no limits exists in the scope LimitScope.Between_Cells then we are sure we are changing
    // rows. Else it is possible we are still traversing the same row so we must perform the row
    // comparison.
    if (!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS) || matcher.row == null) {
      this.countPerRow = 0;
      matcher.setRow(row, offset, length);
    }

    // Clear progress away unless invoker has indicated it should be kept.
    if (!scannerContext.getKeepProgress()) scannerContext.clearProgress();
    
    // Only do a sanity-check if store and comparator are available.
    KeyValue.KVComparator comparator =
        store != null ? store.getComparator() : null;

    int count = 0;
    long totalBytesRead = 0;

    LOOP: do {
      // Update and check the time limit based on the configured value of cellsPerTimeoutCheck
      if ((kvsScanned % cellsPerHeartbeatCheck == 0)) {
        scannerContext.updateTimeProgress();
        if (scannerContext.checkTimeLimit(LimitScope.BETWEEN_CELLS)) {
          return scannerContext.setScannerState(NextState.TIME_LIMIT_REACHED).hasMoreValues();
        }
      }

      if (prevCell != cell) ++kvsScanned; // Do object compare - we set prevKV from the same heap.
      checkScanOrder(prevCell, cell, comparator);
      prevCell = cell;

      ScanQueryMatcher.MatchCode qcode = matcher.match(cell);
      qcode = optimize(qcode, cell);
      switch(qcode) {
        case INCLUDE:
        case INCLUDE_AND_SEEK_NEXT_ROW:
        case INCLUDE_AND_SEEK_NEXT_COL:

          Filter f = matcher.getFilter();
          if (f != null) {
            // TODO convert Scan Query Matcher to be Cell instead of KV based ?
            cell = f.transformCell(cell);
          }

          this.countPerRow++;
          if (storeLimit > -1 &&
              this.countPerRow > (storeLimit + storeOffset)) {
            // do what SEEK_NEXT_ROW does.
            if (!matcher.moreRowsMayExistAfter(cell)) {
              return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
            }
            // Setting the matcher.row = null, will mean that after the subsequent seekToNextRow()
            // the heap.peek() will any way be in the next row. So the SQM.match(cell) need do
            // another compareRow to say the current row is DONE
            matcher.row = null;
            seekToNextRow(cell);
            break LOOP;
          }

          // add to results only if we have skipped #storeOffset kvs
          // also update metric accordingly
          if (this.countPerRow > storeOffset) {
            outResult.add(cell);

            // Update local tracking information
            count++;
            totalBytesRead += CellUtil.estimatedSerializedSizeOf(cell);

            // Update the progress of the scanner context
            scannerContext.incrementSizeProgress(CellUtil.estimatedHeapSizeOfWithoutTags(cell));
            scannerContext.incrementBatchProgress(1);

            if (totalBytesRead > maxRowSize) {
              throw new RowTooBigException("Max row size allowed: " + maxRowSize
                  + ", but the row is bigger than that.");
            }
          }

          if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
            if (!matcher.moreRowsMayExistAfter(cell)) {
              return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
            }
            // Setting the matcher.row = null, will mean that after the subsequent seekToNextRow()
            // the heap.peek() will any way be in the next row. So the SQM.match(cell) need do
            // another compareRow to say the current row is DONE
            matcher.row = null;
            seekToNextRow(cell);
          } else if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
            seekAsDirection(matcher.getKeyForNextColumn(cell));
          } else {
            this.heap.next();
          }

          if (scannerContext.checkBatchLimit(LimitScope.BETWEEN_CELLS)) {
            break LOOP;
          }
          if (scannerContext.checkSizeLimit(LimitScope.BETWEEN_CELLS)) {
            break LOOP;
          }
          continue;

        case DONE:
          // We are sure that this row is done and we are in the next row.
          // So subsequent StoresScanner.next() call need not do another compare
          // and set the matcher.row
          matcher.row = null;
          return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();

        case DONE_SCAN:
          close();
          return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();

        case SEEK_NEXT_ROW:
          // This is just a relatively simple end of scan fix, to short-cut end
          // us if there is an endKey in the scan.
          if (!matcher.moreRowsMayExistAfter(cell)) {
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
          }
          // Setting the matcher.row = null, will mean that after the subsequent seekToNextRow()
          // the heap.peek() will any way be in the next row. So the SQM.match(cell) need do
          // another compareRow to say the current row is DONE
          matcher.row = null;
          seekToNextRow(cell);
          break;

        case SEEK_NEXT_COL:
          seekAsDirection(matcher.getKeyForNextColumn(cell));
          break;

        case SKIP:
          this.heap.next();
          break;

        case SEEK_NEXT_USING_HINT:
          // TODO convert resee to Cell?
          Cell nextKV = matcher.getNextKeyHint(cell);
          if (nextKV != null) {
            seekAsDirection(nextKV);
          } else {
            heap.next();
          }
          break;

        default:
          throw new RuntimeException("UNEXPECTED");
      }
    } while((cell = this.heap.peek()) != null);

    if (count > 0) {
      return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
    }

    // No more keys
    close();
    return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    } finally {
      lock.unlock();
    }
  }

  /*
   * See if we should actually SEEK or rather just SKIP to the next Cell.
   * (see HBASE-13109)
   */
  private ScanQueryMatcher.MatchCode optimize(ScanQueryMatcher.MatchCode qcode, Cell cell) {
    switch(qcode) {
    case INCLUDE_AND_SEEK_NEXT_COL:
    case SEEK_NEXT_COL:
    {
      Cell nextIndexedKey = getNextIndexedKey();
      if (nextIndexedKey != null && nextIndexedKey != KeyValueScanner.NO_NEXT_INDEXED_KEY
          && matcher.compareKeyForNextColumn(nextIndexedKey, cell) >= 0) {
        return qcode == MatchCode.SEEK_NEXT_COL ? MatchCode.SKIP : MatchCode.INCLUDE;
      }
      break;
    }
    case INCLUDE_AND_SEEK_NEXT_ROW:
    case SEEK_NEXT_ROW:
    {
      Cell nextIndexedKey = getNextIndexedKey();
      if (nextIndexedKey != null && nextIndexedKey != KeyValueScanner.NO_NEXT_INDEXED_KEY
          && matcher.compareKeyForNextRow(nextIndexedKey, cell) >= 0) {
        return qcode == MatchCode.SEEK_NEXT_ROW ? MatchCode.SKIP : MatchCode.INCLUDE;
      }
      break;
    }
    default:
      break;
    }
    return qcode;
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

  protected void resetScannerStack(Cell lastTopKey) throws IOException {
    if (heap != null) {
      throw new RuntimeException("StoreScanner.reseek run on an existing heap!");
    }

    /* When we have the scan object, should we not pass it to getScanners()
     * to get a limited set of scanners? We did so in the constructor and we
     * could have done it now by storing the scan object from the constructor */
    List<KeyValueScanner> scanners = getScannersNoCompaction();

    // Seek all scanners to the initial key
    seekScanners(scanners, lastTopKey, false, parallelSeekEnabled);

    // Combine all seeked scanners with a heap
    resetKVHeap(scanners, store.getComparator());

    // Reset the state of the Query Matcher and set to top row.
    // Only reset and call setRow if the row changes; avoids confusing the
    // query matcher if scanning intra-row.
    Cell kv = heap.peek();
    if (kv == null) {
      kv = lastTopKey;
    }
    byte[] row = kv.getRowArray();
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
  protected void checkScanOrder(Cell prevKV, Cell kv,
      KeyValue.KVComparator comparator) throws IOException {
    // Check that the heap gives us KVs in an increasing order.
    assert prevKV == null || comparator == null
        || comparator.compare(prevKV, kv) <= 0 : "Key " + prevKV
        + " followed by a " + "smaller key " + kv + " in cf " + store;
  }

  protected boolean seekToNextRow(Cell kv) throws IOException {
    return reseek(KeyValueUtil.createLastOnRow(kv));
  }

  /**
   * Do a reseek in a normal StoreScanner(scan forward)
   * @param kv
   * @return true if scanner has values left, false if end of scanner
   * @throws IOException
   */
  protected boolean seekAsDirection(Cell kv)
      throws IOException {
    return reseek(kv);
  }

  @Override
  public boolean reseek(Cell kv) throws IOException {
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
      scanners, final Cell kv) throws IOException {
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
  public Cell getNextIndexedKey() {
    return this.heap.getNextIndexedKey();
  }
}

