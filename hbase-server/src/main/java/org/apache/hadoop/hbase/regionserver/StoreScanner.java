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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.PrivateConstants;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.regionserver.ScannerContext.NextState;
import org.apache.hadoop.hbase.regionserver.handler.ParallelSeekHandler;
import org.apache.hadoop.hbase.regionserver.querymatcher.CompactionScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.querymatcher.UserScanQueryMatcher;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * Scanner scans both the memstore and the Store. Coalesce KeyValue stream into List&lt;KeyValue&gt;
 * for a single row.
 * <p>
 * The implementation is not thread safe. So there will be no race between next and close. The only
 * exception is updateReaders, it will be called in the memstore flush thread to indicate that there
 * is a flush.
 */
@InterfaceAudience.Private
public class StoreScanner extends NonReversedNonLazyKeyValueScanner
    implements KeyValueScanner, InternalScanner, ChangedReadersObserver {
  private static final Logger LOG = LoggerFactory.getLogger(StoreScanner.class);
  // In unit tests, the store could be null
  protected final HStore store;
  private final CellComparator comparator;
  private ScanQueryMatcher matcher;
  protected KeyValueHeap heap;
  private boolean cacheBlocks;

  private long countPerRow = 0;
  private int storeLimit = -1;
  private int storeOffset = 0;

  // Used to indicate that the scanner has closed (see HBASE-1107)
  private volatile boolean closing = false;
  private final boolean get;
  private final boolean explicitColumnQuery;
  private final boolean useRowColBloom;
  /**
   * A flag that enables StoreFileScanner parallel-seeking
   */
  private boolean parallelSeekEnabled = false;
  private ExecutorService executor;
  private final Scan scan;
  private final long oldestUnexpiredTS;
  private final long now;
  private final int minVersions;
  private final long maxRowSize;
  private final long cellsPerHeartbeatCheck;
  long memstoreOnlyReads;
  long mixedReads;

  // 1) Collects all the KVHeap that are eagerly getting closed during the
  //    course of a scan
  // 2) Collects the unused memstore scanners. If we close the memstore scanners
  //    before sending data to client, the chunk may be reclaimed by other
  //    updates and the data will be corrupt.
  private final List<KeyValueScanner> scannersForDelayedClose = new ArrayList<>();

  /**
   * The number of KVs seen by the scanner. Includes explicitly skipped KVs, but not
   * KVs skipped via seeking to next row/column. TODO: estimate them?
   */
  private long kvsScanned = 0;
  private Cell prevCell = null;

  private final long preadMaxBytes;
  private long bytesRead;

  /** We don't ever expect to change this, the constant is just for clarity. */
  static final boolean LAZY_SEEK_ENABLED_BY_DEFAULT = true;
  public static final String STORESCANNER_PARALLEL_SEEK_ENABLE =
      "hbase.storescanner.parallel.seek.enable";

  /** Used during unit testing to ensure that lazy seek does save seek ops */
  private static boolean lazySeekEnabledGlobally = LAZY_SEEK_ENABLED_BY_DEFAULT;

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

  /**
   * If the read type is Scan.ReadType.DEFAULT, we will start with pread, and if the kvs we scanned
   * reaches this limit, we will reopen the scanner with stream. The default value is 4 times of
   * block size for this store.
   * If configured with a value <0, for all scans with ReadType DEFAULT, we will open scanner with
   * stream mode itself.
   */
  public static final String STORESCANNER_PREAD_MAX_BYTES = "hbase.storescanner.pread.max.bytes";

  private final Scan.ReadType readType;

  // A flag whether use pread for scan
  // it maybe changed if we use Scan.ReadType.DEFAULT and we have read lots of data.
  private boolean scanUsePread;
  // Indicates whether there was flush during the course of the scan
  private volatile boolean flushed = false;
  // generally we get one file from a flush
  private final List<KeyValueScanner> flushedstoreFileScanners = new ArrayList<>(1);
  // Since CompactingMemstore is now default, we get three memstore scanners from a flush
  private final List<KeyValueScanner> memStoreScannersAfterFlush = new ArrayList<>(3);
  // The current list of scanners
  final List<KeyValueScanner> currentScanners = new ArrayList<>();
  // flush update lock
  private final ReentrantLock flushLock = new ReentrantLock();
  // lock for closing.
  private final ReentrantLock closeLock = new ReentrantLock();

  protected final long readPt;
  private boolean topChanged = false;

  /** An internal constructor. */
  private StoreScanner(HStore store, Scan scan, ScanInfo scanInfo,
      int numColumns, long readPt, boolean cacheBlocks, ScanType scanType) {
    this.readPt = readPt;
    this.store = store;
    this.cacheBlocks = cacheBlocks;
    this.comparator = Preconditions.checkNotNull(scanInfo.getComparator());
    get = scan.isGetScan();
    explicitColumnQuery = numColumns > 0;
    this.scan = scan;
    this.now = EnvironmentEdgeManager.currentTime();
    this.oldestUnexpiredTS = scan.isRaw() ? 0L : now - scanInfo.getTtl();
    this.minVersions = scanInfo.getMinVersions();

    // We look up row-column Bloom filters for multi-column queries as part of
    // the seek operation. However, we also look the row-column Bloom filter
    // for multi-row (non-"get") scans because this is not done in
    // StoreFile.passesBloomFilter(Scan, SortedSet<byte[]>).
    this.useRowColBloom = numColumns > 1 || (!get && numColumns == 1)
        && (store == null || store.getColumnFamilyDescriptor().getBloomFilterType() == BloomType.ROWCOL);
    this.maxRowSize = scanInfo.getTableMaxRowSize();
    this.preadMaxBytes = scanInfo.getPreadMaxBytes();
    if (get) {
      this.readType = Scan.ReadType.PREAD;
      this.scanUsePread = true;
    } else if (scanType != ScanType.USER_SCAN) {
      // For compaction scanners never use Pread as already we have stream based scanners on the
      // store files to be compacted
      this.readType = Scan.ReadType.STREAM;
      this.scanUsePread = false;
    } else {
      if (scan.getReadType() == Scan.ReadType.DEFAULT) {
        if (scanInfo.isUsePread()) {
          this.readType = Scan.ReadType.PREAD;
        } else if (this.preadMaxBytes < 0) {
          this.readType = Scan.ReadType.STREAM;
        } else {
          this.readType = Scan.ReadType.DEFAULT;
        }
      } else {
        this.readType = scan.getReadType();
      }
      // Always start with pread unless user specific stream. Will change to stream later if
      // readType is default if the scan keeps running for a long time.
      this.scanUsePread = this.readType != Scan.ReadType.STREAM;
    }
    this.cellsPerHeartbeatCheck = scanInfo.getCellsPerTimeoutCheck();
    // Parallel seeking is on if the config allows and more there is more than one store file.
    if (store != null && store.getStorefilesCount() > 1) {
      RegionServerServices rsService = store.getHRegion().getRegionServerServices();
      if (rsService != null && scanInfo.isParallelSeekEnabled()) {
        this.parallelSeekEnabled = true;
        this.executor = rsService.getExecutorService();
      }
    }
  }

  private void addCurrentScanners(List<? extends KeyValueScanner> scanners) {
    this.currentScanners.addAll(scanners);
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
  public StoreScanner(HStore store, ScanInfo scanInfo, Scan scan, NavigableSet<byte[]> columns,
      long readPt) throws IOException {
    this(store, scan, scanInfo, columns != null ? columns.size() : 0, readPt,
        scan.getCacheBlocks(), ScanType.USER_SCAN);
    if (columns != null && scan.isRaw()) {
      throw new DoNotRetryIOException("Cannot specify any column for a raw scan");
    }
    matcher = UserScanQueryMatcher.create(scan, scanInfo, columns, oldestUnexpiredTS, now,
      store.getCoprocessorHost());

    store.addChangedReaderObserver(this);

    List<KeyValueScanner> scanners = null;
    try {
      // Pass columns to try to filter out unnecessary StoreFiles.
      scanners = selectScannersFrom(store,
        store.getScanners(cacheBlocks, scanUsePread, false, matcher, scan.getStartRow(),
          scan.includeStartRow(), scan.getStopRow(), scan.includeStopRow(), this.readPt));

      // Seek all scanners to the start of the Row (or if the exact matching row
      // key does not exist, then to the start of the next matching Row).
      // Always check bloom filter to optimize the top row seek for delete
      // family marker.
      seekScanners(scanners, matcher.getStartKey(), explicitColumnQuery && lazySeekEnabledGlobally,
        parallelSeekEnabled);

      // set storeLimit
      this.storeLimit = scan.getMaxResultsPerColumnFamily();

      // set rowOffset
      this.storeOffset = scan.getRowOffsetPerColumnFamily();
      addCurrentScanners(scanners);
      // Combine all seeked scanners with a heap
      resetKVHeap(scanners, comparator);
    } catch (IOException e) {
      clearAndClose(scanners);
      // remove us from the HStore#changedReaderObservers here or we'll have no chance to
      // and might cause memory leak
      store.deleteChangedReaderObserver(this);
      throw e;
    }
  }

  // a dummy scan instance for compaction.
  private static final Scan SCAN_FOR_COMPACTION = new Scan();

  /**
   * Used for store file compaction and memstore compaction.
   * <p>
   * Opens a scanner across specified StoreFiles/MemStoreSegments.
   * @param store who we scan
   * @param scanners ancillary scanners
   * @param smallestReadPoint the readPoint that we should use for tracking versions
   */
  public StoreScanner(HStore store, ScanInfo scanInfo, List<? extends KeyValueScanner> scanners,
      ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
    this(store, scanInfo, scanners, scanType, smallestReadPoint, earliestPutTs, null, null);
  }

  /**
   * Used for compactions that drop deletes from a limited range of rows.
   * <p>
   * Opens a scanner across specified StoreFiles.
   * @param store who we scan
   * @param scanners ancillary scanners
   * @param smallestReadPoint the readPoint that we should use for tracking versions
   * @param dropDeletesFromRow The inclusive left bound of the range; can be EMPTY_START_ROW.
   * @param dropDeletesToRow The exclusive right bound of the range; can be EMPTY_END_ROW.
   */
  public StoreScanner(HStore store, ScanInfo scanInfo, List<? extends KeyValueScanner> scanners,
      long smallestReadPoint, long earliestPutTs, byte[] dropDeletesFromRow,
      byte[] dropDeletesToRow) throws IOException {
    this(store, scanInfo, scanners, ScanType.COMPACT_RETAIN_DELETES, smallestReadPoint,
        earliestPutTs, dropDeletesFromRow, dropDeletesToRow);
  }

  private StoreScanner(HStore store, ScanInfo scanInfo, List<? extends KeyValueScanner> scanners,
      ScanType scanType, long smallestReadPoint, long earliestPutTs, byte[] dropDeletesFromRow,
      byte[] dropDeletesToRow) throws IOException {
    this(store, SCAN_FOR_COMPACTION, scanInfo, 0,
        store.getHRegion().getReadPoint(IsolationLevel.READ_COMMITTED), false, scanType);
    assert scanType != ScanType.USER_SCAN;
    matcher =
        CompactionScanQueryMatcher.create(scanInfo, scanType, smallestReadPoint, earliestPutTs,
          oldestUnexpiredTS, now, dropDeletesFromRow, dropDeletesToRow, store.getCoprocessorHost());

    // Filter the list of scanners using Bloom filters, time range, TTL, etc.
    scanners = selectScannersFrom(store, scanners);

    // Seek all scanners to the initial key
    seekScanners(scanners, matcher.getStartKey(), false, parallelSeekEnabled);
    addCurrentScanners(scanners);
    // Combine all seeked scanners with a heap
    resetKVHeap(scanners, comparator);
  }

  private void seekAllScanner(ScanInfo scanInfo, List<? extends KeyValueScanner> scanners)
      throws IOException {
    // Seek all scanners to the initial key
    seekScanners(scanners, matcher.getStartKey(), false, parallelSeekEnabled);
    addCurrentScanners(scanners);
    resetKVHeap(scanners, comparator);
  }

  // For mob compaction only as we do not have a Store instance when doing mob compaction.
  public StoreScanner(ScanInfo scanInfo, ScanType scanType,
      List<? extends KeyValueScanner> scanners) throws IOException {
    this(null, SCAN_FOR_COMPACTION, scanInfo, 0, Long.MAX_VALUE, false, scanType);
    assert scanType != ScanType.USER_SCAN;
    this.matcher = CompactionScanQueryMatcher.create(scanInfo, scanType, Long.MAX_VALUE, 0L,
      oldestUnexpiredTS, now, null, null, null);
    seekAllScanner(scanInfo, scanners);
  }

  // Used to instantiate a scanner for user scan in test
  StoreScanner(Scan scan, ScanInfo scanInfo, NavigableSet<byte[]> columns,
      List<? extends KeyValueScanner> scanners, ScanType scanType) throws IOException {
    // 0 is passed as readpoint because the test bypasses Store
    this(null, scan, scanInfo, columns != null ? columns.size() : 0, 0L, scan.getCacheBlocks(),
        scanType);
    if (scanType == ScanType.USER_SCAN) {
      this.matcher =
          UserScanQueryMatcher.create(scan, scanInfo, columns, oldestUnexpiredTS, now, null);
    } else {
      this.matcher = CompactionScanQueryMatcher.create(scanInfo, scanType, Long.MAX_VALUE,
        PrivateConstants.OLDEST_TIMESTAMP, oldestUnexpiredTS, now, null, null, null);
    }
    seekAllScanner(scanInfo, scanners);
  }

  // Used to instantiate a scanner for user scan in test
  StoreScanner(Scan scan, ScanInfo scanInfo, NavigableSet<byte[]> columns,
      List<? extends KeyValueScanner> scanners) throws IOException {
    // 0 is passed as readpoint because the test bypasses Store
    this(null, scan, scanInfo, columns != null ? columns.size() : 0, 0L, scan.getCacheBlocks(),
        ScanType.USER_SCAN);
    this.matcher =
        UserScanQueryMatcher.create(scan, scanInfo, columns, oldestUnexpiredTS, now, null);
    seekAllScanner(scanInfo, scanners);
  }

  // Used to instantiate a scanner for compaction in test
  StoreScanner(ScanInfo scanInfo, int maxVersions, ScanType scanType,
      List<? extends KeyValueScanner> scanners) throws IOException {
    // 0 is passed as readpoint because the test bypasses Store
    this(null, maxVersions > 0 ? new Scan().readVersions(maxVersions)
      : SCAN_FOR_COMPACTION, scanInfo, 0, 0L, false, scanType);
    this.matcher = CompactionScanQueryMatcher.create(scanInfo, scanType, Long.MAX_VALUE,
      PrivateConstants.OLDEST_TIMESTAMP, oldestUnexpiredTS, now, null, null, null);
    seekAllScanner(scanInfo, scanners);
  }

  boolean isScanUsePread() {
    return this.scanUsePread;
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
          if (matcher.isUserScan() && totalScannersSoughtBytes >= maxRowSize) {
            throw new RowTooBigException("Max row size allowed: " + maxRowSize
              + ", but row is bigger than that");
          }
          scanner.seek(seekKey);
          Cell c = scanner.peek();
          if (c != null) {
            totalScannersSoughtBytes += PrivateCellUtil.estimatedSerializedSizeOf(c);
          }
        }
      } else {
        parallelSeek(scanners, seekKey);
      }
    }
  }

  protected void resetKVHeap(List<? extends KeyValueScanner> scanners,
      CellComparator comparator) throws IOException {
    // Combine all seeked scanners with a heap
    heap = newKVHeap(scanners, comparator);
  }

  protected KeyValueHeap newKVHeap(List<? extends KeyValueScanner> scanners,
      CellComparator comparator) throws IOException {
    return new KeyValueHeap(scanners, comparator);
  }

  /**
   * Filters the given list of scanners using Bloom filter, time range, and TTL.
   * <p>
   * Will be overridden by testcase so declared as protected.
   */
  protected List<KeyValueScanner> selectScannersFrom(HStore store,
      List<? extends KeyValueScanner> allScanners) {
    boolean memOnly;
    boolean filesOnly;
    if (scan instanceof InternalScan) {
      InternalScan iscan = (InternalScan) scan;
      memOnly = iscan.isCheckOnlyMemStore();
      filesOnly = iscan.isCheckOnlyStoreFiles();
    } else {
      memOnly = false;
      filesOnly = false;
    }

    List<KeyValueScanner> scanners = new ArrayList<>(allScanners.size());

    // We can only exclude store files based on TTL if minVersions is set to 0.
    // Otherwise, we might have to return KVs that have technically expired.
    long expiredTimestampCutoff = minVersions == 0 ? oldestUnexpiredTS : Long.MIN_VALUE;

    // include only those scan files which pass all filters
    for (KeyValueScanner kvs : allScanners) {
      boolean isFile = kvs.isFileScanner();
      if ((!isFile && filesOnly) || (isFile && memOnly)) {
        kvs.close();
        continue;
      }

      if (kvs.shouldUseScanner(scan, store, expiredTimestampCutoff)) {
        scanners.add(kvs);
      } else {
        kvs.close();
      }
    }
    return scanners;
  }

  @Override
  public Cell peek() {
    return heap != null ? heap.peek() : null;
  }

  @Override
  public KeyValue next() {
    // throw runtime exception perhaps?
    throw new RuntimeException("Never call StoreScanner.next()");
  }

  @Override
  public void close() {
    close(true);
  }

  private void close(boolean withDelayedScannersClose) {
    closeLock.lock();
    // If the closeLock is acquired then any subsequent updateReaders()
    // call is ignored.
    try {
      if (this.closing) {
        return;
      }
      if (withDelayedScannersClose) {
        this.closing = true;
      }
      // For mob compaction, we do not have a store.
      if (this.store != null) {
        this.store.deleteChangedReaderObserver(this);
      }
      if (withDelayedScannersClose) {
        clearAndClose(scannersForDelayedClose);
        clearAndClose(memStoreScannersAfterFlush);
        clearAndClose(flushedstoreFileScanners);
        if (this.heap != null) {
          this.heap.close();
          this.currentScanners.clear();
          this.heap = null; // CLOSED!
        }
      } else {
        if (this.heap != null) {
          this.scannersForDelayedClose.add(this.heap);
          this.currentScanners.clear();
          this.heap = null;
        }
      }
    } finally {
      closeLock.unlock();
    }
  }

  @Override
  public boolean seek(Cell key) throws IOException {
    if (checkFlushed()) {
      reopenAfterFlush();
    }
    return this.heap.seek(key);
  }

  /**
   * Get the next row of values from this Store.
   * @param outResult
   * @param scannerContext
   * @return true if there are more rows, false if scanner is done
   */
  @Override
  public boolean next(List<Cell> outResult, ScannerContext scannerContext) throws IOException {
    if (scannerContext == null) {
      throw new IllegalArgumentException("Scanner context cannot be null");
    }
    if (checkFlushed() && reopenAfterFlush()) {
      return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
    }

    // if the heap was left null, then the scanners had previously run out anyways, close and
    // return.
    if (this.heap == null) {
      // By this time partial close should happened because already heap is null
      close(false);// Do all cleanup except heap.close()
      return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    }

    Cell cell = this.heap.peek();
    if (cell == null) {
      close(false);// Do all cleanup except heap.close()
      return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    }

    // only call setRow if the row changes; avoids confusing the query matcher
    // if scanning intra-row

    // If no limits exists in the scope LimitScope.Between_Cells then we are sure we are changing
    // rows. Else it is possible we are still traversing the same row so we must perform the row
    // comparison.
    if (!scannerContext.hasAnyLimit(LimitScope.BETWEEN_CELLS) || matcher.currentRow() == null) {
      this.countPerRow = 0;
      matcher.setToNewRow(cell);
    }

    // Clear progress away unless invoker has indicated it should be kept.
    if (!scannerContext.getKeepProgress()) {
      scannerContext.clearProgress();
    }

    int count = 0;
    long totalBytesRead = 0;
    // track the cells for metrics only if it is a user read request.
    boolean onlyFromMemstore = matcher.isUserScan();
    try {
      LOOP: do {
        // Update and check the time limit based on the configured value of cellsPerTimeoutCheck
        // Or if the preadMaxBytes is reached and we may want to return so we can switch to stream
        // in
        // the shipped method below.
        if (kvsScanned % cellsPerHeartbeatCheck == 0
            || (scanUsePread && readType == Scan.ReadType.DEFAULT && bytesRead > preadMaxBytes)) {
          if (scannerContext.checkTimeLimit(LimitScope.BETWEEN_CELLS)) {
            return scannerContext.setScannerState(NextState.TIME_LIMIT_REACHED).hasMoreValues();
          }
        }
        // Do object compare - we set prevKV from the same heap.
        if (prevCell != cell) {
          ++kvsScanned;
        }
        checkScanOrder(prevCell, cell, comparator);
        int cellSize = PrivateCellUtil.estimatedSerializedSizeOf(cell);
        bytesRead += cellSize;
        if (scanUsePread && readType == Scan.ReadType.DEFAULT && bytesRead > preadMaxBytes) {
          // return immediately if we want to switch from pread to stream. We need this because we
          // can
          // only switch in the shipped method, if user use a filter to filter out everything and
          // rpc
          // timeout is very large then the shipped method will never be called until the whole scan
          // is finished, but at that time we have already scan all the data...
          // See HBASE-20457 for more details.
          // And there is still a scenario that can not be handled. If we have a very large row,
          // which
          // have millions of qualifiers, and filter.filterRow is used, then even if we set the flag
          // here, we still need to scan all the qualifiers before returning...
          scannerContext.returnImmediately();
        }
        prevCell = cell;
        scannerContext.setLastPeekedCell(cell);
        topChanged = false;
        ScanQueryMatcher.MatchCode qcode = matcher.match(cell);
        switch (qcode) {
          case INCLUDE:
          case INCLUDE_AND_SEEK_NEXT_ROW:
          case INCLUDE_AND_SEEK_NEXT_COL:
            Filter f = matcher.getFilter();
            if (f != null) {
              cell = f.transformCell(cell);
            }
            this.countPerRow++;

            // add to results only if we have skipped #storeOffset kvs
            // also update metric accordingly
            if (this.countPerRow > storeOffset) {
              outResult.add(cell);

              // Update local tracking information
              count++;
              totalBytesRead += cellSize;

              /**
               * Increment the metric if all the cells are from memstore.
               * If not we will account it for mixed reads
               */
              onlyFromMemstore = onlyFromMemstore && heap.isLatestCellFromMemstore();
              // Update the progress of the scanner context
              scannerContext.incrementSizeProgress(cellSize, cell.heapSize());
              scannerContext.incrementBatchProgress(1);

              if (matcher.isUserScan() && totalBytesRead > maxRowSize) {
                String message = "Max row size allowed: " + maxRowSize
                    + ", but the row is bigger than that, the row info: "
                    + CellUtil.toString(cell, false) + ", already have process row cells = "
                    + outResult.size() + ", it belong to region = "
                    + store.getHRegion().getRegionInfo().getRegionNameAsString();
                LOG.warn(message);
                throw new RowTooBigException(message);
              }

              if (storeLimit > -1 && this.countPerRow >= (storeLimit + storeOffset)) {
                // do what SEEK_NEXT_ROW does.
                if (!matcher.moreRowsMayExistAfter(cell)) {
                  close(false);// Do all cleanup except heap.close()
                  return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
                }
                matcher.clearCurrentRow();
                seekToNextRow(cell);
                break LOOP;
              }
            }

            if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
              if (!matcher.moreRowsMayExistAfter(cell)) {
                close(false);// Do all cleanup except heap.close()
                return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
              }
              matcher.clearCurrentRow();
              seekOrSkipToNextRow(cell);
            } else if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
              seekOrSkipToNextColumn(cell);
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
            // Optimization for Gets! If DONE, no more to get on this row, early exit!
            if (get) {
              // Then no more to this row... exit.
              close(false);// Do all cleanup except heap.close()
              // update metric
              return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
            }
            matcher.clearCurrentRow();
            return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();

          case DONE_SCAN:
            close(false);// Do all cleanup except heap.close()
            return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();

          case SEEK_NEXT_ROW:
            // This is just a relatively simple end of scan fix, to short-cut end
            // us if there is an endKey in the scan.
            if (!matcher.moreRowsMayExistAfter(cell)) {
              close(false);// Do all cleanup except heap.close()
              return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
            }
            matcher.clearCurrentRow();
            seekOrSkipToNextRow(cell);
            NextState stateAfterSeekNextRow = needToReturn(outResult);
            if (stateAfterSeekNextRow != null) {
              return scannerContext.setScannerState(stateAfterSeekNextRow).hasMoreValues();
            }
            break;

          case SEEK_NEXT_COL:
            seekOrSkipToNextColumn(cell);
            NextState stateAfterSeekNextColumn = needToReturn(outResult);
            if (stateAfterSeekNextColumn != null) {
              return scannerContext.setScannerState(stateAfterSeekNextColumn).hasMoreValues();
            }
            break;

          case SKIP:
            this.heap.next();
            break;

          case SEEK_NEXT_USING_HINT:
            Cell nextKV = matcher.getNextKeyHint(cell);
            if (nextKV != null) {
              int difference = comparator.compare(nextKV, cell);
              if (((!scan.isReversed() && difference > 0)
                || (scan.isReversed() && difference < 0))) {
                seekAsDirection(nextKV);
                NextState stateAfterSeekByHint = needToReturn(outResult);
                if (stateAfterSeekByHint != null) {
                  return scannerContext.setScannerState(stateAfterSeekByHint).hasMoreValues();
                }
                break;
              }
            }
            heap.next();
            break;

          default:
            throw new RuntimeException("UNEXPECTED");
        }

        // when reaching the heartbeat cells, try to return from the loop.
        if (kvsScanned % cellsPerHeartbeatCheck == 0) {
          return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
        }
      } while ((cell = this.heap.peek()) != null);

      if (count > 0) {
        return scannerContext.setScannerState(NextState.MORE_VALUES).hasMoreValues();
      }

      // No more keys
      close(false);// Do all cleanup except heap.close()
      return scannerContext.setScannerState(NextState.NO_MORE_VALUES).hasMoreValues();
    } finally {
      // increment only if we have some result
      if (count > 0 && matcher.isUserScan()) {
        // if true increment memstore metrics, if not the mixed one
        updateMetricsStore(onlyFromMemstore);
      }
    }
  }

  private void updateMetricsStore(boolean memstoreRead) {
    if (store != null) {
      store.updateMetricsStore(memstoreRead);
    } else {
      // for testing.
      if (memstoreRead) {
        memstoreOnlyReads++;
      } else {
        mixedReads++;
      }
    }
  }

  /**
   * If the top cell won't be flushed into disk, the new top cell may be
   * changed after #reopenAfterFlush. Because the older top cell only exist
   * in the memstore scanner but the memstore scanner is replaced by hfile
   * scanner after #reopenAfterFlush. If the row of top cell is changed,
   * we should return the current cells. Otherwise, we may return
   * the cells across different rows.
   * @param outResult the cells which are visible for user scan
   * @return null is the top cell doesn't change. Otherwise, the NextState
   *         to return
   */
  private NextState needToReturn(List<Cell> outResult) {
    if (!outResult.isEmpty() && topChanged) {
      return heap.peek() == null ? NextState.NO_MORE_VALUES : NextState.MORE_VALUES;
    }
    return null;
  }

  private void seekOrSkipToNextRow(Cell cell) throws IOException {
    // If it is a Get Scan, then we know that we are done with this row; there are no more
    // rows beyond the current one: don't try to optimize.
    if (!get) {
      if (trySkipToNextRow(cell)) {
        return;
      }
    }
    seekToNextRow(cell);
  }

  private void seekOrSkipToNextColumn(Cell cell) throws IOException {
    if (!trySkipToNextColumn(cell)) {
      seekAsDirection(matcher.getKeyForNextColumn(cell));
    }
  }

  /**
   * See if we should actually SEEK or rather just SKIP to the next Cell (see HBASE-13109).
   * ScanQueryMatcher may issue SEEK hints, such as seek to next column, next row,
   * or seek to an arbitrary seek key. This method decides whether a seek is the most efficient
   * _actual_ way to get us to the requested cell (SEEKs are more expensive than SKIP, SKIP,
   * SKIP inside the current, loaded block).
   * It does this by looking at the next indexed key of the current HFile. This key
   * is then compared with the _SEEK_ key, where a SEEK key is an artificial 'last possible key
   * on the row' (only in here, we avoid actually creating a SEEK key; in the compare we work with
   * the current Cell but compare as though it were a seek key; see down in
   * matcher.compareKeyForNextRow, etc). If the compare gets us onto the
   * next block we *_SEEK, otherwise we just SKIP to the next requested cell.
   *
   * <p>Other notes:
   * <ul>
   * <li>Rows can straddle block boundaries</li>
   * <li>Versions of columns can straddle block boundaries (i.e. column C1 at T1 might be in a
   * different block than column C1 at T2)</li>
   * <li>We want to SKIP if the chance is high that we'll find the desired Cell after a
   * few SKIPs...</li>
   * <li>We want to SEEK when the chance is high that we'll be able to seek
   * past many Cells, especially if we know we need to go to the next block.</li>
   * </ul>
   * <p>A good proxy (best effort) to determine whether SKIP is better than SEEK is whether
   * we'll likely end up seeking to the next block (or past the next block) to get our next column.
   * Example:
   * <pre>
   * |    BLOCK 1              |     BLOCK 2                   |
   * |  r1/c1, r1/c2, r1/c3    |    r1/c4, r1/c5, r2/c1        |
   *                                   ^         ^
   *                                   |         |
   *                           Next Index Key   SEEK_NEXT_ROW (before r2/c1)
   *
   *
   * |    BLOCK 1                       |     BLOCK 2                      |
   * |  r1/c1/t5, r1/c1/t4, r1/c1/t3    |    r1/c1/t2, r1/c1/T1, r1/c2/T3  |
   *                                            ^              ^
   *                                            |              |
   *                                    Next Index Key        SEEK_NEXT_COL
   * </pre>
   * Now imagine we want columns c1 and c3 (see first diagram above), the 'Next Index Key' of r1/c4
   * is > r1/c3 so we should seek to get to the c1 on the next row, r2. In second case, say we only
   * want one version of c1, after we have it, a SEEK_COL will be issued to get to c2. Looking at
   * the 'Next Index Key', it would land us in the next block, so we should SEEK. In other scenarios
   * where the SEEK will not land us in the next block, it is very likely better to issues a series
   * of SKIPs.
   * @param cell current cell
   * @return true means skip to next row, false means not
   */
  protected boolean trySkipToNextRow(Cell cell) throws IOException {
    Cell nextCell = null;
    // used to guard against a changed next indexed key by doing a identity comparison
    // when the identity changes we need to compare the bytes again
    Cell previousIndexedKey = null;
    do {
      Cell nextIndexedKey = getNextIndexedKey();
      if (nextIndexedKey != null && nextIndexedKey != KeyValueScanner.NO_NEXT_INDEXED_KEY &&
          (nextIndexedKey == previousIndexedKey ||
          matcher.compareKeyForNextRow(nextIndexedKey, cell) >= 0)) {
        this.heap.next();
        ++kvsScanned;
        previousIndexedKey = nextIndexedKey;
      } else {
        return false;
      }
    } while ((nextCell = this.heap.peek()) != null && CellUtil.matchingRows(cell, nextCell));
    return true;
  }

  /**
   * See {@link org.apache.hadoop.hbase.regionserver.StoreScanner#trySkipToNextRow(Cell)}
   * @param cell current cell
   * @return true means skip to next column, false means not
   */
  protected boolean trySkipToNextColumn(Cell cell) throws IOException {
    Cell nextCell = null;
    // used to guard against a changed next indexed key by doing a identity comparison
    // when the identity changes we need to compare the bytes again
    Cell previousIndexedKey = null;
    do {
      Cell nextIndexedKey = getNextIndexedKey();
      if (nextIndexedKey != null && nextIndexedKey != KeyValueScanner.NO_NEXT_INDEXED_KEY &&
          (nextIndexedKey == previousIndexedKey ||
          matcher.compareKeyForNextColumn(nextIndexedKey, cell) >= 0)) {
        this.heap.next();
        ++kvsScanned;
        previousIndexedKey = nextIndexedKey;
      } else {
        return false;
      }
    } while ((nextCell = this.heap.peek()) != null && CellUtil.matchingRowColumn(cell, nextCell));
    // We need this check because it may happen that the new scanner that we get
    // during heap.next() is requiring reseek due of fake KV previously generated for
    // ROWCOL bloom filter optimization. See HBASE-19863 for more details
    if (useRowColBloom && nextCell != null && matcher.compareKeyForNextColumn(nextCell, cell) < 0) {
      return false;
    }
    return true;
  }

  @Override
  public long getReadPoint() {
    return this.readPt;
  }

  private static void clearAndClose(List<KeyValueScanner> scanners) {
    if (scanners == null) {
      return;
    }
    for (KeyValueScanner s : scanners) {
      s.close();
    }
    scanners.clear();
  }

  // Implementation of ChangedReadersObserver
  @Override
  public void updateReaders(List<HStoreFile> sfs, List<KeyValueScanner> memStoreScanners)
      throws IOException {
    if (CollectionUtils.isEmpty(sfs) && CollectionUtils.isEmpty(memStoreScanners)) {
      return;
    }
    boolean updateReaders = false;
    flushLock.lock();
    try {
      if (!closeLock.tryLock()) {
        // The reason for doing this is that when the current store scanner does not retrieve
        // any new cells, then the scanner is considered to be done. The heap of this scanner
        // is not closed till the shipped() call is completed. Hence in that case if at all
        // the partial close (close (false)) has been called before updateReaders(), there is no
        // need for the updateReaders() to happen.
        LOG.debug("StoreScanner already has the close lock. There is no need to updateReaders");
        // no lock acquired.
        clearAndClose(memStoreScanners);
        return;
      }
      // lock acquired
      updateReaders = true;
      if (this.closing) {
        LOG.debug("StoreScanner already closing. There is no need to updateReaders");
        clearAndClose(memStoreScanners);
        return;
      }
      flushed = true;
      final boolean isCompaction = false;
      boolean usePread = get || scanUsePread;
      // SEE HBASE-19468 where the flushed files are getting compacted even before a scanner
      // calls next(). So its better we create scanners here rather than next() call. Ensure
      // these scanners are properly closed() whether or not the scan is completed successfully
      // Eagerly creating scanners so that we have the ref counting ticking on the newly created
      // store files. In case of stream scanners this eager creation does not induce performance
      // penalty because in scans (that uses stream scanners) the next() call is bound to happen.
      List<KeyValueScanner> scanners = store.getScanners(sfs, cacheBlocks, get, usePread,
        isCompaction, matcher, scan.getStartRow(), scan.getStopRow(), this.readPt, false);
      flushedstoreFileScanners.addAll(scanners);
      if (!CollectionUtils.isEmpty(memStoreScanners)) {
        clearAndClose(memStoreScannersAfterFlush);
        memStoreScannersAfterFlush.addAll(memStoreScanners);
      }
    } finally {
      flushLock.unlock();
      if (updateReaders) {
        closeLock.unlock();
      }
    }
    // Let the next() call handle re-creating and seeking
  }

  /**
   * @return if top of heap has changed (and KeyValueHeap has to try the next KV)
   */
  protected final boolean reopenAfterFlush() throws IOException {
    // here we can make sure that we have a Store instance so no null check on store.
    Cell lastTop = heap.peek();
    // When we have the scan object, should we not pass it to getScanners() to get a limited set of
    // scanners? We did so in the constructor and we could have done it now by storing the scan
    // object from the constructor
    List<KeyValueScanner> scanners;
    flushLock.lock();
    try {
      List<KeyValueScanner> allScanners =
          new ArrayList<>(flushedstoreFileScanners.size() + memStoreScannersAfterFlush.size());
      allScanners.addAll(flushedstoreFileScanners);
      allScanners.addAll(memStoreScannersAfterFlush);
      scanners = selectScannersFrom(store, allScanners);
      // Clear the current set of flushed store files scanners so that they don't get added again
      flushedstoreFileScanners.clear();
      memStoreScannersAfterFlush.clear();
    } finally {
      flushLock.unlock();
    }

    // Seek the new scanners to the last key
    seekScanners(scanners, lastTop, false, parallelSeekEnabled);
    // remove the older memstore scanner
    for (int i = currentScanners.size() - 1; i >=0; i--) {
      if (!currentScanners.get(i).isFileScanner()) {
        scannersForDelayedClose.add(currentScanners.remove(i));
      } else {
        // we add the memstore scanner to the end of currentScanners
        break;
      }
    }
    // add the newly created scanners on the flushed files and the current active memstore scanner
    addCurrentScanners(scanners);
    // Combine all seeked scanners with a heap
    resetKVHeap(this.currentScanners, store.getComparator());
    resetQueryMatcher(lastTop);
    if (heap.peek() == null || store.getComparator().compareRows(lastTop, this.heap.peek()) != 0) {
      LOG.info("Storescanner.peek() is changed where before = " + lastTop.toString() +
          ",and after = " + heap.peek());
      topChanged = true;
    } else {
      topChanged = false;
    }
    return topChanged;
  }

  private void resetQueryMatcher(Cell lastTopKey) {
    // Reset the state of the Query Matcher and set to top row.
    // Only reset and call setRow if the row changes; avoids confusing the
    // query matcher if scanning intra-row.
    Cell cell = heap.peek();
    if (cell == null) {
      cell = lastTopKey;
    }
    if ((matcher.currentRow() == null) || !CellUtil.matchingRows(cell, matcher.currentRow())) {
      this.countPerRow = 0;
      // The setToNewRow will call reset internally
      matcher.setToNewRow(cell);
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
      CellComparator comparator) throws IOException {
    // Check that the heap gives us KVs in an increasing order.
    assert prevKV == null || comparator == null || comparator.compare(prevKV, kv) <= 0 : "Key "
        + prevKV + " followed by a smaller key " + kv + " in cf " + store;
  }

  protected boolean seekToNextRow(Cell c) throws IOException {
    return reseek(PrivateCellUtil.createLastOnRow(c));
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
    if (checkFlushed()) {
      reopenAfterFlush();
    }
    if (explicitColumnQuery && lazySeekEnabledGlobally) {
      return heap.requestSeek(kv, true, useRowColBloom);
    }
    return heap.reseek(kv);
  }

  void trySwitchToStreamRead() {
    if (readType != Scan.ReadType.DEFAULT || !scanUsePread || closing ||
        heap.peek() == null || bytesRead < preadMaxBytes) {
      return;
    }
    LOG.debug("Switch to stream read (scanned={} bytes) of {}", bytesRead,
        this.store.getColumnFamilyName());
    scanUsePread = false;
    Cell lastTop = heap.peek();
    List<KeyValueScanner> memstoreScanners = new ArrayList<>();
    List<KeyValueScanner> scannersToClose = new ArrayList<>();
    for (KeyValueScanner kvs : currentScanners) {
      if (!kvs.isFileScanner()) {
        // collect memstorescanners here
        memstoreScanners.add(kvs);
      } else {
        scannersToClose.add(kvs);
      }
    }
    List<KeyValueScanner> fileScanners = null;
    List<KeyValueScanner> newCurrentScanners;
    KeyValueHeap newHeap;
    try {
      // We must have a store instance here so no null check
      // recreate the scanners on the current file scanners
      fileScanners = store.recreateScanners(scannersToClose, cacheBlocks, false, false,
        matcher, scan.getStartRow(), scan.includeStartRow(), scan.getStopRow(),
        scan.includeStopRow(), readPt, false);
      if (fileScanners == null) {
        return;
      }
      seekScanners(fileScanners, lastTop, false, parallelSeekEnabled);
      newCurrentScanners = new ArrayList<>(fileScanners.size() + memstoreScanners.size());
      newCurrentScanners.addAll(fileScanners);
      newCurrentScanners.addAll(memstoreScanners);
      newHeap = newKVHeap(newCurrentScanners, comparator);
    } catch (Exception e) {
      LOG.warn("failed to switch to stream read", e);
      if (fileScanners != null) {
        fileScanners.forEach(KeyValueScanner::close);
      }
      return;
    }
    currentScanners.clear();
    addCurrentScanners(newCurrentScanners);
    this.heap = newHeap;
    resetQueryMatcher(lastTop);
    scannersToClose.forEach(KeyValueScanner::close);
  }

  protected final boolean checkFlushed() {
    // check the var without any lock. Suppose even if we see the old
    // value here still it is ok to continue because we will not be resetting
    // the heap but will continue with the referenced memstore's snapshot. For compactions
    // any way we don't need the updateReaders at all to happen as we still continue with
    // the older files
    if (flushed) {
      // If there is a flush and the current scan is notified on the flush ensure that the
      // scan's heap gets reset and we do a seek on the newly flushed file.
      if (this.closing) {
        return false;
      }
      // reset the flag
      flushed = false;
      return true;
    }
    return false;
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
    List<ParallelSeekHandler> handlers = new ArrayList<>(storeFileScannerCount);
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
    List<KeyValueScanner> allScanners = new ArrayList<>();
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

  @Override
  public void shipped() throws IOException {
    if (prevCell != null) {
      // Do the copy here so that in case the prevCell ref is pointing to the previous
      // blocks we can safely release those blocks.
      // This applies to blocks that are got from Bucket cache, L1 cache and the blocks
      // fetched from HDFS. Copying this would ensure that we let go the references to these
      // blocks so that they can be GCed safely(in case of bucket cache)
      prevCell = KeyValueUtil.toNewKeyCell(this.prevCell);
    }
    matcher.beforeShipped();
    // There wont be further fetch of Cells from these scanners. Just close.
    clearAndClose(scannersForDelayedClose);
    if (this.heap != null) {
      this.heap.shipped();
      // When switching from pread to stream, we will open a new scanner for each store file, but
      // the old scanner may still track the HFileBlocks we have scanned but not sent back to client
      // yet. If we close the scanner immediately then the HFileBlocks may be messed up by others
      // before we serialize and send it back to client. The HFileBlocks will be released in shipped
      // method, so we here will also open new scanners and close old scanners in shipped method.
      // See HBASE-18055 for more details.
      trySwitchToStreamRead();
    }
  }
}
