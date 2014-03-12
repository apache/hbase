/*
 * Copyright 2010 The Apache Software Foundation
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
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueContext;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.ipc.CallInterruptedException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * RegionScanner is an iterator through a bunch of rows in an HRegion.
 * <p>
 * It is used to combine scanners from multiple Stores (aka column families).
 */
public class RegionScanner implements InternalScanner {
//Package local for testability
  public static final Log LOG = LogFactory.getLog(RegionScanner.class);
  KeyValueHeap storeHeap = null;
  private final byte [] stopRow;
  private Filter filter;
  private final int batch;
  private boolean filterClosed = false;
  private long readPt;
  private Scan originalScan;
  private Future<ScanResult> prefetchScanFuture = null;
  private Map<byte[], Store> stores;
  private KeyValue.KVComparator comparator;
  private ConcurrentHashMap<RegionScanner, Long> scannerReadPoints;
  private MultiVersionConsistencyControl mvcc;
  private AtomicBoolean closing;
  private AtomicBoolean closed;
  private HRegionInfo regionInfo;
  private AtomicInteger rowReadCnt;
  private final List<KeyValue> MOCKED_LIST = HRegion.MOCKED_LIST;
  private final ThreadPoolExecutor scanPrefetchThreadPool;

  public RegionScanner(Scan scan, List<KeyValueScanner> additionalScanners,
      RegionContext regionContext, ThreadPoolExecutor scanPrefetchThreadPool)
    throws IOException {
    this.stores = regionContext.getStores();
    this.scannerReadPoints = regionContext.getScannerReadPoints();
    this.comparator = regionContext.getComparator();
    this.mvcc = regionContext.getmvcc();
    this.closing = regionContext.getClosing();
    this.closed = regionContext.getClosed();
    this.regionInfo = regionContext.getRegionInfo();
    this.rowReadCnt = regionContext.getRowReadCnt();
    this.originalScan = scan;
    this.scanPrefetchThreadPool = scanPrefetchThreadPool;

    this.filter = scan.getFilter();
    this.batch = scan.getBatch();
    if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
      this.stopRow = null;
    } else {
      this.stopRow = scan.getStopRow();
    }

    // synchronize on scannerReadPoints so that nobody calculates
    // getSmallestReadPoint, before scannerReadPoints is updated.
    //
    // TODO: "this" reference is escaping here. Refactor to move this logic
    // out of constructor into an initialize method
    synchronized(scannerReadPoints) {
      this.readPt = MultiVersionConsistencyControl.resetThreadReadPoint(mvcc);
      scannerReadPoints.put(this, this.readPt);
    }

    try {
      List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
      if (additionalScanners != null) {
        scanners.addAll(additionalScanners);
      }

      for (Map.Entry<byte[], NavigableSet<byte[]>> entry :
          scan.getFamilyMap().entrySet()) {
        Store store = stores.get(entry.getKey());
        StoreScanner scanner = store.getScanner(scan, entry.getValue());
        scanners.add(scanner);
      }
      this.storeHeap = new KeyValueHeap(scanners, comparator);
    } catch (IOException ioe) {
      LOG.warn("Caught exception while initializing region scanner.", ioe);
      scannerReadPoints.remove(this);
      throw ioe;
    } catch (RuntimeException re) {
      LOG.warn("Caught exception while initializing region scanner.", re);
      scannerReadPoints.remove(this);
      throw re;
    }
  }

  /**
   * Reset both the filter and the old filter.
   */
  protected void resetFilters() {
    if (filter != null) {
      filter.reset();
    }
  }

  @Override
  public boolean next(List<KeyValue> outResults, int limit)
      throws IOException {
    return next(outResults, limit, null, null);
  }

  private void preCondition() throws IOException{
    if (this.filterClosed) {
      throw new UnknownScannerException("Scanner was closed (timed out?) " +
          "after we renewed it. Could be caused by a very slow scanner " +
          "or a lengthy garbage collection");
    }
    if (closing.get() || closed.get()) {
      close();
      throw new NotServingRegionException(regionInfo.getRegionNameAsString() +
        " is closing=" + closing.get() + " or closed=" + closed.get());
    }

    // This could be a new thread from the last time we called next().
    MultiVersionConsistencyControl.setThreadReadPoint(this.readPt);
  }

  /**
   * This class abstracts the results of a single scanner's result. It tracks
   * the list of Result objects if the pre-fetch next was successful, and
   * tracks the exception if the next failed.
   */
  class ScanResult {
    final boolean isException;
    IOException ioException = null;
    Result[] outResults;
    boolean moreRows;

    public ScanResult(IOException ioException) {
      isException = true;
      this.ioException = ioException;
    }

    public ScanResult(boolean moreRows, Result[] outResults) {
      isException = false;
      this.moreRows = moreRows;
      this.outResults = outResults;
    }
  }

  /**
   * This Callable abstracts calling a pre-fetch next. This is called on a
   * threadpool. It makes a pre-fetch next call with the same parameters as
   * the incoming next call. Note that the number of rows to return (nbRows)
   * and/or the memory size for the result is the same as the previous call if
   * pre-fetching is enabled. If these params change dynamically, they will
   * take effect in the subsequent iteration.
   */
  class ScanPrefetcher implements Callable<ScanResult> {
    int nbRows;
    int limit;
    String metric;

    ScanPrefetcher(int nbRows, int limit, String metric) {
      this.nbRows = nbRows;
      this.limit = limit;
      this.metric = metric;
    }

    @Override
    public ScanResult call() {
      ScanResult scanResult = null;
      List<Result> outResults = new ArrayList<Result>();
      List<KeyValue> tmpList = new ArrayList<KeyValue>();
      int currentNbRows = 0;
      boolean moreRows = true;
      try {
        // This is necessary b/c partialResponseSize is not serialized through
        // RPC
        getOriginalScan().setCurrentPartialResponseSize(0);
        int maxResponseSize = getOriginalScan().getMaxResponseSize();
        do {
          moreRows = nextInternal(tmpList, limit, metric, null, true);
          if (!tmpList.isEmpty()) {
            currentNbRows++;
            if (outResults != null) {
              outResults.add(new Result(tmpList));
              tmpList.clear();
            }
          }
          resetFilters();
          if (isFilterDone()) {
            break;
          }

          // While Condition
          // 1. respect maxResponseSize and nbRows whichever comes first,
          // 2. recheck the currentPartialResponseSize is to catch the case
          // where maxResponseSize is saturated and partialRow == false
          // since we allow this case valid in the nextInternal() layer
        } while (moreRows
            && (getOriginalScan().getCurrentPartialResponseSize() <
                maxResponseSize && currentNbRows < nbRows));
        scanResult = new ScanResult(moreRows,
            outResults.toArray(new Result[0]));
      } catch (IOException e) {
        // we should queue the exception as the result so that we can return
        // this when the result is asked for
        scanResult = new ScanResult(e);
      }
      return scanResult;
    }
  }

  /**
   * A method to return all the rows that can fit in the response size.
   * it respects the two stop conditions:
   * 1) scan.getMaxResponseSize
   * 2) scan.getCaching() (which is nbRows)
   * the loop breaks whoever comes first.
   * This is only used by scan(), not get()
   * @param outResults a list of rows to return
   * @param nbRows the number of rows that can be returned at most
   * @param metric the metric name
   * @return true if there are more rows to fetch.
   *
   * This is used by Scans.
   */
  public synchronized Result[] nextRows(int nbRows, String metric)
  throws IOException {
    preCondition();
    boolean prefetchingEnabled = getOriginalScan().getServerPrefetching();
    int limit = this.getOriginalScan().getBatch();
    ScanResult scanResult;
    // if we have a prefetched result, then use it
    if (prefetchingEnabled && prefetchScanFuture != null) {
      try {
        scanResult = prefetchScanFuture.get();
        prefetchScanFuture = null;
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
      if (scanResult.isException) {
        throw scanResult.ioException;
      }
    }
    // if there are no prefetched results, then preform the scan inline
    else {
      ScanPrefetcher scanFetch = new ScanPrefetcher(nbRows, limit, metric);
      scanResult = scanFetch.call();
    }

    // schedule a background prefetch for the next result if prefetch is
    // enabled on scans
    boolean scanDone =
      (scanResult.outResults == null || scanResult.outResults.length == 0);
    if (prefetchingEnabled && !scanDone) {
      ScanPrefetcher callable = new ScanPrefetcher(nbRows, limit, metric);
      prefetchScanFuture = scanPrefetchThreadPool.submit(callable);
    }
    rowReadCnt.addAndGet(scanResult.outResults.length);
    Result[] ret;
    if (scanResult.outResults == null ||
        (isFilterDone() && scanResult.outResults.length == 0)) {
      ret = Result.SENTINEL_RESULT_ARRAY;
    } else {
      ret = scanResult.outResults;
    }
    return ret;
  }

  /**
   * This is used by Gets & unit tests, whereas nextRows() is
   * used by Scans
   */
  @Override
  public boolean next(List<KeyValue> outResults, int limit,
      String metric) throws IOException {
    return next(outResults, limit, metric, null);
  }

  @Override
  public synchronized boolean next(List<KeyValue> outResults, int limit, String metric,
      KeyValueContext kvContext) throws IOException {
    preCondition();
    boolean returnResult;
    if (outResults.isEmpty()) {
       // Usually outResults is empty. This is true when next is called
       // to handle scan or get operation.
      returnResult = nextInternal(outResults, limit, metric, kvContext, false);
    } else {
      List<KeyValue> tmpList = new ArrayList<KeyValue>();
      returnResult = nextInternal(tmpList, limit, metric, kvContext, false);
      outResults.addAll(tmpList);
    }
    rowReadCnt.incrementAndGet();
    resetFilters();
    if (isFilterDone()) {
      return false;
    }
    return returnResult;
  }

  @Override
  public boolean next(List<KeyValue> outResults)
      throws IOException {
    // apply the batching limit by default
    return next(outResults, batch, null, null);
  }

  @Override
  public boolean next(List<KeyValue> outResults, String metric)
      throws IOException {
    // apply the batching limit by default
    return next(outResults, batch, metric);
  }

  /*
   * @return True if a filter rules the scanner is over, done.
   */
  private boolean isFilterDone() {
    return this.filter != null && this.filter.filterAllRemaining();
  }

  /**
   * @param results empty list in which results will be stored
   */
  private boolean nextInternal(List<KeyValue> results, int limit, String metric,
      KeyValueContext kvContext, boolean prefetch)
      throws IOException {

    if (!results.isEmpty()) {
      throw new IllegalArgumentException("First parameter should be an empty list");
    }

    boolean partialRow = getOriginalScan().isPartialRow();
    long maxResponseSize = getOriginalScan().getMaxResponseSize();
    while (true) {
      if (!prefetch && HRegionServer.isCurrentConnectionClosed()) {
        HRegion.incrNumericMetric(HConstants.SERVER_INTERRUPTED_CALLS_KEY, 1);
        LOG.error(HConstants.CLIENT_SOCKED_CLOSED_EXC_MSG);
        throw new CallInterruptedException(HConstants.CLIENT_SOCKED_CLOSED_EXC_MSG);
      }
      byte [] currentRow = peekRow();
      if (isStopRow(currentRow)) {
        if (filter != null && filter.hasFilterRow()) {
          filter.filterRow(results);
        }
        if (filter != null && filter.filterRow()) {
          results.clear();
        }
        return false;
      } else if (filterRowKey(currentRow)) {
        nextRow(currentRow);
        results.clear();
      } else {
        byte [] nextRow;
        do {
          if (!prefetch && HRegionServer.isCurrentConnectionClosed()) {
            HRegion.incrNumericMetric(HConstants.SERVER_INTERRUPTED_CALLS_KEY,
                1);
            LOG.error(HConstants.CLIENT_SOCKED_CLOSED_EXC_MSG);
            throw new CallInterruptedException(HConstants.CLIENT_SOCKED_CLOSED_EXC_MSG);
          }
          this.storeHeap.next(results, limit - results.size(), metric, kvContext);
          if (limit > 0 && results.size() == limit) {
            if (this.filter != null && filter.hasFilterRow())
              throw new IncompatibleFilterException(
                "Filter with filterRow(List<KeyValue>) incompatible with scan with limit!");
            return true; // we are expecting more yes, but also limited to how many we can return.
          }
          // this gaurantees that we still complete the entire row if
          // currentPartialResponseSize exceeds the maxResponseSize.
          if (partialRow && getOriginalScan().getCurrentPartialResponseSize()
               >= maxResponseSize) {
            return true;
          }
        } while (Bytes.equals(currentRow, nextRow = peekRow()));

        final boolean stopRow = isStopRow(nextRow);

        // now that we have an entire row, lets process with a filters:

        // first filter with the filterRow(List)
        if (filter != null && filter.hasFilterRow()) {
          filter.filterRow(results);
        }

        if (results.isEmpty() || filterRow()) {
          nextRow(currentRow);
          results.clear();

          // This row was totally filtered out, if this is NOT the last row,
          // we should continue on.

          if (!stopRow) continue;
        }
        return !stopRow;
      }
    }
  }

  private boolean filterRow() {
    return filter != null
        && filter.filterRow();
  }
  private boolean filterRowKey(byte[] row) {
    return filter != null
        && filter.filterRowKey(row, 0, row.length);
  }

  protected void nextRow(byte [] currentRow) throws IOException {
    while (Bytes.equals(currentRow, peekRow())) {
      this.storeHeap.next(MOCKED_LIST);
    }
    resetFilters();
  }

  private byte[] peekRow() {
    KeyValue kv = this.storeHeap.peek();
    return kv == null ? null : kv.getRow();
  }

  private boolean isStopRow(byte [] currentRow) {
    if (currentRow == null) {
      return true;
    }
    if (stopRow == null) {
      return false;
    }
    return comparator.compareRows(stopRow, 0, stopRow.length, currentRow, 0,
        currentRow.length) <= 0;
  }

  @Override
  public synchronized void close() {
    if (storeHeap != null) {
      storeHeap.close();
      storeHeap = null;
    }
    // no need to sychronize here.
    scannerReadPoints.remove(this);
    this.filterClosed = true;
  }

  KeyValueHeap getStoreHeapForTesting() {
    return storeHeap;
  }

  /**
   * Get the original scan object that was used to create this internal one
   * @return original scan object... used for debug output
   */
  public Scan getOriginalScan() {
    return originalScan;
  }

  @Override
  public boolean next(List<KeyValue> result, int limit,
      KeyValueContext kvContext) throws IOException {
    return next(result, limit, null, kvContext);
  }

  @Override
  public boolean currKeyValueObtainedFromCache() {
    return this.storeHeap.currKeyValueObtainedFromCache();
  }
}
