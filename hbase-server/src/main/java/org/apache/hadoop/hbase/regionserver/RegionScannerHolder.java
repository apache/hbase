/**
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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.Leases.LeaseStillHeldException;

import com.google.common.base.Preconditions;

/**
 * Holder class which holds the RegionScanner, nextCallSeq, ScanPrefetcher
 * and information needed for prefetcher/fetcher.
 *
 * Originally, this is an inner class of HRegionServer. We moved it out
 * since HRegionServer is getting bigger and bigger.
 */
@InterfaceAudience.Private
public class RegionScannerHolder {
  public final static String MAX_PREFETCHED_RESULT_SIZE_KEY
    = "hbase.hregionserver.prefetcher.resultsize.max";
  public final static int MAX_PREFETCHED_RESULT_SIZE_DEFAULT = 256 * 1024 * 1024;

  final static Log LOG = LogFactory.getLog(RegionScannerHolder.class);
  final static String PREFETCHER_THREAD_PREFIX = "scan-prefetch-";

  private final static AtomicLong globalPrefetchedResultSize = new AtomicLong();

  private ThreadPoolExecutor scanPrefetchThreadPool;
  private Map<String, RegionScannerHolder> scanners;
  private long maxScannerResultSize;
  private Configuration conf;
  private Leases leases;

  private boolean prefetching = false;
  private long maxGlobalPrefetchedResultSize;
  private volatile Future<ScanResult> prefetchScanFuture;
  private volatile long prefetchedResultSize;
  private ScanPrefetcher prefetcher;
  private HRegion region;
  private int rows;

  RegionScanner scanner;
  long nextCallSeq = 0L;
  String scannerName;

  /**
   * Get the total size of all prefetched results not retrieved yet.
   */
  public static long getPrefetchedResultSize() {
    return globalPrefetchedResultSize.get();
  }

  /**
   * Construct a RegionScanner holder for a specific region server.
   *
   * @param rs the region server the specific region is on
   * @param s the scanner to be held
   * @param r the region the scanner is for
   */
  RegionScannerHolder(HRegionServer rs, RegionScanner s, HRegion r) {
    scanPrefetchThreadPool = rs.scanPrefetchThreadPool;
    maxScannerResultSize = rs.maxScannerResultSize;
    prefetcher = new ScanPrefetcher();
    scanners = rs.scanners;
    leases = rs.leases;
    conf = rs.conf;
    scanner = s;
    region = r;
  }

  public boolean isPrefetchSubmitted() {
    return prefetchScanFuture != null;
  }

  public HRegionInfo getRegionInfo() {
    return region.getRegionInfo();
  }

  /**
   * Find the current prefetched result size
   */
  public long currentPrefetchedResultSize() {
    return prefetchedResultSize;
  }

  /**
   * Wait till current prefetching task complete,
   * return true if any data retrieved, false otherwise.
   * Used for unit testing only.
   */
  public boolean waitForPrefetchingDone() {
    if (prefetchScanFuture != null) {
      try {
        ScanResult scanResult = prefetchScanFuture.get();
        return scanResult != null && scanResult.results != null
          && !scanResult.results.isEmpty();
      } catch (Throwable t) {
        LOG.debug("Got exception in getting scan result", t);
        if (t instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }
    return false;
  }

  /**
   * Stop any prefetching task and close the scanner.
   * @throws IOException
   */
  public void closeScanner() throws IOException {
    // stop prefetcher if needed.
    if (prefetchScanFuture != null) {
      synchronized (prefetcher) {
        prefetcher.scannerClosing = true;
        prefetchScanFuture.cancel(false);
      }
      prefetchScanFuture = null;
      if (prefetchedResultSize > 0) {
        globalPrefetchedResultSize.addAndGet(-prefetchedResultSize);
        prefetchedResultSize = 0L;
      }
    }
    scanner.close();
  }

  /**
   * Get the prefetched scan result, if any. Otherwise,
   * do a scan synchronously and return the result, which
   * may take some time. Region scan coprocessor, if specified,
   * is invoked properly, which may override the scan result.
   *
   * @param rows the number of rows to scan, which is preferred
   * not to change among scanner.next() calls.
   *
   * @return scan result, which has the data retrieved from
   * the scanner, or some IOException if the scan failed.
   * @throws IOException if failed to retrieve from the scanner.
   */
  public ScanResult getScanResult(final int rows) throws IOException {
    Preconditions.checkArgument(rows > 0, "Number of rows requested must be positive");
    ScanResult scanResult = null;
    this.rows = rows;

    if (prefetchScanFuture == null) {
      // Need to scan inline if not prefetched
      scanResult = prefetcher.call();
    } else {
      // if we have a prefetched result, then use it
      try {
        scanResult = prefetchScanFuture.get();
        if (scanResult.moreResults) {
          int prefetchedRows = scanResult.results.size();
          if (prefetchedRows != 0 && this.rows > prefetchedRows) {
            // Try to scan more since we haven't prefetched enough
            this.rows -= prefetchedRows;
            ScanResult tmp = prefetcher.call();
            if (tmp.isException) {
              return tmp; // Keep the prefetched results for later
            }
            if (tmp.results != null && !tmp.results.isEmpty()) {
              // Merge new results to the old result list
              scanResult.results.addAll(tmp.results);
            }
            // Reset rows for next prefetching
            this.rows = rows;
          }
        }
        prefetchScanFuture = null;
        if (prefetchedResultSize > 0) {
          globalPrefetchedResultSize.addAndGet(-prefetchedResultSize);
          prefetchedResultSize = 0L;
        }
      } catch (ExecutionException ee) {
        throw new IOException("failed to run prefetching task", ee.getCause());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        IOException iie = new InterruptedIOException("scan was interrupted");
        iie.initCause(ie);
        throw iie;
      }
    }

    if (prefetching
        && scanResult.moreResults && !scanResult.results.isEmpty()) {
      long totalPrefetchedResultSize = globalPrefetchedResultSize.get();
      if (totalPrefetchedResultSize < maxGlobalPrefetchedResultSize) {
        // Schedule a background prefetch for the next result
        // if prefetch is enabled on scans and there are more results
        prefetchScanFuture = scanPrefetchThreadPool.submit(prefetcher);
      } else if (LOG.isTraceEnabled()) {
        LOG.trace("One prefetching is skipped for scanner " + scannerName
          + " since total prefetched result size " + totalPrefetchedResultSize
          + " is more than the maximum configured "
          + maxGlobalPrefetchedResultSize);
      }
    }
    return scanResult;
  }

  /**
   * Set the rows to prefetch, and start the prefetching task.
   */
  public void enablePrefetching(int caching) {
    if (caching > 0) {
      rows = caching;
    } else {
      rows = conf.getInt(HConstants.HBASE_CLIENT_SCANNER_CACHING,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
    }
    maxGlobalPrefetchedResultSize = conf.getLong(
      MAX_PREFETCHED_RESULT_SIZE_KEY, MAX_PREFETCHED_RESULT_SIZE_DEFAULT);
    if (globalPrefetchedResultSize.get() < maxGlobalPrefetchedResultSize) {
      prefetchScanFuture = scanPrefetchThreadPool.submit(prefetcher);
    }
    prefetching = true;
  }

  /**
   * This Callable abstracts calling a pre-fetch next. This is called on a
   * threadpool. It makes a pre-fetch next call with the same parameters as
   * the incoming next call. Note that the number of rows to return (nbRows)
   * and/or the memory size for the result is the same as the previous call if
   * pre-fetching is enabled. If these parameters change dynamically,
   * they will take effect in the subsequent iteration.
   */
  class ScanPrefetcher implements Callable<ScanResult> {
    boolean scannerClosing = false;

    public ScanResult call() {
      ScanResult scanResult = null;
      Leases.Lease lease = null;
      try {
        // Remove lease while its being processed in server; protects against case
        // where processing of request takes > lease expiration time.
        lease = leases.removeLease(scannerName);
        List<Result> results = new ArrayList<Result>(rows);
        long currentScanResultSize = 0;
        boolean moreResults = true;

        boolean done = false;
        long maxResultSize = scanner.getMaxResultSize();
        if (maxResultSize <= 0) {
          maxResultSize = maxScannerResultSize;
        }
        String threadName = Thread.currentThread().getName();
        boolean prefetchingThread = threadName.startsWith(PREFETCHER_THREAD_PREFIX);
        // Call coprocessor. Get region info from scanner.
        if (region != null && region.getCoprocessorHost() != null) {
          Boolean bypass = region.getCoprocessorHost().preScannerNext(
            scanner, results, rows);
          if (!results.isEmpty()
              && (prefetchingThread || maxResultSize < Long.MAX_VALUE)) {
            for (Result r : results) {
              for (KeyValue kv : r.raw()) {
                currentScanResultSize += kv.heapSize();
              }
            }
          }
          if (bypass != null && bypass.booleanValue()) {
            done = true;
          }
        }

        if (!done) {
          List<KeyValue> values = new ArrayList<KeyValue>();
          MultiVersionConsistencyControl.setThreadReadPoint(scanner.getMvccReadPoint());
          region.startRegionOperation();
          try {
            int i = 0;
            synchronized(scanner) {
              for (; i < rows
                  && currentScanResultSize < maxResultSize; i++) {
                // Collect values to be returned here
                boolean moreRows = scanner.nextRaw(values);
                if (!values.isEmpty()) {
                  if (prefetchingThread || maxResultSize < Long.MAX_VALUE){
                    for (KeyValue kv : values) {
                      currentScanResultSize += kv.heapSize();
                    }
                  }
                  results.add(new Result(values));
                }
                if (!moreRows) {
                  break;
                }
                values.clear();
              }
            }
            region.readRequestsCount.add(i);
          } finally {
            region.closeRegionOperation();
          }

          // coprocessor postNext hook
          if (region != null && region.getCoprocessorHost() != null) {
            region.getCoprocessorHost().postScannerNext(scanner, results, rows, true);
          }
        }

        // If the scanner's filter - if any - is done with the scan
        // and wants to tell the client to stop the scan. This is done by passing
        // a null result, and setting moreResults to false.
        if (scanner.isFilterDone() && results.isEmpty()) {
          moreResults = false;
          results = null;
        }
        scanResult = new ScanResult(moreResults, results);
        if (prefetchingThread && currentScanResultSize > 0) {
          synchronized (prefetcher) {
            if (!scannerClosing) {
              globalPrefetchedResultSize.addAndGet(currentScanResultSize);
              prefetchedResultSize = currentScanResultSize;
            }
          }
        }
      } catch (IOException e) {
        // we should queue the exception as the result so that we can return
        // this when the result is asked for
        scanResult = new ScanResult(e);
      } finally {
        // We're done. On way out re-add the above removed lease.
        // Adding resets expiration time on lease.
        if (scanners.containsKey(scannerName)) {
          if (lease != null) {
            try {
              leases.addLease(lease);
            } catch (LeaseStillHeldException e) {
              LOG.error("THIS SHOULD NOT HAPPEN", e);
            }
          }
        }
      }
      return scanResult;
    }
  }
}

/**
 * This class abstracts the results of a single scanner's result. It tracks
 * the list of Result objects if the pre-fetch next was successful, and
 * tracks the exception if the next failed.
 */
class ScanResult {
  final boolean isException;
  IOException ioException = null;

  List<Result> results = null;
  boolean moreResults = false;

  public ScanResult(IOException ioException) {
    this.ioException = ioException;
    isException = true;
  }

  public ScanResult(boolean moreResults, List<Result> results) {
    this.moreResults = moreResults;
    this.results = results;
    isException = false;
  }
}
