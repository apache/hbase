/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implements the scanner interface for the HBase client.
 * If there are multiple regions in a table, this scanner will iterate
 * through them all.
 */
public class HTableClientScanner implements ResultScanner, Runnable {
  private static final Log LOG = LogFactory.getLog(HTableClientScanner.class);
  // End of Scanning
  private static final Result[] EOS = new Result[0];

  private static final int MAX_THREADS_IN_POOL = Runtime.getRuntime()
      .availableProcessors();

  private static final ExecutorService executor = new ThreadPoolExecutor(1,
      MAX_THREADS_IN_POOL, 60L, TimeUnit.SECONDS,
      new SynchronousQueue<Runnable>());

  // HEADSUP: The scan internal start row can change as we move through table.
  protected final Scan scan;
  // The number of prefetched and cached results
  private final int caching;
  // Temporary results list in main thread, may be null
  private Result[] currentResults;
  // The position of next unfetched results in currentResults if it is
  // non-null.
  private int currentPos;
  // Whether this client has closed.
  private boolean closed;
  /**
   * The queue transferring fetched Result[] to main thread.
   * When queue.take() returns an EOS, scanning ends.
   */
  private final ArrayBlockingQueue<Result[]> queue;
  // The variable informing fetching thread to stop
  private volatile boolean closing;
  // Contains the exception caught in fetch thread.
  private volatile Throwable exception;

  private final HTable table;

  /**
   * Constructor.
   */
  public HTableClientScanner(Scan scan, HTable table) {
    this.scan = scan;
    this.table = table;
    this.queue = new ArrayBlockingQueue<>(table.getConfiguration().getInt(
        HConstants.HBASE_CLIENT_SCANNER_QUEUE_LENGTH,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_QUEUE_LENGTH));

    if (scan.getCaching() > 0) {
      this.caching = scan.getCaching();
    } else {
      this.caching = table.getScannerCaching();
    }
  }

  HTableClientScanner initialize() {
    executor.execute(this);
    return this;
  }

  @Override
  public Iterator<Result> iterator() {
    return new ResultScannerIterator(this);
  }

  // Throws a Throwable exception as IOException of RuntimeException
  private void throwIOException(Throwable e) throws IOException {
    if (e != null) {
      if (e instanceof IOException) {
        throw (IOException) e;
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetches results from queue to currentResults if it is not null.
   *
   * @return true if more results available, false if end of scanning
   */
  private boolean fetchFromQueue() throws IOException {
    if (currentResults != null) {
      return true;
    }

    if (closed) {
      return false;
    }

    try {
      currentResults = queue.take();
      if (currentResults.length == 0) {
        // End of scanning
        closed = true;
        currentResults = null;

        if (exception != null) {

          // Failure of scanning
          throwIOException(exception);
        }

        return false;
      }

      // Results fetched
      currentPos = 0;
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public Result next() throws IOException {
    if (!fetchFromQueue()) {
      return null;
    }
    Result res = currentResults[currentPos];
    currentPos++;

    if (currentPos >= currentResults.length) {
      currentResults = null;
    }
    return res;
  }

  @Override
  public Result[] next(int nbRows) throws IOException {
    if (!fetchFromQueue()) {
      return null;
    }

    // In case, currentResults is just the results we want, return it directly
    // to avoid extra resource allocation and copying.
    if (currentPos == 0 && nbRows == currentResults.length) {
      Result[] res = currentResults;
      currentResults = null;
      return res;
    }

    Result[] res = new Result[nbRows];
    int len = 0;

    while (len < nbRows) {
      // Move from currentResults
      int n = Math.min(nbRows - len, currentResults.length - currentPos);
      System.arraycopy(currentResults, currentPos, res, len, n);

      len += n;
      currentPos += n;

      if (currentPos == currentResults.length) {
        currentResults = null;

        if (!fetchFromQueue()) {
          // Unexpected partial results, we have to make a copy.
          return Arrays.copyOf(res, len);
        }
      }
    }

    return res;
  }

  @Override
  public void close() {
    if (this.closed) {
      return;
    }
    this.closing = true;
    try {
      while (fetchFromQueue()) {
        // skip all results
        currentResults = null;
      }
    } catch (Throwable e) {
      LOG.debug("Exception on closing", e);
      this.closed = true;
    }
  }

  private Result[] call(ScannerCallable callable) throws IOException {
    return table.getConnectionAndResetOperationContext()
        .getRegionServerWithRetries(callable);
  }

  // Returns a ScannerCallable with a start key
  private ScannerCallable getScannerCallable(byte[] startKey) {
    scan.setStartRow(startKey);
    ScannerCallable s = new ScannerCallable(
        table.getConnectionAndResetOperationContext(), table.getTableName(),
        scan, table.getOptions());
    s.setCaching(caching);
    return s;
  }

  // Closes a callable silently.
  private void closeScanner(ScannerCallable callable) {
    callable.setClose();
    try {
      call(callable);
    } catch (IOException e) {
      // We used to catch this error, interpret, and rethrow. However, we
      // have since decided that it's not nice for a scanner's close to
      // throw exceptions. Chances are it was just an UnknownScanner
      // exception due to lease time out.
      LOG.error("Exception caught during closeScanner", e);
    }
  }

  /**
   * Scans a region server, results are put to queue.
   *
   * @return New start key if scanning does not end, null otherwise
   * @throws IOException
   * @throws InterruptedException
   */
  private byte[] scanRegionServer(byte[] startKey) throws IOException,
      InterruptedException {
    // Open a scanner
    ScannerCallable callable = getScannerCallable(startKey);
    // openScanner
    call(callable);
    HRegionInfo currentRegion = callable.getHRegionInfo();

    Result lastRes = null;
    long lastSuccNextTs = System.currentTimeMillis();
    try {
      while (!closing) {
        Result[] values = call(callable);
        if (values == null) {
          // End of scanning
          return null;
        } else if (values.length == 0) {
          // End of region
          return currentRegion.getEndKey();
        }

        lastRes = values[values.length - 1];
        if (!closing) {
          queue.put(values);
        }
        lastSuccNextTs = System.currentTimeMillis();
      }
    } catch (DoNotRetryIOException e) {
      boolean canRetry = false;
      if (e instanceof UnknownScannerException) {
        long timeoutTs = lastSuccNextTs + table.scannerTimeout;
        long now = System.currentTimeMillis();
        if (now > timeoutTs) {
          // Scanner timeout
          long elapsed = now - lastSuccNextTs;
          ScannerTimeoutException ex = new ScannerTimeoutException(elapsed
              + "ms pased since the last invocation, "
              + "timetout is current set to " + table.scannerTimeout);
          ex.initCause(e);
          throw ex;
        }

        canRetry = true; // scannerTimeout
      } else {
        Throwable cause = e.getCause();
        if (cause != null && cause instanceof NotServingRegionException) {
          canRetry = true;
        }
      }

      if (!canRetry) {
        // Cannot retry, simply throw it out
        throw e;
      }

      if (lastRes != null) {
        return Bytes.nextOf(lastRes.getRow());
      }

      return startKey;
    } finally {
      closeScanner(callable);
    }
    // Only reach here when closing is true
    return null;
  }

  @Override
  public void run() {
    try {
      byte[] startKey = this.scan.getStartRow();
      while (!closing) {
        startKey = scanRegionServer(startKey);
        if (startKey == null || startKey.length == 0) {
          break;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      exception = e;
    } catch (Throwable e) {
      exception = e;
    }

    try {
      queue.put(EOS);
    } catch (InterruptedException e) {
      LOG.info("Fetching thread interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }
}
