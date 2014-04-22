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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
public class HTableClientScanner implements ResultScanner {
  private static final Log LOG = LogFactory.getLog(HTableClientScanner.class);
  // End of Scanning
  private static final Result[] EOS = new Result[0];

  private final ExecutorService executor;

  // Temporary results list in main thread, may be null
  private Result[] currentResults;
  // The position of next unfetched results in currentResults if it is
  // non-null.
  private int currentPos;
  // Whether this client has closed.
  private boolean closed;
  // The queue transferring fetched Result[] to main thread.
  // When queue.take() returns an EOS, scanning ends.
  private final ArrayBlockingQueue<Result[]> queue;
  // A place storing Result[] in case the queue is full. It is set only at
  // fetcher thread, will be cleared in main thread.
  private final AtomicReference<Result[]> justFetched = new AtomicReference<>();
  // Contains exception thrown in fetcher thread.
  private final AtomicReference<Throwable> exception = new AtomicReference<>();
  // The variable informing fetching thread to stop
  private final AtomicBoolean closing = new AtomicBoolean(false);

  private final Fetcher fetcher;

  /**
   * @return a Builder for creating HTableClientScanner.
   */
  public static Builder builder(Scan scan, HTable table) {
    return new Builder(scan, table);
  }

  /**
   * A builder for creating HTableClientScanner.
   */
  public static final class Builder {
    private final Scan scan;
    private final HTable table;
    private ExecutorService executor = HTable.multiActionThreadPool;

    private Builder(Scan scan, HTable table) {
      this.scan = scan;
      this.table = table;
    }

    /**
     * Specifies an alternative ExecutorService for the scanner.
     */
    public Builder setExecutor(ExecutorService vl) {
      this.executor = vl;
      return this;
    }

    /**
     * Builds the HTableClientScanner.
     */
    @SuppressWarnings("resource")
    public HTableClientScanner build() throws IOException {
      return new HTableClientScanner(scan, table, executor).initialize();
    }
  }

  /**
   * Constructor.
   *
   * @param scan The scan internal start row can change as we move through
   *          table.
   */
  private HTableClientScanner(Scan scan, HTable table, ExecutorService executor)
      throws IOException {
    this.executor = executor;
    this.queue = new ArrayBlockingQueue<>(table.getConfiguration().getInt(
        HConstants.HBASE_CLIENT_SCANNER_QUEUE_LENGTH,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_QUEUE_LENGTH));

    int caching;
    if (scan.getCaching() > 0) {
      caching = scan.getCaching();
    } else {
      caching = table.getScannerCaching();
    }

    fetcher = new Fetcher(table, scan, caching, queue, justFetched, exception,
        closing);
  }

  HTableClientScanner initialize() {
    executor.execute(fetcher);

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
   * Fetches results from queue to currentResults if it is null.
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

        Throwable e = this.exception.get();
        if (e != null) {
          // Failure of scanning
          throwIOException(e);
        }

        return false;
      }

      Result[] jf = justFetched.getAndSet(null);
      if (jf != null) {
        // Something is put justFetched because the queue is full when those
        // results are fetched. The fetching task should not be running now.
        queue.add(jf);
        if (jf.length > 0) {
          // We may have more results
          executor.execute(fetcher);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    // Results fetched
    currentPos = 0;
    return true;
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
    this.closing.set(true);
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

  @Override
  public boolean isClosed() {
    return closed;
  }

  /**
   * Returns the number of regions that we've scanned during the current scan.
   * Only used during testing, but it can be used otherwise as well.
   * Can be incorrect in case when the connection to the region fails.
   * @return
   */
  int getNumRegionsScanned() {
    return this.fetcher.numRegionsScanned.get();
  }

  private static class Fetcher implements Runnable {
    // The startKey for opening a scanner.
    private byte[] startKey;
    // The callable for scanning
    private ScannerCallable callable;
    // Current scanning region info.
    private HRegionInfo currentRegion;
    // Timestamp of last successful scan
    private long lastSuccNextTs;
    // The last result returned.
    private Result lastRes = null;

    private final HTable table;
    private final Scan scan;
    private final int caching;

    private final ArrayBlockingQueue<Result[]> queue;
    private final AtomicReference<Result[]> justFetched;
    private final AtomicReference<Throwable> exception;
    private final AtomicBoolean closing;
    private final AtomicInteger numRegionsScanned = new AtomicInteger(0);

    public Fetcher(HTable table, Scan scan, int caching,
        ArrayBlockingQueue<Result[]> queue,
        AtomicReference<Result[]> justFetched,
        AtomicReference<Throwable> exception, AtomicBoolean closing) {
      this.table = table;
      this.scan = scan;
      this.caching = caching;

      this.queue = queue;
      this.justFetched = justFetched;
      this.exception = exception;
      this.closing = closing;

      // Initialize startKey
      startKey = scan.getStartRow();
      if (startKey == null) {
        // In case startKey == null, set it to zero-length byte array since
        // null means end-of-scan.
        startKey = HConstants.EMPTY_BYTE_ARRAY;
      }
    }

    private Result[] call(ScannerCallable callable) throws IOException {
      return table.getConnectionAndResetOperationContext()
          .getRegionServerWithRetries(callable);
    }

    // Returns a ScannerCallable with a start key
    private ScannerCallable getScannerCallable(byte[] startKey) {
      scan.setStartRow(startKey);
      ScannerCallable s =
          new ScannerCallable(table.getConnectionAndResetOperationContext(),
              table.getTableNameStringBytes(), scan, table.getOptions());
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
     * Keep scanning on a region server. If we can get some results, they are
     * returned, otherwise a null is returned.
     *
     * startKey is changed if necessary. At the end of scanning, it is set to
     * null.
     *
     * @return an non-empty array of Result if we get some data. null otherwise.
     */
    private Result[] scanRegionServer() throws IOException,
        InterruptedException {
      if (callable == null) {
        // Open a scanner
        callable = getScannerCallable(startKey);
        // openScanner
        call(callable);
        currentRegion = callable.getHRegionInfo();

        lastRes = null;
        lastSuccNextTs = System.currentTimeMillis();
        this.numRegionsScanned.addAndGet(1);
      }

      boolean keepCallable = false;

      try {
        Result[] values = call(callable);
        if (values == null) {
          // End of scanning
          startKey = null;
        } else if (values.length == 0) {
          // End of region
          startKey = currentRegion.getEndKey();
          // Mark startKey as null for last region.
          if (startKey != null && startKey.length == 0) {
            startKey = null;
          }
        } else {
          // We got some results
          lastRes = values[values.length - 1];
          lastSuccNextTs = System.currentTimeMillis();

          // In this case, we keep callable
          keepCallable = true;

          return values;
        }
      } catch (DoNotRetryIOException e) {
        boolean canRetry = false;
        if (e instanceof UnknownScannerException) {
          // The region server may restarted.
          long timeoutTs = lastSuccNextTs + table.scannerTimeout;
          long now = System.currentTimeMillis();
          if (now > timeoutTs) {
            // Scanner timeout
            long elapsed = now - lastSuccNextTs;
            ScannerTimeoutException ex =
                new ScannerTimeoutException(elapsed
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
          // Skip lastRes since it has been returned.
          startKey = Bytes.nextOf(lastRes.getRow());
        }
      } finally {
        if (!keepCallable) {
          closeScanner(callable);
          callable = null;
        }
      }

      return null;
    }

    /**
     * Puts results in queue or justFetched.
     *
     * @return whether we should continue fetching in this run.
     */
    private boolean putResults(Result[] results) {
      if (!queue.offer(results)) {
        // queue is full, put results in justFetched
        justFetched.set(results);

        if (queue.isEmpty()) {
          // It's possible the queue is empty before justFetched is set
          // and the main thread is blocking on queue.Take().
          // We try move results in justFetched to queue here.
          Result[] js = justFetched.getAndSet(null);
          if (js != null) {
            queue.add(js);
            return true;
          }
          // If js == null, it means the main thread moved justFetched to
          // queue and arranged a new run.
        }
        // Then quit from this run. New run is submitted when some results
        // are taken out of the queue
        return false;
      }
      return true;
    }

    @Override
    public void run() {
      try {
        while (!closing.get() && startKey != null) {
          Result[] results = scanRegionServer();

          if (results != null) {
            if (!putResults(results)) {
              return;
            }
          }
        }
      } catch (Throwable e) {
        exception.set(e);
      }
      // We only get here scanning is over or aborted with exception
      putResults(EOS);
    }
  }
}
