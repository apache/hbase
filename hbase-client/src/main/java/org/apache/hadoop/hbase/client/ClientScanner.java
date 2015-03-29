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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MapReduceProtos;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implements the scanner interface for the HBase client.
 * If there are multiple regions in a table, this scanner will iterate
 * through them all.
 */
@InterfaceAudience.Private
public class ClientScanner extends AbstractClientScanner {
    private final Log LOG = LogFactory.getLog(this.getClass());
    protected Scan scan;
    protected boolean closed = false;
    // Current region scanner is against.  Gets cleared if current region goes
    // wonky: e.g. if it splits on us.
    protected HRegionInfo currentRegion = null;
    protected ScannerCallableWithReplicas callable = null;
    protected final LinkedList<Result> cache = new LinkedList<Result>();
    protected final int caching;
    protected long lastNext;
    // Keep lastResult returned successfully in case we have to reset scanner.
    protected Result lastResult = null;
    protected final long maxScannerResultSize;
    private final ClusterConnection connection;
    private final TableName tableName;
    protected final int scannerTimeout;
    protected boolean scanMetricsPublished = false;
    protected RpcRetryingCaller<Result []> caller;
    protected RpcControllerFactory rpcControllerFactory;
    protected Configuration conf;
    //The timeout on the primary. Applicable if there are multiple replicas for a region
    //In that case, we will only wait for this much timeout on the primary before going
    //to the replicas and trying the same scan. Note that the retries will still happen
    //on each replica and the first successful results will be taken. A timeout of 0 is
    //disallowed.
    protected final int primaryOperationTimeout;
    private int retries;
    protected final ExecutorService pool;

  /**
   * Create a new ClientScanner for the specified table Note that the passed {@link Scan}'s start
   * row maybe changed changed.
   * @param conf The {@link Configuration} to use.
   * @param scan {@link Scan} to use in this scanner
   * @param tableName The table that we wish to scan
   * @param connection Connection identifying the cluster
   * @throws IOException
   */
  public ClientScanner(final Configuration conf, final Scan scan, final TableName tableName,
      ClusterConnection connection, RpcRetryingCallerFactory rpcFactory,
      RpcControllerFactory controllerFactory, ExecutorService pool, int primaryOperationTimeout) throws IOException {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Scan table=" + tableName
            + ", startRow=" + Bytes.toStringBinary(scan.getStartRow()));
      }
      this.scan = scan;
      this.tableName = tableName;
      this.lastNext = System.currentTimeMillis();
      this.connection = connection;
      this.pool = pool;
      this.primaryOperationTimeout = primaryOperationTimeout;
      this.retries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
          HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
      if (scan.getMaxResultSize() > 0) {
        this.maxScannerResultSize = scan.getMaxResultSize();
      } else {
        this.maxScannerResultSize = conf.getLong(
          HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
          HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
      }
      this.scannerTimeout = HBaseConfiguration.getInt(conf,
        HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
        HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

      // check if application wants to collect scan metrics
      initScanMetrics(scan);

      // Use the caching from the Scan.  If not set, use the default cache setting for this table.
      if (this.scan.getCaching() > 0) {
        this.caching = this.scan.getCaching();
      } else {
        this.caching = conf.getInt(
            HConstants.HBASE_CLIENT_SCANNER_CACHING,
            HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
      }

      this.caller = rpcFactory.<Result[]> newCaller();
      this.rpcControllerFactory = controllerFactory;

      this.conf = conf;
      initializeScannerInConstruction();
    }

    protected void initializeScannerInConstruction() throws IOException{
      // initialize the scanner
      nextScanner(this.caching, false);
    }

    protected ClusterConnection getConnection() {
      return this.connection;
    }

    /**
     * @return Table name
     * @deprecated Since 0.96.0; use {@link #getTable()}
     */
    @Deprecated
    protected byte [] getTableName() {
      return this.tableName.getName();
    }

    protected TableName getTable() {
      return this.tableName;
    }

    protected int getRetries() {
      return this.retries;
    }

    protected int getScannerTimeout() {
      return this.scannerTimeout;
    }

    protected Configuration getConf() {
      return this.conf;
    }

    protected Scan getScan() {
      return scan;
    }

    protected ExecutorService getPool() {
      return pool;
    }

    protected int getPrimaryOperationTimeout() {
      return primaryOperationTimeout;
    }

    protected int getCaching() {
      return caching;
    }

    protected long getTimestamp() {
      return lastNext;
    }

    // returns true if the passed region endKey
    protected boolean checkScanStopRow(final byte [] endKey) {
      if (this.scan.getStopRow().length > 0) {
        // there is a stop row, check to see if we are past it.
        byte [] stopRow = scan.getStopRow();
        int cmp = Bytes.compareTo(stopRow, 0, stopRow.length,
          endKey, 0, endKey.length);
        if (cmp <= 0) {
          // stopRow <= endKey (endKey is equals to or larger than stopRow)
          // This is a stop.
          return true;
        }
      }
      return false; //unlikely.
    }

    private boolean possiblyNextScanner(int nbRows, final boolean done) throws IOException {
      // If we have just switched replica, don't go to the next scanner yet. Rather, try
      // the scanner operations on the new replica, from the right point in the scan
      // Note that when we switched to a different replica we left it at a point
      // where we just did the "openScanner" with the appropriate startrow
      if (callable != null && callable.switchedToADifferentReplica()) return true;
      return nextScanner(nbRows, done);
    }

    /*
     * Gets a scanner for the next region.  If this.currentRegion != null, then
     * we will move to the endrow of this.currentRegion.  Else we will get
     * scanner at the scan.getStartRow().  We will go no further, just tidy
     * up outstanding scanners, if <code>currentRegion != null</code> and
     * <code>done</code> is true.
     * @param nbRows
     * @param done Server-side says we're done scanning.
     */
  protected boolean nextScanner(int nbRows, final boolean done)
    throws IOException {
      // Close the previous scanner if it's open
      if (this.callable != null) {
        this.callable.setClose();
        call(scan, callable, caller, scannerTimeout);
        this.callable = null;
      }

      // Where to start the next scanner
      byte [] localStartKey;

      // if we're at end of table, close and return false to stop iterating
      if (this.currentRegion != null) {
        byte [] endKey = this.currentRegion.getEndKey();
        if (endKey == null ||
            Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY) ||
            checkScanStopRow(endKey) ||
            done) {
          close();
          if (LOG.isTraceEnabled()) {
            LOG.trace("Finished " + this.currentRegion);
          }
          return false;
        }
        localStartKey = endKey;
        if (LOG.isTraceEnabled()) {
          LOG.trace("Finished " + this.currentRegion);
        }
      } else {
        localStartKey = this.scan.getStartRow();
      }

      if (LOG.isDebugEnabled() && this.currentRegion != null) {
        // Only worth logging if NOT first region in scan.
        LOG.debug("Advancing internal scanner to startKey at '" +
          Bytes.toStringBinary(localStartKey) + "'");
      }
      try {
        callable = getScannerCallable(localStartKey, nbRows);
        // Open a scanner on the region server starting at the
        // beginning of the region
        call(scan, callable, caller, scannerTimeout);
        this.currentRegion = callable.getHRegionInfo();
        if (this.scanMetrics != null) {
          this.scanMetrics.countOfRegions.incrementAndGet();
        }
      } catch (IOException e) {
        close();
        throw e;
      }
      return true;
    }


  Result[] call(Scan scan, ScannerCallableWithReplicas callable,
      RpcRetryingCaller<Result[]> caller, int scannerTimeout)
      throws IOException, RuntimeException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }
    // callWithoutRetries is at this layer. Within the ScannerCallableWithReplicas,
    // we do a callWithRetries
    return caller.callWithoutRetries(callable, scannerTimeout);
  }

    @InterfaceAudience.Private
    protected ScannerCallableWithReplicas getScannerCallable(byte [] localStartKey,
        int nbRows) {
      scan.setStartRow(localStartKey);
      ScannerCallable s =
          new ScannerCallable(getConnection(), getTable(), scan, this.scanMetrics,
              this.rpcControllerFactory);
      s.setCaching(nbRows);
      ScannerCallableWithReplicas sr = new ScannerCallableWithReplicas(tableName, getConnection(),
       s, pool, primaryOperationTimeout, scan,
       retries, scannerTimeout, caching, conf, caller);
      return sr;
    }

    /**
     * Publish the scan metrics. For now, we use scan.setAttribute to pass the metrics back to the
     * application or TableInputFormat.Later, we could push it to other systems. We don't use metrics
     * framework because it doesn't support multi-instances of the same metrics on the same machine;
     * for scan/map reduce scenarios, we will have multiple scans running at the same time.
     *
     * By default, scan metrics are disabled; if the application wants to collect them, this
     * behavior can be turned on by calling calling {@link Scan#setScanMetricsEnabled(boolean)}
     *
     * <p>This invocation clears the scan metrics. Metrics are aggregated in the Scan instance.
     */
    protected void writeScanMetrics() {
      if (this.scanMetrics == null || scanMetricsPublished) {
        return;
      }
      MapReduceProtos.ScanMetrics pScanMetrics = ProtobufUtil.toScanMetrics(scanMetrics);
      scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA, pScanMetrics.toByteArray());
      scanMetricsPublished = true;
    }

    @Override
    public Result next() throws IOException {
      // If the scanner is closed and there's nothing left in the cache, next is a no-op.
      if (cache.size() == 0 && this.closed) {
        return null;
      }
      if (cache.size() == 0) {
        loadCache();
      }

      if (cache.size() > 0) {
        return cache.poll();
      }

      // if we exhausted this scanner before calling close, write out the scan metrics
      writeScanMetrics();
      return null;
    }

  /**
   * Contact the servers to load more {@link Result}s in the cache.
   */
  protected void loadCache() throws IOException {
    Result[] values = null;
    long remainingResultSize = maxScannerResultSize;
    int countdown = this.caching;

    // We need to reset it if it's a new callable that was created
    // with a countdown in nextScanner
    callable.setCaching(this.caching);
    // This flag is set when we want to skip the result returned.  We do
    // this when we reset scanner because it split under us.
    boolean skipFirst = false;
    boolean retryAfterOutOfOrderException = true;
    // We don't expect that the server will have more results for us if
    // it doesn't tell us otherwise. We rely on the size or count of results
    boolean serverHasMoreResults = false;
    do {
      try {
        if (skipFirst) {
          // Skip only the first row (which was the last row of the last
          // already-processed batch).
          callable.setCaching(1);
          values = call(scan, callable, caller, scannerTimeout);
          // When the replica switch happens, we need to do certain operations
          // again. The scannercallable will openScanner with the right startkey
          // but we need to pick up from there. Bypass the rest of the loop
          // and let the catch-up happen in the beginning of the loop as it
          // happens for the cases where we see exceptions. Since only openScanner
          // would have happened, values would be null
          if (values == null && callable.switchedToADifferentReplica()) {
            if (this.lastResult != null) { //only skip if there was something read earlier
              skipFirst = true;
            }
            this.currentRegion = callable.getHRegionInfo();
            continue;
          }
          callable.setCaching(this.caching);
          skipFirst = false;
        }
        // Server returns a null values if scanning is to stop. Else,
        // returns an empty array if scanning is to go on and we've just
        // exhausted current region.
        values = call(scan,   callable, caller, scannerTimeout);
        if (skipFirst && values != null && values.length == 1) {
          skipFirst = false; // Already skipped, unset it before scanning again
          values = call(scan, callable, caller, scannerTimeout);
        }
        // When the replica switch happens, we need to do certain operations
        // again. The callable will openScanner with the right startkey
        // but we need to pick up from there. Bypass the rest of the loop
        // and let the catch-up happen in the beginning of the loop as it
        // happens for the cases where we see exceptions. Since only openScanner
        // would have happened, values would be null
        if (values == null && callable.switchedToADifferentReplica()) {
          if (this.lastResult != null) { //only skip if there was something read earlier
            skipFirst = true;
          }
          this.currentRegion = callable.getHRegionInfo();
          continue;
        }
        retryAfterOutOfOrderException = true;
      } catch (DoNotRetryIOException e) {
        // DNRIOEs are thrown to make us break out of retries. Some types of DNRIOEs want us
        // to reset the scanner and come back in again.
        if (e instanceof UnknownScannerException) {
          long timeout = lastNext + scannerTimeout;
          // If we are over the timeout, throw this exception to the client wrapped in
          // a ScannerTimeoutException. Else, it's because the region moved and we used the old
          // id against the new region server; reset the scanner.
          if (timeout < System.currentTimeMillis()) {
            long elapsed = System.currentTimeMillis() - lastNext;
            ScannerTimeoutException ex =
                new ScannerTimeoutException(elapsed + "ms passed since the last invocation, "
                    + "timeout is currently set to " + scannerTimeout);
            ex.initCause(e);
            throw ex;
          }
        } else {
          // If exception is any but the list below throw it back to the client; else setup
          // the scanner and retry.
          Throwable cause = e.getCause();
          if ((cause != null && cause instanceof NotServingRegionException) ||
              (cause != null && cause instanceof RegionServerStoppedException) ||
              e instanceof OutOfOrderScannerNextException) {
            // Pass
            // It is easier writing the if loop test as list of what is allowed rather than
            // as a list of what is not allowed... so if in here, it means we do not throw.
          } else {
            throw e;
          }
        }
        // Else, its signal from depths of ScannerCallable that we need to reset the scanner.
        if (this.lastResult != null) {
          // The region has moved. We need to open a brand new scanner at
          // the new location.
          // Reset the startRow to the row we've seen last so that the new
          // scanner starts at the correct row. Otherwise we may see previously
          // returned rows again.
          // (ScannerCallable by now has "relocated" the correct region)
          this.scan.setStartRow(this.lastResult.getRow());

          // Skip first row returned.  We already let it out on previous
          // invocation.
          skipFirst = true;
        }
        if (e instanceof OutOfOrderScannerNextException) {
          if (retryAfterOutOfOrderException) {
            retryAfterOutOfOrderException = false;
          } else {
            // TODO: Why wrap this in a DNRIOE when it already is a DNRIOE?
            throw new DoNotRetryIOException("Failed after retry of " +
                "OutOfOrderScannerNextException: was there a rpc timeout?", e);
          }
        }
        // Clear region.
        this.currentRegion = null;
        // Set this to zero so we don't try and do an rpc and close on remote server when
        // the exception we got was UnknownScanner or the Server is going down.
        callable = null;
        // This continue will take us to while at end of loop where we will set up new scanner.
        continue;
      }
      long currentTime = System.currentTimeMillis();
      if (this.scanMetrics != null) {
        this.scanMetrics.sumOfMillisSecBetweenNexts.addAndGet(currentTime - lastNext);
      }
      lastNext = currentTime;
      if (values != null && values.length > 0) {
        for (Result rs : values) {
          cache.add(rs);
          // We don't make Iterator here
          for (Cell cell : rs.rawCells()) {
            remainingResultSize -= CellUtil.estimatedHeapSizeOf(cell);
          }
          countdown--;
          this.lastResult = rs;
        }
      }
      // We expect that the server won't have more results for us when we exhaust
      // the size (bytes or count) of the results returned. If the server *does* inform us that
      // there are more results, we want to avoid possiblyNextScanner(...). Only when we actually
      // get results is the moreResults context valid.
      if (null != values && values.length > 0 && callable.hasMoreResultsContext()) {
        // Only adhere to more server results when we don't have any partialResults
        // as it keeps the outer loop logic the same.
        serverHasMoreResults = callable.getServerHasMoreResults();
      }
      // Values == null means server-side filter has determined we must STOP
      // !partialResults.isEmpty() means that we are still accumulating partial Results for a
      // row. We should not change scanners before we receive all the partial Results for that
      // row.
    } while (remainingResultSize > 0 && countdown > 0 && !serverHasMoreResults
        && possiblyNextScanner(countdown, values == null));
  }

    @Override
    public void close() {
      if (!scanMetricsPublished) writeScanMetrics();
      if (callable != null) {
        callable.setClose();
        try {
          call(scan, callable, caller, scannerTimeout);
        } catch (UnknownScannerException e) {
           // We used to catch this error, interpret, and rethrow. However, we
           // have since decided that it's not nice for a scanner's close to
           // throw exceptions. Chances are it was just due to lease time out.
        } catch (IOException e) {
           /* An exception other than UnknownScanner is unexpected. */
           LOG.warn("scanner failed to close. Exception follows: " + e);
        }
        callable = null;
      }
      closed = true;
    }
}
