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
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
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
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ClientScanner extends AbstractClientScanner {
    private final Log LOG = LogFactory.getLog(this.getClass());
    protected Scan scan;
    protected boolean closed = false;
    // Current region scanner is against.  Gets cleared if current region goes
    // wonky: e.g. if it splits on us.
    protected HRegionInfo currentRegion = null;
    protected ScannerCallable callable = null;
    protected final LinkedList<Result> cache = new LinkedList<Result>();
    protected final int caching;
    protected long lastNext;
    // Keep lastResult returned successfully in case we have to reset scanner.
    protected Result lastResult = null;
    protected final long maxScannerResultSize;
    private final HConnection connection;
    private final TableName tableName;
    protected final int scannerTimeout;
    protected boolean scanMetricsPublished = false;
    protected RpcRetryingCaller<Result []> caller;
    protected RpcControllerFactory rpcControllerFactory;

    /**
     * Create a new ClientScanner for the specified table. An HConnection will be
     * retrieved using the passed Configuration.
     * Note that the passed {@link Scan}'s start row maybe changed changed.
     *
     * @param conf The {@link Configuration} to use.
     * @param scan {@link Scan} to use in this scanner
     * @param tableName The table that we wish to scan
     * @throws IOException
     */
    @Deprecated
    public ClientScanner(final Configuration conf, final Scan scan,
        final TableName tableName) throws IOException {
      this(conf, scan, tableName, HConnectionManager.getConnection(conf));
    }

    /**
     * @deprecated Use {@link #ClientScanner(Configuration, Scan, TableName)}
     */
    @Deprecated
    public ClientScanner(final Configuration conf, final Scan scan,
        final byte [] tableName) throws IOException {
      this(conf, scan, TableName.valueOf(tableName));
    }


    /**
     * Create a new ClientScanner for the specified table
     * Note that the passed {@link Scan}'s start row maybe changed changed.
     *
     * @param conf The {@link Configuration} to use.
     * @param scan {@link Scan} to use in this scanner
     * @param tableName The table that we wish to scan
     * @param connection Connection identifying the cluster
     * @throws IOException
     */
  public ClientScanner(final Configuration conf, final Scan scan, final TableName tableName,
      HConnection connection) throws IOException {
    this(conf, scan, tableName, connection,
      RpcRetryingCallerFactory.instantiate(conf, connection.getStatisticsTracker()),
        RpcControllerFactory.instantiate(conf));
  }

  /**
   * @deprecated Use {@link #ClientScanner(Configuration, Scan, TableName, HConnection)}
   */
  @Deprecated
  public ClientScanner(final Configuration conf, final Scan scan, final byte [] tableName,
      HConnection connection) throws IOException {
    this(conf, scan, TableName.valueOf(tableName), connection, new RpcRetryingCallerFactory(conf),
        RpcControllerFactory.instantiate(conf));
  }

  /**
   * @deprecated Use
   *             {@link #ClientScanner(Configuration, Scan, TableName, HConnection,
   *             RpcRetryingCallerFactory, RpcControllerFactory)}
   *             instead
   */
  @Deprecated
  public ClientScanner(final Configuration conf, final Scan scan, final TableName tableName,
      HConnection connection, RpcRetryingCallerFactory rpcFactory) throws IOException {
    this(conf, scan, tableName, connection, rpcFactory, RpcControllerFactory.instantiate(conf));
  }

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
      HConnection connection, RpcRetryingCallerFactory rpcFactory,
      RpcControllerFactory controllerFactory) throws IOException {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Scan table=" + tableName
            + ", startRow=" + Bytes.toStringBinary(scan.getStartRow()));
      }
      this.scan = scan;
      this.tableName = tableName;
      this.lastNext = System.currentTimeMillis();
      this.connection = connection;
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

      initializeScannerInConstruction();
    }

    protected void initializeScannerInConstruction() throws IOException{
      // initialize the scanner
      nextScanner(this.caching, false);
    }

    protected HConnection getConnection() {
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

    protected Scan getScan() {
      return scan;
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
        this.caller.callWithRetries(callable);
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
        this.caller.callWithRetries(callable);
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

    @InterfaceAudience.Private
    protected ScannerCallable getScannerCallable(byte [] localStartKey,
        int nbRows) {
      scan.setStartRow(localStartKey);
      ScannerCallable s = new ScannerCallable(getConnection(),
        getTable(), scan, this.scanMetrics, rpcControllerFactory.newController());
      s.setCaching(nbRows);
      return s;
    }

    /**
     * Publish the scan metrics. For now, we use scan.setAttribute to pass the metrics back to the
     * application or TableInputFormat.Later, we could push it to other systems. We don't use metrics
     * framework because it doesn't support multi-instances of the same metrics on the same machine;
     * for scan/map reduce scenarios, we will have multiple scans running at the same time.
     *
     * By default, scan metrics are disabled; if the application wants to collect them, this behavior
     * can be turned on by calling calling:
     *
     * scan.setAttribute(SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE))
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
        Result [] values = null;
        long remainingResultSize = maxScannerResultSize;
        int countdown = this.caching;
        // We need to reset it if it's a new callable that was created
        // with a countdown in nextScanner
        callable.setCaching(this.caching);
        // This flag is set when we want to skip the result returned.  We do
        // this when we reset scanner because it split under us.
        boolean skipFirst = false;
        boolean retryAfterOutOfOrderException  = true;
        do {
          try {
            if (skipFirst) {
              // Skip only the first row (which was the last row of the last
              // already-processed batch).
              callable.setCaching(1);
              values = this.caller.callWithRetries(callable);
              callable.setCaching(this.caching);
              skipFirst = false;
            }
            // Server returns a null values if scanning is to stop.  Else,
            // returns an empty array if scanning is to go on and we've just
            // exhausted current region.
            values = this.caller.callWithRetries(callable);
            if (skipFirst && values != null && values.length == 1) {
              skipFirst = false; // Already skipped, unset it before scanning again
              values = this.caller.callWithRetries(callable);
            }
            retryAfterOutOfOrderException  = true;
          } catch (DoNotRetryIOException e) {
            // DNRIOEs are thrown to make us break out of retries.  Some types of DNRIOEs want us
            // to reset the scanner and come back in again.
            if (e instanceof UnknownScannerException) {
              long timeout = lastNext + scannerTimeout;
              // If we are over the timeout, throw this exception to the client wrapped in
              // a ScannerTimeoutException. Else, it's because the region moved and we used the old
              // id against the new region server; reset the scanner.
              if (timeout < System.currentTimeMillis()) {
                long elapsed = System.currentTimeMillis() - lastNext;
                ScannerTimeoutException ex = new ScannerTimeoutException(
                    elapsed + "ms passed since the last invocation, " +
                        "timeout is currently set to " + scannerTimeout);
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
          if (this.scanMetrics != null ) {
            this.scanMetrics.sumOfMillisSecBetweenNexts.addAndGet(currentTime-lastNext);
          }
          lastNext = currentTime;
          if (values != null && values.length > 0) {
            for (Result rs : values) {
              cache.add(rs);
              for (Cell kv : rs.rawCells()) {
                // TODO make method in Cell or CellUtil
                remainingResultSize -= KeyValueUtil.ensureKeyValue(kv).heapSize();
              }
              countdown--;
              this.lastResult = rs;
            }
          }
          // Values == null means server-side filter has determined we must STOP
        } while (remainingResultSize > 0 && countdown > 0 && nextScanner(countdown, values == null));
      }

      if (cache.size() > 0) {
        return cache.poll();
      }

      // if we exhausted this scanner before calling close, write out the scan metrics
      writeScanMetrics();
      return null;
    }

    @Override
    public void close() {
      if (!scanMetricsPublished) writeScanMetrics();
      if (callable != null) {
        callable.setClose();
        try {
          this.caller.callWithRetries(callable);
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
