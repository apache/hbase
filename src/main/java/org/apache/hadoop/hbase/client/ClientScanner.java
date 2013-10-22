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
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 * Implements the scanner interface for the HBase client.
 * If there are multiple regions in a table, this scanner will iterate
 * through them all.
 */
public class ClientScanner extends AbstractClientScanner {
    private final Log LOG = LogFactory.getLog(this.getClass());
    protected Scan scan;
    protected boolean closed = false;
    // Current region scanner is against.  Gets cleared if current region goes
    // wonky: e.g. if it splits on us.
    protected HRegionInfo currentRegion = null;
    private ScannerCallable callable = null;
    protected final LinkedList<Result> cache = new LinkedList<Result>();
    protected final int caching;
    protected long lastNext;
    // Keep lastResult returned successfully in case we have to reset scanner.
    protected Result lastResult = null;
    protected ScanMetrics scanMetrics = null;
    protected final long maxScannerResultSize;
    private final HConnection connection;
    private final byte[] tableName;
    private final int scannerTimeout;

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
    public ClientScanner(final Configuration conf, final Scan scan,
        final byte[] tableName) throws IOException {
      this(conf, scan, tableName, HConnectionManager.getConnection(conf));
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
    public ClientScanner(final Configuration conf, final Scan scan,
      final byte[] tableName, HConnection connection) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating scanner over "
            + Bytes.toString(tableName)
            + " starting at key '" + Bytes.toStringBinary(scan.getStartRow()) + "'");
      }
      this.scan = scan;
      this.tableName = tableName;
      this.lastNext = System.currentTimeMillis();
      this.connection = connection;
      this.maxScannerResultSize = conf.getLong(
          HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
          HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
      this.scannerTimeout = (int) conf.getLong(
          HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
          HConstants.DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD);

      // check if application wants to collect scan metrics
      byte[] enableMetrics = scan.getAttribute(
        Scan.SCAN_ATTRIBUTES_METRICS_ENABLE);
      if (enableMetrics != null && Bytes.toBoolean(enableMetrics)) {
        scanMetrics = new ScanMetrics();
      }

      // Use the caching from the Scan.  If not set, use the default cache setting for this table.
      if (this.scan.getCaching() > 0) {
        this.caching = this.scan.getCaching();
      } else {
        this.caching = conf.getInt("hbase.client.scanner.caching", 1);
      }

      // initialize the scanner
      initializeScannerInConstruction();
    }
    
    protected void initializeScannerInConstruction() throws IOException{
      // initialize the scanner
      nextScanner(this.caching, false);
    }

    protected HConnection getConnection() {
      return this.connection;
    }

    protected byte[] getTableName() {
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
    private boolean nextScanner(int nbRows, final boolean done)
    throws IOException {
      // Close the previous scanner if it's open
      if (this.callable != null) {
        this.callable.setClose();
        callable.withRetries();
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
          if (LOG.isDebugEnabled()) {
            LOG.debug("Finished with scanning at " + this.currentRegion);
          }
          return false;
        }
        localStartKey = endKey;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Finished with region " + this.currentRegion);
        }
      } else {
        localStartKey = this.scan.getStartRow();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Advancing internal scanner to startKey at '" +
          Bytes.toStringBinary(localStartKey) + "'");
      }
      try {
        callable = getScannerCallable(localStartKey, nbRows);
        // Open a scanner on the region server starting at the
        // beginning of the region
        callable.withRetries();
        this.currentRegion = callable.getHRegionInfo();
        if (this.scanMetrics != null) {
          this.scanMetrics.countOfRegions.inc();
        }
      } catch (IOException e) {
        close();
        throw e;
      }
      return true;
    }

    protected ScannerCallable getScannerCallable(byte [] localStartKey,
        int nbRows) {
      scan.setStartRow(localStartKey);
      ScannerCallable s = new ScannerCallable(getConnection(),
        getTableName(), scan, this.scanMetrics);
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
    protected void writeScanMetrics() throws IOException {
      if (this.scanMetrics == null) {
        return;
      }
      final DataOutputBuffer d = new DataOutputBuffer();
      scanMetrics.write(d);
      scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA, d.getData());
    }

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
        do {
          try {
            if (skipFirst) {
              // Skip only the first row (which was the last row of the last
              // already-processed batch).
              callable.setCaching(1);
              values = callable.withRetries();
              callable.setCaching(this.caching);
              skipFirst = false;
            }
            // Server returns a null values if scanning is to stop.  Else,
            // returns an empty array if scanning is to go on and we've just
            // exhausted current region.
            values = callable.withRetries();
          } catch (DoNotRetryIOException e) {
            if (e instanceof UnknownScannerException) {
              long timeout = lastNext + scannerTimeout;
              // If we are over the timeout, throw this exception to the client
              // Else, it's because the region moved and we used the old id
              // against the new region server; reset the scanner.
              if (timeout < System.currentTimeMillis()) {
                long elapsed = System.currentTimeMillis() - lastNext;
                ScannerTimeoutException ex = new ScannerTimeoutException(
                    elapsed + "ms passed since the last invocation, " +
                        "timeout is currently set to " + scannerTimeout);
                ex.initCause(e);
                throw ex;
              }
            } else {
              Throwable cause = e.getCause();
              if (cause == null || (!(cause instanceof NotServingRegionException)
                  && !(cause instanceof RegionServerStoppedException))) {
                throw e;
              }
            }
            // Else, its signal from depths of ScannerCallable that we got an
            // NSRE on a next and that we need to reset the scanner.
            if (this.lastResult != null) {
              this.scan.setStartRow(this.lastResult.getRow());
              // Skip first row returned.  We already let it out on previous
              // invocation.
              skipFirst = true;
            }
            // Clear region
            this.currentRegion = null;
            continue;
          }
          long currentTime = System.currentTimeMillis();
          if (this.scanMetrics != null ) {
            this.scanMetrics.sumOfMillisSecBetweenNexts.inc(currentTime-lastNext);
          }
          lastNext = currentTime;
          if (values != null && values.length > 0) {
            for (Result rs : values) {
              cache.add(rs);
              for (KeyValue kv : rs.raw()) {
                  remainingResultSize -= kv.heapSize();
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

    /**
     * Get <param>nbRows</param> rows.
     * How many RPCs are made is determined by the {@link Scan#setCaching(int)}
     * setting (or hbase.client.scanner.caching in hbase-site.xml).
     * @param nbRows number of rows to return
     * @return Between zero and <param>nbRows</param> RowResults.  Scan is done
     * if returned array is of zero-length (We never return null).
     * @throws IOException
     */
    public Result [] next(int nbRows) throws IOException {
      // Collect values to be returned here
      ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
      for(int i = 0; i < nbRows; i++) {
        Result next = next();
        if (next != null) {
          resultSets.add(next);
        } else {
          break;
        }
      }
      return resultSets.toArray(new Result[resultSets.size()]);
    }

    public void close() {
      if (callable != null) {
        callable.setClose();
        try {
          callable.withRetries();
        } catch (IOException e) {
          // We used to catch this error, interpret, and rethrow. However, we
          // have since decided that it's not nice for a scanner's close to
          // throw exceptions. Chances are it was just an UnknownScanner
          // exception due to lease time out.
        } finally {
          // we want to output the scan metrics even if an error occurred on close
          try {
            writeScanMetrics();
          } catch (IOException e) {
            // As above, we still don't want the scanner close() method to throw.
          }
        }
        callable = null;
      }
      closed = true;
    }
}
