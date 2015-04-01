/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.client;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClientSmallScanner.SmallScannerCallableFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

/**
 * Client scanner for small reversed scan. Generally, only one RPC is called to fetch the
 * scan results, unless the results cross multiple regions or the row count of
 * results exceed the caching.
 * <p/>
 * For small scan, it will get better performance than {@link ReversedClientScanner}
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ClientSmallReversedScanner extends ReversedClientScanner {
  private static final Log LOG = LogFactory.getLog(ClientSmallReversedScanner.class);
  private RegionServerCallable<Result[]> smallScanCallable = null;
  private SmallScannerCallableFactory callableFactory;

  /**
   * Create a new ReversibleClientScanner for the specified table Note that the
   * passed {@link org.apache.hadoop.hbase.client.Scan}'s start row maybe changed.
   *
   * @param conf       The {@link org.apache.hadoop.conf.Configuration} to use.
   * @param scan       {@link org.apache.hadoop.hbase.client.Scan} to use in this scanner
   * @param tableName  The table that we wish to scan
   * @param connection Connection identifying the cluster
   * @throws java.io.IOException
   */
  public ClientSmallReversedScanner(Configuration conf, Scan scan, TableName tableName,
                                    HConnection connection) throws IOException {
    this(conf, scan, tableName, connection, new SmallScannerCallableFactory());
  }

  /**
   * Create a new ReversibleClientScanner for the specified table. Take note that the passed
   * {@link Scan}'s start row may be changed.
   *
   * @param conf
   *          The {@link Configuration} to use.
   * @param scan
   *          {@link Scan} to use in this scanner
   * @param tableName
   *          The table that we wish to rangeGet
   * @param connection
   *          Connection identifying the cluster
   * @param callableFactory
   *          Factory used to create the callable for scans
   * @throws IOException
   *           If the remote call fails
   */
  @VisibleForTesting
  ClientSmallReversedScanner(Configuration conf, Scan scan, TableName tableName,
      HConnection connection, SmallScannerCallableFactory callableFactory) throws IOException {
    super(conf, scan, tableName, connection);
    this.callableFactory = callableFactory;
  }

  /**
   * Gets a scanner for following scan. Move to next region or continue from the last result or
   * start from the start row.
   *
   * @param nbRows
   * @param done
   *          true if Server-side says we're done scanning.
   * @param currentRegionDone
   *          true if scan is over on current region
   * @return true if has next scanner
   * @throws IOException
   */
  private boolean nextScanner(int nbRows, final boolean done,
                              boolean currentRegionDone) throws IOException {
    // Where to start the next getter
    byte[] localStartKey;
    int cacheNum = nbRows;
    boolean regionChanged = true;
    // if we're at end of table, close and return false to stop iterating
    if (this.currentRegion != null && currentRegionDone) {
      byte[] startKey = this.currentRegion.getStartKey();
      if (startKey == null
          || Bytes.equals(startKey, HConstants.EMPTY_BYTE_ARRAY)
          || checkScanStopRow(startKey) || done) {
        close();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Finished with small scan at " + this.currentRegion);
        }
        return false;
      }
      // We take the row just under to get to the previous region.
      localStartKey = createClosestRowBefore(startKey);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished with region " + this.currentRegion);
      }
    } else if (this.lastResult != null) {
      regionChanged = false;
      localStartKey = createClosestRowBefore(lastResult.getRow());
    } else {
      localStartKey = this.scan.getStartRow();
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Advancing internal small scanner to startKey at '"
          + Bytes.toStringBinary(localStartKey) + "'");
    }

    smallScanCallable = callableFactory.getCallable(
        scan, getConnection(), getTable(), localStartKey, cacheNum, this.rpcControllerFactory);

    if (this.scanMetrics != null && regionChanged) {
      this.scanMetrics.countOfRegions.incrementAndGet();
    }
    return true;
  }

  @Override
  public Result next() throws IOException {
    // If the scanner is closed and there's nothing left in the cache, next is a
    // no-op.
    if (cache.size() == 0 && this.closed) {
      return null;
    }
    if (cache.size() == 0) {
      loadCache();
    }

    if (cache.size() > 0) {
      return cache.poll();
    }
    // if we exhausted this scanner before calling close, write out the scan
    // metrics
    writeScanMetrics();
    return null;
  }

  @Override
  protected void loadCache() throws IOException {
    Result[] values = null;
    long remainingResultSize = maxScannerResultSize;
    int countdown = this.caching;
    boolean currentRegionDone = false;
    // Values == null means server-side filter has determined we must STOP
    while (remainingResultSize > 0 && countdown > 0
        && nextScanner(countdown, values == null, currentRegionDone)) {
      // Server returns a null values if scanning is to stop. Else,
      // returns an empty array if scanning is to go on and we've just
      // exhausted current region.
      values = this.caller.callWithRetries(smallScanCallable, scannerTimeout);
      this.currentRegion = smallScanCallable.getHRegionInfo();
      long currentTime = System.currentTimeMillis();
      if (this.scanMetrics != null) {
        this.scanMetrics.sumOfMillisSecBetweenNexts.addAndGet(currentTime
            - lastNext);
      }
      lastNext = currentTime;
      if (values != null && values.length > 0) {
        for (int i = 0; i < values.length; i++) {
          Result rs = values[i];
          cache.add(rs);
          for (Cell kv : rs.rawCells()) {
            remainingResultSize -= KeyValueUtil.ensureKeyValue(kv).heapSize();
          }
          countdown--;
          this.lastResult = rs;
        }
      }
      if (smallScanCallable.hasMoreResultsContext()) {
        currentRegionDone = !smallScanCallable.getServerHasMoreResults();
      } else {
        currentRegionDone = countdown > 0;
      }
    }
  }

  @Override
  protected void initializeScannerInConstruction() throws IOException {
    // No need to initialize the scanner when constructing instance, do it when
    // calling next(). Do nothing here.
  }

  @Override
  public void close() {
    if (!scanMetricsPublished) writeScanMetrics();
    closed = true;
  }

  @VisibleForTesting
  protected void setScannerCallableFactory(SmallScannerCallableFactory callableFactory) {
    this.callableFactory = callableFactory;
  }

  @VisibleForTesting
  protected void setRpcRetryingCaller(RpcRetryingCaller<Result []> caller) {
    this.caller = caller;
  }

  @VisibleForTesting
  protected void setRpcControllerFactory(RpcControllerFactory rpcControllerFactory) {
    this.rpcControllerFactory = rpcControllerFactory;
  }
}
