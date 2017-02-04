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

import static org.apache.hadoop.hbase.client.ConnectionUtils.createClosestRowBefore;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ScannerCallable.MoreResults;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ExceptionUtil;

/**
 * A reversed client scanner which support backward scanning
 */
@InterfaceAudience.Private
public class ReversedClientScanner extends ClientScanner {
  private static final Log LOG = LogFactory.getLog(ReversedClientScanner.class);

  /**
   * Create a new ReversibleClientScanner for the specified table Note that the
   * passed {@link Scan}'s start row maybe changed.
   * @param conf
   * @param scan
   * @param tableName
   * @param connection
   * @param pool
   * @param primaryOperationTimeout
   * @throws IOException
   */
  public ReversedClientScanner(Configuration conf, Scan scan,
      TableName tableName, ClusterConnection connection,
      RpcRetryingCallerFactory rpcFactory, RpcControllerFactory controllerFactory,
      ExecutorService pool, int primaryOperationTimeout) throws IOException {
    super(conf, scan, tableName, connection, rpcFactory, controllerFactory, pool,
        primaryOperationTimeout);
  }

  @Override
  protected Result[] nextScanner(int nbRows) throws IOException {
    // Close the previous scanner if it's open
    closeScanner();

    // Where to start the next scanner
    byte[] localStartKey;
    boolean locateTheClosestFrontRow = true;
    // if we're at start of table, close and return false to stop iterating
    if (this.currentRegion != null) {
      byte[] startKey = this.currentRegion.getStartKey();
      if (startKey == null || Bytes.equals(startKey, HConstants.EMPTY_BYTE_ARRAY)
          || checkScanStopRow(startKey)) {
        close();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Finished " + this.currentRegion);
        }
        return null;
      }
      localStartKey = startKey;
      // clear mvcc read point if we are going to switch regions
      scan.resetMvccReadPoint();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished " + this.currentRegion);
      }
    } else {
      localStartKey = this.scan.getStartRow();
      if (!Bytes.equals(localStartKey, HConstants.EMPTY_BYTE_ARRAY)) {
        locateTheClosestFrontRow = false;
      }
    }

    if (LOG.isDebugEnabled() && this.currentRegion != null) {
      // Only worth logging if NOT first region in scan.
      LOG.debug("Advancing internal scanner to startKey at '"
          + Bytes.toStringBinary(localStartKey) + "'");
    }
    try {
      // In reversed scan, we want to locate the previous region through current
      // region's start key. In order to get that previous region, first we
      // create a closest row before the start key of current region, then
      // locate all the regions from the created closest row to start key of
      // current region, thus the last one of located regions should be the
      // previous region of current region. The related logic of locating
      // regions is implemented in ReversedScannerCallable
      byte[] locateStartRow = locateTheClosestFrontRow ? createClosestRowBefore(localStartKey)
          : null;
      callable = getScannerCallable(localStartKey, nbRows, locateStartRow);
      // Open a scanner on the region server starting at the
      // beginning of the region
      // callWithoutRetries is at this layer. Within the ScannerCallableWithReplicas,
      // we do a callWithRetries
      Result[] rrs = this.caller.callWithoutRetries(callable, scannerTimeout);
      this.currentRegion = callable.getHRegionInfo();
      if (this.scanMetrics != null) {
        this.scanMetrics.countOfRegions.incrementAndGet();
      }
      if (rrs != null && rrs.length == 0 && callable.moreResultsForScan() == MoreResults.NO) {
        // no results for the scan, return null to terminate the scan.
        return null;
      }
      return rrs;
    } catch (IOException e) {
      ExceptionUtil.rethrowIfInterrupt(e);
      close();
      throw e;
    }
  }

  protected ScannerCallableWithReplicas getScannerCallable(byte[] localStartKey,
      int nbRows, byte[] locateStartRow) {
    scan.setStartRow(localStartKey);
    ScannerCallable s =
        new ReversedScannerCallable(getConnection(), getTable(), scan, this.scanMetrics,
            locateStartRow, this.rpcControllerFactory);
    s.setCaching(nbRows);
    ScannerCallableWithReplicas sr =
        new ScannerCallableWithReplicas(getTable(), getConnection(), s, pool,
            primaryOperationTimeout, scan, getRetries(), getScannerTimeout(), caching, getConf(),
            caller);
    return sr;
  }

  @Override
  // returns true if stopRow >= passed region startKey
  protected boolean checkScanStopRow(final byte[] startKey) {
    if (this.scan.getStopRow().length > 0) {
      // there is a stop row, check to see if we are past it.
      byte[] stopRow = scan.getStopRow();
      int cmp = Bytes.compareTo(stopRow, 0, stopRow.length, startKey, 0,
          startKey.length);
      if (cmp >= 0) {
        // stopRow >= startKey (stopRow is equals to or larger than endKey)
        // This is a stop.
        return true;
      }
    }
    return false; // unlikely.
  }
}
