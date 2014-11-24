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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

/**
 * Client scanner for small scan. Generally, only one RPC is called to fetch the
 * scan results, unless the results cross multiple regions or the row count of
 * results excess the caching.
 *
 * For small scan, it will get better performance than {@link ClientScanner}
 */
@InterfaceAudience.Private
public class ClientSmallScanner extends ClientScanner {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private ScannerCallableWithReplicas smallScanCallable = null;
  // When fetching results from server, skip the first result if it has the same
  // row with this one
  private byte[] skipRowOfFirstResult = null;

  /**
   * Create a new ShortClientScanner for the specified table Note that the
   * passed {@link Scan}'s start row maybe changed changed.
   *
   * @param conf The {@link Configuration} to use.
   * @param scan {@link Scan} to use in this scanner
   * @param tableName The table that we wish to rangeGet
   * @param connection Connection identifying the cluster
   * @param rpcFactory
   * @param pool
   * @param primaryOperationTimeout
   * @throws IOException
   */
  public ClientSmallScanner(final Configuration conf, final Scan scan,
      final TableName tableName, ClusterConnection connection,
      RpcRetryingCallerFactory rpcFactory, RpcControllerFactory controllerFactory,
      ExecutorService pool, int primaryOperationTimeout) throws IOException {
    super(conf, scan, tableName, connection, rpcFactory, controllerFactory, pool,
           primaryOperationTimeout);
  }

  @Override
  protected void initializeScannerInConstruction() throws IOException {
    // No need to initialize the scanner when constructing instance, do it when
    // calling next(). Do nothing here.
  }

  /**
   * Gets a scanner for following scan. Move to next region or continue from the
   * last result or start from the start row.
   * @param nbRows
   * @param done true if Server-side says we're done scanning.
   * @param currentRegionDone true if scan is over on current region
   * @return true if has next scanner
   * @throws IOException
   */
  private boolean nextScanner(int nbRows, final boolean done,
      boolean currentRegionDone) throws IOException {
    // Where to start the next getter
    byte[] localStartKey;
    int cacheNum = nbRows;
    skipRowOfFirstResult = null;
    // if we're at end of table, close and return false to stop iterating
    if (this.currentRegion != null && currentRegionDone) {
      byte[] endKey = this.currentRegion.getEndKey();
      if (endKey == null || Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY)
          || checkScanStopRow(endKey) || done) {
        close();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Finished with small scan at " + this.currentRegion);
        }
        return false;
      }
      localStartKey = endKey;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished with region " + this.currentRegion);
      }
    } else if (this.lastResult != null) {
      localStartKey = this.lastResult.getRow();
      skipRowOfFirstResult = this.lastResult.getRow();
      cacheNum++;
    } else {
      localStartKey = this.scan.getStartRow();
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Advancing internal small scanner to startKey at '"
          + Bytes.toStringBinary(localStartKey) + "'");
    }
    smallScanCallable = getSmallScanCallable(
        getConnection(), getTable(), scan, getScanMetrics(), localStartKey, cacheNum,
        rpcControllerFactory, getPool(), getPrimaryOperationTimeout(),
        getRetries(), getScannerTimeout(), getConf(), caller);
    if (this.scanMetrics != null && skipRowOfFirstResult == null) {
      this.scanMetrics.countOfRegions.incrementAndGet();
    }
    return true;
  }


  static ScannerCallableWithReplicas getSmallScanCallable(
      ClusterConnection connection, TableName table, Scan scan,
      ScanMetrics scanMetrics,  byte[] localStartKey, final int cacheNum,
      RpcControllerFactory controllerFactory, ExecutorService pool, int primaryOperationTimeout,
      int retries, int scannerTimeout, Configuration conf, RpcRetryingCaller<Result []> caller) {
    scan.setStartRow(localStartKey);
    SmallScannerCallable s = new SmallScannerCallable(
      connection, table, scan, scanMetrics, controllerFactory, cacheNum, 0);
    ScannerCallableWithReplicas scannerCallableWithReplicas =
        new ScannerCallableWithReplicas(table, connection,
            s, pool, primaryOperationTimeout, scan, retries,
            scannerTimeout, cacheNum, conf, caller);
    return scannerCallableWithReplicas;
  }

  static class SmallScannerCallable extends ScannerCallable {
    public SmallScannerCallable(
        ClusterConnection connection, TableName table, Scan scan,
        ScanMetrics scanMetrics, RpcControllerFactory controllerFactory, int caching, int id) {
      super(connection, table, scan, scanMetrics, controllerFactory, id);
      this.setCaching(caching);
    }

    @Override
    public Result[] call(int timeout) throws IOException {
      if (this.closed) return null;
      if (Thread.interrupted()) {
        throw new InterruptedIOException();
      }
      ScanRequest request = RequestConverter.buildScanRequest(getLocation()
          .getRegionInfo().getRegionName(), getScan(), getCaching(), true);
      ScanResponse response = null;
      PayloadCarryingRpcController controller = controllerFactory.newController();
      try {
        controller.setPriority(getTableName());
        controller.setCallTimeout(timeout);
        response = getStub().scan(controller, request);
        return ResponseConverter.getResults(controller.cellScanner(),
            response);
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      }
    }

    @Override
    public ScannerCallable getScannerCallableForReplica(int id) {
      return new SmallScannerCallable((ClusterConnection)connection, tableName, getScan(), scanMetrics,
        controllerFactory, getCaching(), id);
    }
  }

  @Override
  public Result next() throws IOException {
    // If the scanner is closed and there's nothing left in the cache, next is a
    // no-op.
    if (cache.size() == 0 && this.closed) {
      return null;
    }
    if (cache.size() == 0) {
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
        // callWithoutRetries is at this layer. Within the ScannerCallableWithReplicas,
        // we do a callWithRetries
        values = this.caller.callWithoutRetries(smallScanCallable, scannerTimeout);
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
            if (i == 0 && this.skipRowOfFirstResult != null
                && Bytes.equals(skipRowOfFirstResult, rs.getRow())) {
              // Skip the first result
              continue;
            }
            cache.add(rs);
            // We don't make Iterator here
            for (Cell cell : rs.rawCells()) {
              remainingResultSize -= CellUtil.estimatedHeapSizeOf(cell);
            }
            countdown--;
            this.lastResult = rs;
          }
        }
        currentRegionDone = countdown > 0;
      }
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
  public void close() {
    if (!scanMetricsPublished) writeScanMetrics();
    closed = true;
  }
}
