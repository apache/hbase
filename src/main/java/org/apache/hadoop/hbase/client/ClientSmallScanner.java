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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

/**
 * Client scanner for small scan. Generally, only one RPC is called to fetch the
 * scan results, unless the results cross multiple regions or the row count of
 * results excess the caching.
 * 
 * For small scan, it will get better performance than {@link ClientScanner}
 */
public class ClientSmallScanner extends ClientScanner {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private ServerCallable<Result[]> smallScanCallable = null;
  // When fetching results from server, skip the first result if it has the same
  // row with this one
  private byte[] skipRowOfFirstResult = null;

  /**
   * Create a new ClientSmallScanner for the specified table. An HConnection
   * will be retrieved using the passed Configuration. Note that the passed
   * {@link Scan} 's start row maybe changed.
   * 
   * @param conf The {@link Configuration} to use.
   * @param scan {@link Scan} to use in this scanner
   * @param tableName The table that we wish to rangeGet
   * @throws IOException
   */
  public ClientSmallScanner(final Configuration conf, final Scan scan,
      final byte[] tableName) throws IOException {
    this(conf, scan, tableName, HConnectionManager.getConnection(conf));
  }

  /**
   * Create a new ClientSmallScanner for the specified table. An HConnection
   * will be retrieved using the passed Configuration. Note that the passed
   * {@link Scan} 's start row maybe changed.
   * @param conf
   * @param scan
   * @param tableName
   * @param connection
   * @throws IOException
   */
  public ClientSmallScanner(final Configuration conf, final Scan scan,
      final byte[] tableName, HConnection connection) throws IOException {
    super(conf, scan, tableName, connection);
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

    if (LOG.isDebugEnabled()) {
      LOG.debug("Advancing internal small scanner to startKey at '"
          + Bytes.toStringBinary(localStartKey) + "'");
    }
    smallScanCallable = getSmallScanCallable(localStartKey, cacheNum);
    if (this.scanMetrics != null && skipRowOfFirstResult == null) {
      this.scanMetrics.countOfRegions.inc();
    }
    return true;
  }

  private ServerCallable<Result[]> getSmallScanCallable(byte[] localStartKey,
      final int nbRows) {
    this.scan.setStartRow(localStartKey);
    ServerCallable<Result[]> callable = new ServerCallable<Result[]>(
        getConnection(), getTableName(), scan.getStartRow()) {
      public Result[] call() throws IOException {
        return server.scan(location.getRegionInfo().getRegionName(), scan,
            nbRows);
      }
    };
    return callable;
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
        values = smallScanCallable.withRetries();
        this.currentRegion = smallScanCallable.getHRegionInfo();
        long currentTime = System.currentTimeMillis();
        if (this.scanMetrics != null) {
          this.scanMetrics.sumOfMillisSecBetweenNexts.inc(currentTime
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
            for (KeyValue kv : rs.raw()) {
              remainingResultSize -= kv.heapSize();
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
    closed = true;
    try {
      writeScanMetrics();
    } catch (IOException e) {
      // As ClientScanner#close, we don't want the scanner close() method to
      // throw.
    }
  }
}