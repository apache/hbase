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

import static org.apache.hadoop.hbase.client.ConnectionUtils.createCloseRowBefore;
import static org.apache.hadoop.hbase.client.ConnectionUtils.incRPCRetriesMetrics;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStartRow;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * A reversed ScannerCallable which supports backward scanning.
 */
@InterfaceAudience.Private
public class ReversedScannerCallable extends ScannerCallable {

  private byte[] locationSearchKey;

  /**
   * @param connection which connection
   * @param tableName table callable is on
   * @param scan the scan to execute
   * @param scanMetrics the ScanMetrics to used, if it is null, ScannerCallable won't collect
   *          metrics
   * @param rpcFactory to create an {@link com.google.protobuf.RpcController} to talk to the
   *          regionserver
   * @param replicaId the replica id
   */
  public ReversedScannerCallable(ClusterConnection connection, TableName tableName, Scan scan,
    ScanMetrics scanMetrics, RpcControllerFactory rpcFactory, int replicaId) {
    super(connection, tableName, scan, scanMetrics, rpcFactory, replicaId);
  }

  @Override
  public void throwable(Throwable t, boolean retrying) {
    // for reverse scans, we need to update cache using the search key found for the reverse scan
    // range in prepare. Otherwise, we will see weird behavior at the table boundaries,
    // when trying to clear cache for an empty row.
    if (location != null && locationSearchKey != null) {
      getConnection().updateCachedLocations(getTableName(),
          location.getRegionInfo().getRegionName(),
          locationSearchKey, t, location.getServerName());
    }
  }

  /**
   * @param reload force reload of server location
   */
  @Override
  public void prepare(boolean reload) throws IOException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }

    if (reload && getTableName() != null && !getTableName().equals(TableName.META_TABLE_NAME)
      && getConnection().isTableDisabled(getTableName())) {
      throw new TableNotEnabledException(getTableName().getNameAsString() + " is disabled.");
    }

    if (!instantiated || reload) {
      // we should use range locate if
      // 1. we do not want the start row
      // 2. the start row is empty which means we need to locate to the last region.
      if (scan.includeStartRow() && !isEmptyStartRow(getRow())) {
        // Just locate the region with the row
        RegionLocations rl = getRegionLocationsForPrepare(getRow());
        this.location = getLocationForReplica(rl);
        this.locationSearchKey = getRow();
      } else {
        // The locateStart row is an approximation. So we need to search between
        // that and the actual row in order to really find the last region
        byte[] locateStartRow = createCloseRowBefore(getRow());
        Pair<HRegionLocation, byte[]> lastRegionAndKey = locateLastRegionInRange(
            locateStartRow, getRow());
        this.location = lastRegionAndKey.getFirst();
        this.locationSearchKey = lastRegionAndKey.getSecond();
      }

      if (location == null || location.getServerName() == null) {
        throw new IOException("Failed to find location, tableName="
          + getTableName() + ", row=" + Bytes.toStringBinary(getRow()) + ", reload="
          + reload);
      }

      setStub(getConnection().getClient(getLocation().getServerName()));
      checkIfRegionServerIsRemote();
      instantiated = true;
    }

    // check how often we retry.
    if (reload) {
      incRPCRetriesMetrics(scanMetrics, isRegionServerRemote);
    }
  }

  /**
   * Get the last region before the endkey, which will be used to execute the reverse scan
   * @param startKey Starting row in range, inclusive
   * @param endKey Ending row in range, exclusive
   * @return The last location, and the rowKey used to find it. May be null,
   *    if a region could not be found.
   */
  private Pair<HRegionLocation, byte[]> locateLastRegionInRange(byte[] startKey, byte[] endKey)
      throws IOException {
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey,
        HConstants.EMPTY_END_ROW);
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException("Invalid range: "
          + Bytes.toStringBinary(startKey) + " > "
          + Bytes.toStringBinary(endKey));
    }

    HRegionLocation lastRegion = null;
    byte[] lastFoundKey = null;
    byte[] currentKey = startKey;

    do {
      RegionLocations rl = getRegionLocationsForPrepare(currentKey);
      HRegionLocation regionLocation = getLocationForReplica(rl);
      if (regionLocation.getRegionInfo().containsRow(currentKey)) {
        lastFoundKey = currentKey;
        lastRegion = regionLocation;
      } else {
        throw new DoNotRetryIOException(
          "Does hbase:meta exist hole? Locating row " + Bytes.toStringBinary(currentKey) +
            " returns incorrect region " + regionLocation.getRegionInfo());
      }
      currentKey = regionLocation.getRegionInfo().getEndKey();
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
        && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));

    return new Pair<>(lastRegion, lastFoundKey);
  }

  @Override
  public ScannerCallable getScannerCallableForReplica(int id) {
    ReversedScannerCallable r = new ReversedScannerCallable(getConnection(), getTableName(),
        this.getScan(), this.scanMetrics, rpcControllerFactory, id);
    r.setCaching(this.getCaching());
    return r;
  }
}
