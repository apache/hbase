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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * A reversed ScannerCallable which supports backward scanning.
 */
@InterfaceAudience.Private
public class ReversedScannerCallable extends ScannerCallable {

  /**
   * @param connection
   * @param tableName
   * @param scan
   * @param scanMetrics
   * @param rpcFactory to create an {@link com.google.protobuf.RpcController} to talk to the
   *          regionserver
   */
  public ReversedScannerCallable(ConnectionImplementation connection, TableName tableName,
      Scan scan, ScanMetrics scanMetrics, RpcControllerFactory rpcFactory) {
    super(connection, tableName, scan, scanMetrics, rpcFactory);
  }

  /**
   * @param connection
   * @param tableName
   * @param scan
   * @param scanMetrics
   * @param rpcFactory to create an {@link com.google.protobuf.RpcController} to talk to the
   *          regionserver
   * @param replicaId the replica id
   */
  public ReversedScannerCallable(ConnectionImplementation connection, TableName tableName,
      Scan scan, ScanMetrics scanMetrics, RpcControllerFactory rpcFactory, int replicaId) {
    super(connection, tableName, scan, scanMetrics, rpcFactory, replicaId);
  }

  /**
   * @param reload force reload of server location
   * @throws IOException
   */
  @Override
  public void prepare(boolean reload) throws IOException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }
    if (!instantiated || reload) {
      // we should use range locate if
      // 1. we do not want the start row
      // 2. the start row is empty which means we need to locate to the last region.
      if (scan.includeStartRow() && !isEmptyStartRow(getRow())) {
        // Just locate the region with the row
        RegionLocations rl = RpcRetryingCallerWithReadReplicas.getRegionLocations(!reload, id,
            getConnection(), getTableName(), getRow());
        this.location = id < rl.size() ? rl.getRegionLocation(id) : null;
        if (location == null || location.getServerName() == null) {
          throw new IOException("Failed to find location, tableName="
              + getTableName() + ", row=" + Bytes.toStringBinary(getRow()) + ", reload="
              + reload);
        }
      } else {
        // Need to locate the regions with the range, and the target location is
        // the last one which is the previous region of last region scanner
        byte[] locateStartRow = createCloseRowBefore(getRow());
        List<HRegionLocation> locatedRegions = locateRegionsInRange(
            locateStartRow, getRow(), reload);
        if (locatedRegions.isEmpty()) {
          throw new DoNotRetryIOException(
              "Does hbase:meta exist hole? Couldn't get regions for the range from "
                  + Bytes.toStringBinary(locateStartRow) + " to "
                  + Bytes.toStringBinary(getRow()));
        }
        this.location = locatedRegions.get(locatedRegions.size() - 1);
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
   * Get the corresponding regions for an arbitrary range of keys.
   * @param startKey Starting row in range, inclusive
   * @param endKey Ending row in range, exclusive
   * @param reload force reload of server location
   * @return A list of HRegionLocation corresponding to the regions that contain
   *         the specified range
   * @throws IOException
   */
  private List<HRegionLocation> locateRegionsInRange(byte[] startKey,
      byte[] endKey, boolean reload) throws IOException {
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey,
        HConstants.EMPTY_END_ROW);
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException("Invalid range: "
          + Bytes.toStringBinary(startKey) + " > "
          + Bytes.toStringBinary(endKey));
    }
    List<HRegionLocation> regionList = new ArrayList<>();
    byte[] currentKey = startKey;
    do {
      RegionLocations rl = RpcRetryingCallerWithReadReplicas.getRegionLocations(!reload, id,
          getConnection(), getTableName(), currentKey);
      HRegionLocation regionLocation = id < rl.size() ? rl.getRegionLocation(id) : null;
      if (regionLocation != null && regionLocation.getRegionInfo().containsRow(currentKey)) {
        regionList.add(regionLocation);
      } else {
        throw new DoNotRetryIOException("Does hbase:meta exist hole? Locating row "
            + Bytes.toStringBinary(currentKey) + " returns incorrect region "
            + (regionLocation == null ? null : regionLocation.getRegionInfo()));
      }
      currentKey = regionLocation.getRegionInfo().getEndKey();
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
        && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
    return regionList;
  }

  @Override
  public ScannerCallable getScannerCallableForReplica(int id) {
    ReversedScannerCallable r = new ReversedScannerCallable(getConnection(), getTableName(),
        this.getScan(), this.scanMetrics, rpcControllerFactory, id);
    r.setCaching(this.getCaching());
    return r;
  }
}
