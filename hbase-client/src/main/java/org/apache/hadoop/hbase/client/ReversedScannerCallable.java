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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * A reversed ScannerCallable which supports backward scanning.
 */
@InterfaceAudience.Private
public class ReversedScannerCallable extends ScannerCallable {
  /**
   * The start row for locating regions. In reversed scanner, may locate the
   * regions for a range of keys when doing
   * {@link ReversedClientScanner#nextScanner(int, boolean)}
   */
  protected final byte[] locateStartRow;

  /**
   * @param connection
   * @param tableName
   * @param scan
   * @param scanMetrics
   * @param locateStartRow The start row for locating regions
   * @param rpcFactory to create an 
   * {@link com.google.protobuf.RpcController} to talk to the regionserver
   */
  public ReversedScannerCallable(ClusterConnection connection, TableName tableName, Scan scan,
      ScanMetrics scanMetrics, byte[] locateStartRow, RpcControllerFactory rpcFactory) {
    super(connection, tableName, scan, scanMetrics, rpcFactory);
    this.locateStartRow = locateStartRow;
  }

  /**
   * @param connection
   * @param tableName
   * @param scan
   * @param scanMetrics
   * @param locateStartRow The start row for locating regions
   * @param rpcFactory to create an 
   * {@link com.google.protobuf.RpcController} to talk to the regionserver
   * @param replicaId the replica id
   */
  public ReversedScannerCallable(ClusterConnection connection, TableName tableName, Scan scan,
      ScanMetrics scanMetrics, byte[] locateStartRow, RpcControllerFactory rpcFactory, int replicaId) {
    super(connection, tableName, scan, scanMetrics, rpcFactory, replicaId);
    this.locateStartRow = locateStartRow;
  }

  /**
   * @deprecated use
   *             {@link #ReversedScannerCallable(ClusterConnection, TableName, Scan, ScanMetrics, byte[], RpcControllerFactory )}
   */
  @Deprecated
  public ReversedScannerCallable(ClusterConnection connection, TableName tableName,
      Scan scan, ScanMetrics scanMetrics, byte[] locateStartRow) {
    this(connection, tableName, scan, scanMetrics, locateStartRow, RpcControllerFactory
        .instantiate(connection.getConfiguration()));
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
      if (locateStartRow == null) {
        // Just locate the region with the row
        RegionLocations rl = RpcRetryingCallerWithReadReplicas.getRegionLocations(reload, id,
            getConnection(), tableName, row);
        this.location = id < rl.size() ? rl.getRegionLocation(id) : null;
        if (this.location == null) {
          throw new IOException("Failed to find location, tableName="
              + tableName + ", row=" + Bytes.toStringBinary(row) + ", reload="
              + reload);
        }
      } else {
        // Need to locate the regions with the range, and the target location is
        // the last one which is the previous region of last region scanner
        List<HRegionLocation> locatedRegions = locateRegionsInRange(
            locateStartRow, row, reload);
        if (locatedRegions.isEmpty()) {
          throw new DoNotRetryIOException(
              "Does hbase:meta exist hole? Couldn't get regions for the range from "
                  + Bytes.toStringBinary(locateStartRow) + " to "
                  + Bytes.toStringBinary(row));
        }
        this.location = locatedRegions.get(locatedRegions.size() - 1);
      }
      setStub(getConnection().getClient(getLocation().getServerName()));
      checkIfRegionServerIsRemote();
      instantiated = true;
    }

    // check how often we retry.
    // HConnectionManager will call instantiateServer with reload==true
    // if and only if for retries.
    if (reload && this.scanMetrics != null) {
      this.scanMetrics.countOfRPCRetries.incrementAndGet();
      if (isRegionServerRemote) {
        this.scanMetrics.countOfRemoteRPCRetries.incrementAndGet();
      }
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
    List<HRegionLocation> regionList = new ArrayList<HRegionLocation>();
    byte[] currentKey = startKey;
    do {
      RegionLocations rl = RpcRetryingCallerWithReadReplicas.getRegionLocations(reload, id,
          getConnection(), tableName, currentKey);
      HRegionLocation regionLocation = id < rl.size() ? rl.getRegionLocation(id) : null;
      if (regionLocation != null && regionLocation.getRegionInfo().containsRow(currentKey)) {
        regionList.add(regionLocation);
      } else {
        throw new DoNotRetryIOException("Does hbase:meta exist hole? Locating row "
            + Bytes.toStringBinary(currentKey) + " returns incorrect region "
            + regionLocation.getRegionInfo());
      }
      currentKey = regionLocation.getRegionInfo().getEndKey();
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
        && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
    return regionList;
  }

  @Override
  public ScannerCallable getScannerCallableForReplica(int id) {
    ReversedScannerCallable r = new ReversedScannerCallable(this.cConnection, this.tableName,
        this.getScan(), this.scanMetrics, this.locateStartRow, controllerFactory, id);
    r.setCaching(this.getCaching());
    return r;
  }
}
