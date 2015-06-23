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
import java.net.ConnectException;
import java.net.SocketTimeoutException;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Similar to {@link RegionServerCallable} but for the AdminService interface. This service callable
 * assumes a Table and row and thus does region locating similar to RegionServerCallable.
 */
@InterfaceAudience.Private
public abstract class RegionAdminServiceCallable<T> implements RetryingCallable<T> {

  protected final ClusterConnection connection;

  protected final RpcControllerFactory rpcControllerFactory;

  protected AdminService.BlockingInterface stub;

  protected HRegionLocation location;

  protected final TableName tableName;
  protected final byte[] row;
  protected final int replicaId;

  protected final static int MIN_WAIT_DEAD_SERVER = 10000;

  public RegionAdminServiceCallable(ClusterConnection connection,
      RpcControllerFactory rpcControllerFactory, TableName tableName, byte[] row) {
    this(connection, rpcControllerFactory, null, tableName, row);
  }

  public RegionAdminServiceCallable(ClusterConnection connection,
      RpcControllerFactory rpcControllerFactory, HRegionLocation location,
      TableName tableName, byte[] row) {
    this(connection, rpcControllerFactory, location,
      tableName, row, RegionReplicaUtil.DEFAULT_REPLICA_ID);
  }

  public RegionAdminServiceCallable(ClusterConnection connection,
      RpcControllerFactory rpcControllerFactory, HRegionLocation location,
      TableName tableName, byte[] row, int replicaId) {
    this.connection = connection;
    this.rpcControllerFactory = rpcControllerFactory;
    this.location = location;
    this.tableName = tableName;
    this.row = row;
    this.replicaId = replicaId;
  }

  @Override
  public void prepare(boolean reload) throws IOException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }

    if (reload || location == null) {
      location = getLocation(!reload);
    }

    if (location == null) {
      // With this exception, there will be a retry.
      throw new HBaseIOException(getExceptionMessage());
    }

    this.setStub(connection.getAdmin(location.getServerName()));
  }

  protected void setStub(AdminService.BlockingInterface stub) {
    this.stub = stub;
  }

  public HRegionLocation getLocation(boolean useCache) throws IOException {
    RegionLocations rl = getRegionLocations(connection, tableName, row, useCache, replicaId);
    if (rl == null) {
      throw new HBaseIOException(getExceptionMessage());
    }
    HRegionLocation location = rl.getRegionLocation(replicaId);
    if (location == null) {
      throw new HBaseIOException(getExceptionMessage());
    }

    return location;
  }

  @Override
  public void throwable(Throwable t, boolean retrying) {
    if (t instanceof SocketTimeoutException ||
        t instanceof ConnectException ||
        t instanceof RetriesExhaustedException ||
        (location != null && getConnection().isDeadServer(location.getServerName()))) {
      // if thrown these exceptions, we clear all the cache entries that
      // map to that slow/dead server; otherwise, let cache miss and ask
      // hbase:meta again to find the new location
      if (this.location != null) getConnection().clearCaches(location.getServerName());
    } else if (t instanceof RegionMovedException) {
      getConnection().updateCachedLocations(tableName, row, t, location);
    } else if (t instanceof NotServingRegionException) {
      // Purge cache entries for this specific region from hbase:meta cache
      // since we don't call connect(true) when number of retries is 1.
      getConnection().deleteCachedRegionLocation(location);
    }
  }

  /**
   * @return {@link HConnection} instance used by this Callable.
   */
  HConnection getConnection() {
    return this.connection;
  }

  //subclasses can override this.
  protected String getExceptionMessage() {
    return "There is no location" + " table=" + tableName
        + " ,replica=" + replicaId + ", row=" + Bytes.toStringBinary(row);
  }

  @Override
  public String getExceptionMessageAdditionalDetail() {
    return null;
  }

  @Override
  public long sleep(long pause, int tries) {
    long sleep = ConnectionUtils.getPauseTime(pause, tries + 1);
    if (sleep < MIN_WAIT_DEAD_SERVER
        && (location == null || connection.isDeadServer(location.getServerName()))) {
      sleep = ConnectionUtils.addJitter(MIN_WAIT_DEAD_SERVER, 0.10f);
    }
    return sleep;
  }

  public static RegionLocations getRegionLocations(
      ClusterConnection connection, TableName tableName, byte[] row,
      boolean useCache, int replicaId)
      throws RetriesExhaustedException, DoNotRetryIOException, InterruptedIOException {
    RegionLocations rl;
    try {
      rl = connection.locateRegion(tableName, row, useCache, true, replicaId);
    } catch (DoNotRetryIOException e) {
      throw e;
    } catch (NeedUnmanagedConnectionException e) {
      throw new DoNotRetryIOException(e);
    } catch (RetriesExhaustedException e) {
      throw e;
    } catch (InterruptedIOException e) {
      throw e;
    } catch (IOException e) {
      throw new RetriesExhaustedException("Can't get the location", e);
    }
    if (rl == null) {
      throw new RetriesExhaustedException("Can't get the locations");
    }

    return rl;
  }
}
