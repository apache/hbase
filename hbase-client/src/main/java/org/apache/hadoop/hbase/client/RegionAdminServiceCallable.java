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

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Similar to RegionServerCallable but for the AdminService interface. This service callable
 * assumes a Table and row and thus does region locating similar to RegionServerCallable.
 * Works against Admin stub rather than Client stub.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
  justification="stub used by ipc")
@InterfaceAudience.Private
public abstract class RegionAdminServiceCallable<T> implements RetryingCallable<T> {
  protected AdminService.BlockingInterface stub;
  protected final RpcControllerFactory rpcControllerFactory;
  private HBaseRpcController controller = null;

  protected final ClusterConnection connection;
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
    if (location != null) {
      connection.updateCachedLocations(tableName, location.getRegionInfo().getRegionName(), row,
          t, location.getServerName());
    }
  }

  /**
   * @return {@link Connection} instance used by this Callable.
   */
  Connection getConnection() {
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
    long sleep = ConnectionUtils.getPauseTime(pause, tries);
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

  /**
   * Override that changes Exception from {@link Exception} to {@link IOException}. It also does
   * setup of an rpcController and calls through to the unimplemented
   * call(PayloadCarryingRpcController) method; implement this method to add your rpc invocation.
   */
  @Override
  // Same trick as in RegionServerCallable so users don't have to copy/paste so much boilerplate
  // and so we contain references to protobuf. We can't set priority on the rpcController as
  // we do in RegionServerCallable because we don't always have a Table when we call.
  public T call(int callTimeout) throws IOException {
    this.controller = rpcControllerFactory.newController();
    this.controller.setPriority(this.tableName);
    this.controller.setCallTimeout(callTimeout);
    try {
      return call(this.controller);
    } catch (Exception e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  HBaseRpcController getCurrentPayloadCarryingRpcController() {
    return this.controller;
  }

  /**
   * Run RPC call.
   * @param rpcController PayloadCarryingRpcController is a mouthful but it at a minimum is a
   * facade on protobuf so we don't have to put protobuf everywhere; we can keep it behind this
   * class.
   * @throws Exception
   */
  protected abstract T call(HBaseRpcController rpcController) throws Exception;
}