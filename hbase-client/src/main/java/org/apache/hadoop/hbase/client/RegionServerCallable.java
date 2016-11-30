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

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implementations make a RPC call against a RegionService via a protobuf Service.
 * Implement rpcCall() and the parent class setClientByServiceName; this latter is where the
 * RPC stub gets set (the appropriate protobuf 'Service'/Client). Be sure to make use of the
 * RpcController that this instance is carrying via #getRpcController().
 *
 * <p>TODO: this class is actually tied to one region, because most of the paths make use of
 *       the regioninfo part of location when building requests. The only reason it works for
 *       multi-region requests (e.g. batch) is that they happen to not use the region parts.
 *       This could be done cleaner (e.g. having a generic parameter and 2 derived classes,
 *       RegionCallable and actual RegionServerCallable with ServerName.
 *
 * @param <T> The class that the ServerCallable handles.
 * @param <S> The protocol to use (Admin or Client or even an Endpoint over in MetaTableAccessor).
 */
// TODO: MasterCallable and this Class have a lot in common. UNIFY!
// Public but should be package private only it is used by MetaTableAccessor. FIX!!
@InterfaceAudience.Private
public abstract class RegionServerCallable<T, S> implements RetryingCallable<T> {
  private final Connection connection;
  private final TableName tableName;
  private final byte[] row;
  /**
   * Some subclasses want to set their own location. Make it protected.
   */
  protected HRegionLocation location;
  protected final static int MIN_WAIT_DEAD_SERVER = 10000;
  protected S stub;

  /**
   * This is 99% of the time a HBaseRpcController but also used doing Coprocessor Endpoints and in
   * this case, it is a ServerRpcControllable which is not a HBaseRpcController.
   * Can be null!
   */
  protected final RpcController rpcController;

  /**
   * @param connection Connection to use.
   * @param rpcController Controller to use; can be shaded or non-shaded.
   * @param tableName Table name to which <code>row</code> belongs.
   * @param row The row we want in <code>tableName</code>.
   */
  public RegionServerCallable(Connection connection, TableName tableName, byte [] row,
      RpcController rpcController) {
    super();
    this.connection = connection;
    if (tableName == null) {
      throw new IllegalArgumentException("Given tableName is null");
    }
    this.tableName = tableName;
    this.row = row;
    this.rpcController = rpcController;
  }

  protected RpcController getRpcController() {
    return this.rpcController;
  }

  protected void setStub(S stub) {
    this.stub = stub;
  }

  protected S getStub() {
    return this.stub;
  }

  /**
   * Override that changes call Exception from {@link Exception} to {@link IOException}.
   * Also does set up of the rpcController.
   */
  public T call(int callTimeout) throws IOException {
    try {
      // Iff non-null and an instance of a SHADED rpcController, do config! Unshaded -- i.e.
      // com.google.protobuf.RpcController or null -- will just skip over this config.
      if (getRpcController() != null) {
        RpcController shadedRpcController = (RpcController)getRpcController();
        // Do a reset to clear previous states, such as CellScanner.
        shadedRpcController.reset();
        if (shadedRpcController instanceof HBaseRpcController) {
          HBaseRpcController hrc = (HBaseRpcController)getRpcController();
          // If it is an instance of HBaseRpcController, we can set priority on the controller based
          // off the tableName. Set call timeout too.
          hrc.setPriority(tableName);
          hrc.setCallTimeout(callTimeout);
        }
      }
      return rpcCall();
    } catch (Exception e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Run the RPC call. Implement this method. To get at the rpcController that has been created
   * and configured to make this rpc call, use getRpcController(). We are trying to contain
   * rpcController references so we don't pollute codebase with protobuf references; keep the
   * protobuf references contained and only present in a few classes rather than all about the
   * code base.
   * @throws Exception
   */
  protected abstract T rpcCall() throws Exception;

  /**
   * Get the RpcController CellScanner.
   * If the RpcController is a HBaseRpcController, which it is in all cases except
   * when we are processing Coprocessor Endpoint, then this method returns a reference to the
   * CellScanner that the HBaseRpcController is carrying. Do it up here in this Callable
   * so we don't have to scatter ugly instanceof tests around the codebase. Will return null
   * if called in a Coproccessor Endpoint context. Should never happen.
   */
  protected CellScanner getRpcControllerCellScanner() {
    return (getRpcController() != null && getRpcController() instanceof HBaseRpcController)?
        ((HBaseRpcController)getRpcController()).cellScanner(): null;
  }

  protected void setRpcControllerCellScanner(CellScanner cellScanner) {
    if (getRpcController() != null && getRpcController() instanceof HBaseRpcController) {
      ((HBaseRpcController)this.rpcController).setCellScanner(cellScanner);
    }
  }

  /**
   * @return {@link ClusterConnection} instance used by this Callable.
   */
  protected ClusterConnection getConnection() {
    return (ClusterConnection) this.connection;
  }

  protected HRegionLocation getLocation() {
    return this.location;
  }

  protected void setLocation(final HRegionLocation location) {
    this.location = location;
  }

  public TableName getTableName() {
    return this.tableName;
  }

  public byte [] getRow() {
    return this.row;
  }

  public void throwable(Throwable t, boolean retrying) {
    if (location != null) {
      getConnection().updateCachedLocations(tableName, location.getRegionInfo().getRegionName(),
          row, t, location.getServerName());
    }
  }

  public String getExceptionMessageAdditionalDetail() {
    return "row '" + Bytes.toString(row) + "' on table '" + tableName + "' at " + location;
  }

  public long sleep(long pause, int tries) {
    long sleep = ConnectionUtils.getPauseTime(pause, tries);
    if (sleep < MIN_WAIT_DEAD_SERVER
        && (location == null || getConnection().isDeadServer(location.getServerName()))) {
      sleep = ConnectionUtils.addJitter(MIN_WAIT_DEAD_SERVER, 0.10f);
    }
    return sleep;
  }

  /**
   * @return the HRegionInfo for the current region
   */
  public HRegionInfo getHRegionInfo() {
    if (this.location == null) {
      return null;
    }
    return this.location.getRegionInfo();
  }

  public void prepare(final boolean reload) throws IOException {
    // check table state if this is a retry
    if (reload && !tableName.equals(TableName.META_TABLE_NAME) &&
        getConnection().isTableDisabled(tableName)) {
      throw new TableNotEnabledException(tableName.getNameAsString() + " is disabled.");
    }
    try (RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
      this.location = regionLocator.getRegionLocation(row);
    }
    if (this.location == null) {
      throw new IOException("Failed to find location, tableName=" + tableName +
          ", row=" + Bytes.toString(row) + ", reload=" + reload);
    }
    setStubByServiceName(this.location.getServerName());
  }

  /**
   * Set the RCP client stub
   * @param serviceName to get the rpc stub for
   * @throws IOException When client could not be created
   */
  protected abstract void setStubByServiceName(ServerName serviceName) throws IOException;
}