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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;

import com.google.protobuf.RpcController;

/**
 * Implementations make an rpc call against a RegionService via a protobuf Service.
 * Implement rpcCall(). Be sure to make use of the RpcController that this instance is carrying
 * via {@link #getRpcController()}.
 *
 * <p>TODO: this class is actually tied to one region, because most of the paths make use of
 *       the regioninfo part of location when building requests. The only reason it works for
 *       multi-region requests (e.g. batch) is that they happen to not use the region parts.
 *       This could be done cleaner (e.g. having a generic parameter and 2 derived classes,
 *       RegionCallable and actual RegionServerCallable with ServerName.
 *
 * @param <T> the class that the ServerCallable handles
 */
@InterfaceAudience.Private
public abstract class RegionServerCallable<T> extends AbstractRegionServerCallable<T> {
  private ClientService.BlockingInterface stub;

  /* This is 99% of the time a PayloadCarryingRpcController but this RegionServerCallable is
   * also used doing Coprocessor Endpoints and in this case, it is a ServerRpcControllable which is
   * not a PayloadCarryingRpcController. Too hard to untangle it all at this stage since
   * downstreamers are using RegionServerCallable invoking CPEPs so just do ugly instanceof
   * checks in the below.
   */
  private final RpcController rpcController;

  /**
   * @param connection Connection to use.
   * @param tableName Table name to which <code>row</code> belongs.
   * @param row The row we want in <code>tableName</code>.
   */
  public RegionServerCallable(Connection connection, RpcControllerFactory rpcControllerFactory,
      TableName tableName, byte [] row) {
    this(connection, rpcControllerFactory.newController(), tableName, row);
  }

  public RegionServerCallable(Connection connection, RpcController rpcController,
      TableName tableName, byte [] row) {
    super(connection, tableName, row);
    this.rpcController = rpcController;
    // If it is an instance of PayloadCarryingRpcController, we can set priority on the
    // controller based off the tableName. RpcController may be null in tests when mocking so allow
    // for null controller.
    if (this.rpcController != null && this.rpcController instanceof PayloadCarryingRpcController) {
      ((PayloadCarryingRpcController)this.rpcController).setPriority(tableName);
    }
  }

  void setClientByServiceName(ServerName service) throws IOException {
    this.setStub(getConnection().getClient(service));
  }

  /**
   * @return Client Rpc protobuf communication stub
   */
  protected ClientService.BlockingInterface getStub() {
    return this.stub;
  }

  /**
   * Set the client protobuf communication stub
   * @param stub to set
   */
  void setStub(final ClientService.BlockingInterface stub) {
    this.stub = stub;
  }

  /**
   * Override that changes call Exception from {@link Exception} to {@link IOException}. It also
   * does setup of an rpcController and calls through to the unimplemented
   * rpcCall() method. If rpcController is an instance of PayloadCarryingRpcController,
   * we will set a timeout on it.
   */
  @Override
  public T call(int callTimeout) throws IOException {
    try {
      if (this.rpcController != null &&
          this.rpcController instanceof PayloadCarryingRpcController) {
        ((PayloadCarryingRpcController)this.rpcController).setCallTimeout(callTimeout);
        // Do a reset of the CellScanner in case we are carrying any Cells since last time through.
        setRpcControllerCellScanner(null);
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

  protected RpcController getRpcController() {
    return this.rpcController;
  }

  /**
   * Get the RpcController CellScanner.
   * If the RpcController is a PayloadCarryingRpcController, which it is in all cases except
   * when we are processing Coprocessor Endpoint, then this method returns a reference to the
   * CellScanner that the PayloadCarryingRpcController is carrying. Do it up here in this Callable
   * so we don't have to scatter ugly instanceof tests around the codebase. Will fail if called in
   * a Coproccessor Endpoint context. Should never happen.
   */
  protected CellScanner getRpcControllerCellScanner() {
    return ((PayloadCarryingRpcController)this.rpcController).cellScanner();
  }

  protected void setRpcControllerCellScanner(CellScanner cellScanner) {
    ((PayloadCarryingRpcController)this.rpcController).setCellScanner(cellScanner);
  }
}