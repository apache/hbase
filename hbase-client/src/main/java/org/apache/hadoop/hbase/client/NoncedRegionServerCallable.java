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

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;

/**
 * Implementations make an rpc call against a RegionService via a protobuf Service.
 * Implement #rpcCall(RpcController) and then call {@link #call(int)} to
 * trigger the rpc. The {@link #call(int)} eventually invokes your
 * #rpcCall(RpcController) meanwhile saving you having to write a bunch of
 * boilerplate. The {@link #call(int)} implementation is from {@link RpcRetryingCaller} so rpcs are
 * retried on fail.
 *
 * <p>TODO: this class is actually tied to one region, because most of the paths make use of
 *       the regioninfo part of location when building requests. The only reason it works for
 *       multi-region requests (e.g. batch) is that they happen to not use the region parts.
 *       This could be done cleaner (e.g. having a generic parameter and 2 derived classes,
 *       RegionCallable and actual RegionServerCallable with ServerName.
 * @param <T> the class that the ServerCallable handles
 */
@InterfaceAudience.Private
public abstract class NoncedRegionServerCallable<T> extends AbstractRegionServerCallable<T> {
  private ClientService.BlockingInterface stub;
  private final HBaseRpcController rpcController;
  private final long nonce;

  /**
   * @param connection Connection to use.
   * @param tableName Table name to which <code>row</code> belongs.
   * @param row The row we want in <code>tableName</code>.
   */
  public NoncedRegionServerCallable(Connection connection, RpcControllerFactory rpcControllerFactory,
      TableName tableName, byte [] row) {
    this(connection, rpcControllerFactory.newController(), tableName, row);
  }

  public NoncedRegionServerCallable(Connection connection, HBaseRpcController rpcController,
      TableName tableName, byte [] row) {
    super(connection, tableName, row);
    this.rpcController = rpcController;
    this.nonce = getConnection().getNonceGenerator().newNonce();
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
   * Override that changes Exception from {@link Exception} to {@link IOException}. It also does
   * setup of an rpcController and calls through to the unimplemented
   * call(PayloadCarryingRpcController) method; implement this method to add your rpc invocation.
   */
  @Override
  public T call(int callTimeout) throws IOException {
    if (this.rpcController != null) {
      this.rpcController.reset();
      this.rpcController.setPriority(tableName);
      this.rpcController.setCallTimeout(callTimeout);
    }
    try {
      return call(this.rpcController);
    } catch (Exception e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Run RPC call.
   * @param rpcController PayloadCarryingRpcController is a mouthful but it at a minimum is a
   * facade on protobuf so we don't have to put protobuf everywhere; we can keep it behind this
   * class.
   * @throws Exception
   */
  protected abstract T call(HBaseRpcController rpcController) throws Exception;

  public HBaseRpcController getRpcController() {
    return this.rpcController;
  }

  long getNonceGroup() {
    return getConnection().getNonceGenerator().getNonceGroup();
  }

  long getNonce() {
    return this.nonce;
  }
}