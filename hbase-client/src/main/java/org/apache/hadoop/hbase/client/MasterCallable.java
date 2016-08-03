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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

/**
 * A RetryingCallable for master operations.
 * @param <V> return type
 */
// Like RegionServerCallable
abstract class MasterCallable<V> implements RetryingCallable<V>, Closeable {
  protected ClusterConnection connection;
  protected MasterKeepAliveConnection master;
  private final PayloadCarryingRpcController rpcController;

  MasterCallable(final Connection connection,
      final RpcControllerFactory rpcConnectionFactory) {
    this.connection = (ClusterConnection) connection;
    this.rpcController = rpcConnectionFactory.newController();
  }

  @Override
  public void prepare(boolean reload) throws IOException {
    this.master = this.connection.getKeepAliveMasterService();
  }

  @Override
  public void close() throws IOException {
    // The above prepare could fail but this would still be called though masterAdmin is null
    if (this.master != null) {
      this.master.close();
    }
  }

  @Override
  public void throwable(Throwable t, boolean retrying) {
  }

  @Override
  public String getExceptionMessageAdditionalDetail() {
    return "";
  }

  @Override
  public long sleep(long pause, int tries) {
    return ConnectionUtils.getPauseTime(pause, tries);
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
  public V call(int callTimeout) throws IOException {
    try {
      this.rpcController.setCallTimeout(callTimeout);
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
  protected abstract V call(PayloadCarryingRpcController rpcController) throws Exception;
}