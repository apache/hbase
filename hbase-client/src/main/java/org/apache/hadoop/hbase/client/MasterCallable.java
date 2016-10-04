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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A RetryingCallable for Master RPC operations.
 * Implement the #rpcCall method. It will be retried on error. See its javadoc and the javadoc of
 * #call(int). See {@link HBaseAdmin} for examples of how this is used. To get at the
 * rpcController that has been created and configured to make this rpc call, use getRpcController().
 * We are trying to contain all protobuf references including references to rpcController so we
 * don't pollute codebase with protobuf references; keep the protobuf references contained and only
 * present in a few classes rather than all about the code base.
 * <p>Like {@link RegionServerCallable} only in here, we can safely be PayloadCarryingRpcController
 * all the time. This is not possible in the similar {@link RegionServerCallable} Callable because
 * it has to deal with Coprocessor Endpoints.
 * @param <V> return type
 */
abstract class MasterCallable<V> implements RetryingCallable<V>, Closeable {
  protected final ClusterConnection connection;
  protected MasterKeepAliveConnection master;
  private final HBaseRpcController rpcController;

  MasterCallable(final Connection connection, final RpcControllerFactory rpcConnectionFactory) {
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
      this.master = null;
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
   * Override that changes the {@link Callable#call()} Exception from {@link Exception} to
   * {@link IOException}. It also does setup of an rpcController and calls through to the rpcCall()
   * method which callers are expected to implement. If rpcController is an instance of
   * PayloadCarryingRpcController, we will set a timeout on it.
   */
  @Override
  // Same trick as in RegionServerCallable so users don't have to copy/paste so much boilerplate
  // and so we contain references to protobuf. We can't set priority on the rpcController as
  // we do in RegionServerCallable because we don't always have a Table when we call.
  public V call(int callTimeout) throws IOException {
    try {
      if (this.rpcController != null) {
        this.rpcController.reset();
        this.rpcController.setCallTimeout(callTimeout);
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
  protected abstract V rpcCall() throws Exception;

  HBaseRpcController getRpcController() {
    return this.rpcController;
  }

  void setPriority(final int priority) {
    if (this.rpcController != null) {
      this.rpcController.setPriority(priority);
    }
  }

  void setPriority(final TableName tableName) {
    if (this.rpcController != null) {
      this.rpcController.setPriority(tableName);
    }
  }

  /**
   * @param regionName RegionName. If hbase:meta, we'll set high priority.
   */
  void setPriority(final byte[] regionName) {
    if (isMetaRegion(regionName)) {
      setPriority(TableName.META_TABLE_NAME);
    }
  }

  private static boolean isMetaRegion(final byte[] regionName) {
    return Bytes.equals(regionName, HRegionInfo.FIRST_META_REGIONINFO.getRegionName())
        || Bytes.equals(regionName, HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes());
  }
}