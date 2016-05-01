/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ipc;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionServerCallable;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;

/**
 * Provides clients with an RPC connection to call coprocessor endpoint {@link com.google.protobuf.Service}s
 * against a given table region.  An instance of this class may be obtained
 * by calling {@link org.apache.hadoop.hbase.client.Table#coprocessorService(byte[])},
 * but should normally only be used in creating a new {@link com.google.protobuf.Service} stub to call the endpoint
 * methods.
 * @see org.apache.hadoop.hbase.client.Table#coprocessorService(byte[])
 */
@InterfaceAudience.Private
public class RegionCoprocessorRpcChannel extends SyncCoprocessorRpcChannel {
  private static final Log LOG = LogFactory.getLog(RegionCoprocessorRpcChannel.class);

  private final ClusterConnection connection;
  private final TableName table;
  private final byte[] row;
  private byte[] lastRegion;
  private int operationTimeout;

  private RpcRetryingCallerFactory rpcCallerFactory;
  private RpcControllerFactory rpcControllerFactory;

  /**
   * Constructor
   * @param conn connection to use
   * @param table to connect to
   * @param row to locate region with
   */
  public RegionCoprocessorRpcChannel(ClusterConnection conn, TableName table, byte[] row) {
    this.connection = conn;
    this.table = table;
    this.row = row;
    this.rpcCallerFactory = conn.getRpcRetryingCallerFactory();
    this.rpcControllerFactory = conn.getRpcControllerFactory();
    this.operationTimeout = conn.getConnectionConfiguration().getOperationTimeout();
  }

  @Override
  protected Message callExecService(RpcController controller,
      Descriptors.MethodDescriptor method, Message request, Message responsePrototype)
          throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Call: "+method.getName()+", "+request.toString());
    }

    if (row == null) {
      throw new IllegalArgumentException("Missing row property for remote region location");
    }

    final RpcController rpcController = controller == null
        ? rpcControllerFactory.newController() : controller;

    final ClientProtos.CoprocessorServiceCall call =
        CoprocessorRpcUtils.buildServiceCall(row, method, request);
    RegionServerCallable<CoprocessorServiceResponse> callable =
        new RegionServerCallable<CoprocessorServiceResponse>(connection, table, row) {
      @Override
      public CoprocessorServiceResponse call(int callTimeout) throws Exception {
        if (rpcController instanceof PayloadCarryingRpcController) {
          ((PayloadCarryingRpcController) rpcController).setPriority(tableName);
        }
        if (rpcController instanceof TimeLimitedRpcController) {
          ((TimeLimitedRpcController) rpcController).setCallTimeout(callTimeout);
        }
        byte[] regionName = getLocation().getRegionInfo().getRegionName();
        return ProtobufUtil.execService(rpcController, getStub(), call, regionName);
      }
    };
    CoprocessorServiceResponse result = rpcCallerFactory.<CoprocessorServiceResponse> newCaller()
        .callWithRetries(callable, operationTimeout);
    Message response = null;
    if (result.getValue().hasValue()) {
      Message.Builder builder = responsePrototype.newBuilderForType();
      ProtobufUtil.mergeFrom(builder, result.getValue().getValue());
      response = builder.build();
    } else {
      response = responsePrototype.getDefaultInstanceForType();
    }
    lastRegion = result.getRegion().getValue().toByteArray();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Result is region=" + Bytes.toStringBinary(lastRegion) + ", value=" + response);
    }
    return response;
  }

  /**
   * Get last region this RpcChannel communicated with
   * @return region name as byte array
   */
  public byte[] getLastRegion() {
    return lastRegion;
  }
}
